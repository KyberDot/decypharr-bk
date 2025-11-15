package rfs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// DiskCache provides persistent disk-based caching with sparse file support.
// Architecture similar to rclone's VFS cache:
// - Sparse files: Only cache accessed byte ranges
// - JSON metadata: Track which ranges are cached
// - Fast read-through: Check metadata before network
// - Write-behind: Async disk writes don't block reads
// - LRU eviction: Respects disk size limits
type DiskCache struct {
	// Configuration
	cacheDir     string
	maxCacheSize int64 // Maximum cache size in bytes
	chunkSize    int64 // Chunk size for alignment

	// Cache files (key: "parent/filename")
	files       *xsync.Map[string, *CachedFile]
	currentSize atomic.Int64
	totalWrites atomic.Int64
	totalReads  atomic.Int64
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
	evictions   atomic.Int64

	// Write queue for async disk writes
	writeQueue chan *WriteRequest
	writePool  *sync.Pool // Buffer pool for writes

	// Batched metadata persistence (avoid goroutine explosion!)
	dirtyFiles   *xsync.Map[string, *CachedFile] // Files with unpersisted metadata
	metadataTick *time.Ticker                    // Flush metadata periodically

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// CachedFile represents a single cached file with sparse ranges
type CachedFile struct {
	// Identification
	parent   string
	filename string
	fileSize int64

	// Cache paths
	dataPath     string // Path to actual data file
	metadataPath string // Path to metadata JSON

	// Range tracking (which parts are cached) - using rclone's ranges
	ranges       Ranges // Sorted, coalesced list of cached ranges
	rangesMu     sync.RWMutex
	totalCached  int64 // Total bytes cached in ranges
	lastAccess   time.Time
	lastModified time.Time

	// File descriptor for sparse file I/O
	file   *os.File
	fileMu sync.Mutex

	// Synchronization
	mu sync.Mutex
}

// FileMetadata is persisted to disk as JSON
type FileMetadata struct {
	Parent       string    `json:"parent"`
	Filename     string    `json:"filename"`
	FileSize     int64     `json:"file_size"`
	Ranges       Ranges    `json:"ranges"`
	LastAccess   time.Time `json:"last_access"`
	LastModified time.Time `json:"last_modified"`
	TotalCached  int64     `json:"total_cached"`
}

// WriteRequest represents an async disk write
type WriteRequest struct {
	parent   string
	filename string
	offset   int64
	data     []byte
}

// DiskCacheConfig configures the disk cache
type DiskCacheConfig struct {
	CacheDir      string
	MaxCacheSize  int64 // Maximum cache size in bytes
	ChunkSize     int64
	WriteWorkers  int // Number of async write workers
	WriteBufferMB int // Write buffer size
}

// DefaultDiskCacheConfig returns default disk cache configuration
func DefaultDiskCacheConfig() *DiskCacheConfig {
	return &DiskCacheConfig{
		CacheDir:      "/tmp/rfs-cache",
		MaxCacheSize:  10 * 1024 * 1024 * 1024, // 10GB default
		ChunkSize:     16 * 1024 * 1024,        // 16MB chunks
		WriteWorkers:  4,                       // 4 async writers
		WriteBufferMB: 64,                      // 64MB write buffer
	}
}

// NewDiskCache creates a new sparse disk cache
func NewDiskCache(cfg *common.FuseConfig) (*DiskCache, error) {
	// Create cache directories
	dataDir := filepath.Join(cfg.CacheDir, "data")
	metaDir := filepath.Join(cfg.CacheDir, "meta")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create meta dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	dc := &DiskCache{
		cacheDir:     cfg.CacheDir,
		maxCacheSize: cfg.CacheDiskSize,
		chunkSize:    cfg.ChunkSize,
		files:        xsync.NewMap[string, *CachedFile](),
		writeQueue:   make(chan *WriteRequest, 1000),
		writePool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, cfg.ChunkSize)
				return &buf
			},
		},
		dirtyFiles:   xsync.NewMap[string, *CachedFile](), // Track dirty metadata
		metadataTick: time.NewTicker(2 * time.Second),     // Flush metadata every 2 seconds
		ctx:          ctx,
		cancel:       cancel,
	}

	// Start write workers
	for i := 0; i < cfg.MaxConcurrentReads; i++ {
		dc.wg.Add(1)
		go dc.writeWorker()
	}

	// Start cleanup worker
	dc.wg.Add(1)
	go dc.cleanupWorker()

	// Start metadata flush worker (batched persistence!)
	dc.wg.Add(1)
	go dc.metadataFlushWorker()

	// Load existing cache metadata
	if err := dc.loadCacheMetadata(); err != nil {
		fmt.Printf("Warning: failed to load cache metadata: %v\n", err)
	}

	return dc, nil
}

// Get retrieves data from disk cache for a specific byte range
// Returns (data, hit, error)
func (dc *DiskCache) Get(parent, filename string, offset, length int64) ([]byte, bool, error) {
	key := buildFileKey(parent, filename)
	dc.totalReads.Add(1)

	// Fast path: Check if file has ANY cached data
	cachedFile, ok := dc.files.Load(key)
	if !ok {
		// File never cached before - fast miss
		dc.cacheMisses.Add(1)
		return nil, false, nil
	}

	// Check if file has any ranges at all (without locking)
	if len(cachedFile.ranges) == 0 {
		// No ranges cached yet - fast miss
		dc.cacheMisses.Add(1)
		return nil, false, nil
	}

	// Check if requested range is cached using rclone's Present method
	cachedFile.rangesMu.RLock()
	present := cachedFile.isRangePresent(offset, length)
	cachedFile.rangesMu.RUnlock()

	if !present {
		// Range not fully cached
		dc.cacheMisses.Add(1)
		return nil, false, nil
	}

	// Read from disk using persistent file descriptor (FAST!)
	cachedFile.mu.Lock()
	cachedFile.lastAccess = time.Now()

	// Ensure file descriptor is open
	cachedFile.fileMu.Lock()
	if cachedFile.file == nil {
		// Open file for reading
		file, err := os.OpenFile(cachedFile.dataPath, os.O_RDONLY, 0644)
		if err != nil {
			cachedFile.fileMu.Unlock()
			cachedFile.mu.Unlock()
			dc.cacheMisses.Add(1)
			return nil, false, nil
		}
		cachedFile.file = file
	}
	file := cachedFile.file
	cachedFile.fileMu.Unlock()
	cachedFile.mu.Unlock()

	// Read directly from persistent file descriptor (NO open/close overhead!)
	data := make([]byte, length)
	n, err := file.ReadAt(data, offset)
	if err != nil && n != int(length) {
		dc.cacheMisses.Add(1)
		return nil, false, nil
	}

	dc.cacheHits.Add(1)
	return data, true, nil
}

// Put stores data in disk cache at a specific offset (async)
func (dc *DiskCache) Put(parent, filename string, offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// Copy data to avoid caller reusing buffer
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	// Queue async write
	select {
	case dc.writeQueue <- &WriteRequest{
		parent:   parent,
		filename: filename,
		offset:   offset,
		data:     dataCopy,
	}:
		dc.totalWrites.Add(1)
		return nil
	case <-dc.ctx.Done():
		return fmt.Errorf("cache closed")
	default:
		// Write queue full - write synchronously
		return dc.writeData(parent, filename, offset, dataCopy)
	}
}

// WriteAtNoOverwrite writes data but skips already-cached ranges
// Returns (written, skipped, error) - CRITICAL for skip detection!
func (dc *DiskCache) WriteAtNoOverwrite(parent, filename string, offset int64, data []byte) (written int, skipped int, err error) {
	if len(data) == 0 {
		return 0, 0, nil
	}

	key := buildFileKey(parent, filename)

	// GetReader or create cached file
	cachedFile, _ := dc.files.LoadOrCompute(key, func() (*CachedFile, bool) {
		return &CachedFile{
			parent:       parent,
			filename:     filename,
			dataPath:     dc.buildDataPath(parent, filename),
			metadataPath: dc.buildMetadataPath(parent, filename),
			ranges:       Ranges{},
			lastAccess:   time.Now(),
			lastModified: time.Now(),
		}, false
	})

	// Ensure parent directory exists
	cachedFile.mu.Lock()
	if err := os.MkdirAll(filepath.Dir(cachedFile.dataPath), 0755); err != nil {
		cachedFile.mu.Unlock()
		return 0, 0, fmt.Errorf("failed to create cache dir: %w", err)
	}
	cachedFile.mu.Unlock()

	// Use WriteAtNoOverwrite - skips cached ranges!
	written, skipped, err = cachedFile.WriteAtNoOverwrite(data, offset)
	if err != nil {
		return
	}

	// Update cache size (only for written bytes, not skipped!)
	if written > 0 {
		dc.currentSize.Add(int64(written))
		dc.totalWrites.Add(1)

		// Mark file as dirty (batched metadata persistence!)
		dc.markDirty(key, cachedFile)

		// Check if eviction needed
		if dc.currentSize.Load() > dc.maxCacheSize {
			go dc.evictOldFiles()
		}
	}

	return written, skipped, nil
}

// writeWorker processes async disk writes
func (dc *DiskCache) writeWorker() {
	defer dc.wg.Done()

	for {
		select {
		case req := <-dc.writeQueue:
			if err := dc.writeData(req.parent, req.filename, req.offset, req.data); err != nil {
				fmt.Printf("Failed to write cache data: %v\n", err)
			}

		case <-dc.ctx.Done():
			return
		}
	}
}

// writeData writes data to disk and updates metadata
func (dc *DiskCache) writeData(parent, filename string, offset int64, data []byte) error {
	key := buildFileKey(parent, filename)

	// GetReader or create cached file
	cachedFile, _ := dc.files.LoadOrCompute(key, func() (*CachedFile, bool) {
		return &CachedFile{
			parent:       parent,
			filename:     filename,
			dataPath:     dc.buildDataPath(parent, filename),
			metadataPath: dc.buildMetadataPath(parent, filename),
			ranges:       Ranges{}, // rclone's ranges
			lastAccess:   time.Now(),
			lastModified: time.Now(),
		}, false
	})

	cachedFile.mu.Lock()
	defer cachedFile.mu.Unlock()

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(cachedFile.dataPath), 0755); err != nil {
		return fmt.Errorf("failed to create cache dir: %w", err)
	}

	// Open file for writing (create if doesn't exist)
	file, err := os.OpenFile(cachedFile.dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open cache file: %w", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	// Write data at offset (sparse file!)
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Update ranges using rclone's ranges - automatically merges overlapping/adjacent ranges!
	cachedFile.rangesMu.Lock()
	newRange := Range{Pos: offset, Size: int64(n)}
	cachedFile.ranges.Insert(newRange)

	// Recalculate total cached bytes
	cachedFile.totalCached = 0
	for _, r := range cachedFile.ranges {
		cachedFile.totalCached += r.Size
	}

	cachedFile.lastModified = time.Now()
	cachedFile.rangesMu.Unlock()

	// Update cache size
	dc.currentSize.Add(int64(n))

	// Mark file as dirty (batched metadata persistence!)
	dc.markDirty(key, cachedFile)

	// Check if eviction needed
	if dc.currentSize.Load() > dc.maxCacheSize {
		go dc.evictOldFiles()
	}

	return nil
}

// isRangePresent checks if a byte range is fully cached using rclone's Present method
func (cf *CachedFile) isRangePresent(offset, length int64) bool {
	r := Range{Pos: offset, Size: length}
	return cf.ranges.Present(r)
}

// findMissing returns the first missing range within the requested range
func (cf *CachedFile) findMissing(offset, length int64) (missingOffset int64, missingLength int64, found bool) {
	r := Range{Pos: offset, Size: length}
	missing := cf.ranges.FindMissing(r)

	if missing.IsEmpty() {
		return 0, 0, false // All cached
	}

	return missing.Pos, missing.Size, true
}

// WriteAtNoOverwrite writes data at offset, but SKIPS already-cached ranges.
// This is the CRITICAL optimization that prevents re-downloading cached data!
// Returns (written bytes, skipped bytes, error)
func (cf *CachedFile) WriteAtNoOverwrite(data []byte, offset int64) (written int, skipped int, err error) {
	if len(data) == 0 {
		return 0, 0, nil
	}

	// Ensure file is open
	cf.fileMu.Lock()
	if cf.file == nil {
		// Open file for sparse writes
		file, openErr := os.OpenFile(cf.dataPath, os.O_CREATE|os.O_RDWR, 0644)
		if openErr != nil {
			cf.fileMu.Unlock()
			return 0, 0, fmt.Errorf("failed to open cache file: %w", openErr)
		}
		cf.file = file
	}
	cf.fileMu.Unlock()

	requestRange := Range{Pos: offset, Size: int64(len(data))}

	// Check what parts of this range are already cached
	cf.rangesMu.RLock()
	foundRanges := cf.ranges.FindAll(requestRange)
	cf.rangesMu.RUnlock()

	currentOffset := offset
	dataOffset := 0

	for _, fr := range foundRanges {
		chunkSize := int(fr.R.Size)

		if fr.Present {
			// This range is already cached - SKIP IT!
			skipped += chunkSize
		} else {
			// This range is NOT cached - write it
			n, writeErr := cf.file.WriteAt(data[dataOffset:dataOffset+chunkSize], currentOffset)
			if writeErr != nil {
				return written, skipped, fmt.Errorf("write failed: %w", writeErr)
			}

			// Update ranges - rclone automatically merges!
			cf.rangesMu.Lock()
			cf.ranges.Insert(Range{Pos: currentOffset, Size: int64(n)})
			cf.rangesMu.Unlock()

			written += n
		}

		currentOffset += int64(chunkSize)
		dataOffset += chunkSize
	}

	// Update last modified time
	cf.mu.Lock()
	cf.lastModified = time.Now()
	cf.mu.Unlock()

	return written, skipped, nil
}

// Close closes the cached file and releases resources
func (cf *CachedFile) Close() error {
	cf.fileMu.Lock()
	defer cf.fileMu.Unlock()

	if cf.file != nil {
		err := cf.file.Close()
		cf.file = nil
		return err
	}

	return nil
}

// markDirty marks a file as having unpersisted metadata changes
func (dc *DiskCache) markDirty(key string, cf *CachedFile) {
	dc.dirtyFiles.Store(key, cf)
}

// metadataFlushWorker periodically flushes dirty metadata to disk (batched!)
func (dc *DiskCache) metadataFlushWorker() {
	defer dc.wg.Done()

	for {
		select {
		case <-dc.metadataTick.C:
			// Flush all dirty metadata
			dc.flushDirtyMetadata()

		case <-dc.ctx.Done():
			// Final flush on shutdown
			dc.flushDirtyMetadata()
			return
		}
	}
}

// flushDirtyMetadata persists all dirty file metadata to disk
func (dc *DiskCache) flushDirtyMetadata() {
	// Collect all dirty files
	dirtyCount := 0
	dc.dirtyFiles.Range(func(key string, cf *CachedFile) bool {
		// Persist this file's metadata
		if err := dc.persistMetadata(cf); err != nil {
			fmt.Printf("Warning: failed to persist metadata for %s: %v\n", key, err)
		}
		dirtyCount++
		return true
	})

	// Clear dirty set after flushing
	if dirtyCount > 0 {
		dc.dirtyFiles.Clear()
	}
}

// persistMetadata saves metadata to disk as JSON
func (dc *DiskCache) persistMetadata(cf *CachedFile) error {
	cf.rangesMu.RLock()
	metadata := FileMetadata{
		Parent:       cf.parent,
		Filename:     cf.filename,
		FileSize:     cf.fileSize,
		Ranges:       cf.ranges,
		LastAccess:   cf.lastAccess,
		LastModified: cf.lastModified,
		TotalCached:  cf.totalCached,
	}
	cf.rangesMu.RUnlock()

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(cf.metadataPath), 0755); err != nil {
		return fmt.Errorf("failed to create metadata dir: %w", err)
	}

	// Write to temp file first
	tempPath := cf.metadataPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, cf.metadataPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to rename metadata: %w", err)
	}

	return nil
}

// loadCacheMetadata loads existing cache metadata on startup
func (dc *DiskCache) loadCacheMetadata() error {
	metaDir := filepath.Join(dc.cacheDir, "meta")
	var totalSize int64
	filesLoaded := 0

	err := filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		// Skip temp files
		if filepath.Ext(path) == ".tmp" {
			_ = os.Remove(path)
			return nil
		}

		// Load metadata
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		var metadata FileMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			fmt.Printf("Warning: corrupted metadata file %s: %v\n", path, err)
			return nil
		}

		// Verify data file exists
		dataPath := dc.buildDataPath(metadata.Parent, metadata.Filename)
		if _, err := os.Stat(dataPath); err != nil {
			// Data file missing - remove metadata
			_ = os.Remove(path)
			return nil
		}

		// Create cached file entry
		key := buildFileKey(metadata.Parent, metadata.Filename)
		cachedFile := &CachedFile{
			parent:       metadata.Parent,
			filename:     metadata.Filename,
			fileSize:     metadata.FileSize,
			dataPath:     dataPath,
			metadataPath: path,
			ranges:       metadata.Ranges,
			totalCached:  metadata.TotalCached,
			lastAccess:   metadata.LastAccess,
			lastModified: metadata.LastModified,
		}

		dc.files.Store(key, cachedFile)
		totalSize += metadata.TotalCached
		filesLoaded++

		return nil
	})

	if err != nil {
		return err
	}

	dc.currentSize.Store(totalSize)
	return nil
}

// evictOldFiles removes least recently used files to free space
func (dc *DiskCache) evictOldFiles() {
	// Collect all files with access times
	type fileWithTime struct {
		key        string
		file       *CachedFile
		lastAccess time.Time
		size       int64
	}

	var files []fileWithTime
	dc.files.Range(func(key string, cf *CachedFile) bool {
		cf.mu.Lock()
		files = append(files, fileWithTime{
			key:        key,
			file:       cf,
			lastAccess: cf.lastAccess,
			size:       cf.totalCached,
		})
		cf.mu.Unlock()
		return true
	})

	// Sort by access time (oldest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].lastAccess.Before(files[j].lastAccess)
	})

	// Evict oldest files until under target
	target := dc.maxCacheSize * 75 / 100 // Target 75% of max
	evicted := 0

	for _, f := range files {
		if dc.currentSize.Load() <= target {
			break
		}

		// Delete data file
		_ = os.Remove(f.file.dataPath)
		// Delete metadata file
		_ = os.Remove(f.file.metadataPath)

		// Update size
		dc.currentSize.Add(-f.size)

		// Remove from cache
		dc.files.Delete(f.key)
		dc.evictions.Add(1)
		evicted++
	}

	if evicted > 0 {
		fmt.Printf("Evicted %d files, cache size: %d MB\n",
			evicted, dc.currentSize.Load()/(1024*1024))
	}
}

// cleanupWorker periodically cleans up orphaned files
func (dc *DiskCache) cleanupWorker() {
	defer dc.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dc.cleanupOrphanedFiles()

		case <-dc.ctx.Done():
			return
		}
	}
}

// cleanupOrphanedFiles removes data files without metadata
func (dc *DiskCache) cleanupOrphanedFiles() {
	dataDir := filepath.Join(dc.cacheDir, "data")

	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		// Check if metadata exists
		found := false
		dc.files.Range(func(key string, cf *CachedFile) bool {
			if cf.dataPath == path {
				found = true
				return false // Stop iteration
			}
			return true
		})

		// Remove orphaned file
		if !found {
			_ = os.Remove(path)
		}

		return nil
	})
}

// buildDataPath creates the disk path for a cached file
func (dc *DiskCache) buildDataPath(parent, filename string) string {
	return filepath.Join(dc.cacheDir, "data", parent, filename)
}

// buildMetadataPath creates the metadata path for a cached file
func (dc *DiskCache) buildMetadataPath(parent, filename string) string {
	return filepath.Join(dc.cacheDir, "meta", parent, filename+".json")
}

// buildFileKey creates a unique key for a file
func buildFileKey(parent, filename string) string {
	if parent == "" {
		return filename
	}
	return parent + "/" + filename
}

// Close shuts down the disk cache
func (dc *DiskCache) Close() error {
	// Stop metadata ticker
	dc.metadataTick.Stop()

	dc.cancel()
	close(dc.writeQueue)
	dc.wg.Wait()

	// Persist all metadata and close file descriptors
	dc.files.Range(func(key string, cf *CachedFile) bool {
		_ = dc.persistMetadata(cf)
		_ = cf.Close() // Close file descriptor
		return true
	})

	return nil
}

// GetStats returns cache statistics
func (dc *DiskCache) GetStats() map[string]interface{} {
	cacheHits := dc.cacheHits.Load()
	cacheMisses := dc.cacheMisses.Load()
	totalReads := cacheHits + cacheMisses
	hitRate := 0.0
	if totalReads > 0 {
		hitRate = float64(cacheHits) / float64(totalReads) * 100
	}

	return map[string]interface{}{
		"cache_size_mb": dc.currentSize.Load() / (1024 * 1024),
		"max_size_mb":   dc.maxCacheSize / (1024 * 1024),
		"files_cached":  dc.files.Size(),
		"cache_hits":    cacheHits,
		"cache_misses":  cacheMisses,
		"hit_rate":      fmt.Sprintf("%.2f%%", hitRate),
		"total_writes":  dc.totalWrites.Load(),
		"total_reads":   dc.totalReads.Load(),
		"evictions":     dc.evictions.Load(),
	}
}

// Clear removes all cached data
func (dc *DiskCache) Clear() error {
	dc.files.Range(func(key string, cf *CachedFile) bool {
		_ = os.Remove(cf.dataPath)
		_ = os.Remove(cf.metadataPath)
		dc.files.Delete(key)
		return true
	})

	dc.currentSize.Store(0)
	return nil
}
