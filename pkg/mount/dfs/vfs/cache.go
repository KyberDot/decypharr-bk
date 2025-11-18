package vfs

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

	"github.com/pkg/xattr"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

const (
	// xattr key for storing cached ranges
	xattrRangesKey = "user.decypharr.ranges"
)

// RangeMetadata stores serializable range information
type RangeMetadata struct {
	Ranges      []common.Range `json:"ranges"`
	LastAccess  int64          `json:"last_access"`
	CachedBytes int64          `json:"cached_bytes"` // Total bytes cached
	Version     int            `json:"version"`      // For future compatibility
}

// Cache manages sparse file caching with sharded range tracking
// All data goes to disk immediately, minimal memory usage
type Cache struct {
	cacheDir string
	maxSize  int64

	files sync.Map // map[string]*CacheFile
	total atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
}

// CacheFile represents a sparse cached file with sharded range tracking
type CacheFile struct {
	path       string
	file       *os.File
	size       int64
	lastAccess atomic.Int64
	refCount   atomic.Int32 // Reference count from active readers

	// Sharded range tracking for high concurrency
	ranges *common.ShardedRanges

	// Background snapshot updater
	snapshotStop chan struct{}
	snapshotOnce sync.Once
}

// NewCache creates a new sparse file cache
func NewCache(ctx context.Context, config *common.FuseConfig) (*Cache, error) {
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	c := &Cache{
		cacheDir: config.CacheDir,
		maxSize:  config.CacheDiskSize,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start periodic metadata saver (every 30 seconds)
	go c.periodicSaver()

	return c, nil
}

// periodicSaver saves cache metadata periodically to prevent data loss
func (c *Cache) periodicSaver() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.saveAllMetadata()
		case <-c.ctx.Done():
			return
		}
	}
}

// saveAllMetadata saves metadata for all open cache files
func (c *Cache) saveAllMetadata() {
	c.files.Range(func(key, value interface{}) bool {
		cf := value.(*CacheFile)
		_ = c.saveRangeMetadata(cf) // Ignore errors, will retry next time
		return true
	})
}

// GetOrCreate gets or creates a cached file and increments refCount
func (c *Cache) GetOrCreate(torrent, filename string, size int64) (*CacheFile, error) {
	key := filepath.Join(torrent, filename)

	if val, ok := c.files.Load(key); ok {
		cf := val.(*CacheFile)
		cf.lastAccess.Store(time.Now().UnixNano())
		cf.refCount.Add(1) // Increment reference count
		return cf, nil
	}

	cachePath := filepath.Join(c.cacheDir, torrent)
	if err := os.MkdirAll(cachePath, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	filePath := filepath.Join(cachePath, filename)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}

	// Truncate to size (creates sparse file)
	if err := file.Truncate(size); err != nil {
		file.Close()
		return nil, fmt.Errorf("truncate cache file: %w", err)
	}

	cf := &CacheFile{
		path:         filePath,
		file:         file,
		size:         size,
		ranges:       common.NewShardedRanges(),
		snapshotStop: make(chan struct{}),
	}

	// Try to load existing range metadata from xattr
	if err := c.loadRangeMetadata(cf); err != nil {
		// Not a fatal error - file might be new or xattr not supported
		// Just log and continue with empty ranges
		cf.lastAccess.Store(time.Now().UnixNano())
	}

	// Start background snapshot updater for this file
	cf.snapshotOnce.Do(func() {
		go cf.ranges.StartSnapshotUpdater(cf.snapshotStop)
	})

	actual, loaded := c.files.LoadOrStore(key, cf)
	if loaded {
		file.Close()
		close(cf.snapshotStop)
		actualCF := actual.(*CacheFile)
		actualCF.refCount.Add(1) // Increment reference count
		return actualCF, nil
	}

	// Set initial refCount to 1 for the newly created cache file
	cf.refCount.Store(1)
	return cf, nil
}

// ReadAt reads from cache (returns false if not cached)
func (c *Cache) ReadAt(cf *CacheFile, p []byte, offset int64) (n int, cached bool, err error) {
	cf.lastAccess.Store(time.Now().UnixNano())

	// Check if range is cached using sharded ranges (fast path)
	present := cf.ranges.Present(common.Range{Pos: offset, Size: int64(len(p))})
	if !present {
		return 0, false, nil
	}

	// Read from sparse file (no locks needed for file I/O)
	n, err = cf.file.ReadAt(p, offset)
	if err != nil {
		return n, false, err
	}

	return n, true, nil
}

// WriteAt writes to cache synchronously (must complete before returning)
func (c *Cache) WriteAt(cf *CacheFile, p []byte, offset int64) error {
	// Check if already cached (fast lockless check)
	r := common.Range{Pos: offset, Size: int64(len(p))}
	if cf.ranges.Present(r) {
		return nil // Already cached
	}

	// Write synchronously to ensure data is available immediately
	// This is critical for Reader which expects writes to complete
	_, err := cf.file.WriteAt(p, offset)
	if err != nil {
		return fmt.Errorf("write to cache file: %w", err)
	}

	// Update ranges using sharded tracking
	cf.ranges.Insert(r)
	c.total.Add(int64(len(p)))

	return nil
}

// WriteBatch writes multiple ranges in a batch for efficiency
func (c *Cache) WriteBatch(cf *CacheFile, writes []WriteOp) error {
	var ranges []common.Range
	var totalBytes int64

	for _, write := range writes {
		r := common.Range{Pos: write.Offset, Size: int64(len(write.Data))}

		// Skip if already cached
		if cf.ranges.Present(r) {
			continue
		}

		// Write to file
		_, err := cf.file.WriteAt(write.Data, write.Offset)
		if err != nil {
			return fmt.Errorf("batch write at %d: %w", write.Offset, err)
		}

		ranges = append(ranges, r)
		totalBytes += r.Size
	}

	// Batch update ranges
	if len(ranges) > 0 {
		cf.ranges.InsertBatch(ranges)
		c.total.Add(totalBytes)
	}

	return nil
}

// WriteOp represents a single write operation for batching
type WriteOp struct {
	Offset int64
	Data   []byte
}

// GetStats returns cache statistics including sharded range stats
func (c *Cache) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"total_size":  c.total.Load(),
		"max_size":    c.maxSize,
		"file_count":  c.getFileCount(),
		"utilization": float64(c.total.Load()) / float64(c.maxSize),
	}

	// Aggregate range stats from all files
	var totalRangeStats = make(map[string]int64)
	c.files.Range(func(key, value interface{}) bool {
		cf := value.(*CacheFile)
		fileStats := cf.ranges.GetStats()
		for k, v := range fileStats {
			totalRangeStats[k] += v
		}
		return true
	})

	for k, v := range totalRangeStats {
		stats[k] = v
	}

	return stats
}

// getFileCount returns the number of cached files
func (c *Cache) getFileCount() int {
	count := 0
	c.files.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// ReleaseCacheFile decrements refCount on a cache file
// Called when a Reader is closed
func (c *Cache) ReleaseCacheFile(cf *CacheFile) {
	cf.refCount.Add(-1)
	cf.lastAccess.Store(time.Now().UnixNano())
}

// CloseCacheFile closes a specific cache file
// Should only be called when refCount is 0
func (c *Cache) CloseCacheFile(cf *CacheFile) error {
	// Save range metadata to xattr before closing
	_ = c.saveRangeMetadata(cf)

	// Stop snapshot updater
	select {
	case <-cf.snapshotStop:
		// Already closed
	default:
		close(cf.snapshotStop)
	}

	// Close file handle
	return cf.file.Close()
}

// loadRangeMetadata loads cached ranges from xattr
func (c *Cache) loadRangeMetadata(cf *CacheFile) error {
	data, err := xattr.Get(cf.path, xattrRangesKey)
	if err != nil {
		// File might be new or xattr not set
		return err
	}

	var meta RangeMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("unmarshal range metadata: %w", err)
	}

	// Restore ranges
	if len(meta.Ranges) > 0 {
		cf.ranges.InsertBatch(meta.Ranges)
	}

	// Restore last access time
	if meta.LastAccess > 0 {
		cf.lastAccess.Store(meta.LastAccess)
	} else {
		cf.lastAccess.Store(time.Now().UnixNano())
	}

	// Restore cached bytes to global total
	if meta.CachedBytes > 0 {
		c.total.Add(meta.CachedBytes)
	}

	return nil
}

// saveRangeMetadata saves cached ranges to xattr
func (c *Cache) saveRangeMetadata(cf *CacheFile) error {
	// Get merged and sorted ranges from snapshot (most efficient)
	snapshot := cf.ranges.GetSnapshot()
	if snapshot == nil {
		// No snapshot yet, create empty metadata
		meta := RangeMetadata{
			Ranges:      []common.Range{},
			LastAccess:  cf.lastAccess.Load(),
			CachedBytes: 0,
			Version:     1,
		}

		data, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal range metadata: %w", err)
		}

		if err := xattr.Set(cf.path, xattrRangesKey, data); err != nil {
			return fmt.Errorf("set xattr: %w", err)
		}
		return nil
	}

	// Use snapshot ranges (already merged and sorted)
	allRanges := snapshot.GetRanges()
	var cachedBytes int64
	for _, r := range allRanges {
		cachedBytes += r.Size
	}

	meta := RangeMetadata{
		Ranges:      allRanges,
		LastAccess:  cf.lastAccess.Load(),
		CachedBytes: cachedBytes,
		Version:     1,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal range metadata: %w", err)
	}

	if err := xattr.Set(cf.path, xattrRangesKey, data); err != nil {
		return fmt.Errorf("set xattr: %w", err)
	}

	return nil
}

// cleanup removes least recently used cache files until under threshold
// Scans entire cache directory on disk, not just open files
func (c *Cache) cleanup() {
	if c.maxSize <= 0 {
		return
	}

	// Calculate actual disk usage by walking cache directory
	type fileEntry struct {
		path       string
		size       int64
		accessTime time.Time
		isOpen     bool
		cacheFile  *CacheFile
	}

	var entries []fileEntry
	totalSize := int64(0)

	// Walk cache directory to find all files
	err := filepath.Walk(c.cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		// Get actual disk usage (physical size for sparse files)
		size := getActualSize(path, info)

		// Check if file is currently open
		relPath, _ := filepath.Rel(c.cacheDir, path)
		val, isOpen := c.files.Load(relPath)

		var cf *CacheFile
		accessTime := info.ModTime()
		if isOpen {
			cf = val.(*CacheFile)
			// Use in-memory access time if available
			if lastAccess := cf.lastAccess.Load(); lastAccess > 0 {
				accessTime = time.Unix(0, lastAccess)
			}
		}

		entries = append(entries, fileEntry{
			path:       path,
			size:       size,
			accessTime: accessTime,
			isOpen:     isOpen,
			cacheFile:  cf,
		})
		totalSize += size

		return nil
	})

	if err != nil {
		return
	}

	// Check if cleanup needed
	if totalSize <= c.maxSize {
		return
	}

	// Target: reduce to 80% of maxSize to avoid thrashing
	targetSize := int64(float64(c.maxSize) * 0.8)
	toRemove := totalSize - targetSize

	// Sort by access time (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].accessTime.Before(entries[j].accessTime)
	})

	// Remove oldest files until we're under target
	removed := int64(0)
	for _, entry := range entries {
		if removed >= toRemove {
			break
		}

		// CRITICAL: Only close/remove if not actively in use
		if entry.isOpen && entry.cacheFile != nil {
			// Check if any readers are using this cache file
			if entry.cacheFile.refCount.Load() > 0 {
				// Skip - file is actively in use by readers
				continue
			}

			// Remove from cache map first
			relPath, _ := filepath.Rel(c.cacheDir, entry.path)
			c.files.Delete(relPath)

			// Close the cache file (safe now since refCount is 0)
			_ = c.CloseCacheFile(entry.cacheFile)
		}

		// Remove file from disk
		if err := os.Remove(entry.path); err == nil {
			removed += entry.size
		}
	}

	// Update total size
	c.total.Store(totalSize - removed)
}

// Close closes cache and all associated resources
func (c *Cache) Close() error {
	c.cancel()

	// Close all cache files
	c.files.Range(func(key, value interface{}) bool {
		cf := value.(*CacheFile)
		_ = c.CloseCacheFile(cf)
		return true
	})

	return nil
}

// ForceCleanup triggers an immediate cleanup cycle
func (c *Cache) ForceCleanup() {
	c.cleanup()
}

// GetCacheFile gets a cache file by torrent and filename (for debugging)
func (c *Cache) GetCacheFile(torrent, filename string) (*CacheFile, bool) {
	key := filepath.Join(torrent, filename)
	val, ok := c.files.Load(key)
	if !ok {
		return nil, false
	}
	return val.(*CacheFile), true
}
