package vfs

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
)

// StatsTracker is a lightweight struct for tracking VFS statistics
type StatsTracker struct {
	cacheHits      *atomic.Int64
	cacheMisses    *atomic.Int64
	networkReads   *atomic.Int64
	networkBytes   *atomic.Int64
	totalReadOps   *atomic.Int64
	totalReadBytes *atomic.Int64
}

// TrackCacheHit increments cache hit counter
func (st *StatsTracker) TrackCacheHit() {
	if st != nil && st.cacheHits != nil {
		st.cacheHits.Add(1)
	}
}

// TrackCacheMiss increments cache miss counter
func (st *StatsTracker) TrackCacheMiss() {
	if st != nil && st.cacheMisses != nil {
		st.cacheMisses.Add(1)
	}
}

// TrackNetworkRead increments network read counters
func (st *StatsTracker) TrackNetworkRead(bytes int64) {
	if st != nil && st.networkReads != nil && st.networkBytes != nil {
		st.networkReads.Add(1)
		st.networkBytes.Add(bytes)
	}
}

// TrackReadOp increments read operation counters
func (st *StatsTracker) TrackReadOp(bytes int64) {
	if st != nil && st.totalReadOps != nil && st.totalReadBytes != nil {
		st.totalReadOps.Add(1)
		st.totalReadBytes.Add(bytes)
	}
}

// CacheType represents the type of caching to perform
type CacheType int

const (
	CacheTypeOther CacheType = iota
	CacheTypeFFProbe
)

// String returns the string representation of the cache type
func (ct CacheType) String() string {
	switch ct {
	case CacheTypeFFProbe:
		return "ffprobe"
	case CacheTypeOther:
		return "other"
	default:
		return "unknown"
	}
}

// CacheRequest represents a request to cache a file range
type CacheRequest struct {
	TorrentName string
	FileName    string
	FileSize    int64
	StartOffset int64
	EndOffset   int64
	CacheType   CacheType
}

// Manager manages sparse files for all remote files
type Manager struct {
	debrid      *store.Cache
	config      *config.FuseConfig
	files       *lru.Cache[string, *SparseFile]
	mu          sync.RWMutex
	closeCtx    context.Context
	closeCancel context.CancelFunc
	wg          sync.WaitGroup

	// Stats tracking
	cacheHits      atomic.Int64
	cacheMisses    atomic.Int64
	networkReads   atomic.Int64
	networkBytes   atomic.Int64
	totalReadOps   atomic.Int64
	totalReadBytes atomic.Int64

	// Stats tracker for passing to readers/files
	stats *StatsTracker
}

// NewManager creates a sparseFile manager
func NewManager(debridCache *store.Cache, fuseConfig *config.FuseConfig) *Manager {
	// Each SparseFile holds: open FD + bitmap + state
	files, _ := lru.NewWithEvict(50, func(key string, sf *SparseFile) {
		_ = sf.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:      fuseConfig,
		debrid:      debridCache,
		files:       files,
		closeCtx:    ctx,
		closeCancel: cancel,
	}

	// Create stats tracker that references the Manager's atomic counters
	m.stats = &StatsTracker{
		cacheHits:      &m.cacheHits,
		cacheMisses:    &m.cacheMisses,
		networkReads:   &m.networkReads,
		networkBytes:   &m.networkBytes,
		totalReadOps:   &m.totalReadOps,
		totalReadBytes: &m.totalReadBytes,
	}

	m.wg.Add(1)
	go m.closeIdleFilesLoop()

	return m
}

func (m *Manager) closeIdleFilesLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.closeIdleFiles()
		case <-m.closeCtx.Done():
			return
		}
	}
}

// GetOrCreateFile gets or creates a sparse file for caching
func (m *Manager) GetOrCreateFile(torrentName, filename string, size int64) (*SparseFile, error) {
	key := sanitizeForPath(filepath.Join(torrentName, filename))

	m.mu.RLock()
	if sf, ok := m.files.Get(key); ok {
		m.mu.RUnlock()
		// Verify the sparse file still exists on disk
		if !m.sparseFileExists(sf) {
			// File was deleted, remove from cache and recreate
			m.files.Remove(key)
		} else {
			return sf, nil
		}
	} else {
		m.mu.RUnlock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check after acquiring write lock
	if sf, ok := m.files.Get(key); ok {
		if m.sparseFileExists(sf) {
			return sf, nil
		}
		// File was deleted, remove from cache
		m.files.Remove(key)
	}

	sf, err := newSparseFile(m.config.CacheDir, torrentName, filename, size, m.config.ChunkSize, m.stats)
	if err != nil {
		return nil, err
	}

	m.files.Add(key, sf)
	return sf, nil
}

// sparseFileExists checks if the sparse file exists on disk
func (m *Manager) sparseFileExists(sf *SparseFile) bool {
	_, err := os.Stat(sf.path)
	return err == nil
}

// CreateReader creates a reader optimized for scan operations
func (m *Manager) CreateReader(torrentName string, torrentFile types.File) (*Handle, error) {
	sf, err := m.GetOrCreateFile(torrentName, torrentFile.Name, torrentFile.Size)
	if err != nil {
		return nil, err
	}

	// For scans, use smaller chunks and more aggressive concurrency
	chunkSize := m.config.ChunkSize
	readAhead := m.config.ReadAheadSize
	maxConcurrent := m.config.MaxConcurrentReads

	if readAhead == 0 {
		readAhead = chunkSize * 2 // Minimum 2 chunks ahead for smooth playback
	}

	// Prevent deadlock: semaphore size must be at least 1
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	reader := NewReader(m.debrid, torrentName, torrentFile, sf, chunkSize, readAhead, maxConcurrent, m.stats)
	return NewHandle(reader), nil
}

// Close closes all sparse files
func (m *Manager) Close() error {
	m.closeCancel()
	m.wg.Wait()

	m.files.Purge() // Calls evict callback for all entries
	return nil
}

func (m *Manager) CloseFile(filePath string) error {
	m.files.Remove(filePath) // Calls evict callback
	return nil
}

func (m *Manager) RemoveFile(filePath string) error {
	if sf, exists := m.files.Peek(filePath); exists {
		if err := sf.removeFromDisk(); err != nil {
			return err
		}
		m.files.Remove(filePath)
	}
	return nil
}

func (m *Manager) Cleanup(ctx context.Context) error {
	// Clean up cache directory, runs every x duration
	ticker := time.NewTicker(m.config.CacheCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *Manager) cleanup() {
	// Check the actual disk usage of the cache directory
	totalSize, fileList, err := m.scanCacheDirectory()
	if err != nil {
		return
	}

	// Check if we exceed the max size
	maxSize := m.config.CacheDiskSize
	if totalSize <= maxSize {
		return // Under limit, nothing to do
	}

	// Need to free space - target 90% to avoid thrashing
	targetSize := maxSize * 9 / 10
	toFree := totalSize - targetSize

	// Sort files by modification time (oldest first)
	sort.Slice(fileList, func(i, j int) bool {
		return fileList[i].modTime.Before(fileList[j].modTime)
	})

	// Remove oldest files until we're under target
	var freed int64
	for _, file := range fileList {
		if freed >= toFree {
			break
		}

		// Remove the sparse file and its bitmap
		if err := m.removeFileFromDisk(file.path); err != nil {
			continue
		}

		freed += file.size
	}
}

type cachedFileInfo struct {
	path    string    // Full path to sparse file
	size    int64     // Combined size (file + bitmap)
	modTime time.Time // Last modification time
}

// scanCacheDirectory walks the cache directory and returns total size and file list
// scanCacheDirectory walks the cache directory and returns total size and file list
func (m *Manager) scanCacheDirectory() (int64, []cachedFileInfo, error) {
	var totalSize int64
	var fileList []cachedFileInfo
	seenFiles := make(map[string]bool)

	err := filepath.Walk(m.config.CacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Skip bitmap files for now, we'll account for them with their parent
		if strings.HasSuffix(path, ".bitmap") {
			totalSize += info.Size() // Bitmaps are small, regular size is fine
			return nil
		}

		// This is a sparse file - get actual disk usage
		if !seenFiles[path] {
			// Get actual blocks allocated (platform-specific)
			fileSize, err := m.getActualDiskUsage(path, info)
			if err != nil {
				// Fallback to logical size if we can't get actual usage
				fileSize = info.Size()
			}

			// Add bitmap size if it exists
			bitmapPath := path + ".bitmap"
			if bitmapInfo, err := os.Stat(bitmapPath); err == nil {
				fileSize += bitmapInfo.Size()
			}

			totalSize += fileSize
			fileList = append(fileList, cachedFileInfo{
				path:    path,
				size:    fileSize,
				modTime: info.ModTime(),
			})
			seenFiles[path] = true
		}

		return nil
	})

	return totalSize, fileList, err
}

// getActualDiskUsage returns the actual disk space used by a file (accounting for sparse files)
func (m *Manager) getActualDiskUsage(path string, info os.FileInfo) (int64, error) {
	// Get the underlying syscall.Stat_t structure
	sys := info.Sys()
	if sys == nil {
		return info.Size(), nil
	}

	// Platform-specific handling
	switch stat := sys.(type) {
	case *syscall.Stat_t:
		// Linux/Unix: blocks are 512 bytes, Blocks field gives count
		// Actual size = Blocks * 512
		return stat.Blocks * 512, nil
	default:
		// Fallback for unsupported platforms
		return info.Size(), nil
	}
}

// removeFileFromDisk removes a sparse file and its bitmap from disk
func (m *Manager) removeFileFromDisk(sparseFilePath string) error {
	// Also remove from in-memory cache if present
	// Construct the cache key from the path
	relPath, err := filepath.Rel(m.config.CacheDir, sparseFilePath)
	if err == nil {
		m.files.Remove(relPath)
	}

	// Remove sparse file
	if err := os.Remove(sparseFilePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove bitmap
	bitmapPath := sparseFilePath + ".bitmap"
	if err := os.Remove(bitmapPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// GetStats returns VFS cache statistics
func (m *Manager) GetStats() map[string]interface{} {
	m.mu.RLock()
	cacheSize := m.files.Len()
	m.mu.RUnlock()

	// Get actual disk usage statistics
	totalSize, fileList, _ := m.scanCacheDirectory()

	cacheHits := m.cacheHits.Load()
	cacheMisses := m.cacheMisses.Load()
	totalAccess := cacheHits + cacheMisses
	hitRate := 0.0
	if totalAccess > 0 {
		hitRate = float64(cacheHits) / float64(totalAccess)
	}

	stats := map[string]interface{}{
		"cache_hits":        cacheHits,
		"cache_misses":      cacheMisses,
		"cache_hit_rate":    hitRate,
		"network_requests":  m.networkReads.Load(),
		"network_bytes":     m.networkBytes.Load(),
		"read_ops":          m.totalReadOps.Load(),
		"read_bytes":        m.totalReadBytes.Load(),
		"cached_files":      cacheSize,
		"cache_disk_used":   totalSize,
		"cache_disk_limit":  m.config.CacheDiskSize,
		"cache_files_count": len(fileList),
		"chunk_size":        m.config.ChunkSize,
		"read_ahead_size":   m.config.ReadAheadSize,
	}

	return stats
}

// TrackCacheHit increments cache hit counter
func (m *Manager) TrackCacheHit() {
	m.cacheHits.Add(1)
}

// TrackCacheMiss increments cache miss counter
func (m *Manager) TrackCacheMiss() {
	m.cacheMisses.Add(1)
}

// TrackNetworkRead increments network read counters
func (m *Manager) TrackNetworkRead(bytes int64) {
	m.networkReads.Add(1)
	m.networkBytes.Add(bytes)
}

// TrackReadOp increments read operation counters
func (m *Manager) TrackReadOp(bytes int64) {
	m.totalReadOps.Add(1)
	m.totalReadBytes.Add(bytes)
}
