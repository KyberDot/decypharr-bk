package vfs

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// StatsTracker is a lightweight struct for tracking VFS statistics
type StatsTracker struct {
	activeReads atomic.Int64
	openedFiles atomic.Int64
}

// TrackActiveRead increments/decrements active read counter
func (st *StatsTracker) TrackActiveRead(delta int64) {
	st.activeReads.Add(delta)
}

func (st *StatsTracker) TrackOpenFiles(delta int64) {
	st.openedFiles.Add(delta)
}

// BuildCacheKey returns the canonical cache key used for tracking files in the VFS manager.
// The key mirrors the on-disk layout (sanitized parent directory + sanitized filename).
func BuildCacheKey(parent, name string) string {
	sanitizedParent := sanitizeForPath(parent)
	sanitizedName := sanitizeForPath(name)

	if sanitizedParent == "" {
		return sanitizedName
	}

	return filepath.Join(sanitizedParent, sanitizedName)
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

// FileAccessInfo tracks file access patterns for smart caching
type FileAccessInfo struct {
	TorrentName     string
	FileName        string
	LastAccessTime  time.Time
	LastReadOffset  int64
	FileSize        int64
	AccessCount     atomic.Int64
	IsNearEnd       atomic.Bool
	NextEpisode     string // Next episode filename if detected
	NextEpisodePath string // Full path to next episode
}

// Manager manages files for all remote files
type Manager struct {
	config      *common.FuseConfig
	files       *xsync.Map[string, *File]
	closeCtx    context.Context
	closeCancel context.CancelFunc

	// Stats tracker for passing to readers/files
	stats   *StatsTracker
	manager *manager.Manager

	// Smart caching: track file access for episode detection
	fileAccessTracker *xsync.Map[string, *FileAccessInfo]

	// Cached directory size (updated during cleanup)
	cachedDirSize atomic.Int64
	lastSizeCheck atomic.Int64 // Unix timestamp

}

// NewManager creates a file manager
func NewManager(manager *manager.Manager, fuseConfig *common.FuseConfig) *Manager {
	// Create stats tracker
	statsTracker := &StatsTracker{}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:            fuseConfig,
		manager:           manager,
		files:             xsync.NewMap[string, *File](),
		closeCtx:          ctx,
		closeCancel:       cancel,
		stats:             statsTracker,
		fileAccessTracker: xsync.NewMap[string, *FileAccessInfo](),
	}

	go m.closeIdleFilesLoop()
	go m.Cleanup(ctx)
	go m.metadataPersistLoop()

	return m
}

func (m *Manager) closeIdleFilesLoop() {
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

func (m *Manager) metadataPersistLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.flushDirtyMetadata()
		case <-m.closeCtx.Done():
			return
		}
	}
}

func (m *Manager) flushDirtyMetadata() {
	m.files.Range(func(key string, f *File) bool {
		_ = f.persistMetadata(false)
		return true
	})
}

func (m *Manager) closeIdleFiles() {
	threshold := time.Now().Add(-m.config.FileIdleTimeout).UnixNano()

	// Iterate through all files and close idle ones
	m.files.Range(func(key string, f *File) bool {
		lastAccess := f.lastAccess.Load()
		if lastAccess > 0 && lastAccess < threshold {
			_ = f.closeFD()
		}
		return true // Continue iteration
	})
}

// CreateReader creates a unified File with download capabilities
func (m *Manager) CreateReader(info *manager.FileInfo) (*File, error) {
	key := BuildCacheKey(info.Parent(), info.Name())

	// Check if already exists
	if f, ok := m.files.Load(key); ok {
		return f, nil
	}

	// Create new file with download capabilities
	chunkSize := m.config.ChunkSize
	readAhead := m.config.ReadAheadSize

	if readAhead == 0 {
		readAhead = chunkSize * 2 // Minimum 2 chunks ahead for smooth playback
	}

	f, err := newFile(m.config.CacheDir, info, chunkSize, readAhead, m.stats, m.manager, m)
	if err != nil {
		return nil, err
	}

	// Try to store (race-safe)
	actual, loaded := m.files.LoadOrStore(key, f)
	if loaded {
		// Someone else created it first, close ours and use theirs
		_ = f.Close()
		return actual, nil
	}

	// We created it successfully
	m.stats.TrackOpenFiles(1)
	return f, nil
}

// Close closes all files
func (m *Manager) Close() error {
	m.closeCancel()

	// Close all files
	m.files.Range(func(key string, f *File) bool {
		_ = f.Close()
		return true
	})
	m.files.Clear()
	return nil
}

func (m *Manager) CloseFile(cacheKey string) error {
	if f, ok := m.files.LoadAndDelete(cacheKey); ok {
		m.stats.TrackOpenFiles(-1)
		return f.Close()
	}
	return nil
}

func (m *Manager) RemoveFile(cacheKey string) error {
	if f, ok := m.files.LoadAndDelete(cacheKey); ok {
		m.stats.TrackOpenFiles(-1)
		if err := f.removeFromDisk(); err != nil {
			return err
		}
	}
	if cacheKey != "" {
		_ = m.deleteMetadata(cacheKey)
	}
	return nil
}

func (m *Manager) Cleanup(ctx context.Context) {
	// Clean up cache directory, runs every x duration
	cleanupTicker := time.NewTicker(m.config.CacheCleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-cleanupTicker.C:
			m.cleanup()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) cleanup() {
	// Quick check: if we have recent cached size and it's under limit, skip scan
	now := time.Now().Unix()
	lastCheck := m.lastSizeCheck.Load()
	cachedSize := m.cachedDirSize.Load()

	// If we checked recently (within 30s) and size is under 80% of limit, skip cleanup
	if cachedSize > 0 && (now-lastCheck) < 30 && cachedSize < (m.config.CacheDiskSize*8/10) {
		return
	}

	// Scan metadata directory first (much faster)
	// Falls back to actual cache scan if needed
	totalSize, fileList, err := m.scanMetadataDirectory()
	if err != nil || len(fileList) == 0 {
		// Fallback to actual cache scan
		totalSize, fileList, err = m.scanCacheDirectory()
		if err != nil {
			// Log error but don't crash
			return
		}
	}

	// Update cached size for stats
	m.cachedDirSize.Store(totalSize)
	m.lastSizeCheck.Store(time.Now().Unix())

	maxSize := m.config.CacheDiskSize
	if totalSize <= maxSize {
		return // Under limit, nothing to do
	}

	// Calculate how much to free - target 90% to avoid thrashing
	targetSize := maxSize * 9 / 10
	toFree := totalSize - targetSize

	// Sort by access time (least recently accessed first)
	// This gives us true LRU eviction
	sort.Slice(fileList, func(i, j int) bool {
		return fileList[i].accessTime.Before(fileList[j].accessTime)
	})

	// Remove least recently accessed files until under target
	var freed int64
	var filesRemoved int
	for _, fileInfo := range fileList {
		if freed >= toFree {
			break
		}

		// Remove the file (with proper locking and cleanup)
		if err := m.removeFile(fileInfo.cacheKey, fileInfo.path); err != nil {
			// Log but continue with other files
			continue
		}

		freed += fileInfo.size
		filesRemoved++
	}

	// Update cached size after cleanup
	if filesRemoved > 0 {
		newSize := totalSize - freed
		m.cachedDirSize.Store(newSize)
	}
}

type cachedFileInfo struct {
	path       string    // Full path to sparse file
	cacheKey   string    // Key in LRU cache (for removal)
	size       int64     // Actual disk usage (sparse-aware)
	accessTime time.Time // Last access time (for LRU eviction)
}

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

		// Skip metadata directory
		if strings.Contains(path, ".meta") {
			return nil
		}

		// Skip if already processed
		if seenFiles[path] {
			return nil
		}
		seenFiles[path] = true

		// GetReader actual disk usage (accounts for sparse files)
		fileSize, err := m.getActualDiskUsage(path, info)
		if err != nil {
			// Fallback to logical size
			fileSize = info.Size()
		}

		// Calculate cache key from path
		cacheKey, err := filepath.Rel(m.config.CacheDir, path)
		if err != nil {
			cacheKey = "" // Will skip removal if can't calculate key
		}

		// GetReader access time - prefer from in-memory tracking
		accessTime := m.getFileAccessTime(cacheKey, info)

		totalSize += fileSize
		fileList = append(fileList, cachedFileInfo{
			path:       path,
			cacheKey:   cacheKey,
			size:       fileSize,
			accessTime: accessTime,
		})

		return nil
	})

	return totalSize, fileList, err
}

// getFileAccessTime is implemented in platform-specific files:
// - cache_unix.go (Linux, FreeBSD, OpenBSD, NetBSD)
// - cache_darwin.go (macOS)
// - cache_windows.go (Windows)

// getActualDiskUsage returns the actual disk space used by a file (accounting for sparse files)
func (m *Manager) getActualDiskUsage(path string, info os.FileInfo) (int64, error) {
	// GetReader the underlying syscall.Stat_t structure
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

// removeFile removes a file from both in-memory cache and disk (thread-safe)
func (m *Manager) removeFile(cacheKey, diskPath string) error {
	// First, remove from in-memory cache
	if cacheKey != "" {
		if sf, ok := m.files.LoadAndDelete(cacheKey); ok {
			// Close the file cleanly before removing
			_ = sf.Close()
			m.stats.TrackOpenFiles(-1)
		}
	}

	// Then remove from disk
	if err := m.removeFileFromDisk(diskPath); err != nil {
		return err
	}

	if cacheKey != "" {
		_ = m.deleteMetadata(cacheKey)
	}

	return nil
}

// removeFileFromDisk removes a cached file from disk
func (m *Manager) removeFileFromDisk(filePath string) error {
	// Remove cached file
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// GetStats returns VFS cache statistics
func (m *Manager) GetStats() map[string]interface{} {
	// Use cached directory size if recent (within 30 seconds)
	now := time.Now().Unix()
	lastCheck := m.lastSizeCheck.Load()
	cachedSize := m.cachedDirSize.Load()

	var totalSize int64
	if cachedSize > 0 && (now-lastCheck) < 30 {
		// Use cached value (fast)
		totalSize = cachedSize
	} else {
		// Scan metadata directory (MUCH faster than scanning actual files)
		// Falls back to actual cache scan if metadata doesn't exist
		size, _, err := m.scanMetadataDirectory()
		if err != nil || size == 0 {
			// Fallback to actual cache scan
			size, _, _ = m.scanCacheDirectory()
		}
		totalSize = size
		m.cachedDirSize.Store(size)
		m.lastSizeCheck.Store(now)
	}

	stats := map[string]interface{}{
		"cache_dir_size":  totalSize,
		"cache_dir_limit": m.config.CacheDiskSize,
		"active_reads":    m.stats.activeReads.Load(),
		"opened_files":    m.stats.openedFiles.Load(),
	}

	// Aggregate memory buffer stats from all files
	var totalMemHits, totalMemMisses, totalEvictions, totalFlushes, totalFlushBytes int64
	var totalMemoryUsed, totalMemoryLimit int64
	var totalChunksCount int64
	fileCount := 0

	m.files.Range(func(key string, f *File) bool {
		if f.memBuffer != nil {
			memStats := f.memBuffer.GetStats()
			fileCount++

			if hits, ok := memStats["hits"].(int64); ok {
				totalMemHits += hits
			}
			if misses, ok := memStats["misses"].(int64); ok {
				totalMemMisses += misses
			}
			if evictions, ok := memStats["evictions"].(int64); ok {
				totalEvictions += evictions
			}
			if flushes, ok := memStats["flushes"].(int64); ok {
				totalFlushes += flushes
			}
			if flushBytes, ok := memStats["flush_bytes"].(int64); ok {
				totalFlushBytes += flushBytes
			}
			if memUsed, ok := memStats["memory_used"].(int64); ok {
				totalMemoryUsed += memUsed
			}
			if memLimit, ok := memStats["memory_limit"].(int64); ok {
				totalMemoryLimit += memLimit
			}
			if chunksCount, ok := memStats["chunks_count"].(int); ok {
				totalChunksCount += int64(chunksCount)
			}
		}
		return true
	})

	// Add memory buffer stats if we have any files with buffers
	if fileCount > 0 {
		hitRate := 0.0
		total := totalMemHits + totalMemMisses
		if total > 0 {
			hitRate = float64(totalMemHits) / float64(total) * 100.0
		}

		stats["memory_buffer"] = map[string]interface{}{
			"hits":         totalMemHits,
			"misses":       totalMemMisses,
			"hit_rate_pct": hitRate,
			"evictions":    totalEvictions,
			"flushes":      totalFlushes,
			"flush_bytes":  totalFlushBytes,
			"memory_used":  totalMemoryUsed,
			"memory_limit": totalMemoryLimit,
			"chunks_count": totalChunksCount,
			"files_count":  fileCount,
		}
	}

	return stats
}

// === Utility Functions ===

// Global replacer for path sanitization (avoids repeated allocations)
var pathSanitizer = strings.NewReplacer(
	"/", "_",
	"\\", "_",
	":", "_",
	"*", "_",
	"?", "_",
	"\"", "_",
	"<", "_",
	">", "_",
	"|", "_",
)

// sanitizeForPath makes a string safe for use in file paths
func sanitizeForPath(name string) string {
	// Replace problematic characters with underscores
	sanitized := pathSanitizer.Replace(name)

	// Limit length to prevent filesystem issues
	if len(sanitized) > 200 {
		sanitized = sanitized[:200]
	}

	return sanitized
}
