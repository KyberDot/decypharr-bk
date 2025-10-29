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
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
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

// Manager manages sparse files for all remote files
type Manager struct {
	debrid      *store.Cache
	config      *config.FuseConfig
	files       *xsync.Map[string, *SparseFile]
	closeCtx    context.Context
	closeCancel context.CancelFunc

	// Stats tracker for passing to readers/files
	stats *StatsTracker

	// Smart caching: track file access for episode detection
	fileAccessTracker *xsync.Map[string, *FileAccessInfo]

	// Cached directory size (updated during cleanup)
	cachedDirSize atomic.Int64
	lastSizeCheck atomic.Int64 // Unix timestamp
}

// NewManager creates a sparseFile manager
func NewManager(debridCache *store.Cache, fuseConfig *config.FuseConfig) *Manager {
	// Create stats tracker
	statsTracker := &StatsTracker{}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:            fuseConfig,
		debrid:            debridCache,
		files:             xsync.NewMap[string, *SparseFile](),
		closeCtx:          ctx,
		closeCancel:       cancel,
		stats:             statsTracker,
		fileAccessTracker: xsync.NewMap[string, *FileAccessInfo](),
	}
	go m.closeIdleFilesLoop()
	go m.Cleanup(ctx)

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

func (m *Manager) closeIdleFiles() {
	threshold := time.Now().Add(-m.config.FileIdleTimeout)

	// Iterate through all files and close idle ones
	m.files.Range(func(key string, sf *SparseFile) bool {
		sf.mu.RLock()
		shouldClose := sf.lastAccess.Before(threshold) && sf.file != nil
		sf.mu.RUnlock()

		if shouldClose {
			_ = sf.closeFD()
		}
		return true // Continue iteration
	})
}

// GetOrCreateFile gets or creates a sparse file for caching
func (m *Manager) GetOrCreateFile(torrentName, filename string, size int64) (*SparseFile, error) {
	key := sanitizeForPath(filepath.Join(torrentName, filename))

	// Try to get existing file
	if sf, ok := m.files.Load(key); ok {
		// Verify the sparse file still exists on disk
		if m.sparseFileExists(sf) {
			return sf, nil
		}
		// File was deleted, remove from cache
		m.files.Delete(key)
		m.stats.TrackOpenFiles(-1)
	}

	// Create new sparse file
	sf, err := newSparseFile(m.config.CacheDir, torrentName, filename, size, m.config.ChunkSize, m.stats, m)
	if err != nil {
		return nil, err
	}

	// Try to store it, or use existing if someone else created it first
	actual, loaded := m.files.LoadOrStore(key, sf)
	if loaded {
		// Someone else created it first, close ours and use theirs
		_ = sf.Close()
		return actual, nil
	}

	// We created it successfully
	m.stats.TrackOpenFiles(1)
	return sf, nil
}

// sparseFileExists checks if the sparse file exists on disk
func (m *Manager) sparseFileExists(sf *SparseFile) bool {
	_, err := os.Stat(sf.path)
	return err == nil
}

// CreateReader creates a unified File with download capabilities
func (m *Manager) CreateReader(torrentName string, torrentFile types.File) (*File, error) {
	key := sanitizeForPath(filepath.Join(torrentName, torrentFile.Name))

	// Check if already exists
	if f, ok := m.files.Load(key); ok {
		// Make sure it has download capabilities
		if f.debrid != nil {
			return f, nil
		}
		// Old file without download support, remove it
		m.files.Delete(key)
		_ = f.Close()
	}

	// Create new file with download capabilities
	chunkSize := m.config.ChunkSize
	readAhead := m.config.ReadAheadSize

	if readAhead == 0 {
		readAhead = chunkSize * 2 // Minimum 2 chunks ahead for smooth playback
	}

	f, err := newFile(m.config.CacheDir, m.debrid, torrentName, torrentFile, chunkSize, readAhead, m.stats, m)
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

// Close closes all sparse files
func (m *Manager) Close() error {
	m.closeCancel()

	// Close all files
	m.files.Range(func(key string, sf *SparseFile) bool {
		_ = sf.Close()
		return true
	})
	m.files.Clear()
	return nil
}

func (m *Manager) CloseFile(filePath string) error {
	if sf, ok := m.files.LoadAndDelete(filePath); ok {
		m.stats.TrackOpenFiles(-1)
		return sf.Close()
	}
	return nil
}

func (m *Manager) RemoveFile(filePath string) error {
	if sf, ok := m.files.LoadAndDelete(filePath); ok {
		m.stats.TrackOpenFiles(-1)
		if err := sf.removeFromDisk(); err != nil {
			return err
		}
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

// syncAllMetadata saves metadata for all dirty files
// BUT: Only syncs files that haven't been accessed recently (not actively playing)
func (m *Manager) syncAllMetadata() {
	now := time.Now()

	// Iterate through all cached files
	m.files.Range(func(key string, sf *SparseFile) bool {
		// Only sync if dirty AND not recently accessed
		// If file was accessed in last 10 seconds, skip (likely active playback)
		sf.mu.RLock()
		isDirty := sf.dirty
		recentlyAccessed := now.Sub(sf.lastAccess) < 10*time.Second
		hasRanges := sf.ranges != nil
		sf.mu.RUnlock()

		if !isDirty || !hasRanges || recentlyAccessed {
			return true // Skip files that are clean, empty, or actively playing
		}

		// Sync in background to avoid blocking
		go func(sparseFile *SparseFile) {
			sparseFile.mu.Lock()
			defer sparseFile.mu.Unlock()
			if sparseFile.dirty && sparseFile.ranges != nil {
				_ = sparseFile.saveMetadata()
				sparseFile.dirty = false
			}
		}(sf)

		return true // Continue iteration
	})
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

		// Get actual disk usage (accounts for sparse files)
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

		// Get access time - prefer from in-memory tracking
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
	return m.removeFileFromDisk(diskPath)
}

// removeFileFromDisk removes a sparse file from disk
func (m *Manager) removeFileFromDisk(sparseFilePath string) error {
	// Remove sparse file
	if err := os.Remove(sparseFilePath); err != nil && !os.IsNotExist(err) {
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
		"chunk_size":      m.config.ChunkSize,
		"read_ahead_size": m.config.ReadAheadSize,
		"buffer_size":     m.config.BufferSize,
	}

	return stats
}

// prefetchNextEpisode prefetches the beginning of the next episode
func (m *Manager) prefetchNextEpisode(ctx context.Context, torrentName string, nextEpisode types.File) {
	// Create a File with download capabilities for next episode
	f, err := m.CreateReader(torrentName, nextEpisode)
	if err != nil {
		return
	}

	// Prefetch first few chunks (enough for instant start of next episode)
	numChunksToPrefetch := int64(3) // Prefetch first 3 chunks (24MB with 8MB chunks)
	totalChunks := (nextEpisode.Size + m.config.ChunkSize - 1) / m.config.ChunkSize
	if numChunksToPrefetch > totalChunks {
		numChunksToPrefetch = totalChunks
	}

	// Download chunks in background
	for chunkIdx := int64(0); chunkIdx < numChunksToPrefetch; chunkIdx++ {
		go f.downloadChunkAsync(ctx, chunkIdx)
	}
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
