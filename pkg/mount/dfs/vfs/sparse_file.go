package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
)

// sanitizeForPath makes a string safe for use in file paths
func sanitizeForPath(name string) string {
	// Replace problematic characters with underscores
	replacer := strings.NewReplacer(
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
	sanitized := replacer.Replace(name)

	// Limit length to prevent filesystem issues
	if len(sanitized) > 200 {
		sanitized = sanitized[:200]
	}

	return sanitized
}

// SparseFile represents a cached file on disk with a bitmap tracking downloaded chunks
type SparseFile struct {
	path      string
	size      int64
	chunkSize int64

	file       *os.File
	bitmap     *bitset.BitSet
	mu         sync.RWMutex
	lastAccess time.Time
	stats      *StatsTracker // Stats tracker for cache hits/misses
}

// newSparseFile creates or opens a sparse cached file
func newSparseFile(cacheDir, torrentName, fileName string, size, chunkSize int64, stats *StatsTracker) (*SparseFile, error) {
	fileName = sanitizeForPath(fileName)
	torrentDir := filepath.Join(cacheDir, sanitizeForPath(torrentName))
	bitmapPath := filepath.Join(torrentDir, fileName+".bitmap")
	cachePath := filepath.Join(torrentDir, fileName)

	// Ensure sparseFile directory exists
	if err := os.MkdirAll(torrentDir, 0755); err != nil {
		return nil, fmt.Errorf("create sparseFile dir: %w", err)
	}

	// Open or create file
	file, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open sparseFile file: %w", err)
	}

	// Set file size (sparse allocation)
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate sparseFile file: %w", err)
	}

	// Calculate number of chunks
	numChunks := (size + chunkSize - 1) / chunkSize
	bitmap := bitset.New(uint(numChunks))

	// Try to load existing bitmap
	if data, err := os.ReadFile(bitmapPath); err == nil && len(data) > 0 {
		_ = bitmap.UnmarshalBinary(data)
	}

	return &SparseFile{
		path:      cachePath,
		size:      size,
		chunkSize: chunkSize,
		file:      file,
		bitmap:    bitmap,
		stats:     stats,
	}, nil
}

// ReadAt reads from sparseFile if available, returns false if chunk missing
func (sf *SparseFile) ReadAt(p []byte, offset int64) (n int, cached bool, err error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	sf.lastAccess = time.Now()

	// Check if this range is cached first (before opening file)
	startChunk := offset / sf.chunkSize
	endChunk := (offset + int64(len(p)) - 1) / sf.chunkSize

	// Check all chunks are present
	for chunk := startChunk; chunk <= endChunk; chunk++ {
		if !sf.bitmap.Test(uint(chunk)) {
			// Track cache miss
			sf.stats.TrackCacheMiss()
			return 0, false, nil // Not cached
		}
	}

	// Track cache hit
	sf.stats.TrackCacheHit()

	// Check if file exists before trying to open it
	if _, err := os.Stat(sf.path); os.IsNotExist(err) {
		// File was deleted externally - clear bitmap and return not cached
		sf.bitmap.ClearAll()
		return 0, false, nil
	}

	// Open file only when needed
	file := sf.file
	if file == nil {
		file, err = os.OpenFile(sf.path, os.O_RDWR, 0644)
		if err != nil {
			// If file doesn't exist, clear the bitmap
			if os.IsNotExist(err) {
				sf.bitmap.ClearAll()
				return 0, false, nil
			}
			return 0, false, err
		}
		// Don't store the file descriptor - close it after read
	}

	// All chunks present, read from disk
	n, err = file.ReadAt(p, offset)

	// Close FD immediately if we just opened it (not reusing existing)
	if sf.file == nil {
		_ = file.Close()
	}

	return n, true, err
}

// WriteAt writes data and marks chunks as cached
func (sf *SparseFile) WriteAt(p []byte, offset int64) (int, error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Open file if not already open
	file := sf.file
	if file == nil {
		var err error
		file, err = os.OpenFile(sf.path, os.O_RDWR, 0644)
		if err != nil {
			return 0, err
		}
		// Don't store the file descriptor - close it after write
	}

	n, err := file.WriteAt(p, offset)
	if err != nil {
		if sf.file == nil {
			_ = file.Close()
		}
		return n, err
	}

	// Mark chunks as cached
	startChunk := offset / sf.chunkSize
	endChunk := (offset + int64(n) - 1) / sf.chunkSize
	for chunk := startChunk; chunk <= endChunk; chunk++ {
		sf.bitmap.Set(uint(chunk))
	}

	// Close FD immediately if we just opened it
	if sf.file == nil {
		_ = file.Close()
	}

	return n, nil
}

// Sync flushes data and bitmap to disk
func (sf *SparseFile) Sync() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Sync file data if file is open
	if sf.file != nil {
		if err := sf.file.Sync(); err != nil {
			return err
		}
	}

	// Save bitmap
	data, err := sf.bitmap.MarshalBinary()
	if err != nil {
		return err
	}

	bitmapPath := sf.path + ".bitmap"
	return os.WriteFile(bitmapPath, data, 0644)
}

func (sf *SparseFile) closeFD() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if sf.file != nil {
		err := sf.file.Close()
		sf.file = nil
		return err
	}
	return nil
}

func (m *Manager) closeIdleFiles() {
	threshold := time.Now().Add(-m.config.FileIdleTimeout)

	m.mu.RLock()
	keys := m.files.Keys()
	m.mu.RUnlock()

	for _, key := range keys {
		m.mu.RLock()
		sf, ok := m.files.Peek(key)
		m.mu.RUnlock()

		if ok {
			sf.mu.RLock()
			if sf.lastAccess.Before(threshold) && sf.file != nil {
				sf.mu.RUnlock()
				_ = sf.closeFD()
			} else {
				sf.mu.RUnlock()
			}
		}
	}
}

// Close closes the sparse file and saves bitmap
func (sf *SparseFile) Close() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Save bitmap
	data, _ := sf.bitmap.MarshalBinary()
	bitmapPath := sf.path + ".bitmap"
	_ = os.WriteFile(bitmapPath, data, 0644)

	// Close file
	if sf.file != nil {
		return sf.file.Close()
	}
	return nil
}

func (sf *SparseFile) removeFromDisk() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Close file if open
	if sf.file != nil {
		_ = sf.file.Close()
		sf.file = nil
	}

	// Remove sparse file
	if err := os.Remove(sf.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
