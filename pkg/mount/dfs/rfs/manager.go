package rfs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// Manager manages the lifecycle of Reader instances.
// Features:
// - Reader pooling and reuse across file handles
// - Automatic cleanup of idle readers
// - Global configuration for all readers
// - Shared disk cache across all readers
type Manager struct {
	manager *manager.Manager
	logger  zerolog.Logger
	config  *common.FuseConfig

	// Reader pool (key: parent/filename)
	readers *xsync.Map[string, *ReaderEntry]

	// Shared disk cache
	diskCache *DiskCache

	// Lifecycle
	ctx         context.Context
	cancel      context.CancelFunc
	cleanupStop chan struct{}
	wg          sync.WaitGroup

	// Statistics
	totalReaders  atomic.Int32
	activeReaders atomic.Int32
	reuseCount    atomic.Int64
}

// ReaderEntry tracks a reader and its metadata
type ReaderEntry struct {
	reader     *Reader
	refCount   atomic.Int32
	lastAccess atomic.Int64 // Unix nano timestamp
	mu         sync.Mutex
}

// NewManager creates a new RFS manager
func NewManager(mgr *manager.Manager, cfg *common.FuseConfig) *Manager {

	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		manager:     mgr,
		readers:     xsync.NewMap[string, *ReaderEntry](),
		ctx:         ctx,
		cancel:      cancel,
		cleanupStop: make(chan struct{}),
		logger:      logger.New("dfs-stream"),
		config:      cfg,
	}

	// Initialize shared disk cache if enabled
	if cfg.CacheDir != "" && cfg.CacheDiskSize > 0 {
		diskCache, err := NewDiskCache(cfg)
		if err != nil {
			fmt.Printf("Warning: failed to initialize disk cache: %v\n", err)
		} else {
			m.diskCache = diskCache
		}
	}

	// Start cleanup goroutine
	m.wg.Add(1)
	go m.cleanupLoop(cfg.FileIdleTimeout, cfg.CacheCleanupInterval)

	return m
}

// GetReader returns a reader for the given file, creating if needed.
// Readers are pooled and reused across multiple FileHandles.
func (m *Manager) GetReader(info *manager.FileInfo) (*Reader, error) {
	key := buildReaderKey(info.Parent(), info.Name())

	// Fast path: reader exists
	if entry, ok := m.readers.Load(key); ok {
		entry.refCount.Add(1)
		entry.lastAccess.Store(time.Now().UnixNano())
		m.reuseCount.Add(1)
		return entry.reader, nil
	}

	// Slow path: create new reader
	reader := NewReader(m.ctx, m.manager, info, m.config)

	// Share disk cache with reader
	if m.diskCache != nil {
		reader.diskCache = m.diskCache
		reader.downloader.diskCache = m.diskCache
	}

	entry := &ReaderEntry{
		reader: reader,
	}
	entry.refCount.Store(1)
	entry.lastAccess.Store(time.Now().UnixNano())

	// Try to store
	actual, loaded := m.readers.LoadOrStore(key, entry)
	if loaded {
		// Someone else created it first - close ours and use theirs
		_ = reader.Close()
		actual.refCount.Add(1)
		actual.lastAccess.Store(time.Now().UnixNano())
		m.reuseCount.Add(1)
		return actual.reader, nil
	}

	// We created it
	m.totalReaders.Add(1)
	m.activeReaders.Add(1)
	return reader, nil
}

// ReleaseReader decrements the reference count for a reader
func (m *Manager) ReleaseReader(info *manager.FileInfo) {
	key := buildReaderKey(info.Parent(), info.Name())

	if entry, ok := m.readers.Load(key); ok {
		entry.refCount.Add(-1)
		entry.lastAccess.Store(time.Now().UnixNano())
	}
}

// CloseReader explicitly closes a reader
func (m *Manager) CloseReader(parent, name string) error {
	key := buildReaderKey(parent, name)

	if entry, ok := m.readers.LoadAndDelete(key); ok {
		m.activeReaders.Add(-1)
		return entry.reader.Close()
	}

	return nil
}

// cleanupLoop periodically removes idle readers
func (m *Manager) cleanupLoop(idleTimeout, interval time.Duration) {
	defer m.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupIdle(idleTimeout)

		case <-m.cleanupStop:
			return
		}
	}
}

// cleanupIdle removes readers that haven't been accessed recently
func (m *Manager) cleanupIdle(idleTimeout time.Duration) {
	now := time.Now().UnixNano()
	threshold := now - idleTimeout.Nanoseconds()

	var toRemove []string

	m.readers.Range(func(key string, entry *ReaderEntry) bool {
		lastAccess := entry.lastAccess.Load()
		refCount := entry.refCount.Load()

		// Remove if idle and no references
		if refCount <= 0 && lastAccess < threshold {
			toRemove = append(toRemove, key)
		}

		return true
	})

	// Remove idle readers
	for _, key := range toRemove {
		if entry, ok := m.readers.LoadAndDelete(key); ok {
			_ = entry.reader.Close()
			m.activeReaders.Add(-1)
		}
	}
}

// Close shuts down the manager and all readers
func (m *Manager) Close() error {
	m.cancel()

	// Check if cleanupStop is already closed
	select {
	case <-m.cleanupStop:
		// Already closed
	default:
		close(m.cleanupStop)
	}
	m.wg.Wait()

	// Close all readers
	m.readers.Range(func(key string, entry *ReaderEntry) bool {
		_ = entry.reader.Close()
		return true
	})
	m.readers.Clear()

	// Close shared disk cache
	if m.diskCache != nil {
		_ = m.diskCache.Close()
	}

	return nil
}

// GetStats returns manager statistics
func (m *Manager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_readers":  m.totalReaders.Load(),
		"active_readers": m.activeReaders.Load(),
		"reuse_count":    m.reuseCount.Load(),
	}
}

// GetReaderStats returns stats for a specific reader
func (m *Manager) GetReaderStats(parent, name string) map[string]interface{} {
	key := buildReaderKey(parent, name)

	if entry, ok := m.readers.Load(key); ok {
		stats := entry.reader.GetStats()
		stats["ref_count"] = entry.refCount.Load()
		return stats
	}

	return nil
}

// buildReaderKey creates a unique key for a file
func buildReaderKey(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
