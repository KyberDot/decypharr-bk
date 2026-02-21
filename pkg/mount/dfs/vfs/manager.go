package vfs

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
)

// Manager manages VFS lifecycle
type Manager struct {
	manager *manager.Manager
	cache   *Cache
	logger  zerolog.Logger

	files *xsync.Map[string, *fileEntry]

	ctx    context.Context
	cancel context.CancelFunc

	totalFiles  atomic.Int32
	activeFiles atomic.Int32

	// Cumulative counters (since service start)
	totalBytesRead atomic.Int64
	totalErrors    atomic.Int64
}

// fileEntry tracks file metadata
type fileEntry struct {
	item     *CacheItem
	refCount atomic.Int32
}

// NewManager creates a new VFS manager
func NewManager(ctx context.Context, mgr *manager.Manager, config *config.FuseConfig) (*Manager, error) {
	ctx, cancel := context.WithCancel(ctx)

	cache, err := NewCache(ctx, mgr, config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	m := &Manager{
		manager: mgr,
		cache:   cache,
		logger:  logger.New("vfs"),
		files:   xsync.NewMap[string, *fileEntry](),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Wire stat counters so downloaders flow up to this manager
	cache.bytesReadCounter = &m.totalBytesRead
	cache.errorsCounter = &m.totalErrors

	return m, nil
}

// GetFile returns a streaming file handle
func (m *Manager) GetFile(info *manager.FileInfo) (*StreamingFile, error) {
	key := buildFileKey(info.Parent(), info.Name())

	// Fast path: existing file
	if entry, ok := m.files.Load(key); ok {
		entry.refCount.Add(1)
		return NewStreamingFile(entry.item), nil
	}

	// Get or create cache item
	item, err := m.cache.GetItem(info.Parent(), info.Name(), info.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to get cache item: %w", err)
	}

	entry := &fileEntry{item: item}
	entry.refCount.Store(1)

	// Store or return existing
	actual, loaded := m.files.LoadOrStore(key, entry)
	if loaded {
		// Another goroutine created it first
		actual.refCount.Add(1)
		return NewStreamingFile(actual.item), nil
	}

	m.totalFiles.Add(1)
	m.activeFiles.Add(1)
	return NewStreamingFile(item), nil
}

// ReleaseFile decrements the reference count
func (m *Manager) ReleaseFile(info *manager.FileInfo) {
	key := buildFileKey(info.Parent(), info.Name())

	if entry, ok := m.files.Load(key); ok {
		if entry.refCount.Add(-1) <= 0 {
			m.files.Delete(key)
			m.activeFiles.Add(-1)
			// Downloaders are stopped in CacheItem.Release() when opens reaches 0.
		}
	}
}

// Close shuts down the manager
func (m *Manager) Close() error {
	m.cancel()

	// Close all files
	m.files.Range(func(key string, entry *fileEntry) bool {
		if entry.item != nil {
			entry.item.Close()
		}
		return true
	})
	m.files.Clear()

	// Close cache
	if m.cache != nil {
		m.cache.Close()
	}

	return nil
}

// Stats returns manager statistics.
func (m *Manager) Stats() manager.VFSDetail {
	var cs manager.CacheDetail
	if m.cache != nil {
		cs = m.cache.Stats()
	}

	return manager.VFSDetail{
		TotalFiles:     m.totalFiles.Load(),
		ActiveFiles:    m.activeFiles.Load(),
		Cache:          cs,
		TotalBytesRead: m.totalBytesRead.Load(),
		TotalErrors:    m.totalErrors.Load(),
	}
}

func buildFileKey(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
