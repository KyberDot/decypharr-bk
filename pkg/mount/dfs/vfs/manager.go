package vfs

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
	"golang.org/x/sync/singleflight"
)

// Manager manages Reader lifecycle
type Manager struct {
	manager *manager.Manager
	logger  zerolog.Logger
	config  *common.FuseConfig

	readers *xsync.Map[string, *ReaderEntry]
	cache   *Cache // Shared sparse file cache

	ctx       context.Context
	cancel    context.CancelFunc
	cleanupSg singleflight.Group

	totalReaders  atomic.Int32
	activeReaders atomic.Int32
	reuseCount    atomic.Int64
}

// ReaderEntry tracks reader metadata
type ReaderEntry struct {
	reader     *Reader
	refCount   atomic.Int32
	lastAccess atomic.Int64
}

// NewManager creates RFS manager
func NewManager(mgr *manager.Manager, cfg *common.FuseConfig) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create shared sparse cache
	cache, err := NewCache(ctx, cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	m := &Manager{
		manager: mgr,
		readers: xsync.NewMap[string, *ReaderEntry](),
		cache:   cache,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger.New("dfs-vfs"),
		config:  cfg,
	}
	go m.cleanupLoop(ctx, cfg.CacheCleanupInterval)

	return m, nil
}

// GetReader returns reader for file
func (m *Manager) GetReader(info *manager.FileInfo) (*Reader, error) {
	key := buildReaderKey(info.Parent(), info.Name())

	if entry, ok := m.readers.Load(key); ok {
		entry.refCount.Add(1)
		entry.lastAccess.Store(time.Now().UnixNano())
		m.reuseCount.Add(1)
		return entry.reader, nil
	}

	reader, err := NewReader(m.ctx, m.manager, info, m.cache, m.config)
	if err != nil {
		return nil, err
	}

	entry := &ReaderEntry{
		reader: reader,
	}
	entry.refCount.Store(1)
	entry.lastAccess.Store(time.Now().UnixNano())

	actual, loaded := m.readers.LoadOrStore(key, entry)
	if loaded {
		_ = reader.Close()
		actual.refCount.Add(1)
		actual.lastAccess.Store(time.Now().UnixNano())
		m.reuseCount.Add(1)
		return actual.reader, nil
	}

	m.totalReaders.Add(1)
	m.activeReaders.Add(1)
	return reader, nil
}

// ReleaseReader decrements refcount
func (m *Manager) ReleaseReader(info *manager.FileInfo) {
	key := buildReaderKey(info.Parent(), info.Name())

	if entry, ok := m.readers.Load(key); ok {
		entry.refCount.Add(-1)
		entry.lastAccess.Store(time.Now().UnixNano())
	}
}

// cleanupLoop removes idle readers
func (m *Manager) cleanupLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, _, _ = m.cleanupSg.Do("idle_reader_cleanup", func() (interface{}, error) {
				m.cleanupUnusedReaders()
				return nil, nil
			})

			_, _, _ = m.cleanupSg.Do("cache_cleanup", func() (interface{}, error) {
				m.cache.cleanup()
				return nil, nil
			})

		case <-ctx.Done():
			return
		}
	}
}

// cleanupUnusedReaders removes readers with zero refcount
// This runs periodically to clean up readers from closed file handles
func (m *Manager) cleanupUnusedReaders() {
	var toRemove []string

	m.readers.Range(func(key string, entry *ReaderEntry) bool {
		refCount := entry.refCount.Load()

		// Only cleanup readers with no active file handles
		// When FileIdleTimeout is 0, we clean up immediately when refCount hits 0
		if refCount <= 0 {
			toRemove = append(toRemove, key)
		}
		return true
	})

	if len(toRemove) > 0 {
		m.logger.Debug().Int("count", len(toRemove)).Msg("Cleaning up unused readers")
	}

	for _, key := range toRemove {
		if entry, ok := m.readers.LoadAndDelete(key); ok {
			_ = entry.reader.Close()
			m.activeReaders.Add(-1)
		}
	}
}

// Close shuts down manager
func (m *Manager) Close() error {
	m.cancel()

	m.readers.Range(func(key string, entry *ReaderEntry) bool {
		_ = entry.reader.Close()
		return true
	})
	m.readers.Clear()

	if m.cache != nil {
		_ = m.cache.Close()
	}

	return nil
}

// GetStats returns statistics including cache and reader stats
func (m *Manager) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"type":           "dfs",
		"ready":          true,
		"enabled":        true,
		"total_readers":  m.totalReaders.Load(),
		"active_readers": m.activeReaders.Load(),
		"reuse_count":    m.reuseCount.Load(),
	}

	// Add cache stats if available
	if m.cache != nil {
		cacheStats := m.cache.GetStats()
		stats["cache_total_size"] = cacheStats["total_size"]
		stats["cache_max_size"] = cacheStats["max_size"]
		stats["cache_file_count"] = cacheStats["file_count"]
		stats["cache_utilization"] = cacheStats["utilization"]

		// Add range tracking stats
		for k, v := range cacheStats {
			if k != "total_size" && k != "max_size" && k != "file_count" && k != "utilization" {
				stats["cache_"+k] = v
			}
		}
	}

	return stats
}

func buildReaderKey(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
