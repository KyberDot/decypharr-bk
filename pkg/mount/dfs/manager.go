package dfs

import (
	"context"
	"sync"
)

var (
	globalManager *Manager
	globalOnce    sync.Once
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	mounts map[string]*Mount
	mu     sync.RWMutex
}

// NewManager creates a new  FUSE filesystem manager
func NewManager() *Manager {
	globalOnce.Do(func() {
		globalManager = &Manager{
			mounts: make(map[string]*Mount),
		}
	})
	return globalManager
}

// GetGlobalManager returns the global DFS manager instance (used to avoid import cycles)
func GetGlobalManager() *Manager {
	return globalManager
}

// RegisterMount registers a mount instance
func (m *Manager) RegisterMount(name string, mount *Mount) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mounts[name] = mount
}

// UnregisterMount removes a mount instance
func (m *Manager) UnregisterMount(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mounts, name)
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	// This doesn't have any preparation, Mount.Start handles starting mount for each debrid
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	// Mounter handles this
	return nil
}

func (m *Manager) IsReady() bool {
	return true
}

func (m *Manager) GetStats() (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled": true,
		"ready":   true,
		"type":    m.Type(),
	}

	// Collect stats from all registered mounts
	if len(m.mounts) > 0 {
		mountsInfo := make(map[string]interface{})
		var totalCacheHits, totalCacheMisses, totalNetworkReqs, totalNetworkBytes int64
		var totalReadOps, totalReadBytes, totalCachedFiles, totalCacheDiskUsed int64

		for name, mount := range m.mounts {
			if mount != nil && mount.vfs != nil {
				vfsStats := mount.vfs.GetStats()
				mountInfo := map[string]interface{}{
					"name":       name,
					"mounted":    true,
					"mount_path": mount.config.MountPath,
					"stats":      vfsStats,
				}
				mountsInfo[name] = mountInfo

				// Aggregate stats
				if cacheHits, ok := vfsStats["cache_hits"].(int64); ok {
					totalCacheHits += cacheHits
				}
				if cacheMisses, ok := vfsStats["cache_misses"].(int64); ok {
					totalCacheMisses += cacheMisses
				}
				if netReqs, ok := vfsStats["network_requests"].(int64); ok {
					totalNetworkReqs += netReqs
				}
				if netBytes, ok := vfsStats["network_bytes"].(int64); ok {
					totalNetworkBytes += netBytes
				}
				if readOps, ok := vfsStats["read_ops"].(int64); ok {
					totalReadOps += readOps
				}
				if readBytes, ok := vfsStats["read_bytes"].(int64); ok {
					totalReadBytes += readBytes
				}
				if cachedFiles, ok := vfsStats["cached_files"].(int); ok {
					totalCachedFiles += int64(cachedFiles)
				}
				if diskUsed, ok := vfsStats["cache_disk_used"].(int64); ok {
					totalCacheDiskUsed += diskUsed
				}
			}
		}

		stats["mounts"] = mountsInfo

		// Add aggregated stats
		totalAccess := totalCacheHits + totalCacheMisses
		hitRate := 0.0
		if totalAccess > 0 {
			hitRate = float64(totalCacheHits) / float64(totalAccess)
		}
		stats["stats"] = map[string]interface{}{
			"cache_hits":       totalCacheHits,
			"cache_misses":     totalCacheMisses,
			"cache_hit_rate":   hitRate,
			"network_requests": totalNetworkReqs,
			"network_bytes":    totalNetworkBytes,
			"read_ops":         totalReadOps,
			"read_bytes":       totalReadBytes,
			"cached_files":     totalCachedFiles,
			"cache_disk_used":  totalCacheDiskUsed,
		}
	}

	return stats, nil
}

// Type returns the type of mount manager
func (m *Manager) Type() string {
	return "dfs"
}
