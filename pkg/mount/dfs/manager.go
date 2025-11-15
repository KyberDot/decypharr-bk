package dfs

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	mounts  map[string]*Mount
	manager *manager.Manager
	logger  zerolog.Logger
	mu      sync.RWMutex
	ready   atomic.Bool
}

// NewManager creates a new  FUSE filesystem manager
func NewManager(manager *manager.Manager) *Manager {
	m := &Manager{
		manager: manager,
		logger:  logger.New("dfs"),
	}
	m.registerMounts()
	return m
}

func (m *Manager) registerMounts() {
	mounts := make(map[string]*Mount)
	for mountName := range m.manager.MountPaths() {
		mnt, err := NewMount(mountName, m.manager)
		if err != nil {
			m.logger.Error().Err(err).Msgf("Failed to create FUSE mount for debrid: %s", mountName)
			continue
		}
		mounts[mountName] = mnt
	}
	m.mu.Lock()
	m.mounts = mounts
	m.mu.Unlock()
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, mount := range m.mounts {
		wg.Add(1)
		go func(name string, mount *Mount) {
			defer wg.Done()
			if err := mount.Start(ctx); err != nil {
				m.logger.Error().Err(err).Msgf("Failed to mount FUSE filesystem for debrid: %s", name)
			} else {
				m.logger.Info().Msgf("Successfully mounted FUSE filesystem for debrid: %s", name)
			}
		}(name, mount)
	}
	wg.Wait()
	m.ready.Store(true)
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, mount := range m.mounts {
		if err := mount.Stop(); err != nil {
			m.logger.Error().Err(err).Msgf("Failed to unmount FUSE filesystem for debrid: %s", name)
		} else {
			m.logger.Info().Msgf("Successfully unmounted FUSE filesystem for debrid: %s", name)
		}
	}
	return nil
}

func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

// Stats returns unified statistics across all DFS mounts
func (m *Manager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Aggregate stats from all mounts
	aggregated := NewDFSStats()
	mountsInfo := make(map[string]interface{})

	var firstMountConfigSet bool

	for name, mount := range m.mounts {
		mountStats := mount.Stats()
		if mountStats == nil {
			continue
		}

		// Store individual mount stats
		mountsInfo[name] = mountStats.ToMap()

		// Aggregate totals
		aggregated.CacheDirSize.Add(mountStats.CacheDirSize)
		aggregated.CacheDirLimit.Add(mountStats.CacheDirLimit)
		aggregated.ActiveReads.Add(mountStats.ActiveReads)
		aggregated.OpenedFiles.Add(int64(mountStats.OpenedFiles))

		// GetReader config values from first mount (same across all mounts)
		if !firstMountConfigSet && mount.rfs != nil {
			rfsStats := mount.rfs.GetStats()
			// RFS doesn't expose chunk_size/read_ahead/buffer in stats
			// Use config values instead
			if mount.config != nil {
				aggregated.ChunkSize = mount.config.ChunkSize
				aggregated.ReadAheadSize = mount.config.ReadAheadSize
				aggregated.BufferSize = mount.config.BufferSize
			}
			_ = rfsStats // Avoid unused variable
			firstMountConfigSet = true
		}
	}

	return map[string]interface{}{
		"enabled": true,
		"ready":   m.ready.Load(),
		"type":    m.Type(),
		"mounts":  mountsInfo,
		"stats":   aggregated.ToMap(), // Clean, unified stats
	}
}

func (m *Manager) Type() string {
	return "dfs"
}
