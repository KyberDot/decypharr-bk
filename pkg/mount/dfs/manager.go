package dfs

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	mount   *Mount
	manager *manager.Manager
	logger  zerolog.Logger
	ready   atomic.Bool
}

// NewManager creates a new  FUSE filesystem manager
func NewManager(manager *manager.Manager) *Manager {
	m := &Manager{
		manager: manager,
		logger:  logger.New("dfs"),
	}
	m.registerMount()
	return m
}

func (m *Manager) registerMount() {
	mountInfo := m.manager.FirstMountInfo()
	if mountInfo == nil {
		m.logger.Error().Msg("No mount info available to register DFS mount")
		return
	}
	mnt, err := NewMount(mountInfo.Name(), m.manager)
	if err != nil {
		m.logger.Error().Err(err).Msgf("Failed to create DFS mount for: %s", mountInfo.Name())
		return
	}
	m.mount = mnt
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	if m.mount == nil {
		return fmt.Errorf("mount not initialized")
	}
	if err := m.mount.Start(ctx); err != nil {
		m.logger.Error().Err(err).Msgf("Failed to mount FUSE filesystem")
	} else {
		m.logger.Info().Msgf("Successfully mounted FUSE filesystem for debrid")
	}
	m.ready.Store(true)
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	if m.mount == nil {
		return fmt.Errorf("mount not initialized")
	}
	return m.mount.Stop()
}

func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

// PreCache pre-caches file headers for faster scanning
// For DFS, we download the first chunk which contains metadata
func (m *Manager) PreCache(filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	const headerSize = 256 * 1024 // 256KB - enough for container headers

	for _, filePath := range filePaths {
		// Extract torrent name and filename from path
		// Path format: mountPath/torrentName/filename
		relPath := filePath
		if len(m.mount.config.MountPath) > 0 && len(filePath) > len(m.mount.config.MountPath) {
			relPath = filePath[len(m.mount.config.MountPath):]
		}
		relPath = strings.TrimPrefix(relPath, "/")

		parts := strings.SplitN(relPath, "/", 2)
		if len(parts) < 2 {
			continue
		}

		torrentName := parts[0]
		filename := parts[1]

		// Get file info from manager
		fileInfo, err := m.manager.GetTorrentFile(torrentName, filename)
		if err != nil {
			m.logger.Debug().Err(err).Str("file", filePath).Msg("Failed to get file info for pre-cache")
			continue
		}

		// Get or create reader (this will trigger cache creation)
		reader, err := m.mount.vfs.GetReader(fileInfo)
		if err != nil {
			m.logger.Debug().Err(err).Str("file", filePath).Msg("Failed to get reader for pre-cache")
			continue
		}

		// Pre-fetch header
		size := min(headerSize, fileInfo.Size())
		if err := reader.Prefetch(0, size); err != nil {
			m.logger.Debug().Err(err).Str("file", filePath).Msg("Failed to prefetch header")
		}

		// Release reader
		m.mount.vfs.ReleaseReader(fileInfo)
	}

	return nil
}

func (m *Manager) Refresh(dirs []string) error {
	for _, dir := range dirs {
		m.mount.refreshDirectory(dir)
	}
	return nil
}

// Stats returns unified statistics across all DFS mounts
func (m *Manager) Stats() map[string]interface{} {
	// Aggregate stats from all mounts
	stats := map[string]interface{}{
		"enabled": true,
		"ready":   m.ready.Load(),
		"type":    m.Type(),
	}
	for key, stat := range m.mount.Stats() {
		stats[key] = stat
	}
	return stats
}

func (m *Manager) Type() string {
	return "dfs"
}
