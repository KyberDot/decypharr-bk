package dfs

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
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
	mountInfo := m.manager.RootInfo()
	if mountInfo == nil {
		m.logger.Error().Msg("No mount info available to register DFS mount")
		return
	}
	// Use the new backend-aware mount constructor
	// This will use anacrolix backend by default (supports Fuse-T on macOS)
	mnt, err := NewMount(mountInfo.Name(), m.manager, backend.DefaultBackend())
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

func (m *Manager) Refresh(dirs []string) error {
	for _, dir := range dirs {
		m.mount.RefreshDirectory(dir)
	}
	return nil
}

// Stats returns unified mount statistics
func (m *Manager) Stats() *manager.MountStats {
	ms := &manager.MountStats{
		Enabled: true,
		Ready:   m.ready.Load(),
		Type:    m.Type(),
	}
	if m.mount != nil {
		ms.DFS = m.mount.dfsDetail()
	}
	return ms
}

func (m *Manager) Type() string {
	return "dfs"
}
