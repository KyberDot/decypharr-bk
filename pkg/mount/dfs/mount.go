package dfs

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// Mount implements a FUSE filesystem with pluggable backends
type Mount struct {
	vfs         *vfs.Manager
	config      *fuseconfig.FuseConfig
	logger      zerolog.Logger
	rootNode    interface{} // backend-specific root node
	backend     backend.Backend
	manager     *manager.Manager
	name        string
	ready       atomic.Bool
	backendType backend.Type
}

// RootNodeWrapper implements backend.RootNode interface
type RootNodeWrapper struct {
	vfs      *vfs.Manager
	config   *fuseconfig.FuseConfig
	manager  *manager.Manager
	rootNode interface{}
}

func (r *RootNodeWrapper) GetVFS() *vfs.Manager {
	return r.vfs
}

func (r *RootNodeWrapper) GetConfig() *fuseconfig.FuseConfig {
	return r.config
}

func (r *RootNodeWrapper) GetManager() *manager.Manager {
	return r.manager
}

func (r *RootNodeWrapper) GetRootDir() interface{} {
	return r.rootNode
}

// Start starts the FUSE filesystem
func (m *Mount) Start(ctx context.Context) error {
	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Str("backend", string(m.backendType)).
		Msg("Starting DFS with backend")

	// Wrap root node for backend interface
	wrapper := &RootNodeWrapper{
		vfs:      m.vfs,
		config:   m.config,
		manager:  m.manager,
		rootNode: m.rootNode,
	}

	// Mount using the backend
	if err := m.backend.Mount(ctx, wrapper); err != nil {
		return fmt.Errorf("backend mount failed: %w", err)
	}

	m.ready.Store(true)
	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Str("backend", string(m.backendType)).
		Msg("DFS started successfully")
	return nil
}

// Stop stops the FUSE filesystem
func (m *Mount) Stop() error {
	m.logger.Info().
		Str("backend", string(m.backendType)).
		Msg("Stopping FUSE filesystem")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Unmount using backend
	if err := m.backend.Unmount(ctx); err != nil {
		m.logger.Warn().Err(err).Msg("Backend unmount error")
	}

	// Close VFS manager
	if m.vfs != nil {
		if err := m.vfs.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close VFS")
		}
	}

	m.ready.Store(false)
	return nil
}

// dfsDetail returns DFS-specific statistics for the unified MountStats.
func (m *Mount) dfsDetail() *manager.DFSDetail {
	d := &manager.DFSDetail{
		Backend: string(m.backendType),
		Ready:   m.ready.Load(),
	}
	if m.config != nil {
		d.MountPath = m.config.MountPath
	}
	if m.vfs != nil {
		d.VFS = m.vfs.Stats()
	}
	return d
}

// Type returns the mount type
func (m *Mount) Type() string {
	return "dfs"
}

// IsReady returns whether the mount is ready
func (m *Mount) IsReady() bool {
	return m.ready.Load() && m.backend.IsReady()
}

// Backend returns the backend type being used
func (m *Mount) Backend() backend.Type {
	return m.backendType
}

// newMount creates a new Mount with the common setup (used by platform-specific NewMount)
func newMount(mountName string, mgr *manager.Manager, backendType backend.Type) (*Mount, *vfs.Manager, *fuseconfig.FuseConfig, error) {
	fuseConfig, err := fuseconfig.ParseFuseConfig(mountName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse FUSE config: %w", err)
	}

	vfsManager, err := vfs.NewManager(context.Background(), mgr, fuseConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create vfs manager: %w", err)
	}

	log := logger.New("dfs").With().Str("mount", mountName).Str("backend", string(backendType)).Logger()

	mount := &Mount{
		vfs:         vfsManager,
		config:      fuseConfig,
		logger:      log,
		manager:     mgr,
		name:        mountName,
		backendType: backendType,
	}

	return mount, vfsManager, fuseConfig, nil
}
