package external

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/rclone"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type Manager struct {
	mount   *Mount
	manager *manager.Manager
	client  *rclone.Client
	logger  zerolog.Logger
}

// NewManager creates a new external rclone manager
// This does nothing, just a placeholder to satisfy the interface
func NewManager(manager *manager.Manager) *Manager {
	_logger := logger.New("external")
	cfg := config.Get()
	rcloneClient := rclone.NewClient(
		cfg.Mount.ExternalRclone.RCUrl,
		cfg.Mount.ExternalRclone.RCUsername,
		cfg.Mount.ExternalRclone.RCPassword,
		_logger,
	)
	mnt, err := NewMount(manager)
	if err != nil {
		_logger.Error().Err(err).Msg("Failed to create external rclone mount")
	}
	m := &Manager{
		manager: manager,
		logger:  _logger,
		client:  rcloneClient,
		mount:   mnt,
	}
	return m
}

func (m *Manager) Start(ctx context.Context) error {
	return nil
}

func (m *Manager) Stop() error {
	return nil
}

// PreCache pre-caches file headers for faster scanning
// For external mounts, we read small chunks to populate any underlying cache
func (m *Manager) PreCache(filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	for _, filePath := range filePaths {
		if err := m.mount.preCacheFile(filePath); err != nil {
			m.logger.Debug().Err(err).Str("file", filePath).Msg("Failed to pre-cache file")
			// Continue with other files
		}
	}
	return nil
}

func (m *Manager) Refresh(dirs []string) error {
	return m.client.Refresh(dirs, "")
}

func (m *Manager) IsReady() bool {
	return true
}

func (m *Manager) Type() string {
	return "external"
}
