package rclone

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/rclone"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Mount represents a mount using the rclone RC client
type Mount struct {
	Provider  string
	MountPath string
	WebDAVURL string
	logger    zerolog.Logger
	info      atomic.Value
	client    *rclone.Client
}

func (m *Mount) Stats() map[string]interface{} {
	info := m.getMountInfo()
	mounted := false
	if info != nil {
		mounted = info.Mounted
	}
	return map[string]interface{}{
		"enabled":   true,
		"ready":     mounted,
		"type":      m.Type(),
		"provider":  m.Provider,
		"mountPath": m.MountPath,
		"webdavURL": m.WebDAVURL,
		"mounted":   mounted,
	}
}

// NewMount creates a new RC-based mount
func NewMount(mountName string, mgr *manager.Manager, rcClient *rclone.Client) (*Mount, error) {
	cfg := config.Get()
	bindAddress := cfg.BindAddress
	if bindAddress == "" {
		bindAddress = "localhost"
	}
	_logger := logger.New("rclone").With().Str("mount", mountName).Logger()

	baseUrl := fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	webdavUrl, err := url.JoinPath(baseUrl, cfg.URLBase, "webdav", mountName)
	if err != nil {
		return nil, fmt.Errorf("failed to construct WebDAV URL for %s: %w", mountName, err)
	}

	mountPath := filepath.Join(cfg.Mount.MountPath, mountName)
	if !strings.HasSuffix(webdavUrl, "/") {
		webdavUrl += "/"
	}

	m := &Mount{
		Provider:  mountName,
		MountPath: mountPath,
		WebDAVURL: webdavUrl,
		logger:    _logger,
		client:    rcClient,
	}
	mgr.AddEventHandlers(mountName, manager.NewEventHandlers(m))
	return m, nil
}

func (m *Mount) getMountInfo() *MountInfo {
	info, ok := m.info.Load().(*MountInfo)
	if !ok {
		return nil
	}
	return info
}

func (m *Mount) IsMounted() bool {
	info := m.getMountInfo()
	return info != nil && info.Mounted
}

// Start creates the mount using rclone RC
func (m *Mount) Start(ctx context.Context) error {
	// Check if already mounted
	if m.IsMounted() {
		m.logger.Info().Msg("Mount is already mounted")
		return nil
	}

	// Try to ping rcd
	if err := m.client.Ping(); err != nil {
		return fmt.Errorf("rclone RC server is not reachable: %w", err)
	}

	if err := m.mountWithRetry(3); err != nil {
		m.logger.Error().Msg("Mount operation failed")
		return fmt.Errorf("mount failed for %s", m.Provider)
	}
	go m.MonitorMounts(ctx)
	return nil
}

func (m *Mount) Stop() error {
	return m.Unmount()
}

func (m *Mount) Type() string {
	return "rcloneFS"
}

// Unmount removes the mount using rclone RC
func (m *Mount) Unmount() error {

	if !m.IsMounted() {
		m.logger.Info().Msgf("Mount %s is not mounted, skipping unmount", m.Provider)
		return nil
	}

	m.logger.Info().Msg("Unmounting via RC")

	if err := m.unmount(); err != nil {
		return fmt.Errorf("failed to unmount %s via RC: %w", m.Provider, err)
	}

	m.logger.Info().Msgf("Successfully unmounted %s", m.Provider)
	return nil
}

// Refresh refreshes directories in the VFS cache
func (m *Mount) Refresh(dirs []string) error {
	mountInfo := m.getMountInfo()
	if mountInfo == nil || !mountInfo.Mounted {
		return fmt.Errorf("mount is not mounted")
	}

	if err := m.client.Refresh(dirs, fmt.Sprintf("%s:", m.Provider)); err != nil {
		m.logger.Error().Err(err).
			Msg("Failed to refresh directory")
		return fmt.Errorf("failed to refresh directory %s for provider %s: %w", dirs, m.Provider, err)
	}
	return nil
}
