package rclone

import (
	"context"
	"fmt"
	"net/url"
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
	MountPath string
	WebDAVURL string
	logger    zerolog.Logger
	info      atomic.Value
	client    *rclone.Client
}

// NewMount creates a new RC-based mount
func NewMount(mgr *manager.Manager, rcClient *rclone.Client) (*Mount, error) {
	cfg := config.Get()
	bindAddress := cfg.BindAddress
	if bindAddress == "" {
		bindAddress = "localhost"
	}

	baseUrl := fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	webdavUrl, err := url.JoinPath(baseUrl, cfg.URLBase, "webdav")
	if err != nil {
		return nil, fmt.Errorf("failed to construct WebDAV URL: %w", err)
	}

	if !strings.HasSuffix(webdavUrl, "/") {
		webdavUrl += "/"
	}

	m := &Mount{
		MountPath: cfg.Mount.MountPath,
		WebDAVURL: webdavUrl,
		logger:    logger.New("rclone-mount"),
		client:    rcClient,
	}
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
	if err := m.client.Ping(ctx); err != nil {
		return fmt.Errorf("rclone RC server is not reachable: %w", err)
	}

	if err := m.mountWithRetry(3); err != nil {
		m.logger.Error().Msg("Mount operation failed")
		return fmt.Errorf("mount failed for")
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
		m.logger.Info().Msgf("Mount is not mounted, skipping unmount")
		return nil
	}

	m.logger.Info().Msg("Unmounting via RC")

	m.unmount()

	m.logger.Info().Msgf("Successfully unmounted %s", m.MountPath)
	return nil
}
