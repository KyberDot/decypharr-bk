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
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
)

// Mount represents a mount using the rclone RC client
type Mount struct {
	Provider     string
	MountPath    string
	WebDAVURL    string
	debridConfig config.Debrid
	logger       zerolog.Logger
	info         atomic.Value
}

// NewMount creates a new RC-based mount
func NewMount(debridCache *store.Cache) (*Mount, error) {
	cfg := config.Get()
	debridConfig := debridCache.GetConfig()
	bindAddress := cfg.BindAddress
	if bindAddress == "" {
		bindAddress = "localhost"
	}
	_logger := logger.New("rclone")

	baseUrl := fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	webdavUrl, err := url.JoinPath(baseUrl, cfg.URLBase, "webdav", debridConfig.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to construct WebDAV URL for %s: %w", debridConfig.Name, err)
	}

	mountPath := debridConfig.RcloneMountPath
	if debridConfig.RcloneMountPath == "" {
		mountPath = filepath.Join(cfg.Rclone.MountPath, debridConfig.Name)
	}
	if !strings.HasSuffix(webdavUrl, "/") {
		webdavUrl += "/"
	}

	return &Mount{
		Provider:     debridConfig.Name,
		MountPath:    mountPath,
		WebDAVURL:    webdavUrl,
		debridConfig: debridConfig,

		logger: _logger,
	}, nil
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
	if !pingServer() {
		return fmt.Errorf("rclone RCD can't start")
	}

	m.logger.Info().Msg("Creating mount via RC")

	if err := m.mountWithRetry(3); err != nil {
		m.logger.Error().Msg("Mount operation failed")
		return fmt.Errorf("mount failed for %s", m.Provider)
	}

	go m.MonitorMounts(ctx)

	m.logger.Info().Msgf("Successfully mounted")
	return nil
}

func (m *Mount) Stop(ctx context.Context) error {
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
	args := map[string]interface{}{
		"fs": fmt.Sprintf("%s:", m.Provider),
	}
	for i, dir := range dirs {
		if dir != "" {
			if i == 0 {
				args["dir"] = dir
			} else {
				args[fmt.Sprintf("dir%d", i+1)] = dir
			}
		}
	}
	req := RCRequest{
		Command: "vfs/forget",
		Args:    args,
	}

	_, err := makeRequest(req, true)
	if err != nil {
		m.logger.Error().Err(err).
			Msg("Failed to refresh directory")
		return fmt.Errorf("failed to refresh directory %s for provider %s: %w", dirs, m.Provider, err)
	}

	req = RCRequest{
		Command: "vfs/refresh",
		Args:    args,
	}

	_, err = makeRequest(req, true)
	if err != nil {
		m.logger.Error().Err(err).
			Msg("Failed to refresh directory")
		return fmt.Errorf("failed to refresh directory %s for provider %s: %w", dirs, m.Provider, err)
	}
	return nil
}
