package rclone

import (
	"context"
	"fmt"
	"net/url"
	"os"
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

// preCacheFile pre-caches a single file by reading header chunks
func (m *Mount) preCacheFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File has probably been moved by arr, return silently
			return nil
		}
		return fmt.Errorf("failed to open file: %s: %v", filePath, err)
	}
	defer file.Close()

	// Pre-cache the file header (first 256KB) using 16KB chunks
	if err := readSmallChunks(file, 0, 256*1024, 16*1024); err != nil {
		return err
	}
	// Also read at 1MB offset (for some container formats)
	if err := readSmallChunks(file, 1024*1024, 64*1024, 16*1024); err != nil {
		return err
	}
	return nil
}

// readSmallChunks reads small chunks from file to populate cache
func readSmallChunks(file *os.File, startPos int64, totalToRead int, chunkSize int) error {
	_, err := file.Seek(startPos, 0)
	if err != nil {
		return err
	}

	buf := make([]byte, chunkSize)
	bytesRemaining := totalToRead

	for bytesRemaining > 0 {
		toRead := chunkSize
		if bytesRemaining < chunkSize {
			toRead = bytesRemaining
		}

		n, err := file.Read(buf[:toRead])
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		bytesRemaining -= n
	}
	return nil
}
