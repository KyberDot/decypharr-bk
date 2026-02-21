package rclone

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/retry"
	"github.com/sirrobot01/decypharr/internal/utils"
	"golang.org/x/net/context"
)

// mountWithRetry attempts to mount with retry logic using avast/retry-go
func (m *Mount) mountWithRetry(maxRetries int) error {
	return retry.Do(
		func() error {
			return m.performMount()
		},
		retry.Attempts(uint(maxRetries)+1),
		retry.Delay(config.DefaultRetryDelay),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
		retry.RetryIf(func(err error) bool {
			return true // Always retry on error
		}),
	)
}

// performMount performs a single mount attempt
func (m *Mount) performMount() error {
	cfg := config.Get()

	// Create mount directory if not on windows
	if runtime.GOOS != "windows" {
		_ = os.MkdirAll(m.MountPath, 0755)
	}

	// Check if already mounted
	mountInfo := m.getMountInfo()

	if mountInfo != nil && mountInfo.Mounted {
		m.logger.Info().Msg("Already mounted")
		return nil
	}

	// Clean up any stale mount first
	if mountInfo != nil && !mountInfo.Mounted {
		err := m.forceUnmount()
		if err != nil {
			return err
		}
	}

	// Create rclone config for this provider
	if err := m.createConfig(); err != nil {
		return fmt.Errorf("failed to create rclone config: %w", err)
	}

	// Prepare mount arguments
	mountArgs := map[string]interface{}{
		"fs":         FSName,
		"mountPoint": m.MountPath,
	}
	mountOpt := map[string]interface{}{
		"AllowNonEmpty": true,
		"AllowOther":    true,
		"DebugFUSE":     false,
		"DeviceName":    "decypharr",
		"VolumeName":    "decypharr",
	}

	if cfg.Mount.Rclone.AsyncRead != nil {
		mountOpt["AsyncRead"] = *cfg.Mount.Rclone.AsyncRead
	}

	if cfg.Mount.Rclone.UseMmap {
		mountOpt["UseMmap"] = cfg.Mount.Rclone.UseMmap
	}

	if cfg.Mount.Rclone.Transfers != 0 {
		mountOpt["Transfers"] = cfg.Mount.Rclone.Transfers
	}

	configOpts := make(map[string]interface{})

	if cfg.Mount.Rclone.BufferSize != "" {
		configOpts["BufferSize"] = cfg.Mount.Rclone.BufferSize
	}

	if len(configOpts) > 0 {
		// Only add _config if there are options to set
		mountArgs["_config"] = configOpts
	}
	vfsOpt := map[string]interface{}{
		"CacheMode":    cfg.Mount.Rclone.VfsCacheMode,
		"DirCacheTime": cfg.Mount.Rclone.DirCacheTime,
	}
	vfsOpt["PollInterval"] = 0 // Poll interval not supported for webdav, set to 0

	// AddOrUpdate VFS options if caching is enabled
	if cfg.Mount.Rclone.VfsCacheMode != "off" {

		if cfg.Mount.Rclone.VfsCacheMaxAge != "" {
			vfsOpt["CacheMaxAge"] = cfg.Mount.Rclone.VfsCacheMaxAge
		}
		if cfg.Mount.Rclone.VfsDiskSpaceTotal != "" {
			vfsOpt["DiskSpaceTotalSize"] = cfg.Mount.Rclone.VfsDiskSpaceTotal
		}
		if cfg.Mount.Rclone.VfsReadChunkSizeLimit != "" {
			vfsOpt["ChunkSizeLimit"] = cfg.Mount.Rclone.VfsReadChunkSizeLimit
		}

		if cfg.Mount.Rclone.VfsCacheMaxSize != "" {
			vfsOpt["CacheMaxSize"] = cfg.Mount.Rclone.VfsCacheMaxSize
		}
		if cfg.Mount.Rclone.VfsCachePollInterval != "" {
			vfsOpt["CachePollInterval"] = cfg.Mount.Rclone.VfsCachePollInterval
		}
		if cfg.Mount.Rclone.VfsReadChunkSize != "" {
			vfsOpt["ChunkSize"] = cfg.Mount.Rclone.VfsReadChunkSize
		}
		if cfg.Mount.Rclone.VfsReadAhead != "" {
			vfsOpt["ReadAhead"] = cfg.Mount.Rclone.VfsReadAhead
		}

		if cfg.Mount.Rclone.VfsCacheMinFreeSpace != "" {
			vfsOpt["CacheMinFreeSpace"] = cfg.Mount.Rclone.VfsCacheMinFreeSpace
		}

		if cfg.Mount.Rclone.VfsFastFingerprint {
			vfsOpt["FastFingerprint"] = cfg.Mount.Rclone.VfsFastFingerprint
		}

		if cfg.Mount.Rclone.VfsReadChunkStreams != 0 {
			vfsOpt["ChunkStreams"] = cfg.Mount.Rclone.VfsReadChunkStreams
		}

		if cfg.Mount.Rclone.NoChecksum {
			vfsOpt["NoChecksum"] = cfg.Mount.Rclone.NoChecksum
		}
		if cfg.Mount.Rclone.NoModTime {
			vfsOpt["NoModTime"] = cfg.Mount.Rclone.NoModTime
		}
	}

	// AddOrUpdate mount options based on configuration
	if cfg.Mount.Rclone.UID != 0 {
		vfsOpt["UID"] = cfg.Mount.Rclone.UID
	}
	if cfg.Mount.Rclone.GID != 0 {
		vfsOpt["GID"] = cfg.Mount.Rclone.GID
	}

	if cfg.Mount.Rclone.Umask != "" {
		umask, err := strconv.ParseInt(cfg.Mount.Rclone.Umask, 8, 32)
		if err == nil {
			vfsOpt["Umask"] = uint32(umask)
		}
	}

	if cfg.Mount.Rclone.AttrTimeout != "" {
		if attrTimeout, err := utils.ParseDuration(cfg.Mount.Rclone.AttrTimeout); err == nil {
			mountOpt["AttrTimeout"] = attrTimeout.String()
		}
	}

	mountArgs["vfsOpt"] = vfsOpt
	mountArgs["mountOpt"] = mountOpt

	if err := m.client.Mount(context.Background(), mountArgs); err != nil {
		_ = m.forceUnmount()
		return fmt.Errorf("failed to mount %s via RC: %w", m.MountPath, err)
	}

	// Store mount info
	mntInfo := &MountInfo{
		LocalPath:  m.MountPath,
		WebDAVURL:  m.WebDAVURL,
		Mounted:    true,
		MountedAt:  time.Now().Format(time.RFC3339),
		ConfigName: ConfigName,
	}

	m.info.Store(mntInfo)

	return nil
}

// unmount is the internal unmount function
func (m *Mount) unmount() {
	mountInfo := m.getMountInfo()

	if mountInfo == nil || !mountInfo.Mounted {
		m.logger.Info().Msg("Mount not found or already unmounted")
		return
	}

	m.logger.Info().Msg("Unmounting")

	// Try RC unmount first

	err := m.client.Unmount(context.Background(), mountInfo.LocalPath)

	// If RC unmount fails or server is not ready, try force unmount
	if err != nil {
		m.logger.Warn().Err(err).Msg("RC unmount failed, trying force unmount")
		if err := m.forceUnmount(); err != nil {
			m.logger.Error().Err(err).Msg("Force unmount failed")
			// Don't return error here, update the state anyway
		}
	}

	// Update mount info
	mountInfo.Mounted = false
	mountInfo.Error = ""
	if err != nil {
		mountInfo.Error = err.Error()
	}
	m.logger.Info().Msg("Unmount completed")
	return
}

// createConfig creates an rclone config entry for the provider
func (m *Mount) createConfig() error {
	args := map[string]interface{}{
		"name": ConfigName,
		"type": "webdav",
		"parameters": map[string]interface{}{
			"url":             m.WebDAVURL,
			"vendor":          "other",
			"pacer_min_sleep": "0",
		},
	}
	if err := m.client.CreateConfig(context.Background(), args); err != nil {
		return fmt.Errorf("failed to create rclone config for %s: %w", m.MountPath, err)
	}
	return nil
}

// forceUnmount attempts to force unmount a path using system commands
func (m *Mount) forceUnmount() error {
	methods := [][]string{
		{"umount", m.MountPath},
		{"umount", "-l", m.MountPath}, // lazy unmount
		{"fusermount", "-uz", m.MountPath},
		{"fusermount3", "-uz", m.MountPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, method := range methods {
		if err := m.tryUnmountCommand(ctx, method...); err == nil {
			m.logger.Info().
				Strs("command", method).
				Msg("Successfully unmounted using system command")
			return nil
		}
	}

	return fmt.Errorf("all force unmount attempts failed for %s", m.MountPath)
}

// tryUnmountCommand tries to run an unmount command
func (m *Mount) tryUnmountCommand(ctx context.Context, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	return cmd.Run()
}
