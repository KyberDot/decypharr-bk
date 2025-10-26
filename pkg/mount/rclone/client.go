package rclone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

var httpClient = http.DefaultClient

func makeRequest(req RCRequest, close bool) (*http.Response, error) {
	reqBody, err := json.Marshal(req.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("http://localhost:%s/%s", RCPort, req.Command)
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// Read the response body to get more details
		defer func(Body io.ReadCloser) {
			_ = Body.Close()
		}(resp.Body)
		var errorResp RCResponse
		if err := json.NewDecoder(resp.Body).Decode(&errorResp); err != nil {
			return nil, fmt.Errorf("request failed with status %s, but could not decode error response: %w", resp.Status, err)
		}
		if errorResp.Error != "" {
			return nil, fmt.Errorf("%s", errorResp.Error)
		} else {
			return nil, fmt.Errorf("request failed with status %s and no error message", resp.Status)
		}
	}

	if close {
		defer func() {
			_ = resp.Body.Close()
		}()
	}

	return resp, nil
}

// mountWithRetry attempts to mount with retry logic
func (m *Mount) mountWithRetry(maxRetries int) error {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry
			wait := time.Duration(attempt*2) * time.Second
			m.logger.Debug().
				Int("attempt", attempt).
				Msg("Retrying mount operation")
			time.Sleep(wait)
		}

		if err := m.performMount(); err != nil {
			m.logger.Error().
				Err(err).
				Int("attempt", attempt+1).
				Msg("Mount attempt failed")
			continue
		}

		return nil // Success
	}
	return fmt.Errorf("mount failed for %s", m.Provider)
}

// performMount performs a single mount attempt
func (m *Mount) performMount() error {
	cfg := config.Get()

	// Create mount directory
	if err := os.MkdirAll(m.MountPath, 0755); err != nil {
		return fmt.Errorf("failed to create mount directory %s: %w", m.MountPath, err)
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
		"fs":         fmt.Sprintf("%s:", m.Provider),
		"mountPoint": m.MountPath,
	}
	mountOpt := map[string]interface{}{
		"AllowNonEmpty": true,
		"AllowOther":    true,
		"DebugFUSE":     false,
		"DeviceName":    fmt.Sprintf("decypharr-%s", m.Provider),
		"VolumeName":    fmt.Sprintf("decypharr-%s", m.Provider),
	}

	if cfg.Rclone.AsyncRead != nil {
		mountOpt["AsyncRead"] = *cfg.Rclone.AsyncRead
	}

	if cfg.Rclone.UseMmap {
		mountOpt["UseMmap"] = cfg.Rclone.UseMmap
	}

	if cfg.Rclone.Transfers != 0 {
		mountOpt["Transfers"] = cfg.Rclone.Transfers
	}

	configOpts := make(map[string]interface{})

	if cfg.Rclone.BufferSize != "" {
		configOpts["BufferSize"] = cfg.Rclone.BufferSize
	}

	if len(configOpts) > 0 {
		// Only add _config if there are options to set
		mountArgs["_config"] = configOpts
	}
	vfsOpt := map[string]interface{}{
		"CacheMode":    cfg.Rclone.VfsCacheMode,
		"DirCacheTime": cfg.Rclone.DirCacheTime,
	}
	vfsOpt["PollInterval"] = 0 // Poll interval not supported for webdav, set to 0

	// Add VFS options if caching is enabled
	if cfg.Rclone.VfsCacheMode != "off" {

		if cfg.Rclone.VfsCacheMaxAge != "" {
			vfsOpt["CacheMaxAge"] = cfg.Rclone.VfsCacheMaxAge
		}
		if cfg.Rclone.VfsDiskSpaceTotal != "" {
			vfsOpt["DiskSpaceTotalSize"] = cfg.Rclone.VfsDiskSpaceTotal
		}
		if cfg.Rclone.VfsReadChunkSizeLimit != "" {
			vfsOpt["ChunkSizeLimit"] = cfg.Rclone.VfsReadChunkSizeLimit
		}

		if cfg.Rclone.VfsCacheMaxSize != "" {
			vfsOpt["CacheMaxSize"] = cfg.Rclone.VfsCacheMaxSize
		}
		if cfg.Rclone.VfsCachePollInterval != "" {
			vfsOpt["CachePollInterval"] = cfg.Rclone.VfsCachePollInterval
		}
		if cfg.Rclone.VfsReadChunkSize != "" {
			vfsOpt["ChunkSize"] = cfg.Rclone.VfsReadChunkSize
		}
		if cfg.Rclone.VfsReadAhead != "" {
			vfsOpt["ReadAhead"] = cfg.Rclone.VfsReadAhead
		}

		if cfg.Rclone.VfsCacheMinFreeSpace != "" {
			vfsOpt["CacheMinFreeSpace"] = cfg.Rclone.VfsCacheMinFreeSpace
		}

		if cfg.Rclone.VfsFastFingerprint {
			vfsOpt["FastFingerprint"] = cfg.Rclone.VfsFastFingerprint
		}

		if cfg.Rclone.VfsReadChunkStreams != 0 {
			vfsOpt["ChunkStreams"] = cfg.Rclone.VfsReadChunkStreams
		}

		if cfg.Rclone.NoChecksum {
			vfsOpt["NoChecksum"] = cfg.Rclone.NoChecksum
		}
		if cfg.Rclone.NoModTime {
			vfsOpt["NoModTime"] = cfg.Rclone.NoModTime
		}
	}

	// Add mount options based on configuration
	if cfg.Rclone.UID != 0 {
		vfsOpt["UID"] = cfg.Rclone.UID
	}
	if cfg.Rclone.GID != 0 {
		vfsOpt["GID"] = cfg.Rclone.GID
	}

	if cfg.Rclone.Umask != "" {
		umask, err := strconv.ParseInt(cfg.Rclone.Umask, 8, 32)
		if err == nil {
			vfsOpt["Umask"] = uint32(umask)
		}
	}

	if cfg.Rclone.AttrTimeout != "" {
		if attrTimeout, err := time.ParseDuration(cfg.Rclone.AttrTimeout); err == nil {
			mountOpt["AttrTimeout"] = attrTimeout.String()
		}
	}

	mountArgs["vfsOpt"] = vfsOpt
	mountArgs["mountOpt"] = mountOpt
	// Make the mount request
	req := RCRequest{
		Command: "mount/mount",
		Args:    mountArgs,
	}

	_, err := makeRequest(req, true)
	if err != nil {
		// Clean up mount point on failure
		_ = m.forceUnmount()
		return fmt.Errorf("failed to create mount for %s: %w", m.Provider, err)
	}

	// Store mount info
	mntInfo := &MountInfo{
		Provider:   m.Provider,
		LocalPath:  m.MountPath,
		WebDAVURL:  m.WebDAVURL,
		Mounted:    true,
		MountedAt:  time.Now().Format(time.RFC3339),
		ConfigName: m.Provider,
	}

	m.info.Store(mntInfo)

	return nil
}

// unmount is the internal unmount function
func (m *Mount) unmount() error {
	mountInfo := m.getMountInfo()

	if mountInfo == nil || !mountInfo.Mounted {
		m.logger.Info().Msg("Mount not found or already unmounted")
		return nil
	}

	m.logger.Info().Msg("Unmounting")

	// Try RC unmount first
	req := RCRequest{
		Command: "mount/unmount",
		Args: map[string]interface{}{
			"mountPoint": mountInfo.LocalPath,
		},
	}

	var rcErr error
	_, rcErr = makeRequest(req, true)

	// If RC unmount fails or server is not ready, try force unmount
	if rcErr != nil {
		m.logger.Warn().Err(rcErr).Msg("RC unmount failed, trying force unmount")
		if err := m.forceUnmount(); err != nil {
			m.logger.Error().Err(err).Msg("Force unmount failed")
			// Don't return error here, update the state anyway
		}
	}

	// Update mount info
	mountInfo.Mounted = false
	mountInfo.Error = ""
	if rcErr != nil {
		mountInfo.Error = rcErr.Error()
	}
	m.logger.Info().Msg("Unmount completed")
	return nil
}

// createConfig creates an rclone config entry for the provider
func (m *Mount) createConfig() error {
	req := RCRequest{
		Command: "config/create",
		Args: map[string]interface{}{
			"name": m.Provider,
			"type": "webdav",
			"parameters": map[string]interface{}{
				"url":             m.WebDAVURL,
				"vendor":          "other",
				"pacer_min_sleep": "0",
			},
		},
	}

	_, err := makeRequest(req, true)
	if err != nil {
		return fmt.Errorf("failed to create config %s: %w", m.Provider, err)
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

	for _, method := range methods {
		if err := m.tryUnmountCommand(method...); err == nil {
			m.logger.Info().
				Strs("command", method).
				Msg("Successfully unmounted using system command")
			return nil
		}
	}

	return fmt.Errorf("all force unmount attempts failed for %s", m.MountPath)
}

// tryUnmountCommand tries to run an unmount command
func (m *Mount) tryUnmountCommand(args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.Command(args[0], args[1:]...)
	return cmd.Run()
}
