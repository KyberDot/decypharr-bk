package rclone

import (
	"context"
	"fmt"
	"time"
)

// checkMountHealth checks if a specific mount is healthy
func (m *Mount) checkMountHealth() bool {
	// Try to list the root directory of the mount
	req := RCRequest{
		Command: "operations/list",
		Args: map[string]interface{}{
			"fs":     fmt.Sprintf("%s:", m.Provider),
			"remote": "",
		},
	}

	_, err := makeRequest(req, true)
	return err == nil
}

// RecoverMount attempts to recover a failed mount
func (m *Mount) RecoverMount() error {
	mountInfo := m.getMountInfo()

	if mountInfo == nil {
		return fmt.Errorf("mount for provider %s does not exist", m.Provider)
	}

	m.logger.Warn().Msg("Attempting to recover mount")

	// First try to unmount cleanly
	if err := m.unmount(); err != nil {
		m.logger.Error().Err(err).Msg("Failed to unmount during recovery")
	}

	// Wait a moment
	time.Sleep(1 * time.Second)

	// Try to remount
	if err := m.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to recover mount for %s: %w", m.Provider, err)
	}

	m.logger.Info().Msg("Successfully recovered mount")
	return nil
}

// MonitorMounts continuously monitors mount health and attempts recovery
func (m *Mount) MonitorMounts(ctx context.Context) {

	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Debug().Msg("Mount monitoring stopped")
			return
		case <-ticker.C:
			m.performMountHealthCheck()
		}
	}
}

// performMountHealthCheck checks and attempts to recover unhealthy mounts
func (m *Mount) performMountHealthCheck() {
	if !m.checkMountHealth() {
		m.logger.Warn().Msg("Mount health check failed, attempting recovery")

		// Mark mount as unhealthy
		mountInfo := m.getMountInfo()
		if mountInfo == nil {
			return
		}
		mountInfo.Error = "Health check failed"
		mountInfo.Mounted = false
		m.info.Store(mountInfo)

		// Attempt recovery
		go func() {
			if err := m.RecoverMount(); err != nil {
				m.logger.Error().Msg("Failed to recover mount")
			}
		}()
	}
}
