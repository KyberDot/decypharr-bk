package rclone

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
)

const (
	RCPort = "5572"
)

// Manager handles the rclone RC server and provides mount operations
type Manager struct {
	cmd           *exec.Cmd
	configDir     string
	logger        zerolog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	httpClient    *http.Client
	serverReady   chan struct{}
	serverStarted bool
	mu            sync.RWMutex
}

type MountInfo struct {
	Provider   string `json:"provider"`
	LocalPath  string `json:"local_path"`
	WebDAVURL  string `json:"webdav_url"`
	Mounted    bool   `json:"mounted"`
	MountedAt  string `json:"mounted_at,omitempty"`
	ConfigName string `json:"config_name"`
	Error      string `json:"error,omitempty"`
}

type RCRequest struct {
	Command string                 `json:"command"`
	Args    map[string]interface{} `json:"args,omitempty"`
}

type RCResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// NewManager creates a new rclone RC manager
func NewManager() *Manager {
	cfg := config.Get()
	configDir := filepath.Join(cfg.Path, "rclone")

	// Ensure config directory exists
	if err := os.MkdirAll(configDir, 0755); err != nil {
		_logger := logger.New("rclone")
		_logger.Error().Err(err).Msg("Failed to create rclone config directory")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		configDir:   configDir,
		logger:      logger.New("rclone"),
		ctx:         ctx,
		cancel:      cancel,
		httpClient:  &http.Client{Timeout: 60 * time.Second},
		serverReady: make(chan struct{}),
	}
}

// Start starts the rclone RC server
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.serverStarted {
		return nil
	}

	cfg := config.Get()
	if !cfg.Rclone.Enabled {
		m.logger.Info().Msg("Rclone is disabled, skipping RC server startup")
		return nil
	}

	logFile := filepath.Join(logger.GetLogPath(), "rclone.log")

	// Delete old log file if it exists
	if _, err := os.Stat(logFile); err == nil {
		if err := os.Remove(logFile); err != nil {
			return fmt.Errorf("failed to remove old rclone log file: %w", err)
		}
	}

	args := []string{
		"rcd",
		"--rc-addr", ":" + RCPort,
		"--rc-no-auth", // We'll handle auth at the application level
		"--config", filepath.Join(m.configDir, "rclone.conf"),
		"--log-file", logFile,
	}

	logLevel := cfg.Rclone.LogLevel
	if logLevel != "" {
		if !slices.Contains([]string{"DEBUG", "INFO", "NOTICE", "ERROR"}, logLevel) {
			logLevel = "INFO"
		}
		args = append(args, "--log-level", logLevel)
	}

	if cfg.Rclone.CacheDir != "" {
		if err := os.MkdirAll(cfg.Rclone.CacheDir, 0755); err == nil {
			args = append(args, "--cache-dir", cfg.Rclone.CacheDir)
		}
	}
	m.cmd = exec.CommandContext(ctx, "rclone", args...)

	// Capture output for debugging
	var stdout, stderr bytes.Buffer
	m.cmd.Stdout = &stdout
	m.cmd.Stderr = &stderr

	if err := m.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start rclone: %v stdout: %s stderr: %s", err, stdout.String(), stderr.String())
	}
	m.serverStarted = true

	// Wait for server to be ready in a goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error().Interface("panic", r).Msg("Panic in rclone RC server monitor")
			}
		}()

		m.waitForServer()
		close(m.serverReady)

		// Wait for command to finish and log output
		err := m.cmd.Wait()
		switch {
		case err == nil:
			m.logger.Info().Msg("Rclone RC server exited normally")

		case errors.Is(err, context.Canceled):
			m.logger.Info().Msg("Rclone RC server terminated: context canceled")

		case WasHardTerminated(err): // SIGKILL on *nix; non-zero exit on Windows
			m.logger.Info().Msg("Rclone RC server hard-terminated")

		default:
			if code, ok := ExitCode(err); ok {
				m.logger.Debug().Int("exit_code", code).Err(err).
					Str("stderr", stderr.String()).
					Str("stdout", stdout.String()).
					Msg("Rclone RC server error")
			} else {
				m.logger.Debug().Err(err).Str("stderr", stderr.String()).
					Str("stdout", stdout.String()).Msg("Rclone RC server error (no exit code)")
			}
		}
	}()
	return nil
}

// Stop stops the rclone RC server and unmounts all mounts
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.serverStarted {
		return nil
	}

	m.logger.Info().Msg("Stopping rclone RC server")
	// Cancel context and stop process
	m.cancel()

	if m.cmd != nil && m.cmd.Process != nil {
		// Try graceful shutdown first
		if err := m.cmd.Process.Signal(os.Interrupt); err != nil {
			if killErr := m.cmd.Process.Kill(); killErr != nil {
				return killErr
			}
		}

		// Wait for process to exit with timeout
		done := make(chan error, 1)
		go func() {
			done <- m.cmd.Wait()
		}()

		select {
		case <-time.After(2 * time.Second):
			if err := m.cmd.Process.Kill(); err != nil {
				// Check if the process already finished
				if !strings.Contains(err.Error(), "process already finished") {
					return err
				}
			}

			// Still wait for the Wait() to complete to clean up the process
			select {
			case <-done:
				m.logger.Info().Msg("Rclone process cleanup completed")
			case <-time.After(5 * time.Second):
				m.logger.Error().Msg("Process cleanup timeout")
			}
		}
	}

	m.serverStarted = false
	m.logger.Info().Msg("Rclone RC server stopped")
	return nil
}

// waitForServer waits for the RC server to become available
func (m *Manager) waitForServer() {
	maxAttempts := 30
	for i := 0; i < maxAttempts; i++ {
		if m.ctx.Err() != nil {
			return
		}

		if pingServer() {
			m.logger.Info().Msg("Rclone RC server is ready")
			return
		}

		time.Sleep(time.Second)
	}

	m.logger.Error().Msg("Rclone RC server not responding - mount operations will be disabled")
}

// pingServer checks if the RC server is responding
func pingServer() bool {
	req := RCRequest{Command: "core/version"}
	_, err := makeRequest(req, true)
	return err == nil
}

// IsReady returns true if the RC server is ready
func (m *Manager) IsReady() bool {
	select {
	case <-m.serverReady:
		return true
	default:
		return false
	}
}

// waitForReady waits for the RC server to be ready
func (m *Manager) waitForReady(timeout time.Duration) error {
	select {
	case <-m.serverReady:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for rclone RC server to be ready")
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *Manager) GetLogger() zerolog.Logger {
	return m.logger
}

func (m *Manager) Type() string {
	return "rclone"
}
