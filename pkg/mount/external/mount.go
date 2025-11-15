package external

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type Mount struct {
	config     config.ExternalRclone
	httpClient *req.Client
	logger     zerolog.Logger
	name       string
}

func NewMount(mountName string, mgr *manager.Manager) (*Mount, error) {
	cfg := config.Get().Mount.ExternalRclone
	// Create HTTP client
	httpClient := req.C().
		SetTimeout(1*time.Minute).
		SetCommonBasicAuth(cfg.RCUsername, cfg.RCPassword).
		SetCommonHeader("Content-Type", "application/x-www-form-urlencoded")

	m := &Mount{
		config:     config.Get().Mount.ExternalRclone,
		httpClient: httpClient,
		logger:     logger.New("external").With().Str("mount", mountName).Logger(),
		name:       mountName,
	}

	mgr.AddEventHandlers(mountName, manager.NewEventHandlers(m))
	return m, nil
}

func (m *Mount) Start(ctx context.Context) error {
	return nil
}

func (m *Mount) Stop() error {
	return nil
}

func (m *Mount) Refresh(dirs []string) error {
	return m.refresh(dirs)
}

func (m *Mount) refresh(dirs []string) error {
	cfg := m.config

	if cfg.RCUrl == "" {
		return nil
	}
	// Create form data
	data := m.buildRcloneRequestData(dirs)

	if err := m.sendRcloneRequest("vfs/forget", data); err != nil {
		m.logger.Error().Err(err).Msg("Failed to send rclone vfs/forget request")
	}

	if err := m.sendRcloneRequest("vfs/refresh", data); err != nil {
		m.logger.Error().Err(err).Msg("Failed to send rclone vfs/refresh request")
	}

	return nil
}

func (m *Mount) buildRcloneRequestData(dirs []string) string {
	var data strings.Builder
	for index, dir := range dirs {
		if dir != "" {
			if index == 0 {
				data.WriteString("dir=" + dir)
			} else {
				data.WriteString("&dir" + fmt.Sprint(index+1) + "=" + dir)
			}
		}
	}
	return data.String()
}

func (m *Mount) sendRcloneRequest(endpoint, data string) error {
	resp, err := m.httpClient.R().
		SetBody(data).
		Post("/" + endpoint)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("failed to perform %s: %s - %s", endpoint, resp.Status, string(body))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (m *Mount) Type() string {
	return "rcloneExternal"
}
