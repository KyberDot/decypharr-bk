package external

import "context"

type Manager struct{}

// NewManager creates a new external rclone manager
// This does nothing, just a placeholder to satisfy the interface
func NewManager() *Manager {
	return &Manager{}
}

func (m *Manager) Start(ctx context.Context) error {
	return nil
}

func (m *Manager) Stop() error {
	return nil
}

func (m *Manager) IsReady() bool {
	return true
}

func (m *Manager) GetStats() (map[string]interface{}, error) {
	return map[string]interface{}{
		"enabled": true,
		"ready":   true,
		"type":    m.Type(),
	}, nil
}

func (m *Manager) Type() string {
	return "external"
}
