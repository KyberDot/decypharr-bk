package manager

import (
	"context"
	"strings"
)

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]interface{}
	IsReady() bool
	Type() string
	PreCache(filePaths []string) error
	Refresh(dirs []string) error
}

func (m *Manager) RefreshEntries(refreshMount bool) {
	// Refresh entries
	m.entry.Refresh()

	// Refresh mount if needed
	if refreshMount {
		go func() {
			_ = m.RefreshMount()
		}()
	}
}

func (m *Manager) RefreshMount() error {
	dirs := strings.FieldsFunc(m.config.RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}

	// Call event handler if set
	if m.mountManager != nil && m.mountManager.Refresh != nil {
		return m.mountManager.Refresh(dirs)
	}
	return nil
}

// PreCache calls the mount's PreCache method via event handler
func (m *Manager) PreCache(filePaths []string) error {
	if m.mountManager != nil && m.mountManager.PreCache != nil {
		return m.mountManager.PreCache(filePaths)
	}
	return nil
}

type stubMountManager struct{}

func (s *stubMountManager) PreCache(filePaths []string) error {
	return nil
}

func (s *stubMountManager) Refresh(dirs []string) error {
	return nil
}

func NewStubMountManager() MountManager {
	return &stubMountManager{}
}

func (s *stubMountManager) Start(ctx context.Context) error {
	return nil
}
func (s *stubMountManager) Stop() error {
	return nil
}
func (s *stubMountManager) Stats() map[string]interface{} {
	return map[string]interface{}{
		"message": "no mount configured",
	}
}
func (s *stubMountManager) IsReady() bool {
	return false
}
func (s *stubMountManager) Type() string {
	return "none"
}
