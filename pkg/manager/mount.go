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
}

type Mount interface {
	Start(ctx context.Context) error
	Stop() error
	Refresh(dirs []string) error
	Type() string
}

type EventHandler struct {
	OnRefresh func(dirs []string) error
}

func (m *Manager) AddEventHandlers(name string, e *EventHandler) {
	m.events.Store(name, e)
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

	// We only want to refresh the default mount, which is the first one
	handler, ok := m.events.Load(m.firstDebrid)
	if ok && handler != nil {
		return handler.OnRefresh(dirs)
	} else {
		// Attempt to refresh all mounts if no specific handler found
		m.events.Range(func(key string, h *EventHandler) bool {
			_ = h.OnRefresh(dirs)
			return true
		})
	}

	return nil
}

func NewEventHandlers(mounter Mount) *EventHandler {
	return &EventHandler{
		OnRefresh: mounter.Refresh,
	}
}

type stubMountManager struct{}

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
