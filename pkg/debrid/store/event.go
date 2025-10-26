package store

import (
	"context"
	"strings"
)

type Event struct {
	OnRefresh func(dirs []string) error
	OnStart   func(ctx context.Context) error
	OnStop    func(ctx context.Context) error
}

func (c *Cache) SetEventHandlers(e *Event) {
	c.event = e
}

func (c *Cache) TriggerRefreshEvent(refreshMount bool) {
	c.torrents.refreshListing()

	dirs := strings.FieldsFunc(c.config.RcRefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}

	// If no specific directories provided, refresh root
	if len(dirs) == 0 {
		dirs = []string{"/"}
	}
	if refreshMount {
		if c.event != nil && c.event.OnRefresh != nil {
			err := c.event.OnRefresh(dirs)
			if err != nil {
				c.logger.Error().Err(err).Msg("Failed to refresh mount")
			}
		}
	}
}

func (c *Cache) TriggerStartEvent(ctx context.Context) error {
	if c.event != nil && c.event.OnStart != nil {
		return c.event.OnStart(ctx)
	}
	return nil
}

func (c *Cache) TriggerStopEvent(ctx context.Context) error {
	if c.event != nil && c.event.OnStop != nil {
		return c.event.OnStop(ctx)
	}
	return nil
}
