package mount

import (
	"context"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs"
	"github.com/sirrobot01/decypharr/pkg/mount/external"
	"github.com/sirrobot01/decypharr/pkg/mount/rclone"
)

type Mounter interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Refresh(dirs []string) error
	Type() string
}

func NewMounter(debridCache *store.Cache) (Mounter, error) {
	cfg := config.Get()
	if cfg.Dfs.Enabled {
		// Use FUSE filesystem manager
		return dfs.NewMount(debridCache)
	}
	if cfg.Rclone.Enabled {
		// Use rclone manager
		return rclone.NewMount(debridCache)
	}

	// Fallback to external rclone
	return external.NewMount(debridCache)
}

func NewEventHandlers(mounter Mounter) *store.Event {
	return &store.Event{
		OnStart:   mounter.Start,
		OnStop:    mounter.Stop,
		OnRefresh: mounter.Refresh,
	}
}
