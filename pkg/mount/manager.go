package mount

import (
	"context"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs"
	"github.com/sirrobot01/decypharr/pkg/mount/external"
	"github.com/sirrobot01/decypharr/pkg/mount/rclone"
)

type Manager interface {
	Start(ctx context.Context) error
	Stop() error
	IsReady() bool
	GetStats() (map[string]interface{}, error)
	Type() string
}

func NewManager() Manager {
	cfg := config.Get()
	if cfg.Dfs.Enabled {
		// Use FUSE filesystem manager
		return dfs.NewManager()
	}
	if cfg.Rclone.Enabled {
		// Use rclone manager
		return rclone.NewManager()
	}

	// Fallback to external rclone
	return external.NewManager()
}
