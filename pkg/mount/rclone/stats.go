package rclone

import (
	"context"

	"github.com/sirrobot01/decypharr/internal/rclone"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Stats represents rclone statistics
type Stats struct {
	Type      string                   `json:"type"`
	Enabled   bool                     `json:"enabled"`
	Ready     bool                     `json:"ready"`
	Core      rclone.CoreStatsResponse `json:"core"`
	Memory    rclone.MemoryStats       `json:"memory"`
	Mount     *MountInfo               `json:"mounts"`
	Bandwidth rclone.BandwidthStats    `json:"bandwidth"`
	Version   rclone.VersionResponse   `json:"version"`
}

// Stats returns unified mount statistics
func (m *Manager) Stats() *manager.MountStats {
	ms := &manager.MountStats{
		Enabled: true,
		Ready:   m.IsReady(),
		Type:    m.Type(),
	}

	ctx := context.Background()

	detail := &manager.RcloneDetail{}

	coreStats, err := m.client.GetCoreStats(ctx)
	if err == nil {
		detail.Core = *coreStats
	}

	memStats, err := m.client.GetMemoryUsage(ctx)
	if err == nil {
		detail.Memory = *memStats
	}

	bwStats, err := m.client.GetBandwidthStats(ctx)
	if err == nil && bwStats != nil {
		detail.Bandwidth = *bwStats
	}

	if m.mount != nil {
		info := m.mount.getMountInfo()
		if info != nil {
			detail.Mount = &manager.RcloneMountInfo{
				LocalPath:  info.LocalPath,
				WebDAVURL:  info.WebDAVURL,
				Mounted:    info.Mounted,
				MountedAt:  info.MountedAt,
				ConfigName: info.ConfigName,
				Error:      info.Error,
			}
		}
	}

	versionResp, err := m.client.GetVersion(ctx)
	if err == nil {
		detail.Version = *versionResp
	}

	ms.Rclone = detail
	return ms
}
