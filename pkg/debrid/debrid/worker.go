package debrid

import (
	"context"
	"github.com/sirrobot01/decypharr/internal/utils"
	"time"
)

func (c *Cache) StartSchedule() error {
	// For now, we just want to refresh the listing and download links
	ctx := context.Background()
	downloadLinkJob, err := utils.ScheduleJob(ctx, c.downloadLinksRefreshInterval, nil, c.refreshDownloadLinks)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to add download link refresh job")
	}
	if t, err := downloadLinkJob.NextRun(); err == nil {
		c.logger.Trace().Msgf("Next download link refresh job: %s", t.Format("2006-01-02 15:04:05"))
	}

	torrentJob, err := utils.ScheduleJob(ctx, c.torrentRefreshInterval, nil, c.refreshTorrents)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to add torrent refresh job")
	}
	if t, err := torrentJob.NextRun(); err == nil {
		c.logger.Trace().Msgf("Next torrent refresh job: %s", t.Format("2006-01-02 15:04:05"))
	}

	// Schedule the reset invalid links job
	// This job will run every 24 hours
	// and reset the invalid links in the cache
	cet, _ := time.LoadLocation("CET")
	resetLinksJob, err := utils.ScheduleJob(ctx, "00:00", cet, c.resetInvalidLinks)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to add reset invalid links job")
	}
	if t, err := resetLinksJob.NextRun(); err == nil {
		c.logger.Trace().Msgf("Next reset invalid download links job at: %s", t.Format("2006-01-02 15:04:05"))
	}

	return nil
}
