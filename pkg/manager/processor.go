package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/httpclient"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// AddNewTorrent creates a torrent from import request and processes it
func (m *Manager) AddNewTorrent(ctx context.Context, importReq *ImportRequest) error {
	var (
		debridTorrent *debridTypes.Torrent
		err           error
	)

	// Check if already exists in storage/It's already download
	//existing, err := m.storage.GetReader(torrent.InfoHash)
	//if err == nil && existing != nil && existing.IsValid() {
	//	m.logger.Info().
	//		Str("name", torrent.Name).
	//		Str("info_hash", torrent.InfoHash).
	//		Msg("Torrent already exists in storage, skipping submitting to debrid")
	//	placement := existing.GetActivePlacement(torrent.InfoHash)
	//	if placement != nil {
	//		client := m.DebridClient(placement.Debrid)
	//		if client != nil {
	//			debridTorrent, err = client.GetTorrent(placement.ID)
	//			if err != nil {
	//				debridTorrent = nil
	//			}
	//		}
	//	}
	//}

	debridTorrent, err = m.SendToDebrid(ctx, importReq)
	if err != nil {
		// Check if too many active downloads
		var httpErr *utils.HTTPError
		if errors.As(err, &httpErr) && httpErr.Code == "too_many_active_downloads" {
			m.logger.Warn().Msgf("Too many active downloads, marking as queued: %s", importReq.Magnet.Name)
			if err := m.queue.ReQueue(importReq); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("failed to submit torrent to debrid: %w", err)
	}

	// Create managed torrent with InfoHash as primary key
	torrent := &storage.Torrent{
		InfoHash:         importReq.Magnet.InfoHash,
		Name:             importReq.Magnet.Name,
		OriginalFilename: importReq.Magnet.Name,
		Size:             importReq.Magnet.Size,
		Bytes:            importReq.Magnet.Size,
		Magnet:           importReq.Magnet.Link,
		Category:         importReq.Arr.Name,
		SavePath:         filepath.Join(importReq.DownloadFolder, importReq.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.TorrentStateDownloading,
		Progress:         0,
		Action:           importReq.Action,
		DownloadUncached: importReq.DownloadUncached,
		CallbackURL:      importReq.CallBackUrl,
		SkipMultiSeason:  importReq.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Placements:       make(map[string]*storage.Placement),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	torrent.ContentPath = torrent.SymlinkPath()

	// Add to queue
	if err := m.queue.Add(torrent); err != nil {
		return fmt.Errorf("failed to add torrent to queue: %w", err)
	}

	// Process in background
	go func() {
		err := m.processNewTorrent(ctx, torrent, debridTorrent)
		if err != nil {
			m.logger.Error().Err(err).Str("name", torrent.Name).Msg("Error processing new torrent")
			torrent.MarkAsError(err)
			_ = m.queue.Update(torrent)
			return
		}
	}()

	return nil
}

func (m *Manager) processQueuedTorrents(ctx context.Context) {
	queuedTorrents := m.queue.ListFilter("", storage.TorrentStateDownloading, nil, "", true)
	if len(queuedTorrents) == 0 {
		return
	}
	for _, torrent := range queuedTorrents {
		// Process only active downloading torrents
		if torrent.ActiveDebrid != "" && torrent.Status == debridTypes.TorrentStatusDownloading {
			go m.processQueuedTorrent(ctx, torrent)
		}
	}
}

func (m *Manager) processQueuedTorrent(ctx context.Context, torrent *storage.Torrent) {
	placement := torrent.GetActivePlacement()
	if placement == nil {
		m.logger.Error().Str("name", torrent.Name).Msg("No active placement found for queued torrent")
		torrent.MarkAsError(fmt.Errorf("no active placement found"))
		_ = m.queue.Update(torrent)
		return
	}

	client := m.DebridClient(torrent.ActiveDebrid)
	if client == nil {
		m.logger.Error().Str("debrid", torrent.ActiveDebrid).Msg("Debrid client not found")
		torrent.MarkAsError(fmt.Errorf("debrid client not found: %s", torrent.ActiveDebrid))
		_ = m.queue.Update(torrent)
		return
	}

	magnet, err := utils.GetMagnetInfo(torrent.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(torrent.InfoHash, torrent.Name)
	}

	arr := m.arr.GetOrCreate(torrent.Category)

	debridTorrent := &debridTypes.Torrent{
		Id:               placement.ID,
		InfoHash:         torrent.InfoHash,
		Magnet:           magnet,
		Name:             magnet.Name,
		Arr:              arr,
		Size:             torrent.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: torrent.DownloadUncached,
	}

	dbT, err := client.CheckStatus(debridTorrent)
	if err != nil {
		m.logger.Error().Err(err).Str("name", torrent.Name).Msg("Error checking status")
		torrent.MarkAsError(err)
		_ = m.queue.Update(torrent)

		// Delete from debrid on error
		go func() {
			if dbT != nil && dbT.Id != "" {
				_ = client.DeleteTorrent(dbT.Id)
			}
		}()
		return
	}

	debridTorrent = dbT

	if debridTorrent == nil {
		m.logger.Error().Str("name", torrent.Name).Msg("Debrid torrent not found")
		torrent.MarkAsError(fmt.Errorf("debrid torrent not found"))
		_ = m.queue.Update(torrent)
		return
	}

	if debridTorrent.Status == debridTypes.TorrentStatusError {
		m.logger.Error().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Str("status", string(debridTorrent.Status)).
			Msg("Torrent in error state")
		torrent.MarkAsError(fmt.Errorf("torrent in error state on debrid: %s", debridTorrent.Debrid))
		_ = m.queue.Update(torrent)
		return
	}

	// Update torrent progress
	torrent.Progress = debridTorrent.Progress / 100.0
	torrent.Speed = debridTorrent.Speed
	torrent.Size = debridTorrent.GetSize()
	torrent.Seeders = debridTorrent.Seeders
	torrent.UpdatedAt = time.Now()

	// Update placement progress
	if placement := torrent.GetActivePlacement(); placement != nil {
		placement.Progress = torrent.Progress
	}

	_ = m.queue.Update(torrent)

	m.logger.Debug().
		Str("debrid", debridTorrent.Debrid).
		Str("name", debridTorrent.Name).
		Float64("progress", debridTorrent.Progress).
		Msg("Download progress")

	// Check if done or failed
	if debridTorrent.Status == debridTypes.TorrentStatusDownloaded {
		m.processAction(torrent)
	}
}

func (m *Manager) processAction(torrent *storage.Torrent) {
	torrent.Status = debridTypes.TorrentStatusDownloaded
	torrent.UpdatedAt = time.Now()
	_ = m.queue.Update(torrent)
	m.logger.Info().
		Str("name", torrent.Name).
		Str("action", string(torrent.Action)).
		Msg("Download completed, processing action")

	// Now add torrent to the main storage
	if err := m.AddOrUpdate(torrent, func(t *storage.Torrent) {
		m.RefreshEntries(true)
	}); err != nil {
		return
	}
	err := m.downloader.download(torrent)
	if err != nil {
		return
	}
}

// processTorrent handles the complete torrent lifecycle
func (m *Manager) processNewTorrent(ctx context.Context, torrent *storage.Torrent, debridTorrent *debridTypes.Torrent) error {
	// Update status to submitting
	torrent.UpdatedAt = time.Now()
	_ = m.queue.Update(torrent)

	// AddOrUpdate placement
	_ = torrent.AddPlacement(debridTorrent)
	torrent.ActiveDebrid = debridTorrent.Debrid
	torrent.Bytes = debridTorrent.GetSize()
	torrent.Size = debridTorrent.GetSize()
	torrent.Name = debridTorrent.Name
	torrent.OriginalFilename = debridTorrent.OriginalFilename
	torrent.UpdatedAt = time.Now()
	// AddOrUpdate files here
	for _, file := range debridTorrent.Files {
		tFile := &storage.File{
			Name:      file.Name,
			Size:      file.Size,
			ByteRange: file.ByteRange,
			Deleted:   file.Deleted,
			IsRar:     file.IsRar,
			InfoHash:  torrent.InfoHash,
			AddedOn:   torrent.AddedOn,
		}
		torrent.Files[file.Name] = tFile
	}
	_ = m.queue.Update(torrent)

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		m.logger.Info().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Msg("Started downloading torrent")
		return nil
	}

	// Mark placement as downloaded
	if placement := torrent.GetActivePlacement(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}

	// Process post-download action
	go m.processAction(torrent)
	return nil
}

// SendToDebrid submits a magnet to debrid service(s) - replaces debrid.Process
func (m *Manager) SendToDebrid(ctx context.Context, importRequest *ImportRequest) (*debridTypes.Torrent, error) {
	debridTorrent := &debridTypes.Torrent{
		InfoHash: importRequest.Magnet.InfoHash,
		Magnet:   importRequest.Magnet,
		Name:     importRequest.Magnet.Name,
		Arr:      importRequest.Arr,
		Size:     importRequest.Magnet.Size,
		Files:    make(map[string]debridTypes.File),
	}

	clients := m.FilterDebrid(func(c common.Client) bool {
		if importRequest.SelectedDebrid != "" && c.Config().Name != importRequest.SelectedDebrid {
			return false
		}
		return true
	})

	if len(clients) == 0 {
		return nil, fmt.Errorf("no debrid clients available")
	}

	errs := make([]error, 0, len(clients))

	overrideDownloadUncached := importRequest.DownloadUncached
	// Override first, arr second, debrid third
	if !overrideDownloadUncached && importRequest.Arr.DownloadUncached != nil {
		// Arr cached is set
		overrideDownloadUncached = *importRequest.Arr.DownloadUncached
	}

	for _, db := range clients {
		_logger := db.Logger()
		_logger.Info().
			Str("Debrid", db.Config().Name).
			Str("Arr", importRequest.Arr.Name).
			Str("Hash", debridTorrent.InfoHash).
			Str("Name", debridTorrent.Name).
			Str("Action", string(importRequest.Action)).
			Msg("Processing torrent")

		// If debrid.DownloadUncached is true, it overrides everything
		if db.Config().DownloadUncached || overrideDownloadUncached {
			debridTorrent.DownloadUncached = true
		}

		dbt, err := db.SubmitMagnet(debridTorrent)
		if err != nil || dbt == nil || dbt.Id == "" {
			errs = append(errs, err)
			continue
		}
		dbt.Arr = importRequest.Arr
		_logger.Info().Str("id", dbt.Id).Msgf("Torrent: %s submitted to %s", dbt.Name, db.Config().Name)

		torrent, err := db.CheckStatus(dbt)
		if err != nil && torrent != nil && torrent.Id != "" {
			// Delete the torrent if it was not downloaded
			go func(id string) {
				_ = db.DeleteTorrent(id)
			}(torrent.Id)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if torrent == nil {
			errs = append(errs, fmt.Errorf("torrent %s returned nil after checking status", dbt.Name))
			continue
		}
		return torrent, nil
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("failed to process torrent: no clients available")
	}
	joinedErrors := errors.Join(errs...)
	return nil, fmt.Errorf("failed to process torrent: %w", joinedErrors)
}

// sendCallback sends a callback HTTP request with torrent status
func (m *Manager) sendCallback(callbackURL string, torrent *storage.Torrent, status string, err error) {
	if callbackURL == "" {
		return
	}

	// Create payload
	payload := map[string]interface{}{
		"hash":     torrent.InfoHash,
		"name":     torrent.Name,
		"status":   status,
		"category": torrent.Category,
		"debrid":   torrent.ActiveDebrid,
	}

	if err != nil {
		payload["error"] = err.Error()
	}

	if torrent.ContentPath != "" {
		payload["content_path"] = torrent.ContentPath
	}

	data, jsonErr := json.Marshal(payload)
	if jsonErr != nil {
		m.logger.Error().Err(jsonErr).Msg("Failed to marshal callback payload")
		return
	}

	client := httpclient.DefaultClient()

	_, err = client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(data).
		Post(callbackURL)
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to send callback request")
		return
	}
}
