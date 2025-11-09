package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Sync synchronizes the cached torrents with all debrid services
func (m *Manager) Sync(ctx context.Context) error {
	m.logger.Info().Msg("Starting indexing...")

	// Load cached torrents from storage
	cachedTorrents, err := m.loadFromStorage(ctx)
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to load cache")
	}

	// Fetch torrents from all debrid clients and merge
	allDebridTorrents := make(map[string]map[string]*types.Torrent) // debrid_name -> torrent_id -> torrent
	totalTorrents := 0

	var mu sync.Mutex
	var wg sync.WaitGroup

	m.clients.Range(func(name string, client debrid.Client) bool {
		wg.Add(1)
		go func(name string, client debrid.Client) {
			defer wg.Done()
			if client == nil {
				return
			}

			torrents, err := client.GetTorrents()
			if err != nil {
				m.logger.Error().Err(err).Str("debrid", name).Msg("Failed to get torrents from debrid")
				return
			}

			m.logger.Info().Msgf("%d torrents found from %s", len(torrents), name)

			torrentMap := make(map[string]*types.Torrent, len(torrents))
			for _, t := range torrents {
				torrentMap[t.Id] = t
			}

			mu.Lock()
			allDebridTorrents[name] = torrentMap
			totalTorrents += len(torrents)
			mu.Unlock()
		}(name, client)
		return true
	})

	wg.Wait()

	m.logger.Info().Msgf("%d total torrents found across all debrids", totalTorrents)

	// Build unified view: infohash -> Torrent with multiple placements
	newTorrents := make([]*types.Torrent, 0)
	deletedTorrents := make([]string, 0)

	// Track all infohashes and debrid IDs we've seen
	seenInfoHashes := make(map[string]struct{})
	debridIDToInfoHash := make(map[string]string) // debrid_id -> infohash (for deletion check)

	// Process each debrid's torrents
	for debridName, torrents := range allDebridTorrents {
		for debridID, t := range torrents {
			infoHash := t.InfoHash
			seenInfoHashes[infoHash] = struct{}{}
			debridIDToInfoHash[debridName+":"+debridID] = infoHash

			// Check if this torrent exists in cache
			cached, exists := cachedTorrents[infoHash]

			if !exists {
				// Brand new torrent - add to processing queue
				newTorrents = append(newTorrents, t)
			} else {
				// Existing torrent - check if this debrid placement is new
				if !cached.HasPlacement(debridName) {
					// New placement on this debrid
					newTorrents = append(newTorrents, t)
				}
			}
		}
	}

	// Check for deleted torrents
	for _, cached := range cachedTorrents {
		// Check each placement to see if it still exists
		for debridName, placement := range cached.Placements {
			key := debridName + ":" + placement.ID
			if _, exists := debridIDToInfoHash[key]; !exists {
				// This placement no longer exists in debrid
				deletedTorrents = append(deletedTorrents, cached.InfoHash+":"+debridName)
			}
		}
	}

	// Handle deletions
	if len(deletedTorrents) > 0 {
		m.logger.Trace().Msgf("Found %d deleted placements", len(deletedTorrents))
		for _, deletion := range deletedTorrents {
			// Format: "infohash:debridName"
			parts := splitOnce(deletion, ":")
			if len(parts) == 2 {
				infohash, debridName := parts[0], parts[1]
				if torr, ok := cachedTorrents[infohash]; ok {
					torr.RemovePlacement(debridName, nil)

					// If no placements left, delete the entire torr
					if len(torr.Placements) == 0 {
						delete(cachedTorrents, infohash)
						if err := m.storage.Delete(infohash); err != nil {
							m.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to delete torr")
						}
					} else {
						// Update the torr with removed placement
						if err := m.AddOrUpdate(torr, nil); err != nil {
							m.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to update torr")
						}
					}
				}
			}
		}
	}

	// Store initial cached torrents
	m.logger.Debug().Msgf("Loaded %d torrents from cache", len(cachedTorrents))

	// Process new torrents
	if len(newTorrents) > 0 {
		m.logger.Trace().Msgf("Found %d new torrents/placements", len(newTorrents))
		if err := m.syncTorrents(ctx, newTorrents, cachedTorrents); err != nil {
			return fmt.Errorf("failed to sync torrents: %v", err)
		}
	}

	// Refresh entries and mounts here
	go m.RefreshEntries(true)

	m.logger.Info().Msgf("Indexing complete, %d torrents loaded", len(cachedTorrents))
	return nil
}

// loadFromStorage loads all cached torrents from storage
func (m *Manager) loadFromStorage(ctx context.Context) (map[string]*storage.Torrent, error) {
	torrents, err := m.storage.List(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list torrents: %w", err)
	}

	torrentMap := make(map[string]*storage.Torrent, len(torrents))
	for _, t := range torrents {
		torrentMap[t.InfoHash] = t
	}

	return torrentMap, nil
}

// syncTorrents processes new torrents in parallel
func (m *Manager) syncTorrents(ctx context.Context, torrents []*types.Torrent, cachedTorrents map[string]*storage.Torrent) error {
	workers := 10 // Use a reasonable number of workers

	workChan := make(chan *types.Torrent, min(workers, len(torrents)))

	var processed int64
	var errorCount int64

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case t, ok := <-workChan:
					if !ok {
						return // Channel closed, exit goroutine
					}

					if err := m.processTorrent(t, cachedTorrents); err != nil {
						m.logger.Error().Err(err).Str("torrent", t.Name).Msg("sync error")
						atomic.AddInt64(&errorCount, 1)
					}

					count := atomic.AddInt64(&processed, 1)
					if count%1000 == 0 {
						m.logger.Info().Msgf("Progress: %d/%d torrents processed", count, len(torrents))
					}

				case <-ctx.Done():
					return // Context cancelled, exit goroutine
				}
			}
		}()
	}

	// Feed work to workers
	for _, t := range torrents {
		select {
		case workChan <- t:
			// Work sent successfully
		case <-ctx.Done():
			break // Context cancelled
		}
	}

	// Signal workers that no more work is coming
	close(workChan)

	// Wait for all workers to complete
	wg.Wait()

	m.logger.Info().Msgf("Sync complete: %d torrents processed, %d errors", len(torrents), errorCount)
	return nil
}

// processTorrent processes a single torrent from debrid
func (m *Manager) processTorrent(t *types.Torrent, cachedTorrents map[string]*storage.Torrent) error {
	// Check if torrent is complete
	isComplete := func(files map[string]types.File) bool {
		_complete := len(files) > 0
		for _, file := range files {
			if file.Link == "" {
				_complete = false
				break
			}
		}
		return _complete
	}

	// Get the debrid client to update the torrent if needed
	client := m.DebridClient(t.Debrid)
	if client == nil {
		return fmt.Errorf("debrid client not found: %s", t.Debrid)
	}

	if !isComplete(t.Files) {
		if err := client.UpdateTorrent(t); err != nil {
			return fmt.Errorf("failed to update torrent: %w", err)
		}
	}

	if !isComplete(t.Files) {
		m.logger.Debug().
			Str("torrent_id", t.Id).
			Str("torrent_name", t.Name).
			Int("total_files", len(t.Files)).
			Msg("Torrent still not complete after refresh, marking as bad")
		return nil
	}

	// Parse added timestamp
	addedOn, err := time.Parse(time.RFC3339, t.Added)
	if err != nil {
		addedOn = time.Now()
	}

	// Check if we have an existing managed torrent
	mt, exists := cachedTorrents[t.InfoHash]
	if !exists {
		// Create new managed torrent
		size := t.Size
		if size == 0 {
			// Fallback to summing file sizes
			size = t.Bytes
		}
		mt = &storage.Torrent{
			InfoHash:         t.InfoHash,
			Name:             t.Name,
			OriginalFilename: t.OriginalFilename,
			Size:             size,
			Bytes:            size,
			Magnet:           t.Magnet.Link,
			ActiveDebrid:     t.Debrid,
			Placements:       make(map[string]*storage.Placement),
			Files:            make(map[string]*storage.File),
			Status:           t.Status,
			Progress:         t.Progress,
			Speed:            t.Speed,
			Seeders:          t.Seeders,
			IsComplete:       len(t.Files) > 0,
			Bad:              false,
			AddedOn:          addedOn,
			CreatedAt:        addedOn,
			UpdatedAt:        time.Now(),
		}
		mt.Folder = mt.GetFolder()
		cachedTorrents[t.InfoHash] = mt
	}

	// Populate global Files metadata (only if empty)
	if len(mt.Files) == 0 {
		for _, f := range t.GetFiles() {
			mt.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Size:      f.Size,
				IsRar:     f.IsRar,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
				InfoHash:  t.InfoHash,
				Debrid:    t.Debrid,
			}
		}
	}

	// AddOrUpdate or update placement for this debrid
	placement := mt.AddPlacement(t)
	placement.Progress = t.Progress
	if t.Status == types.TorrentStatusDownloaded {
		downloadedAt := addedOn
		placement.DownloadedAt = &downloadedAt
	}

	// Populate placement-specific file data
	for _, f := range t.GetFiles() {
		placement.Files[f.Name] = &storage.PlacementFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}

	// If this is the first placement or the only one, make it active
	if mt.ActiveDebrid == "" || len(mt.Placements) == 1 {
		if t.Status == types.TorrentStatusDownloaded {
			_ = mt.ActivatePlacement(t.Debrid)
		}
	}

	// Save to storage
	if err := m.AddOrUpdate(mt, nil); err != nil {
		return fmt.Errorf("failed to save torrent: %w", err)
	}

	return nil
}

// splitOnce splits a string on the first occurrence of sep
func splitOnce(s, sep string) []string {
	parts := make([]string, 0, 2)
	if idx := len(s); idx >= 0 {
		for i := 0; i < len(s); i++ {
			if s[i:i+len(sep)] == sep {
				parts = append(parts, s[:i])
				parts = append(parts, s[i+len(sep):])
				return parts
			}
		}
	}
	return []string{s}
}
