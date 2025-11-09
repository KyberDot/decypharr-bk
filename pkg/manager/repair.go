package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// GetBrokenFiles checks if files in a torrent are broken and attempts to fix them
// Returns the list of broken files, or empty if successfully repaired
// This implementation aligns with cache.GetBrokenFiles behavior
func (m *Manager) GetBrokenFiles(torrent *storage.Torrent, filenames []string) []string {
	if torrent.ActiveDebrid == "" {
		return filenames
	}

	repairStrategy := config.Get().Repair.Strategy
	brokenFiles := make([]string, 0)

	// Check which files need checking
	files := make(map[string]*storage.File)
	if len(filenames) > 0 {
		for _, name := range filenames {
			if f, ok := torrent.Files[name]; ok {
				files[name] = f
			}
		}
	} else {
		files = torrent.Files
	}

	// First pass: check for missing links and refresh if needed
	placement := torrent.GetActivePlacement(torrent.InfoHash)
	if placement == nil {
		return filenames
	}

	// Get the debrid client for link checking
	client := m.DebridClient(torrent.ActiveDebrid)
	if client == nil {
		m.logger.Error().Str("debrid", torrent.ActiveDebrid).Msg("Debrid client not found")
		return filenames
	}

	// Second pass: check links validity in parallel
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a mutex to protect brokenFiles slice and torrent-wide failure flag
	var mu sync.Mutex
	torrentWideFailed := false

	wg.Add(len(files))

	for name, file := range files {
		go func(name string, file *storage.File) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
			}

			placementFile := placement.Files[name]
			if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
				mu.Lock()
				if repairStrategy == config.RepairStrategyPerTorrent {
					torrentWideFailed = true
					mu.Unlock()
					cancel() // Signal all other goroutines to stop
					return
				} else {
					// per_file strategy - only mark this file as broken
					brokenFiles = append(brokenFiles, name)
				}
				mu.Unlock()
				return
			}

			// Check if the link is still valid
			link := placementFile.Link
			if link == "" {
				link = placementFile.Id
			}

			if link != "" {
				if err := client.CheckLink(link); err != nil {
					if errors.Is(err, utils.HosterUnavailableError) {
						mu.Lock()
						if repairStrategy == config.RepairStrategyPerTorrent {
							torrentWideFailed = true
							mu.Unlock()
							cancel() // Signal all other goroutines to stop
							return
						} else {
							// per_file strategy - only mark this file as broken
							brokenFiles = append(brokenFiles, name)
						}
						mu.Unlock()
					}
				}
			}
		}(name, file)
	}

	wg.Wait()

	// Handle the result based on strategy
	if repairStrategy == config.RepairStrategyPerTorrent && torrentWideFailed {
		// Mark all files as broken for per_torrent strategy
		for name := range files {
			brokenFiles = append(brokenFiles, name)
		}
	}
	// For per_file strategy, brokenFiles already contains only the broken ones

	// Try to fix the torrent if broken files were found
	if len(brokenFiles) > 0 {
		m.logger.Info().
			Str("infohash", torrent.InfoHash).
			Int("broken_files", len(brokenFiles)).
			Msg("Detected broken files, attempting to fix torrent")

		// Use Fixer to repair the torrent
		result, err := m.fixer.FixTorrent(m.ctx, torrent, false)
		if err != nil || !result.Success {
			m.logger.Error().
				Err(err).
				Str("infohash", torrent.InfoHash).
				Msg("Failed to fix torrent")
			return brokenFiles
		} else {
			// Refresh torrent after successful fix
			return []string{}
		}
	}

	// No broken files
	return []string{}
}

// MoveTorrent attempts to repair a torrent by moving it to a new debrid service
func (m *Manager) MoveTorrent(ctx context.Context, torrent *storage.Torrent) error {

	result, err := m.fixer.FixTorrent(ctx, torrent, true) // Always move to the next debrid rather than trying to fix current debrid
	if err != nil {
		return err
	}

	if !result.Success {
		return fmt.Errorf("moving failed after %d attempts: %w", result.AttemptsCount, result.Error)
	}

	m.logger.Info().
		Str("name", torrent.Name).
		Str("new_debrid", result.NewDebrid).
		Msg("Torrent moved successfully")

	return nil
}

// IsFailedToReinsert checks if a torrent has been marked as failed to re-insert
func (m *Manager) IsFailedToReinsert(infohash string) bool {
	if m.fixer == nil {
		return false
	}
	return m.fixer.IsFailedToReinsert(infohash)
}

// ResetRepairState manually resets the repair failure state for a torrent
func (m *Manager) ResetRepairState(infohash string) {
	if m.fixer != nil {
		m.fixer.ResetFailureState(infohash)
	}
}
