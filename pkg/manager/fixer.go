package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Fixer handles torrent repair with cascading re-insertion across debrids
type Fixer struct {
	manager *Manager

	// Track re-insertion attempts and failures
	reInsertAttempts   *xsync.Map[string, int]           // infohash:debrid -> attempt count
	failedToReinsert   *xsync.Map[string, struct{}]      // infohash -> failed completely
	inFlightRepairs    *xsync.Map[string, *FixerRequest] // infohash -> repair request
	debridOrder        []string                          // Order of debrids to try (from config)
	maxReinsertRetries int
}

// FixerRequest tracks an ongoing repair operation
type FixerRequest struct {
	InfoHash         string
	CurrentDebrid    string
	AttemptedDebrids []string
	StartedAt        time.Time
	LastAttempt      time.Time
	result           chan *FixResult
}

// FixResult is the result of a fix operation
type FixResult struct {
	Success       bool
	NewDebrid     string
	Error         error
	AttemptsCount int
}

// NewFixer creates a new Fixer instance
func NewFixer(manager *Manager) *Fixer {
	// Get debrid order from config
	cfg := config.Get()
	debridOrder := make([]string, 0, len(cfg.Debrids))
	for _, d := range cfg.Debrids {
		debridOrder = append(debridOrder, d.Name)
	}

	return &Fixer{
		manager:            manager,
		reInsertAttempts:   xsync.NewMap[string, int](),
		failedToReinsert:   xsync.NewMap[string, struct{}](),
		inFlightRepairs:    xsync.NewMap[string, *FixerRequest](),
		debridOrder:        debridOrder,
		maxReinsertRetries: 2, // Retry each debrid up to 2 times
	}
}

// FixTorrent attempts to fix a broken torrent by re-inserting across debrids
// Strategy:
// 1. Try to re-insert on current active debrid, except if skipCurrent is true
// 2. If fails, cascade through other debrids in config order
// 3. Skip debrids where torrent already exists (unless they're also broken)
// 4. Mark as completely failed if all debrids fail
func (f *Fixer) FixTorrent(ctx context.Context, torrent *storage.Torrent, skipCurrent bool) (*FixResult, error) {
	// Check if torrent is already marked as completely failed
	if _, failed := f.failedToReinsert.Load(torrent.InfoHash); failed {
		return nil, fmt.Errorf("torrent %s already marked as failed to re-insert", torrent.Name)
	}

	// Check if repair is already in flight
	if req, exists := f.inFlightRepairs.Load(torrent.InfoHash); exists {
		f.manager.logger.Debug().
			Str("infohash", torrent.InfoHash).
			Str("torrent", torrent.Name).
			Msg("Repair already in progress, waiting for result")

		// Wait for existing repair to complete
		select {
		case result := <-req.result:
			return result, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Minute):
			return nil, fmt.Errorf("repair timeout for %s", torrent.Name)
		}
	}

	// Create new repair request
	req := &FixerRequest{
		InfoHash:         torrent.InfoHash,
		AttemptedDebrids: make([]string, 0),
		StartedAt:        time.Now(),
		LastAttempt:      time.Now(),
		result:           make(chan *FixResult, 1),
	}
	f.inFlightRepairs.Store(torrent.InfoHash, req)
	defer f.inFlightRepairs.Delete(torrent.InfoHash)
	req.CurrentDebrid = torrent.ActiveDebrid

	f.manager.logger.Info().
		Str("infohash", torrent.InfoHash).
		Str("name", torrent.Name).
		Str("active_debrid", torrent.ActiveDebrid).
		Msg("Starting torrent fix operation")

	// Build debrid attempt order: current debrid first, then others in config order
	attemptOrder := f.buildAttemptOrder(torrent, skipCurrent)

	var lastErr error
	totalAttempts := 0

	for _, debridName := range attemptOrder {
		select {
		case <-ctx.Done():
			result := &FixResult{Success: false, Error: ctx.Err(), AttemptsCount: totalAttempts}
			req.result <- result
			return result, ctx.Err()
		default:
		}

		req.AttemptedDebrids = append(req.AttemptedDebrids, debridName)
		req.LastAttempt = time.Now()

		f.manager.logger.Info().
			Str("debrid", debridName).
			Str("infohash", torrent.InfoHash).
			Int("attempt", totalAttempts+1).
			Msg("Attempting re-insertion")

		// Attempt re-insertion on this debrid
		success, err := f.MoveTorrent(torrent, debridName, true)
		totalAttempts++

		if success {
			f.manager.logger.Info().
				Str("debrid", debridName).
				Str("infohash", torrent.InfoHash).
				Msg("Successfully re-inserted torrent")

			// Mark as successful
			f.clearFailureState(torrent.InfoHash, debridName)

			result := &FixResult{
				Success:       true,
				NewDebrid:     debridName,
				Error:         nil,
				AttemptsCount: totalAttempts,
			}
			req.result <- result
			return result, nil
		}

		lastErr = err
		f.manager.logger.Warn().
			Err(err).
			Str("debrid", debridName).
			Str("infohash", torrent.InfoHash).
			Str("name", torrent.Name).
			Msg("Re-insertion failed, trying next debrid")

		// Track attempt
		attemptKey := fmt.Sprintf("%s:%s", torrent.InfoHash, debridName)
		attempts, _ := f.reInsertAttempts.LoadOrCompute(attemptKey, func() (int, bool) {
			return 0, true
		})
		f.reInsertAttempts.Store(attemptKey, attempts+1)
	}

	// All debrids failed - mark as completely failed
	f.manager.logger.Error().
		Str("infohash", torrent.InfoHash).
		Int("attempts", totalAttempts).
		Msg("All re-insertion attempts failed")

	f.failedToReinsert.Store(torrent.InfoHash, struct{}{})

	// Mark torrent as bad
	torrent.Bad = true
	torrent.UpdatedAt = time.Now()
	_ = f.manager.AddOrUpdate(torrent, func(t *storage.Torrent) {
		f.manager.RefreshEntries(true)
	})

	result := &FixResult{
		Success:       false,
		Error:         fmt.Errorf("all re-insertion attempts failed: %w", lastErr),
		AttemptsCount: totalAttempts,
	}
	req.result <- result
	return result, result.Error
}

// MoveTorrent attempts to re-insert a torrent on a specific debrid
func (f *Fixer) MoveTorrent(torrent *storage.Torrent, debridName string, reinsert bool) (bool, error) {
	defer func() {
		// Save to storage
		_ = f.manager.AddOrUpdate(torrent, nil) // No need to refresh mounts
	}()

	client := f.manager.DebridClient(debridName)
	if client == nil {
		return false, fmt.Errorf("debrid client %s not found", debridName)
	}

	// Check if placement already exists on this debrid
	// Find placement for this debrid and torrent's infohash
	placementKey := storage.GetPlacementKey(debridName, torrent.InfoHash)
	placement, hasPlacement := torrent.Placements[placementKey]
	var oldID string
	if hasPlacement && placement != nil && placement.ID != "" && !reinsert {
		// Activate the existing placement
		if err := torrent.ActivatePlacement(debridName); err != nil {
			return false, fmt.Errorf("failed to activate existing placement: %w", err)
		}
		torrent.Bad = false
		torrent.UpdatedAt = time.Now()
		return true, nil
	}
	if placement != nil {
		oldID = placement.ID
	}

	// Construct magnet
	magnet, err := utils.GetMagnetInfo(torrent.Magnet, f.manager.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(torrent.InfoHash, torrent.Name)
	}

	// Submit to debrid
	newDebridTorrent := &types.Torrent{
		Name:             torrent.Name,
		Magnet:           magnet,
		InfoHash:         torrent.InfoHash,
		Size:             torrent.Size,
		Files:            make(map[string]types.File),
		DownloadUncached: false,
	}

	newDebridTorrent, err = client.SubmitMagnet(newDebridTorrent)
	if err != nil {
		return false, fmt.Errorf("failed to submit magnet: %w", err)
	}

	if newDebridTorrent == nil || newDebridTorrent.Id == "" {
		return false, fmt.Errorf("failed to submit magnet: empty torrent")
	}

	// Check status
	newDebridTorrent.DownloadUncached = false
	newDebridTorrent, err = client.CheckStatus(newDebridTorrent)
	if err != nil {
		// Delete the failed torrent
		if newDebridTorrent != nil && newDebridTorrent.Id != "" {
			_ = client.DeleteTorrent(newDebridTorrent.Id)
		}
		return false, fmt.Errorf("failed to check status: %w", err)
	}

	// Verify files have links
	if len(newDebridTorrent.Files) == 0 {
		_ = client.DeleteTorrent(newDebridTorrent.Id)
		return false, fmt.Errorf("no files in torrent after re-insertion")
	}

	for _, f := range newDebridTorrent.GetFiles() {
		if f.Link == "" && f.Id == "" {
			_ = client.DeleteTorrent(newDebridTorrent.Id)
			return false, fmt.Errorf("empty link/id for file %s", f.Name)
		}
	}

	// Update torrent with new placement
	placement = torrent.AddPlacement(newDebridTorrent)

	// Populate placement files
	for _, f := range newDebridTorrent.GetFiles() {
		placement.Files[f.Name] = &storage.PlacementFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}

	// Update global files metadata if needed
	for _, f := range newDebridTorrent.GetFiles() {
		if _, exists := torrent.Files[f.Name]; !exists {
			torrent.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Size:      f.Size,
				IsRar:     f.IsRar,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
				InfoHash:  torrent.InfoHash,
				Debrid:    debridName,
			}
		}
	}

	// Activate this debrid
	if err := torrent.ActivatePlacement(debridName); err != nil {
		f.manager.logger.Warn().Err(err).Msg("failed to activate placement")
	}

	torrent.Bad = false
	torrent.UpdatedAt = time.Now()

	// Delete old torrent from debrid if different ID
	if oldID != "" && oldID != newDebridTorrent.Id {
		go func() {
			_ = client.DeleteTorrent(oldID)
		}()
	}

	return true, nil
}

func (f *Fixer) ReInsertTorrent(torrent *storage.Torrent) (bool, error) {
	return f.MoveTorrent(torrent, torrent.ActiveDebrid, true)
}

// buildAttemptOrder creates the order of debrids to attempt re-insertion
// Priority: current active debrid first, then others in config order
// If skipCurrent is true, current active debrid is skipped
func (f *Fixer) buildAttemptOrder(torrent *storage.Torrent, skipCurrent bool) []string {
	order := make([]string, 0, len(f.debridOrder))

	// AddOrUpdate other debrids in config order
	for _, debridName := range f.debridOrder {
		if debridName == torrent.ActiveDebrid && skipCurrent {
			continue
		}
		order = append(order, debridName)
	}

	return order
}

// clearFailureState clears failure tracking for a torrent
func (f *Fixer) clearFailureState(infohash, debridName string) {
	// Remove from failed list
	f.failedToReinsert.Delete(infohash)

	// Clear attempt counters
	attemptKey := fmt.Sprintf("%s:%s", infohash, debridName)
	f.reInsertAttempts.Delete(attemptKey)
}

// IsFailedToReinsert checks if a torrent has been marked as failed to re-insert
func (f *Fixer) IsFailedToReinsert(infohash string) bool {
	_, failed := f.failedToReinsert.Load(infohash)
	return failed
}

// GetAttemptCount returns the number of re-insertion attempts for a torrent on a debrid
func (f *Fixer) GetAttemptCount(infohash, debridName string) int {
	attemptKey := fmt.Sprintf("%s:%s", infohash, debridName)
	attempts, _ := f.reInsertAttempts.Load(attemptKey)
	return attempts
}

// ResetFailureState manually resets the failure state for a torrent
func (f *Fixer) ResetFailureState(infohash string) {
	f.failedToReinsert.Delete(infohash)

	// Clear all attempt counters for this torrent
	f.reInsertAttempts.Range(func(key string, value int) bool {
		if len(key) >= len(infohash) && key[:len(infohash)] == infohash {
			f.reInsertAttempts.Delete(key)
		}
		return true
	})
}
