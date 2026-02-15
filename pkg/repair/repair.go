package repair

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/errgroup"
)

type contexts struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type discoveredFile struct {
	ArrName  string
	File     arr.ContentFile
	InfoHash string
	Protocol config.Protocol
}

type Repair struct {
	manager     *manager.Manager
	autoProcess bool
	logger      zerolog.Logger
	workers     int
	ctx         context.Context

	activeContexts *xsync.Map[string, contexts]
}

const (
	actionReinsertTorrent = "reinsert_torrent"
	actionReacquireArr    = "arr_reacquire"
	ScopeManagedEntries   = "managed_entries"
)

func New(mgr *manager.Manager) *Repair {
	cfg := config.Get()
	workers := max(2, runtime.NumCPU())
	if cfg.Repair.Workers > 0 {
		workers = cfg.Repair.Workers
	}

	r := &Repair{
		logger:         logger.New("repair"),
		autoProcess:    cfg.Repair.AutoProcess,
		manager:        mgr,
		workers:        workers,
		ctx:            context.Background(),
		activeContexts: xsync.NewMap[string, contexts](),
	}

	if err := r.manager.Storage().PrepareRepairDataV2(); err != nil {
		r.logger.Warn().Err(err).Msg("Failed to prepare v2 repair storage")
	}

	return r
}

func (r *Repair) Run(ctx context.Context) {
	r.logger.Debug().Bool("auto_process", r.autoProcess).Int("workers", r.workers).Msg("Starting scheduled repair run")
	if _, err := r.addJobWithContext(ctx, []string{}, []string{}, r.autoProcess, true); err != nil {
		r.logger.Error().Err(err).Msg("Error running recurring repair job")
	}
}

func (r *Repair) Stop() {
	r.activeContexts.Range(func(_ string, value contexts) bool {
		if value.cancel != nil {
			value.cancel()
		}
		return true
	})
}

func (r *Repair) AddJob(arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) (string, error) {
	return r.addJobWithContext(r.ctx, arrsNames, mediaIDs, autoProcess, recurrent)
}

func (r *Repair) addJobWithContext(ctx context.Context, arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) (string, error) {
	if err := r.preRunChecks(); err != nil {
		return "", err
	}

	managedEntriesScope := len(arrsNames) == 1 && arrsNames[0] == ScopeManagedEntries
	arrs := r.getArrs(arrsNames)
	if managedEntriesScope {
		arrs = nil
	}
	if !managedEntriesScope && len(arrs) == 0 {
		return "", fmt.Errorf("no arr services available for repair")
	}

	mode := storage.RepairModeDetectOnly
	if autoProcess {
		mode = storage.RepairModeDetectAndRepair
	}
	key := storage.RepairJobKey(arrs, mediaIDs, mode)

	if active := r.findActiveJobByKey(key); active != nil {
		r.logger.Debug().
			Str("active_job_id", active.ID).
			Str("job_key", key).
			Msg("Repair job already active for requested scope")
		return "", fmt.Errorf("repair job already active for this scope")
	}

	job := &storage.Job{
		ID:          uuid.NewString(),
		Arrs:        arrs,
		MediaIDs:    append([]string(nil), mediaIDs...),
		StartedAt:   time.Now(),
		Status:      storage.JobStarted,
		Stage:       storage.JobStageQueued,
		Mode:        mode,
		UniqueKey:   key,
		AutoProcess: autoProcess,
		Recurrent:   recurrent,
		BrokenItems: make(map[string][]arr.ContentFile),
		Stats:       storage.RepairStats{},
	}

	if err := r.manager.Storage().SaveRepairJob(key, job); err != nil {
		return "", err
	}
	scope := "arr"
	if managedEntriesScope {
		scope = ScopeManagedEntries
	}
	r.logger.Info().
		Str("job_id", job.ID).
		Str("scope", scope).
		Str("mode", string(job.Mode)).
		Int("arr_count", len(job.Arrs)).
		Int("media_count", len(job.MediaIDs)).
		Bool("auto_process", job.AutoProcess).
		Bool("recurrent", job.Recurrent).
		Int("workers", r.workers).
		Msg("Repair job queued")

	runCtx, cancel := context.WithCancel(ctx)
	r.activeContexts.Store(job.ID, contexts{ctx: runCtx, cancel: cancel})

	go r.runJob(runCtx, job)
	return job.ID, nil
}

func (r *Repair) StopJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}

	switch job.Status {
	case storage.JobCompleted, storage.JobFailed, storage.JobCancelled:
		return fmt.Errorf("job %s cannot be stopped (status: %s)", id, job.Status)
	}

	if ctxObj, ok := r.activeContexts.Load(id); ok && ctxObj.cancel != nil {
		ctxObj.cancel()
	}

	job.Status = storage.JobCancelled
	job.Stage = storage.JobStageCancelled
	job.CompletedAt = time.Now()
	job.Error = "job cancelled by user"
	r.saveToStorage(job)

	r.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventRepairCancelled,
		Status:  "cancelled",
		Message: job.DiscordContext(),
	})
	r.logger.Info().
		Str("job_id", job.ID).
		Str("stage", string(job.Stage)).
		Msg("Repair job cancelled by user")
	return nil
}

func (r *Repair) GetJob(id string) *storage.Job {
	job, err := r.manager.Storage().GetRepairJob(id)
	if err != nil {
		r.logger.Error().Err(err).Msgf("Failed to get repair job %s", id)
		return nil
	}
	return job
}

func (r *Repair) GetJobs() []*storage.Job {
	jobs, err := r.manager.Storage().LoadAllRepairJobs()
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to load repair jobs")
		return nil
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})
	return jobs
}

func (r *Repair) ProcessJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}
	if job.Status != storage.JobPending {
		return fmt.Errorf("job %s is not pending", id)
	}
	if len(job.BrokenItems) == 0 {
		job.Status = storage.JobCompleted
		job.Stage = storage.JobStageCompleted
		job.CompletedAt = time.Now()
		r.saveToStorage(job)
		r.logger.Info().
			Str("job_id", job.ID).
			Msg("Pending repair job completed without execution (no broken items)")
		return nil
	}

	ctx, cancel := context.WithCancel(r.ctx)
	r.activeContexts.Store(job.ID, contexts{ctx: ctx, cancel: cancel})

	job.Mode = storage.RepairModeDetectAndRepair
	job.AutoProcess = true
	job.Status = storage.JobProcessing
	job.Stage = storage.JobStageExecuting
	r.saveToStorage(job)
	r.logger.Info().
		Str("job_id", job.ID).
		Int("planned_actions", len(job.Actions)).
		Int("broken_items", countBrokenItems(job.BrokenItems)).
		Msg("Repair job moved from pending to processing")

	go r.runExecution(ctx, job)
	return nil
}

func (r *Repair) DeleteJobs(ids []string) {
	for _, id := range ids {
		if id == "" {
			continue
		}
		if ctxObj, ok := r.activeContexts.Load(id); ok && ctxObj.cancel != nil {
			ctxObj.cancel()
		}
		r.activeContexts.Delete(id)
		if err := r.manager.Storage().DeleteRepairJob(id); err != nil {
			r.logger.Error().Err(err).Msgf("Failed to delete repair job %s", id)
		}
	}
}

func (r *Repair) runJob(ctx context.Context, job *storage.Job) {
	defer func() {
		r.activeContexts.Delete(job.ID)
		r.saveToStorage(job)
	}()

	job.Status = storage.JobStarted
	job.Stage = storage.JobStageDiscovering
	r.saveToStorage(job)
	r.logger.Info().
		Str("job_id", job.ID).
		Str("mode", string(job.Mode)).
		Int("arr_count", len(job.Arrs)).
		Int("media_count", len(job.MediaIDs)).
		Msg("Repair discovery started")

	brokenItems, discovered, scanStats, err := r.scanBroken(ctx, job.Arrs, job.MediaIDs)
	if err != nil {
		r.handleRunError(ctx, job, err)
		return
	}

	job.Stats.Discovered += scanStats.Discovered
	job.Stats.Probed += scanStats.Probed
	job.Stats.Broken += scanStats.Broken
	job.Stats.Unknown += scanStats.Unknown
	job.BrokenItems = brokenItems
	r.logger.Info().
		Str("job_id", job.ID).
		Int("discovered", scanStats.Discovered).
		Int("probed", scanStats.Probed).
		Int("broken", scanStats.Broken).
		Int("unknown", scanStats.Unknown).
		Int("broken_arrs", len(brokenItems)).
		Msg("Repair discovery completed")

	job.Stage = storage.JobStagePlanning
	job.Actions = r.planActions(discovered)
	job.Stats.Planned = len(job.Actions)
	r.saveToStorage(job)

	if len(brokenItems) == 0 {
		job.Stage = storage.JobStageCompleted
		job.Status = storage.JobCompleted
		job.CompletedAt = time.Now()
		r.saveToStorage(job)
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairComplete,
			Status:  "success",
			Message: job.DiscordContext(),
		})
		r.logger.Info().
			Str("job_id", job.ID).
			Msg("Repair job completed with no issues found")
		return
	}

	if job.Mode == storage.RepairModeDetectOnly {
		job.Status = storage.JobPending
		r.saveToStorage(job)
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairPending,
			Status:  "pending",
			Message: job.DiscordContext(),
		})
		r.logger.Info().
			Str("job_id", job.ID).
			Int("broken_items", countBrokenItems(job.BrokenItems)).
			Int("planned_actions", len(job.Actions)).
			Msg("Repair job pending manual execution")
		return
	}

	r.runExecution(ctx, job)
}

func (r *Repair) runExecution(ctx context.Context, job *storage.Job) {
	defer func() {
		r.activeContexts.Delete(job.ID)
		r.saveToStorage(job)
	}()

	initialBroken := countBrokenItems(job.BrokenItems)
	job.Status = storage.JobProcessing
	job.Stage = storage.JobStageExecuting
	r.saveToStorage(job)
	r.logger.Info().
		Str("job_id", job.ID).
		Int("actions", len(job.Actions)).
		Int("initial_broken", initialBroken).
		Msg("Repair execution started")

	execErr := r.executeActions(ctx, job)
	if execErr != nil && errors.Is(ctx.Err(), context.Canceled) {
		r.handleRunError(ctx, job, ctx.Err())
		return
	}
	if execErr != nil {
		r.logger.Warn().
			Err(execErr).
			Str("job_id", job.ID).
			Msg("Repair execution finished with action errors; proceeding to verification")
	}

	job.Stage = storage.JobStageVerifying
	r.saveToStorage(job)
	r.logger.Debug().
		Str("job_id", job.ID).
		Msg("Repair verification scan started")

	remaining, _, verifyStats, verifyErr := r.scanBroken(ctx, job.Arrs, job.MediaIDs)
	if verifyErr != nil {
		r.handleRunError(ctx, job, verifyErr)
		return
	}

	job.Stats.Probed += verifyStats.Probed
	job.Stats.Unknown += verifyStats.Unknown

	remainingBroken := countBrokenItems(remaining)
	if remainingBroken > initialBroken {
		remainingBroken = initialBroken
	}
	fixed := initialBroken - remainingBroken
	if fixed < 0 {
		fixed = 0
	}

	job.BrokenItems = remaining
	job.Stats.Fixed += fixed
	job.Stats.Failed += remainingBroken
	r.logger.Info().
		Str("job_id", job.ID).
		Int("fixed", fixed).
		Int("remaining_broken", remainingBroken).
		Int("verify_probed", verifyStats.Probed).
		Int("verify_unknown", verifyStats.Unknown).
		Msg("Repair verification completed")

	if execErr == nil && remainingBroken == 0 {
		job.Status = storage.JobCompleted
		job.Stage = storage.JobStageCompleted
		job.CompletedAt = time.Now()
		r.saveToStorage(job)
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairComplete,
			Status:  "success",
			Message: job.DiscordContext(),
		})
		r.logger.Info().
			Str("job_id", job.ID).
			Int("executed_actions", job.Stats.Executed).
			Msg("Repair job completed successfully")
		return
	}

	if execErr != nil {
		job.Error = execErr.Error()
	} else {
		job.Error = fmt.Sprintf("%d broken files remain after verification", remainingBroken)
	}
	job.Status = storage.JobFailed
	job.Stage = storage.JobStageFailed
	job.FailedAt = time.Now()
	job.CompletedAt = time.Now()
	r.saveToStorage(job)
	r.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventRepairFailed,
		Status:  "error",
		Message: job.DiscordContext(),
		Error:   errors.New(job.Error),
	})
	r.logger.Error().
		Str("job_id", job.ID).
		Str("error", job.Error).
		Int("remaining_broken", remainingBroken).
		Msg("Repair job failed")
}

func (r *Repair) executeActions(ctx context.Context, job *storage.Job) error {
	if len(job.Actions) == 0 {
		if countBrokenItems(job.BrokenItems) > 0 && len(job.Arrs) > 0 {
			job.Actions = []*storage.RepairAction{
				{
					ID:       uuid.NewString(),
					Type:     actionReacquireArr,
					Protocol: config.ProtocolAll,
					Status:   storage.RepairActionPlanned,
				},
			}
		}
	}

	var firstErr error

	for _, action := range job.Actions {
		if action == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		action.Status = storage.RepairActionRunning
		action.StartedAt = time.Now()
		r.saveToStorage(job)
		r.logger.Debug().
			Str("job_id", job.ID).
			Str("action_id", action.ID).
			Str("action_type", action.Type).
			Str("entry_id", action.EntryID).
			Str("protocol", string(action.Protocol)).
			Msg("Repair action started")

		switch action.Type {
		case actionReinsertTorrent:
			entry, err := r.manager.GetEntry(action.EntryID)
			if err != nil {
				action.Status = storage.RepairActionFailed
				action.Error = err.Error()
				if firstErr == nil {
					firstErr = fmt.Errorf("reinsert failed for %s: %w", action.EntryID, err)
				}
				r.logger.Warn().
					Err(err).
					Str("job_id", job.ID).
					Str("action_id", action.ID).
					Str("entry_id", action.EntryID).
					Msg("Repair action failed to fetch entry for reinsert")
				break
			}
			if err := r.manager.ReinsertEntry(ctx, entry); err != nil {
				action.Status = storage.RepairActionFailed
				action.Error = err.Error()
				if firstErr == nil {
					firstErr = fmt.Errorf("reinsert failed for %s: %w", action.EntryID, err)
				}
				r.logger.Warn().
					Err(err).
					Str("job_id", job.ID).
					Str("action_id", action.ID).
					Str("entry_id", action.EntryID).
					Msg("Repair action failed to reinsert entry")
				break
			}
			action.Status = storage.RepairActionSucceeded
			job.Stats.Executed++

		case actionReacquireArr:
			if err := r.processBrokenItems(ctx, job.BrokenItems); err != nil {
				action.Status = storage.RepairActionFailed
				action.Error = err.Error()
				if firstErr == nil {
					firstErr = err
				}
				r.logger.Warn().
					Err(err).
					Str("job_id", job.ID).
					Str("action_id", action.ID).
					Msg("Repair action failed to reacquire broken items via Arr")
				break
			}
			action.Status = storage.RepairActionSucceeded
			job.Stats.Executed++

		default:
			action.Status = storage.RepairActionSkipped
			action.Error = "unknown action"
		}

		action.CompletedAt = time.Now()
		r.saveToStorage(job)
		r.logger.Debug().
			Str("job_id", job.ID).
			Str("action_id", action.ID).
			Str("action_type", action.Type).
			Str("status", string(action.Status)).
			Msg("Repair action completed")
	}

	return firstErr
}

func (r *Repair) processBrokenItems(ctx context.Context, brokenItems map[string][]arr.ContentFile) error {
	if len(brokenItems) == 0 {
		return nil
	}
	total := countBrokenItems(brokenItems)
	r.logger.Debug().
		Int("arr_count", len(brokenItems)).
		Int("broken_items", total).
		Int("workers", r.workers).
		Msg("Processing broken items through Arr services")

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, min(r.workers, len(brokenItems))))

	for arrName, items := range brokenItems {
		items := append([]arr.ContentFile(nil), items...)

		g.Go(func() error {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}

			a := r.manager.Arr().Get(arrName)
			if a == nil {
				return fmt.Errorf("arr %s not found", arrName)
			}
			r.logger.Trace().
				Str("arr", arrName).
				Int("files", len(items)).
				Msg("Processing broken files for Arr service")
			if err := a.DeleteFiles(items); err != nil {
				return fmt.Errorf("failed to delete broken files for %s: %w", arrName, err)
			}
			if err := a.SearchMissing(items); err != nil {
				return fmt.Errorf("failed to search missing for %s: %w", arrName, err)
			}
			return nil
		})
	}

	return g.Wait()
}

func (r *Repair) scanBroken(ctx context.Context, arrNames, mediaIDs []string) (map[string][]arr.ContentFile, []discoveredFile, storage.RepairStats, error) {
	brokenByArr := make(map[string][]arr.ContentFile)
	discovered := make([]discoveredFile, 0)
	stats := storage.RepairStats{}

	if len(arrNames) == 0 {
		return r.scanManagedEntries(ctx, mediaIDs)
	}
	r.logger.Debug().
		Int("arr_count", len(arrNames)).
		Int("media_id_filter_count", len(mediaIDs)).
		Int("workers", r.workers).
		Msg("Scanning Arr services for broken items")

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, min(r.workers, len(arrNames))))

	for _, arrName := range arrNames {
		g.Go(func() error {
			arrBroken, arrDiscovered, arrStats, err := r.scanArr(gctx, arrName, mediaIDs)
			if err != nil {
				return err
			}

			mu.Lock()
			if len(arrBroken) > 0 {
				brokenByArr[arrName] = append(brokenByArr[arrName], arrBroken...)
			}
			discovered = append(discovered, arrDiscovered...)
			mergeStats(&stats, arrStats)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, stats, err
	}

	for arrName, files := range brokenByArr {
		brokenByArr[arrName] = dedupeContentFiles(files)
	}
	r.logger.Debug().
		Int("arr_count", len(arrNames)).
		Int("broken_arrs", len(brokenByArr)).
		Int("discovered", stats.Discovered).
		Int("probed", stats.Probed).
		Int("broken", stats.Broken).
		Int("unknown", stats.Unknown).
		Msg("Arr scan finished")

	return brokenByArr, discovered, stats, nil
}

func (r *Repair) scanManagedEntries(ctx context.Context, scopeIDs []string) (map[string][]arr.ContentFile, []discoveredFile, storage.RepairStats, error) {
	brokenByScope := make(map[string][]arr.ContentFile)
	discovered := make([]discoveredFile, 0)
	stats := storage.RepairStats{}

	filters := make(map[string]struct{}, len(scopeIDs))
	for _, id := range scopeIDs {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			filters[trimmed] = struct{}{}
		}
	}

	items := make([]*storage.EntryItem, 0)
	if err := r.manager.Storage().ForEachEntryItem(func(item *storage.EntryItem) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if item == nil || len(item.Files) == 0 {
			return nil
		}
		if len(filters) > 0 && !entryItemMatchesScope(item, filters) {
			return nil
		}
		items = append(items, item)
		return nil
	}); err != nil {
		return nil, nil, stats, err
	}

	if len(items) == 0 {
		r.logger.Debug().
			Int("scope_filter_count", len(filters)).
			Msg("Managed-entry scan found no items to probe")
		return brokenByScope, discovered, stats, nil
	}

	r.logger.Debug().
		Int("entry_items", len(items)).
		Int("scope_filter_count", len(filters)).
		Int("workers", r.workers).
		Msg("Scanning managed entries for broken files")

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, min(r.workers, len(items))))

	for _, item := range items {
		item := item
		g.Go(func() error {
			itemBroken, itemDiscovered, itemStats := r.scanEntryItem(gctx, item)
			mu.Lock()
			if len(itemBroken) > 0 {
				brokenByScope[ScopeManagedEntries] = append(brokenByScope[ScopeManagedEntries], itemBroken...)
			}
			discovered = append(discovered, itemDiscovered...)
			mergeStats(&stats, itemStats)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, stats, err
	}

	if files, ok := brokenByScope[ScopeManagedEntries]; ok {
		brokenByScope[ScopeManagedEntries] = dedupeContentFiles(files)
	}

	r.logger.Debug().
		Int("entry_items", len(items)).
		Int("discovered", stats.Discovered).
		Int("probed", stats.Probed).
		Int("broken", stats.Broken).
		Int("unknown", stats.Unknown).
		Msg("Managed-entry scan completed")

	return brokenByScope, discovered, stats, nil
}

func (r *Repair) scanEntryItem(ctx context.Context, item *storage.EntryItem) ([]arr.ContentFile, []discoveredFile, storage.RepairStats) {
	stats := storage.RepairStats{}
	broken := make([]arr.ContentFile, 0)
	discovered := make([]discoveredFile, 0)

	if item == nil || len(item.Files) == 0 {
		return broken, discovered, stats
	}

	filenames := make([]string, 0, len(item.Files))
	fileByName := make(map[string]*storage.File, len(item.Files))
	for name, file := range item.Files {
		if file == nil || file.Deleted {
			continue
		}
		filenames = append(filenames, name)
		fileByName[name] = file
	}

	if len(filenames) == 0 {
		return broken, discovered, stats
	}

	stats.Discovered += len(filenames)
	results := r.manager.ProbeEntryFiles(ctx, item, filenames)
	stats.Probed += len(results)

	for _, result := range results {
		switch result.Status {
		case manager.FileProbeBroken:
			fileMeta := fileByName[result.Name]
			candidate := arr.ContentFile{
				Name:       result.Name,
				Path:       filepath.Join(item.Name, result.Name),
				TargetPath: result.Name,
				IsBroken:   true,
			}
			if fileMeta != nil {
				candidate.Size = fileMeta.Size
				if candidate.Path == "" && fileMeta.Path != "" {
					candidate.Path = fileMeta.Path
				}
			}
			broken = append(broken, candidate)
			discovered = append(discovered, discoveredFile{
				File:     candidate,
				InfoHash: result.InfoHash,
				Protocol: result.Protocol,
			})
			stats.Broken++
		case manager.FileProbeUnknown:
			stats.Unknown++
		}
	}

	return broken, discovered, stats
}

func (r *Repair) scanArr(ctx context.Context, arrName string, mediaIDs []string) ([]arr.ContentFile, []discoveredFile, storage.RepairStats, error) {
	stats := storage.RepairStats{}
	a := r.manager.Arr().Get(arrName)
	if a == nil {
		return nil, nil, stats, fmt.Errorf("arr %s not found", arrName)
	}

	media := make([]arr.Content, 0)
	if len(mediaIDs) == 0 {
		items, err := a.GetMedia("")
		if err != nil {
			return nil, nil, stats, err
		}
		media = append(media, items...)
	} else {
		for _, mediaID := range mediaIDs {
			items, err := a.GetMedia(mediaID)
			if err != nil {
				r.logger.Warn().Err(err).Str("arr", arrName).Str("media_id", mediaID).Msg("Skipping media during repair scan")
				continue
			}
			media = append(media, items...)
		}
	}

	if len(media) == 0 {
		r.logger.Trace().Str("arr", arrName).Msg("Arr scan returned no media")
		return nil, nil, stats, nil
	}

	broken := make([]arr.ContentFile, 0)
	discovered := make([]discoveredFile, 0)

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, min(r.workers, len(media))))

	for _, content := range media {
		g.Go(func() error {
			mediaBroken, mediaDiscovered, mediaStats := r.scanMedia(gctx, arrName, content)
			mu.Lock()
			broken = append(broken, mediaBroken...)
			discovered = append(discovered, mediaDiscovered...)
			mergeStats(&stats, mediaStats)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, stats, err
	}

	return dedupeContentFiles(broken), discovered, stats, nil
}

func (r *Repair) scanMedia(ctx context.Context, arrName string, media arr.Content) ([]arr.ContentFile, []discoveredFile, storage.RepairStats) {
	stats := storage.RepairStats{}
	broken := make([]arr.ContentFile, 0)
	discovered := make([]discoveredFile, 0)

	uniqueParents := collectFiles(media)
	for entryPath, files := range uniqueParents {
		select {
		case <-ctx.Done():
			return broken, discovered, stats
		default:
		}

		stats.Discovered += len(files)
		torrentName := filepath.Clean(filepath.Base(entryPath))
		entry, err := r.manager.GetEntryItem(torrentName)
		if err != nil {
			r.logger.Warn().Err(err).Str("arr", arrName).Str("entry_path", entryPath).Msg("Failed to get entry for discovered file; marking as unknown")
			stats.Unknown += len(files)
			continue
		}

		filePaths := make([]string, 0, len(files))
		fileByPath := make(map[string]arr.ContentFile, len(files))
		for _, file := range files {
			filePaths = append(filePaths, file.TargetPath)
			fileByPath[file.TargetPath] = file
		}

		probeResults := r.manager.ProbeEntryFiles(ctx, entry, filePaths)
		stats.Probed += len(probeResults)
		for _, result := range probeResults {
			switch result.Status {
			case manager.FileProbeBroken:
				candidate, ok := fileByPath[result.Name]
				if !ok {
					continue
				}
				candidate.IsBroken = true
				broken = append(broken, candidate)
				discovered = append(discovered, discoveredFile{
					ArrName:  arrName,
					File:     candidate,
					InfoHash: result.InfoHash,
					Protocol: result.Protocol,
				})
				stats.Broken++
			case manager.FileProbeUnknown:
				stats.Unknown++
			}
		}
	}
	if len(uniqueParents) > 0 {
		r.logger.Trace().
			Str("arr", arrName).
			Int("media_id", media.Id).
			Int("entries", len(uniqueParents)).
			Int("discovered", stats.Discovered).
			Int("probed", stats.Probed).
			Int("broken", stats.Broken).
			Int("unknown", stats.Unknown).
			Msg("Media scan completed")
	}

	return broken, discovered, stats
}

func (r *Repair) planActions(discovered []discoveredFile) []*storage.RepairAction {
	actions := make([]*storage.RepairAction, 0)
	torrentEntries := make(map[string]struct{})
	hasBroken := len(discovered) > 0
	hasArrBroken := false

	for _, item := range discovered {
		if item.Protocol == config.ProtocolTorrent && item.InfoHash != "" {
			torrentEntries[item.InfoHash] = struct{}{}
		}
		if item.ArrName != "" {
			hasArrBroken = true
		}
	}

	for infohash := range torrentEntries {
		actions = append(actions, &storage.RepairAction{
			ID:       uuid.NewString(),
			Type:     actionReinsertTorrent,
			EntryID:  infohash,
			Protocol: config.ProtocolTorrent,
			Status:   storage.RepairActionPlanned,
		})
	}

	if hasBroken && hasArrBroken {
		actions = append(actions, &storage.RepairAction{
			ID:       uuid.NewString(),
			Type:     actionReacquireArr,
			Protocol: config.ProtocolAll,
			Status:   storage.RepairActionPlanned,
		})
	}

	sort.Slice(actions, func(i, j int) bool {
		return actions[i].Type < actions[j].Type
	})
	return actions
}

func (r *Repair) getArrs(arrNames []string) []string {
	arrs := make([]string, 0)
	if len(arrNames) == 0 {
		for _, a := range r.manager.Arr().GetAll() {
			if a.SkipRepair {
				continue
			}
			arrs = append(arrs, a.Name)
		}
	} else {
		for _, name := range arrNames {
			a := r.manager.Arr().Get(name)
			if a == nil || a.Host == "" || a.Token == "" || a.SkipRepair {
				continue
			}
			arrs = append(arrs, a.Name)
		}
	}
	return arrs
}

func (r *Repair) findActiveJobByKey(key string) *storage.Job {
	if key == "" {
		return nil
	}
	jobs := r.GetJobs()
	for _, job := range jobs {
		if job == nil {
			continue
		}
		if job.UniqueKey != key {
			continue
		}
		switch job.Status {
		case storage.JobStarted, storage.JobProcessing, storage.JobPending:
			return job
		}
	}
	return nil
}

func (r *Repair) preRunChecks() error {
	if r.manager == nil {
		return fmt.Errorf("manager not initialized")
	}
	if r.manager.Storage() == nil {
		return fmt.Errorf("storage not initialized")
	}
	return nil
}

func (r *Repair) saveToStorage(job *storage.Job) {
	if job == nil {
		return
	}
	if err := r.manager.Storage().SaveRepairJob(job.UniqueKey, job); err != nil {
		r.logger.Error().Err(err).Msgf("Failed to save repair job %s", job.ID)
	}
}

func (r *Repair) handleRunError(ctx context.Context, job *storage.Job, err error) {
	if err == nil {
		return
	}

	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		job.Status = storage.JobCancelled
		job.Stage = storage.JobStageCancelled
		job.CompletedAt = time.Now()
		job.Error = "job was cancelled"
		r.saveToStorage(job)
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairCancelled,
			Status:  "cancelled",
			Message: job.DiscordContext(),
		})
		r.logger.Info().
			Str("job_id", job.ID).
			Str("stage", string(job.Stage)).
			Msg("Repair job cancelled")
		return
	}

	job.Status = storage.JobFailed
	job.Stage = storage.JobStageFailed
	job.FailedAt = time.Now()
	job.CompletedAt = time.Now()
	job.Error = err.Error()
	r.saveToStorage(job)

	r.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventRepairFailed,
		Status:  "error",
		Message: job.DiscordContext(),
		Error:   err,
	})
	r.logger.Error().
		Err(err).
		Str("job_id", job.ID).
		Str("stage", string(job.Stage)).
		Msg("Repair job failed")
}

func entryItemMatchesScope(item *storage.EntryItem, filters map[string]struct{}) bool {
	if item == nil || len(filters) == 0 {
		return true
	}
	if _, ok := filters[item.Name]; ok {
		return true
	}
	for _, f := range item.Files {
		if f == nil {
			continue
		}
		if _, ok := filters[f.InfoHash]; ok {
			return true
		}
		if _, ok := filters[f.Name]; ok {
			return true
		}
	}
	return false
}

func dedupeContentFiles(files []arr.ContentFile) []arr.ContentFile {
	if len(files) <= 1 {
		return files
	}

	unique := make(map[string]arr.ContentFile, len(files))
	for _, file := range files {
		key := file.Path
		if file.TargetPath != "" {
			key = file.TargetPath
		}
		if key == "" {
			key = fmt.Sprintf("%d:%d:%s", file.Id, file.FileId, file.Name)
		}
		unique[key] = file
	}

	out := make([]arr.ContentFile, 0, len(unique))
	for _, file := range unique {
		out = append(out, file)
	}
	return out
}

func countBrokenItems(broken map[string][]arr.ContentFile) int {
	total := 0
	for _, items := range broken {
		total += len(items)
	}
	return total
}

func mergeStats(dst *storage.RepairStats, src storage.RepairStats) {
	dst.Discovered += src.Discovered
	dst.Probed += src.Probed
	dst.Broken += src.Broken
	dst.Planned += src.Planned
	dst.Executed += src.Executed
	dst.Fixed += src.Fixed
	dst.Failed += src.Failed
	dst.Unknown += src.Unknown
}
