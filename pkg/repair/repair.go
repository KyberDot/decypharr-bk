package repair

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/errgroup"
)

type contexts struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type Repair struct {
	manager     *manager.Manager
	interval    string
	autoProcess bool
	logger      zerolog.Logger
	workers     int
	scheduler   gocron.Scheduler

	debridPathCache sync.Map // debridPath:debridName cache.Emptied after each run
	ctx             context.Context

	activeContexts *xsync.Map[string, contexts] // jobID:context
}

func New(mgr *manager.Manager) *Repair {
	cfg := config.Get()
	workers := runtime.NumCPU() * 20
	if cfg.Repair.Workers > 0 {
		workers = cfg.Repair.Workers
	}

	r := &Repair{
		logger:         logger.New("repair"),
		interval:       cfg.Repair.Interval,
		autoProcess:    cfg.Repair.AutoProcess,
		manager:        mgr,
		workers:        workers,
		ctx:            context.Background(),
		activeContexts: xsync.NewMap[string, contexts](),
	}

	return r
}

func (r *Repair) Reset() {
	// Stop scheduler
	if r.scheduler != nil {
		if err := r.scheduler.Shutdown(); err != nil {
			r.logger.Error().Err(err).Msg("Error shutting down scheduler")
		}
	}

}

func (r *Repair) Start(ctx context.Context) error {

	r.scheduler, _ = gocron.NewScheduler(gocron.WithLocation(time.Local))

	if jd, err := utils.ConvertToJobDef(r.interval); err != nil {
		r.logger.Error().Err(err).Str("interval", r.interval).Msg("Error converting interval")
	} else {
		_, err2 := r.scheduler.NewJob(jd, gocron.NewTask(func() {
			r.logger.Info().Msgf("Repair job started at %s", time.Now().Format("15:04:05"))
			if err := r.AddJob([]string{}, []string{}, r.autoProcess, true); err != nil {
				r.logger.Error().Err(err).Msg("Error running repair job")
			}
		}))
		if err2 != nil {
			r.logger.Error().Err(err2).Msg("Error creating repair job")
		} else {
			r.scheduler.Start()
			r.logger.Info().Msgf("Repair job scheduled every %s", r.interval)
		}
	}

	<-ctx.Done()

	r.logger.Info().Msg("Stopping repair scheduler")
	r.Reset()

	return nil
}

func (r *Repair) getArrs(arrNames []string) []string {
	arrs := make([]string, 0)
	if len(arrNames) == 0 {
		// No specific arrs, get all
		// Also check if any arrs are set to skip repair
		_arrs := r.manager.Arr().GetAll()
		for _, a := range _arrs {
			if a.SkipRepair {
				continue
			}
			arrs = append(arrs, a.Name)
		}
	} else {
		for _, name := range arrNames {
			a := r.manager.Arr().Get(name)
			if a == nil || a.Host == "" || a.Token == "" {
				continue
			}
			arrs = append(arrs, a.Name)
		}
	}

	return arrs
}

func jobKey(arrNames []string, mediaIDs []string) string {
	return fmt.Sprintf("%s-%s", strings.Join(arrNames, ","), strings.Join(mediaIDs, ","))
}

func (r *Repair) reset(j *storage.Job) {
	// Update job for rerun
	j.Status = storage.JobStarted
	j.StartedAt = time.Now()
	j.CompletedAt = time.Time{}
	j.FailedAt = time.Time{}
	j.BrokenItems = nil
	j.Error = ""
	if j.Recurrent || j.Arrs == nil {
		j.Arrs = r.getArrs([]string{}) // GetReader new arrs
	}
}

func (r *Repair) newJob(arrsNames []string, mediaIDs []string) *storage.Job {
	arrs := r.getArrs(arrsNames)
	return &storage.Job{
		ID:        uuid.New().String(),
		Arrs:      arrs,
		MediaIDs:  mediaIDs,
		StartedAt: time.Now(),
		Status:    storage.JobStarted,
	}
}

// initRun initializes the repair run, setting up necessary configurations, checks and caches
func (r *Repair) initRun(ctx context.Context) {
}

// // onComplete is called when the repair job is completed
func (r *Repair) onComplete() {
	// Set the cache maps to nil
	r.debridPathCache = sync.Map{}
}

func (r *Repair) preRunChecks() error {
	if r.manager == nil {
		return fmt.Errorf("manager not initialized")
	}
	return nil
}

func (r *Repair) AddJob(arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) error {
	key := jobKey(arrsNames, mediaIDs)

	// Check for existing running job
	job := r.manager.Storage().GetRepairJobByUniqueKey(key)
	if job != nil && job.Status == storage.JobStarted {
		return fmt.Errorf("job already running")
	}
	job = r.newJob(arrsNames, mediaIDs)
	if job == nil {
		return fmt.Errorf("failed to create job")
	}
	job.AutoProcess = autoProcess
	job.Recurrent = recurrent
	job.ID = cmp.Or(job.ID, uuid.New().String())
	r.reset(job)

	ctx, cancelFunc := context.WithCancel(r.ctx)

	r.activeContexts.Store(job.ID, contexts{
		ctx:    ctx,
		cancel: cancelFunc,
	})

	// Save job
	if err := r.manager.Storage().SaveRepairJob(key, job); err != nil {
		return err
	}

	go func() {
		if err := r.repair(ctx, job); err != nil {
			r.logger.Error().Err(err).Msg("Error running repair")
			if !errors.Is(ctx.Err(), context.Canceled) {
				job.FailedAt = time.Now()
				job.Error = err.Error()
				job.Status = storage.JobFailed
				job.CompletedAt = time.Now()
			} else {
				job.FailedAt = time.Now()
				job.Error = err.Error()
				job.Status = storage.JobFailed
				job.CompletedAt = time.Now()
			}
		}
		r.onComplete() // Clear caches and maps after job completion
	}()
	return nil
}

func (r *Repair) StopJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}

	// Check if job can be stopped
	if job.Status != storage.JobStarted && job.Status != storage.JobProcessing {
		return fmt.Errorf("job %s cannot be stopped (status: %s)", id, job.Status)
	}
	ctx, ok := r.activeContexts.Load(id)
	if !ok {
		return fmt.Errorf("job %s context not found", id)
	}

	// Cancel the job
	if ctx.cancel != nil {
		ctx.cancel()
		r.logger.Info().Msgf("Job %s cancellation requested", id)
		go func() {
			if job.Status == storage.JobStarted || job.Status == storage.JobProcessing {
				job.Status = storage.JobCancelled
				job.BrokenItems = nil
				// Clear active context
				r.activeContexts.Delete(id)
				job.CompletedAt = time.Now()
				job.Error = "Job was cancelled by user"
				r.saveToStorage(job)
			}
		}()

		return nil
	}

	return fmt.Errorf("job %s cannot be cancelled", id)
}

func (r *Repair) saveToStorage(job *storage.Job) {
	if job == nil {
		return
	}
	if err := r.manager.Storage().SaveRepairJob(jobKey(job.Arrs, job.MediaIDs), job); err != nil {
		r.logger.Error().Err(err).Msgf("Failed to save job %s to storage", job.ID)
	}
}

func (r *Repair) repair(ctx context.Context, job *storage.Job) error {
	defer r.saveToStorage(job)
	if err := r.preRunChecks(); err != nil {
		return err
	}

	// Initialize the run
	r.initRun(ctx)

	// Use a mutex to protect concurrent access to brokenItems
	var mu sync.Mutex
	brokenItems := map[string][]arr.ContentFile{}
	g, ctx := errgroup.WithContext(ctx)

	for _, a := range job.Arrs {
		a := a // Capture range variable
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var items []arr.ContentFile
			var err error

			if len(job.MediaIDs) == 0 {
				items, err = r.repairArr(ctx, job, a, "")
				if err != nil {
					r.logger.Error().Err(err).Msgf("Error repairing %s", a)
					return err
				}
			} else {
				for _, id := range job.MediaIDs {
					someItems, err := r.repairArr(ctx, job, a, id)
					if err != nil {
						r.logger.Error().Err(err).Msgf("Error repairing %s with ID %s", a, id)
						return err
					}
					items = append(items, someItems...)
				}
			}

			// Safely append the found items to the shared slice
			if len(items) > 0 {
				mu.Lock()
				brokenItems[a] = items
				mu.Unlock()
			}

			return nil
		})
	}

	// Wait for all goroutines to complete and check for errors
	if err := g.Wait(); err != nil {
		// Check if j0b was canceled
		if errors.Is(ctx.Err(), context.Canceled) {
			job.Status = storage.JobCancelled
			job.CompletedAt = time.Now()
			job.Error = "Job was cancelled"
			return fmt.Errorf("job cancelled")
		}

		job.FailedAt = time.Now()
		job.Error = err.Error()
		job.Status = storage.JobFailed
		job.CompletedAt = time.Now()
		go func() {
			if err := r.manager.SendDiscordMessage("repair_failed", "error", job.DiscordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
		return err
	}

	if len(brokenItems) == 0 {
		job.CompletedAt = time.Now()
		job.Status = storage.JobCompleted

		go func() {
			if err := r.manager.SendDiscordMessage("repair_complete", "success", job.DiscordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()

		return nil
	}

	job.BrokenItems = brokenItems
	if job.AutoProcess {
		// Job is already processed
		job.CompletedAt = time.Now() // Mark as completed
		job.Status = storage.JobCompleted
		go func() {
			if err := r.manager.SendDiscordMessage("repair_complete", "success", job.DiscordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
	} else {
		job.Status = storage.JobPending
		go func() {
			if err := r.manager.SendDiscordMessage("repair_pending", "pending", job.DiscordContext()); err != nil {
				r.logger.Error().Msgf("Error sending discord message: %v", err)
			}
		}()
	}
	return nil
}

func (r *Repair) repairArr(ctx context.Context, job *storage.Job, _arr string, tmdbId string) ([]arr.ContentFile, error) {
	brokenItems := make([]arr.ContentFile, 0)
	a := r.manager.Arr().Get(_arr)

	r.logger.Info().Msgf("Starting repair for %s", a.Name)
	media, err := a.GetMedia(tmdbId)
	if err != nil {
		r.logger.Info().Msgf("Failed to get %s media: %v", a.Name, err)
		return brokenItems, err
	}
	r.logger.Info().Msgf("Found %d %s media", len(media), a.Name)

	if len(media) == 0 {
		r.logger.Info().Msgf("No %s media found", a.Name)
		return brokenItems, nil
	}
	// Check first media to confirm mounts are accessible
	if err := r.checkMountUp(media); err != nil {
		r.logger.Error().Err(err).Msgf("Mount check failed for %s", a.Name)
		return brokenItems, fmt.Errorf("mount check failed: %w", err)
	}

	// Mutex for brokenItems
	var mu sync.Mutex
	var wg sync.WaitGroup
	workerChan := make(chan arr.Content, min(len(media), r.workers))

	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range workerChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				items := r.getBrokenFiles(ctx, m)
				if items != nil {
					r.logger.Debug().Msgf("Found %d broken files for %s", len(items), m.Title)
					if job.AutoProcess {
						r.logger.Info().Msgf("Auto processing %d broken items for %s", len(items), m.Title)

						// Delete broken items
						if err := a.DeleteFiles(items); err != nil {
							r.logger.Debug().Msgf("Failed to delete broken items for %s: %v", m.Title, err)
						}

						// Search for missing items
						if err := a.SearchMissing(items); err != nil {
							r.logger.Debug().Msgf("Failed to search missing items for %s: %v", m.Title, err)
						}
					}

					mu.Lock()
					brokenItems = append(brokenItems, items...)
					mu.Unlock()
				}
			}
		}()
	}

	go func() {
		defer close(workerChan)
		for _, m := range media {
			select {
			case <-ctx.Done():
				return
			case workerChan <- m:
			}
		}
	}()

	wg.Wait()
	if len(brokenItems) == 0 {
		r.logger.Info().Msgf("No broken items found for %s", a.Name)
		return brokenItems, nil
	}

	r.logger.Info().Msgf("Repair completed for %s. %d broken items found", a.Name, len(brokenItems))
	return brokenItems, nil
}

// checkMountUp checks if the mounts are accessible
func (r *Repair) checkMountUp(media []arr.Content) error {
	firstMedia := media[0]
	for _, m := range media {
		if len(m.Files) > 0 {
			firstMedia = m
			break
		}
	}
	files := firstMedia.Files
	if len(files) == 0 {
		return fmt.Errorf("no files found in media %s", firstMedia.Title)
	}
	for _, file := range files {
		if _, err := os.Stat(file.Path); os.IsNotExist(err) {
			// If the file does not exist, we can't check the symlink target
			r.logger.Debug().Msgf("File %s does not exist, skipping repair", file.Path)
			return fmt.Errorf("file %s does not exist, skipping repair", file.Path)
		}
		// GetReader the symlink target
		symlinkPath := getSymlinkTarget(file.Path)
		if symlinkPath != "" {
			r.logger.Trace().Msgf("Found symlink target for %s: %s", file.Path, symlinkPath)
			if _, err := os.Stat(symlinkPath); os.IsNotExist(err) {
				r.logger.Debug().Msgf("Symlink target %s does not exist, skipping repair", symlinkPath)
				return fmt.Errorf("symlink target %s does not exist for %s. skipping repair", symlinkPath, file.Path)
			}
		}
	}
	return nil
}

func (r *Repair) getBrokenFiles(ctx context.Context, media arr.Content) []arr.ContentFile {
	if r.manager == nil {
		r.logger.Info().Msg("No manager found. Can't check broken files")
		return nil
	}

	// GetReader debrid clients from somewhere - this might need to be passed in or obtained from a global registry
	// For now, we'll use an empty map and the function will skip files it can't find
	clients := make(map[string]common.Client)

	brokenFiles := make([]arr.ContentFile, 0)
	uniqueParents := collectFiles(media)
	for torrentPath, files := range uniqueParents {
		select {
		case <-ctx.Done():
			return brokenFiles
		default:
		}
		brokenFilesForTorrent := r.checkTorrentFiles(torrentPath, files, clients, r.manager)
		if len(brokenFilesForTorrent) > 0 {
			brokenFiles = append(brokenFiles, brokenFilesForTorrent...)
		}
	}
	if len(brokenFiles) == 0 {
		return nil
	}
	r.logger.Debug().Msgf("%d broken files found for %s", len(brokenFiles), media.Title)
	return brokenFiles
}

func (r *Repair) GetJob(id string) *storage.Job {
	job, err := r.manager.Storage().GetRepairJob(id)
	if err != nil {
		r.logger.Error().Err(err).Msgf("Failed to get job %s", id)
		return nil
	}
	return job
}

func (r *Repair) GetJobs() []*storage.Job {
	jobs, err := r.manager.Storage().LoadAllRepairJobs()
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to load repair jobs")
		return []*storage.Job{}
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
		return fmt.Errorf("job %s not pending", id)
	}
	if job.StartedAt.IsZero() {
		return fmt.Errorf("job %s not started", id)
	}
	if !job.CompletedAt.IsZero() {
		return fmt.Errorf("job %s already completed", id)
	}
	if !job.FailedAt.IsZero() {
		return fmt.Errorf("job %s already failed", id)
	}

	brokenItems := job.BrokenItems
	if len(brokenItems) == 0 {
		r.logger.Info().Msgf("No broken items found for job %s", id)
		job.CompletedAt = time.Now()
		job.Status = storage.JobCompleted
		return nil
	}

	ctxObj, ok := r.activeContexts.Load(id)
	if !ok {
		c, cancel := context.WithCancel(r.ctx)
		ctxObj = contexts{
			ctx:    c,
			cancel: cancel,
		}
		r.activeContexts.Store(id, ctxObj)
	}

	g, ctx := errgroup.WithContext(ctxObj.ctx)
	g.SetLimit(r.workers)

	for arrName, items := range brokenItems {
		items := items
		arrName := arrName
		g.Go(func() error {

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			a := r.manager.Arr().Get(arrName)
			if a == nil {
				r.logger.Error().Msgf("Arr %s not found", arrName)
				return nil
			}

			if err := a.DeleteFiles(items); err != nil {
				r.logger.Error().Err(err).Msgf("Failed to delete broken items for %s", arrName)
				return nil
			}
			// Search for missing items
			if err := a.SearchMissing(items); err != nil {
				r.logger.Error().Err(err).Msgf("Failed to search missing items for %s", arrName)
				return nil
			}
			return nil
		})
	}

	// Update job status to in-progress
	job.Status = storage.JobProcessing
	r.saveToStorage(job)

	// Launch a goroutine to wait for completion and update the job
	go func() {
		if err := g.Wait(); err != nil {
			job.FailedAt = time.Now()
			job.Error = err.Error()
			job.CompletedAt = time.Now()
			job.Status = storage.JobFailed
			r.logger.Error().Err(err).Msgf("Job %s failed", id)
		} else {
			job.CompletedAt = time.Now()
			job.Status = storage.JobCompleted
			r.logger.Info().Msgf("Job %s completed successfully", id)
		}

		r.saveToStorage(job)
	}()

	return nil
}

func (r *Repair) DeleteJobs(ids []string) {
	for _, id := range ids {
		if id == "" {
			continue
		}
		err := r.manager.Storage().DeleteRepairJob(id)
		if err != nil {
			r.logger.Error().Err(err).Msgf("Failed to delete job %s", id)
		}
	}
}

// Cleanup Cleans up the repair instance
func (r *Repair) Cleanup() {
	r.manager = nil
	r.ctx = nil
	r.logger.Info().Msg("Repair stopped")
}
