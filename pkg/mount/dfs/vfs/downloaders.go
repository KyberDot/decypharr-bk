package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/manager"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

const (
	// maxDownloaderIdleTime is how long a downloader waits before stopping
	maxDownloaderIdleTime = 5 * time.Second
	// maxSkipBytes is how far a downloader will skip before restarting
	maxSkipBytes = 1 << 20 // 1MB
	// maxErrorCount is the number of errors before giving up
	maxErrorCount = 10
	// downloaderWindow is how close a read must be to reuse a downloader
	downloaderWindow = 4 * 1024 * 1024 // 4MB
	// kickerInterval is how often to check waiters
	kickerInterval = 5 * time.Second
	// kickDebounce is the max delay before notifying waiters after new data arrives
	kickDebounce = 20 * time.Millisecond
	// idleTimeout is how long before stopping all downloaders due to inactivity
	idleTimeout = 30 * time.Second
	// circuitCooldownDuration is how long to block requests after max errors reached
	circuitCooldownDuration = 20 * time.Minute
	// maxChunkDoublings bounds sequential chunk growth (2^6 = 64x)
	maxChunkDoublings = 6

	// sequentialThreshold is how many consecutive sequential reads
	// before enabling aggressive read-ahead
	sequentialThreshold = 3
	// sequentialGap is the max gap between reads to still be considered sequential
	sequentialGap = 64 * 1024 // 64KB (kernel read-ahead alignment)
	// minReadForPrefetch - don't prefetch for tiny reads (likely probing)
	minReadForPrefetch = 64 * 1024 // 64KB
)

// accessPattern tracks read patterns to detect sequential vs random access
type accessPattern struct {
	mu              sync.Mutex
	lastOffset      int64
	lastSize        int64
	sequentialCount int
	confirmed       bool
}

func (ap *accessPattern) record(offset, size int64) {
	ap.mu.Lock()
	defer ap.mu.Unlock()

	if ap.confirmed {
		return // Already confirmed, no need to track
	}

	if ap.lastSize > 0 {
		expected := ap.lastOffset + ap.lastSize
		// Sequential: current read starts at or near where last read ended
		if offset >= ap.lastOffset && offset <= expected+sequentialGap {
			ap.sequentialCount++
			if ap.sequentialCount >= sequentialThreshold {
				ap.confirmed = true
			}
		} else {
			// Non-sequential (backward seek or large jump)
			ap.sequentialCount = 0
		}
	}

	ap.lastOffset = offset
	ap.lastSize = size
}

func (ap *accessPattern) isSequential() bool {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	return ap.confirmed
}

// Downloaders coordinates multiple concurrent downloads to a cache item
type Downloaders struct {
	ctx           context.Context
	cancel        context.CancelFunc
	item          *CacheItem
	manager       *manager.Manager
	chunkSize     int64
	readAheadSize int64
	retries       int

	mu         sync.Mutex
	dls        []*downloader
	waiters    []waiter
	errorCount int
	lastErr    error
	closed     bool
	wg         sync.WaitGroup

	pattern accessPattern

	// streamID is the active stream registration ID for tracking
	streamID string

	// Idle timeout tracking
	lastActivity atomic.Int64  // Unix nano timestamp of last download activity
	idle         atomic.Bool   // True when all downloaders stopped due to idle
	kickerDone   chan struct{} // Signals kicker goroutine has exited
	kickCh       chan struct{} // Non-blocking kick signals from writers

	// Circuit breaker - blocks all requests when max errors reached
	circuitOpen   atomic.Bool  // True when circuit is "open" (blocking all requests)
	circuitOpenAt atomic.Int64 // Unix nano timestamp when circuit opened
}

// ensureStreamTracked makes sure the active stream is registered when reads begin.
func (dls *Downloaders) ensureStreamTracked() {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	if dls.closed || dls.streamID != "" {
		return
	}

	dls.streamID = dls.manager.TrackStream(dls.item.entry, dls.item.filename, "DFS")
}

// untrackStreamLocked removes the stream registration. Caller must hold dls.mu.
func (dls *Downloaders) untrackStreamLocked() {
	if dls.streamID == "" {
		return
	}
	dls.manager.UntrackStream(dls.streamID)
	dls.streamID = ""
}

// waiter represents a caller waiting for a range to be downloaded
type waiter struct {
	r       ranges.Range
	errChan chan<- error
}

// downloader represents a single download goroutine
type downloader struct {
	dls  *Downloaders
	quit chan struct{}
	kick chan struct{}

	mu        sync.Mutex
	start     int64 // Starting offset
	offset    int64 // Current offset
	maxOffset int64 // How far to download
	skipped   int64 // Consecutive skipped bytes
	stopped   bool
	closed    bool

	baseChunkSize    int64
	currentChunkSize int64
	maxChunkSize     int64

	wg sync.WaitGroup

	idleTimer *time.Timer
}

// NewDownloaders creates a new download coordinator
func NewDownloaders(ctx context.Context, mgr *manager.Manager, item *CacheItem, cfg *fuseconfig.FuseConfig) *Downloaders {
	ctx, cancel := context.WithCancel(ctx)
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024
	}
	readAheadSize := cfg.ReadAheadSize
	if readAheadSize <= 0 {
		readAheadSize = chunkSize * 4 // Default: 4 chunks ahead
	}
	retries := cfg.Retries
	if retries <= 0 {
		retries = 3
	}

	dls := &Downloaders{
		ctx:           ctx,
		cancel:        cancel,
		item:          item,
		manager:       mgr,
		chunkSize:     chunkSize,
		readAheadSize: readAheadSize,
		retries:       retries,
		// streamID is populated lazily when the first read occurs.
		streamID: "",
		kickCh:   make(chan struct{}, 1),
	}
	dls.touchActivity() // Initialize activity timestamp

	// Background kicker to handle stalled waiters and idle detection
	dls.startKicker()

	return dls
}

// Download blocks until the range r is on disk
func (dls *Downloaders) Download(r ranges.Range) error {
	// Circuit breaker: reject immediately if circuit is open
	if dls.isCircuitOpen() {
		GlobalVFSStats.CircuitRejects.Add(1)
		return fmt.Errorf("circuit breaker open, cooldown active: last error: %w", dls.lastErr)
	}

	dls.ensureStreamTracked()

	// Update activity timestamp for idle detection
	dls.touchActivity()

	// Lazy restart: if we went idle, restart the kicker goroutine
	if dls.idle.Load() {
		dls.idle.Store(false)
		dls.ensureKickerRunning()
	}

	// Record access pattern before taking lock
	dls.pattern.record(r.Pos, r.Size)

	dls.mu.Lock()
	if dls.closed {
		dls.mu.Unlock()
		return errors.New("downloaders closed")
	}

	// Fast path: already have it
	if dls.item.HasRange(r) {
		GlobalVFSStats.CacheHits.Add(1)
		dls.mu.Unlock()
		return nil
	}

	GlobalVFSStats.CacheMisses.Add(1)

	// Create waiter channel
	errChan := make(chan error, 1)
	dls.waiters = append(dls.waiters, waiter{r: r, errChan: errChan})

	// Ensure downloader running
	if err := dls.ensureDownloaderLocked(r); err != nil {
		// Remove our waiter on error
		dls.removeWaiterLocked(errChan)
		dls.mu.Unlock()
		return err
	}

	dls.mu.Unlock()

	// Block until range is fulfilled or error
	return <-errChan
}

// requestKick signals the kicker goroutine to notify waiters (non-blocking).
func (dls *Downloaders) requestKick() {
	if dls == nil {
		return
	}
	select {
	case dls.kickCh <- struct{}{}:
	default:
	}
}

// removeWaiterLocked removes a waiter by its channel (call with lock held)
func (dls *Downloaders) removeWaiterLocked(errChan chan<- error) {
	for i, w := range dls.waiters {
		if w.errChan == errChan {
			dls.waiters = append(dls.waiters[:i], dls.waiters[i+1:]...)
			return
		}
	}
}

// ensureDownloaderLocked finds or creates a downloader for the range
func (dls *Downloaders) ensureDownloaderLocked(r ranges.Range) error {
	requested := r
	// Clip to what's missing
	r = dls.item.FindMissing(r)
	if r.Size <= 0 {
		dls.extendSequentialTargetLocked(requested)
		return nil // Nothing to download
	}
	targetEnd := dls.initialEnd(r)

	// Check error count
	if dls.errorCount >= maxErrorCount {
		return fmt.Errorf("too many errors (%d): last error: %w", dls.errorCount, dls.lastErr)
	}

	// Look for existing downloader in range
	dls.removeClosed()
	window := int64(downloaderWindow)
	if half := dls.chunkSize / 2; half > window {
		window = half
	}
	for _, dl := range dls.dls {
		start, offset := dl.getRange()
		if r.Pos >= start && r.Pos < offset+window {
			// Extend existing downloader
			GlobalVFSStats.DownloadersReused.Add(1)
			sequentialTarget := dls.sequentialEnd(requested)
			dl.setMaxOffset(sequentialTarget)
			return nil
		}
	}

	// Start new downloader
	GlobalVFSStats.DownloadersCreated.Add(1)
	return dls.newDownloaderLocked(r, targetEnd)
}

// newDownloaderLocked creates and starts a new downloader
func (dls *Downloaders) newDownloaderLocked(r ranges.Range, targetEnd int64) error {
	baseChunk := dls.chunkSize
	if baseChunk <= 0 {
		baseChunk = 4 * 1024 * 1024
	}

	dl := &downloader{
		dls:              dls,
		quit:             make(chan struct{}),
		kick:             make(chan struct{}, 1),
		start:            r.Pos,
		offset:           r.Pos,
		maxOffset:        targetEnd,
		baseChunkSize:    baseChunk,
		currentChunkSize: baseChunk,
		maxChunkSize:     maxChunkSizeFor(baseChunk),
	}

	dls.dls = append(dls.dls, dl)

	dl.wg.Add(1)
	go func() {
		defer dl.wg.Done()
		n, err := dl.run()
		dl.close(err)
		dls.countErrors(n, err)
		dls.requestKick()
	}()

	return nil
}

// removeClosed removes closed downloaders from the list
func (dls *Downloaders) removeClosed() {
	newDls := dls.dls[:0]
	for _, dl := range dls.dls {
		if !dl.isClosed() {
			newDls = append(newDls, dl)
		}
	}
	dls.dls = newDls
}

// countErrors tracks errors and resets on success
func (dls *Downloaders) countErrors(n int64, err error) {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	if err == nil && n > 0 {
		dls.errorCount = 0
		dls.lastErr = nil
		// Success resets circuit breaker
		dls.resetCircuitLocked()
		return
	}
	if err != nil {
		dls.errorCount++
		dls.lastErr = err
		if !customerror.IsSilentError(err) {
			dls.item.logger.Debug().Err(err).Int("count", dls.errorCount).Msg("download error")
		}
		if !customerror.IsRetriableError(err) {
			dls.errorCount = maxErrorCount
		}
		// Trip circuit breaker when max errors reached
		if dls.errorCount >= maxErrorCount {
			dls.openCircuitLocked()
		}
	}
}

// kickWaiters checks all waiters and fulfills completed ones
func (dls *Downloaders) kickWaiters() {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	if len(dls.waiters) == 0 {
		return
	}

	// Check circuit state once to avoid spinning
	circuitOpen := dls.circuitOpen.Load()

	remaining := dls.waiters[:0]
	for _, w := range dls.waiters {
		// Clip range to actual file size
		r := w.r
		r.Clip(dls.item.info.Size)

		if dls.item.HasRange(r) {
			w.errChan <- nil // Fulfilled!
		} else if circuitOpen || dls.errorCount >= maxErrorCount {
			// Circuit is open or max errors reached - fail waiter without creating new downloaders
			w.errChan <- dls.lastErr
		} else {
			remaining = append(remaining, w)
			// Ensure there's a downloader for this waiter
			_ = dls.ensureDownloaderLocked(w.r)
		}
	}
	dls.waiters = remaining
}

func (dls *Downloaders) initialEnd(r ranges.Range) int64 {
	// New downloaders always start conservative - just one chunk
	chunk := dls.chunkSize
	if chunk <= 0 {
		chunk = 4 * 1024 * 1024
	}
	target := r.Pos + chunk
	if target > dls.item.info.Size {
		target = dls.item.info.Size
	}
	return target
}

func (dls *Downloaders) sequentialEnd(r ranges.Range) int64 {
	chunk := dls.chunkSize
	if chunk <= 0 {
		chunk = 4 * 1024 * 1024
	}

	// Only extend with read-ahead if:
	// 1. Sequential access pattern is confirmed
	// 2. Read size suggests streaming (not probing)
	shouldPrefetch := dls.pattern.isSequential() && r.Size >= minReadForPrefetch

	var ahead int64
	if shouldPrefetch {
		ahead = dls.readAheadSize
		if ahead <= 0 {
			ahead = chunk * 4 // Default: 4 chunks ahead
		}
	} else {
		ahead = chunk // Conservative: just one chunk
	}

	target := r.Pos + ahead
	if target > dls.item.info.Size {
		target = dls.item.info.Size
	}
	return target
}

func (dls *Downloaders) extendSequentialTargetLocked(r ranges.Range) {
	window := int64(downloaderWindow)
	if half := dls.chunkSize / 2; half > window {
		window = half
	}
	for _, dl := range dls.dls {
		start, offset := dl.getRange()
		if r.Pos >= start && r.Pos < offset+window {
			target := dls.sequentialEnd(r)
			dl.setMaxOffset(target)
			return
		}
	}
}

func maxChunkSizeFor(base int64) int64 {
	if base <= 0 {
		return base
	}

	maxChunk := base
	for i := 0; i < maxChunkDoublings; i++ {
		if maxChunk > math.MaxInt64/2 {
			return math.MaxInt64
		}
		maxChunk *= 2
	}
	return maxChunk
}

// Close stops all downloaders and returns unfulfilled waiters with error
func (dls *Downloaders) Close(inErr error) error {
	dls.mu.Lock()
	dls.closed = true
	dls.untrackStreamLocked()

	// Stop all downloaders
	for _, dl := range dls.dls {
		dl.stop()
	}
	dls.mu.Unlock()

	// Wait for downloaders to finish
	for _, dl := range dls.dls {
		dl.wg.Wait()
	}

	dls.cancel()
	dls.wg.Wait()

	// Close remaining waiters
	dls.mu.Lock()
	for _, w := range dls.waiters {
		if inErr != nil {
			w.errChan <- inErr
		} else {
			w.errChan <- errors.New("downloaders closed")
		}
	}
	dls.waiters = nil
	dls.dls = nil
	dls.mu.Unlock()

	return nil
}

// touchActivity updates the last activity timestamp
func (dls *Downloaders) touchActivity() {
	dls.lastActivity.Store(time.Now().UnixNano())
}

// isCircuitOpen returns true if the circuit breaker is open and cooldown hasn't expired
func (dls *Downloaders) isCircuitOpen() bool {
	if !dls.circuitOpen.Load() {
		return false
	}
	// Check if cooldown has expired using raw nanoseconds (no allocation)
	openedAt := dls.circuitOpenAt.Load()
	if openedAt == 0 {
		return false
	}
	if time.Now().UnixNano()-openedAt >= int64(circuitCooldownDuration) {
		// Cooldown expired - reset circuit and clear error budget
		dls.mu.Lock()
		openedAt = dls.circuitOpenAt.Load()
		if openedAt != 0 && time.Now().UnixNano()-openedAt >= int64(circuitCooldownDuration) {
			dls.circuitOpen.Store(false)
			dls.circuitOpenAt.Store(0)
			dls.errorCount = 0
			dls.lastErr = nil
		}
		dls.mu.Unlock()
		return false
	}
	return true
}

// openCircuitLocked trips the circuit breaker. Caller must hold dls.mu.
func (dls *Downloaders) openCircuitLocked() {
	if dls.circuitOpen.Load() {
		return // Already open
	}
	dls.circuitOpen.Store(true)
	dls.circuitOpenAt.Store(time.Now().UnixNano())
	GlobalVFSStats.CircuitTrips.Add(1)
}

// resetCircuitLocked resets the circuit breaker after successful download. Caller must hold dls.mu.
func (dls *Downloaders) resetCircuitLocked() {
	if !dls.circuitOpen.Load() {
		return // Already closed
	}
	dls.circuitOpen.Store(false)
	dls.circuitOpenAt.Store(0)
}

// checkIdleTimeout returns true if idle timeout has been reached and stops all downloaders
func (dls *Downloaders) checkIdleTimeout() bool {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	// Don't timeout if already closed or already idle
	if dls.closed {
		return true
	}

	// Don't timeout if there are active waiters
	if len(dls.waiters) > 0 {
		return false
	}

	// Check if any downloaders are still running
	activeDownloaders := 0
	for _, dl := range dls.dls {
		if !dl.isClosed() {
			activeDownloaders++
		}
	}

	// Check idle timeout
	lastActivity := dls.lastActivity.Load()
	if lastActivity == 0 {
		return false
	}

	idleDuration := time.Since(time.Unix(0, lastActivity))
	if idleDuration < idleTimeout {
		return false
	}

	// Idle timeout reached - stop all downloaders
	for _, dl := range dls.dls {
		dl.stop()
	}
	dls.dls = nil
	dls.untrackStreamLocked()
	dls.idle.Store(true)

	return true
}

// StopAll stops all active downloaders but keeps the Downloaders struct alive
// for potential reuse. This is called when all file handles are closed.
func (dls *Downloaders) StopAll() {
	dls.mu.Lock()
	dls.untrackStreamLocked()

	// Copy slice before unlocking to avoid race during Wait
	dlsCopy := make([]*downloader, len(dls.dls))
	copy(dlsCopy, dls.dls)

	// Stop all downloaders
	for _, dl := range dlsCopy {
		dl.stop()
	}
	dls.dls = nil
	dls.idle.Store(true)
	dls.mu.Unlock()

	// Wait for them to finish (using copy, safe to iterate without lock)
	for _, dl := range dlsCopy {
		dl.wg.Wait()
	}
}

// ensureKickerRunning restarts the kicker goroutine if it has stopped
func (dls *Downloaders) ensureKickerRunning() {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	// Check if kicker has exited (non-blocking check)
	select {
	case <-dls.kickerDone:
		// Kicker has exited, need to restart it
		dls.startKicker()
	default:
		// Kicker still running
	}
}

// startKicker starts the background goroutine that debounces waiter notifications.
func (dls *Downloaders) startKicker() {
	dls.kickerDone = make(chan struct{})
	dls.wg.Add(1)
	go func() {
		defer dls.wg.Done()
		defer close(dls.kickerDone)

		ticker := time.NewTicker(kickerInterval)
		defer ticker.Stop()

		var debounceTimer *time.Timer
		var debounceC <-chan time.Time

		for {
			select {
			case <-ticker.C:
				dls.kickWaiters()
				if dls.checkIdleTimeout() {
					if debounceTimer != nil {
						debounceTimer.Stop()
					}
					return
				}
			case <-dls.kickCh:
				if debounceTimer == nil {
					debounceTimer = time.NewTimer(kickDebounce)
					debounceC = debounceTimer.C
				}
			case <-debounceC:
				debounceTimer = nil
				debounceC = nil
				dls.kickWaiters()
			case <-dls.ctx.Done():
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return
			}
		}
	}()
}

// downloader methods

// run is the main download loop
func (dl *downloader) run() (totalBytes int64, err error) {
	for {
		// Single lock to get all state
		start, targetEnd, chunkSize, fileSize, stopped := dl.getState()
		if stopped || start >= fileSize {
			return totalBytes, nil
		}

		// Nothing to do - wait for more work or timeout
		if start >= targetEnd {
			if !dl.waitForWork() {
				return totalBytes, nil
			}
			continue
		}

		// Calculate chunk boundaries
		// Always download at least chunkSize to reduce Stream calls
		chunkEnd := start + chunkSize
		if chunkEnd > fileSize {
			chunkEnd = fileSize
		}

		// Ensure we're downloading something meaningful
		if chunkEnd <= start {
			continue
		}

		// Download with retry
		written, chunkErr := dl.downloadChunkWithRetry(start, chunkEnd)
		totalBytes += written

		if chunkErr != nil {
			if errors.Is(chunkErr, io.EOF) {
				return totalBytes, nil
			}
			if dl.dls.ctx.Err() != nil {
				return totalBytes, dl.dls.ctx.Err()
			}
			return totalBytes, chunkErr
		}
	}
}

// getState returns current download state with single lock acquisition
func (dl *downloader) getState() (start, targetEnd, chunkSize, fileSize int64, stopped bool) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	chunkSize = dl.currentChunkSize
	if chunkSize <= 0 {
		chunkSize = dl.baseChunkSize
		if chunkSize <= 0 {
			chunkSize = 4 * 1024 * 1024
		}
	}

	fileSize = dl.dls.item.info.Size
	targetEnd = dl.maxOffset
	if targetEnd > fileSize {
		targetEnd = fileSize
	}

	return dl.offset, targetEnd, chunkSize, fileSize, dl.stopped
}

// waitForWork blocks until new work arrives or timeout
func (dl *downloader) waitForWork() bool {
	if dl.idleTimer == nil {
		dl.idleTimer = time.NewTimer(maxDownloaderIdleTime)
	} else {
		if !dl.idleTimer.Stop() {
			select {
			case <-dl.idleTimer.C:
			default:
			}
		}
		dl.idleTimer.Reset(maxDownloaderIdleTime)
	}
	select {
	case <-dl.quit:
		return false
	case <-dl.kick:
		return true
	case <-dl.idleTimer.C:
		return false
	}
}

// downloadChunkWithRetry downloads a chunk with retry logic
func (dl *downloader) downloadChunkWithRetry(start, end int64) (int64, error) {
	attempts := dl.retryAttempts()
	chunkLen := end - start
	delay := config.DefaultRetryDelay
	maxDelay := config.DefaultRetryDelayMax

	for attempt := 1; attempt <= attempts; attempt++ {
		written, err := dl.streamChunk(start, end)

		if err == nil {
			dl.adjustChunkSize(chunkLen, written, true)
			return written, nil
		}

		dl.adjustChunkSize(chunkLen, written, false)

		// Non-retriable conditions
		if errors.Is(err, io.EOF) {
			return written, err
		}
		if dl.dls.ctx.Err() != nil {
			return written, dl.dls.ctx.Err()
		}
		if !customerror.IsRetriableError(err) {
			return written, err
		}

		// Last attempt failed
		if attempt == attempts {
			return written, err
		}

		// Track retry attempt
		GlobalVFSStats.RetryAttempts.Add(1)

		// Log and backoff
		if !customerror.IsSilentError(err) {
			dl.dls.item.logger.Debug().
				Err(err).
				Int("attempt", attempt).
				Msg("stream error, retrying")
		}

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-dl.dls.ctx.Done():
			timer.Stop()
			return written, dl.dls.ctx.Err()
		}

		// Exponential backoff
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return 0, errors.New("exhausted retries")
}

// getRange returns the current download range
func (dl *downloader) getRange() (start, offset int64) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.start, dl.offset
}

func (dl *downloader) streamChunk(start, end int64) (int64, error) {
	dl.mu.Lock()
	if dl.stopped {
		dl.mu.Unlock()
		return 0, io.EOF
	}
	dl.mu.Unlock()

	// Check if this range is already cached BEFORE calling the streaming function.
	// This avoids expensive network/reader operations for already-present data.
	requestedRange := ranges.Range{Pos: start, Size: end - start}
	missingRange := dl.dls.item.FindMissing(requestedRange)
	if missingRange.Size <= 0 {
		// All data already present - just advance offset and return
		dl.mu.Lock()
		dl.offset = end
		dl.mu.Unlock()
		GlobalVFSStats.CacheHits.Add(1)
		return 0, nil
	}

	// Stream the missing portion
	// Advance offset to skip already-cached data before the missing range
	if missingRange.Pos > start {
		dl.mu.Lock()
		dl.offset = missingRange.Pos
		dl.mu.Unlock()
	}

	writer := &cacheWriter{
		dl:     dl,
		item:   dl.dls.item,
		offset: missingRange.Pos,
	}

	GlobalVFSStats.StreamCalls.Add(1)
	err := dl.dls.manager.Stream(
		dl.dls.ctx,
		dl.dls.item.entry,
		dl.dls.item.filename,
		missingRange.Pos,
		missingRange.Pos+missingRange.Size-1, // manager.Stream uses inclusive end
		writer,
		nil,
		"DFS",
	)

	// Track stats
	if writer.written > 0 {
		GlobalVFSStats.TotalChunks.Add(1)
		GlobalVFSStats.TotalBytes.Add(writer.written)
	}

	if err != nil {
		GlobalVFSStats.DownloadErrors.Add(1)
		if dl.dls.ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return writer.written, dl.dls.ctx.Err()
		}
		return writer.written, err
	}

	// Ensure we made progress (either written data or skipped existing data)
	// If offset hasn't moved, we're in an infinite loop
	if writer.offset == missingRange.Pos {
		return writer.written, errors.New("stream produced no data")
	}

	// Final kick to notify waiters of any remaining data
	if writer.written > 0 {
		dl.dls.requestKick()
	}

	return writer.written, nil
}

// setMaxOffset extends the download range
func (dl *downloader) setMaxOffset(max int64) {
	dl.mu.Lock()
	if max > dl.maxOffset {
		dl.maxOffset = max
	}
	dl.mu.Unlock()

	// Kick to wake up if waiting
	select {
	case dl.kick <- struct{}{}:
	default:
	}
}

func (dl *downloader) adjustChunkSize(chunkLen, written int64, success bool) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	// Only reset on actual failure, not on partial writes due to pre-cached data
	// If success is false, it means the stream itself failed
	if !success {
		dl.currentChunkSize = dl.baseChunkSize
		return
	}

	// If no data needed to be written (all cached), don't change chunk size
	if chunkLen <= 0 {
		return
	}

	// Double chunk size on successful download
	next := dl.currentChunkSize * 2
	if next <= 0 {
		next = dl.baseChunkSize
	}
	if next > dl.maxChunkSize {
		next = dl.maxChunkSize
	}
	dl.currentChunkSize = next
}

// stop signals the downloader to stop
func (dl *downloader) stop() {
	dl.mu.Lock()
	if !dl.stopped {
		dl.stopped = true
		close(dl.quit)
	}
	dl.mu.Unlock()
}

// close marks the downloader as closed
func (dl *downloader) close(err error) {
	dl.mu.Lock()
	dl.closed = true
	dl.mu.Unlock()
}

// isClosed returns true if downloader is closed
func (dl *downloader) isClosed() bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.closed
}

func (dl *downloader) retryAttempts() int {
	if dl.dls.retries <= 0 {
		return 3
	}
	return dl.dls.retries
}

// cacheWriter writes to the sparse cache, tracking progress
type cacheWriter struct {
	dl      *downloader
	item    *CacheItem
	offset  int64
	written int64
}

func (w *cacheWriter) Write(p []byte) (int, error) {
	n, skipped, err := w.item.WriteAtNoOverwrite(p, w.offset)
	if err != nil {
		return n, err
	}

	w.dl.mu.Lock()
	// Track skipped bytes
	if skipped == n {
		w.dl.skipped += int64(skipped)
	} else {
		w.dl.skipped = 0
	}
	w.dl.offset += int64(n)

	// Stop if skipping too much (seeking happened elsewhere)
	if w.dl.skipped > maxSkipBytes {
		w.dl.stopped = true
		w.dl.mu.Unlock()
		return n, io.EOF // Signal to stop streaming
	}
	w.dl.mu.Unlock()

	w.offset += int64(n)
	actuallyWritten := int64(n - skipped)
	w.written += actuallyWritten

	if actuallyWritten > 0 {
		w.dl.dls.requestKick()
	}

	return n, nil
}
