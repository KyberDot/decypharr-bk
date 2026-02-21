package reader

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/retry"
	"golang.org/x/sync/errgroup"
)

// SegmentFetcher handles downloading segments from NNTP with deduplication and retry.
//
// Key features:
//   - Request deduplication: Only one goroutine fetches a segment at a time
//   - Semaphore for connection limiting
//   - Background prefetch queue for read-ahead
//   - Streams directly to disk via cache's StreamWriter
type SegmentFetcher struct {
	client *nntp.Client
	cache  *SegmentCache
	config Config
	logger zerolog.Logger
	stats  *ReaderStats

	// Concurrency control
	semaphore chan struct{} // Limits concurrent downloads

	// Request deduplication
	inFlight   map[int]*fetchPromise
	inFlightMu sync.Mutex

	// Background prefetch
	prefetchCh chan int
	prefetchWg sync.WaitGroup

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// fetchPromise allows multiple goroutines to wait for the same segment download.
type fetchPromise struct {
	done chan struct{}
	err  error
}

// NewSegmentFetcher creates a new segment fetcher.
func NewSegmentFetcher(
	ctx context.Context,
	client *nntp.Client,
	cache *SegmentCache,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) *SegmentFetcher {
	ctx, cancel := context.WithCancel(ctx)

	maxConns := config.MaxConnections
	if maxConns < 1 {
		maxConns = 8
	}

	sf := &SegmentFetcher{
		client:     client,
		cache:      cache,
		config:     config,
		logger:     logger.With().Str("component", "fetcher").Logger(),
		stats:      stats,
		semaphore:  make(chan struct{}, maxConns),
		inFlight:   make(map[int]*fetchPromise),
		prefetchCh: make(chan int, 256), // Buffer for prefetch hints
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start prefetch workers
	numPrefetchWorkers := maxConns
	for i := 0; i < numPrefetchWorkers; i++ {
		sf.prefetchWg.Add(1)
		go sf.prefetchWorker(i)
	}

	return sf
}

// Fetch downloads a segment synchronously, with deduplication.
// Multiple goroutines calling Fetch for the same segment will share the download.
func (sf *SegmentFetcher) Fetch(ctx context.Context, segIdx int) error {
	// Fast path: already cached
	state := sf.cache.GetState(segIdx)
	switch state {
	case StateOnDisk:
		return nil
	case StateFailed:
		return sf.cache.GetError(segIdx)
	}

	// Check if someone else is already fetching
	sf.inFlightMu.Lock()
	if promise, ok := sf.inFlight[segIdx]; ok {
		sf.inFlightMu.Unlock()
		// Wait for existing fetch
		select {
		case <-promise.done:
			return promise.err
		case <-ctx.Done():
			return ctx.Err()
		case <-sf.ctx.Done():
			return sf.ctx.Err()
		}
	}

	// We're the first - create promise
	promise := &fetchPromise{done: make(chan struct{})}
	sf.inFlight[segIdx] = promise
	sf.inFlightMu.Unlock()

	// Actually fetch
	err := sf.doFetch(ctx, segIdx)
	promise.err = err
	close(promise.done)

	// Cleanup
	sf.inFlightMu.Lock()
	delete(sf.inFlight, segIdx)
	sf.inFlightMu.Unlock()

	return err
}

// doFetch performs the actual NNTP download.
func (sf *SegmentFetcher) doFetch(ctx context.Context, segIdx int) error {
	seg := sf.cache.GetSegment(segIdx)
	if seg == nil {
		return ErrSegmentNotFound
	}

	// Try to mark as fetching (atomic transition Empty -> Fetching)
	if !sf.cache.MarkFetching(segIdx) {
		// Someone else is fetching or it's already cached
		state := sf.cache.GetState(segIdx)
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		case StateFetching:
			// Wait for the other fetcher
			return sf.cache.WaitForSegment(ctx, segIdx)
		}
	}

	// Acquire connection slot
	select {
	case sf.semaphore <- struct{}{}:
		defer func() { <-sf.semaphore }()
	case <-ctx.Done():
		sf.cache.ResetState(segIdx)
		return ctx.Err()
	case <-sf.ctx.Done():
		sf.cache.ResetState(segIdx)
		return sf.ctx.Err()
	}

	messageID := seg.MessageID
	timeout := sf.config.DownloadTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	// Download with retry
	err := retry.Do(
		func() error {
			downloadCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			return sf.client.ExecuteWithFailover(downloadCtx, func(conn *nntp.Connection) error {
				// Get the disk stream writer from cache
				writer := sf.cache.StreamWriter(segIdx)
				if writer == nil {
					return ErrCacheClosed
				}

				// Stream body directly to disk
				n, err := conn.StreamBody(messageID, writer)
				if err != nil {
					return err
				}

				// Treat zero-byte articles as missing — the article exists on the
				// server but its body is empty/corrupted after yEnc decoding.
				if n == 0 {
					return &nntp.Error{
						Type:    nntp.ErrorTypeArticleNotFound,
						Message: "article produced no data after decoding",
					}
				}

				// Finalize the write (updates cache state)
				if dw, ok := writer.(*diskStreamWriter); ok {
					dw.Finalize()
				}

				return nil
			})
		},
		retry.Attempts(uint(sf.config.MaxRetries)),
		retry.Delay(sf.config.RetryDelay),
		retry.RetryIf(sf.isRetryable),
		retry.Context(ctx),
		retry.OnRetry(func(n uint, err error) {
			sf.stats.DownloadRetries.Add(1)
			sf.logger.Debug().
				Err(err).
				Int("segment", segIdx).
				Uint("attempt", n+1).
				Msg("retrying segment download")
		}),
	)

	if err != nil {
		sf.stats.DownloadErrors.Add(1)
		sf.cache.MarkFailed(segIdx, err)
		return err
	}

	sf.stats.Downloads.Add(1)
	return nil
}

// isRetryable determines if an error should trigger a retry.
func (sf *SegmentFetcher) isRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Article not found is permanent
	if nntp.IsArticleNotFoundError(err) {
		return false
	}

	// Context errors are not retryable
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}

	// All other errors (connection issues, etc.) are retryable
	return true
}

// QueuePrefetch adds a segment to the background prefetch queue (non-blocking).
func (sf *SegmentFetcher) QueuePrefetch(segIdx int) {
	// Check if already cached or shutting down
	state := sf.cache.GetState(segIdx)
	if state == StateOnDisk || state == StateFetching {
		return
	}

	select {
	case <-sf.ctx.Done():
		return // Shutting down, don't send
	case sf.prefetchCh <- segIdx:
		// Queued successfully
	default:
		// Queue full, drop the hint
		sf.stats.PrefetchMisses.Add(1)
	}
}

// QueuePrefetchRange queues multiple segments for prefetch.
func (sf *SegmentFetcher) QueuePrefetchRange(startSeg, endSeg int) {
	for i := startSeg; i <= endSeg; i++ {
		sf.QueuePrefetch(i)
	}
}

// prefetchWorker processes segments from the prefetch queue.
func (sf *SegmentFetcher) prefetchWorker(id int) {
	defer sf.prefetchWg.Done()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case segIdx := <-sf.prefetchCh:
			// Check if still needed
			state := sf.cache.GetState(segIdx)
			if state == StateOnDisk {
				sf.stats.PrefetchHits.Add(1)
				continue
			}

			// Fetch with a timeout
			fetchCtx, cancel := context.WithTimeout(sf.ctx, sf.config.DownloadTimeout)
			err := sf.Fetch(fetchCtx, segIdx)
			cancel()

			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				sf.logger.Debug().
					Err(err).
					Int("segment", segIdx).
					Msg("prefetch failed")
			}
		}
	}
}

// EnsureSegments fetches all segments in the range concurrently,
// returning when all are available. The semaphore limits connection usage.
func (sf *SegmentFetcher) EnsureSegments(ctx context.Context, startSeg, endSeg int) error {
	// Count how many segments actually need fetching
	needsFetch := false
	for i := startSeg; i <= endSeg; i++ {
		state := sf.cache.GetState(i)
		if state != StateOnDisk {
			needsFetch = true
			break
		}
	}
	if !needsFetch {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	for i := startSeg; i <= endSeg; i++ {
		state := sf.cache.GetState(i)
		if state == StateOnDisk {
			continue
		}
		i := i
		g.Go(func() error {
			return sf.Fetch(gctx, i)
		})
	}
	return g.Wait()
}

// Close stops all workers and waits for them to finish.
// Uses context cancellation instead of closing the channel to avoid
// a race with concurrent QueuePrefetch sends.
func (sf *SegmentFetcher) Close() {
	sf.cancel()
	sf.prefetchWg.Wait()
}

// Error types
var (
	ErrSegmentNotFound = &segmentError{msg: "segment not found"}
	ErrCacheClosed     = &segmentError{msg: "cache closed"}
)

type segmentError struct {
	msg string
}

func (e *segmentError) Error() string {
	return e.msg
}
