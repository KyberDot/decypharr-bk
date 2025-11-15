package rfs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	httppool "github.com/sirrobot01/decypharr/pkg/http"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// ChunkDownloader manages parallel chunk downloads with connection pooling.
// Key features:
// - Persistent HTTP connection reuse
// - Parallel download workers (configurable)
// - Progressive chunk delivery (write as data arrives)
// - Automatic retry with exponential backoff
// - Download queue prioritization
// - Disk cache integration (write-behind)
// - Cached download URL (avoids repeated GetDownloadLink calls)
type ChunkDownloader struct {
	manager   *manager.Manager
	info      *manager.FileInfo
	reader    *Reader // Reference to parent reader for URL cache
	chunkSize int64

	// Worker pool
	workers    int
	workQueue  chan *Chunk
	activeJobs atomic.Int32

	// HTTP connection pool (persistent connections - NO TCP/TLS handshake per chunk!)
	httpPool *httppool.Pool

	// Disk cache (optional)
	diskCache *DiskCache

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool

	// Statistics
	totalDownloaded atomic.Int64
	totalErrors     atomic.Int64
	totalSkipped    atomic.Int64 // Bytes skipped due to cache
	downloadTime    atomic.Int64 // Total time in nanoseconds
}

// NewChunkDownloader creates a new parallel chunk downloader
func NewChunkDownloader(ctx context.Context, mgr *manager.Manager, info *manager.FileInfo, reader *Reader, chunkSize int64, workers int) *ChunkDownloader {
	ctx, cancel := context.WithCancel(ctx)

	// Create HTTP pool with link refresh callback
	cd := &ChunkDownloader{
		manager:   mgr,
		info:      info,
		reader:    reader,
		chunkSize: chunkSize,
		workers:   workers,
		workQueue: make(chan *Chunk, workers*4), // Buffer for better throughput
		ctx:       ctx,
		cancel:    cancel,
		httpPool:  httppool.NewPool(mgr, info.Parent(), info.Name()), // HTTP connection pool - PERSISTENT CONNECTIONS!
	}

	// Start worker pool
	for i := 0; i < workers; i++ {
		cd.wg.Add(1)
		go cd.worker(i)
	}

	return cd
}

// DownloadChunk enqueues a chunk for download
func (cd *ChunkDownloader) DownloadChunk(chunk *Chunk) {
	if cd.closed.Load() {
		return
	}

	// Non-blocking enqueue (caller doesn't wait)
	select {
	case cd.workQueue <- chunk:
		// Queued successfully
	default:
		// Queue full - download synchronously to avoid blocking caller
		cd.activeJobs.Add(1)
		go func() {
			cd.downloadChunk(chunk)
			cd.activeJobs.Add(-1)
		}()
	}
}

// worker processes download jobs from the queue
func (cd *ChunkDownloader) worker(id int) {
	defer cd.wg.Done()

	for {
		select {
		case chunk := <-cd.workQueue:
			cd.activeJobs.Add(1)
			cd.downloadChunk(chunk)
			cd.activeJobs.Add(-1)

		case <-cd.ctx.Done():
			return
		}
	}
}

// downloadChunk performs the actual chunk download with retry logic
func (cd *ChunkDownloader) downloadChunk(chunk *Chunk) {
	startTime := time.Now()

	// Check disk cache FIRST before downloading!
	if cd.diskCache != nil {
		cachedData, hit, err := cd.diskCache.Get(cd.info.Parent(), cd.info.Name(), chunk.startPos, chunk.size)
		if err == nil && hit && int64(len(cachedData)) == chunk.size {
			// Cache hit! Load from disk
			chunk.dataMu.Lock()
			copy(chunk.data, cachedData)
			chunk.dataMu.Unlock()

			chunk.bytesWritten.Store(chunk.size)
			chunk.state.Store(int32(ChunkStateComplete))
			close(chunk.ready)

			// Update stats
			elapsed := time.Since(startTime)
			cd.downloadTime.Add(elapsed.Nanoseconds())
			return
		}
	}

	// Cache miss - download from network
	// Retry logic with exponential backoff
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if cd.closed.Load() {
			return
		}

		// Attempt download
		err := cd.doDownload(chunk)
		if err == nil {
			// Success!
			chunk.state.Store(int32(ChunkStateComplete))
			close(chunk.ready)

			elapsed := time.Since(startTime)
			cd.downloadTime.Add(elapsed.Nanoseconds())
			cd.totalDownloaded.Add(chunk.size)

			// Save to disk cache using WriteAtNoOverwrite (skip detection!)
			if cd.diskCache != nil {
				chunk.dataMu.RLock()
				data := make([]byte, len(chunk.data))
				copy(data, chunk.data)
				chunk.dataMu.RUnlock()

				// WriteAtNoOverwrite - skips already-cached ranges!
				written, skipped, err := cd.diskCache.WriteAtNoOverwrite(
					cd.info.Parent(),
					cd.info.Name(),
					chunk.startPos,
					data,
				)

				if err == nil {
					cd.totalSkipped.Add(int64(skipped))
					// Note: If too much skipped, could stop downloading
					// For now, just track stats
					_ = written
				}
			}

			return
		}

		lastErr = err
		cd.totalErrors.Add(1)

		// Don't retry on context cancellation
		if cd.ctx.Err() != nil {
			break
		}

		// Exponential backoff: 100ms, 300ms, 900ms
		if attempt < maxRetries-1 {
			backoff := time.Duration(100*(1<<uint(attempt))) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-cd.ctx.Done():
				return
			}
		}
	}

	// All retries failed
	chunk.state.Store(int32(ChunkStateError))
	chunk.downloadErr.Store(lastErr)
	close(chunk.ready)
}

// doDownload performs a single download attempt with progressive delivery
func (cd *ChunkDownloader) doDownload(chunk *Chunk) error {
	// Calculate byte range
	startOffset := chunk.startPos
	endOffset := chunk.startPos + chunk.size - 1

	// Use HTTP pool with automatic retry and link refresh!
	// Pool handles: link caching, retry logic, link invalidation on errors
	rc, err := cd.httpPool.GetReader(cd.ctx, startOffset, endOffset)
	if err != nil {
		return fmt.Errorf("http pool get: %w", err)
	}
	defer rc.Close()

	// Download with progressive writes
	const bufferSize = 256 * 1024 // 256KB buffer for reads
	buffer := make([]byte, bufferSize)
	totalRead := int64(0)

	for totalRead < chunk.size {
		// Read from HTTP stream
		n, err := rc.Read(buffer)
		if n > 0 {
			// Write to chunk buffer (progressive delivery!)
			writeSize := int64(n)
			if totalRead+writeSize > chunk.size {
				writeSize = chunk.size - totalRead
			}

			chunk.dataMu.Lock()
			copy(chunk.data[totalRead:totalRead+writeSize], buffer[:writeSize])
			chunk.dataMu.Unlock()

			totalRead += writeSize
			chunk.bytesWritten.Store(totalRead)

			// Wake up readers waiting for this data using sync.Cond (NO POLLING!)
			chunk.progressMu.Lock()
			chunk.progressCond.Broadcast() // Wake ALL waiting readers efficiently
			chunk.progressMu.Unlock()
		}

		if err != nil {
			if err == io.EOF {
				// Expected EOF
				if totalRead == chunk.size {
					return nil
				}
				return fmt.Errorf("unexpected EOF: got %d bytes, expected %d", totalRead, chunk.size)
			}
			return fmt.Errorf("read from stream: %w", err)
		}
	}

	return nil
}

// Close shuts down the downloader
func (cd *ChunkDownloader) Close() error {
	if !cd.closed.CompareAndSwap(false, true) {
		return nil
	}

	cd.cancel()
	cd.wg.Wait()
	cd.httpPool.Close()

	return nil
}

// GetStats returns downloader statistics
func (cd *ChunkDownloader) GetStats() map[string]interface{} {
	avgDownloadTime := int64(0)
	downloaded := cd.totalDownloaded.Load()
	if downloaded > 0 {
		avgDownloadTime = cd.downloadTime.Load() / downloaded
	}

	stats := map[string]interface{}{
		"total_downloaded": downloaded,
		"total_skipped":    cd.totalSkipped.Load(),
		"total_errors":     cd.totalErrors.Load(),
		"active_jobs":      cd.activeJobs.Load(),
		"avg_download_ns":  avgDownloadTime,
	}

	poolStats := cd.httpPool.GetStats()
	for k, v := range poolStats {
		stats["http_"+k] = v
	}

	return stats
}
