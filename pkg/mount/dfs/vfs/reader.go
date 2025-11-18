package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// Reader implements on-demand file reading with sharded sparse caching
type Reader struct {
	manager     *manager.Manager
	torrentName string
	filename    string
	fileSize    int64

	// Configuration
	chunkSize     int64 // From cfg.ChunkSize
	bufferSize    int64 // From cfg.BufferSize (for HTTP read buffer)
	readAhead     int64 // From cfg.ReadAheadSize
	maxConcurrent int   // From cfg.MaxConcurrentReads

	// Sparse cache (disk-based with sharded ranges)
	cache     *Cache
	cacheFile *CacheFile

	// Download synchronization
	downloads       *xsync.Map[int64, *downloadOperation]
	activeDownloads atomic.Int32 // Count of active downloads

	// Statistics
	stats struct {
		reads        atomic.Int64
		cacheHits    atomic.Int64
		cacheMisses  atomic.Int64
		downloads    atomic.Int64
		bytesRead    atomic.Int64
		bytesWritten atomic.Int64
	}

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
}

// downloadOperation tracks a single download operation with better coordination
type downloadOperation struct {
	offset    int64
	size      int64
	done      chan struct{} // Closed when download completes
	err       error
	startTime time.Time
}

// NewReader creates a reader with sharded caching
func NewReader(ctx context.Context, mgr *manager.Manager, info *manager.FileInfo, cache *Cache, cfg *common.FuseConfig) (*Reader, error) {
	ctx, cancel := context.WithCancel(ctx)
	torrentName, filename, fileSize := info.Parent(), info.Name(), info.Size()

	cf, err := cache.GetOrCreate(torrentName, filename, fileSize)
	if err != nil {
		cancel()
		return nil, err
	}

	// Set defaults if not configured
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024 // Default 4MB
	}

	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = 256 * 1024 // Default 256KB
	}

	maxConcurrent := cfg.MaxConcurrentReads
	if maxConcurrent <= 0 {
		maxConcurrent = 4 // Default 4
	}

	readAhead := cfg.ReadAheadSize
	if readAhead < 0 {
		readAhead = 0 // Disable read-ahead if negative
	}

	r := &Reader{
		manager:       mgr,
		fileSize:      fileSize,
		torrentName:   torrentName,
		filename:      filename,
		chunkSize:     chunkSize,
		bufferSize:    bufferSize,
		readAhead:     readAhead,
		maxConcurrent: maxConcurrent,
		cache:         cache,
		cacheFile:     cf,
		downloads:     xsync.NewMap[int64, *downloadOperation](),
		ctx:           ctx,
		cancel:        cancel,
	}
	return r, nil
}

// ReadAt reads data at offset (FUSE callback)
// Optimized algorithm with sharded caching:
// 1. Fast lockless cache check using sharded ranges
// 2. If miss, download with optimal chunking and read-ahead
// 3. Use concurrent downloads for large requests
func (r *Reader) ReadAt(p []byte, offset int64) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	if offset >= r.fileSize {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if offset+readSize > r.fileSize {
		readSize = r.fileSize - offset
		p = p[:readSize]
	}

	r.stats.reads.Add(1)
	r.stats.bytesRead.Add(readSize)

	// Try to read from cache first (fast lockless path)
	n, cached, err := r.cache.ReadAt(r.cacheFile, p, offset)
	if cached && err == nil && int64(n) == readSize {
		r.stats.cacheHits.Add(1)
		return n, nil
	}

	r.stats.cacheMisses.Add(1)

	// Calculate range to download with read-ahead and chunk alignment
	downloadStart, downloadSize := r.calculateDownloadRange(offset, readSize)

	// Download the missing range
	if err := r.downloadRange(downloadStart, downloadSize); err != nil {
		return 0, fmt.Errorf("download failed: %w", err)
	}

	// Read from cache again (should succeed now)
	n, cached, err = r.cache.ReadAt(r.cacheFile, p, offset)
	if err != nil {
		return 0, fmt.Errorf("cache read failed after download: %w", err)
	}
	if !cached {
		return 0, fmt.Errorf("cache read failed: data not present after download")
	}

	return n, nil
}

// calculateDownloadRange determines optimal download range with chunking and read-ahead
func (r *Reader) calculateDownloadRange(offset, size int64) (start, downloadSize int64) {
	// Calculate end with read-ahead
	end := offset + size
	if r.readAhead > 0 {
		end += r.readAhead
	}

	// Align to chunk boundaries for better cache efficiency
	start = (offset / r.chunkSize) * r.chunkSize
	end = ((end + r.chunkSize - 1) / r.chunkSize) * r.chunkSize

	// Clamp to file bounds
	if end > r.fileSize {
		end = r.fileSize
	}

	return start, end - start
}

// downloadRange downloads a range of data with deduplication and concurrency
func (r *Reader) downloadRange(offset, size int64) error {
	// Quick check if already fully cached
	r.cacheFile.lastAccess.Store(time.Now().UnixNano())

	if r.cacheFile.ranges.Present(common.Range{Pos: offset, Size: size}) {
		return nil // Already fully cached
	}

	// Check if download is already in progress for this range
	dl, exists := r.downloads.Load(offset)
	if exists {
		// Download already in progress, wait for it
		select {
		case <-dl.done:
			return dl.err
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	// Start new download operation
	dl = &downloadOperation{
		offset:    offset,
		size:      size,
		done:      make(chan struct{}),
		startTime: time.Now(),
	}

	// Use LoadOrStore to handle race condition
	actual, loaded := r.downloads.LoadOrStore(offset, dl)
	if loaded {
		// Another goroutine beat us to it, wait for their download
		select {
		case <-actual.done:
			return actual.err
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	// We're responsible for this download
	r.stats.downloads.Add(1)

	// Perform the download
	dl.err = r.doDownload(offset, size)

	// Clean up and signal completion
	r.downloads.Delete(offset)
	close(dl.done)
	return dl.err
}

// doDownload performs the actual HTTP download with intelligent concurrency
func (r *Reader) doDownload(offset, size int64) error {
	// For small downloads, use sequential to minimize overhead
	if size <= r.chunkSize || r.maxConcurrent <= 1 {
		return r.doSequentialDownload(offset, size)
	}

	// Calculate optimal piece size for concurrent download
	pieceSize := size / int64(r.maxConcurrent)

	// If pieces would be too small, use sequential download
	if pieceSize < r.bufferSize {
		return r.doSequentialDownload(offset, size)
	}

	// Use concurrent download for large ranges
	return r.doConcurrentDownload(offset, size)
}

// doConcurrentDownload splits download across multiple workers
func (r *Reader) doConcurrentDownload(offset, size int64) error {
	pieceSize := size / int64(r.maxConcurrent)

	var wg sync.WaitGroup
	errChan := make(chan error, r.maxConcurrent)

	for i := 0; i < r.maxConcurrent; i++ {
		pieceStart := offset + (int64(i) * pieceSize)
		pieceEnd := pieceStart + pieceSize
		if i == r.maxConcurrent-1 {
			pieceEnd = offset + size // Last piece gets remainder
		}
		actualPieceSize := pieceEnd - pieceStart

		wg.Add(1)
		r.activeDownloads.Add(1)

		go func(start, sz int64, workerID int) {
			defer wg.Done()
			defer r.activeDownloads.Add(-1)

			if err := r.doSequentialDownload(start, sz); err != nil {
				select {
				case errChan <- fmt.Errorf("worker %d failed: %w", workerID, err):
				default: // Channel might be full
				}
			}
		}(pieceStart, actualPieceSize, i)
	}

	wg.Wait()
	close(errChan)

	// Return first error if any
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// doSequentialDownload downloads a range sequentially with optimized buffering
func (r *Reader) doSequentialDownload(offset, size int64) error {
	endOffset := offset + size - 1
	rc, err := r.manager.StreamReader(r.ctx, r.torrentName, r.filename, offset, endOffset)
	if err != nil {
		return fmt.Errorf("http get [%d-%d]: %w", offset, endOffset, err)
	}
	defer func(rc io.ReadCloser) {
		_ = rc.Close()
	}(rc)

	// Use buffer pool to reduce allocations
	buffer := make([]byte, r.bufferSize)
	var writeOps []WriteOp
	totalRead := int64(0)

	for totalRead < size {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}

		n, readErr := rc.Read(buffer)
		if n > 0 {
			writeSize := int64(n)
			if totalRead+writeSize > size {
				writeSize = size - totalRead
			}

			currentOffset := offset + totalRead

			// Accumulate writes for batching
			writeOps = append(writeOps, WriteOp{
				Offset: currentOffset,
				Data:   make([]byte, writeSize), // Copy data
			})
			copy(writeOps[len(writeOps)-1].Data, buffer[:writeSize])

			totalRead += writeSize

			// Batch writes when we have enough or on completion
			if len(writeOps) >= 8 || totalRead == size || readErr != nil {
				if err := r.cache.WriteBatch(r.cacheFile, writeOps); err != nil {
					return fmt.Errorf("cache batch write: %w", err)
				}
				r.stats.bytesWritten.Add(totalRead)

				// Clear write ops for next batch
				writeOps = writeOps[:0]
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				if totalRead == size {
					return nil // Success
				}
				return fmt.Errorf("unexpected EOF: got %d bytes, expected %d", totalRead, size)
			}
			return fmt.Errorf("read error at %d: %w", offset+totalRead, readErr)
		}
	}

	return nil
}

// GetStats returns comprehensive statistics
func (r *Reader) GetStats() map[string]interface{} {
	cacheStats := r.cache.GetStats()

	stats := map[string]interface{}{
		// Reader stats
		"reads":            r.stats.reads.Load(),
		"cache_hits":       r.stats.cacheHits.Load(),
		"cache_misses":     r.stats.cacheMisses.Load(),
		"downloads":        r.stats.downloads.Load(),
		"bytes_read":       r.stats.bytesRead.Load(),
		"bytes_written":    r.stats.bytesWritten.Load(),
		"active_downloads": r.activeDownloads.Load(),

		// File info
		"file_size":      r.fileSize,
		"chunk_size":     r.chunkSize,
		"buffer_size":    r.bufferSize,
		"read_ahead":     r.readAhead,
		"max_concurrent": r.maxConcurrent,
	}

	// Add cache stats
	for k, v := range cacheStats {
		stats["cache_"+k] = v
	}

	// Calculate hit ratio
	totalReads := r.stats.reads.Load()
	if totalReads > 0 {
		hitRatio := float64(r.stats.cacheHits.Load()) / float64(totalReads)
		stats["cache_hit_ratio"] = hitRatio
	}

	return stats
}

// GetActiveDownloads returns information about currently active downloads
func (r *Reader) GetActiveDownloads() map[int64]*downloadOperation {
	active := make(map[int64]*downloadOperation)
	r.downloads.Range(func(key int64, value *downloadOperation) bool {
		active[key] = value
		return true
	})
	return active
}

// WaitForDownloads waits for all active downloads to complete
func (r *Reader) WaitForDownloads() error {
	var activeOps []*downloadOperation

	r.downloads.Range(func(key int64, value *downloadOperation) bool {
		activeOps = append(activeOps, value)
		return true
	})

	for _, op := range activeOps {
		select {
		case <-op.done:
			if op.err != nil {
				return op.err
			}
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}

	return nil
}

// Close closes the reader and waits for downloads to complete
func (r *Reader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Cancel context to stop new downloads
	r.cancel()

	// Optionally wait a short time for downloads to complete gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = r.WaitForDownloads()
		close(done)
	}()

	select {
	case <-done:
		// All downloads completed
	case <-ctx.Done():
		// Timeout - downloads may still be running but we'll proceed with cleanup
	}

	// Release cache file reference
	// This decrements refCount so cache cleanup can eventually evict the file
	if r.cacheFile != nil {
		r.cache.ReleaseCacheFile(r.cacheFile)
	}

	return nil
}

// Prefetch proactively downloads a range to warm the cache
func (r *Reader) Prefetch(offset, size int64) error {
	if r.closed.Load() {
		return io.ErrClosedPipe
	}

	// Align to chunk boundaries
	start, downloadSize := r.calculateDownloadRange(offset, size)

	// Check if already cached
	if r.cacheFile.ranges.Present(common.Range{Pos: start, Size: downloadSize}) {
		return nil
	}

	// Download in background (don't wait)
	go func() {
		err := r.downloadRange(start, downloadSize)
		if err != nil {
			return
		}
	}()

	return nil
}
