package rfs

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// Reader provides high-performance streaming reads with intelligent prefetching.
// Architecture:
// - Fixed-size aligned chunks (e.g., 128MB boundaries)
// - Persistent HTTP connections reused across reads
// - Sequential read detection triggers aggressive prefetching
// - Lock-free read paths for hot chunks
// - Progressive delivery from in-progress downloads
// - Disk cache for persistent storage (rclone-like)
// - Cached download URL with TTL (avoid repeated database/API calls)
type Reader struct {
	// File metadata
	info      *manager.FileInfo
	manager   *manager.Manager
	chunkSize int64
	fileSize  int64

	// Chunk storage (fixed alignment)
	chunks      map[int64]*Chunk // key: chunk index (0, 1, 2, ...)
	chunksMu    sync.RWMutex
	chunkPool   *sync.Pool   // Reusable chunk buffers
	maxChunks   int          // Maximum chunks to keep in memory
	totalChunks atomic.Int64 // Current number of chunks

	// Memory management (BufferSize enforcement)
	memoryLimit   int64        // Maximum memory to use (from BufferSize config)
	memoryUsage   atomic.Int64 // Current memory usage in bytes
	evictionRatio float64      // When to trigger eviction (default 0.9 = 90%)

	// Disk cache (persistent storage)
	diskCache *DiskCache

	// Read-ahead state
	readAheadSize   int64        // How far ahead to prefetch (bytes)
	lastReadOffset  atomic.Int64 // Last read position
	lastReadTime    atomic.Int64 // Unix nano timestamp
	sequentialReads atomic.Int64 // Count of sequential reads
	isSequential    atomic.Bool  // Sequential pattern detected

	// Download management
	downloader *ChunkDownloader
	maxWorkers int // Parallel download workers

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
	wg     sync.WaitGroup

	// Statistics
	stats *ReaderStats
}

// Chunk represents a fixed-size aligned chunk of file data.
// Chunks are identified by index: chunk 0 = bytes [0, chunkSize),
// chunk 1 = bytes [chunkSize, 2*chunkSize), etc.
type Chunk struct {
	index     int64 // Chunk index
	startPos  int64 // File offset where chunk starts
	size      int64 // Actual chunk size (last chunk may be smaller)
	chunkSize int64 // Configured chunk size

	// Data storage
	data   []byte       // Chunk data buffer
	dataMu sync.RWMutex // RWMutex allows concurrent reads!

	// Download state
	state        atomic.Int32 // ChunkState enum
	bytesWritten atomic.Int64 // How much has been downloaded
	downloadErr  atomic.Value // Download error if any

	// Coordination (sync.Cond for efficient waiting - NO POLLING!)
	progressMu   sync.Mutex
	progressCond *sync.Cond    // Signaled when more data arrives
	ready        chan struct{} // Closed when download completes
	waiters      atomic.Int32  // Count of waiting readers

	// LRU tracking (lock-free)
	lastAccess atomic.Int64 // Unix nano timestamp of last access
	refCount   atomic.Int32 // Reference count for safe eviction
}

// ChunkState represents the state of a chunk
type ChunkState int32

const (
	ChunkStateEmpty       ChunkState = 0 // Not started
	ChunkStateDownloading ChunkState = 1 // Download in progress
	ChunkStateComplete    ChunkState = 2 // Download complete
	ChunkStateError       ChunkState = 3 // Download failed
)

// ReaderStats tracks reader performance metrics
type ReaderStats struct {
	BytesRead        atomic.Int64
	ChunksDownloaded atomic.Int64
	CacheHits        atomic.Int64
	CacheMisses      atomic.Int64
	SequentialReads  atomic.Int64
	RandomReads      atomic.Int64
	ReadAheadHits    atomic.Int64
}

// NewReader creates a new high-performance reader
func NewReader(ctx context.Context, mgr *manager.Manager, info *manager.FileInfo, cfg *common.FuseConfig) *Reader {

	ctx, cancel := context.WithCancel(ctx)

	// Calculate memory limit from BufferSize config
	memoryLimit := cfg.BufferSize
	if memoryLimit <= 0 {
		// Default: 128MB per file if not configured
		memoryLimit = 128 * 1024 * 1024
	}

	// Calculate max chunks based on memory limit
	maxChunks := int(memoryLimit / cfg.ChunkSize)
	if maxChunks < 2 {
		maxChunks = 2 // Minimum 2 chunks for reasonable performance
	}

	r := &Reader{
		info:          info,
		manager:       mgr,
		chunkSize:     cfg.ChunkSize,
		fileSize:      info.Size(),
		chunks:        make(map[int64]*Chunk),
		maxChunks:     maxChunks,
		memoryLimit:   memoryLimit,
		evictionRatio: 0.9, // Start evicting at 90% memory usage
		readAheadSize: cfg.ReadAheadSize,
		maxWorkers:    cfg.MaxConcurrentReads,
		ctx:           ctx,
		cancel:        cancel,
		stats:         &ReaderStats{},
		chunkPool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, cfg.ChunkSize)
				return &buf
			},
		},
	}

	// Note: disk cache is set by the manager (shared across all readers)

	// Initialize downloader
	r.downloader = NewChunkDownloader(ctx, mgr, info, r, cfg.ChunkSize, cfg.MaxConcurrentReads)

	return r
}

// ReadAt reads len(p) bytes starting at offset.
// Implements io.ReaderAt interface.
// This is the hot path - optimized for zero allocations on cache hits.
func (r *Reader) ReadAt(p []byte, offset int64) (int, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	if offset >= r.fileSize {
		return 0, io.EOF
	}

	// Adjust read size if it extends beyond file
	readSize := int64(len(p))
	if offset+readSize > r.fileSize {
		readSize = r.fileSize - offset
		p = p[:readSize]
	}

	// CRITICAL OPTIMIZATION: Check disk cache FIRST!
	if r.diskCache != nil {
		cachedData, hit, err := r.diskCache.Get(r.info.Parent(), r.info.Name(), offset, readSize)
		if err == nil && hit {
			// Cache hit! Return immediately
			copy(p, cachedData)
			r.stats.BytesRead.Add(readSize)
			return int(readSize), nil
		}
	}

	// Cache miss - download from network
	// Detect sequential reads and trigger prefetching
	r.detectSequentialRead(offset)

	// Read from chunks (may span multiple chunks)
	totalRead := 0
	currentOffset := offset

	for totalRead < len(p) {
		// Calculate which chunk contains currentOffset
		chunkIdx := currentOffset / r.chunkSize
		offsetInChunk := currentOffset % r.chunkSize

		// GetReader or create chunk
		chunk := r.getOrCreateChunk(chunkIdx)

		// Ensure chunk is downloading
		if chunk.state.Load() == int32(ChunkStateEmpty) {
			r.startChunkDownload(chunk)
		}

		// Read from chunk (waits for data if needed)
		n, err := r.readFromChunk(chunk, p[totalRead:], offsetInChunk)
		totalRead += n
		currentOffset += int64(n)

		if err != nil {
			if totalRead > 0 {
				// Return partial read
				r.stats.BytesRead.Add(int64(totalRead))
				return totalRead, nil
			}
			return 0, err
		}

		// If we read less than requested, we're at chunk boundary or EOF
		if n == 0 {
			break
		}
	}

	r.stats.BytesRead.Add(int64(totalRead))
	return totalRead, nil
}

// detectSequentialRead detects sequential access patterns and triggers prefetching
func (r *Reader) detectSequentialRead(offset int64) {
	lastOffset := r.lastReadOffset.Load()
	now := time.Now().UnixNano()

	// Update last read
	r.lastReadOffset.Store(offset)
	r.lastReadTime.Store(now)

	// Check if sequential: forward read within reasonable distance
	// More lenient threshold for better detection
	const sequentialThreshold = 4 * 1024 * 1024 // 4MB threshold (larger for video)
	delta := offset - lastOffset

	// Sequential = forward reads (including exact position repeats)
	isSeq := delta >= 0 && delta <= sequentialThreshold

	if isSeq {
		count := r.sequentialReads.Add(1)

		// Enable read-ahead IMMEDIATELY on first sequential read!
		// rclone doesn't wait - it prefetches aggressively from the start
		if count >= 1 && !r.isSequential.Load() {
			r.isSequential.Store(true)
			r.stats.SequentialReads.Add(1)
		}

		// Always trigger read-ahead for sequential patterns
		r.triggerReadAhead(offset)
	} else {
		// Random access detected
		r.sequentialReads.Store(0)
		r.isSequential.Store(false)
		r.stats.RandomReads.Add(1)
	}
}

// triggerReadAhead starts downloading chunks ahead of current position
func (r *Reader) triggerReadAhead(currentOffset int64) {
	// BACKPRESSURE: Check if we're already downloading too many chunks
	// This prevents unbounded memory usage from aggressive prefetching
	activeJobs := r.downloader.activeJobs.Load()
	if activeJobs >= int32(r.maxWorkers*2) {
		// Too many active downloads - skip prefetch to avoid memory explosion
		return
	}

	// Check memory usage FIRST (most important constraint)
	currentMemory := r.memoryUsage.Load()
	if currentMemory >= int64(float64(r.memoryLimit)*r.evictionRatio) {
		// Memory threshold reached - skip prefetch
		return
	}

	// Also check total chunks in memory
	totalChunks := r.totalChunks.Load()
	if totalChunks >= int64(r.maxChunks) {
		// Chunk limit reached - skip prefetch
		return
	}

	// Calculate how many chunks to prefetch
	endOffset := currentOffset + r.readAheadSize
	if endOffset > r.fileSize {
		endOffset = r.fileSize
	}

	startChunkIdx := currentOffset / r.chunkSize
	endChunkIdx := endOffset / r.chunkSize

	// Limit prefetch window to avoid queueing too much work
	maxPrefetch := int64(r.maxWorkers) // Prefetch at most maxWorkers chunks ahead
	if endChunkIdx-startChunkIdx > maxPrefetch {
		endChunkIdx = startChunkIdx + maxPrefetch
	}

	// Start downloading chunks in the read-ahead window
	for chunkIdx := startChunkIdx; chunkIdx <= endChunkIdx; chunkIdx++ {
		// Double-check backpressure before each chunk
		if r.downloader.activeJobs.Load() >= int32(r.maxWorkers*2) {
			break
		}

		chunk := r.getOrCreateChunk(chunkIdx)

		// Only start download if not already downloading/complete
		if chunk.state.CompareAndSwap(int32(ChunkStateEmpty), int32(ChunkStateDownloading)) {
			r.downloader.DownloadChunk(chunk)
		}
	}
}

// getOrCreateChunk returns the chunk for the given index, creating if needed
func (r *Reader) getOrCreateChunk(chunkIdx int64) *Chunk {
	// Fast path: read lock (lock-free access time update!)
	r.chunksMu.RLock()
	chunk, exists := r.chunks[chunkIdx]
	r.chunksMu.RUnlock()

	if exists {
		// Update access time atomically (no locks needed!)
		chunk.lastAccess.Store(time.Now().UnixNano())
		return chunk
	}

	// Slow path: write lock and create
	r.chunksMu.Lock()
	defer r.chunksMu.Unlock()

	// Double-check after acquiring write lock
	if chunk, exists := r.chunks[chunkIdx]; exists {
		chunk.lastAccess.Store(time.Now().UnixNano())
		return chunk
	}

	// Check memory limit BEFORE creating new chunk
	currentMemory := r.memoryUsage.Load()
	evictionThreshold := int64(float64(r.memoryLimit) * r.evictionRatio)

	// Evict if we're over threshold OR if we're at maxChunks
	if currentMemory >= evictionThreshold || len(r.chunks) >= r.maxChunks {
		r.evictOldChunksLocked()
	}

	// Create new chunk
	startPos := chunkIdx * r.chunkSize
	size := r.chunkSize
	if startPos+size > r.fileSize {
		size = r.fileSize - startPos
	}

	// Dynamic buffer sizing: use pool for full chunks, direct allocation for small ones
	var buf []byte
	if size == r.chunkSize {
		// Full chunk - use pool for memory reuse
		bufPtr := r.chunkPool.Get().(*[]byte)
		buf = *bufPtr
	} else {
		// Partial chunk (e.g., last chunk) - allocate exact size to avoid waste
		buf = make([]byte, size)
	}

	chunk = &Chunk{
		index:     chunkIdx,
		startPos:  startPos,
		size:      size,
		chunkSize: r.chunkSize,
		data:      buf,
		ready:     make(chan struct{}),
	}

	// Initialize sync.Cond for efficient waiting (NO POLLING!)
	chunk.progressCond = sync.NewCond(&chunk.progressMu)

	// Initialize refcount to 1 (caller has a reference)
	chunk.refCount.Store(1)

	chunk.lastAccess.Store(time.Now().UnixNano())
	chunk.state.Store(int32(ChunkStateEmpty))

	r.chunks[chunkIdx] = chunk
	r.totalChunks.Add(1)

	// Track memory usage
	r.memoryUsage.Add(size)

	return chunk
}

// evictOldChunksLocked evicts the oldest chunks (LRU)
// MUST be called with chunksMu write lock held!
func (r *Reader) evictOldChunksLocked() {
	// Calculate current read position for smart eviction
	currentPos := r.lastReadOffset.Load()
	currentChunkIdx := currentPos / r.chunkSize

	// Find oldest chunks, but keep:
	// 1. Current chunk
	// 2. Chunks currently downloading
	// 3. Chunks within read-ahead window
	type chunkAge struct {
		idx        int64
		accessTime int64 // Unix nano timestamp
		size       int64 // Chunk size for memory tracking
	}

	var candidates []chunkAge
	readAheadChunks := (r.readAheadSize / r.chunkSize) + 1

	for idx, chunk := range r.chunks {
		// Don't evict downloading chunks
		if chunk.state.Load() == int32(ChunkStateDownloading) {
			continue
		}

		// Don't evict chunks in read-ahead window
		if idx >= currentChunkIdx && idx <= currentChunkIdx+readAheadChunks {
			continue
		}

		// GetReader access time atomically
		accessTime := chunk.lastAccess.Load()
		candidates = append(candidates, chunkAge{idx: idx, accessTime: accessTime, size: chunk.size})
	}

	// Sort by access time (oldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].accessTime < candidates[j].accessTime
	})

	// Determine how much to evict based on memory pressure
	currentMemory := r.memoryUsage.Load()
	targetMemory := int64(float64(r.memoryLimit) * 0.7) // Evict down to 70% to avoid thrashing

	var memoryToFree int64
	if currentMemory > targetMemory {
		memoryToFree = currentMemory - targetMemory
	}

	// Also use chunk count as secondary constraint
	maxChunkEvictions := len(r.chunks) / 4
	if maxChunkEvictions < 1 {
		maxChunkEvictions = 1
	}

	evicted := 0
	memoryFreed := int64(0)

	for _, ca := range candidates {
		// Stop if we've freed enough memory AND evicted enough chunks
		if memoryFreed >= memoryToFree && evicted >= 1 {
			break
		}

		// Don't evict too many chunks at once
		if evicted >= maxChunkEvictions && memoryFreed >= memoryToFree {
			break
		}

		chunk := r.chunks[ca.idx]
		if chunk == nil {
			continue
		}

		// SAFE EVICTION: Only evict if refCount is 0 (no active readers)
		if chunk.refCount.Load() > 0 {
			continue // Skip - still being read
		}

		// Return buffer to pool (only for full-size buffers!)
		if chunk.state.Load() == int32(ChunkStateComplete) {
			// Only return full-size buffers to pool (avoid memory waste)
			if chunk.size == chunk.chunkSize {
				r.chunkPool.Put(&chunk.data)
			}
			// Partial chunks will be GC'd naturally
		}

		// Track memory freed
		memoryFreed += chunk.size

		// Remove from map
		delete(r.chunks, ca.idx)
		r.totalChunks.Add(-1)
		evicted++
	}

	// Update memory usage atomically
	if memoryFreed > 0 {
		r.memoryUsage.Add(-memoryFreed)
	}
}

// startChunkDownload initiates download for a chunk
func (r *Reader) startChunkDownload(chunk *Chunk) {
	if !chunk.state.CompareAndSwap(int32(ChunkStateEmpty), int32(ChunkStateDownloading)) {
		return // Already downloading
	}

	r.downloader.DownloadChunk(chunk)
}

// readFromChunk reads data from a chunk, waiting for download if necessary
func (r *Reader) readFromChunk(chunk *Chunk, p []byte, offsetInChunk int64) (int, error) {
	// Increment refcount (SAFE EVICTION - prevent eviction during read)
	chunk.refCount.Add(1)
	defer chunk.refCount.Add(-1)

	// Calculate how much we can read from this chunk
	availableInChunk := chunk.size - offsetInChunk
	toRead := int64(len(p))
	if toRead > availableInChunk {
		toRead = availableInChunk
	}

	if toRead <= 0 {
		return 0, nil
	}

	// Wait for required data to be downloaded
	endOffsetInChunk := offsetInChunk + toRead

	for {
		state := ChunkState(chunk.state.Load())
		bytesWritten := chunk.bytesWritten.Load()

		switch state {
		case ChunkStateComplete:
			// Chunk fully downloaded - read it
			chunk.dataMu.RLock()
			n := copy(p[:toRead], chunk.data[offsetInChunk:offsetInChunk+toRead])
			chunk.dataMu.RUnlock()

			r.stats.CacheHits.Add(1)
			return n, nil

		case ChunkStateDownloading:
			// Check if we have enough data
			if bytesWritten >= endOffsetInChunk {
				// Sufficient data available - progressive read!
				chunk.dataMu.RLock()
				n := copy(p[:toRead], chunk.data[offsetInChunk:offsetInChunk+toRead])
				chunk.dataMu.RUnlock()

				if r.isSequential.Load() {
					r.stats.ReadAheadHits.Add(1)
				}
				return n, nil
			}

			// Wait for more data using sync.Cond (NO POLLING!)
			chunk.waiters.Add(1)

			// Use a goroutine to handle context cancellation
			done := make(chan struct{})
			go func() {
				chunk.progressMu.Lock()
				chunk.progressCond.Wait() // Efficient wait - no CPU spinning!
				chunk.progressMu.Unlock()
				close(done)
			}()

			select {
			case <-chunk.ready:
				chunk.waiters.Add(-1)
				// Chunk completed, loop to read
				continue
			case <-done:
				chunk.waiters.Add(-1)
				// More data arrived, check again
				continue
			case <-r.ctx.Done():
				chunk.waiters.Add(-1)
				return 0, r.ctx.Err()
			}

		case ChunkStateError:
			// Download failed
			if err := chunk.downloadErr.Load(); err != nil {
				return 0, err.(error)
			}
			return 0, fmt.Errorf("chunk download failed")

		case ChunkStateEmpty:
			// Should not happen (startChunkDownload should be called first)
			return 0, fmt.Errorf("chunk not initialized")
		}
	}
}

// Close closes the reader and cleans up resources
func (r *Reader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}

	r.cancel()
	err := r.downloader.Close()
	if err != nil {
		return err
	}
	r.wg.Wait()

	// Return ALL buffers to pool (critical for memory cleanup!)
	r.chunksMu.Lock()
	memoryFreed := int64(0)
	for idx, chunk := range r.chunks {
		// Return buffer to pool regardless of state
		// This prevents memory leaks when reader is closed
		if chunk != nil && chunk.data != nil {
			r.chunkPool.Put(&chunk.data)
			memoryFreed += chunk.size
		}
		delete(r.chunks, idx)
	}
	r.totalChunks.Store(0)
	r.memoryUsage.Store(0)
	r.chunksMu.Unlock()

	// Close disk cache (note: cache is shared across readers, so don't close it here)
	// The manager will handle closing the shared disk cache

	return nil
}

// GetStats returns reader statistics
func (r *Reader) GetStats() map[string]interface{} {
	r.chunksMu.RLock()
	totalChunks := len(r.chunks)
	var completeChunks, downloadingChunks int
	for _, chunk := range r.chunks {
		state := ChunkState(chunk.state.Load())
		if state == ChunkStateComplete {
			completeChunks++
		} else if state == ChunkStateDownloading {
			downloadingChunks++
		}
	}
	r.chunksMu.RUnlock()

	memoryUsage := r.memoryUsage.Load()
	memoryLimit := r.memoryLimit
	memoryUtilization := float64(0)
	if memoryLimit > 0 {
		memoryUtilization = float64(memoryUsage) / float64(memoryLimit) * 100
	}

	return map[string]interface{}{
		"bytes_read":          r.stats.BytesRead.Load(),
		"chunks_total":        totalChunks,
		"chunks_complete":     completeChunks,
		"chunks_downloading":  downloadingChunks,
		"cache_hits":          r.stats.CacheHits.Load(),
		"cache_misses":        r.stats.CacheMisses.Load(),
		"sequential_reads":    r.stats.SequentialReads.Load(),
		"random_reads":        r.stats.RandomReads.Load(),
		"readahead_hits":      r.stats.ReadAheadHits.Load(),
		"is_sequential":       r.isSequential.Load(),
		"memory_usage_bytes":  memoryUsage,
		"memory_usage_mb":     memoryUsage / (1024 * 1024),
		"memory_limit_bytes":  memoryLimit,
		"memory_limit_mb":     memoryLimit / (1024 * 1024),
		"memory_utilization":  fmt.Sprintf("%.1f%%", memoryUtilization),
		"max_chunks":          r.maxChunks,
	}
}
