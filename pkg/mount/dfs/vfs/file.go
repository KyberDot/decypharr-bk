package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// Buffer pool for streaming downloads (128KB buffers)
var downloadBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 128*1024) // 128KB
		return &buf
	},
}

// downloadJob tracks an active chunk download with progressive updates
type downloadJob struct {
	chunkStart int64
	chunkSize  int64
	done       chan error
	mu         sync.Mutex
	completed  bool

	// Progressive download support
	bytesDownloaded atomic.Int64  // How much has been downloaded so far
	minThreshold    int64         // Minimum bytes before early return
	thresholdReady  chan struct{} // Signals when minThreshold is reached
	thresholdOnce   sync.Once     // Ensures threshold signal sent only once
}

// PlaybackState tracks sequential read patterns for optimization
type PlaybackState struct {
	lastReadOffset     atomic.Int64                    // Last read position
	sequentialReads    atomic.Int64                    // Count of sequential reads
	activeStreamReader atomic.Pointer[StreamingReader] // Current stream reader
	playbackActive     atomic.Bool                     // Is this file being actively played?
	lastAccessTime     atomic.Int64                    // When was this file last accessed
	mu                 sync.RWMutex                    // Protects stream reader updates
}

// File represents a cached remote file with integrated download capabilities
// Combines sparse file tracking, download logic, and prefetching in a single efficient struct
type File struct {
	// File metadata
	path      string
	info      *manager.FileInfo
	chunkSize int64

	// On-disk file and range tracking
	file       *os.File
	ranges     *common.Ranges // Lazy-loaded from metadata
	rangesOnce sync.Once      // Ensures ranges are loaded exactly once

	// Memory buffer (ultra-fast memory-first caching)
	memBuffer *MemoryBuffer

	// Download configuration
	readAhead int64

	// Active downloads tracking
	downloading *xsync.Map[int64, *downloadJob]

	// OPTIMIZATION: Playback state tracking
	playbackState *PlaybackState

	// Stats and lifecycle
	stats           *StatsTracker
	manager         *manager.Manager // Reference to save metadata
	vfsManager      *Manager         // Reference to VFS manager
	lastAccess      atomic.Int64
	modTime         time.Time
	dirty           bool // Has unflushed changes to metadata
	bytesDownloaded atomic.Int64

	// Lifecycle management
	mu        sync.RWMutex
	closeOnce sync.Once
	closeChan chan struct{} // Signals all goroutines to stop
	closed    atomic.Bool   // Indicates if file is closed
}

// newFile creates or opens a cached file with download capabilities
func newFile(cacheDir string, info *manager.FileInfo, chunkSize, readAhead int64, stats *StatsTracker, manager *manager.Manager, vfsManager *Manager) (*File, error) {
	sanitizedFileName := sanitizeForPath(info.Name())
	torrentDir := filepath.Join(cacheDir, sanitizeForPath(info.Parent()))
	cachePath := filepath.Join(torrentDir, sanitizedFileName)

	// Ensure directory exists
	if err := os.MkdirAll(torrentDir, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	// Open or create file
	file, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}

	// Set file size (sparse allocation)
	if err := file.Truncate(info.Size()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate cache file: %w", err)
	}

	// Create memory buffer for ultra-fast access
	// Use context that will be cancelled when file closes
	bufferCtx := context.Background()
	memBufferSize := vfsManager.config.BufferSize
	if memBufferSize <= 0 {
		// OPTIMIZED: Increased from 16MB to 32MB for better streaming buffering
		// 32MB provides better buffering for high-bitrate content
		memBufferSize = 32 * 1024 * 1024 // Default 32MB if not configured
	}
	memBuffer := NewMemoryBuffer(bufferCtx, memBufferSize, chunkSize)
	memBuffer.AttachFile(file) // Attach for async flushing

	f := &File{
		path:          cachePath,
		info:          info,
		chunkSize:     chunkSize,
		file:          file,
		ranges:        nil, // Lazy-loaded via rangesOnce
		memBuffer:     memBuffer,
		readAhead:     readAhead,
		downloading:   xsync.NewMap[int64, *downloadJob](),
		stats:         stats,
		manager:       manager,
		vfsManager:    vfsManager,
		modTime:       info.ModTime(),
		dirty:         false,
		closeChan:     make(chan struct{}),
		playbackState: &PlaybackState{},
	}

	f.lastAccess.Store(time.Now().UnixNano())
	f.playbackState.lastReadOffset.Store(-1)
	f.playbackState.lastAccessTime.Store(time.Now().UnixNano())

	return f, nil
}

func (f *File) touchAccess() {
	now := time.Now().UnixNano()
	f.lastAccess.Store(now)
	f.playbackState.lastAccessTime.Store(now)
}

func (f *File) lastAccessTime() time.Time {
	ts := f.lastAccess.Load()
	if ts == 0 {
		return time.Time{}
	}
	return time.Unix(0, ts)
}

// isSequentialRead checks if this read follows the previous read (indicating streaming playback)
func (f *File) isSequentialRead(offset int64, size int64) bool {
	lastOffset := f.playbackState.lastReadOffset.Load()
	if lastOffset == -1 {
		// First read
		return true
	}

	// Consider sequential if this read starts within 1 chunk of where the last read ended
	// This handles slight overlaps or gaps that video players sometimes create
	tolerance := f.chunkSize
	expectedOffset := lastOffset

	return offset >= (expectedOffset-tolerance) && offset <= (expectedOffset+tolerance)
}

// updatePlaybackState updates playback tracking after a read
func (f *File) updatePlaybackState(offset int64, bytesRead int) {
	f.playbackState.lastReadOffset.Store(offset + int64(bytesRead))

	if f.isSequentialRead(offset, int64(bytesRead)) {
		// Sequential read detected
		current := f.playbackState.sequentialReads.Add(1)
		if current >= 3 {
			// 3+ sequential reads = active playback
			f.playbackState.playbackActive.Store(true)
		}
	} else {
		// Non-sequential read (seek) - reset counter but don't disable playback immediately
		f.playbackState.sequentialReads.Store(0)
		// Keep playbackActive true for a while to handle seeks during playback
	}
}

// ensureFileOpen lazily (re)opens the on-disk sparse file if it was closed.
func (f *File) ensureFileOpen() error {
	f.mu.RLock()
	if f.file != nil {
		f.mu.RUnlock()
		return nil
	}
	f.mu.RUnlock()

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		return nil
	}

	file, err := os.OpenFile(f.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen cache file: %w", err)
	}
	f.file = file

	if f.memBuffer != nil {
		f.memBuffer.AttachFile(file)
	}

	return nil
}

// ensureRangesLoaded ensures ranges are loaded exactly once (thread-safe)
func (f *File) ensureRangesLoaded() {
	f.rangesOnce.Do(func() {
		f.ranges = common.NewRanges()
		if !f.loadCachedMetadata() {
			_ = f.scanExistingData()
		}
	})
}

func (f *File) loadCachedMetadata() bool {
	if f.vfsManager == nil {
		return false
	}

	meta, err := f.vfsManager.loadMetadata(f.info.Parent(), f.info.Name())
	if err != nil || meta == nil {
		return false
	}

	for _, r := range meta.Ranges {
		f.ranges.Insert(r)
	}

	if !meta.ATime.IsZero() {
		f.lastAccess.Store(meta.ATime.UnixNano())
	}
	if !meta.ModTime.IsZero() {
		f.modTime = meta.ModTime
	}

	var cached int64
	for _, r := range meta.Ranges {
		cached += r.Size
	}
	if cached > 0 {
		f.bytesDownloaded.Store(cached)
	}

	f.mu.Lock()
	f.dirty = meta.Dirty
	f.mu.Unlock()

	return true
}

// scanExistingData scans the file to detect which chunks already have data
// This is a fallback when metadata doesn't exist
func (f *File) scanExistingData() error {
	// Read file in chunks and check if they contain non-zero data
	// This is a heuristic - we check a few bytes per chunk for performance

	numChunks := (f.info.Size() + f.chunkSize - 1) / f.chunkSize
	sample := make([]byte, 4096) // Sample first 4KB of each chunk

	for chunkIdx := int64(0); chunkIdx < numChunks; chunkIdx++ {
		offset := chunkIdx * f.chunkSize
		readSize := int64(len(sample))
		if offset+readSize > f.info.Size() {
			readSize = f.info.Size() - offset
		}

		n, err := f.file.ReadAt(sample[:readSize], offset)
		if err != nil && n == 0 {
			continue // Chunk likely not downloaded
		}

		// Check if sample contains non-zero bytes
		hasData := false
		for i := 0; i < n; i++ {
			if sample[i] != 0 {
				hasData = true
				break
			}
		}

		if hasData {
			// Mark entire chunk as present
			chunkEnd := offset + f.chunkSize
			if chunkEnd > f.info.Size() {
				chunkEnd = f.info.Size()
			}
			f.ranges.Insert(common.Range{
				Pos:  offset,
				Size: chunkEnd - offset,
			})
		}
	}

	return nil
}

// readAt reads from cache if data is available, returns false if not cached
// Supports partial reads: if only part of the requested range is cached, returns what's available
func (f *File) readAt(p []byte, offset int64) (n int, cached bool, err error) {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	if err := f.ensureFileOpen(); err != nil {
		return 0, false, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	f.touchAccess()

	requestedRange := common.Range{Pos: offset, Size: int64(len(p))}

	// Check if requested range is fully cached
	if f.ranges.Present(requestedRange) {
		// Fully cached - read everything
		n, err = f.file.ReadAt(p, offset)
		if err != nil {
			return n, false, err
		}
		return n, true, nil
	}

	// Not fully cached - check if we can do a partial read
	// Find what gaps exist in the requested range
	missing := f.ranges.FindMissing(requestedRange)
	if len(missing) == 0 {
		// Shouldn't happen (Present() returned false but no missing ranges)
		// Treat as fully cached
		n, err = f.file.ReadAt(p, offset)
		if err != nil {
			return n, false, err
		}
		return n, true, nil
	}

	// Check if there's cached data at the beginning of the request
	firstMissing := missing[0]
	if firstMissing.Pos > offset {
		// We have cached data from offset to firstMissing.Pos
		availableSize := firstMissing.Pos - offset
		n, err = f.file.ReadAt(p[:availableSize], offset)
		if err != nil && err != io.EOF {
			return n, false, err
		}
		return n, true, nil // Partial read success
	}

	// No cached data at the start of the requested range
	return 0, false, nil
}

// WriteAt writes data and updates the range map
func (f *File) WriteAt(p []byte, offset int64) (int, error) {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	if err := f.ensureFileOpen(); err != nil {
		return 0, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Write to file
	n, err := f.file.WriteAt(p, offset)
	if err != nil {
		return n, err
	}

	// Mark this range as cached
	if n > 0 {
		f.ranges.Insert(common.Range{
			Pos:  offset,
			Size: int64(n),
		})
		f.dirty = true
	}

	f.touchAccess()
	return n, nil
}

// Sync flushes data to disk
func (f *File) Sync() error {
	if err := f.ensureFileOpen(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Sync file data
	if f.file != nil {
		return f.file.Sync()
	}

	return nil
}

// closeFD closes the file descriptor
func (f *File) closeFD() error {
	if f.memBuffer != nil {
		_ = f.memBuffer.Flush()
		f.memBuffer.AttachFile(nil)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		err := f.file.Close()
		f.file = nil
		return err
	}
	return nil
}

// Close closes the file and stops all downloads
func (f *File) Close() error {
	var firstErr error

	// Close any active streaming reader
	if sr := f.playbackState.activeStreamReader.Load(); sr != nil {
		_ = sr.Close()
	}

	if err := f.persistMetadata(true); err != nil {
		firstErr = err
	}

	_ = f.CloseDownloads()

	if err := f.closeFD(); err != nil && firstErr == nil {
		firstErr = err
	}

	if f.memBuffer != nil {
		if err := f.memBuffer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// removeFromDisk removes the sparse file from disk
func (f *File) removeFromDisk() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close file if open
	if f.file != nil {
		_ = f.file.Close()
		f.file = nil
	}

	// Remove sparse file
	if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (f *File) persistMetadata(force bool) error {
	if f.vfsManager == nil {
		return nil
	}

	f.ensureRangesLoaded()

	f.mu.RLock()
	dirty := f.dirty
	modTime := f.modTime
	f.mu.RUnlock()

	if !dirty && !force {
		return nil
	}

	meta := &Metadata{
		ModTime: modTime,
		ATime:   f.lastAccessTime(),
		Size:    f.info.Size(),
		Ranges:  f.ranges.GetRanges(),
		Dirty:   false,
	}

	if meta.ModTime.IsZero() {
		meta.ModTime = time.Now()
	}
	if meta.ATime.IsZero() {
		meta.ATime = time.Now()
	}

	if err := f.vfsManager.saveMetadata(f.info.Parent(), f.info.Name(), meta); err != nil {
		return err
	}

	f.mu.Lock()
	f.dirty = false
	f.mu.Unlock()

	return nil
}

// GetCachedSize returns the total bytes downloaded
func (f *File) GetCachedSize() int64 {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return 0
	}
	return f.ranges.Size()
}

// GetCachedRanges returns all cached ranges (for debugging/stats)
func (f *File) GetCachedRanges() []common.Range {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return nil
	}
	return f.ranges.GetRanges()
}

// FindMissing returns ranges that need to be downloaded
func (f *File) FindMissing(offset, length int64) []common.Range {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		// If ranges failed to load, assume everything is missing
		return []common.Range{{Pos: offset, Size: length}}
	}

	return f.ranges.FindMissing(common.Range{
		Pos:  offset,
		Size: length,
	})
}

// IsCached checks if a range is cached WITHOUT allocating buffers
// This is much more efficient than readAt for cache checks
func (f *File) IsCached(offset, length int64) bool {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return false
	}

	return f.ranges.Present(common.Range{
		Pos:  offset,
		Size: length,
	})
}

// OPTIMIZED: ReadAt with persistent streaming and better caching
func (f *File) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	size := f.info.Size()
	if offset >= size {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if offset+readSize > size {
		readSize = size - offset
		p = p[:readSize]
	}

	f.touchAccess()

	// FAST PATH: Check memory buffer first (< 1ms)
	data, found := f.memBuffer.Get(offset, readSize)
	if found {
		// Memory hit! Ultra-fast return
		n := copy(p, data)
		f.updatePlaybackState(offset, n)
		// Trigger prefetch in background for active playback
		if f.playbackState.playbackActive.Load() {
			go f.intelligentPrefetch(ctx, offset+readSize)
		}
		return n, nil
	}

	// Check if fully cached on disk (zero-allocation check)
	if f.IsCached(offset, readSize) {
		// Disk cache hit - read and store in memory for next time
		n, _, err := f.readAt(p, offset)
		if err == nil {
			// Store in memory buffer for future fast access
			if f.memBuffer != nil && n > 0 {
				_ = f.memBuffer.Put(offset, p[:n])
			}
			f.updatePlaybackState(offset, n)

			// Trigger prefetch for active playback
			if f.playbackState.playbackActive.Load() {
				go f.intelligentPrefetch(ctx, offset+readSize)
			}
		}
		return n, err
	}

	// Cache miss - need to download
	f.stats.TrackActiveRead(1)
	defer f.stats.TrackActiveRead(-1)

	// For sequential reads during active playback, use persistent streaming
	if f.isSequentialRead(offset, readSize) && f.playbackState.playbackActive.Load() {
		return f.persistentStreamingRead(ctx, p, offset)
	}

	// For non-sequential reads, use optimized chunk-based download
	return f.chunkBasedRead(ctx, p, offset)
}

// persistentStreamingRead uses a persistent StreamingReader for continuous playback
func (f *File) persistentStreamingRead(ctx context.Context, p []byte, offset int64) (int, error) {
	// Try to reuse existing streaming reader
	currentSR := f.playbackState.activeStreamReader.Load()

	if currentSR != nil && currentSR.CanServe(offset, int64(len(p))) {
		// Reuse existing reader with ReadAt for efficient seeking
		n, err := currentSR.ReadAt(p, offset)
		if err == nil {
			// Store in memory buffer
			if f.memBuffer != nil && n > 0 {
				_ = f.memBuffer.Put(offset, p[:n])
			}
			f.updatePlaybackState(offset, n)
			return n, nil
		}

		// Error with current reader, will create new one below
		_ = currentSR.Close()
		f.playbackState.activeStreamReader.Store(nil)
	}

	// Create new streaming reader for larger range
	// For video playback, read ahead more aggressively
	readAheadSize := f.readAhead
	if readAheadSize < 32*1024*1024 { // Minimum 32MB for smooth video
		readAheadSize = 32 * 1024 * 1024
	}

	endOffset := offset + readAheadSize
	if endOffset > f.info.Size() {
		endOffset = f.info.Size()
	}

	newSR := NewStreamingReader(ctx, f.manager, f.info, offset, endOffset, f)
	f.playbackState.activeStreamReader.Store(newSR)

	// Read from new streaming reader
	n, err := newSR.ReadAt(p, offset)
	if err == nil {
		// Store in memory buffer
		if f.memBuffer != nil && n > 0 {
			_ = f.memBuffer.Put(offset, p[:n])
		}
		f.updatePlaybackState(offset, n)

		// Start background prefetch
		go f.intelligentPrefetch(ctx, offset+int64(n))
	}

	return n, err
}

// chunkBasedRead handles non-sequential reads efficiently
func (f *File) chunkBasedRead(ctx context.Context, p []byte, offset int64) (int, error) {
	// Use the existing streamingReadAt but with improvements
	n, err := f.streamingReadAt(ctx, p, offset)
	if err == nil {
		// Store in memory buffer for future fast access
		if f.memBuffer != nil && n > 0 {
			_ = f.memBuffer.Put(offset, p[:n])
		}
		f.updatePlaybackState(offset, n)
	}
	return n, err
}

// intelligentPrefetch downloads chunks ahead intelligently based on playback patterns
func (f *File) intelligentPrefetch(ctx context.Context, currentOffset int64) {
	if f.closed.Load() || !f.playbackState.playbackActive.Load() {
		return
	}

	// More intelligent prefetch based on playback state
	sequentialCount := f.playbackState.sequentialReads.Load()

	// Determine prefetch aggressiveness based on access pattern
	var numChunks int64
	if sequentialCount >= 5 {
		// Heavy sequential access - prefetch more aggressively
		numChunks = 4 // 32MB with 8MB chunks
	} else if sequentialCount >= 3 {
		// Moderate sequential access
		numChunks = 2 // 16MB
	} else {
		// Light prefetch
		numChunks = 1 // 8MB
	}

	// Limit based on configured read ahead
	if f.readAhead > 0 {
		maxChunks := (f.readAhead + f.chunkSize - 1) / f.chunkSize
		if numChunks > maxChunks {
			numChunks = maxChunks
		}
	}

	startChunk := currentOffset / f.chunkSize
	endChunk := startChunk + numChunks
	totalChunks := (f.info.Size() + f.chunkSize - 1) / f.chunkSize
	if endChunk > totalChunks {
		endChunk = totalChunks
	}

	// Download chunks with limited concurrency to prevent memory explosion
	semaphore := make(chan struct{}, 2) // Max 2 concurrent downloads

	for chunkIdx := startChunk; chunkIdx < endChunk; chunkIdx++ {
		select {
		case <-f.closeChan:
			return
		case semaphore <- struct{}{}:
			go func(idx int64) {
				defer func() { <-semaphore }()
				f.downloadChunkAsync(ctx, idx)
			}(chunkIdx)
		}
	}
}

// downloadChunkAsync downloads a chunk in background (non-blocking)
func (f *File) downloadChunkAsync(ctx context.Context, chunkIdx int64) {
	// Check if file is closed
	if f.closed.Load() {
		return
	}

	// Check if already cached (zero-allocation check)
	chunkStart := chunkIdx * f.chunkSize
	chunkEnd := chunkStart + f.chunkSize
	if chunkEnd > f.info.Size() {
		chunkEnd = f.info.Size()
	}
	if f.IsCached(chunkStart, chunkEnd-chunkStart) {
		return // Already cached
	}

	// Check if already downloading
	if _, exists := f.downloading.Load(chunkIdx); exists {
		return // Already downloading
	}

	// Start download
	_ = f.downloadChunk(ctx, chunkIdx)
}

// downloadChunk downloads a specific chunk (blocking) using fixed chunk size
func (f *File) downloadChunk(ctx context.Context, chunkIdx int64) error {
	chunkStart := chunkIdx * f.chunkSize
	chunkEnd := chunkStart + f.chunkSize
	if chunkEnd > f.info.Size() {
		chunkEnd = f.info.Size()
	}
	actualSize := chunkEnd - chunkStart

	// Check if already downloading - if so, wait for it
	if job, exists := f.downloading.Load(chunkIdx); exists {
		return <-job.done // Wait for existing download to complete
	}

	// Create download job with progressive support
	job := &downloadJob{
		chunkStart:     chunkStart,
		chunkSize:      actualSize,
		done:           make(chan error, 1),
		minThreshold:   0,
		thresholdReady: make(chan struct{}),
	}

	// Try to store job (race condition safe)
	actual, loaded := f.downloading.LoadOrStore(chunkIdx, job)
	if loaded {
		return <-actual.done
	}

	// Start background download
	go func() {
		err := f.doDownloadWithJob(ctx, job)
		job.mu.Lock()
		job.completed = true
		job.mu.Unlock()
		job.done <- err
		close(job.done)

		// Delete immediately after completion
		f.downloading.Delete(chunkIdx)

		if err == nil {
			f.bytesDownloaded.Add(actualSize)
		}
	}()
	// No threshold, wait for completion
	return <-job.done
}

// doDownloadWithJob performs the actual HTTP download with optional progressive signaling
func (f *File) doDownloadWithJob(ctx context.Context, job *downloadJob) error {
	offset := job.chunkStart
	size := job.chunkSize
	end := offset + size - 1

	// Implement retry logic for robustness
	var rc io.ReadCloser
	var err error

	for retries := 0; retries < 3; retries++ {
		rc, err = f.manager.StreamReader(ctx, f.info.Parent(), f.info.Name(), offset, end)
		if err == nil {
			break
		}

		// Brief delay before retry
		select {
		case <-time.After(time.Duration(retries+1) * 100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err != nil {
		return fmt.Errorf("get download link after retries: %w", err)
	}

	defer func(rc io.ReadCloser) {
		_ = rc.Close()
	}(rc)

	// GetReader buffer from pool (128KB)
	bufPtr := downloadBufferPool.Get().(*[]byte)
	buffer := *bufPtr
	defer downloadBufferPool.Put(bufPtr)

	// Allocate write buffer (512KB) - batch multiple reads before writing
	// This reduces lock acquisitions from 64 to 16 per 8MB chunk
	const writeBatchSize = 512 * 1024 // 512KB
	writeBuf := make([]byte, 0, writeBatchSize)
	currentOffset := offset
	totalRead := int64(0)
	thresholdSignaled := false

	for totalRead < size {
		// Check if file was closed
		if f.closed.Load() {
			return fmt.Errorf("file closed during download")
		}

		// Read from stream into pooled buffer
		n, err := rc.Read(buffer)
		if n > 0 {
			// Append to write buffer
			remaining := size - totalRead
			toWrite := int64(n)
			if toWrite > remaining {
				toWrite = remaining
			}

			writeBuf = append(writeBuf, buffer[:toWrite]...)

			// Write in batches to reduce lock contention
			if len(writeBuf) >= writeBatchSize || totalRead+toWrite >= size || err == io.EOF {
				written, writeErr := f.WriteAt(writeBuf, currentOffset)
				if writeErr != nil {
					return fmt.Errorf("write to file at offset %d: %w", currentOffset, writeErr)
				}

				currentOffset += int64(written)
				totalRead += int64(written)
				writeBuf = writeBuf[:0] // Reset buffer but keep capacity

				// Progressive download: signal when threshold reached
				if job != nil && !thresholdSignaled {
					job.bytesDownloaded.Store(totalRead)
					if totalRead >= job.minThreshold {
						job.thresholdOnce.Do(func() {
							close(job.thresholdReady)
						})
						thresholdSignaled = true
					}
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			// Partial data already written incrementally above
			// Return error so caller knows download is incomplete
			return fmt.Errorf("read chunk at offset %d (downloaded %d/%d bytes): %w",
				currentOffset, totalRead, size, err)
		}
	}

	// Ensure threshold is signaled even if we completed before reaching it
	if job != nil && !thresholdSignaled {
		job.bytesDownloaded.Store(totalRead)
		job.thresholdOnce.Do(func() {
			close(job.thresholdReady)
		})
	}

	return nil
}

// CloseDownloads stops all download goroutines
func (f *File) CloseDownloads() error {
	// Signal all goroutines to stop (only once)
	f.closeOnce.Do(func() {
		f.closed.Store(true)
		close(f.closeChan)
	})
	return nil
}

// Stats returns download statistics
func (f *File) Stats() map[string]interface{} {
	return map[string]interface{}{
		"bytes_downloaded":  f.bytesDownloaded.Load(),
		"active_downloads":  f.downloading.Size(),
		"chunk_size":        f.chunkSize,
		"read_ahead":        f.readAhead,
		"cached_size":       f.GetCachedSize(),
		"total_size":        f.info.Size(),
		"sequential_reads":  f.playbackState.sequentialReads.Load(),
		"playback_active":   f.playbackState.playbackActive.Load(),
		"has_active_stream": f.playbackState.activeStreamReader.Load() != nil,
	}
}

// streamingReadAt performs a streaming read using the ring buffer
// This is optimized for sequential playback with instant startup
// IMPORTANT: Only reads what's requested, does NOT download entire file
func (f *File) streamingReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	// Calculate read size
	readSize := int64(len(p))
	if offset+readSize > f.info.Size() {
		readSize = f.info.Size() - offset
	}

	// Create streaming reader - will only download what we need
	endOffset := offset + readSize - 1
	sr := NewStreamingReader(ctx, f.manager, f.info, offset, endOffset, f)
	defer func() {
		// Close immediately to stop background downloading
		_ = sr.Close()
	}()

	// Read all data from stream (ONLY the requested amount)
	totalRead := 0
	for totalRead < len(p) {
		n, err := sr.Read(p[totalRead:])
		totalRead += n

		if err != nil {
			if err == io.EOF {
				// EOF is expected when we've read all available data
				if totalRead > 0 {
					return totalRead, nil
				}
				return 0, io.EOF
			}
			// Return actual error with any data read so far
			if totalRead > 0 {
				return totalRead, nil
			}
			return 0, err
		}

		// Check if we've read enough
		if totalRead >= len(p) || totalRead >= int(readSize) {
			// Got what we needed - return immediately
			// Close will stop background download
			break
		}

		// Check context cancellation
		if ctx.Err() != nil {
			if totalRead > 0 {
				return totalRead, nil
			}
			return 0, ctx.Err()
		}
	}

	return totalRead, nil
}
