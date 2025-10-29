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
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
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
	bytesDownloaded atomic.Int64 // How much has been downloaded so far
	minThreshold    int64         // Minimum bytes before early return
	thresholdReady  chan struct{} // Signals when minThreshold is reached
	thresholdOnce   sync.Once     // Ensures threshold signal sent only once
}

// File represents a cached remote file with integrated download capabilities
// Combines sparse file tracking, download logic, and prefetching in a single efficient struct
type File struct {
	// File metadata
	path        string
	torrentName string
	fileName    string
	fileLink    string
	size        int64
	chunkSize   int64

	// On-disk file and range tracking
	file       *os.File
	ranges     *ranges.Ranges // Lazy-loaded from metadata
	rangesOnce sync.Once      // Ensures ranges are loaded exactly once

	// Download configuration
	debrid    *store.Cache
	readAhead int64

	// Active downloads tracking
	downloading *xsync.Map[int64, *downloadJob]

	// Stats and lifecycle
	stats           *StatsTracker
	manager         *Manager // Reference to save metadata
	lastAccess      time.Time
	modTime         time.Time
	dirty           bool // Has unflushed changes to metadata
	bytesDownloaded atomic.Int64

	// Lifecycle management
	mu        sync.RWMutex
	closeOnce sync.Once
	closeChan chan struct{} // Signals all goroutines to stop
	closed    atomic.Bool   // Indicates if file is closed
}

// SparseFile is an alias for File to maintain backward compatibility during transition
type SparseFile = File

// newFile creates or opens a cached file with download capabilities
func newFile(cacheDir string, debridCache *store.Cache, torrentName string, torrentFile types.File, chunkSize, readAhead int64, stats *StatsTracker, manager *Manager) (*File, error) {
	sanitizedFileName := sanitizeForPath(torrentFile.Name)
	torrentDir := filepath.Join(cacheDir, sanitizeForPath(torrentName))
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
	if err := file.Truncate(torrentFile.Size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate cache file: %w", err)
	}

	f := &File{
		path:        cachePath,
		torrentName: torrentName,
		fileName:    torrentFile.Name,
		fileLink:    torrentFile.Link,
		size:        torrentFile.Size,
		chunkSize:   chunkSize,
		file:        file,
		ranges:      nil, // Lazy-loaded via rangesOnce
		debrid:      debridCache,
		readAhead:   readAhead,
		downloading: xsync.NewMap[int64, *downloadJob](),
		stats:       stats,
		manager:     manager,
		lastAccess:  time.Now(),
		modTime:     time.Now(),
		dirty:       false,
		closeChan:   make(chan struct{}),
	}

	return f, nil
}

// newSparseFile is a compatibility wrapper for existing code
func newSparseFile(cacheDir, torrentName, fileName string, size, chunkSize int64, stats *StatsTracker, manager *Manager) (*SparseFile, error) {
	// Create a minimal File for backward compatibility
	torrentFile := types.File{
		Name: fileName,
		Size: size,
		Link: "", // Not needed for basic sparse file operations
	}
	return newFile(cacheDir, nil, torrentName, torrentFile, chunkSize, 0, stats, manager)
}

// loadRanges lazy-loads ranges by scanning the actual file
// Ranges are always rebuilt from actual data (fast enough with sampling)
// This should only be called via ensureRangesLoaded() which uses sync.Once
func (f *SparseFile) loadRanges() {
	// Scan existing file to rebuild ranges
	f.ranges = ranges.New()
	_ = f.scanExistingData() // Ignore errors, ranges will be empty on failure
}

// ensureRangesLoaded ensures ranges are loaded exactly once (thread-safe)
func (f *SparseFile) ensureRangesLoaded() {
	f.rangesOnce.Do(f.loadRanges)
}

// scanExistingData scans the file to detect which chunks already have data
// This is a fallback when metadata doesn't exist
func (f *SparseFile) scanExistingData() error {
	// Read file in chunks and check if they contain non-zero data
	// This is a heuristic - we check a few bytes per chunk for performance

	numChunks := (f.size + f.chunkSize - 1) / f.chunkSize
	sample := make([]byte, 4096) // Sample first 4KB of each chunk

	for chunkIdx := int64(0); chunkIdx < numChunks; chunkIdx++ {
		offset := chunkIdx * f.chunkSize
		readSize := int64(len(sample))
		if offset+readSize > f.size {
			readSize = f.size - offset
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
			if chunkEnd > f.size {
				chunkEnd = f.size
			}
			f.ranges.Insert(ranges.Range{
				Pos:  offset,
				Size: chunkEnd - offset,
			})
		}
	}

	return nil
}

// saveMetadata persists the current state to disk
func (f *SparseFile) saveMetadata() error {
	if f.manager == nil || f.ranges == nil {
		return nil
	}

	meta := &Metadata{
		ModTime:     f.modTime,
		ATime:       f.lastAccess,
		Size:        f.size,
		Ranges:      f.ranges.GetRanges(),
		Fingerprint: "", // Could add ETag/hash here
		Dirty:       f.dirty,
	}

	if err := f.manager.saveMetadata(f.torrentName, f.fileName, meta); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	return nil
}

// ReadAt reads from cache if data is available, returns false if not cached
// Supports partial reads: if only part of the requested range is cached, returns what's available
func (f *SparseFile) ReadAt(p []byte, offset int64) (n int, cached bool, err error) {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	f.lastAccess = time.Now()

	requestedRange := ranges.Range{Pos: offset, Size: int64(len(p))}

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
func (f *SparseFile) WriteAt(p []byte, offset int64) (int, error) {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Write to file
	n, err := f.file.WriteAt(p, offset)
	if err != nil {
		return n, err
	}

	// Mark this range as cached
	if n > 0 {
		f.ranges.Insert(ranges.Range{
			Pos:  offset,
			Size: int64(n),
		})
		f.dirty = true
	}

	f.lastAccess = time.Now()
	return n, nil
}

// Sync flushes data to disk
func (f *SparseFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Sync file data
	if f.file != nil {
		return f.file.Sync()
	}

	return nil
}

// closeFD closes the file descriptor
func (f *SparseFile) closeFD() error {
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
func (f *SparseFile) Close() error {
	// Stop all downloads first
	_ = f.CloseDownloads()

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		err := f.file.Close()
		f.file = nil
		return err
	}
	return nil
}

// removeFromDisk removes the sparse file from disk
func (f *SparseFile) removeFromDisk() error {
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

// GetCachedSize returns the total bytes downloaded
func (f *SparseFile) GetCachedSize() int64 {
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
func (f *SparseFile) GetCachedRanges() []ranges.Range {
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
func (f *SparseFile) FindMissing(offset, length int64) []ranges.Range {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		// If ranges failed to load, assume everything is missing
		return []ranges.Range{{Pos: offset, Size: length}}
	}

	return f.ranges.FindMissing(ranges.Range{
		Pos:  offset,
		Size: length,
	})
}

// IsCached checks if a range is cached WITHOUT allocating buffers
// This is much more efficient than ReadAt for cache checks
func (f *SparseFile) IsCached(offset, length int64) bool {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return false
	}

	return f.ranges.Present(ranges.Range{
		Pos:  offset,
		Size: length,
	})
}

// ============================================================================
// Download Methods (merged from Reader)
// ============================================================================

// getDownloadLink retrieves the download link for this file
func (f *File) getDownloadLink() (types.DownloadLink, error) {
	if f.debrid == nil {
		return types.DownloadLink{}, fmt.Errorf("no debrid client configured")
	}

	downloadLink, err := f.debrid.GetDownloadLink(f.torrentName, f.fileName, f.fileLink)
	if err != nil {
		return downloadLink, err
	}
	err = downloadLink.Valid()
	if err != nil {
		return types.DownloadLink{}, err
	}
	return downloadLink, nil
}

// ReadAtWithDownload reads data with progressive download for instant playback start
// Downloads minimum threshold (512KB) then returns immediately while continuing in background
// This enables playback to start in ~0.1-0.3 seconds instead of waiting for full 8MB chunk
func (f *File) ReadAtWithDownload(ctx context.Context, p []byte, offset int64) (int, error) {
	if offset >= f.size {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if offset+readSize > f.size {
		readSize = f.size - offset
		p = p[:readSize]
	}

	// Check if fully cached (zero-allocation check)
	if f.IsCached(offset, readSize) {
		// Cache hit - read and trigger prefetch
		n, _, err := f.ReadAt(p, offset)
		if err == nil {
			go f.aggressiveSequentialPrefetch(ctx, offset+readSize)
		}
		return n, err
	}

	// Cache miss - need to download
	// Calculate which chunks we need
	startChunk := offset / f.chunkSize
	endChunk := (offset + readSize - 1) / f.chunkSize

	// Track active read
	f.stats.TrackActiveRead(1)
	defer f.stats.TrackActiveRead(-1)

	// Progressive download: Calculate minimum threshold for instant playback
	// Use 512KB as minimum - enough for video player to start, small enough to download fast
	const minThreshold int64 = 512 * 1024 // 512KB

	// For small reads (< 512KB), just download what's needed
	threshold := minThreshold
	if readSize < minThreshold {
		threshold = readSize
	}

	// Progressive approach: Download first chunk with early return at threshold
	// This allows playback to start after ~512KB instead of waiting for full 8MB chunk
	if err := f.downloadChunkWithThreshold(ctx, startChunk, threshold); err != nil {
		return 0, err
	}

	// Start downloading remaining chunks in background (non-blocking)
	numChunks := endChunk - startChunk + 1
	if numChunks > 1 {
		for chunkIdx := startChunk + 1; chunkIdx <= endChunk; chunkIdx++ {
			chunkIdx := chunkIdx // Capture loop variable
			go f.downloadChunk(ctx, chunkIdx)
		}
	}

	// Trigger prefetch for chunks beyond the current request (only once, not in cache hit path)
	go f.aggressiveSequentialPrefetch(ctx, (endChunk+1)*f.chunkSize)

	// Read from file now - at least 512KB is ready from first chunk
	// Note: May return partial data if background downloads haven't completed
	// This is intentional - we return what's available immediately for fast playback start
	n, _, err := f.ReadAt(p, offset)
	if err != nil && err != io.EOF {
		return n, err
	}

	// If we got data, return it (even if partial)
	if n > 0 {
		return n, nil
	}

	// No data available - this shouldn't happen since we just downloaded threshold
	return 0, fmt.Errorf("no data available after download")
}

// aggressiveSequentialPrefetch downloads chunks ahead for smooth playback
func (f *File) aggressiveSequentialPrefetch(ctx context.Context, currentOffset int64) {
	if f.closed.Load() {
		return
	}

	// Download 4-6 chunks ahead for smooth playback
	numChunks := int64(6)
	if f.readAhead > 0 {
		numChunks = (f.readAhead + f.chunkSize - 1) / f.chunkSize
		if numChunks < 4 {
			numChunks = 4 // Minimum 4 chunks for smooth playback
		}
	}

	startChunk := currentOffset / f.chunkSize
	endChunk := startChunk + numChunks
	totalChunks := (f.size + f.chunkSize - 1) / f.chunkSize
	if endChunk > totalChunks {
		endChunk = totalChunks
	}

	// Download chunks concurrently
	for chunkIdx := startChunk; chunkIdx < endChunk; chunkIdx++ {
		select {
		case <-f.closeChan:
			return
		default:
			go f.downloadChunkAsync(ctx, chunkIdx)
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
	if chunkEnd > f.size {
		chunkEnd = f.size
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
	return f.downloadChunkWithThreshold(ctx, chunkIdx, 0) // No early return
}

// downloadChunkWithThreshold downloads a chunk with optional early return
// If minThreshold > 0, returns as soon as that many bytes are available
// Background download continues to completion
func (f *File) downloadChunkWithThreshold(ctx context.Context, chunkIdx int64, minThreshold int64) error {
	chunkStart := chunkIdx * f.chunkSize
	chunkEnd := chunkStart + f.chunkSize
	if chunkEnd > f.size {
		chunkEnd = f.size
	}
	actualSize := chunkEnd - chunkStart

	// Check if already cached (zero-allocation check)
	if f.IsCached(chunkStart, actualSize) {
		return nil // Already cached
	}

	// If threshold requested, check if we have at least that much cached
	if minThreshold > 0 {
		// Check partial cache
		missing := f.FindMissing(chunkStart, actualSize)
		if len(missing) == 0 {
			return nil // Fully cached
		}

		// Calculate how much we already have
		cachedBytes := actualSize
		for _, m := range missing {
			cachedBytes -= m.Size
		}

		if cachedBytes >= minThreshold {
			// We already have enough cached data
			// Start background download for the rest if not already downloading
			if _, exists := f.downloading.Load(chunkIdx); !exists {
				go f.downloadChunk(ctx, chunkIdx)
			}
			return nil
		}
	}

	// Check if already downloading - if so, wait for it
	if job, exists := f.downloading.Load(chunkIdx); exists {
		// If threshold specified, wait for threshold, otherwise wait for completion
		if minThreshold > 0 && job.thresholdReady != nil {
			select {
			case <-job.thresholdReady:
				return nil // Threshold reached, return early
			case err := <-job.done:
				return err // Download completed or failed
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return <-job.done // Wait for existing download to complete
	}

	// Create download job with progressive support
	job := &downloadJob{
		chunkStart:     chunkStart,
		chunkSize:      actualSize,
		done:           make(chan error, 1),
		minThreshold:   minThreshold,
		thresholdReady: make(chan struct{}),
	}

	// Try to store job (race condition safe)
	actual, loaded := f.downloading.LoadOrStore(chunkIdx, job)
	if loaded {
		// Someone else started downloading
		if minThreshold > 0 && actual.thresholdReady != nil {
			select {
			case <-actual.thresholdReady:
				return nil // Threshold reached
			case err := <-actual.done:
				return err // Download completed or failed
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return <-actual.done
	}

	// Start background download
	go func() {
		err := f.doDownloadProgressive(ctx, job)
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

	// Wait for threshold or completion
	if minThreshold > 0 {
		select {
		case <-job.thresholdReady:
			return nil // Threshold reached, return early
		case err := <-job.done:
			return err // Download completed or failed before threshold
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// No threshold, wait for completion
	return <-job.done
}

// doDownload performs the actual HTTP download with streaming writes
// Uses pooled buffers and batched writes to minimize allocations and lock contention
func (f *File) doDownload(ctx context.Context, offset, size int64) error {
	return f.doDownloadWithJob(ctx, offset, size, nil)
}

// doDownloadProgressive performs progressive download with threshold signaling
func (f *File) doDownloadProgressive(ctx context.Context, job *downloadJob) error {
	return f.doDownloadWithJob(ctx, job.chunkStart, job.chunkSize, job)
}

// doDownloadWithJob performs the actual HTTP download with optional progressive signaling
func (f *File) doDownloadWithJob(ctx context.Context, offset, size int64, job *downloadJob) error {
	end := offset + size - 1
	rc, err := f.debrid.StreamReader(ctx, offset, end, f.getDownloadLink)
	if err != nil {
		return fmt.Errorf("get download link: %w", err)
	}
	defer rc.Close()

	// Get buffer from pool (128KB)
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
		"bytes_downloaded": f.bytesDownloaded.Load(),
		"active_downloads": f.downloading.Size(),
		"chunk_size":       f.chunkSize,
		"read_ahead":       f.readAhead,
		"cached_size":      f.GetCachedSize(),
		"total_size":       f.size,
	}
}
