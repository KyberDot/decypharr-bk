package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// Reader downloads multiple chunks concurrently like rclone --transfers
type Reader struct {
	size        int64
	debrid      *store.Cache
	sparseFile  *SparseFile
	torrentName string
	fileName    string
	fileLink    string

	chunkSize     int64 // Size of each chunk (e.g., 8MB)
	readAhead     int64 // Read-ahead size (e.g., 12MB)
	maxConcurrent int   // Max concurrent downloads (e.g., 4)

	// Active downloads tracking
	downloading *xsync.Map[int64, *downloadJob] // map[int64]*downloadJob

	// Semaphore to limit concurrent downloads
	//downloadSem chan struct{}

	// Stats
	bytesDownloaded atomic.Int64
	stats           *StatsTracker // Stats tracker for network/read operations
}

type downloadJob struct {
	chunkStart int64
	chunkSize  int64
	done       chan error
	mu         sync.Mutex
	completed  bool
}

// NewReader creates a reader with parallel chunk downloading
func NewReader(debridCache *store.Cache, torrentName string, torrentFile types.File, sparseFile *SparseFile, chunkSize, readAhead int64, maxConcurrent int, stats *StatsTracker) *Reader {
	return &Reader{
		size:          torrentFile.Size,
		torrentName:   torrentName,
		fileName:      torrentFile.Name,
		fileLink:      torrentFile.Link,
		debrid:        debridCache,
		sparseFile:    sparseFile,
		chunkSize:     chunkSize,
		readAhead:     readAhead,
		maxConcurrent: maxConcurrent,
		downloading:   xsync.NewMap[int64, *downloadJob](),
		stats:         stats,
		//downloadSem:   make(chan struct{}, maxConcurrent),
	}
}

func (pr *Reader) getDownloadLink() (types.DownloadLink, error) {
	// Check if we already have a final URL cached
	downloadLink, err := pr.debrid.GetDownloadLink(pr.torrentName, pr.fileName, pr.fileLink)
	if err != nil {
		return downloadLink, err
	}
	err = downloadLink.Valid()
	if err != nil {
		return types.DownloadLink{}, err
	}
	return downloadLink, nil
}

// ReadAt reads data with parallel chunk downloading
func (pr *Reader) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	if offset >= pr.size {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if offset+readSize > pr.size {
		readSize = pr.size - offset
		p = p[:readSize]
	}

	// Try sparseFile first
	n, cached, err := pr.sparseFile.ReadAt(p, offset)
	if cached && err == nil {
		// Cache hit - track read operation and trigger read-ahead
		pr.stats.TrackReadOp(int64(n))
		go pr.scheduleReadAhead(ctx, offset+readSize)
		return n, nil
	}

	// Cache miss - need to download
	// Calculate which chunks we need
	startChunk := offset / pr.chunkSize
	endChunk := (offset + readSize - 1) / pr.chunkSize

	// Download required chunks (maybe multiple if read spans chunks)
	for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
		if err := pr.downloadChunk(ctx, chunkIdx); err != nil {
			return 0, err
		}
	}

	// Read from sparseFile now
	n, _, err = pr.sparseFile.ReadAt(p, offset)
	if err != nil {
		return n, err
	}

	// Track read operation
	pr.stats.TrackReadOp(int64(n))

	// Trigger read-ahead for next chunks
	go pr.scheduleReadAhead(ctx, offset+readSize)

	return n, nil
}

// scheduleReadAhead schedules read-ahead chunks for download
func (pr *Reader) scheduleReadAhead(ctx context.Context, fromOffset int64) {
	// Calculate read-ahead range
	readAheadEnd := fromOffset + pr.readAhead
	if readAheadEnd > pr.size {
		readAheadEnd = pr.size
	}

	startChunk := fromOffset / pr.chunkSize
	endChunk := (readAheadEnd - 1) / pr.chunkSize

	// Schedule chunks for background download
	for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
		go pr.downloadChunkAsync(ctx, chunkIdx)
	}
}

// downloadChunkAsync downloads a chunk in background (non-blocking)
func (pr *Reader) downloadChunkAsync(ctx context.Context, chunkIdx int64) {
	// Check if already cached
	chunkStart := chunkIdx * pr.chunkSize
	testBuf := make([]byte, 1)
	if _, cached, _ := pr.sparseFile.ReadAt(testBuf, chunkStart); cached {
		return // Already cached
	}

	// Check if already downloading
	if _, exists := pr.downloading.Load(chunkIdx); exists {
		return // Already downloading
	}

	// Start download
	_ = pr.downloadChunk(ctx, chunkIdx)
}

// downloadChunk downloads a specific chunk (blocking)
func (pr *Reader) downloadChunk(ctx context.Context, chunkIdx int64) error {
	chunkStart := chunkIdx * pr.chunkSize
	chunkEnd := chunkStart + pr.chunkSize
	if chunkEnd > pr.size {
		chunkEnd = pr.size
	}
	chunkSize := chunkEnd - chunkStart

	// Check sparseFile first
	testBuf := make([]byte, 1)
	if _, cached, _ := pr.sparseFile.ReadAt(testBuf, chunkStart); cached {
		return nil // Already cached
	}

	// Check if already downloading - if so, wait for it
	if job, exists := pr.downloading.Load(chunkIdx); exists {
		return <-job.done // Wait for existing download
	}

	// Create download job
	job := &downloadJob{
		chunkStart: chunkStart,
		chunkSize:  chunkSize,
		done:       make(chan error, 1),
	}

	// Try to store job (race condition safe)
	actual, loaded := pr.downloading.LoadOrStore(chunkIdx, job)
	if loaded {
		// Someone else started downloading, wait for them
		return <-actual.done
	}

	go func() {
		// Acquire semaphore to limit concurrent downloads
		//pr.downloadSem <- struct{}{}
		//defer func() { <-pr.downloadSem }()
		err := pr.doDownload(ctx, chunkStart, chunkSize)
		job.mu.Lock()
		job.completed = true
		job.mu.Unlock()
		job.done <- err
		close(job.done)

		// Delete immediately after completion
		pr.downloading.Delete(chunkIdx)

		if err == nil {
			pr.bytesDownloaded.Add(chunkSize)
		}
	}()

	// Wait for download to complete
	return <-job.done
}

// doDownload performs the actual HTTP download
func (pr *Reader) doDownload(ctx context.Context, offset, size int64) error {
	end := offset + size - 1
	rc, err := pr.debrid.StreamReader(ctx, offset, end, pr.getDownloadLink)
	if err != nil {
		return fmt.Errorf("get download link: %w", err)
	}
	defer func(rc io.ReadCloser) {
		_ = rc.Close()
	}(rc)

	// Read chunk
	buffer := make([]byte, size)
	totalRead := 0

	for totalRead < int(size) {
		n, err := rc.Read(buffer[totalRead:])
		totalRead += n

		if err == io.EOF {
			break
		}
		if err != nil {
			// Write partial data
			if totalRead > 0 {
				_, _ = pr.sparseFile.WriteAt(buffer[:totalRead], offset)
			}
			return fmt.Errorf("read chunk: %w", err)
		}
	}

	// Write to sparseFile
	if totalRead > 0 {
		_, err := pr.sparseFile.WriteAt(buffer[:totalRead], offset)
		if err != nil {
			return fmt.Errorf("write to sparseFile: %w", err)
		}
		// Track network read
		pr.stats.TrackNetworkRead(int64(totalRead))
	}

	return nil
}

func (pr *Reader) Close() error {
	// No persistent resources to close
	return nil
}

// Stats returns download statistics
func (pr *Reader) Stats() map[string]interface{} {

	return map[string]interface{}{
		"bytes_downloaded": pr.bytesDownloaded.Load(),
		"active_downloads": pr.downloading.Size(),
		"chunk_size":       pr.chunkSize,
		"read_ahead":       pr.readAhead,
	}
}
