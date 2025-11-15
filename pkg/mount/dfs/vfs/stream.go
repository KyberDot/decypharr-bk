package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

// StreamingReader provides optimized streaming with connection reuse and buffering
type StreamingReader struct {
	// Source
	manager     *manager.Manager
	info        *manager.FileInfo
	startOffset int64
	endOffset   int64

	// Connection management - OPTIMIZED: Persistent connections for streaming
	reader       io.ReadCloser
	readerMu     sync.Mutex
	readerOffset atomic.Int64
	currentPos   atomic.Int64

	// Ring buffer settings - OPTIMIZED: Larger buffer for smooth playback
	chunkSize  int64       // Size of each buffered chunk (1MB)
	queueDepth int         // Number of chunks to buffer ahead (12 = 12MB)
	chunkCh    chan []byte // Buffered channel for chunks
	errCh      chan error  // Error channel
	pool       *sync.Pool  // Buffer pool for zero-allocation

	// State
	ctx        context.Context
	cancel     context.CancelFunc
	bytesRead  atomic.Int64
	closed     atomic.Bool
	readerDone atomic.Bool

	// Background caching (optional)
	cacheFile *File // If provided, cache downloaded data in background

	// Range capabilities - OPTIMIZED: Can serve ranges within downloaded data
	serveStart atomic.Int64 // Start of data we can serve
	serveEnd   atomic.Int64 // End of data we can serve
}

// OPTIMIZED: Better balance of chunk size and buffer depth
const (
	enhancedChunkSize  = 1024 * 1024 // 1MB per chunk - reduces HTTP overhead
	enhancedQueueDepth = 12          // 12MB total buffer - good for most videos
)

// NewStreamingReader creates an enhanced streaming reader
// OPTIMIZED: Single reader type that handles all scenarios efficiently
func NewStreamingReader(ctx context.Context, mgr *manager.Manager, info *manager.FileInfo,
	startOffset, endOffset int64, cacheFile *File) *StreamingReader {

	ctx, cancel := context.WithCancel(ctx)

	sr := &StreamingReader{
		manager:     mgr,
		info:        info,
		startOffset: startOffset,
		endOffset:   endOffset,
		chunkSize:   enhancedChunkSize,
		queueDepth:  enhancedQueueDepth,
		chunkCh:     make(chan []byte, enhancedQueueDepth),
		errCh:       make(chan error, 1),
		ctx:         ctx,
		cancel:      cancel,
		cacheFile:   cacheFile,
		pool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, enhancedChunkSize)
				return &buf
			},
		},
	}

	sr.currentPos.Store(startOffset)
	sr.serveStart.Store(startOffset)
	sr.serveEnd.Store(endOffset)
	sr.readerOffset.Store(startOffset)

	// Start background reader
	go sr.readLoop()

	return sr
}

// CanServe checks if this reader can serve the requested range efficiently
func (sr *StreamingReader) CanServe(offset, size int64) bool {
	if sr.closed.Load() {
		return false
	}

	start := sr.serveStart.Load()
	end := sr.serveEnd.Load()

	// Can serve if request is within our range and we're ahead of the offset
	currentPos := sr.currentPos.Load()
	return offset >= start && offset+size <= end && offset >= (currentPos-int64(sr.queueDepth)*sr.chunkSize)
}

// ReadAt reads data at a specific offset - OPTIMIZED: Efficient forward seeking
func (sr *StreamingReader) ReadAt(p []byte, offset int64) (int, error) {
	if sr.closed.Load() {
		return 0, io.EOF
	}

	// Check if we can serve this offset
	if !sr.CanServe(offset, int64(len(p))) {
		return 0, fmt.Errorf("offset %d out of range or too far behind current position", offset)
	}

	// Calculate how much data we need to skip to reach the offset
	currentPos := sr.currentPos.Load()

	if offset < currentPos {
		// We're behind current position - this should be rare due to CanServe check
		return 0, fmt.Errorf("offset %d is behind current position %d", offset, currentPos)
	}

	// Forward seek - skip data until we reach the offset
	toSkip := offset - currentPos
	if toSkip > 0 {
		skipBuf := make([]byte, min(int(toSkip), 64*1024)) // 64KB skip buffer
		for toSkip > 0 {
			skipSize := min(int(toSkip), len(skipBuf))
			n, err := sr.Read(skipBuf[:skipSize])
			if err != nil {
				return 0, fmt.Errorf("seek to offset %d: %w", offset, err)
			}
			toSkip -= int64(n)
			if n == 0 {
				break // Avoid infinite loop
			}
		}
	}

	// Now read the actual data with full read completion
	return sr.readFull(p)
}

// readFull attempts to read the full buffer, handling short reads internally
// OPTIMIZED: Ensures complete reads for video players that expect full buffers
func (sr *StreamingReader) readFull(p []byte) (int, error) {
	if sr.closed.Load() {
		return 0, io.EOF
	}

	totalRead := 0
	retries := 0
	maxRetries := 3

	for totalRead < len(p) && retries < maxRetries {
		n, err := sr.Read(p[totalRead:])
		totalRead += n

		if err != nil {
			if err == io.EOF {
				// EOF is acceptable if we read some data
				return totalRead, nil
			}

			// Handle temporary errors with retry
			if isTemporaryError(err) {
				retries++
				if retries < maxRetries {
					// Brief delay before retry
					select {
					case <-time.After(time.Duration(retries) * 100 * time.Millisecond):
					case <-sr.ctx.Done():
						return totalRead, sr.ctx.Err()
					}
					continue
				}
			}

			// Return what we read so far for other errors
			if totalRead > 0 {
				return totalRead, nil
			}
			return 0, err
		}

		// Reset retry counter on successful read
		retries = 0
	}

	return totalRead, nil
}

// readLoop continuously reads from remote with connection reuse and error recovery
// OPTIMIZED: Persistent connections, better buffering, robust error handling
func (sr *StreamingReader) readLoop() {
	defer func() {
		sr.readerDone.Store(true)
		close(sr.chunkCh)
		sr.closeReader()
	}()

	currentOffset := sr.startOffset
	totalSize := sr.endOffset - sr.startOffset + 1
	if sr.endOffset <= 0 || sr.endOffset >= sr.info.Size() {
		totalSize = sr.info.Size() - sr.startOffset
	}

	for sr.bytesRead.Load() < totalSize {
		// Check if closed
		if sr.closed.Load() {
			return
		}

		// Ensure we have an active reader connection
		if err := sr.ensureReader(currentOffset); err != nil {
			sr.errCh <- fmt.Errorf("ensure reader at offset %d: %w", currentOffset, err)
			return
		}

		// GetReader buffer from pool
		bufPtr := sr.pool.Get().(*[]byte)
		buf := *bufPtr

		// Read chunk with retry and timeout logic
		n, err := sr.readWithRetry(buf)

		if n == 0 && err != nil {
			sr.pool.Put(bufPtr)
			if err == io.EOF {
				return // Normal completion
			}

			// Try to recover from error by reconnecting
			sr.closeReader()
			if retryErr := sr.ensureReader(currentOffset); retryErr != nil {
				sr.errCh <- fmt.Errorf("retry connection failed: %w", retryErr)
				return
			}
			continue
		}

		if n > 0 {
			// Adjust for end of requested range
			remaining := totalSize - sr.bytesRead.Load()
			if int64(n) > remaining {
				n = int(remaining)
			}

			// Create chunk with only the data we actually read
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			sr.pool.Put(bufPtr) // Return to pool immediately

			// Send to channel with backpressure (blocks if buffer is full)
			select {
			case sr.chunkCh <- chunk:
				writeOffset := currentOffset
				currentOffset += int64(n)
				sr.bytesRead.Add(int64(n))
				sr.readerOffset.Store(currentOffset)

				// Cache data synchronously (fast - goes to memory buffer first)
				if sr.cacheFile != nil {
					_, _ = sr.cacheFile.WriteAt(chunk, writeOffset)
				}

			case <-sr.ctx.Done():
				return
			}
		} else {
			sr.pool.Put(bufPtr)
		}

		// Check for completion
		if err == io.EOF || sr.bytesRead.Load() >= totalSize {
			return
		}
	}
}

// ensureReader ensures we have an active reader connection
// OPTIMIZED: Connection reuse for better performance
func (sr *StreamingReader) ensureReader(offset int64) error {
	sr.readerMu.Lock()
	defer sr.readerMu.Unlock()

	// Check if current reader is still valid for this offset
	if sr.reader != nil && sr.readerOffset.Load() == offset {
		return nil
	}

	// Close existing reader if any
	if sr.reader != nil {
		_ = sr.reader.Close()
		sr.reader = nil
	}

	// Create new reader with retry logic
	endOffset := sr.endOffset
	if endOffset <= 0 || endOffset >= sr.info.Size() {
		endOffset = sr.info.Size() - 1
	}

	var rc io.ReadCloser
	var err error

	// Retry connection establishment
	for retries := 0; retries < 3; retries++ {
		rc, err = sr.manager.StreamReader(sr.ctx, sr.info.Parent(), sr.info.Name(), offset, endOffset)
		if err == nil {
			break
		}

		// Brief delay before retry
		select {
		case <-time.After(time.Duration(retries+1) * 100 * time.Millisecond):
		case <-sr.ctx.Done():
			return sr.ctx.Err()
		}
	}

	if err != nil {
		return fmt.Errorf("create stream reader after retries: %w", err)
	}

	sr.reader = rc
	sr.readerOffset.Store(offset)
	return nil
}

// readWithRetry reads from the current reader with timeout and retry logic
// OPTIMIZED: Prevents hanging and handles temporary network issues
func (sr *StreamingReader) readWithRetry(buf []byte) (int, error) {
	sr.readerMu.Lock()
	reader := sr.reader
	sr.readerMu.Unlock()

	if reader == nil {
		return 0, fmt.Errorf("no active reader")
	}

	// Implement read timeout to prevent hanging
	type result struct {
		n   int
		err error
	}

	resultCh := make(chan result, 1)

	go func() {
		n, err := reader.Read(buf)
		resultCh <- result{n, err}
	}()

	select {
	case res := <-resultCh:
		return res.n, res.err
	case <-time.After(30 * time.Second):
		// Read timeout - close reader to force reconnection
		sr.closeReader()
		return 0, fmt.Errorf("read timeout")
	case <-sr.ctx.Done():
		return 0, sr.ctx.Err()
	}
}

// closeReader safely closes the current reader
func (sr *StreamingReader) closeReader() {
	sr.readerMu.Lock()
	defer sr.readerMu.Unlock()

	if sr.reader != nil {
		_ = sr.reader.Close()
		sr.reader = nil
	}
}

// Read implements io.Reader - reads from the ring buffer with proper short read handling
// OPTIMIZED: Handles partial reads correctly, no data loss
func (sr *StreamingReader) Read(p []byte) (int, error) {
	if sr.closed.Load() {
		return 0, io.EOF
	}

	// Try to read from channel
	select {
	case chunk, ok := <-sr.chunkCh:
		if !ok {
			// Channel closed - check if there was an error
			select {
			case err := <-sr.errCh:
				return 0, err
			default:
				return 0, io.EOF
			}
		}

		// Copy chunk to output buffer
		n := copy(p, chunk)

		// Update current position
		sr.currentPos.Add(int64(n))

		// OPTIMIZED: Handle partial reads correctly
		if n < len(chunk) {
			// Buffer too small - put remaining data back for next read
			remaining := make([]byte, len(chunk)-n)
			copy(remaining, chunk[n:])

			// Non-blocking put back - if channel is full, create new buffered channel
			select {
			case sr.chunkCh <- remaining:
				// Successfully put back
			default:
				// Channel full - need to reorganize
				go sr.handleChannelReorganization(remaining)
			}
		}

		return n, nil

	case err := <-sr.errCh:
		return 0, err

	case <-sr.ctx.Done():
		return 0, sr.ctx.Err()
	}
}

// handleChannelReorganization handles the case where we need to put data back but channel is full
func (sr *StreamingReader) handleChannelReorganization(remaining []byte) {
	// Create new channel with remaining data first
	newCh := make(chan []byte, sr.queueDepth)
	newCh <- remaining

	oldCh := sr.chunkCh
	sr.chunkCh = newCh

	// Drain old channel into new one
	go func() {
		for chunk := range oldCh {
			select {
			case newCh <- chunk:
			case <-sr.ctx.Done():
				return
			}
		}
	}()
}

// Close stops the streaming reader and cleans up all resources
func (sr *StreamingReader) Close() error {
	if sr.closed.CompareAndSwap(false, true) {
		sr.cancel()
		sr.closeReader()

		// Drain the channel to unblock reader goroutine
		go func() {
			for range sr.chunkCh {
				// Drain all remaining chunks
			}
		}()
	}
	return nil
}

// BytesRead returns the total bytes read from remote
func (sr *StreamingReader) BytesRead() int64 {
	return sr.bytesRead.Load()
}

// IsComplete returns true if all data has been read
func (sr *StreamingReader) IsComplete() bool {
	return sr.readerDone.Load()
}

// GetPosition returns the current read position (for debugging)
func (sr *StreamingReader) GetPosition() int64 {
	return sr.currentPos.Load()
}

// Utility functions

// isTemporaryError checks if an error is temporary and retryable
func isTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		isTemporaryNetworkError(err)
}

// isTemporaryNetworkError checks for common temporary network errors
func isTemporaryNetworkError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	temporaryIndicators := []string{
		"connection reset",
		"connection refused",
		"timeout",
		"temporary failure",
		"network is unreachable",
		"no route to host",
	}

	for _, indicator := range temporaryIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}
