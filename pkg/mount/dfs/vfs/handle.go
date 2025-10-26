package vfs

import (
	"context"
	"io"
	"sync"
)

// Handle represents an open file handle for reading
type Handle struct {
	reader *Reader
	offset int64
	mu     sync.Mutex
}

// NewHandle creates a new file handle
func NewHandle(reader *Reader) *Handle {
	return &Handle{
		reader: reader,
		offset: 0,
	}
}

// Read reads data at current offset
func (h *Handle) Read(ctx context.Context, p []byte) (int, error) {
	h.mu.Lock()
	offset := h.offset
	h.mu.Unlock()

	n, err := h.reader.ReadAt(ctx, p, offset)

	h.mu.Lock()
	h.offset += int64(n)
	h.mu.Unlock()

	return n, err
}

// ReadAt reads data at specific offset (doesn't change current offset)
func (h *Handle) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	return h.reader.ReadAt(ctx, p, offset)
}

// Seek changes the current read offset
func (h *Handle) Seek(offset int64, whence int) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch whence {
	case io.SeekStart:
		h.offset = offset
	case io.SeekCurrent:
		h.offset += offset
	case io.SeekEnd:
		h.offset = h.reader.size + offset
	default:
		return 0, io.ErrUnexpectedEOF
	}

	return h.offset, nil
}

// Close closes the handle
func (h *Handle) Close() error {
	// Sync sparseFile on close
	return h.reader.sparseFile.Sync()
}
