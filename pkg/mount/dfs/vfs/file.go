package vfs

import (
	"errors"
	"io"
	"sync/atomic"
)

// StreamingFile is the FUSE file interface for VFS
type StreamingFile struct {
	item     *CacheItem
	fileSize int64
	closed   atomic.Bool
}

// NewStreamingFile creates a new streaming file handle
func NewStreamingFile(item *CacheItem) *StreamingFile {
	item.Open() // Increment opens count
	GlobalVFSStats.FilesOpened.Add(1)
	return &StreamingFile{
		item:     item,
		fileSize: item.info.Size,
	}
}

// ReadAt implements io.ReaderAt
func (f *StreamingFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, errors.New("file closed")
	}

	if off >= f.fileSize {
		return 0, io.EOF
	}

	// Clamp read size
	readSize := int64(len(p))
	if off+readSize > f.fileSize {
		readSize = f.fileSize - off
		p = p[:readSize]
	}

	n, err := f.item.ReadAt(p, off)

	// Handle partial read at EOF
	if n < int(readSize) && err == nil {
		err = io.EOF
	}

	return n, err
}

// Size returns the file size
func (f *StreamingFile) Size() int64 {
	return f.fileSize
}

// Close closes the file handle
func (f *StreamingFile) Close() error {
	if f.closed.Swap(true) {
		return nil
	}
	f.item.Release() // Decrement opens count
	GlobalVFSStats.FilesClosed.Add(1)
	return nil
}
