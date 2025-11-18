package dfs

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// File implements a FUSE file with RFS streaming
type File struct {
	fs.Inode
	config    *common.FuseConfig
	logger    zerolog.Logger
	info      *manager.FileInfo
	createdAt time.Time
	content   []byte // For files like version.txt
	vfs       *vfs.Manager
}

// FileHandle implements file operations with RFS
// Key optimization: RFS Reader is persistent across reads, not recreated per read!
type FileHandle struct {
	file       *File
	reader     *vfs.Reader // Persistent reader with connection pooling
	readerOnce sync.Once   // Ensures reader created exactly once
	readerErr  error       // Stores any error from reader creation
	closed     atomic.Bool
	lastAccess atomic.Int64
	logger     zerolog.Logger
}

var _ = (fs.NodeOpener)((*File)(nil))
var _ = (fs.NodeGetattrer)((*File)(nil))
var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))
var _ = (fs.FileFlusher)((*FileHandle)(nil))
var _ = (fs.FileFsyncer)((*FileHandle)(nil))

// newFile creates a new file
func newFile(vfsManager *vfs.Manager, config *common.FuseConfig, info *manager.FileInfo, logger zerolog.Logger) *File {
	return &File{
		config: config,
		logger: logger,
		info:   info,
		vfs:    vfsManager,
	}
}

// Getattr returns file attributes
func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	var modTime uint64
	if f.createdAt.IsZero() {
		modTime = uint64(time.Now().Unix())
	} else {
		modTime = uint64(f.createdAt.Unix())
	}
	out.Mode = 0644 | fuse.S_IFREG
	out.Size = uint64(f.info.Size())
	out.Nlink = 1 // Files always have 1 link (themselves)
	out.Blksize = 4096
	out.Blocks = (uint64(f.info.Size()) + 511) / 512 // Number of 512-byte blocks
	out.Uid = f.config.UID
	out.Gid = f.config.GID
	out.Atime = modTime
	out.Mtime = modTime
	out.Ctime = modTime
	out.AttrValid = uint64(f.config.AttrTimeout.Seconds())
	return 0
}

// Open creates file handle with VFS
func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {

	fh := &FileHandle{
		file:   f,
		logger: f.logger,
	}

	fh.lastAccess.Store(time.Now().Unix())
	return fh, 0, 0
}

// Read implements RFS streaming with persistent reader
func (fh *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.lastAccess.Store(time.Now().Unix())

	if fh.closed.Load() {
		return nil, syscall.EBADF
	}

	if off >= fh.file.info.Size() {
		return fuse.ReadResultData([]byte{}), 0
	}

	// Handle static content
	if len(fh.file.content) > 0 {
		data := fh.readFromStaticContent(off, int64(len(dest)))
		return fuse.ReadResultData(data), 0
	}

	if fh.file.info.IsRemote() {
		fh.readerOnce.Do(func() {
			var err error
			fh.reader, err = fh.file.vfs.GetReader(fh.file.info)
			fh.readerErr = err
		})

		// Check if creation failed
		if fh.readerErr != nil {
			return nil, syscall.EIO
		}
	}

	// Ensure we have an RFS reader
	if fh.reader == nil {
		fh.logger.Error().Msg("RFS reader is nil")
		return nil, syscall.EIO
	}

	// Read from RFS (with automatic prefetching, connection reuse, etc.)
	n, err := fh.reader.ReadAt(dest, off)
	if err != nil && !skippableError(err) {
		fh.logger.Error().
			Err(err).
			Int64("offset", off).
			Int("size", len(dest)).
			Msg("Failed to read from RFS")
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func skippableError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) {
		return true
	}
	return false
}

// readFromStaticContent handles static content like version.txt
func (fh *FileHandle) readFromStaticContent(offset, size int64) []byte {
	content := fh.file.content
	end := offset + size
	if end > int64(len(content)) {
		end = int64(len(content))
	}
	if offset >= int64(len(content)) {
		return []byte{}
	}
	return content[offset:end]
}

// Release closes the file handle
func (fh *FileHandle) Release(ctx context.Context) syscall.Errno {
	if !fh.closed.CompareAndSwap(false, true) {
		return 0
	}

	// Release RFS reader reference
	// The reader is pooled and managed by RFS Manager
	// It will be automatically cleaned up when idle
	if fh.reader != nil {
		fh.file.vfs.ReleaseReader(fh.file.info)
	}

	return 0
}

func (fh *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

func (fh *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}
