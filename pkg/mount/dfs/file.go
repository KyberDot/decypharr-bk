package dfs

import (
	"context"
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
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/rfs"
)

// File implements a FUSE file with RFS streaming
type File struct {
	fs.Inode
	config    *common.FuseConfig
	logger    zerolog.Logger
	info      *manager.FileInfo
	createdAt time.Time
	content   []byte // For files like version.txt
	rfs       *rfs.Manager
}

// FileHandle implements file operations with RFS
// Key optimization: RFS Reader is persistent across reads, not recreated per read!
type FileHandle struct {
	file       *File
	rfsReader  *rfs.Reader // Persistent reader with connection pooling
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
func newFile(rfsManager *rfs.Manager, config *common.FuseConfig, info *manager.FileInfo, logger zerolog.Logger) *File {
	return &File{
		config: config,
		logger: logger,
		info:   info,
		rfs:    rfsManager,
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

	// Lazy-create RFS reader on first read
	// CRITICAL: Reader is created ONCE and persists across all reads!
	// This enables:
	// - HTTP connection reuse
	// - Sequential read detection
	// - Intelligent prefetching
	// - Progressive chunk delivery
	if fh.file.info.IsRemote() {
		fh.readerOnce.Do(func() {
			var err error
			fh.rfsReader, err = fh.file.rfs.GetReader(fh.file.info)
			fh.readerErr = err
		})

		// Check if creation failed
		if fh.readerErr != nil {
			fh.logger.Error().Err(fh.readerErr).Msg("Failed to create RFS reader")
			return nil, syscall.EIO
		}
	}

	// Ensure we have an RFS reader
	if fh.rfsReader == nil {
		return nil, syscall.EIO
	}

	// Read from RFS (with automatic prefetching, connection reuse, etc.)
	n, err := fh.rfsReader.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		fh.logger.Error().Err(err).Int64("offset", off).Int("size", len(dest)).Msg("RFS read failed")
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
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
	if fh.rfsReader != nil {
		fh.file.rfs.ReleaseReader(fh.file.info)
	}

	return 0
}

func (fh *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

func (fh *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}
