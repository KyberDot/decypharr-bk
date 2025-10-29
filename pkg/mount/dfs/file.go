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
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// File implements a FUSE file with sparse file caching
type File struct {
	fs.Inode
	config      *config.FuseConfig
	logger      zerolog.Logger
	torrentName string
	torrentFile types.File
	createdAt   time.Time
	content     []byte // For files like version.txt
	vfs         *vfs.Manager
}

func (f *File) IsRemote() bool {
	return f.vfs != nil && f.torrentName != "" && len(f.content) == 0
}

// FileHandle implements file operations with VFS
type FileHandle struct {
	file         *File
	vfsFile      *vfs.File // Direct file handle (no more Handle wrapper)
	vfsOnce      sync.Once // Ensures VFS file created exactly once
	vfsCreateErr error     // Stores any error from VFS file creation
	closed       atomic.Bool
	lastAccess   atomic.Int64
	logger       zerolog.Logger
}

var _ = (fs.NodeOpener)((*File)(nil))
var _ = (fs.NodeGetattrer)((*File)(nil))
var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))
var _ = (fs.FileFlusher)((*FileHandle)(nil))
var _ = (fs.FileFsyncer)((*FileHandle)(nil))

// newFile creates a new file
func newFile(vfsCache *vfs.Manager, config *config.FuseConfig, torrentName string, torrentFile types.File, createdAt time.Time, content []byte, logger zerolog.Logger) *File {
	return &File{
		config:      config,
		logger:      logger,
		torrentName: torrentName,
		torrentFile: torrentFile,
		content:     content,
		createdAt:   createdAt,
		vfs:         vfsCache,
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
	out.Size = uint64(f.torrentFile.Size)
	out.Nlink = 1 // Files always have 1 link (themselves)
	out.Blksize = 4096
	out.Blocks = (uint64(f.torrentFile.Size) + 511) / 512 // Number of 512-byte blocks
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

// Read implements VFS reading
func (fh *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.lastAccess.Store(time.Now().Unix())

	if fh.closed.Load() {
		return nil, syscall.EBADF
	}

	if off >= fh.file.torrentFile.Size {
		return fuse.ReadResultData([]byte{}), 0
	}

	// Handle static content
	if len(fh.file.content) > 0 {
		data := fh.readFromStaticContent(off, int64(len(dest)))
		return fuse.ReadResultData(data), 0
	}

	// Lazy-create VFS file on first read (not on Open)
	// This prevents unnecessary sparse file creation when file browsers
	// just open files for metadata without actually reading content
	// Uses sync.Once to ensure thread-safe creation
	if fh.file.IsRemote() {
		fh.vfsOnce.Do(func() {
			var err error
			fh.vfsFile, err = fh.file.vfs.CreateReader(fh.file.torrentName, fh.file.torrentFile)
			fh.vfsCreateErr = err
		})

		// Check if creation failed
		if fh.vfsCreateErr != nil {
			return nil, syscall.EIO
		}
	}

	// Ensure we have a VFS file
	if fh.vfsFile == nil {
		return nil, syscall.EIO
	}

	// Use ReadAtWithDownload for direct reading with download support
	n, err := fh.vfsFile.ReadAtWithDownload(ctx, dest, off)
	if err != nil && err != io.EOF {
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

	// Close VFS file
	// Note: We DON'T decrement the counter here because:
	// 1. Counter is incremented when File is created (CreateReader)
	// 2. Counter is decremented when File is evicted from cache or explicitly closed
	// 3. Multiple FileHandles can reference the same File (via cache)
	// 4. Decrementing here would cause double-decrement when cache evicts the file
	//
	// We also DON'T call vfsFile.Close() because that would stop all downloads
	// and close the file descriptor. The file is shared and managed by the cache.
	// Just sync any pending writes.
	if fh.vfsFile != nil {
		if err := fh.vfsFile.Sync(); err != nil {
			fh.logger.Error().Err(err).Msg("failed to sync VFS file")
		}
	}

	return 0
}

func (fh *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

func (fh *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return 0
}
