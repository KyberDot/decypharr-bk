package dfs

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
	"github.com/sirrobot01/decypharr/pkg/version"
	"golang.org/x/sync/singleflight"
)

type DirLevel int

const (
	LevelRoot DirLevel = iota // This is __all__, version.txt, torrents, __bad__ and custom dirs
	LevelTorrent
	LevelFile
)

// Dir implements a FUSE directory with sparse file caching
type Dir struct {
	fs.Inode
	vfs           *vfs.Manager
	debrid        *store.Cache
	level         DirLevel
	name          string
	children      *xsync.Map[string, *ChildEntry] // key is the child name
	config        *config.FuseConfig
	logger        zerolog.Logger
	populated     atomic.Bool
	modTime       uint64
	populateGroup singleflight.Group // Deduplicate concurrent population requests
}

type ChildEntry struct {
	node fs.InodeEmbedder
	attr fs.StableAttr
}

var _ = (fs.NodeLookuper)((*Dir)(nil))
var _ = (fs.NodeReaddirer)((*Dir)(nil))
var _ = (fs.NodeGetattrer)((*Dir)(nil))
var _ = (fs.NodeUnlinker)((*Dir)(nil))

// NewDir creates a new directory
func NewDir(vfsCache *vfs.Manager, debridCache *store.Cache, name string, level DirLevel, modTime uint64, config *config.FuseConfig, logger zerolog.Logger) *Dir {
	return &Dir{
		vfs:      vfsCache,
		debrid:   debridCache,
		name:     name,
		children: xsync.NewMap[string, *ChildEntry](),
		level:    level,
		config:   config,
		logger:   logger,
		modTime:  modTime,
	}
}

// Getattr returns directory attributes
func (d *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Size = 4096 // Standard directory size
	out.Nlink = 2   // Directories have 2 links (itself + "." entry)
	out.Uid = d.config.UID
	out.Gid = d.config.GID
	out.Atime = d.modTime
	out.Mtime = d.modTime
	out.Ctime = d.modTime
	out.AttrValid = uint64(d.config.AttrTimeout.Seconds())
	return 0
}

func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// First check if child already exists in cache (fast path)
	child, exists := d.children.Load(name)
	if exists {
		return d.returnExistingChild(ctx, name, child, out)
	}

	// Not in cache - behavior depends on directory level
	switch d.level {
	case LevelRoot, LevelTorrent:
		// For root and torrent levels, populate all children
		// (these are small - just a few directories)
		d.populateChildren(ctx)

		// Try again after population
		child, exists = d.children.Load(name)
		if !exists {
			return nil, syscall.ENOENT
		}
		return d.returnExistingChild(ctx, name, child, out)

	case LevelFile:
		// For file level, ONLY load the specific requested file
		// Don't load all files in the torrent
		return d.loadSingleFile(ctx, name, out)

	default:
		return nil, syscall.ENOENT
	}
}

// returnExistingChild handles returning an already-cached child
func (d *Dir) returnExistingChild(ctx context.Context, name string, child *ChildEntry, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if child.node == nil {
		if child.attr.Mode&fuse.S_IFDIR != 0 {
			// It's a directory - create the Dir node
			child.node = NewDir(d.vfs, d.debrid, name, d.level+1, d.modTime, d.config, d.logger)
		} else {
			// It's a file - shouldn't happen as files are always created fully
			return nil, syscall.ENOENT
		}
	}

	// Set entry attributes
	out.Attr.Mode = child.attr.Mode
	out.Attr.Uid = d.config.UID
	out.Attr.Gid = d.config.GID
	out.Attr.Atime = d.modTime
	out.Attr.Mtime = d.modTime
	out.Attr.Ctime = d.modTime

	// Set file size for regular files
	if child.attr.Mode&fuse.S_IFREG != 0 {
		if fileNode, ok := child.node.(*File); ok {
			out.Attr.Size = uint64(fileNode.torrentFile.Size)
		}
	}

	out.AttrValid = uint64(d.config.AttrTimeout.Seconds())
	out.EntryValid = uint64(d.config.EntryTimeout.Seconds())

	// Check if we already have an inode for this child
	if existingChild := d.GetChild(name); existingChild != nil {
		return existingChild, 0
	}

	// Create new inode for the child
	return d.NewInode(ctx, child.node, child.attr), 0
}

// loadSingleFile loads only one specific file without populating all children
func (d *Dir) loadSingleFile(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Get torrent from debrid cache
	torrent := d.debrid.GetTorrentByName(d.name)
	if torrent == nil {
		return nil, syscall.ENOENT
	}

	// Search for the specific file
	file, exists := torrent.GetFile(name)
	if !exists {
		return nil, syscall.ENOENT
	}
	d.addFile(torrent, file)

	// Now retrieve and return it
	child, ex := d.children.Load(name)
	if !ex {
		return nil, syscall.ENOENT
	}

	return d.returnExistingChild(ctx, name, child, out)
}

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Ensure children are populated
	d.populateChildren(ctx)

	entries := make([]fuse.DirEntry, 0, d.children.Size())

	d.children.Range(func(key string, child *ChildEntry) bool {
		entries = append(entries, fuse.DirEntry{
			Mode: child.attr.Mode,
			Name: key,
			Ino:  child.attr.Ino,
		})
		return true
	})

	return fs.NewListDirStream(entries), 0
}

func (d *Dir) Refresh() {
	// Reset our internal populated flag
	d.populated.Store(false)
	_ = d.NotifyEntry(d.name)
}

// Unlink removes a child from this directory
func (d *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	// Check if the child exists
	child, exists := d.children.Load(name)
	if !exists {
		return syscall.ENOENT
	}

	// Handle different types of deletions based on directory level and file type
	switch d.level {
	case LevelRoot:
		return syscall.EPERM
	case LevelTorrent:
		// For torrent level, check if it's a directory (torrent) that can be deleted
		if child.attr.Mode&fuse.S_IFDIR != 0 {
			// Delete torrent from debrid store
			d.debrid.RemoveTorrent(name)
			d.logger.Info().Str("name", name).Msg("Removed torrent from debrid store")
		}

	case LevelFile:
		// Get torrent name from node
		node := child.node
		fileNode, ok := node.(*File)
		if !ok {
			return syscall.EINVAL
		}

		// Remove file from debrid cache
		if err := d.debrid.RemoveFile(fileNode.torrentName, fileNode.torrentFile.Name); err != nil {
			d.logger.Error().Err(err).Str("file", fileNode.torrentFile.Name).Str("torrent", d.name).Msg("Failed to remove file from debrid cache")
			return syscall.EIO
		}

		// Close the file from range_manager
		_ = d.vfs.CloseFile(filepath.Join(fileNode.torrentName, fileNode.torrentFile.Name))
	}

	// Remove from our children map
	d.children.Delete(name)

	return 0
}

func (d *Dir) populateChildren(ctx context.Context) {
	if d.populated.Load() {
		return
	}

	// Use singleflight to ensure only one goroutine populates
	// Multiple concurrent lookups will wait for the same result
	_, _, _ = d.populateGroup.Do("populate", func() (interface{}, error) {
		// Double-check after acquiring singleflight lock
		if d.populated.Load() {
			return nil, nil
		}

		switch d.level {
		case LevelRoot:
			d.populateRootChildren(ctx)
		case LevelTorrent:
			d.populateTorrentChildren(ctx)
		case LevelFile:
			d.populateFileChildren(ctx)
		}

		d.populated.Store(true)
		return nil, nil
	})
}

// PopulateRoot populates the root directory with initial entries
func (d *Dir) populateRootChildren(ctx context.Context) {
	// Add version.txt file
	d.addVersionFile()

	// Add standard directories
	now := uint64(time.Now().Unix())
	d.addDirectory("__all__", now)
	d.addDirectory("torrents", now)
	d.addDirectory("__bad__", now)

	customFolders := d.debrid.GetCustomFolders()
	for _, folder := range customFolders {
		d.addDirectory(folder, now)
	}
}

func (d *Dir) populateTorrentChildren(ctx context.Context) {
	files := d.debrid.GetListing(d.name)
	if files == nil {
		return
	}
	for _, file := range files {
		if file.IsDir() {
			// Just store metadata
			name := file.Name()
			fullPath := d.name + "/" + name

			entry := &ChildEntry{
				node: nil, // Create lazily on first access
				attr: fs.StableAttr{
					Mode: fuse.S_IFDIR | 0755,
					Ino:  hashPath(fullPath),
				},
			}
			d.children.Store(name, entry)
		}
	}
}

func (d *Dir) populateFileChildren(ctx context.Context) {
	// Get files for this torrent from your debrid cache
	torrent := d.debrid.GetTorrentByName(d.name)
	if torrent == nil {
		return
	}

	files := torrent.GetFiles()

	for _, file := range files {
		d.addFile(torrent, file)
	}
}

func (d *Dir) addVersionFile() {
	vs := version.GetInfo().String()
	content := []byte(vs + "\n")
	fileNode := newFile(
		d.vfs,
		d.config,
		"",
		types.File{Name: "version.txt", Size: int64(len(content))},
		time.Now(),
		content,
		d.logger,
	)

	entry := &ChildEntry{
		node: fileNode,
		attr: fs.StableAttr{
			Mode: fuse.S_IFREG | 0644,
			Ino:  hashPath("version.txt"),
		},
	}

	d.children.Store("version.txt", entry)
}

func (d *Dir) addDirectory(name string, modTime uint64) {
	dirNode := NewDir(d.vfs, d.debrid, name, d.level+1, modTime, d.config, d.logger)

	// Hash full path to ensure unique inodes
	fullPath := d.name + "/" + name

	entry := &ChildEntry{
		node: dirNode,
		attr: fs.StableAttr{
			Mode: fuse.S_IFDIR | 0755,
			Ino:  hashPath(fullPath),
		},
	}

	d.children.Store(name, entry)
}

func (d *Dir) addFile(torrent *store.CachedTorrent, torrentFile types.File) {
	// Create file node based on your file info
	torrentName := internString(d.name)
	fileNode := newFile(d.vfs, d.config, torrentName, torrentFile, torrent.AddedOn, nil, d.logger)

	// Hash full path to ensure unique inodes
	fullPath := d.name + "/" + torrentFile.Name

	entry := &ChildEntry{
		node: fileNode,
		attr: fs.StableAttr{
			Mode: fuse.S_IFREG | 0644,
			Ino:  hashPath(fullPath),
		},
	}

	d.children.Store(torrentFile.Name, entry)
}
