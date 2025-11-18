package dfs

import (
	"context"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
	"golang.org/x/sync/singleflight"
)

type DirLevel int

const (
	LevelRoot DirLevel = iota // This is __all__, version.txt, torrents, __bad__ and custom dirs
	LevelTorrent
	LevelFile
)

// Dir implements a FUSE directory with RFS streaming
type Dir struct {
	fs.Inode
	vfs           *vfs.Manager
	level         DirLevel
	name          string
	children      *xsync.Map[string, *ChildEntry] // key is the child name
	config        *common.FuseConfig
	logger        zerolog.Logger
	populated     atomic.Bool
	modTime       uint64
	manager       *manager.Manager
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
func NewDir(vfsManager *vfs.Manager, manager *manager.Manager, name string, level DirLevel, modTime uint64, config *common.FuseConfig, logger zerolog.Logger) *Dir {
	return &Dir{
		vfs:      vfsManager,
		name:     name,
		children: xsync.NewMap[string, *ChildEntry](),
		level:    level,
		config:   config,
		logger:   logger,
		modTime:  modTime,
		manager:  manager,
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
		d.populateChildren(ctx)
		// Try again after population
		child, exists = d.children.Load(name)
		if !exists {
			return nil, syscall.ENOENT
		}
		return d.returnExistingChild(ctx, name, child, out)

	case LevelFile:
		// For file level, only load the specific file
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
			child.node = NewDir(d.vfs, d.manager, name, d.level+1, d.modTime, d.config, d.logger)
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
			out.Attr.Size = uint64(fileNode.info.Size())
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
	// GetReader torrent from source
	info, err := d.manager.GetTorrentFile(d.name, name)
	if err != nil {
		return nil, syscall.EIO
	}
	d.addFile(info)

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
	case LevelFile:
		// GetReader torrent name from node
		node := child.node
		fileNode, ok := node.(*File)
		if !ok {
			return syscall.EINVAL
		}
		// Remove file from source
		if err := d.manager.RemoveEntry(fileNode.info); err != nil {
			d.logger.Error().Err(err).Str("file", fileNode.info.Name()).Msg("Failed to remove file from source")
			return syscall.EIO
		}
		// Note: Readers are automatically cleaned up by RFS manager's idle timeout
	default:
		return syscall.EPERM
	}

	// Remove from our children map
	d.children.Delete(name)

	return 0
}

// RmDir removes a directory from this directory
func (d *Dir) RmDir(ctx context.Context, name string) syscall.Errno {
	// Check if the child exists
	child, exists := d.children.Load(name)
	if !exists {
		return syscall.ENOENT
	}

	// Handle different types of deletions based on directory level and file type
	switch d.level {
	case LevelTorrent:
		// GetReader torrent name from node
		node := child.node
		torrentDir, ok := node.(*Dir)
		if !ok {
			return syscall.EINVAL
		}
		// Remove torrent from source
		info, err := d.manager.GetTorrentEntry(torrentDir.name)
		if err != nil {
			return syscall.EIO
		}
		if err := d.manager.RemoveEntry(info); err != nil {
			d.logger.Error().Err(err).Str("torrent", torrentDir.name).Msg("Failed to remove torrent from source")
			return syscall.EIO
		}
	default:
		return syscall.EPERM
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
			d.populateTorrents(ctx)
		case LevelFile:
			d.populateTorrent(ctx)
		}

		d.populated.Store(true)
		return nil, nil
	})
}

// PopulateRoot populates the root directory with initial entries
func (d *Dir) populateRootChildren(ctx context.Context) {
	// AddOrUpdate standard directories
	entries := d.manager.GetEntries()
	for _, entry := range entries {
		if entry.IsDir() {
			d.addDirectory(&entry)
		} else {
			d.addContentFile(entry)
		}
	}
}

func (d *Dir) populateTorrents(ctx context.Context) {
	_, entries := d.manager.GetEntryChildren(d.name)
	if entries == nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			// Just store metadata
			name := entry.Name()
			fullPath := d.name + "/" + name

			childEntry := &ChildEntry{
				node: nil, // Create lazily on first access
				attr: fs.StableAttr{
					Mode: fuse.S_IFDIR | 0755,
					Ino:  hashPath(fullPath),
				},
			}
			d.children.Store(name, childEntry)
		}
	}
}

func (d *Dir) populateTorrent(ctx context.Context) {
	// GetReader files for this torrent from source
	current, children := d.manager.GetTorrentChildren(d.name)
	if current == nil || children == nil {
		return
	}

	// Iterate over files map
	for _, child := range children {
		d.addFile(&child)
	}
}

func (d *Dir) addContentFile(entry manager.FileInfo) {
	fileNode := newFile(
		d.vfs,
		d.config,
		&entry,
		d.logger,
	)

	childEntry := &ChildEntry{
		node: fileNode,
		attr: fs.StableAttr{
			Mode: fuse.S_IFREG | 0644,
			Ino:  hashPath(entry.Name()),
		},
	}

	d.children.Store(entry.Name(), childEntry)
}

func (d *Dir) addDirectory(entry *manager.FileInfo) {
	dirNode := NewDir(d.vfs, d.manager, entry.Name(), d.level+1, uint64(entry.ModTime().Unix()), d.config, d.logger)

	// Hash full path to ensure unique inodes
	fullPath := d.name + "/" + entry.Name()

	e := &ChildEntry{
		node: dirNode,
		attr: fs.StableAttr{
			Mode: fuse.S_IFDIR | 0755,
			Ino:  hashPath(fullPath),
		},
	}

	d.children.Store(entry.Name(), e)
}

func (d *Dir) addFile(entry *manager.FileInfo) {

	fileNode := newFile(d.vfs, d.config, entry, d.logger)

	// Hash full path to ensure unique inodes
	fullPath := d.name + "/" + entry.Name()

	e := &ChildEntry{
		node: fileNode,
		attr: fs.StableAttr{
			Mode: fuse.S_IFREG | 0644,
			Ino:  hashPath(fullPath),
		},
	}

	d.children.Store(entry.Name(), e)
}
