//go:build !windows

package hanwen

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
)

const (
	// ReadTimeout is the maximum time a single read operation can take
	// Increased to 120s to handle slow debrid/CDN connections
	ReadTimeout  = 120 * time.Second
	AttrTimeout  = 30 * time.Second
	EntryTimeout = 1 * time.Second
)

func init() {
	backend.Register(backend.Hanwen, NewBackend)
}

// Backend implements the hanwen/go-fuse backend
type Backend struct {
	config      *config.FuseConfig
	logger      zerolog.Logger
	server      *fuse.Server
	ready       atomic.Bool
	unmountFunc func(ctx context.Context)
	rootNode    backend.RootNode
}

// NewBackend creates a new hanwen backend
func NewBackend(config *config.FuseConfig) (backend.Backend, error) {
	return &Backend{
		config: config,
		logger: logger.New("hanwen-backend"),
	}, nil
}

// Mount mounts the filesystem using hanwen/go-fuse
func (b *Backend) Mount(ctx context.Context, root backend.RootNode) error {
	b.rootNode = root

	// Create mount point if it doesn't exist(skip if on Windows)
	if runtime.GOOS != "windows" {
		_ = os.MkdirAll(b.config.MountPath, 0755)
	}
	// Try to unmount if already mounted
	b.forceUnmount()

	mountOpt := fuse.MountOptions{
		FsName:               "decypharr",
		Debug:                false,
		Name:                 "decypharr",
		DisableXAttrs:        true,
		IgnoreSecurityLabels: true,
		MaxWrite:             1024 * 1024,
		AllowOther:           b.config.AllowOther,
	}

	var opt []string

	if b.config.DefaultPermissions {
		opt = append(opt, "default_permissions")
	}

	if runtime.GOOS == "darwin" {
		opt = append(opt, "volname=decypharr")
		opt = append(opt, "noapplexattr")
		opt = append(opt, "noappledouble")
	}

	mountOpt.Options = opt

	// get the hanwen-specific root dir
	rootDir, ok := root.GetRootDir().(*Dir)
	if !ok {
		return fmt.Errorf("root node must be hanwen Dir type")
	}

	// Configure FUSE options
	// Use short entry timeout (1s) to ensure new files appear quickly
	entryTimeout := EntryTimeout
	attrTimeout := AttrTimeout
	opts := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: mountOpt,
		UID:          b.config.UID,
		GID:          b.config.GID,
	}

	// Start timer before creating NodeFS - adjust timeout duration as needed
	mountCtx, cancel := context.WithTimeout(ctx, b.config.DaemonTimeout)
	defer cancel()

	// Channel to receive the result of fs.Mount
	type fsResult struct {
		server *fuse.Server
		err    error
	}
	fsResultChan := make(chan fsResult, 1)

	// Run fs.Mount in a goroutine
	go func() {
		server, err := fs.Mount(b.config.MountPath, rootDir, opts)
		fsResultChan <- fsResult{server: server, err: err}
	}()

	var server *fuse.Server
	select {
	case result := <-fsResultChan:
		server = result.server
		if result.err != nil {
			return fmt.Errorf("failed to create mount: %w", result.err)
		}
	case <-mountCtx.Done():
		b.ready.Store(false)
		return fmt.Errorf("timeout creating mount: %w", mountCtx.Err())
	}

	b.server = server

	// Now wait for the mount to be ready with the same timeout context
	b.logger.Info().
		Str("mount_path", b.config.MountPath).
		Msg("Waiting for mount to be ready")

	waitChan := make(chan error, 1)
	go func() {
		waitChan <- server.WaitMount()
	}()

	select {
	case err := <-waitChan:
		if err != nil {
			_ = server.Unmount() // cleanup on error
			return fmt.Errorf("failed to wait for mount: %w", err)
		}
	case <-mountCtx.Done():
		_ = server.Unmount() // cleanup on timeout
		return fmt.Errorf("timeout waiting for mount to be ready: %w", mountCtx.Err())
	}

	umount := func(ctx context.Context) {
		b.logger.Info().Msg("Unmounting filesystem")

		// Create a channel to track completion
		done := make(chan struct{})

		go func() {
			// Close VFS manager
			if root.GetVFS() != nil {
				if err := root.GetVFS().Close(); err != nil {
					b.logger.Warn().Err(err).Msg("Failed to close VFS")
				}
			}

			_ = server.Unmount()
			time.Sleep(1 * time.Second)

			// Check if still mounted
			if _, err := os.Stat(b.config.MountPath); err == nil {
				b.logger.Warn().Msg("FUSE filesystem still mounted, attempting force unmount")
				b.forceUnmount()
			}

			close(done)
		}()

		// Wait for unmount to complete or context timeout
		select {
		case <-done:
			b.logger.Info().Msg("Filesystem unmounted successfully")
		case <-ctx.Done():
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount timed out, forcing unmount")
			b.forceUnmount()
		}
	}

	b.unmountFunc = umount
	b.ready.Store(true)
	return nil
}

// Unmount unmounts the filesystem
func (b *Backend) Unmount(ctx context.Context) error {
	b.logger.Info().Msg("Unmounting hanwen backend")
	if b.unmountFunc != nil {
		b.unmountFunc(ctx)
	} else {
		// Use force unmount
		b.forceUnmount()
	}

	// Close VFS manager
	if b.rootNode != nil && b.rootNode.GetVFS() != nil {
		if err := b.rootNode.GetVFS().Close(); err != nil {
			b.logger.Warn().Err(err).Msg("Failed to close VFS")
		}
	}
	return nil
}

// WaitReady waits for the mount to be ready
func (b *Backend) WaitReady(ctx context.Context) error {
	if b.server == nil {
		return fmt.Errorf("server not initialized")
	}
	return b.server.WaitMount()
}

// IsReady returns true if the mount is ready
func (b *Backend) IsReady() bool {
	return b.ready.Load()
}

// Type returns the backend type
func (b *Backend) Type() backend.Type {
	return backend.Hanwen
}

// forceUnmount attempts to force unmount a path using system commands
func (b *Backend) forceUnmount() {
	methods := [][]string{
		{"umount", b.config.MountPath},
		{"umount", "-l", b.config.MountPath}, // lazy unmount
		{"fusermount", "-uz", b.config.MountPath},
		{"fusermount3", "-uz", b.config.MountPath},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, method := range methods {
		if err := b.tryUnmountCommand(ctx, method...); err == nil {
			return
		}
		if ctx.Err() != nil {
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount command timed out")
			return
		}
	}
}

// tryUnmountCommand tries to run an unmount command
func (b *Backend) tryUnmountCommand(ctx context.Context, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	return cmd.Run()
}

// RootDir wraps the hanwen Dir to implement backend.RootNode interface
type RootDir struct {
	*Dir
}

// Getattr returns root directory attributes
func (r *RootDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Nlink = 2 // Directories have 2 links (itself + "." entry)
	out.Uid = r.config.UID
	out.Gid = r.config.GID
	now := time.Now()
	out.Atime = uint64(now.Unix())
	out.Mtime = uint64(now.Unix())
	out.Ctime = uint64(now.Unix())
	out.AttrValid = uint64(AttrTimeout.Seconds())
	return 0
}

// GetVFS returns the VFS manager
func (r *RootDir) GetVFS() *config.FuseConfig {
	return r.config
}
