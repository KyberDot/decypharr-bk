//go:build linux || freebsd || openbsd || netbsd

package vfs

import (
	"os"
	"syscall"
	"time"
)

// getFileAccessTime returns the access time for a file (platform-specific)
func (m *Manager) getFileAccessTime(cacheKey string, fileInfo os.FileInfo) time.Time {
	// Try to get from in-memory tracking first
	if cacheKey != "" {
		if sf, ok := m.files.Load(cacheKey); ok {
			sf.mu.RLock()
			accessTime := sf.lastAccess
			sf.mu.RUnlock()
			return accessTime
		}
	}

	// Fallback: use file system access time (Linux/BSD)
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		return time.Unix(int64(stat.Atim.Sec), int64(stat.Atim.Nsec))
	}

	// Last resort: use modification time
	return fileInfo.ModTime()
}
