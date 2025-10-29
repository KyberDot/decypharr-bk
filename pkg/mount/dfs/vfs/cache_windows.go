//go:build windows

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

	// Fallback: use file system access time (Windows)
	if stat, ok := fileInfo.Sys().(*syscall.Win32FileAttributeData); ok {
		// Convert Windows FILETIME to Unix time
		return time.Unix(0, stat.LastAccessTime.Nanoseconds())
	}

	// Last resort: use modification time
	return fileInfo.ModTime()
}
