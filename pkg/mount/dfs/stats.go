package dfs

import (
	"sync/atomic"
)

// Stats provides unified statistics across all DFS mounts.
// All fields are atomic for lock-free concurrent access.
type Stats struct {
	// Disk cache statistics
	CacheDirSize  atomic.Int64 // Total bytes used across all mounts
	CacheDirLimit atomic.Int64 // Total cache limit across all mounts

	// File operations
	OpenedFiles atomic.Int64 // Currently opened files
	ActiveReads atomic.Int64 // Currently active read operations

	// Cumulative counters (since service start)
	TotalBytesRead atomic.Int64 // Total bytes downloaded/read
	TotalErrors    atomic.Int64 // Total read/download errors
}

// Reset resets all statistics to zero
func (s *Stats) Reset() {
	s.CacheDirSize.Store(0)
	s.CacheDirLimit.Store(0)
	s.OpenedFiles.Store(0)
	s.ActiveReads.Store(0)
	s.TotalBytesRead.Store(0)
	s.TotalErrors.Store(0)
}

