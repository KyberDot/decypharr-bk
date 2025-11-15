package common

import (
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
)

// StatsTracker is a lightweight struct for tracking VFS statistics
type StatsTracker struct {
	ActiveReads atomic.Int64
	OpenedFiles *xsync.Map[string, struct{}]
}

// NewStatsTracker creates a new StatsTracker
func NewStatsTracker() *StatsTracker {
	return &StatsTracker{
		OpenedFiles: xsync.NewMap[string, struct{}](),
	}
}

// TrackActiveRead increments/decrements active read counter
func (st *StatsTracker) TrackActiveRead(delta int64) {
	st.ActiveReads.Add(delta)
}

func (st *StatsTracker) AddOpenedFile(file string) {
	st.OpenedFiles.Store(file, struct{}{})
}

func (st *StatsTracker) RemoveOpenedFile(file string) {
	st.OpenedFiles.Delete(file)
}
