package stats

import (
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// Snapshot holds a point-in-time stats snapshot.
// Using typed structs avoids map[string]any allocations on every JSON encode.
type Snapshot struct {
	System       SystemStats             `json:"system"`
	Debrids      []types.Stats           `json:"debrids"`
	Mount        *manager.MountStats     `json:"mount"`
	Usenet       *usenet.UsenetStats     `json:"usenet,omitempty"`
	ActiveStreams ActiveStreamStats       `json:"active_streams"`
	Storage      StorageStats            `json:"storage"`
	Queue        QueueStats              `json:"queue"`
	Arrs         ArrStats                `json:"arrs"`
	Repair       manager.RepairJobCounts `json:"repair"`
}

type SystemStats struct {
	HeapAllocMB   string `json:"heap_alloc_mb"`
	MemoryUsed    string `json:"memory_used"`
	GCCycles      uint32 `json:"gc_cycles"`
	Goroutines    int    `json:"goroutines"`
	NumCPU        int    `json:"num_cpu"`
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	GoVersion     string `json:"go_version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Uptime        string `json:"uptime"`
	StartTime     string `json:"start_time"`
}

type ActiveStreamStats struct {
	Count   int                     `json:"count"`
	Streams []*manager.ActiveStream `json:"streams"`
}

type StorageStats struct {
	DBSize       int64 `json:"db_size"`
	TotalEntries int   `json:"total_entries"`
}

type QueueStats struct {
	Pending int `json:"pending"`
}

type ArrStats struct {
	Count int      `json:"count"`
	Names []string `json:"names"`
}
