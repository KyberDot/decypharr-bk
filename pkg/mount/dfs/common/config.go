package common

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// FuseConfig holds the simplified configuration for the FUSE filesystem
type FuseConfig struct {
	MountPath string
	CacheDir  string

	// Cache
	CacheDiskSize        int64 // in bytes
	CacheCleanupInterval time.Duration

	CacheExpiry     time.Duration
	FileIdleTimeout time.Duration

	// Performance settings
	ChunkSize          int64
	MaxConcurrentReads int
	ReadAheadSize      int64
	BufferSize         int64 // In-memory buffer size
	DaemonTimeout      time.Duration

	// File system settings
	UID                uint32
	GID                uint32
	Umask              uint32
	AsyncRead          bool
	AllowOther         bool
	DefaultPermissions bool

	// Advanced settings
	AttrTimeout     time.Duration
	EntryTimeout    time.Duration
	NegativeTimeout time.Duration

	// Health and monitoring
	StatsInterval time.Duration

	// Memory-only mode (no disk caching)
	MemoryOnlyMode   bool  // Enable pure memory streaming
	MemoryLimit      int64 // Per-file memory limit (default: 100MB)
	MemoryChunkSize  int64 // Memory chunk size (default: 2MB)
	MemoryBufferSize int64 // Ring buffer size (default: 8MB)
}

// DefaultFuseConfig returns a streaming-optimized default configuration
func DefaultFuseConfig() *FuseConfig {
	return &FuseConfig{
		// Performance defaults optimized for streaming
		MaxConcurrentReads:   config.DefaultDFSMaxConcurrentRead,
		DaemonTimeout:        time.Second * 10, // Longer timeout for reliability
		CacheExpiry:          24 * time.Hour,   // Longer cache for popular content
		CacheCleanupInterval: 5 * time.Minute,  // More frequent cleanup
		AsyncRead:            true,
		FileIdleTimeout:      1 * time.Minute, // Idle file handle timeout

		// File system defaults
		UID:                1000,
		GID:                1000,
		Umask:              0022,
		AllowOther:         true,
		DefaultPermissions: true,

		// Advanced defaults optimized for streaming
		AttrTimeout:     30 * time.Second, // Longer attribute caching
		EntryTimeout:    30 * time.Second, // Longer entry caching
		NegativeTimeout: 5 * time.Second,  // Short negative cache

		// Health defaults
		StatsInterval: 1 * time.Minute, // More frequent stats for monitoring
	}
}

// ParseFuseConfig converts config.DFS to internal FuseConfig
func ParseFuseConfig(mountName string) (*FuseConfig, error) {
	fuseConfig := DefaultFuseConfig()
	mainCfg := config.Get()
	cfg := mainCfg.Mount.DFS
	totalDebrids := len(mainCfg.Debrids)

	fuseConfig.CacheDir = filepath.Join(cfg.CacheDir, mountName)
	fuseConfig.MountPath = filepath.Join(mainCfg.Mount.MountPath, mountName)

	// Parse durations
	if cfg.AttrTimeout != "" {
		timeout, err := time.ParseDuration(cfg.AttrTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid attr timeout: %w", err)
		}
		fuseConfig.AttrTimeout = timeout
	}

	if cfg.EntryTimeout != "" {
		timeout, err := time.ParseDuration(cfg.EntryTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid entry timeout: %w", err)
		}
		fuseConfig.EntryTimeout = timeout
	}

	if cfg.DaemonTimeout != "" {
		timeout, err := time.ParseDuration(cfg.DaemonTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid read timeout: %w", err)
		}
		fuseConfig.DaemonTimeout = timeout
	}
	if cfg.DiskCacheSize != "" {
		size, err := parseSize(cfg.DiskCacheSize)
		if err != nil {
			return nil, fmt.Errorf("invalid disk cache size: %w", err)
		}
		fuseConfig.CacheDiskSize = size / int64(totalDebrids)
	}

	if cfg.CacheCleanupInterval != "" {
		interval, err := time.ParseDuration(cfg.CacheCleanupInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid cache cleanup interval: %w", err)
		}
		fuseConfig.CacheCleanupInterval = interval
	}

	if cfg.ChunkSize != "" {
		size, err := parseSize(cfg.ChunkSize)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk size: %w", err)
		}
		fuseConfig.ChunkSize = size
	}

	if cfg.CacheExpiry != "" {
		ttl, err := time.ParseDuration(cfg.CacheExpiry)
		if err != nil {
			return nil, fmt.Errorf("invalid memory cache TTL: %w", err)
		}
		fuseConfig.CacheExpiry = ttl
	}

	if cfg.ReadAheadSize != "" {
		size, err := parseSize(cfg.ReadAheadSize)
		if err != nil {
			return nil, fmt.Errorf("invalid read-ahead size: %w", err)
		}
		fuseConfig.ReadAheadSize = size
	}

	if cfg.BufferSize != "" {
		size, err := parseSize(cfg.BufferSize)
		if err != nil {
			return nil, fmt.Errorf("invalid buffer size: %w", err)
		}
		fuseConfig.BufferSize = size
	}

	// Only override if user explicitly set it to non-zero
	if cfg.MaxConcurrentReads > 0 {
		fuseConfig.MaxConcurrentReads = cfg.MaxConcurrentReads
	}

	// Memory-only mode configuration
	fuseConfig.MemoryOnlyMode = cfg.MemoryOnlyMode
	if cfg.MemoryLimit != "" {
		size, err := parseSize(cfg.MemoryLimit)
		if err != nil {
			return nil, fmt.Errorf("invalid memory limit: %w", err)
		}
		fuseConfig.MemoryLimit = size / int64(totalDebrids) // Split among debrids
	} else {
		fuseConfig.MemoryLimit = 100 * 1024 * 1024 // Default 100MB per file
	}

	if cfg.MemoryChunkSize != "" {
		size, err := parseSize(cfg.MemoryChunkSize)
		if err != nil {
			return nil, fmt.Errorf("invalid memory chunk size: %w", err)
		}
		fuseConfig.MemoryChunkSize = size
	} else {
		fuseConfig.MemoryChunkSize = 2 * 1024 * 1024 // Default 2MB
	}

	if cfg.MemoryBufferSize != "" {
		size, err := parseSize(cfg.MemoryBufferSize)
		if err != nil {
			return nil, fmt.Errorf("invalid memory buffer size: %w", err)
		}
		fuseConfig.MemoryBufferSize = size
	} else {
		fuseConfig.MemoryBufferSize = 8 * 1024 * 1024 // Default 8MB
	}

	// Otherwise keep the default (4) from DefaultFuseConfig()
	fuseConfig.UID = cfg.UID
	fuseConfig.GID = cfg.GID
	fuseConfig.AllowOther = cfg.AllowOther
	fuseConfig.AsyncRead = cfg.AsyncRead
	fuseConfig.DefaultPermissions = cfg.DefaultPermissions

	if cfg.Umask != "" {
		umask, err := parseUmask(cfg.Umask)
		if err != nil {
			return nil, fmt.Errorf("invalid umask: %w", err)
		}
		fuseConfig.Umask = umask
	}

	return fuseConfig, nil
}

// parseUmask parses umask strings like "0022"
func parseUmask(umaskStr string) (uint32, error) {
	var umask uint32
	if _, err := fmt.Sscanf(umaskStr, "%o", &umask); err != nil {
		return 0, fmt.Errorf("invalid umask format: %s", umaskStr)
	}
	return umask, nil
}

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(strings.ToUpper(sizeStr))

	var multiplier int64 = 1
	var numStr string

	switch {
	case strings.HasSuffix(sizeStr, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(sizeStr, "TB")
	case strings.HasSuffix(sizeStr, "GB"):
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(sizeStr, "GB")
	case strings.HasSuffix(sizeStr, "MB"):
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(sizeStr, "MB")
	case strings.HasSuffix(sizeStr, "KB"):
		multiplier = 1024
		numStr = strings.TrimSuffix(sizeStr, "KB")
	case strings.HasSuffix(sizeStr, "B"):
		multiplier = 1
		numStr = strings.TrimSuffix(sizeStr, "B")
	default:
		numStr = sizeStr
	}

	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	return num * multiplier, nil
}

// StreamingStats tracks streaming-specific performance metrics
type StreamingStats struct {
	// Network stats
	NetworkRequests   int64
	NetworkBytes      int64
	NetworkErrors     int64
	ConnectionReuse   int64
	PipelinedRequests int64

	// Performance stats
	ReadLatencyMs    float64
	RangeFetches     int64
	CacheHitRate     float64
	PrefetchHitRate  float64
	StreamingLatency float64

	// Streaming quality metrics
	StreamingInterruptions int64
	BufferUnderrunsMs      int64
	SeekOperations         int64
	ConcurrentStreams      int64
}

type Stats struct {
	// Network stats
	NetworkRequests int64
	NetworkBytes    int64
	NetworkErrors   int64

	// Performance stats
	ReadLatencyMs float64
	RangeFetches  int64
}
