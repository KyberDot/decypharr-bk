package config

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// Configuration constants optimized for streaming
const (
	DefaultChunkSize       = 8 * 1024 * 1024  // 4MB chunks - good balance for metadata ops
	DefaultReadAheadSize   = 16 * 1024 * 1024 // 16MB read-ahead buffer for streaming
	DefaultConcurrentReads = 4
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
	DaemonTimeout      time.Duration

	// File system settings
	UID                uint32
	GID                uint32
	Umask              uint32
	AsyncRead          bool
	AllowOther         bool
	AllowRoot          bool
	DefaultPermissions bool

	// Advanced settings
	AttrTimeout     time.Duration
	EntryTimeout    time.Duration
	NegativeTimeout time.Duration

	// Health and monitoring
	StatsInterval time.Duration
}

// DefaultFuseConfig returns a streaming-optimized default configuration
func DefaultFuseConfig() *FuseConfig {
	return &FuseConfig{
		// Performance defaults optimized for streaming
		MaxConcurrentReads:   DefaultConcurrentReads,
		DaemonTimeout:        time.Second * 10,       // Longer timeout for reliability
		ChunkSize:            DefaultChunkSize,       // Larger chunks for fewer requests
		CacheDiskSize:        5 * 1024 * 1024 * 1024, // 5GB disk cache
		CacheExpiry:          24 * time.Hour,         // Longer cache for popular content
		CacheCleanupInterval: 5 * time.Minute,        // More frequent cleanup
		AsyncRead:            true,
		ReadAheadSize:        DefaultReadAheadSize,
		FileIdleTimeout:      10 * time.Minute, // Idle file handle timeout

		// File system defaults
		UID:                1000,
		GID:                1000,
		Umask:              0022,
		AllowOther:         true,
		AllowRoot:          false,
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
func ParseFuseConfig(debridConfig config.Debrid) (*FuseConfig, error) {
	fuseConfig := DefaultFuseConfig()
	mainCfg := config.Get()
	cfg := mainCfg.Dfs
	totalDebrids := len(mainCfg.Debrids)

	fuseConfig.CacheDir = filepath.Join(cfg.CacheDir, debridConfig.Name)
	fuseConfig.MountPath = debridConfig.RcloneMountPath
	if debridConfig.RcloneMountPath == "" {
		fuseConfig.MountPath = filepath.Join(cfg.MountPath, debridConfig.Name)
	}

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

	// Only override if user explicitly set it to non-zero
	if cfg.MaxConcurrentReads > 0 {
		fuseConfig.MaxConcurrentReads = cfg.MaxConcurrentReads
	}

	// Otherwise keep the default (4) from DefaultFuseConfig()
	fuseConfig.UID = cfg.UID
	fuseConfig.GID = cfg.GID
	fuseConfig.AllowOther = cfg.AllowOther
	fuseConfig.AllowRoot = cfg.AllowRoot
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
