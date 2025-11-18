package config

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type (
	RepairStrategy     string
	RepairMode         string
	WebDavFolderNaming string
	MountType          string
	DownloadAction     string
)

const (
	MountTypeRclone         MountType = "rclone"
	MountTypeDFS            MountType = "dfs"
	MountTypeExternalRclone MountType = "external_rclone"
	MountTypeNone           MountType = "none"
)

const (
	DownloadActionSymlink  DownloadAction = "symlink"
	DownloadActionDownload DownloadAction = "download"
	DownloadActionStrm     DownloadAction = "strm"
	DownloadActionNone     DownloadAction = "none"
)

const (
	RepairModeArr RepairMode = "arr"
	RepairModeAll RepairMode = "all"

	RepairStrategyPerFile    RepairStrategy = "per_file"
	RepairStrategyPerTorrent RepairStrategy = "per_torrent"

	WebDavUseFileName          WebDavFolderNaming = "filename"
	WebDavUseOriginalName      WebDavFolderNaming = "original"
	WebDavUseFileNameNoExt     WebDavFolderNaming = "filename_no_ext"
	WebDavUseOriginalNameNoExt WebDavFolderNaming = "original_no_ext"
	WebdavUseHash              WebDavFolderNaming = "infohash"
)

var (
	instance   *Config
	once       sync.Once
	configPath string
)

type Debrid struct {
	Provider                     string   `json:"provider,omitempty"` // realdebrid, alldebrid, debridlink, torbox
	Name                         string   `json:"name,omitempty"`
	APIKey                       string   `json:"api_key,omitempty"`
	DownloadAPIKeys              []string `json:"download_api_keys,omitempty"`
	Folder                       string   `json:"folder,omitempty"` // Deprecated. Use Mount MountPath instead.
	DownloadUncached             bool     `json:"download_uncached,omitempty"`
	RateLimit                    string   `json:"rate_limit,omitempty"` // 200/minute or 10/second
	RepairRateLimit              string   `json:"repair_rate_limit,omitempty"`
	DownloadRateLimit            string   `json:"download_rate_limit,omitempty"`
	Proxy                        string   `json:"proxy,omitempty"`
	UnpackRar                    bool     `json:"unpack_rar,omitempty"`
	AddSamples                   bool     `json:"add_samples,omitempty"`
	MinimumFreeSlot              int      `json:"minimum_free_slot,omitempty"` // Minimum active pots to use this debrid
	Limit                        int      `json:"limit,omitempty"`             // Maximum number of total torrents
	TorrentsRefreshInterval      string   `json:"torrents_refresh_interval,omitempty"`
	DownloadLinksRefreshInterval string   `json:"download_links_refresh_interval,omitempty"`
	Workers                      int      `json:"workers,omitempty"`
	AutoExpireLinksAfter         string   `json:"auto_expire_links_after,omitempty"`

	// Folder
	FolderNaming string `json:"folder_naming,omitempty"` // Deprecated. Use global setting instead.

	// Rclone
	RcUrl         string `json:"rc_url,omitempty"`          // Deprecated. Use global setting instead.
	RcUser        string `json:"rc_user,omitempty"`         // Deprecated. Use global setting instead.
	RcPass        string `json:"rc_pass,omitempty"`         // Deprecated. Use global setting instead.
	RcRefreshDirs string `json:"rc_refresh_dirs,omitempty"` // Deprecated. Use global setting instead.

	// Directories
	Directories map[string]WebdavDirectories `json:"directories,omitempty"` // Deprecated. Use global setting instead.
}

// QBitTorrent is deprecated. Use Manager instead.
// Kept for backward compatibility with existing configs.
type QBitTorrent struct {
	DownloadFolder      string   `json:"download_folder,omitempty"`
	Categories          []string `json:"categories,omitempty"`
	RefreshInterval     int      `json:"refresh_interval,omitempty"`
	SkipPreCache        bool     `json:"skip_pre_cache,omitempty"`
	MaxDownloads        int      `json:"max_downloads,omitempty"`
	AlwaysRmTrackerUrls bool     `json:"always_rm_tracker_urls,omitempty"`
}

type Arr struct {
	Name             string `json:"name,omitempty"`
	Host             string `json:"host,omitempty"`
	Token            string `json:"token,omitempty"`
	Cleanup          bool   `json:"cleanup,omitempty"`
	SkipRepair       bool   `json:"skip_repair,omitempty"`
	DownloadUncached *bool  `json:"download_uncached,omitempty"`
	SelectedDebrid   string `json:"selected_debrid,omitempty"`
	Source           string `json:"source,omitempty"` // The source of the arr, e.g. "auto", "config", "". Auto means it was automatically detected from the arr
}

type CustomFolders struct {
	Filters map[string]string `json:"filters,omitempty"`
}

type Repair struct {
	Enabled     bool           `json:"enabled,omitempty"`
	Mode        RepairMode     `json:"repair_mode,omitempty"`
	Interval    string         `json:"interval,omitempty"`
	AutoProcess bool           `json:"auto_process,omitempty"`
	Workers     int            `json:"workers,omitempty"`
	ReInsert    bool           `json:"reinsert,omitempty"`
	Strategy    RepairStrategy `json:"strategy,omitempty"`
}

type Auth struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	APIToken string `json:"api_token,omitempty"`
}

type Rclone struct {
	// Global mount folder where all providers will be mounted as subfolders
	Enabled   bool   `json:"enabled,omitempty"`
	MountPath string `json:"mount_path,omitempty"`
	Port      string `json:"port,omitempty"`
	// Cache settings
	CacheDir string `json:"cache_dir,omitempty"`
	// VFS settings
	VfsCacheMode          string `json:"vfs_cache_mode,omitempty"`            // off, minimal, writes, full
	VfsCacheMaxAge        string `json:"vfs_cache_max_age,omitempty"`         // Maximum age of objects in the cache (default 1h)
	VfsDiskSpaceTotal     string `json:"vfs_disk_space_total,omitempty"`      // Total disk space available for the cache (default off)
	VfsCacheMaxSize       string `json:"vfs_cache_max_size,omitempty"`        // Maximum size of the cache (default off)
	VfsCachePollInterval  string `json:"vfs_cache_poll_interval,omitempty"`   // How often to poll for changes (default 1m)
	VfsReadChunkSize      string `json:"vfs_read_chunk_size,omitempty"`       // Read chunk size (default 128M)
	VfsReadChunkSizeLimit string `json:"vfs_read_chunk_size_limit,omitempty"` // Max chunk size (default off)
	VfsReadAhead          string `json:"vfs_read_ahead,omitempty"`            // read ahead size
	BufferSize            string `json:"buffer_size,omitempty"`               // Buffer size for reading files (default 16M)
	BwLimit               string `json:"bw_limit,omitempty"`                  // Bandwidth limit (default off)

	VfsCacheMinFreeSpace string `json:"vfs_cache_min_free_space,omitempty"`
	VfsFastFingerprint   bool   `json:"vfs_fast_fingerprint,omitempty"`
	VfsReadChunkStreams  int    `json:"vfs_read_chunk_streams,omitempty"`
	AsyncRead            *bool  `json:"async_read,omitempty"` // Use async read for files
	Transfers            int    `json:"transfers,omitempty"`  // Number of transfers to use (default 4)
	UseMmap              bool   `json:"use_mmap,omitempty"`

	// File system settings
	UID   uint32 `json:"uid,omitempty"` // User ID for mounted files
	GID   uint32 `json:"gid,omitempty"` // Group ID for mounted files
	Umask string `json:"umask,omitempty"`

	// Timeout settings
	AttrTimeout  string `json:"attr_timeout,omitempty"`   // Attribute cache timeout (default 1s)
	DirCacheTime string `json:"dir_cache_time,omitempty"` // Directory cache time (default 5m)

	// Performance settings
	NoModTime  bool `json:"no_modtime,omitempty"`  // Don't read/write modification time
	NoChecksum bool `json:"no_checksum,omitempty"` // Don't checksum files on upload

	LogLevel string `json:"log_level,omitempty"`
}

type DFS struct {
	// Core settings
	CacheExpiry          string `json:"cache_expiry,omitempty"`           // 1h, 30m etc
	CacheDir             string `json:"cache_dir,omitempty"`              // /tmp/decypharr-cache
	DiskCacheSize        string `json:"disk_cache_size,omitempty"`        // 10GB, 50GB etc
	CacheCleanupInterval string `json:"cache_cleanup_interval,omitempty"` // 10m, 1h etc
	// Performance settings

	ChunkSize          string `json:"chunk_size,omitempty"`           // 1MB, 4MB etc
	ReadAheadSize      string `json:"read_ahead_size,omitempty"`      // Read ahead size. e.g default to 16MB
	MaxConcurrentReads int    `json:"max_concurrent_reads,omitempty"` // Maximum concurrent read operations
	BufferSize         string `json:"buffer_size,omitempty"`          // In-memory buffer size for fast access (e.g., 4MB)

	DaemonTimeout string `json:"daemon_timeout,omitempty"` // Time after which the FUSE daemon will exit if idle

	// File system settings
	UID                uint32 `json:"uid,omitempty"`                 // User ID for mounted files
	GID                uint32 `json:"gid,omitempty"`                 // Group ID for mounted files
	Umask              string `json:"umask,omitempty"`               // File permissions mask
	AllowOther         bool   `json:"allow_other,omitempty"`         // Allow other users to access mount
	DefaultPermissions bool   `json:"default_permissions,omitempty"` // Enable permission checking

	// Advanced settings
	AttrTimeout     string `json:"attr_timeout,omitempty"`     // Attribute cache timeout
	EntryTimeout    string `json:"entry_timeout,omitempty"`    // Directory entry cache timeout
	NegativeTimeout string `json:"negative_timeout,omitempty"` // Negative lookup cache timeout

	// Health and monitoring
	StatsInterval string `json:"stats_interval,omitempty"` // How often to log stats
}

type ExternalRclone struct {
	RCUrl      string `json:"rc_url,omitempty"`
	RCUsername string `json:"rc_username,omitempty"`
	RCPassword string `json:"rc_password,omitempty"`
}

type Mount struct {
	Type      MountType `json:"type,omitempty"`
	MountPath string    `json:"mount_path,omitempty"`

	Rclone         Rclone         `json:"rclone,omitempty"`
	DFS            DFS            `json:"dfs,omitempty"`
	ExternalRclone ExternalRclone `json:"external_rclone,omitempty"`
}

type Config struct {
	// server
	BindAddress string `json:"bind_address,omitempty"`
	URLBase     string `json:"url_base,omitempty"`
	AppURL      string `json:"app_url,omitempty"`
	Port        string `json:"port,omitempty"`

	LogLevel string   `json:"log_level,omitempty"`
	Debrids  []Debrid `json:"debrids,omitempty"`

	Arrs   []Arr  `json:"arrs,omitempty"`
	Repair Repair `json:"repair,omitempty"`

	QBitTorrent QBitTorrent `json:"qbittorrent,omitempty"` // Deprecated: use Manager instead
	Rclone      Rclone      `json:"rclone,omitempty"`      // Deprecated: use Mounts instead
	Mount       Mount       `json:"mount,omitempty"`

	AllowedExt         []string `json:"allowed_file_types,omitempty"`
	MinFileSize        string   `json:"min_file_size,omitempty"`
	MaxFileSize        string   `json:"max_file_size,omitempty"`
	DiscordWebhook     string   `json:"discord_webhook_url,omitempty"`
	RemoveStalledAfter string   `json:"remove_stalled_after,omitzero"`
	CallbackURL        string   `json:"callback_url,omitempty"`
	EnableWebdavAuth   bool     `json:"enable_webdav_auth,omitempty"`
	UseAuth            bool     `json:"use_auth,omitempty"`
	SetupCompleted     bool     `json:"setup_completed,omitempty"` // Tracks if initial setup wizard was completed
	Auth               *Auth    `json:"-"`

	// Manager

	DownloadFolder        string                   `json:"download_folder,omitempty"`
	RefreshInterval       string                   `json:"refresh_interval,omitempty"`
	MaxDownloads          int                      `json:"max_downloads,omitempty"`
	SkipPreCache          bool                     `json:"skip_pre_cache,omitempty"`
	AlwaysRmTrackerUrls   bool                     `json:"always_rm_tracker_urls,omitempty"`
	Categories            []string                 `json:"categories,omitempty"`
	FolderNaming          WebDavFolderNaming       `json:"folder_naming,omitempty"`
	CustomFolders         map[string]CustomFolders `json:"custom_folders,omitempty"`
	DefaultDownloadAction DownloadAction           `json:"default_download_action,omitempty"`

	RefreshDirs  string `json:"refresh_dirs,omitempty"`
	Retries      int    `json:"retries,omitempty"`
	SkipAutoMove bool   `json:"skip_auto_move,omitempty"`
}

func (c *Config) JsonFile() string {
	return filepath.Join(GetMainPath(), "config.json")
}
func (c *Config) AuthFile() string {
	return filepath.Join(GetMainPath(), "auth.json")
}

func (c *Config) TorrentsFile() string {
	return filepath.Join(GetMainPath(), "torrents.json")
}

func (c *Config) loadConfig() error {
	// Load the config file
	// Read the JSON config file directly
	configFile := c.JsonFile()
	fmt.Printf("Loading config from %s\n", configFile)
	data, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Config file not found, creating a new one at %s\n", configFile)
			// Create a default config file if it doesn't exist
			if err := c.createConfig(); err != nil {
				return fmt.Errorf("failed to create config file: %w", err)
			}
			return c.Save()
		}
		return fmt.Errorf("error reading config file: %w", err)
	}

	// Parse JSON
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("error parsing config JSON: %w", err)
	}

	// Set defaults for any missing values
	c.setDefaults()

	// Apply environment variable overrides
	c.applyEnvOverrides()

	return nil
}

// applyEnvOverrides applies environment variable overrides with DECYPHARR_ prefix
// Environment variables use __ (double underscore) for nested fields and array indices
// Examples:
//
//	DECYPHARR_PORT=9090
//	DECYPHARR_DOWNLOAD_FOLDER=/downloads
//	DECYPHARR_DEBRIDS__0__NAME=realdebrid
//	DECYPHARR_DEBRIDS__0__API_KEY=abc123
func (c *Config) applyEnvOverrides() {
	// Helper to get env var with prefix
	getEnv := func(key string) string {
		return os.Getenv("DECYPHARR_" + key)
	}

	// Helper to parse boolean values
	parseBool := func(val string) bool {
		return val == "true" || val == "1" || val == "yes"
	}

	// Root level fields
	if val := getEnv("PORT"); val != "" {
		c.Port = val
	}
	if val := getEnv("BIND_ADDRESS"); val != "" {
		c.BindAddress = val
	}
	if val := getEnv("URL_BASE"); val != "" {
		c.URLBase = val
	}
	if val := getEnv("LOG_LEVEL"); val != "" {
		c.LogLevel = val
	}
	if val := getEnv("USE_AUTH"); val != "" {
		c.UseAuth = parseBool(val)
	}

	// Manager settings
	if val := getEnv("DOWNLOAD_FOLDER"); val != "" {
		c.DownloadFolder = val
	}
	if val := getEnv("REFRESH_INTERVAL"); val != "" {
		c.RefreshInterval = val
	}
	if val := getEnv("MAX_DOWNLOADS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			c.MaxDownloads = v
		}
	}
	if val := getEnv("SKIP_PRE_CACHE"); val != "" {
		c.SkipPreCache = parseBool(val)
	}
	if val := getEnv("ALWAYS_RM_TRACKER_URLS"); val != "" {
		c.AlwaysRmTrackerUrls = parseBool(val)
	}
	if val := getEnv("MIN_FILE_SIZE"); val != "" {
		c.MinFileSize = val
	}
	if val := getEnv("MAX_FILE_SIZE"); val != "" {
		c.MaxFileSize = val
	}
	if val := getEnv("DISCORD_WEBHOOK_URL"); val != "" {
		c.DiscordWebhook = val
	}
	if val := getEnv("CALLBACK_URL"); val != "" {
		c.CallbackURL = val
	}
	if val := getEnv("REMOVE_STALLED_AFTER"); val != "" {
		c.RemoveStalledAfter = val
	}
	if val := getEnv("ENABLE_WEBDAV_AUTH"); val != "" {
		c.EnableWebdavAuth = parseBool(val)
	}
	if val := getEnv("RETRIES"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			c.Retries = v
		}
	}

	if val := getEnv("SKIP_AUTO_MOVE"); val != "" {
		c.SkipAutoMove = parseBool(val)
	}
	// Manager categories array
	for i := 0; i < 100; i++ { // Support up to 100 categories
		key := fmt.Sprintf("CATEGORIES__%d", i)
		if val := getEnv(key); val != "" {
			if i >= len(c.Categories) {
				c.Categories = append(c.Categories, make([]string, i-len(c.Categories)+1)...)
			}
			c.Categories[i] = val
		} else {
			break
		}
	}
	// Manager allowed extensions array
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("ALLOWED_FILE_TYPES__%d", i)
		if val := getEnv(key); val != "" {
			if i >= len(c.AllowedExt) {
				c.AllowedExt = append(c.AllowedExt, make([]string, i-len(c.AllowedExt)+1)...)
			}
			c.AllowedExt[i] = val
		} else {
			break
		}
	}

	// Repair settings
	if val := getEnv("REPAIR__ENABLED"); val != "" {
		c.Repair.Enabled = parseBool(val)
	}
	if val := getEnv("REPAIR__INTERVAL"); val != "" {
		c.Repair.Interval = val
	}
	if val := getEnv("REPAIR__WORKERS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			c.Repair.Workers = v
		}
	}
	if val := getEnv("REPAIR__STRATEGY"); val != "" {
		c.Repair.Strategy = RepairStrategy(val)
	}
	if val := getEnv("REPAIR__AUTO_PROCESS"); val != "" {
		c.Repair.AutoProcess = parseBool(val)
	}

	// DFS settings
	if val := getEnv("MOUNT__DFS__CACHE_DIR"); val != "" {
		c.Mount.DFS.CacheDir = val
	}
	if val := getEnv("MOUNT__DFS__CHUNK_SIZE"); val != "" {
		c.Mount.DFS.ChunkSize = val
	}
	if val := getEnv("MOUNT__DFS__READ_AHEAD_SIZE"); val != "" {
		c.Mount.DFS.ReadAheadSize = val
	}
	if val := getEnv("MOUNT__DFS__MAX_CONCURRENT_READS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			c.Mount.DFS.MaxConcurrentReads = v
		}
	}
	if val := getEnv("MOUNT__DFS__CACHE_EXPIRY"); val != "" {
		c.Mount.DFS.CacheExpiry = val
	}
	if val := getEnv("MOUNT__DFS__DISK_CACHE_SIZE"); val != "" {
		c.Mount.DFS.DiskCacheSize = val
	}
	if val := getEnv("MOUNT__DFS__CACHE_CLEANUP_INTERVAL"); val != "" {
		c.Mount.DFS.CacheCleanupInterval = val
	}
	if val := getEnv("MOUNT__DFS__BUFFER_SIZE"); val != "" {
		c.Mount.DFS.BufferSize = val
	}
	if val := getEnv("MOUNT__DFS__DAEMON_TIMEOUT"); val != "" {
		c.Mount.DFS.DaemonTimeout = val
	}
	if val := getEnv("MOUNT__DFS__UID"); val != "" {
		if v, err := strconv.ParseUint(val, 10, 32); err == nil {
			c.Mount.DFS.UID = uint32(v)
		}
	}
	if val := getEnv("MOUNT__DFS__GID"); val != "" {
		if v, err := strconv.ParseUint(val, 10, 32); err == nil {
			c.Mount.DFS.GID = uint32(v)
		}
	}
	if val := getEnv("MOUNT__DFS__UMASK"); val != "" {
		c.Mount.DFS.Umask = val
	}
	if val := getEnv("MOUNT__DFS__ALLOW_OTHER"); val != "" {
		c.Mount.DFS.AllowOther = parseBool(val)
	}
	if val := getEnv("MOUNT__DFS__ATTR_TIMEOUT"); val != "" {
		c.Mount.DFS.AttrTimeout = val
	}
	if val := getEnv("MOUNT__DFS__ENTRY_TIMEOUT"); val != "" {
		c.Mount.DFS.EntryTimeout = val
	}
	if val := getEnv("MOUNT__DFS__NEGATIVE_TIMEOUT"); val != "" {
		c.Mount.DFS.NegativeTimeout = val
	}

	// Rclone settings
	if val := getEnv("RCLONE__RC_PORT"); val != "" {
		c.Mount.Rclone.Port = val
	}
	if val := getEnv("RCLONE__LOG_LEVEL"); val != "" {
		c.Mount.Rclone.LogLevel = val
	}
	if val := getEnv("RCLONE__VFS_CACHE_MODE"); val != "" {
		c.Mount.Rclone.VfsCacheMode = val
	}
	if val := getEnv("RCLONE__CACHE_DIR"); val != "" {
		c.Mount.Rclone.CacheDir = val
	}
	if val := getEnv("RCLONE__TRANSFERS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			c.Mount.Rclone.Transfers = v
		}
	}

	// Debrid providers array
	for i := 0; i < 10; i++ { // Support up to 10 debrid providers
		prefix := fmt.Sprintf("DEBRIDS__%d__", i)
		if val := getEnv(prefix + "NAME"); val != "" {
			// Ensure array is large enough
			if i >= len(c.Debrids) {
				c.Debrids = append(c.Debrids, make([]Debrid, i-len(c.Debrids)+1)...)
			}
			c.Debrids[i].Name = val

			// Set other debrid fields
			if apiKey := getEnv(prefix + "API_KEY"); apiKey != "" {
				c.Debrids[i].APIKey = apiKey
			}
			if folder := getEnv(prefix + "FOLDER"); folder != "" {
				c.Debrids[i].Folder = folder
			}
			if provider := getEnv(prefix + "PROVIDER"); provider != "" {
				c.Debrids[i].Provider = provider
			}
			if proxy := getEnv(prefix + "PROXY"); proxy != "" {
				c.Debrids[i].Proxy = proxy
			}
		}
	}

	// Arr applications array
	for i := 0; i < 20; i++ { // Support up to 20 arr applications
		prefix := fmt.Sprintf("ARRS__%d__", i)
		if val := getEnv(prefix + "NAME"); val != "" {
			// Ensure array is large enough
			if i >= len(c.Arrs) {
				c.Arrs = append(c.Arrs, make([]Arr, i-len(c.Arrs)+1)...)
			}
			c.Arrs[i].Name = val

			// Set other arr fields
			if host := getEnv(prefix + "HOST"); host != "" {
				c.Arrs[i].Host = host
			}
			if token := getEnv(prefix + "TOKEN"); token != "" {
				c.Arrs[i].Token = token
			}
			if cleanup := getEnv(prefix + "CLEANUP"); cleanup != "" {
				c.Arrs[i].Cleanup = parseBool(cleanup)
			}
		}
	}
}

func validateDebrids(debrids []Debrid) error {
	if len(debrids) == 0 {
		return errors.New("no debrids configured")
	}

	for _, debrid := range debrids {
		// Basic field validation
		if debrid.APIKey == "" {
			return errors.New("debrid api key is required")
		}
	}

	return nil
}

func validateRepair(config *Repair) error {
	if !config.Enabled {
		return nil
	}
	if config.Interval == "" {
		return errors.New("repair interval is required")
	}
	return nil
}

func ValidateConfig(config *Config) error {
	// Run validations concurrently

	if err := validateDebrids(config.Debrids); err != nil {
		return err
	}

	if config.DownloadFolder == "" {
		return errors.New("download folder is required")
	}

	if err := validateRepair(&config.Repair); err != nil {
		return err
	}

	return nil
}

// generateAPIToken creates a new random API token
func generateAPIToken() (string, error) {
	bytes := make([]byte, 32) // 256-bit token
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func SetConfigPath(path string) {
	configPath = path
}

func GetMainPath() string {
	return configPath
}

func Get() *Config {
	once.Do(func() {
		instance = &Config{} // Initialize instance first
		if err := instance.loadConfig(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "configuration Error: %v\n", err)
			os.Exit(1)
		}
	})
	return instance
}

func (c *Config) GetMinFileSize() int64 {
	// 0 means no limit
	if c.MinFileSize == "" {
		return 0
	}
	s, err := ParseSize(c.MinFileSize)
	if err != nil {
		return 0
	}
	return s
}

func (c *Config) GetMaxFileSize() int64 {
	// 0 means no limit
	if c.MaxFileSize == "" {
		return 0
	}
	s, err := ParseSize(c.MaxFileSize)
	if err != nil {
		return 0
	}
	return s
}

func (c *Config) IsSizeAllowed(size int64) bool {
	if size == 0 {
		return true // Maybe the debrid hasn't reported the size yet
	}
	if c.GetMinFileSize() > 0 && size < c.GetMinFileSize() {
		return false
	}
	if c.GetMaxFileSize() > 0 && size > c.GetMaxFileSize() {
		return false
	}
	return true
}

func (c *Config) SecretKey() string {
	return cmp.Or(os.Getenv("DECYPHARR_SECRET_KEY"), "\"wqj(v%lj*!-+kf@4&i95rhh_!5_px5qnuwqbr%cjrvrozz_r*(\"")
}

func (c *Config) GetAuth() *Auth {
	if !c.UseAuth {
		return nil
	}
	if c.Auth == nil {
		c.Auth = &Auth{}
		if _, err := os.Stat(c.AuthFile()); err == nil {
			file, err := os.ReadFile(c.AuthFile())
			if err == nil {
				_ = json.Unmarshal(file, c.Auth)
			}
		}
	}
	return c.Auth
}

func (c *Config) SaveAuth(auth *Auth) error {
	c.Auth = auth
	data, err := json.Marshal(auth)
	if err != nil {
		return err
	}
	return os.WriteFile(c.AuthFile(), data, 0644)
}

func (c *Config) CheckSetup() error {
	return ValidateConfig(c)
}

func (c *Config) NeedsAuth() bool {
	return c.UseAuth && (c.Auth == nil || c.Auth.Username == "" || c.Auth.Password == "")
}

func (c *Config) updateDebrid(d Debrid) Debrid {
	workers := runtime.NumCPU() * 50
	perDebrid := workers / len(c.Debrids)

	if d.Provider == "" {
		d.Provider = d.Name
	}

	var downloadKeys []string

	if len(d.DownloadAPIKeys) > 0 {
		downloadKeys = d.DownloadAPIKeys
	} else {
		// If no download API keys are specified, use the main API key
		downloadKeys = []string{d.APIKey}
	}
	d.DownloadAPIKeys = downloadKeys

	if d.TorrentsRefreshInterval == "" {
		d.TorrentsRefreshInterval = DefaultTorrentsRefreshInterval
	}
	if d.DownloadLinksRefreshInterval == "" {
		d.DownloadLinksRefreshInterval = DefaultDownloadsRefreshInterval
	}
	if d.Workers == 0 {
		d.Workers = perDebrid
	}
	if d.AutoExpireLinksAfter == "" {
		d.AutoExpireLinksAfter = DefaultAutoExpireLinksAfter
	}

	return d
}

// migrateQBitTorrentToManager migrates deprecated QBitTorrent config to Manager
// This ensures backward compatibility with existing configs
func (c *Config) migrateQBitTorrentToManager() {
	// If Manager fields are not set but QBitTorrent fields are, migrate them
	if c.DownloadFolder == "" && c.QBitTorrent.DownloadFolder != "" {
		c.DownloadFolder = c.QBitTorrent.DownloadFolder
	}

	if len(c.Categories) == 0 && len(c.QBitTorrent.Categories) > 0 {
		c.Categories = c.QBitTorrent.Categories
	}

	if c.RefreshInterval == "" && c.QBitTorrent.RefreshInterval > 0 {
		c.RefreshInterval = fmt.Sprintf("%ds", c.QBitTorrent.RefreshInterval)
	}

	if !c.SkipPreCache && c.QBitTorrent.SkipPreCache {
		c.SkipPreCache = c.QBitTorrent.SkipPreCache
	}

	if c.MaxDownloads == 0 && c.QBitTorrent.MaxDownloads > 0 {
		c.MaxDownloads = c.QBitTorrent.MaxDownloads
	}

	if !c.AlwaysRmTrackerUrls && c.QBitTorrent.AlwaysRmTrackerUrls {
		c.AlwaysRmTrackerUrls = c.QBitTorrent.AlwaysRmTrackerUrls
	}

	// Set default download folder if not set
	if c.DownloadFolder == "" {
		c.DownloadFolder = filepath.Join(GetMainPath(), "downloads")
	}

	// Set default categories if not set
	if len(c.Categories) == 0 {
		c.Categories = []string{"sonarr", "radarr"}
	}

	// Set default refresh interval if not set
	if c.RefreshInterval == "" {
		c.RefreshInterval = "30s"
	}
}

func (c *Config) setDefaults() {
	// Migrate deprecated fields to Manager (backward compatibility)
	c.migrateQBitTorrentToManager()

	if c.DefaultDownloadAction == "" {
		c.DefaultDownloadAction = DownloadActionSymlink
	}

	for i, debrid := range c.Debrids {
		c.Debrids[i] = c.updateDebrid(debrid)
	}

	firstDebrid := Debrid{}
	if len(c.Debrids) > 0 {
		firstDebrid = c.Debrids[0]
	}

	if c.Mount.Type == "" {
		if c.Rclone.Enabled {
			c.Mount.Type = MountTypeRclone
			c.Mount.MountPath = c.Rclone.MountPath
			c.Mount.Rclone = c.Rclone
		}
	}

	if c.Mount.MountPath == "" {
		// Set MountPath from debridConfig.Folder by splliting it
		// debrid.Folder is usually {mount_path}/{debrid_name}/__all__ or {mount_path}/{debrid_name}/torrents
		if len(c.Debrids) > 0 {
			c.Mount.MountPath = filepath.Dir(filepath.Dir(firstDebrid.Folder)) // GetReader parent of parent directory
		}
	}

	// Move WebDav global settings to Manager if not set
	if c.Mount.ExternalRclone.RCUrl == "" {
		c.Mount.ExternalRclone.RCUrl = firstDebrid.RcUrl
	}
	if c.Mount.ExternalRclone.RCUsername == "" {
		c.Mount.ExternalRclone.RCUsername = firstDebrid.RcUser
	}
	if c.Mount.ExternalRclone.RCPassword == "" {
		c.Mount.ExternalRclone.RCPassword = firstDebrid.RcPass
	}

	if c.FolderNaming == "" {
		c.FolderNaming = WebDavFolderNaming(firstDebrid.FolderNaming)
	}

	// Set default allowed extensions if not set in Manager
	if len(c.AllowedExt) == 0 {
		c.AllowedExt = getDefaultExtensions()
	}

	// Set default error threshold for multi-debrid switching
	if c.Retries == 0 {
		c.Retries = 3 // Default to 3 consecutive errors before switching
	}

	// Basic defaults
	if c.URLBase == "" {
		c.URLBase = "/"
	}
	// validate url base starts with /
	if !strings.HasPrefix(c.URLBase, "/") {
		c.URLBase = "/" + c.URLBase
	}
	if !strings.HasSuffix(c.URLBase, "/") {
		c.URLBase += "/"
	}

	if c.Port == "" {
		c.Port = DefaultPort
	}

	if c.LogLevel == "" {
		c.LogLevel = DefaultLogLevel
	}

	// Set repair defaults
	if c.Repair.Mode == "" {
		c.Repair.Mode = RepairModeArr
	}
	if c.Repair.Strategy == "" {
		c.Repair.Strategy = RepairStrategyPerTorrent
	}
	if c.Repair.Interval == "" {
		c.Repair.Interval = "1h"
	}
	if c.Repair.Workers == 0 {
		c.Repair.Workers = 5
	}

	// Rclone defaults
	if c.Mount.Type == MountTypeRclone {
		c.Mount.Rclone.Port = cmp.Or(c.Rclone.Port, DefaultRclonePort)
		if c.Mount.Rclone.AsyncRead == nil {
			_asyncTrue := true
			c.Mount.Rclone.AsyncRead = &_asyncTrue
		}
		c.Mount.Rclone.VfsCacheMode = cmp.Or(c.Mount.Rclone.VfsCacheMode, "off")
		if c.Mount.Rclone.UID == 0 {
			c.Mount.Rclone.UID = uint32(os.Getuid())
		}
		if c.Mount.Rclone.GID == 0 {
			if runtime.GOOS == "windows" {
				// On Windows, we use the current user's SID as GID
				c.Mount.Rclone.GID = uint32(os.Getuid()) // Windows does not have GID, using UID instead
			} else {
				c.Mount.Rclone.GID = uint32(os.Getgid())
			}
		}
		if c.Mount.Rclone.Transfers == 0 {
			c.Mount.Rclone.Transfers = 4 // Default number of transfers
		}
		if c.Mount.Rclone.VfsCacheMode != "off" {
			c.Mount.Rclone.VfsCachePollInterval = cmp.Or(c.Rclone.VfsCachePollInterval, "1m") // Clean cache every minute
		}
		c.Mount.Rclone.DirCacheTime = cmp.Or(c.Rclone.DirCacheTime, "5m")
		c.Mount.Rclone.LogLevel = cmp.Or(c.Rclone.LogLevel, strings.ToUpper(DefaultLogLevel))
	}

	// DFS defaults
	if c.Mount.Type == MountTypeDFS {
		if c.Mount.DFS.CacheDir == "" {
			c.Mount.DFS.CacheDir = filepath.Join(GetMainPath(), "fs", "cache")
		}
		if c.Mount.DFS.ChunkSize == "" {
			c.Mount.DFS.ChunkSize = DefaultDFSChunkSize
		}
		if c.Mount.DFS.ReadAheadSize == "" {
			c.Mount.DFS.ReadAheadSize = DefaultDFSReadAheadSize
		}
		if c.Mount.DFS.MaxConcurrentReads == 0 {
			c.Mount.DFS.MaxConcurrentReads = DefaultDFSMaxConcurrentRead
		}
		if c.Mount.DFS.CacheExpiry == "" {
			c.Mount.DFS.CacheExpiry = DefaultDFSCacheExpiry
		}
		if c.Mount.DFS.DiskCacheSize == "" {
			c.Mount.DFS.DiskCacheSize = DefaultDFSDiskCacheSize
		}
	}
	// Load the auth file
	c.Auth = c.GetAuth()

	// Generate API token if auth is enabled and no token exists
	if c.UseAuth {
		if c.Auth == nil {
			c.Auth = &Auth{}
		}
		if c.Auth.APIToken == "" {
			if token, err := generateAPIToken(); err == nil {
				c.Auth.APIToken = token
				// Save the updated auth config
				_ = c.SaveAuth(c.Auth)
			}
		}
	}

	// Set folder naming from first debrid if available
	if len(c.Debrids) > 0 && c.FolderNaming == "" {
		c.FolderNaming = WebDavFolderNaming(c.Debrids[0].FolderNaming)
	}
}

func (c *Config) Save() error {
	c.setDefaults()
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(c.JsonFile(), data, 0644); err != nil {
		fmt.Printf("Failed to write config file: %v\n", err)
		return err
	}
	return nil
}

func Reset() {
	once = sync.Once{}
	instance = nil
}

func (c *Config) createConfig() error {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(GetMainPath(), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}
	c.URLBase = "/"
	c.Port = DefaultPort
	c.LogLevel = DefaultLogLevel
	c.UseAuth = true
	return nil
}
