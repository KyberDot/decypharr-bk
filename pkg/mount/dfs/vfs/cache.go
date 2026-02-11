package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/singleflight"
)

const (
	metaFlushInterval = 2 * time.Second

	// How long to keep unused cache items around before removing(no delete on disk, just remove from map and close file. Cleanup loop will remove from disk eventually.
	itemIdleTimeout = 15 * time.Minute

	// cacheEvictThreshold is the percentage of max cache size at which eviction starts.
	cacheEvictThreshold = 0.90
)

// Cache manages sparse cache files for streaming
type Cache struct {
	config *config.FuseConfig
	logger zerolog.Logger

	items     *xsync.Map[string, *CacheItem]
	totalSize atomic.Int64
	itemCount atomic.Int64

	manager *manager.Manager

	ctx    context.Context
	cancel context.CancelFunc

	createGroup singleflight.Group
}

type candidateEntry struct {
	key        string
	path       string
	atime      time.Time
	mtime      time.Time
	cachedSize int64 // Actual bytes on disk (from ranges)
	opens      int32
	inMap      bool // Whether this item is loaded in the cache map
}

// NewCache creates a new sparse file cache
func NewCache(ctx context.Context, mgr *manager.Manager, config *config.FuseConfig) (*Cache, error) {
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache dir: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	c := &Cache{
		config:  config,
		logger:  logger.New("dfs"),
		items:   xsync.NewMap[string, *CacheItem](),
		manager: mgr,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Prime cache stats before serving requests so totalSize reflects existing files.
	if err := c.rebuildIndex(); err != nil {
		c.logger.Warn().Err(err).Msg("cache index rebuild failed")
	}
	go c.cleanupLoop()
	return c, nil
}

func (c *Cache) addBytes(delta int64) {
	if delta <= 0 {
		return
	}
	c.totalSize.Add(delta)
}

func (c *Cache) rebuildIndex() error {
	now := utils.Now()
	candidates, totalSize := c.scanDiskCandidates()

	maxSize := c.config.CacheDiskSize
	threshold := int64(0)
	if maxSize > 0 {
		threshold = int64(float64(maxSize) * cacheEvictThreshold)
		if threshold <= 0 {
			threshold = maxSize
		}
	}

	totalSize, _ = c.evictCandidates(now, candidates, totalSize, threshold)

	c.totalSize.Store(totalSize)
	return nil
}

// GetItem returns or creates a cache item for the given file
func (c *Cache) GetItem(entryName, filename string, fileSize int64) (*CacheItem, error) {
	key := buildCacheKey(entryName, filename)

	// Fast path: already exists
	if item, ok := c.items.Load(key); ok {
		item.touch()
		return item, nil
	}

	// Slow path: create with singleflight to avoid global lock
	val, err, _ := c.createGroup.Do(key, func() (interface{}, error) {
		if item, ok := c.items.Load(key); ok {
			item.touch()
			return item, nil
		}
		item, err := c.newItem(key, entryName, filename, fileSize)
		if err != nil {
			return nil, err
		}
		c.items.Store(key, item)
		c.itemCount.Add(1)
		return item, nil
	})
	if err != nil {
		return nil, err
	}
	item := val.(*CacheItem)
	item.touch()
	return item, nil
}

func (c *Cache) scanDiskCandidates() ([]candidateEntry, int64) {
	var candidates []candidateEntry
	var totalSize int64

	walkErr := filepath.WalkDir(c.config.CacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Don't log every error - could be a race with creation
			return nil
		}

		// Skip non-files and non-meta.json files
		if d.IsDir() {
			return nil
		}
		if d.Name() != "meta.json" {
			return nil
		}

		// Found a meta.json - the parent directory is the cache entry
		entryDir := filepath.Dir(path)

		// Derive cache key from path first
		key, err := filepath.Rel(c.config.CacheDir, entryDir)
		if err != nil {
			return nil
		}
		key = filepath.ToSlash(key)

		var opens int32
		var inMap bool
		if item, ok := c.items.Load(key); ok {
			opens = item.opens.Load()
			inMap = true
		}

		// Read and parse metadata
		var info ItemInfo
		metaData, metaErr := os.ReadFile(path)
		if metaErr != nil {
			// Only remove if not in the map - could be a race with creation
			if !inMap {
				_ = os.RemoveAll(entryDir)
			}
			return nil
		}

		if err := json.Unmarshal(metaData, &info); err != nil {
			// Only remove corrupt metadata if not actively in use
			if !inMap {
				c.logger.Warn().Err(err).Str("path", entryDir).Msg("corrupt cache metadata, removing entry")
				_ = os.RemoveAll(entryDir)
			}
			return nil
		}

		// Verify data file exists
		dataPath := filepath.Join(entryDir, "data")
		dataStat, err := os.Stat(dataPath)
		if err != nil {
			// Only remove if not in the map
			if !inMap {
				_ = os.RemoveAll(entryDir)
			}
			return nil
		}

		// Calculate actual cached bytes from downloaded ranges
		cachedSize := info.Rs.Size()
		if cachedSize <= 0 && info.Size > 0 {
			cachedSize = dataStat.Size()
		}

		// Set default times if missing
		atime := info.ATime
		mtime := info.ModTime
		if atime.IsZero() {
			atime = mtime
		}
		if mtime.IsZero() {
			mtime = dataStat.ModTime()
			if atime.IsZero() {
				atime = mtime
			}
		}

		candidate := candidateEntry{
			key:        key,
			path:       entryDir,
			atime:      atime,
			mtime:      mtime,
			cachedSize: cachedSize,
			opens:      opens,
			inMap:      inMap,
		}
		candidates = append(candidates, candidate)
		totalSize += cachedSize

		return nil
	})
	if walkErr != nil {
		c.logger.Warn().Err(walkErr).Msg("cache cleanup walk failed")
	}

	return candidates, totalSize
}

func (c *Cache) evictCandidates(now time.Time, candidates []candidateEntry, totalSize, threshold int64) (int64, int) {
	removed := make(map[string]struct{})
	removeCandidate := func(candidate candidateEntry) {
		if _, skip := removed[candidate.key]; skip {
			return
		}
		// Never remove items that are in the map or have open handles
		if candidate.inMap || candidate.opens > 0 {
			return
		}
		if err := os.RemoveAll(candidate.path); err != nil {
			c.logger.Warn().Err(err).Str("path", candidate.path).Msg("failed to remove cache entry")
		}
		removed[candidate.key] = struct{}{}
	}

	// Phase 1: Remove expired entries (only if not in map)
	if c.config.CacheExpiry > 0 {
		for _, candidate := range candidates {
			if !candidate.inMap && candidate.opens == 0 && now.Sub(candidate.atime) > c.config.CacheExpiry {
				removeCandidate(candidate)
				totalSize -= candidate.cachedSize
			}
		}
	}

	// Phase 2: If still over threshold, remove oldest entries (only if not in map)
	if threshold > 0 && totalSize > threshold {
		// Sort by access time, then modification time (oldest first)
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].atime.Equal(candidates[j].atime) {
				return candidates[i].mtime.Before(candidates[j].mtime)
			}
			return candidates[i].atime.Before(candidates[j].atime)
		})

		for _, candidate := range candidates {
			if totalSize <= threshold {
				break
			}
			if candidate.inMap || candidate.opens > 0 {
				continue
			}
			if _, skip := removed[candidate.key]; skip {
				continue
			}
			removeCandidate(candidate)
			totalSize -= candidate.cachedSize
		}
	}

	return totalSize, len(removed)
}

// newItem creates a new cache item
func (c *Cache) newItem(key, entryName, filename string, fileSize int64) (*CacheItem, error) {
	// Create directory structure
	itemDir := filepath.Join(c.config.CacheDir, key)
	if err := os.MkdirAll(itemDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create item dir: %w", err)
	}

	cachePath := filepath.Join(itemDir, "data")
	metaPath := filepath.Join(itemDir, "meta.json")

	// Try to load existing metadata
	var info ItemInfo
	if data, err := os.ReadFile(metaPath); err == nil {
		if err := json.Unmarshal(data, &info); err != nil {
			c.logger.Warn().Err(err).Str("key", key).Msg("corrupt metadata, resetting")
			info = ItemInfo{}
		}
	}

	// Open or create sparse file
	fd, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache file: %w", err)
	}

	// Make it sparse and set size
	if err := setSparse(fd); err != nil {
		c.logger.Warn().Err(err).Msg("failed to set sparse (may not be supported)")
	}

	if err := fd.Truncate(fileSize); err != nil {
		fd.Close()
		return nil, fmt.Errorf("failed to truncate cache file: %w", err)
	}

	// Update info
	info.Size = fileSize
	info.ModTime = utils.Now()
	info.ATime = utils.Now()
	_logger := c.logger.With().Str("entry", entryName).Str("filename", filename).Logger()
	log := logger.NewRateLimitedLogger(logger.WithLogger(_logger))
	entry, err := c.manager.GetEntryByName(entryName, filename)
	if err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("failed to get storage entry: %w", err)
	}

	item := &CacheItem{
		cache:    c,
		key:      key,
		entry:    entry,
		filename: filename,
		file:     fd,
		metaPath: metaPath,
		info:     info,
		logger:   log.Rate(buildCacheKey(entryName, filename)),
	}

	// Create downloaders coordinator
	item.downloaders = NewDownloaders(c.ctx, c.manager, item, c.config)
	item.startMetaWriter()
	item.markMetadataDirty()

	return item, nil
}

// cleanupLoop runs periodic cleanup
func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(c.config.CacheCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.ctx.Done():
			return
		}
	}
}

// cleanup removes old and excess cache items
func (c *Cache) cleanup() {
	now := utils.Now()

	var evicted []string
	c.items.Range(func(key string, item *CacheItem) bool {
		if item.opens.Load() > 0 {
			return true // Still open, keep in map
		}

		item.metaMu.RLock()
		lastAccess := item.info.ATime
		item.metaMu.RUnlock()

		if now.Sub(lastAccess) > itemIdleTimeout {
			evicted = append(evicted, key)
		}
		return true
	})

	// Actually evict the items (outside the Range to avoid concurrent modification)
	for _, key := range evicted {
		if item, ok := c.items.LoadAndDelete(key); ok {
			item.Close()
			c.itemCount.Add(-1)
		}
	}

	maxSize := c.config.CacheDiskSize
	threshold := int64(float64(maxSize) * cacheEvictThreshold)
	if threshold <= 0 {
		threshold = maxSize
	}

	// If cache expiry is disabled and we're under threshold, skip disk scan.
	if c.config.CacheExpiry <= 0 && (maxSize <= 0 || c.totalSize.Load() <= threshold) {
		return
	}

	oldSize := c.totalSize.Load()
	candidates, totalSize := c.scanDiskCandidates()

	totalSize, removedCount := c.evictCandidates(now, candidates, totalSize, threshold)

	if removedCount > 0 && oldSize > totalSize {
		c.logger.Trace().Msgf("cache cleanup removed %d entries, freed %s (total size: %s)", removedCount, utils.FormatSize(oldSize-totalSize), utils.FormatSize(totalSize))
	}

	c.totalSize.Store(totalSize)
}

// Close shuts down the cache
func (c *Cache) Close() error {
	c.cancel()

	c.items.Range(func(key string, item *CacheItem) bool {
		item.Close()
		return true
	})
	c.items.Clear()
	c.itemCount.Store(0)

	return nil
}

// GetStats returns cache statistics
func (c *Cache) GetStats() map[string]interface{} {
	maxSize := c.config.CacheDiskSize
	utilization := 0.0
	if maxSize > 0 {
		utilization = float64(c.totalSize.Load()) / float64(maxSize)
	}

	return map[string]interface{}{
		"type":        "vfs",
		"total_size":  c.totalSize.Load(),
		"max_size":    c.config.CacheDiskSize,
		"item_count":  c.itemCount.Load(),
		"utilization": utilization,
	}
}

// CacheItem represents a single cached file
type CacheItem struct {
	cache    *Cache
	key      string
	entry    *storage.Entry
	filename string

	file     *os.File
	metaPath string

	info ItemInfo

	opens       atomic.Int32 // Number of open handles (prevents eviction)
	logger      *logger.RateLimitedEvent
	downloaders *Downloaders // Download coordinator

	metaMu sync.RWMutex
	fileMu sync.RWMutex
	dlMu   sync.Mutex

	metaDirty   atomic.Bool
	metaFlushCh chan struct{}
	metaStopCh  chan struct{}
	metaWG      sync.WaitGroup
}

func (item *CacheItem) startMetaWriter() {
	item.metaFlushCh = make(chan struct{}, 1)
	item.metaStopCh = make(chan struct{})
	item.metaWG.Add(1)
	go item.metaWriterLoop()
}

func (item *CacheItem) stopMetaWriter() {
	if item.metaStopCh == nil {
		return
	}
	close(item.metaStopCh)
	item.metaWG.Wait()
	item.metaStopCh = nil
	item.metaFlushCh = nil
}

func (item *CacheItem) metaWriterLoop() {
	defer item.metaWG.Done()
	ticker := time.NewTicker(metaFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			item.flushMetadata(false)
		case <-item.metaFlushCh:
			item.flushMetadata(false)
		case <-item.metaStopCh:
			item.flushMetadata(true)
			return
		}
	}
}

func (item *CacheItem) markMetadataDirty() {
	item.metaDirty.Store(true)
	if ch := item.metaFlushCh; ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (item *CacheItem) flushMetadata(force bool) {
	if !force && !item.metaDirty.Load() {
		return
	}
	item.metaMu.RLock()
	info := item.info
	if len(info.Rs) > 0 {
		rsCopy := make(ranges.Ranges, len(info.Rs))
		copy(rsCopy, info.Rs)
		info.Rs = rsCopy
	}
	item.metaMu.RUnlock()

	data, err := json.Marshal(info)
	if err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to marshal cache metadata")
		return
	}
	// Confirm directory exists before writing metadata (in case it was deleted by cleanup)
	if err := os.MkdirAll(filepath.Dir(item.metaPath), 0755); err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to create cache directory for metadata")
		return
	}
	if err := os.WriteFile(item.metaPath, data, 0644); err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to write cache metadata")
		return
	}
	item.metaDirty.Store(false)
}

// ItemInfo is persisted to disk
type ItemInfo struct {
	Size    int64         `json:"size"`
	Rs      ranges.Ranges `json:"ranges"` // Downloaded regions
	ModTime time.Time     `json:"mod_time"`
	ATime   time.Time     `json:"atime"`
}

// touch updates access time
func (item *CacheItem) touch() {
	item.metaMu.Lock()
	item.info.ATime = utils.Now()
	item.metaMu.Unlock()
	item.markMetadataDirty()
}

// Open increments the open count (prevents eviction)
func (item *CacheItem) Open() {
	item.opens.Add(1)
	item.touch()
}

// Release decrements the open count
func (item *CacheItem) Release() {
	newCount := item.opens.Add(-1)
	if newCount < 0 {
		item.opens.Store(0)
	}
}

// StopDownloaders stops active downloads but keeps the cache item alive
// for potential cache reuse. This is called when all file handles are closed.
func (item *CacheItem) StopDownloaders() {
	item.dlMu.Lock()
	dls := item.downloaders
	item.dlMu.Unlock()

	if dls != nil {
		dls.StopAll()
	}
}

// ReadAt reads from the sparse file, downloading if needed
func (item *CacheItem) ReadAt(p []byte, off int64) (int, error) {
	if off >= item.info.Size {
		return 0, io.EOF
	}

	// Clamp read size
	readSize := int64(len(p))
	if off+readSize > item.info.Size {
		readSize = item.info.Size - off
		p = p[:readSize]
	}

	r := ranges.Range{Pos: off, Size: readSize}

	// Ensure data is on disk (may block)
	item.dlMu.Lock()
	dls := item.downloaders
	item.dlMu.Unlock()
	if dls == nil {
		return 0, errors.New("downloaders closed")
	}
	if err := dls.Download(r); err != nil {
		return 0, fmt.Errorf("download failed: %w", err)
	}

	// Read from sparse file
	item.fileMu.RLock()
	if item.file == nil {
		item.fileMu.RUnlock()
		return 0, errors.New("cache file closed")
	}
	f := item.file
	n, err := f.ReadAt(p, off)
	item.fileMu.RUnlock()
	if n > 0 {
		dropFileCache(f, off, int64(n))
	}
	return n, err
}

// WriteAtNoOverwrite writes only bytes not already present
func (item *CacheItem) WriteAtNoOverwrite(p []byte, off int64) (n, skipped int, err error) {
	writeRange := ranges.Range{Pos: off, Size: int64(len(p))}
	n = len(p)
	skipped = 0

	// Find all present/absent regions
	item.metaMu.RLock()
	rsSnapshot := append(ranges.Ranges(nil), item.info.Rs...)
	item.metaMu.RUnlock()
	frs := rsSnapshot.FindAll(writeRange)

	item.fileMu.Lock()
	if item.file == nil {
		item.fileMu.Unlock()
		return n, skipped, errors.New("cache file closed")
	}
	f := item.file
	for _, fr := range frs {
		if fr.Present {
			// Skip - already on disk
			skipped += int(fr.R.Size)
			continue
		}
		// Write missing part
		localOff := fr.R.Pos - off
		_, err = f.WriteAt(p[localOff:localOff+fr.R.Size], fr.R.Pos)
		if err != nil {
			item.fileMu.Unlock()
			return n, skipped, err
		}
	}
	item.fileMu.Unlock()

	// Mark range as present
	item.metaMu.Lock()
	before := item.info.Rs.Size()
	item.info.Rs.Insert(writeRange)
	after := item.info.Rs.Size()
	item.metaMu.Unlock()
	item.markMetadataDirty()

	if delta := after - before; delta > 0 {
		item.cache.addBytes(delta)
	}
	return n, skipped, nil
}

// HasRange returns true if entire range is on disk
func (item *CacheItem) HasRange(r ranges.Range) bool {
	item.metaMu.RLock()
	defer item.metaMu.RUnlock()
	return item.info.Rs.Present(r)
}

// FindMissing returns portion of r not yet downloaded
func (item *CacheItem) FindMissing(r ranges.Range) ranges.Range {
	item.metaMu.RLock()
	defer item.metaMu.RUnlock()

	// Clip to file size
	if r.End() > item.info.Size {
		r.Size = item.info.Size - r.Pos
	}
	if r.Size <= 0 {
		return ranges.Range{}
	}
	return item.info.Rs.FindMissing(r)
}

// Close closes the cache item and saves metadata
func (item *CacheItem) Close() error {
	// Stop downloaders without holding the downloaders lock to avoid deadlocks.
	item.dlMu.Lock()
	dls := item.downloaders
	item.downloaders = nil
	item.dlMu.Unlock()

	if dls != nil {
		dls.Close(nil)
	}

	item.stopMetaWriter()
	item.flushMetadata(true)

	item.fileMu.Lock()
	if item.file != nil {
		item.file.Close()
		item.file = nil
	}
	item.fileMu.Unlock()
	return nil
}

// Helper functions

func buildCacheKey(entryName, filename string) string {
	// Create safe filesystem key
	return fmt.Sprintf("%s/%s", entryName, filename)
}

// setSparse attempts to make a file sparse (platform-specific)
func setSparse(f *os.File) error {
	// On Unix, files are sparse by default when using Truncate
	// On Windows, we'd need to use DeviceIoControl with FSCTL_SET_SPARSE
	// For now, just return nil - sparse behavior is automatic on most systems
	return nil
}
