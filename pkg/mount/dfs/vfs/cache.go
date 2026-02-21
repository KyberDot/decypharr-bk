package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	itemIdleTimeout = 1 * time.Minute

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
	threshold   int64

	// Stat counters (owned by VFS Manager, set after construction)
	bytesReadCounter *atomic.Int64
	errorsCounter    *atomic.Int64
}

type candidateEntry struct {
	key        string
	path       string // entry directory (for evict of empty dirs)
	dataPath   string // path to data file
	metaPath   string // path to metadata .json file
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

	maxSize := config.CacheDiskSize
	threshold := int64(0)
	if maxSize > 0 {
		threshold = int64(float64(maxSize) * cacheEvictThreshold)
		if threshold <= 0 {
			threshold = maxSize
		}
	}
	c := &Cache{
		config:    config,
		logger:    logger.New("dfs"),
		items:     xsync.NewMap[string, *CacheItem](),
		manager:   mgr,
		ctx:       ctx,
		cancel:    cancel,
		threshold: threshold,
	}
	go c.evictLoop()
	go c.cleanupLoop()
	return c, nil
}

// GetItem returns or creates a cache item for the given file.
// Note: ATime is NOT updated here — it's only updated on Open() to avoid
// stat/getattr calls keeping items alive and defeating idle evict.
func (c *Cache) GetItem(entryName, filename string, fileSize int64) (*CacheItem, error) {
	key := buildCacheKey(entryName, filename)

	// Fast path: already exists
	if item, ok := c.items.Load(key); ok {
		return item, nil
	}

	// Slow path: create with singleflight to avoid global lock
	val, err, _ := c.createGroup.Do(key, func() (interface{}, error) {
		if item, ok := c.items.Load(key); ok {
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
	return val.(*CacheItem), nil
}

func (c *Cache) scanDiskCandidates() ([]candidateEntry, int64) {
	var candidates []candidateEntry
	var totalSize int64

	topEntries, err := os.ReadDir(c.config.CacheDir)
	if err != nil {
		c.logger.Warn().Err(err).Msg("failed to read cache directory")
		return candidates, totalSize
	}

	for _, topEntry := range topEntries {
		if !topEntry.IsDir() {
			continue
		}

		entryName := topEntry.Name()
		entryDir := filepath.Join(c.config.CacheDir, entryName)

		subEntries, err := os.ReadDir(entryDir)
		if err != nil {
			continue
		}

		// Remove empty directories
		if len(subEntries) == 0 {
			_ = os.RemoveAll(entryDir)
			continue
		}

		// Find data/meta pairs by .json suffix
		for _, sub := range subEntries {
			if sub.IsDir() || !strings.HasSuffix(sub.Name(), ".json") {
				continue
			}

			// Derive the data filename from the meta filename
			filename := strings.TrimSuffix(sub.Name(), ".json")
			metaPath := filepath.Join(entryDir, sub.Name())
			dataPath := filepath.Join(entryDir, filename)
			key := buildCacheKey(entryName, filename)

			var opens int32
			var inMap bool
			if item, ok := c.items.Load(key); ok {
				opens = item.opens.Load()
				inMap = true
			}

			// Read and parse metadata
			var info ItemInfo
			metaData, metaErr := os.ReadFile(metaPath)
			if metaErr != nil {
				c.logger.Warn().Err(metaErr).Str("path", metaPath).Msg("failed to read cache metadata")
				continue
			}

			if err := json.Unmarshal(metaData, &info); err != nil {
				c.logger.Warn().Err(err).Str("path", metaPath).Msg("corrupt cache metadata")
				continue
			}

			// Verify data file exists
			dataStat, err := os.Stat(dataPath)
			if err != nil {
				c.logger.Warn().Err(err).Str("path", dataPath).Msg("cache data file missing")
				continue
			}

			cachedSize := info.Rs.Size()

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
			candidates = append(candidates, candidateEntry{
				key:        key,
				path:       entryDir,
				dataPath:   dataPath,
				metaPath:   metaPath,
				atime:      atime,
				mtime:      mtime,
				cachedSize: cachedSize,
				opens:      opens,
				inMap:      inMap,
			})
			totalSize += cachedSize
		}
	}

	return candidates, totalSize
}

func (c *Cache) evictCandidates(now time.Time, candidates []candidateEntry, totalSize int64, thresholdOverride int64) (int64, int) {
	threshold := c.threshold
	if thresholdOverride > 0 {
		threshold = thresholdOverride
	}

	removed := make(map[string]struct{})
	removeCandidate := func(candidate candidateEntry) {
		if _, skip := removed[candidate.key]; skip {
			return
		}
		// Never remove items that are in the map or have open handles
		if candidate.inMap || candidate.opens > 0 {
			return
		}
		// Remove only the specific data + meta files, not the entire entry directory
		if candidate.dataPath != "" {
			if err := os.Remove(candidate.dataPath); err != nil && !os.IsNotExist(err) {
				c.logger.Warn().Err(err).Str("path", candidate.dataPath).Msg("failed to remove cache data file")
			}
		}
		if candidate.metaPath != "" {
			if err := os.Remove(candidate.metaPath); err != nil && !os.IsNotExist(err) {
				c.logger.Warn().Err(err).Str("path", candidate.metaPath).Msg("failed to remove cache meta file")
			}
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
	itemDir := filepath.Join(c.config.CacheDir, entryName)
	if err := os.MkdirAll(itemDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create item dir: %w", err)
	}

	cachePath := filepath.Join(itemDir, filename)
	metaPath := filepath.Join(itemDir, filename+".json")

	// Try to load existing metadata
	var info ItemInfo
	if data, err := os.ReadFile(metaPath); err == nil {
		if err := json.Unmarshal(data, &info); err != nil {
			c.logger.Warn().Err(err).Str("key", key).Msg("corrupt metadata, resetting")
			info = ItemInfo{}
		}
	}

	// if cachePath is a directory, remove it to avoid conflicts with file creation
	if stat, err := os.Stat(cachePath); err == nil && stat.IsDir() {
		if err := os.RemoveAll(cachePath); err != nil {
			return nil, fmt.Errorf("failed to remove directory at cache path: %w", err)
		}
	}

	// Open or create sparse file
	fd, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache file: %w", err)
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
		metaPath: metaPath,
		info:     info,
		logger:   log.Rate(buildCacheKey(entryName, filename)),
	}
	item.file.Store(fd)
	item.atime.Store(info.ATime.UnixNano())

	// Initialize copy-on-write ranges from loaded metadata
	rs := make(ranges.Ranges, len(info.Rs))
	copy(rs, info.Rs)
	item.ranges.Store(&rs)

	// Create downloaders coordinator
	item.downloaders = NewDownloaders(c.ctx, c.manager, item, c.config, c.bytesReadCounter, c.errorsCounter)
	item.startMetaWriter()
	item.markMetadataDirty()

	return item, nil
}

// evictLoop runs periodic evict
func (c *Cache) evictLoop() {
	ticker := time.NewTicker(c.config.CacheCleanupInterval)
	defer ticker.Stop()

	// Run evict immediately on startup to remove stale items before they can be accessed
	c.evict()

	for {
		select {
		case <-ticker.C:
			c.evict()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(itemIdleTimeout / 2)
	defer ticker.Stop()

	c.cleanupItems(itemIdleTimeout)

	for {
		select {
		case <-ticker.C:
			c.cleanupItems(itemIdleTimeout)
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cache) cleanupItems(idleThreshold time.Duration) {
	now := utils.Now()

	var evicted []string
	c.items.Range(func(key string, item *CacheItem) bool {
		if item.opens.Load() > 0 {
			return true // Still open, keep in map
		}
		lastAccess := time.Unix(0, item.atime.Load())
		if now.Sub(lastAccess) > idleThreshold {
			evicted = append(evicted, key)
		}
		return true
	})

	// Actually evict the items (outside the Range to avoid concurrent modification)
	for _, key := range evicted {
		if item, ok := c.items.LoadAndDelete(key); ok {
			_ = item.Close()
			c.itemCount.Add(-1)
		}
	}
}

// evict removes old and excess cache items
func (c *Cache) evict() {
	now := utils.Now()

	oldSize := c.totalSize.Load()
	candidates, totalSize := c.scanDiskCandidates()

	// If cache expiry is disabled and we're under threshold, skip disk scan.
	if c.config.CacheExpiry <= 0 && (c.threshold <= 0 || totalSize <= c.threshold) {
		return
	}

	totalSize, removedCount := c.evictCandidates(now, candidates, totalSize, 0)

	if removedCount > 0 && oldSize > totalSize {
		c.logger.Trace().Msgf("cache evict removed %d entries, freed %s (total size: %s)", removedCount, utils.FormatSize(oldSize-totalSize), utils.FormatSize(totalSize))
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

// Stats returns cache statistics.
func (c *Cache) Stats() manager.CacheDetail {
	maxSize := c.config.CacheDiskSize
	utilization := 0.0
	if maxSize > 0 {
		utilization = float64(c.totalSize.Load()) / float64(maxSize)
	}

	return manager.CacheDetail{
		TotalSize:   c.totalSize.Load(),
		MaxSize:     c.config.CacheDiskSize,
		ItemCount:   c.itemCount.Load(),
		Utilization: utilization,
	}
}

// CacheItem represents a single cached file
type CacheItem struct {
	cache    *Cache
	key      string
	entry    *storage.Entry
	filename string

	file     atomic.Pointer[os.File] // Lock-free file access (pread/pwrite are thread-safe)
	metaPath string

	info ItemInfo

	// Lock-free access time tracking — avoids metaMu + disk flush on every Open()
	atime atomic.Int64 // Unix nano timestamp, synced to info.ATime on flush

	// Copy-on-write ranges — reads are lock-free via atomic load
	ranges atomic.Pointer[ranges.Ranges] // Current downloaded ranges

	opens       atomic.Int32 // Number of open handles (prevents eviction)
	logger      *logger.RateLimitedEvent
	downloaders *Downloaders // Download coordinator

	metaMu   sync.RWMutex // Protects info (except ATime and Rs which use atomics)
	rangesMu sync.Mutex   // Serializes range mutations (copy-on-write)
	dlMu     sync.Mutex

	metaDirty   atomic.Bool
	metaFlushCh chan struct{}
	metaStopCh  chan struct{}
	metaWG      sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
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
	item.metaMu.RUnlock()

	// Sync atomic ATime and copy-on-write ranges into the snapshot
	info.ATime = time.Unix(0, item.atime.Load())
	if rs := item.ranges.Load(); rs != nil && len(*rs) > 0 {
		rsCopy := make(ranges.Ranges, len(*rs))
		copy(rsCopy, *rs)
		info.Rs = rsCopy
	}

	data, err := json.Marshal(info)
	if err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to marshal cache metadata")
		return
	}
	// Confirm directory exists before writing metadata (in case it was deleted by evict)
	if err := os.MkdirAll(filepath.Dir(item.metaPath), 0755); err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to create cache directory for metadata")
		return
	}
	// Atomic write: write to temp file then rename to avoid corrupt reads
	// from scanDiskCandidates racing with this write.
	tmpPath := item.metaPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to write cache metadata")
		return
	}
	if err := os.Rename(tmpPath, item.metaPath); err != nil {
		item.cache.logger.Warn().Err(err).Str("key", item.key).Msg("failed to rename cache metadata")
		_ = os.Remove(tmpPath)
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

// touch updates access time (lock-free, no disk flush)
func (item *CacheItem) touch() {
	item.atime.Store(utils.Now().UnixNano())
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

	// Read from sparse file (lock-free — pread is thread-safe)
	f := item.file.Load()
	if f == nil {
		return 0, errors.New("cache file closed")
	}
	n, err := f.ReadAt(p, off)
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

	// Find all present/absent regions (lock-free read of copy-on-write ranges)
	rs := item.ranges.Load()
	if rs == nil {
		return n, skipped, errors.New("cache closed")
	}
	frs := rs.FindAll(writeRange)

	// Write to sparse file (lock-free — pwrite is thread-safe, downloaders own non-overlapping offsets)
	f := item.file.Load()
	if f == nil {
		return n, skipped, errors.New("cache file closed")
	}
	for _, fr := range frs {
		if fr.Present {
			skipped += int(fr.R.Size)
			continue
		}
		localOff := fr.R.Pos - off
		_, err = f.WriteAt(p[localOff:localOff+fr.R.Size], fr.R.Pos)
		if err != nil {
			return n, skipped, err
		}
	}

	// Copy-on-write range update: serialize mutations, publish atomically
	item.rangesMu.Lock()
	current := item.ranges.Load()
	updated := make(ranges.Ranges, len(*current))
	copy(updated, *current)
	updated.Insert(writeRange)
	item.ranges.Store(&updated)
	item.rangesMu.Unlock()

	item.markMetadataDirty()
	return n, skipped, nil
}

// HasRange returns true if entire range is on disk (lock-free)
func (item *CacheItem) HasRange(r ranges.Range) bool {
	rs := item.ranges.Load()
	if rs == nil {
		return false
	}
	return rs.Present(r)
}

// FindMissing returns portion of r not yet downloaded (lock-free)
func (item *CacheItem) FindMissing(r ranges.Range) ranges.Range {
	// Clip to file size
	if r.End() > item.info.Size {
		r.Size = item.info.Size - r.Pos
	}
	if r.Size <= 0 {
		return ranges.Range{}
	}
	rs := item.ranges.Load()
	if rs == nil {
		return r
	}
	return rs.FindMissing(r)
}

// Close closes the cache item and saves metadata
func (item *CacheItem) Close() error {
	item.closeOnce.Do(func() {
		// Stop downloaders without holding the downloaders lock to avoid deadlocks.
		item.dlMu.Lock()
		dls := item.downloaders
		item.downloaders = nil
		item.dlMu.Unlock()

		if dls != nil {
			if err := dls.Close(nil); err != nil && item.closeErr == nil {
				item.closeErr = err
			}
		}

		item.stopMetaWriter()
		item.flushMetadata(true)

		// Atomically swap file to nil, then close the old fd
		if f := item.file.Swap(nil); f != nil {
			if err := f.Close(); err != nil && item.closeErr == nil {
				item.closeErr = err
			}
		}
	})
	return item.closeErr
}

// Helper functions

func buildCacheKey(entryName, filename string) string {
	// Create safe filesystem key
	return fmt.Sprintf("%s/%s", entryName, filename)
}
