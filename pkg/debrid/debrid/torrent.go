package debrid

import (
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type torrentCache struct {
	mu         sync.RWMutex
	byID       map[string]string
	byName     map[string]*CachedTorrent
	listing    atomic.Value
	sortNeeded bool
}

func newTorrentCache() *torrentCache {
	tc := &torrentCache{
		byID:       make(map[string]string),
		byName:     make(map[string]*CachedTorrent),
		sortNeeded: false,
	}
	tc.listing.Store(make([]os.FileInfo, 0))
	return tc
}

func (tc *torrentCache) getByID(id string) (*CachedTorrent, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	torrent, exists := tc.byID[id]
	if !exists {
		return nil, false
	}
	t, ok := tc.byName[torrent]
	return t, ok
}

func (tc *torrentCache) getByIDName(id string) (string, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	name, exists := tc.byID[id]
	return name, exists
}

func (tc *torrentCache) getByName(name string) (*CachedTorrent, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	torrent, exists := tc.byName[name]
	return torrent, exists
}

func (tc *torrentCache) set(id, name string, torrent *CachedTorrent) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.byID[id] = name
	tc.byName[name] = torrent
	tc.sortNeeded = true
}

func (tc *torrentCache) getListing() []os.FileInfo {
	// Fast path: if we have a sorted list and no changes since last sort
	if !tc.sortNeeded {
		return tc.listing.Load().([]os.FileInfo)
	}

	// Slow path: need to sort
	return tc.refreshListing()
}

func (tc *torrentCache) refreshListing() []os.FileInfo {
	tc.mu.Lock()
	size := len(tc.byName)
	tc.mu.Unlock()
	if size == 0 {
		var empty []os.FileInfo
		tc.listing.Store(empty)
		tc.sortNeeded = false
		return empty
	}

	// Create sortable entries
	type sortableFile struct {
		name    string
		modTime time.Time
	}

	tc.mu.Lock()
	sortables := make([]sortableFile, 0, len(tc.byName))

	for name, torrent := range tc.byName {
		sortables = append(sortables, sortableFile{
			name:    name,
			modTime: torrent.AddedOn,
		})
	}
	tc.mu.Unlock()

	// Sort by name
	sort.Slice(sortables, func(i, j int) bool {
		return sortables[i].name < sortables[j].name
	})

	// Create fileInfo objects
	files := make([]os.FileInfo, 0, len(sortables))
	for _, sf := range sortables {
		files = append(files, &fileInfo{
			name:    sf.name,
			size:    0,
			mode:    0755 | os.ModeDir,
			modTime: sf.modTime,
			isDir:   true,
		})
	}

	tc.listing.Store(files)
	tc.sortNeeded = false
	return files
}

func (tc *torrentCache) getAll() map[string]*CachedTorrent {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	result := make(map[string]*CachedTorrent)
	for name, torrent := range tc.byName {
		result[name] = torrent
	}
	return result
}

func (tc *torrentCache) getAllIDs() []string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	ids := make([]string, 0, len(tc.byID))
	for id := range tc.byID {
		ids = append(ids, id)
	}
	return ids
}

func (tc *torrentCache) getIdMaps() map[string]string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	res := make(map[string]string)
	for id, name := range tc.byID {
		res[id] = name
	}
	return res
}

func (tc *torrentCache) removeId(id string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.byID, id)
	tc.sortNeeded = true
}

func (tc *torrentCache) remove(name string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.byName, name)
	tc.sortNeeded = true
}
