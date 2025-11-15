package manager

import (
	"strings"

	"github.com/puzpuzpuz/xsync/v4"
)

const torrentEntryCachePrefix = "torrent::"

func (m *Manager) initEntryCache() {
	m.entry = NewEntryCache(m)
}

type EntryCacheItem struct {
	current  *FileInfo
	children []FileInfo
	refresh  bool
}

type EntryCache struct {
	manager *Manager
	entries *xsync.Map[string, EntryCacheItem] // key is entry name e.g __all__, __bad__, custom folder names, torrent::torrent_folder
}

func NewEntryCache(manager *Manager) *EntryCache {
	return &EntryCache{
		manager: manager,
		entries: xsync.NewMap[string, EntryCacheItem](),
	}
}

func (e *EntryCache) Get(name string) (*FileInfo, []FileInfo) {
	item, ok := e.entries.Load(name)
	if !ok {
		item = e.refreshEntry(name)
	}
	return item.current, item.children
}

func (e *EntryCache) refreshEntry(name string) EntryCacheItem {
	if strings.HasPrefix(name, torrentEntryCachePrefix) {
		// This is a torrent folder
		torrentName := strings.TrimPrefix(name, torrentEntryCachePrefix)
		current, children := e.manager.getTorrentChildren(torrentName)
		item := EntryCacheItem{
			current:  current,
			children: children,
		}
		e.entries.Store(name, item)
		return item
	} else {
		// This is either a __all__, __bad__ or custom folder
		current, children := e.manager.getEntryChildren(name)
		item := EntryCacheItem{
			current:  current,
			children: children,
		}
		e.entries.Store(name, item)
		return item
	}
}

func (e *EntryCache) Refresh() {
	items := e.manager.GetEntries()
	for _, item := range items {
		e.refreshEntry(item.Name())
	}
}
