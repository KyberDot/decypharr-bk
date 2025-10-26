package dfs

import (
	"hash"
	"hash/fnv"
	"sync"

	"github.com/puzpuzpuz/xsync/v4"
)

var hasherPool = sync.Pool{
	New: func() interface{} {
		return fnv.New64a()
	},
}

var stringCache *xsync.Map[string, string] = xsync.NewMap[string, string]()

func internString(s string) string {
	if s == "" {
		return ""
	}
	if cached, ok := stringCache.Load(s); ok {
		return cached
	}
	stringCache.Store(s, s)
	return s
}

func hashPath(path string) uint64 {
	h := hasherPool.Get().(hash.Hash64)
	defer hasherPool.Put(h)

	h.Reset()
	_, _ = h.Write([]byte(path))
	hs := h.Sum64()
	if hs <= 1 {
		hs = 2
	}
	return hs
}
