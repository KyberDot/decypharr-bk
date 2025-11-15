package vfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/common"
)

// Metadata stores information about a cached file
// This is persisted to disk as JSON in the vfsMeta directory
type Metadata struct {
	ModTime     time.Time      `json:"mod_time"`    // Modification time of remote file
	ATime       time.Time      `json:"atime"`       // Last access time (for LRU)
	Size        int64          `json:"size"`        // Total file size
	Ranges      []common.Range `json:"ranges"`      // Which parts are cached
	Fingerprint string         `json:"fingerprint"` // Hash/ETag for cache validation
	Dirty       bool           `json:"dirty"`       // Has unflushed writes
}

// metadataPath returns the path to the metadata file for a given cache file
func (m *Manager) metadataPath(torrentName, fileName string) string {
	torrentName = sanitizeForPath(torrentName)
	fileName = sanitizeForPath(fileName)

	metaRoot := filepath.Join(m.config.CacheDir, ".meta")
	return filepath.Join(metaRoot, torrentName, fileName+".json")
}

// saveMetadata saves metadata to disk as JSON
func (m *Manager) saveMetadata(torrentName, fileName string, meta *Metadata) error {
	metaPath := m.metadataPath(torrentName, fileName)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}

	// Write to temporary file first (atomic write)
	tmpPath := metaPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp meta file: %w", err)
	}
	defer file.Close()

	// Encode with indentation for debugging
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")
	if err := encoder.Encode(meta); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("encode metadata: %w", err)
	}

	if err := file.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync meta file: %w", err)
	}

	file.Close()

	// Atomic rename
	if err := os.Rename(tmpPath, metaPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename meta file: %w", err)
	}

	return nil
}

// loadMetadata loads metadata from disk
func (m *Manager) loadMetadata(torrentName, fileName string) (*Metadata, error) {
	metaPath := m.metadataPath(torrentName, fileName)

	file, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No metadata file exists
		}
		return nil, fmt.Errorf("open meta file: %w", err)
	}
	defer file.Close()

	var meta Metadata
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&meta); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}

	return &meta, nil
}

// deleteMetadata removes metadata using the canonical cache key (sanitized path).
func (m *Manager) deleteMetadata(cacheKey string) error {
	if cacheKey == "" {
		return nil
	}

	metaRoot := filepath.Join(m.config.CacheDir, ".meta")
	metaPath := filepath.Join(metaRoot, cacheKey+".json")

	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// scanMetadataDirectory scans the metadata directory and returns total cached size and file list
// This is much faster than scanning actual cache files
func (m *Manager) scanMetadataDirectory() (int64, []cachedFileInfo, error) {
	metaRoot := filepath.Join(m.config.CacheDir, ".meta")

	var totalSize int64
	var fileList []cachedFileInfo

	if _, err := os.Stat(metaRoot); os.IsNotExist(err) {
		return 0, nil, nil
	}

	err := filepath.Walk(metaRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Only process .json metadata files
		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		// Load metadata to get actual cached size and access time
		file, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer file.Close()

		var meta Metadata
		if err := json.NewDecoder(file).Decode(&meta); err != nil {
			return nil // Skip corrupted metadata
		}

		// Calculate cached size from ranges
		var cachedSize int64
		for _, r := range meta.Ranges {
			cachedSize += r.Size
		}
		totalSize += cachedSize

		// Extract torrent and file name from path
		relPath, _ := filepath.Rel(metaRoot, path)
		relPath = relPath[:len(relPath)-5] // Remove .json extension

		fileList = append(fileList, cachedFileInfo{
			cacheKey:   relPath,
			accessTime: meta.ATime,
		})

		return nil
	})

	return totalSize, fileList, err
}
