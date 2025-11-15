package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	bolt "go.etcd.io/bbolt"
)

const (
	// Bucket names
	queuedBucket       = "queued"
	cachedBucket       = "cached"
	nameEntryBucket    = "name_entry"
	metaBucket         = "meta"
	repairBucket       = "repair_jobs"
	repairUniqueBucket = "repair_unique"

	// Meta keys
	migrationKey = "migration:status"
)

// Storage handles persistence of managed torrents using bbolt + MsgPack
type Storage struct {
	db     *bolt.DB
	logger zerolog.Logger
}

// NewStorage creates a new storage instance
func NewStorage(dbPath string) (*Storage, error) {
	// Ensure the directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	// AddOrUpdate .db extension if not present
	if filepath.Ext(dbPath) == "" {
		dbPath = dbPath + ".db"
	}

	// Open bbolt database with optimized settings
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout:      1 * time.Second,
		NoGrowSync:   false,
		FreelistType: bolt.FreelistArrayType,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt db: %w", err)
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range []string{queuedBucket, cachedBucket, nameEntryBucket, metaBucket, repairBucket, repairUniqueBucket} {
			_, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	s := &Storage{
		db:     db,
		logger: logger.New("manager-storage"),
	}

	return s, nil
}

// Close closes the database
func (s *Storage) Close() error {
	return s.db.Close()
}

// Stats returns storage statistics
func (s *Storage) Stats() map[string]interface{} {
	stats := make(map[string]interface{})

	err := s.db.View(func(tx *bolt.Tx) error {
		// Calculate file size
		fileInfo, err := os.Stat(s.db.Path())
		var fileSize int64
		if err == nil {
			fileSize = fileInfo.Size()
		}

		stats["total_size"] = fileSize
		stats["page_count"] = tx.Size()

		// GetReader database-level stats
		dbStats := s.db.Stats()
		stats["tx_count"] = dbStats.TxN
		stats["open_tx_count"] = dbStats.OpenTxN

		return nil
	})

	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to retrieve storage stats")
		return stats
	}

	return stats
}

// Backup creates a backup of the database
func (s *Storage) Backup(path string) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(path, 0600)
	})
}
