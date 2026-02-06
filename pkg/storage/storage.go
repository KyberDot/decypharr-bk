package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

var storeNames = []string{"entries", "queue", "items", "repair_jobs", "repair_keys"}

// Storage handles persistence using HybridStore
type Storage struct {
	// HybridStore instances for different data types
	entries    *hybrid.Store // cached entries
	queue      *hybrid.Store // queued entries
	entryItems *hybrid.Store // name -> infohash index
	repairJobs *hybrid.Store // repair jobs
	repairKeys *hybrid.Store // repair unique keys
	dir        string
	logger     zerolog.Logger
}

func createItemStores(baseDir string, baseConfig hybrid.Config) (map[string]*hybrid.Store, error) {
	items := make(map[string]*hybrid.Store)
	for _, name := range storeNames {
		config := baseConfig
		config.DataPath = filepath.Join(baseDir, name+".db")
		store, err := hybrid.New(config)
		if err != nil {
			// Cleanup previously created stores
			for _, it := range items {
				_ = it.Close()
			}
			return nil, fmt.Errorf("failed to create %s store: %w", name, err)
		}
		items[name] = store
	}
	return items, nil
}

// NewStorage creates a new storage instance with HybridStore
func NewStorage(dbPath string) (*Storage, error) {
	dbPath = filepath.Clean(dbPath)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	log := logger.New("storage")

	// Base config
	baseConfig := hybrid.Config{
		CacheSize:           5000,
		SyncInterval:        time.Second,
		CompactionThreshold: 0.5,
		AutoCompact:         true,
	}

	// Create item stores
	itemStores, err := createItemStores(dbPath, baseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create item stores: %w", err)
	}

	entries := itemStores["entries"]
	queue := itemStores["queue"]
	entryItems := itemStores["items"]
	repairJobs := itemStores["repair_jobs"]
	repairKeys := itemStores["repair_keys"]

	s := &Storage{
		entries:    entries,
		queue:      queue,
		entryItems: entryItems,
		repairJobs: repairJobs,
		repairKeys: repairKeys,
		dir:        dbPath,
		logger:     log,
	}

	// Migrate metadata to new format (adds Protocol, Bad, AddedOn to index)
	go func() {
		if count, err := s.MigrateMetadata(); err != nil {
			log.Warn().Err(err).Msg("Metadata migration failed")
		} else if count > 0 {
			log.Info().Int("count", count).Msg("Migrated entry metadata to new format")
		}
	}()

	return s, nil
}

// Close closes all storage components
func (s *Storage) Close() error {
	var errs []error
	if s.entries != nil {
		if err := s.entries.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.queue != nil {
		if err := s.queue.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.entryItems != nil {
		if err := s.entryItems.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.repairJobs != nil {
		if err := s.repairJobs.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.repairKeys != nil {
		if err := s.repairKeys.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing storage: %v", errs)
	}
	return nil
}

// Stats returns storage statistics
func (s *Storage) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	size := int64(0)
	// Read the disk usage of the data directory
	err := filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return stats
	}
	stats["total_size"] = size
	return stats
}

// Backup syncs all stores (for HybridStore, data is already on disk)
func (s *Storage) Backup(path string) error {
	s.logger.Info().Str("path", path).Msg("Backup - syncing stores")
	return nil
}

// SaveMigrationStatus saves the system migration status
func (s *Storage) SaveMigrationStatus(status *SystemMigrationStatus) error {
	pb := SystemMigrationStatusToProto(status)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return s.entries.Put("__migration_status__", data, nil)
}

// GetMigrationStatus retrieves the system migration status
func (s *Storage) GetMigrationStatus() (*SystemMigrationStatus, error) {
	data, err := s.entries.Get("__migration_status__")
	if err != nil {
		return nil, err
	}
	var pb SystemMigrationStatusProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToSystemMigrationStatus(&pb), nil
}
