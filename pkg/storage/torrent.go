package storage

import (
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

// AddOrUpdate adds or updates a torrent with automatic bucket management
func (s *Storage) AddOrUpdate(torrent *Torrent) error {
	torrent.UpdatedAt = time.Now()
	return s.db.Update(func(tx *bolt.Tx) error {
		cachedBkt := tx.Bucket([]byte(cachedBucket))
		// Do name index handling
		if err := s.handleNameAdd(tx, torrent); err != nil {
			return err
		}

		// Always prefer newer torrent data
		// over existing data, so check timestamps
		existingData := cachedBkt.Get([]byte(torrent.InfoHash))
		if existingData != nil {
			var existing Torrent
			if err := msgpack.Unmarshal(existingData, &existing); err == nil {
				if existing.AddedOn.After(torrent.AddedOn) {
					// Existing is newer, skip update
					return nil
				}
			}
		}

		data, err := msgpack.Marshal(torrent)
		if err != nil {
			return fmt.Errorf("failed to marshal torrent: %w", err)
		}

		if err := cachedBkt.Put([]byte(torrent.InfoHash), data); err != nil {
			return fmt.Errorf("failed to set torrent: %w", err)
		}

		return nil
	})
}

// BatchAddOrUpdate adds or updates multiple torrents in a single transaction
func (s *Storage) BatchAddOrUpdate(torrents []*Torrent) error {
	if len(torrents) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		cachedBkt := tx.Bucket([]byte(cachedBucket))
		for _, torrent := range torrents {
			torrent.UpdatedAt = time.Now()

			if err := s.handleNameAdd(tx, torrent); err != nil {
				return err
			}

			// Check existing torrent for timestamp
			existingData := cachedBkt.Get([]byte(torrent.InfoHash))
			if existingData != nil {
				var existing Torrent
				if err := msgpack.Unmarshal(existingData, &existing); err == nil {
					if existing.AddedOn.After(torrent.AddedOn) {
						// Existing is newer, skip update
						continue
					}
				}
			}

			// No merge - regular save
			data, err := msgpack.Marshal(torrent)
			if err != nil {
				return fmt.Errorf("failed to marshal torrent: %w", err)
			}

			if err := cachedBkt.Put([]byte(torrent.InfoHash), data); err != nil {
				return fmt.Errorf("failed to set torrent: %w", err)
			}
		}

		return nil
	})
}

func (s *Storage) getInfoHash(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("either infohash or name must be provided")
	}

	var infohash string

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(nameEntryBucket))
		if bucket == nil {
			return fmt.Errorf("name index bucket not found")
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("torrent not found by name: %s", name)
		}
		var entry TorrentEntry
		if err := msgpack.Unmarshal(data, &entry); err != nil {
			return fmt.Errorf("failed to unmarshal name entry: %w", err)
		}
		return nil
	})

	return infohash, err
}

func (s *Storage) Exists(infohash string) (bool, error) {
	var exists bool

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		data := bucket.Get([]byte(infohash))
		exists = data != nil
		return nil
	})

	return exists, err
}

// Get retrieves a torrent by InfoHash
func (s *Storage) Get(infohash string) (*Torrent, error) {
	var torr Torrent

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		data := bucket.Get([]byte(infohash))
		if data == nil {
			return fmt.Errorf("torrent not found: %s", infohash)
		}

		return msgpack.Unmarshal(data, &torr)
	})

	return &torr, err
}

func (s *Storage) GetByHashAndCategory(infohash string) (*Torrent, error) {
	return s.Get(infohash)
}

// List retrieves all cached torrents with optional filtering
func (s *Storage) List(filter func(*Torrent) bool) ([]*Torrent, error) {
	var torrents []*Torrent

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			var torr Torrent
			if err := msgpack.Unmarshal(v, &torr); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to unmarshal torrent")
				return nil // Skip corrupted entries
			}

			if filter == nil || filter(&torr) {
				torrents = append(torrents, &torr)
			}
			return nil
		})
	})

	return torrents, err
}

// ForEach iterates over torrents in streaming fashion to avoid loading all into memory
func (s *Storage) ForEach(fn func(*Torrent) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			var torr Torrent
			if err := msgpack.Unmarshal(v, &torr); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to unmarshal torrent")
				return nil // Skip corrupted entries
			}

			return fn(&torr)
		})
	})
}

// ForEachBatch iterates over torrents in batches to control memory usage
func (s *Storage) ForEachBatch(batchSize int, fn func([]*Torrent) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		batch := make([]*Torrent, 0, batchSize)

		err := bucket.ForEach(func(k, v []byte) error {
			var torr Torrent
			if err := msgpack.Unmarshal(v, &torr); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to unmarshal torrent")
				return nil // Skip corrupted entries
			}

			batch = append(batch, &torr)

			// Process batch when it reaches the size limit
			if len(batch) >= batchSize {
				if err := fn(batch); err != nil {
					return err
				}
				// Clear batch for reuse
				batch = batch[:0]
			}
			return nil
		})

		// Process remaining items
		if err == nil && len(batch) > 0 {
			err = fn(batch)
		}

		return err
	})
}

// Delete removes a torrent
func (s *Storage) Delete(infohash string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		return bucket.Delete([]byte(infohash))
	})
}

// DeleteBatch deletes multiple torrents
func (s *Storage) DeleteBatch(infohashes []string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		for _, infohash := range infohashes {
			if err := bucket.Delete([]byte(infohash)); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to delete torrent")
			}
		}
		return nil
	})
}

// Count returns the total number of torrents
func (s *Storage) Count() (int, error) {
	count := 0

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(cachedBucket))
		if bucket == nil {
			return fmt.Errorf("cached bucket not found")
		}

		stats := bucket.Stats()
		count = stats.KeyN
		return nil
	})

	return count, err
}
