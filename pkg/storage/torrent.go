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
	torrent.State = torrent.GetState()

	return s.db.Update(func(tx *bolt.Tx) error {
		cachedBkt := tx.Bucket([]byte(cachedBucket))
		nameIdxBkt := tx.Bucket([]byte(nameIndexBucket))

		nameKey := []byte(torrent.GetFolder())
		existingInfoHash := nameIdxBkt.Get(nameKey)

		// CHECK FOR NAME COLLISION
		if existingInfoHash != nil && string(existingInfoHash) != torrent.InfoHash {
			existing, err := s.Get(string(existingInfoHash))
			if err == nil {
				s.logger.Debug().
					Str("name", torrent.GetFolder()).
					Str("existing_hash", existing.InfoHash).
					Str("new_hash", torrent.InfoHash).
					Msg("Merging torrents with same name")

				// MERGE THE TORRENTS
				merged := MergeTorrents(existing, torrent)

				// Save merged version under existing infohash
				data, _ := msgpack.Marshal(merged)
				if err := cachedBkt.Put([]byte(existing.InfoHash), data); err != nil {
					return err
				}

				// Name index still points to existing infohash
				return nil
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

		if err := nameIdxBkt.Put(nameKey, []byte(torrent.InfoHash)); err != nil {
			return fmt.Errorf("failed to set name index: %w", err)
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
		bucket := tx.Bucket([]byte(nameIndexBucket))
		if bucket == nil {
			return fmt.Errorf("name index bucket not found")
		}

		data := bucket.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("torrent not found by name: %s", name)
		}
		infohash = string(data)
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

func (s *Storage) GetByHashAndCategory(infohash, category string) (*Torrent, error) {
	// For bbolt, category is not used in cached bucket
	return s.Get(infohash)
}

func (s *Storage) GetByName(name string) (*Torrent, error) {
	infohash, err := s.getInfoHash(name)
	if err != nil {
		return nil, err
	}
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

func (s *Storage) DeleteByName(name string) error {
	infohash, err := s.getInfoHash(name)
	if err != nil {
		return err
	}
	return s.Delete(infohash)
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
