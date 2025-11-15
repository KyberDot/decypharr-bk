package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) AddQueue(torrent *Torrent) error {
	torrent.CreatedAt = time.Now()
	torrent.UpdatedAt = time.Now()

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		data, err := msgpack.Marshal(torrent)
		if err != nil {
			return fmt.Errorf("failed to marshal queued torrent: %w", err)
		}

		key := []byte(strings.ToLower(torrent.InfoHash))
		return bucket.Put(key, data)
	})
}

func (s *Storage) UpdateQueue(torrent *Torrent) error {
	torrent.UpdatedAt = time.Now()

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		data, err := msgpack.Marshal(torrent)
		if err != nil {
			return fmt.Errorf("failed to marshal queued torrent: %w", err)
		}

		key := []byte(strings.ToLower(torrent.InfoHash))
		return bucket.Put(key, data)
	})
}

func (s *Storage) GetQueued(infohash string) (*Torrent, error) {
	var torr Torrent

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		key := []byte(strings.ToLower(infohash))
		data := bucket.Get(key)
		if data == nil {
			return fmt.Errorf("torrent not found: %s", infohash)
		}

		return msgpack.Unmarshal(data, &torr)
	})

	return &torr, err
}

func (s *Storage) DeleteQueued(infohash string, cleanup func(t *Torrent) error) error {
	key := strings.ToLower(infohash)
	// Perform cleanup if provided
	if cleanup != nil {
		// First retrieve the torrent for cleanup
		torr, err := s.GetQueued(key)
		if err != nil {
			return fmt.Errorf("failed to get queued torrent for deletion: %w", err)
		}
		if err := cleanup(torr); err != nil {
			s.logger.Warn().Err(err).Msg("Cleanup function failed for queued torrent")
		}
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}
		return bucket.Delete([]byte(key))
	})
}

func (s *Storage) DeleteWhereQueued(predicate func(*Torrent) bool, cleanup func(t *Torrent) error) error {
	// First, collect keys to delete
	var keysToDelete [][]byte

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			var torr Torrent
			if err := msgpack.Unmarshal(v, &torr); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to unmarshal queued torrent")
				return nil // Skip corrupted entries
			}

			if predicate == nil || predicate(&torr) {
				// Perform cleanup if provided
				if cleanup != nil {
					if err := cleanup(&torr); err != nil {
						s.logger.Warn().Err(err).Msg("Cleanup function failed for queued torrent")
					}
				}

				// Store key for deletion
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				keysToDelete = append(keysToDelete, keyCopy)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

	// Delete collected keys
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		for _, key := range keysToDelete {
			if err := bucket.Delete(key); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to delete queued torrent")
			}
		}
		return nil
	})
}

func (s *Storage) FilterQueued(filter func(*Torrent) bool) ([]*Torrent, error) {
	var torrents []*Torrent

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			var torr Torrent
			if err := msgpack.Unmarshal(v, &torr); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to unmarshal queued torrent")
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

func (s *Storage) UpdateWhereQueued(filter func(*Torrent) bool, updateFunc func(*Torrent) bool) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(queuedBucket))
		if bucket == nil {
			return fmt.Errorf("queued bucket not found")
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var torr Torrent
			if err := msgpack.Unmarshal(v, &torr); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to unmarshal queued torrent")
				continue
			}

			if filter == nil || filter(&torr) {
				if updateFunc != nil {
					changed := updateFunc(&torr)
					if changed {
						data, err := msgpack.Marshal(&torr)
						if err != nil {
							s.logger.Warn().Err(err).Msg("Failed to marshal updated queued torrent")
							continue
						}
						if err := bucket.Put(k, data); err != nil {
							s.logger.Warn().Err(err).Msg("Failed to update queued torrent")
						}
					}
				}

			}
		}
		return nil
	})
}
