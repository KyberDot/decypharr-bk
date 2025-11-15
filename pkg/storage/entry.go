package storage

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

func (s *Storage) handleNameAdd(tx *bolt.Tx, torrent *Torrent) error {
	nameKey := []byte(torrent.GetFolder())
	nameIdxBkt := tx.Bucket([]byte(nameEntryBucket))
	existingByte := nameIdxBkt.Get(nameKey)
	var entry TorrentEntry
	if err := msgpack.Unmarshal(existingByte, &entry); err != nil {
		entry = TorrentEntry{
			Files: make(map[string]*File),
		}
	}
	// Update entry files listing
	for fileName, file := range torrent.Files {
		if existingFile, exists := entry.Files[fileName]; exists {
			// Prefer the newest file entry from AddedOn
			if file.AddedOn.After(existingFile.AddedOn) {
				entry.Files[fileName] = file
			} else {
				// Keep existing
				entry.Files[fileName] = existingFile
			}
		} else {
			entry.Files[fileName] = file
		}
	}

	if len(entry.Files) == 0 {
		s.logger.Info().Str("name", torrent.GetFolder()).Msg("Skipping name index update - no files found")
	}

	entry.Name = torrent.GetFolder()
	entry.Size = entry.GetSize()
	// Marshal and save back
	updatedData, err := msgpack.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal name entry: %w", err)
	}

	return nameIdxBkt.Put(nameKey, updatedData)
}

func (s *Storage) GetEntries() map[string]struct{} {
	entries := make(map[string]struct{})
	_ = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(nameEntryBucket))
		if bucket == nil {
			return fmt.Errorf("name index bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			entries[string(k)] = struct{}{}
			return nil
		})
	})
	return entries
}

func (s *Storage) UpdateTorrentEntry(torrent *Torrent) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.handleNameAdd(tx, torrent)
	})
}

func (s *Storage) GetEntry(name string) (*TorrentEntry, error) {
	var entry TorrentEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(nameEntryBucket))
		if bucket == nil {
			return fmt.Errorf("name index bucket not found")
		}
		data := bucket.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("torrent entry not found by name: %s", name)
		}
		return msgpack.Unmarshal(data, &entry)
	})

	return &entry, err
}

// ForEachEntry iterates over torrents in streaming fashion to avoid loading all into memory
func (s *Storage) ForEachEntry(fn func(*TorrentEntry) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(nameEntryBucket))
		if bucket == nil {
			return fmt.Errorf("name index bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			var entry TorrentEntry
			if err := msgpack.Unmarshal(v, &entry); err != nil {
				return fmt.Errorf("failed to unmarshal torrent entry: %w", err)
			}
			return fn(&entry)
		})
	})
}
