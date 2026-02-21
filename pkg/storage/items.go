package storage

import (
	"github.com/sirrobot01/decypharr/pkg/storage/hybrid"
	"google.golang.org/protobuf/proto"
)

// GetEntryItems returns all entry item names
func (s *Storage) GetEntryItems() map[string]struct{} {
	items := make(map[string]struct{})
	_ = s.entryItems.ForEachMeta(func(key string, meta *hybrid.IndexEntry) error {
		items[key] = struct{}{}
		return nil
	})
	return items
}

// UpdateEntryItem updates an entry item from an entry
func (s *Storage) UpdateEntryItem(entry *Entry) error {
	s.updateEntryItem(entry)
	return nil
}

func (s *Storage) UpdateItem(item *EntryItem) error {
	pb := EntryItemToProto(item)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return s.entryItems.Put(item.Name, data, nil)
}

// GetEntryItem retrieves an entry item by name
func (s *Storage) GetEntryItem(name string) (*EntryItem, error) {
	data, err := s.entryItems.Get(name)
	if err != nil {
		return nil, err
	}

	var pb EntryItemProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToEntryItem(&pb), nil
}

// ForEachEntryItem iterates over entry items
func (s *Storage) ForEachEntryItem(fn func(*EntryItem) error) error {
	return s.entryItems.ForEach(func(key string, value []byte) error {
		var pb EntryItemProto
		if proto.Unmarshal(value, &pb) != nil {
			return nil
		}
		return fn(ProtoToEntryItem(&pb))
	})
}
