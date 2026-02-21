package usenet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"google.golang.org/protobuf/proto"
)

const (
	metaFileExtension = ".meta"
	metaDirName       = "meta"
)

const (
	NZBStatusPending     = "pending"
	NZBStatusParsing     = "parsing"
	NZBStatusDownloading = "downloading"
	NZBStatusCompleted   = "completed"
	NZBStatusFailed      = "failed"
)

// NZBStorage handles file-based persistence of NZB metadata using protobuf
type NZBStorage struct {
	metaDir string
	logger  zerolog.Logger
	mu      sync.RWMutex // Protects file operations
}

// NewNZBStorage creates a new file-based NZB storage
func NewNZBStorage() (*NZBStorage, error) {
	metaDir := filepath.Join(config.GetMainPath(), "usenet", metaDirName)
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create meta directory: %w", err)
	}

	return &NZBStorage{
		metaDir: metaDir,
		logger:  logger.New("nzb-storage"),
	}, nil
}

// metaFilePath returns the path for a given NZB ID
func (s *NZBStorage) metaFilePath(id string) string {
	return filepath.Join(s.metaDir, id+metaFileExtension)
}

// AddNZB saves an NZB to file storage
func (s *NZBStorage) AddNZB(nzb *storage.NZB) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pb := nzbToProto(nzb)
	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("failed to marshal NZB: %w", err)
	}

	path := s.metaFilePath(nzb.ID)

	// Write atomically using temp file
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write NZB meta file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename NZB meta file: %w", err)
	}

	return nil
}

// GetNZB retrieves an NZB from file storage
func (s *NZBStorage) GetNZB(id string) (*storage.NZB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.metaFilePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("nzb not found: %s", id)
		}
		return nil, fmt.Errorf("failed to read NZB meta file: %w", err)
	}

	var pb NZBProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NZB: %w", err)
	}

	return protoToNZB(&pb), nil
}

// DeleteNZB removes an NZB from file storage
func (s *NZBStorage) DeleteNZB(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.metaFilePath(id)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete NZB meta file: %w", err)
	}
	return nil
}

// ForEachNZB iterates over all NZBs in storage
func (s *NZBStorage) ForEachNZB(fn func(*storage.NZB) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return fmt.Errorf("failed to read meta directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}

		path := filepath.Join(s.metaDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			s.logger.Warn().Err(err).Str("file", entry.Name()).Msg("Failed to read NZB meta file")
			continue
		}

		var pb NZBProto
		if err := proto.Unmarshal(data, &pb); err != nil {
			s.logger.Warn().Err(err).Str("file", entry.Name()).Msg("Failed to unmarshal NZB")
			continue
		}

		nzb := protoToNZB(&pb)
		if err := fn(nzb); err != nil {
			return err
		}
	}

	return nil
}

// GetAllNZBIDs returns all NZB IDs in storage
func (s *NZBStorage) GetAllNZBIDs() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta directory: %w", err)
	}

	var ids []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}
		// Extract ID from filename (remove .meta extension)
		id := entry.Name()[:len(entry.Name())-len(metaFileExtension)]
		ids = append(ids, id)
	}

	return ids, nil
}

// Exists checks if an NZB exists in storage
func (s *NZBStorage) Exists(id string) bool {
	path := s.metaFilePath(id)
	_, err := os.Stat(path)
	return err == nil
}

// Count returns the number of NZBs in storage
func (s *NZBStorage) Count() (int, error) {
	ids, err := s.GetAllNZBIDs()
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// NZBStorageStats holds NZB storage statistics.
type NZBStorageStats struct {
	Count      int    `json:"count"`
	TotalBytes int64  `json:"total_bytes"`
	MetaDir    string `json:"meta_dir"`
}

// Stats returns storage statistics.
func (s *NZBStorage) Stats() NZBStorageStats {
	count, _ := s.Count()

	var totalSize int64
	entries, _ := os.ReadDir(s.metaDir)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			continue
		}
		info, err := entry.Info()
		if err == nil {
			totalSize += info.Size()
		}
	}

	return NZBStorageStats{
		Count:      count,
		TotalBytes: totalSize,
		MetaDir:    s.metaDir,
	}
}

// ============================================================================
// Conversion functions between storage.NZB and NZBProto
// ============================================================================

func nzbToProto(nzb *storage.NZB) *NZBProto {
	pb := &NZBProto{
		Id:               nzb.ID,
		Name:             nzb.Name,
		Title:            nzb.Title,
		Path:             nzb.Path,
		TotalSize:        nzb.TotalSize,
		DatePostedUnix:   nzb.DatePosted.Unix(),
		Category:         nzb.Category,
		Groups:           nzb.Groups,
		Downloaded:       nzb.Downloaded,
		AddedOnUnix:      nzb.AddedOn.Unix(),
		LastActivityUnix: nzb.LastActivity.Unix(),
		Status:           nzb.Status,
		Progress:         nzb.Progress,
		Percentage:       nzb.Percentage,
		SizeDownloaded:   nzb.SizeDownloaded,
		Eta:              nzb.ETA,
		Speed:            nzb.Speed,
		CompletedOnUnix:  nzb.CompletedOn.Unix(),
		IsBad:            nzb.IsBad,
		Storage:          nzb.Storage,
		FailMessage:      nzb.FailMessage,
		Password:         nzb.Password,
	}

	pb.Files = make([]*NZBFileProto, len(nzb.Files))
	for i, f := range nzb.Files {
		pb.Files[i] = nzbFileToProto(&f)
	}

	return pb
}

func nzbFileToProto(f *storage.NZBFile) *NZBFileProto {
	pb := &NZBFileProto{
		NzbId:         f.NzbID,
		Name:          f.Name,
		InternalPath:  f.InternalPath,
		Size:          f.Size,
		StartOffset:   f.StartOffset,
		Groups:        f.Groups,
		FileType:      string(f.FileType),
		Password:      f.Password,
		IsDeleted:     f.IsDeleted,
		IsStored:      f.IsStored,
		SegmentSize:   f.SegmentSize,
		EncryptionKey: f.EncryptionKey,
		EncryptionIv:  f.EncryptionIV,
		IsEncrypted:   f.IsEncrypted,
	}

	pb.Segments = make([]*NZBSegmentProto, len(f.Segments))
	for i, s := range f.Segments {
		pb.Segments[i] = &NZBSegmentProto{
			Number:           int32(s.Number),
			MessageId:        s.MessageID,
			Bytes:            s.Bytes,
			StartOffset:      s.StartOffset,
			EndOffset:        s.EndOffset,
			Group:            s.Group,
			SegmentDataStart: s.SegmentDataStart,
		}
	}

	return pb
}

func protoToNZB(pb *NZBProto) *storage.NZB {
	nzb := &storage.NZB{
		ID:             pb.Id,
		Name:           pb.Name,
		Title:          pb.Title,
		Path:           pb.Path,
		TotalSize:      pb.TotalSize,
		DatePosted:     time.Unix(pb.DatePostedUnix, 0),
		Category:       pb.Category,
		Groups:         pb.Groups,
		Downloaded:     pb.Downloaded,
		AddedOn:        time.Unix(pb.AddedOnUnix, 0),
		LastActivity:   time.Unix(pb.LastActivityUnix, 0),
		Status:         pb.Status,
		Progress:       pb.Progress,
		Percentage:     pb.Percentage,
		SizeDownloaded: pb.SizeDownloaded,
		ETA:            pb.Eta,
		Speed:          pb.Speed,
		CompletedOn:    time.Unix(pb.CompletedOnUnix, 0),
		IsBad:          pb.IsBad,
		Storage:        pb.Storage,
		FailMessage:    pb.FailMessage,
		Password:       pb.Password,
	}

	nzb.Files = make([]storage.NZBFile, len(pb.Files))
	for i, f := range pb.Files {
		nzb.Files[i] = protoToNZBFile(f)
	}

	return nzb
}

func protoToNZBFile(pb *NZBFileProto) storage.NZBFile {
	f := storage.NZBFile{
		NzbID:         pb.NzbId,
		Name:          pb.Name,
		InternalPath:  pb.InternalPath,
		Size:          pb.Size,
		StartOffset:   pb.StartOffset,
		Groups:        pb.Groups,
		FileType:      storage.NZBFileType(pb.FileType),
		Password:      pb.Password,
		IsDeleted:     pb.IsDeleted,
		IsStored:      pb.IsStored,
		SegmentSize:   pb.SegmentSize,
		EncryptionKey: pb.EncryptionKey,
		EncryptionIV:  pb.EncryptionIv,
		IsEncrypted:   pb.IsEncrypted,
	}

	f.Segments = make([]storage.NZBSegment, len(pb.Segments))
	for i, s := range pb.Segments {
		f.Segments[i] = storage.NZBSegment{
			Number:           int(s.Number),
			MessageID:        s.MessageId,
			Bytes:            s.Bytes,
			StartOffset:      s.StartOffset,
			EndOffset:        s.EndOffset,
			Group:            s.Group,
			SegmentDataStart: s.SegmentDataStart,
		}
	}

	return f
}
