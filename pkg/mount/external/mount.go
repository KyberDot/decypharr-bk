package external

import (
	"context"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type Mount struct {
	logger zerolog.Logger
}

func NewMount(mgr *manager.Manager) (*Mount, error) {
	m := &Mount{
		logger: logger.New("external-mount"),
	}
	return m, nil
}

func (m *Mount) Start(ctx context.Context) error {
	return nil
}

func (m *Mount) Stop() error {
	return nil
}

func (m *Mount) Type() string {
	return "rcloneExternal"
}

// preCacheFile pre-caches a single file by reading header chunks
func (m *Mount) preCacheFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File has probably been moved by arr, return silently
			return nil
		}
		return fmt.Errorf("failed to open file: %s: %v", filePath, err)
	}
	defer file.Close()

	// Pre-cache the file header (first 256KB) using 16KB chunks
	if err := readSmallChunks(file, 0, 256*1024, 16*1024); err != nil {
		return err
	}
	// Also read at 1MB offset (for some container formats)
	if err := readSmallChunks(file, 1024*1024, 64*1024, 16*1024); err != nil {
		return err
	}
	return nil
}

// readSmallChunks reads small chunks from file to populate cache
func readSmallChunks(file *os.File, startPos int64, totalToRead int, chunkSize int) error {
	_, err := file.Seek(startPos, 0)
	if err != nil {
		return err
	}

	buf := make([]byte, chunkSize)
	bytesRemaining := totalToRead

	for bytesRemaining > 0 {
		toRead := chunkSize
		if bytesRemaining < chunkSize {
			toRead = bytesRemaining
		}

		n, err := file.Read(buf[:toRead])
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		bytesRemaining -= n
	}
	return nil
}
