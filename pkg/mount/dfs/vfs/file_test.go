package vfs

import (
	"os"
	"runtime"
	"testing"
)

func TestSparseFileImplementation(t *testing.T) {
	// Create temp directory for test
	tempDir := t.TempDir()

	// Create a sparse file
	stats := &StatsTracker{}
	sf, err := newSparseFile(tempDir, "test-torrent", "test-file.mkv", 100*1024*1024, 8*1024*1024, stats, nil)
	if err != nil {
		t.Fatalf("Failed to create sparse file: %v", err)
	}
	defer sf.Close()

	// Write some data to chunk 0
	chunk0Data := make([]byte, 8*1024*1024)
	for i := range chunk0Data {
		chunk0Data[i] = byte(i % 256)
	}

	n, err := sf.WriteAt(chunk0Data, 0)
	if err != nil {
		t.Fatalf("Failed to write chunk 0: %v", err)
	}
	if n != len(chunk0Data) {
		t.Fatalf("Expected to write %d bytes, wrote %d", len(chunk0Data), n)
	}

	// Try to read chunk 0 (should be cached)
	readBuf := make([]byte, 8*1024*1024)
	n, cached, err := sf.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("Failed to read chunk 0: %v", err)
	}
	if !cached {
		t.Fatal("Expected chunk 0 to be cached")
	}
	if n != len(readBuf) {
		t.Fatalf("Expected to read %d bytes, read %d", len(readBuf), n)
	}

	// Verify data matches
	for i := 0; i < len(chunk0Data); i++ {
		if readBuf[i] != chunk0Data[i] {
			t.Fatalf("Data mismatch at byte %d: expected %d, got %d", i, chunk0Data[i], readBuf[i])
		}
	}

	// Try to read chunk 1 (should NOT be cached)
	smallBuf := make([]byte, 1024)
	_, cached, err = sf.ReadAt(smallBuf, 8*1024*1024) // Offset at chunk 1
	if err != nil {
		t.Fatalf("Unexpected error reading uncached chunk: %v", err)
	}
	if cached {
		t.Fatal("Expected chunk 1 to NOT be cached")
	}

	// Write to chunk 1
	chunk1Data := make([]byte, 8*1024*1024)
	for i := range chunk1Data {
		chunk1Data[i] = byte((i + 128) % 256)
	}

	_, err = sf.WriteAt(chunk1Data, 8*1024*1024)
	if err != nil {
		t.Fatalf("Failed to write chunk 1: %v", err)
	}

	// Now chunk 1 should be cached
	_, cached, err = sf.ReadAt(readBuf, 8*1024*1024)
	if err != nil {
		t.Fatalf("Failed to read chunk 1: %v", err)
	}
	if !cached {
		t.Fatal("Expected chunk 1 to be cached after write")
	}

	// Test Sync
	err = sf.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	t.Logf("Test passed on %s using %s sparse file implementation", runtime.GOOS, getImplementationType())
}

func getImplementationType() string {
	return "range map with metadata persistence"
}

func TestSparseFileAcrossRestart(t *testing.T) {
	tempDir := t.TempDir()
	torrentName := "test-torrent"
	fileName := "test-file.mkv"
	fileSize := int64(100 * 1024 * 1024)
	chunkSize := int64(8 * 1024 * 1024)

	// Create sparse file and write data
	stats := &StatsTracker{}
	sf, err := newSparseFile(tempDir, torrentName, fileName, fileSize, chunkSize, stats, nil)
	if err != nil {
		t.Fatalf("Failed to create sparse file: %v", err)
	}

	// Write to chunk 0
	chunk0Data := make([]byte, chunkSize)
	for i := range chunk0Data {
		chunk0Data[i] = 0xAB
	}
	_, err = sf.WriteAt(chunk0Data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Close the file
	err = sf.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Reopen the file (simulating restart)
	sf2, err := newSparseFile(tempDir, torrentName, fileName, fileSize, chunkSize, stats, nil)
	if err != nil {
		t.Fatalf("Failed to reopen sparse file: %v", err)
	}
	defer sf2.Close()

	// Check if chunk 0 is still cached
	readBuf := make([]byte, chunkSize)
	_, cached, err := sf2.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if !cached {
		t.Fatal("Expected chunk 0 to still be cached after restart")
	}

	// Verify data
	for i := 0; i < len(readBuf); i++ {
		if readBuf[i] != 0xAB {
			t.Fatalf("Data mismatch after restart at byte %d", i)
		}
	}

	t.Logf("Restart persistence test passed on %s", runtime.GOOS)
}

func TestRemoveFromDisk(t *testing.T) {
	tempDir := t.TempDir()

	stats := &StatsTracker{}
	sf, err := newSparseFile(tempDir, "test-torrent", "test-file.mkv", 10*1024*1024, 1*1024*1024, stats, nil)
	if err != nil {
		t.Fatalf("Failed to create sparse file: %v", err)
	}

	// Write some data
	data := make([]byte, 1024)
	_, err = sf.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Get the file path
	filePath := sf.path

	// Remove from disk
	err = sf.removeFromDisk()
	if err != nil {
		t.Fatalf("Failed to remove from disk: %v", err)
	}

	// Verify file is gone
	_, err = os.Stat(filePath)
	if !os.IsNotExist(err) {
		t.Fatal("Expected file to be deleted")
	}

	// Note: Metadata file should also be deleted (handled by removeFromDisk)
}

func BenchmarkSparseFileRead(b *testing.B) {
	tempDir := b.TempDir()

	stats := &StatsTracker{}
	sf, err := newSparseFile(tempDir, "bench-torrent", "bench-file.mkv", 1024*1024*1024, 8*1024*1024, stats, nil)
	if err != nil {
		b.Fatalf("Failed to create sparse file: %v", err)
	}
	defer sf.Close()

	// Write one chunk
	chunkData := make([]byte, 8*1024*1024)
	_, err = sf.WriteAt(chunkData, 0)
	if err != nil {
		b.Fatalf("Failed to write: %v", err)
	}

	readBuf := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = sf.ReadAt(readBuf, 0)
	}

	b.ReportMetric(float64(b.N*len(readBuf))/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

func BenchmarkSparseFileWrite(b *testing.B) {
	tempDir := b.TempDir()

	stats := &StatsTracker{}
	sf, err := newSparseFile(tempDir, "bench-torrent", "bench-file.mkv", 1024*1024*1024, 8*1024*1024, stats, nil)
	if err != nil {
		b.Fatalf("Failed to create sparse file: %v", err)
	}
	defer sf.Close()

	writeData := make([]byte, 4096)

	b.ResetTimer()
	offset := int64(0)
	for i := 0; i < b.N; i++ {
		_, _ = sf.WriteAt(writeData, offset)
		offset = (offset + int64(len(writeData))) % (8 * 1024 * 1024)
	}

	b.ReportMetric(float64(b.N*len(writeData))/b.Elapsed().Seconds()/1024/1024, "MB/s")
}

func TestSanitizeForPath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal-file.mkv", "normal-file.mkv"},
		{"file/with/slashes.mp4", "file_with_slashes.mp4"},
		{"file\\with\\backslashes.avi", "file_with_backslashes.avi"},
		{"file:with:colons.mkv", "file_with_colons.mkv"},
		{"file*with?special<chars>.mp4", "file_with_special_chars_.mp4"},
	}

	for _, test := range tests {
		result := sanitizeForPath(test.input)
		if result != test.expected {
			t.Errorf("sanitizeForPath(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}
}
