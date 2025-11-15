package common

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// RangeTracker provides lazy, batched range tracking to minimize lock contention.
// Instead of updating ranges immediately on every write, updates are queued
// and processed in batches by a background goroutine.
//
// This dramatically reduces lock contention: from 16+ locks per 8MB chunk
// to just 1-2 locks per chunk.
type RangeTracker struct {
	// Core range storage
	ranges *Ranges
	mu     sync.RWMutex

	// Lazy update queue (lock-free submission)
	updateQueue chan Range
	flushTicker *time.Ticker
	flushBatch  int // Number of ranges to batch before flushing

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	// Statistics
	stats rangeStats

	// Callbacks
	onDirty func() // Called when ranges become dirty
}

// rangeStats tracks range tracking statistics
type rangeStats struct {
	rangesInserted   atomic.Int64
	batchesProcessed atomic.Int64
	immediateFlushes atomic.Int64
	deferredFlushes  atomic.Int64
	totalSize        atomic.Int64
}

// RangeTrackerConfig configures the range tracker
type RangeTrackerConfig struct {
	QueueSize      int           // Update queue size (default: 1000)
	FlushInterval  time.Duration // How often to flush batches (default: 100ms)
	FlushBatchSize int           // Batch size before immediate flush (default: 100)
	OnDirty        func()        // Callback when ranges become dirty
}

// DefaultRangeTrackerConfig returns optimized default configuration
func DefaultRangeTrackerConfig() *RangeTrackerConfig {
	return &RangeTrackerConfig{
		QueueSize:      1000,
		FlushInterval:  100 * time.Millisecond,
		FlushBatchSize: 100,
	}
}

// NewRangeTracker creates a new lazy range tracker.
func NewRangeTracker(ctx context.Context, config *RangeTrackerConfig) *RangeTracker {
	if config == nil {
		config = DefaultRangeTrackerConfig()
	}

	ctx, cancel := context.WithCancel(ctx)

	rt := &RangeTracker{
		ranges:      NewRanges(),
		updateQueue: make(chan Range, config.QueueSize),
		flushTicker: time.NewTicker(config.FlushInterval),
		flushBatch:  config.FlushBatchSize,
		ctx:         ctx,
		cancel:      cancel,
		onDirty:     config.OnDirty,
	}

	// Start background processor
	rt.wg.Add(1)
	go rt.processUpdates()

	return rt
}

// Add queues a range for lazy addition.
// This is non-blocking and lock-free in the common case.
func (rt *RangeTracker) Add(r Range) {
	if r.Size <= 0 {
		return
	}

	select {
	case rt.updateQueue <- r:
		// Queued successfully
	default:
		// Queue full, flush immediately (blocks)
		rt.flushOne(r)
		rt.stats.immediateFlushes.Add(1)
	}
}

// Present checks if a range is present (read lock only).
func (rt *RangeTracker) Present(r Range) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.ranges.Present(r)
}

// FindMissing finds missing ranges within a requested range (read lock only).
func (rt *RangeTracker) FindMissing(r Range) []Range {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.ranges.FindMissing(r)
}

// GetRanges returns all current ranges (read lock only).
func (rt *RangeTracker) GetRanges() []Range {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.ranges.GetRanges()
}

// Size returns the total size of all ranges (read lock only).
func (rt *RangeTracker) Size() int64 {
	return rt.stats.totalSize.Load()
}

// processUpdates runs in background, batching range updates.
func (rt *RangeTracker) processUpdates() {
	defer rt.wg.Done()

	batch := make([]Range, 0, rt.flushBatch)

	for {
		select {
		case r := <-rt.updateQueue:
			batch = append(batch, r)

			// Flush if batch is full
			if len(batch) >= rt.flushBatch {
				rt.flushBatchInterval(batch)
				batch = batch[:0]
			}

		case <-rt.flushTicker.C:
			// Periodic flush
			if len(batch) > 0 {
				rt.flushBatchInterval(batch)
				batch = batch[:0]
				rt.stats.deferredFlushes.Add(1)
			}

		case <-rt.ctx.Done():
			// Final flush before exit
			if len(batch) > 0 {
				rt.flushBatchInterval(batch)
			}
			return
		}
	}
}

// flushBatchInterval flushes a batch of ranges with a single lock.
func (rt *RangeTracker) flushBatchInterval(batch []Range) {
	if len(batch) == 0 {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	var addedSize int64
	for _, r := range batch {
		// Track size before insertion
		oldSize := rt.ranges.Size()

		// Insert range (will merge with existing)
		rt.ranges.Insert(r)

		// Track size change
		newSize := rt.ranges.Size()
		addedSize += newSize - oldSize

		rt.stats.rangesInserted.Add(1)
	}

	// Update total size
	rt.stats.totalSize.Add(addedSize)
	rt.stats.batchesProcessed.Add(1)

	// Call dirty callback if provided
	if rt.onDirty != nil && len(batch) > 0 {
		rt.onDirty()
	}
}

// flushOne flushes a single range immediately (with lock).
func (rt *RangeTracker) flushOne(r Range) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	oldSize := rt.ranges.Size()
	rt.ranges.Insert(r)
	newSize := rt.ranges.Size()

	rt.stats.totalSize.Add(newSize - oldSize)
	rt.stats.rangesInserted.Add(1)

	if rt.onDirty != nil {
		rt.onDirty()
	}
}

// Flush forces immediate processing of all pending updates.
func (rt *RangeTracker) Flush() {
	// Drain queue and process
	batch := make([]Range, 0, rt.flushBatch)

	for {
		select {
		case r := <-rt.updateQueue:
			batch = append(batch, r)
			if len(batch) >= rt.flushBatch {
				rt.flushBatchInterval(batch)
				batch = batch[:0]
			}
		default:
			// Queue empty
			if len(batch) > 0 {
				rt.flushBatchInterval(batch)
			}
			return
		}
	}
}

// Close stops the range tracker and flushes pending updates.
func (rt *RangeTracker) Close() error {
	rt.closeOnce.Do(func() {
		// Stop ticker
		rt.flushTicker.Stop()

		// Cancel context to stop background goroutine
		rt.cancel()

		// Wait for background goroutine to finish (will flush remaining)
		rt.wg.Wait()
	})

	return nil
}

// Stats returns statistics about the range tracker.
func (rt *RangeTracker) Stats() map[string]interface{} {
	return map[string]interface{}{
		"ranges_inserted":   rt.stats.rangesInserted.Load(),
		"batches_processed": rt.stats.batchesProcessed.Load(),
		"immediate_flushes": rt.stats.immediateFlushes.Load(),
		"deferred_flushes":  rt.stats.deferredFlushes.Load(),
		"total_size":        rt.stats.totalSize.Load(),
		"queue_length":      len(rt.updateQueue),
		"queue_capacity":    cap(rt.updateQueue),
		"num_ranges":        len(rt.GetRanges()),
	}
}

// LoadFromMetadata initializes ranges from saved metadata.
func (rt *RangeTracker) LoadFromMetadata(savedRanges []Range) {
	if len(savedRanges) == 0 {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	var totalSize int64
	for _, r := range savedRanges {
		rt.ranges.Insert(r)
		totalSize += r.Size
	}

	rt.stats.totalSize.Store(totalSize)
}

// Clone creates a copy of the current ranges (for metadata persistence).
func (rt *RangeTracker) Clone() []Range {
	// First flush pending updates
	rt.Flush()

	// Then read ranges
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	return rt.ranges.GetRanges()
}

// Reset clears all ranges.
func (rt *RangeTracker) Reset() {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.ranges = NewRanges()
	rt.stats.totalSize.Store(0)
}

// IsComplete checks if all ranges from 0 to size are present.
func (rt *RangeTracker) IsComplete(size int64) bool {
	if size <= 0 {
		return true
	}

	rt.mu.RLock()
	defer rt.mu.RUnlock()

	return rt.ranges.Present(Range{Pos: 0, Size: size})
}

// GetCoverage returns the percentage of file covered (0.0 to 1.0).
func (rt *RangeTracker) GetCoverage(totalSize int64) float64 {
	if totalSize <= 0 {
		return 0.0
	}

	currentSize := rt.Size()
	if currentSize >= totalSize {
		return 1.0
	}

	return float64(currentSize) / float64(totalSize)
}

// GetFragmentation returns a fragmentation metric (number of separate ranges).
// Lower is better (1 = completely contiguous).
func (rt *RangeTracker) GetFragmentation() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	return len(rt.ranges.GetRanges())
}
