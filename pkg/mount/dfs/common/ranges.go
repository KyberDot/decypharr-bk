// Inspired by rclone's ranges package with optimizations for video streaming.

package common

import (
	"fmt"
	"sort"
	"sync"
)

// Range represents a contiguous byte range [Pos, Pos+Size)
type Range struct {
	Pos  int64 // Starting position
	Size int64 // Length in bytes
}

// End returns the exclusive end position of the range
func (r Range) End() int64 {
	return r.Pos + r.Size
}

// Overlaps returns true if this range overlaps or touches another range
func (r Range) Overlaps(other Range) bool {
	return r.Pos <= other.End() && other.Pos <= r.End()
}

// Contains returns true if this range fully contains another range
func (r Range) Contains(other Range) bool {
	return r.Pos <= other.Pos && other.End() <= r.End()
}

// Ranges is a sorted, coalesced list of byte ranges
// It maintains the invariant that ranges are non-overlapping and sorted
type Ranges struct {
	mu     sync.RWMutex
	ranges []Range
}

// NewRanges creates a new Ranges tracker
func NewRanges() *Ranges {
	return &Ranges{
		ranges: make([]Range, 0),
	}
}

// Insert adds a new range and coalesces with existing ranges
func (rs *Ranges) Insert(r Range) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if r.Size <= 0 {
		return // Empty range, nothing to do
	}

	// Find insertion point using binary search
	idx := sort.Search(len(rs.ranges), func(i int) bool {
		return rs.ranges[i].Pos >= r.Pos
	})

	// Insert the range
	rs.ranges = append(rs.ranges, Range{})
	copy(rs.ranges[idx+1:], rs.ranges[idx:])
	rs.ranges[idx] = r

	// Coalesce with adjacent ranges
	rs.coalesce(idx)
}

// coalesce merges overlapping or adjacent ranges around index idx
func (rs *Ranges) coalesce(idx int) {
	if idx < 0 || idx >= len(rs.ranges) {
		return
	}

	// Merge backwards
	for idx > 0 {
		prev := &rs.ranges[idx-1]
		curr := &rs.ranges[idx]

		if prev.Overlaps(*curr) {
			// Merge prev into curr
			merged := Range{
				Pos:  min(prev.Pos, curr.Pos),
				Size: max(prev.End(), curr.End()) - min(prev.Pos, curr.Pos),
			}
			rs.ranges[idx] = merged
			// Remove prev
			rs.ranges = append(rs.ranges[:idx-1], rs.ranges[idx:]...)
			idx--
		} else {
			break
		}
	}

	// Merge forwards
	for idx < len(rs.ranges)-1 {
		curr := &rs.ranges[idx]
		next := &rs.ranges[idx+1]

		if curr.Overlaps(*next) {
			// Merge next into curr
			merged := Range{
				Pos:  min(curr.Pos, next.Pos),
				Size: max(curr.End(), next.End()) - min(curr.Pos, next.Pos),
			}
			rs.ranges[idx] = merged
			// Remove next
			rs.ranges = append(rs.ranges[:idx+1], rs.ranges[idx+2:]...)
		} else {
			break
		}
	}
}

// Present returns true if the entire range is already tracked
func (rs *Ranges) Present(r Range) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if r.Size <= 0 {
		return true // Empty range is always present
	}

	// Find first range that could contain r
	idx := rs.find(r.Pos)
	if idx < 0 {
		return false
	}

	// Check if the found range fully contains r
	return rs.ranges[idx].Contains(r)
}

// find returns the index of the range containing pos, or -1 if not found
func (rs *Ranges) find(pos int64) int {
	idx := sort.Search(len(rs.ranges), func(i int) bool {
		return rs.ranges[i].End() > pos
	})

	if idx < len(rs.ranges) && rs.ranges[idx].Pos <= pos {
		return idx
	}
	return -1
}

// FindMissing returns ranges that are NOT present in the requested range
func (rs *Ranges) FindMissing(r Range) []Range {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if r.Size <= 0 {
		return nil
	}

	var missing []Range
	currentPos := r.Pos

	for i := 0; i < len(rs.ranges); i++ {
		rng := rs.ranges[i]

		// If this range starts after our current position, we have a gap
		if rng.Pos > currentPos {
			gapEnd := min(rng.Pos, r.End())
			if gapEnd > currentPos {
				missing = append(missing, Range{
					Pos:  currentPos,
					Size: gapEnd - currentPos,
				})
			}
		}

		// Move current position past this range
		if rng.End() > currentPos {
			currentPos = rng.End()
		}

		// If we've covered the entire requested range, we're done
		if currentPos >= r.End() {
			return missing
		}
	}

	// If there's still uncovered area at the end
	if currentPos < r.End() {
		missing = append(missing, Range{
			Pos:  currentPos,
			Size: r.End() - currentPos,
		})
	}

	return missing
}

// Size returns the total number of bytes tracked across all ranges
func (rs *Ranges) Size() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var total int64
	for _, r := range rs.ranges {
		total += r.Size
	}
	return total
}

// Len returns the number of ranges
func (rs *Ranges) Len() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.ranges)
}

// String returns a human-readable representation
func (rs *Ranges) String() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if len(rs.ranges) == 0 {
		return "Ranges{empty}"
	}

	return fmt.Sprintf("Ranges{%d ranges, %d bytes}", len(rs.ranges), rs.Size())
}

// GetRanges returns a copy of all ranges (for inspection/debugging)
func (rs *Ranges) GetRanges() []Range {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	result := make([]Range, len(rs.ranges))
	copy(result, rs.ranges)
	return result
}
