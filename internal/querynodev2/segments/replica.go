package segments

import (
	"sync"
	"sync/atomic"
)

// Segment represents a segment in the QueryNode.
type Segment struct {
	ID          int64
	EntityCount int64
	refCount    int32
	onRelease   func()
}

// NewSegment creates a new Segment.
func NewSegment(id int64, entityCount int64, onRelease func()) *Segment {
	return &Segment{
		ID:          id,
		EntityCount: entityCount,
		refCount:    1, // Initial reference held by the replica
		onRelease:   onRelease,
	}
}

// Ref increments the reference count of the segment.
func (s *Segment) Ref() {
	atomic.AddInt32(&s.refCount, 1)
}

// Unref decrements the reference count of the segment.
// If the reference count drops to 0, the onRelease callback is triggered.
func (s *Segment) Unref() {
	if atomic.AddInt32(&s.refCount, -1) == 0 {
		if s.onRelease != nil {
			s.onRelease()
		}
	}
}

// GetRefCount returns the current reference count (mainly for testing).
func (s *Segment) GetRefCount() int32 {
	return atomic.LoadInt32(&s.refCount)
}

// Snapshot represents a point-in-time snapshot of active segments.
type Snapshot struct {
	Segments []*Segment
}

// Release releases all segments in the snapshot.
func (s *Snapshot) Release() {
	for _, seg := range s.Segments {
		seg.Unref()
	}
}

// Replica manages the active segments in the QueryNode.
type Replica struct {
	mu       sync.RWMutex
	segments map[int64]*Segment
}

// NewReplica creates a new Replica.
func NewReplica() *Replica {
	return &Replica{
		segments: make(map[int64]*Segment),
	}
}

// AddSegment adds a segment to the replica.
func (r *Replica) AddSegment(s *Segment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segments[s.ID] = s
}

// RemoveSegment removes a segment from the replica and decrements its reference count.
func (r *Replica) RemoveSegment(id int64) {
	r.mu.Lock()
	s, exists := r.segments[id]
	if exists {
		delete(r.segments, id)
	}
	r.mu.Unlock()

	if exists {
		s.Unref()
	}
}

// ReplaceSegments atomically adds new segments and removes old segments.
func (r *Replica) ReplaceSegments(toAdd []*Segment, toRemove []int64) {
	r.mu.Lock()
	for _, s := range toAdd {
		r.segments[s.ID] = s
	}
	removedSegments := make([]*Segment, 0, len(toRemove))
	for _, id := range toRemove {
		if s, exists := r.segments[id]; exists {
			delete(r.segments, id)
			removedSegments = append(removedSegments, s)
		}
	}
	r.mu.Unlock()

	for _, s := range removedSegments {
		s.Unref()
	}
}

// GetSnapshot returns a point-in-time snapshot of the active segments.
// It increments the reference count of all returned segments.
func (r *Replica) GetSnapshot() *Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	segments := make([]*Segment, 0, len(r.segments))
	for _, s := range r.segments {
		s.Ref()
		segments = append(segments, s)
	}
	return &Snapshot{Segments: segments}
}

// GetSegments returns a slice of active segments (for compatibility/testing).
// It increments the reference count of all returned segments.
func (r *Replica) GetSegments() []*Segment {
	r.mu.RLock()
	defer r.mu.RUnlock()

	segments := make([]*Segment, 0, len(r.segments))
	for _, s := range r.segments {
		s.Ref()
		segments = append(segments, s)
	}
	return segments
}
