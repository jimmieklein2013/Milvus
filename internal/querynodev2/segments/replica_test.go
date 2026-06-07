package segments

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplicaConcurrentCompaction(t *testing.T) {
	replica := NewReplica()

	// Initial state: two segments with 500 entities each (total 1000)
	seg1 := NewSegment(1, 500, nil)
	seg2 := NewSegment(2, 500, nil)
	replica.AddSegment(seg1)
	replica.AddSegment(seg2)

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Thread A: Continuously simulating compaction handoff
	wg.Add(1)
	go func() {
		defer wg.Done()
		var version int64 = 3
		for {
			select {
			case <-stopChan:
				return
			default:
				// Create new compacted segment with 1000 entities
				newSegID := atomic.AddInt64(&version, 1)
				newSeg := NewSegment(newSegID, 1000, nil)

				// Get current active segments to remove later
				snap := replica.GetSnapshot()
				var oldIDs []int64
				for _, s := range snap.Segments {
					oldIDs = append(oldIDs, s.ID)
				}
				snap.Release()

				// Perform ordered handoff: Add before Remove
				replica.AddSegment(newSeg)
				for _, id := range oldIDs {
					replica.RemoveSegment(id)
				}

				time.Sleep(10 * time.Microsecond)
			}
		}
	}()

	// Thread B: Continuously calling GetSegments() and verifying entity count consistency
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				segments := replica.GetSegments()
				
				var totalEntities int64
				for _, s := range segments {
					totalEntities += s.EntityCount
				}

				// Release the segments after checking
				for _, s := range segments {
					s.Unref()
				}

				// Assert that the total entities never drop below the expected 1000
				if totalEntities < 1000 {
					t.Errorf("Race condition detected! Total entities dropped to %d", totalEntities)
				}
			}
		}
	}()

	// Run the test for 500 milliseconds
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

func TestReplicaAtomicReplace(t *testing.T) {
	replica := NewReplica()

	// Initial state: two segments with 500 entities each (total 1000)
	seg1 := NewSegment(1, 500, nil)
	seg2 := NewSegment(2, 500, nil)
	replica.AddSegment(seg1)
	replica.AddSegment(seg2)

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Thread A: Continuously simulating atomic compaction handoff using ReplaceSegments
	wg.Add(1)
	go func() {
		defer wg.Done()
		var version int64 = 3
		for {
			select {
			case <-stopChan:
				return
			default:
				newSegID := atomic.AddInt64(&version, 1)
				newSeg := NewSegment(newSegID, 1000, nil)

				snap := replica.GetSnapshot()
				var oldIDs []int64
				for _, s := range snap.Segments {
					oldIDs = append(oldIDs, s.ID)
				}
				snap.Release()

				// Atomic replacement
				replica.ReplaceSegments([]*Segment{newSeg}, oldIDs)

				time.Sleep(10 * time.Microsecond)
			}
		}
	}()

	// Thread B: Continuously calling GetSegments() and verifying entity count consistency
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				segments := replica.GetSegments()
				
				var totalEntities int64
				for _, s := range segments {
					totalEntities += s.EntityCount
				}

				// Release the segments after checking
				for _, s := range segments {
					s.Unref()
				}

				// Assert that the total entities is exactly 1000 (since replacement is atomic,
				// it should never be 2000 or any other value, always exactly 1000)
				if totalEntities != 1000 {
					t.Errorf("Atomic replacement failed! Total entities is %d, expected 1000", totalEntities)
				}
			}
		}
	}()

	// Run the test for 500 milliseconds
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

func TestSegmentReleaseCallback(t *testing.T) {
	var released int32
	onRelease := func() {
		atomic.StoreInt32(&released, 1)
	}

	replica := NewReplica()
	seg := NewSegment(1, 100, onRelease)
	replica.AddSegment(seg)

	// Get segment (refCount becomes 2)
	segments := replica.GetSegments()
	if len(segments) != 1 {
		t.Fatalf("Expected 1 segment, got %d", len(segments))
	}

	// Remove from replica (refCount becomes 1)
	replica.RemoveSegment(1)
	if atomic.LoadInt32(&released) != 0 {
		t.Fatal("Segment released prematurely while query is still holding a reference")
	}

	// Release query reference (refCount becomes 0, triggers onRelease)
	segments[0].Unref()
	if atomic.LoadInt32(&released) != 1 {
		t.Fatal("Segment was not released after all references were dropped")
	}
}
