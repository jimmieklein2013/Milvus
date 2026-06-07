package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
)

func main() {
	fmt.Println("Starting QueryNode Segment Routing Race Condition Simulation...")

	replica := segments.NewReplica()

	// Initial state: two segments with 500 entities each (total 1000)
	seg1 := segments.NewSegment(1, 500, func() {
		fmt.Println("Segment 1 resources released")
	})
	seg2 := segments.NewSegment(2, 500, func() {
		fmt.Println("Segment 2 resources released")
	})
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
				newSegID := atomic.AddInt64(&version, 1)
				newSeg := segments.NewSegment(newSegID, 1000, func() {
					// fmt.Printf("Segment %d resources released\n", newSegID)
				})

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
		for i := 0; i < 10000; i++ {
			segmentsList := replica.GetSegments()
			
			var totalEntities int64
			for _, s := range segmentsList {
				totalEntities += s.EntityCount
			}

			// Release the segments after checking
			for _, s := range segmentsList {
				s.Unref()
			}

			// Assert that the total entities never drop below the expected 1000
			if totalEntities < 1000 {
				panic(fmt.Sprintf("Race condition detected! Total entities dropped to %d", totalEntities))
			}
		}
	}()

	// Run the simulation for a bit
	time.Sleep(200 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	fmt.Println("Simulation completed successfully! No race conditions detected.")
}
