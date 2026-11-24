package pipeline

import (
	"testing"
)

func TestRingSnapshotOrderAndOwnership(t *testing.T) {
	ring, err := NewRing(3)
	if err != nil {
		t.Fatal(err)
	}
	if got := ring.Snapshot(); len(got) != 0 {
		t.Fatalf("new ring returned %d samples", len(got))
	}
	for _, second := range []int{0, 10, 20, 30} {
		sample := testSample(second)
		if err := ring.Append(sample); err != nil {
			t.Fatal(err)
		}
		// The caller retains its own sample; changing it must not alter the ring.
		sample.CPU.UsageUS = 99
	}
	snapshot := ring.Snapshot()
	if len(snapshot) != 3 {
		t.Fatalf("snapshot length = %d, want 3", len(snapshot))
	}
	for i, second := range []int{10, 20, 30} {
		if !snapshot[i].At.Equal(testSample(second).At) || snapshot[i].CPU.UsageUS != 7 {
			t.Fatalf("snapshot[%d] = %+v, want second %d and original CPU", i, snapshot[i], second)
		}
	}
	snapshot[0].At = testEpoch
	snapshot[0].CPU.UsageUS = 88
	again := ring.Snapshot()
	if !again[0].At.Equal(testSample(10).At) || again[0].CPU.UsageUS != 7 {
		t.Fatal("modifying a snapshot changed the ring")
	}
}
