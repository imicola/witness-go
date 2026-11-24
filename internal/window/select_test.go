package window

import (
	"witness-go/internal/scene"
	"testing"
	"time"
)

func TestLearnerCounterexample(t *testing.T) {
	// t.Skip("待本人选择一个坏实现并编写能击穿它的输入及断言；不要仅取消 Skip")
	samples := []scene.Sample{
		{At: time.Unix(0, 0), Identity: scene.Identity{Target: "A", RunID: "1"}},
		{At: time.Unix(10, 0), Identity: scene.Identity{Target: "A", RunID: "2"}},
		{At: time.Unix(20, 0), Identity: scene.Identity{Target: "A", RunID: "1"}},
	}
	maxGap := 10 * time.Second
	requested := scene.Window{
		Start:  time.Unix(0, 0),
		End:    time.Unix(20, 0),
		MaxGap: maxGap,
	}
	event := scene.Event{
		Key:        "test_event",
		Identity:   scene.Identity{Target: "A", RunID: "1"},
		ReceivedAt: time.Unix(5, 0),
	}
	selection, err := Select(samples, event, requested)
	if err != nil {
		t.Fatalf("Select returned error: %v", err)
	}
	if selection.First == nil || !selection.First.Equal(time.Unix(0, 0)) {
		t.Errorf("Expected first sample at 0, got %v", selection.First)
	}
	if selection.Last == nil || !selection.Last.Equal(time.Unix(20, 0)) {
		t.Errorf("Expected last sample at 20, got %v", selection.Last)
	}
	if len(selection.Samples) != 2 {
		t.Errorf("Expected 2 samples, got %d", len(selection.Samples))
	}
	if len(selection.Gaps) != 1 {
		t.Errorf("Expected 1 gap, got %d", len(selection.Gaps))
	} else {
		gap := selection.Gaps[0]
		if gap.Kind != "internal" || !gap.From.Equal(time.Unix(0, 0)) || !gap.To.Equal(time.Unix(20, 0)) {
			t.Errorf("Unexpected gap: %+v", gap)
		}
	}
}
