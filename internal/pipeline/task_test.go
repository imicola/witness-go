package pipeline

import (
	"errors"
	"witness-go/internal/scene"
	"testing"
	"time"
)

var testEpoch = time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
var testIdentity = scene.Identity{Target: "A", RunID: "one"}

func testSample(s int) scene.Sample {
	return scene.Sample{Identity: testIdentity, At: testEpoch.Add(time.Duration(s) * time.Second), CPU: &scene.CPU{UsageUS: 7}}
}
func newTestTask(t *testing.T, cap int) (*Ring, *Task) {
	t.Helper()
	r, err := NewRing(3)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []int{0, 10, 20} {
		if err := r.Append(testSample(s)); err != nil {
			t.Fatal(err)
		}
	}
	task, err := Start(r, scene.Event{Identity: testIdentity, ReceivedAt: testEpoch.Add(20 * time.Second)}, scene.Window{Start: testEpoch, End: testEpoch.Add(40 * time.Second), MaxGap: 10 * time.Second}, cap)
	if err != nil {
		t.Fatal(err)
	}
	return r, task
}

func TestStartRejectsNegativeCapacity(t *testing.T) {
	defer func() {
		if x := recover(); x != nil {
			t.Errorf("Start panicked rather than returning error: %v", x)
		}
	}()
	r, err := NewRing(1)
	if err != nil {
		t.Fatal(err)
	}
	task, err := Start(r, scene.Event{}, scene.Window{}, -1)
	if task != nil {
		t.Fatal("Start returned a task on failure")
	}
	if err == nil {
		t.Fatal("expected error")
	}
}
func TestOfferIgnoresOutsideWindowWhenFull(t *testing.T) {
	_, task := newTestTask(t, 3)
	if err := task.Offer(testSample(50)); err != nil {
		t.Fatalf("out-of-window testSample should be ignored, got %v", err)
	}
	if task.failure != nil || len(task.samples) != 3 {
		t.Fatal("ignored testSample changed task")
	}
}
func TestOfferRetainsInvalidSampleError(t *testing.T) {
	for _, kind := range []string{"identity", "time"} {
		t.Run(kind, func(t *testing.T) {
			_, task := newTestTask(t, 5)
			s := testSample(30)
			if kind == "identity" {
				s.Identity.RunID = ""
			} else {
				s.At = time.Time{}
			}
			err := task.Offer(s)
			if err == nil {
				t.Fatal("invalid testSample accepted/ignored")
			}
			if task.failure == nil {
				t.Fatal("failure not retained")
			}
			_, later := task.Finish(testEpoch.Add(40 * time.Second))
			if !errors.Is(later, err) {
				t.Fatal("Finish returned a different error from Offer")
			}
		})
	}
}
func TestStartCopiesSourceTime(t *testing.T) {
	r, err := NewRing(1)
	if err != nil {
		t.Fatal(err)
	}
	source := testEpoch
	event := scene.Event{Identity: testIdentity, ReceivedAt: testEpoch, SourceTime: &source}
	task, err := Start(r, event, scene.Window{Start: testEpoch, End: testEpoch.Add(40 * time.Second), MaxGap: 10 * time.Second}, 5)
	if err != nil {
		t.Fatal(err)
	}
	source = source.Add(time.Hour)
	if !task.event.SourceTime.Equal(testEpoch) {
		t.Fatal("caller changed task source time")
	}
}
func TestTaskPreservesWindowAcrossBufferOverwrite(t *testing.T) {
	r, task := newTestTask(t, 5)
	if _, err := task.Finish(testEpoch.Add(39 * time.Second)); !errors.Is(err, ErrNotReady) {
		t.Fatal(err)
	}
	for _, sec := range []int{30, 40} {
		s := testSample(sec)
		if err := r.Append(s); err != nil {
			t.Fatal(err)
		}
		if err := task.Offer(s); err != nil {
			t.Fatal(err)
		}
		s.CPU.UsageUS = 99
	}
	out, err := task.Finish(testEpoch.Add(40 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Samples) != 5 || len(out.Gaps) != 0 {
		t.Fatal(out)
	}
	if out.Samples[3].CPU.UsageUS != 7 {
		t.Fatal("Offer CPU alias")
	}
	out.Samples[0].CPU.UsageUS = 88
	if task.samples[0].CPU.UsageUS != 7 {
		t.Fatal("Finish result alias")
	}
	if err := task.Offer(testSample(40)); !errors.Is(err, ErrFinished) {
		t.Fatal(err)
	}
	if _, err := task.Finish(testEpoch.Add(40 * time.Second)); !errors.Is(err, ErrFinished) {
		t.Fatal(err)
	}
	snap := r.Snapshot()
	if len(snap) != 3 || !snap[0].At.Equal(testSample(20).At) {
		t.Fatal("ring coverage")
	}
	snap[0].CPU.UsageUS = 99
	if r.Snapshot()[0].CPU.UsageUS != 7 {
		t.Fatal("snapshot alias")
	}
}
func TestTaskRetainsCapacityFailure(t *testing.T) {
	_, task := newTestTask(t, 3)
	if err := task.Offer(testSample(30)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if _, err := task.Finish(testEpoch.Add(40 * time.Second)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
}
