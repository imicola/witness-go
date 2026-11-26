package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
	"witness-go/internal/scene"
)

type exportedRecord struct {
	event scene.Event
	sel   scene.Selection
}

type fakeExporter struct {
	records []exportedRecord
	failErr error
}

func (f *fakeExporter) Write(ctx context.Context, ev scene.Event, sel scene.Selection) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.failErr != nil {
		err := f.failErr
		f.failErr = nil
		return err
	}
	f.records = append(f.records, exportedRecord{
		event: cloneEvent(ev),
		sel:   sel,
	})
	return nil
}

func coordBaseTime() time.Time {
	return time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
}

func coordSample(base time.Time, offsetSec int, id scene.Identity, usage, throttled uint64) scene.Sample {
	return scene.Sample{
		Identity: id,
		At:       base.Add(time.Duration(offsetSec) * time.Second),
		CPU: &scene.CPU{
			UsageUS:     usage,
			ThrottledUS: throttled,
		},
	}
}

func coordEvent(base time.Time, offsetSec int, key string, id scene.Identity) scene.Event {
	return scene.Event{
		Key:        key,
		Identity:   id,
		ReceivedAt: base.Add(time.Duration(offsetSec) * time.Second),
	}
}

func newTestCoordinator(t *testing.T, ringCap int, cfg CoordinatorConfig) (*Coordinator, *fakeExporter) {
	t.Helper()
	ring, err := NewRing(ringCap)
	if err != nil {
		t.Fatalf("NewRing failed: %v", err)
	}
	exp := &fakeExporter{}
	coord, err := NewCoordinator(ring, exp, cfg)
	if err != nil {
		t.Fatalf("NewCoordinator failed: %v", err)
	}
	return coord, exp
}

func TestCoordinatorFullWindowLifecycle(t *testing.T) {
	ctx := context.Background()
	base := coordBaseTime()
	id := scene.Identity{Target: "svc-a", RunID: "run-1"}

	coord, exp := newTestCoordinator(t, 3, CoordinatorConfig{
		PreWindow:      20 * time.Second,
		PostWindow:     20 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 5,
		MaxSeenKeys:    8,
	})

	// Pre-window samples: 00s, 10s, 20s fill the capacity-3 ring.
	for _, sec := range []int{0, 10, 20} {
		if err := coord.OnSample(ctx, coordSample(base, sec, id, uint64(sec*100), uint64(sec*10))); err != nil {
			t.Fatalf("OnSample(%ds) error: %v", sec, err)
		}
	}

	// Alert arrives at 20s -> window [00s, 40s].
	dec, err := coord.OnEvent(ctx, coordEvent(base, 20, "alert-cpu-1", id))
	if err != nil || dec != DecisionStarted {
		t.Fatalf("OnEvent got (%q, %v), want (%q, nil)", dec, err, DecisionStarted)
	}
	if !coord.HasActiveTask() {
		t.Fatalf("expected active task after DecisionStarted")
	}

	// Post-window samples 30s and 40s overwrite 00s and 10s in the ring,
	// while the active task must retain all five points [00..40].
	if err := coord.OnSample(ctx, coordSample(base, 30, id, 3000, 300)); err != nil {
		t.Fatalf("OnSample(30s) error: %v", err)
	}
	exported, err := coord.Tick(ctx, base.Add(39*time.Second))
	if err != nil || exported {
		t.Fatalf("Tick(39s) got (%v, %v), want (false, nil)", exported, err)
	}
	if err := coord.OnSample(ctx, coordSample(base, 40, id, 4000, 400)); err != nil {
		t.Fatalf("OnSample(40s) error: %v", err)
	}
	exported, err = coord.Tick(ctx, base.Add(40*time.Second))
	if err != nil || !exported {
		t.Fatalf("Tick(40s) got (%v, %v), want (true, nil)", exported, err)
	}
	if coord.HasActiveTask() {
		t.Fatalf("expected no active task after export")
	}
	if !coord.HasSeen("alert-cpu-1") {
		t.Fatalf("expected alert-cpu-1 marked seen after successful export")
	}
	if len(exp.records) != 1 {
		t.Fatalf("expected 1 export record, got %d", len(exp.records))
	}
	gotSel := exp.records[0].sel
	if len(gotSel.Samples) != 5 || len(gotSel.Gaps) != 0 {
		t.Fatalf("got %d samples and %d gaps, want 5 samples and 0 gaps", len(gotSel.Samples), len(gotSel.Gaps))
	}
}

func TestCoordinatorBusyRejectsDifferentKeyWhileActive(t *testing.T) {
	ctx := context.Background()
	base := coordBaseTime()
	id := scene.Identity{Target: "svc-a", RunID: "run-1"}

	coord, exp := newTestCoordinator(t, 4, CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 4,
		MaxSeenKeys:    4,
	})

	if err := coord.OnSample(ctx, coordSample(base, 10, id, 100, 10)); err != nil {
		t.Fatalf("OnSample(10s) error: %v", err)
	}
	dec, err := coord.OnEvent(ctx, coordEvent(base, 10, "key-1", id))
	if err != nil || dec != DecisionStarted {
		t.Fatalf("key-1 OnEvent got (%q, %v), want started", dec, err)
	}

	// Different event key while key-1 is active must return DecisionBusy and preserve key-1.
	dec, err = coord.OnEvent(ctx, coordEvent(base, 15, "key-2", id))
	if err != nil || dec != DecisionBusy {
		t.Fatalf("key-2 OnEvent while busy got (%q, %v), want (%q, nil)", dec, err, DecisionBusy)
	}

	if err := coord.OnSample(ctx, coordSample(base, 20, id, 200, 20)); err != nil {
		t.Fatalf("OnSample(20s) error: %v", err)
	}
	if exported, err := coord.Tick(ctx, base.Add(20*time.Second)); err != nil || !exported {
		t.Fatalf("Tick(20s) got (%v, %v), want (true, nil)", exported, err)
	}
	if len(exp.records) != 1 || exp.records[0].event.Key != "key-1" {
		t.Fatalf("unexpected exported records: %+v", exp.records)
	}

	// After key-1 completes, key-2 can be accepted.
	dec, err = coord.OnEvent(ctx, coordEvent(base, 25, "key-2", id))
	if err != nil || dec != DecisionStarted {
		t.Fatalf("key-2 after completion got (%q, %v), want started", dec, err)
	}
}

func TestCoordinatorSeenKeyCapacityEviction(t *testing.T) {
	ctx := context.Background()
	base := coordBaseTime()
	id := scene.Identity{Target: "svc-a", RunID: "run-1"}

	coord, _ := newTestCoordinator(t, 4, CoordinatorConfig{
		PreWindow:      0,
		PostWindow:     0,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 4,
		MaxSeenKeys:    2,
	})

	for i, key := range []string{"k1", "k2", "k3"} {
		sec := (i + 1) * 10
		if err := coord.OnSample(ctx, coordSample(base, sec, id, uint64(sec), 0)); err != nil {
			t.Fatalf("OnSample(%s) error: %v", key, err)
		}
		if dec, err := coord.OnEvent(ctx, coordEvent(base, sec, key, id)); err != nil || dec != DecisionStarted {
			t.Fatalf("OnEvent(%s) got (%q, %v), want started", key, dec, err)
		}
		if exported, err := coord.Tick(ctx, base.Add(time.Duration(sec)*time.Second)); err != nil || !exported {
			t.Fatalf("Tick(%s) got (%v, %v), want (true, nil)", key, exported, err)
		}
	}

	if coord.HasSeen("k1") {
		t.Fatalf("expected oldest key k1 to be evicted when MaxSeenKeys=2")
	}
	if !coord.HasSeen("k2") || !coord.HasSeen("k3") {
		t.Fatalf("expected k2 and k3 to remain in seen table")
	}
}

func TestCoordinatorOfferCapacityFailureReleasesActiveTask(t *testing.T) {
	ctx := context.Background()
	base := coordBaseTime()
	id := scene.Identity{Target: "svc-a", RunID: "run-1"}

	coord, exp := newTestCoordinator(t, 4, CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     20 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 1,
		MaxSeenKeys:    4,
	})

	if err := coord.OnSample(ctx, coordSample(base, 10, id, 100, 10)); err != nil {
		t.Fatalf("OnSample(10s) error: %v", err)
	}
	if dec, err := coord.OnEvent(ctx, coordEvent(base, 10, "cap-key", id)); err != nil || dec != DecisionStarted {
		t.Fatalf("OnEvent got (%q, %v), want started", dec, err)
	}

	// Second sample exceeds MaxTaskSamples=1 -> must fail with ErrCapacity, release active, and NOT mark seen.
	err := coord.OnSample(ctx, coordSample(base, 20, id, 200, 20))
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("OnSample(20s) error = %v, want ErrCapacity", err)
	}
	if coord.HasActiveTask() {
		t.Fatalf("expected active task cleared after Offer failure")
	}
	if coord.HasSeen("cap-key") {
		t.Fatalf("failed task must not mark cap-key as seen")
	}
	if len(exp.records) != 0 {
		t.Fatalf("expected 0 exports, got %d", len(exp.records))
	}
}

func TestLearnerCoordinatorCounterexample(t *testing.T) {
	// TODO(learner): 编写至少一条能击穿错误 Coordinator 实现的反例测试，例如：
	// 场景 A（重复通知去重）：同一个 Event.Key 在后窗收集期间再次到达、以及导出完成后再次到达，
	//   都应返回 DecisionDuplicate，不重置活动任务，最终 Exporter 只被调用一次且保留完整前后窗。
	// 场景 B（导出失败不误吞重试）：首次 Tick 时 fakeExporter.failErr 注入写盘错误，
	//   验证 Tick 返回错误、active 被清理（不会永久 Busy）、Event.Key 未被记入 seen；
	//   随后同一 Event.Key 重试到达时能重新 DecisionStarted 并成功导出一次。
	// t.Fatal("TODO(learner): implement counterexample test for dedup and/or export-failure retry")
	ctx := context.Background()
	base1 := coordBaseTime()
	id1 := scene.Identity{Target: "svc-a", RunID: "run-1"}

	coord, exp := newTestCoordinator(t, 4, CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 4,
		MaxSeenKeys:    4,
	})

	// 场景 A：重复通知去重
	// 前置窗样本 00s、10s 进 ring；10s 告警开启窗口 [00s, 20s]。
	for _, sec := range []int{0, 10} {
		if err := coord.OnSample(ctx, coordSample(base1, sec, id1, uint64(sec*100), uint64(sec*10))); err != nil {
			t.Fatalf("OnSample(%ds) error: %v", sec, err)
		}
	}
	if dec, err := coord.OnEvent(ctx, coordEvent(base1, 10, "cap-key", id1)); err != nil || dec != DecisionStarted {
		t.Fatalf("OnEvent(10s) got (%q, %v), want started", dec, err)
	}
	// 后窗收集期间同一 key 重发：必须判重，且不得打断活动任务。
	if dec, err := coord.OnEvent(ctx, coordEvent(base1, 15, "cap-key", id1)); err != nil || dec != DecisionDuplicate {
		t.Fatalf("OnEvent(15s duplicate) got (%q, %v), want duplicate", dec, err)
	}
	if !coord.HasActiveTask() {
		t.Fatalf("expected active task preserved after duplicate event during post-window")
	}
	// 20s 后窗样本补到窗口末端；[00s, 20s] 共 3 个样本，间隔均为 10s = MaxGap，无缺口。
	if err := coord.OnSample(ctx, coordSample(base1, 20, id1, 2000, 200)); err != nil {
		t.Fatalf("OnSample(20s) error: %v", err)
	}
	if exported, err := coord.Tick(ctx, base1.Add(20*time.Second)); err != nil || !exported {
		t.Fatalf("Tick(20s) got (%v, %v), want (true, nil)", exported, err)
	}
	if coord.HasActiveTask() {
		t.Fatalf("expected no active task after export")
	}
	if !coord.HasSeen("cap-key") {
		t.Fatalf("expected cap-key marked seen after successful export")
	}
	if len(exp.records) != 1 {
		t.Fatalf("expected 1 export record, got %d", len(exp.records))
	}
	getSel := exp.records[0].sel
	if len(getSel.Samples) != 3 || len(getSel.Gaps) != 0 {
		t.Fatalf("got %d samples and %d gaps, want 3 samples and 0 gaps", len(getSel.Samples), len(getSel.Gaps))
	}
	// 导出后同一 key 再次到达：仍判重，且不得产生第二次导出。
	if dec, err := coord.OnEvent(ctx, coordEvent(base1, 25, "cap-key", id1)); err != nil || dec != DecisionDuplicate {
		t.Fatalf("OnEvent(25s after export) got (%q, %v), want duplicate", dec, err)
	}
	if len(exp.records) != 1 {
		t.Fatalf("duplicate after export must not export again, got %d records", len(exp.records))
	}

	// 场景 B：导出失败不误吞重试
	coord2, exp2 := newTestCoordinator(t, 4, CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 4,
		MaxSeenKeys:    4,
	})
	exp2.failErr = errors.New("simulated export failure")
	if err := coord2.OnSample(ctx, coordSample(base1, 10, id1, 100, 10)); err != nil {
		t.Fatalf("OnSample(10s) error: %v", err)
	}
	if dec, err := coord2.OnEvent(ctx, coordEvent(base1, 10, "cap-key", id1)); err != nil || dec != DecisionStarted {
		t.Fatalf("OnEvent got (%q, %v), want started", dec, err)
	}
	if exported, err := coord2.Tick(ctx, base1.Add(20*time.Second)); err == nil || exported {
		t.Fatalf("Tick got (%v, %v), want (false, error)", exported, err)
	}
	if coord2.HasActiveTask() {
		t.Fatalf("expected active task cleared after failed export")
	}
	if coord2.HasSeen("cap-key") {
		t.Fatalf("failed task must not mark cap-key as seen")
	}
	if len(exp2.records) != 0 {
		t.Fatalf("expected 0 exports, got %d", len(exp2.records))
	}
	if dec, err := coord2.OnEvent(ctx, coordEvent(base1, 30, "cap-key", id1)); err != nil || dec != DecisionStarted {
		t.Fatalf("OnEvent got (%q, %v), want started", dec, err)
	}
	if exported, err := coord2.Tick(ctx, base1.Add(40*time.Second)); err != nil || !exported {
		t.Fatalf("Tick got (%v, %v), want (true, nil)", exported, err)
	}
	if !coord2.HasSeen("cap-key") {
		t.Fatalf("successful task must mark cap-key as seen")
	}
	if len(exp2.records) != 1 {
		t.Fatalf("expected 1 export record after retry, got %d", len(exp2.records))
	}

}
