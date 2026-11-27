package pipeline

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
	"witness-go/internal/scene"
)

type syncExporter struct {
	mu         sync.Mutex
	records    []exportedRecord
	exportedCh chan exportedRecord
}

func newSyncExporter() *syncExporter {
	return &syncExporter{
		exportedCh: make(chan exportedRecord, 8),
	}
}

func (e *syncExporter) Write(ctx context.Context, ev scene.Event, sel scene.Selection) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rec := exportedRecord{
		event: cloneEvent(ev),
		sel:   sel,
	}
	e.mu.Lock()
	e.records = append(e.records, rec)
	e.mu.Unlock()
	e.exportedCh <- rec
	return nil
}

func (e *syncExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.records)
}

type sampleStep struct {
	sample scene.Sample
	err    error
}

type scriptedSampler struct {
	mu    sync.Mutex
	steps []sampleStep
	idx   int
}

func (s *scriptedSampler) Sample(ctx context.Context) (scene.Sample, error) {
	if err := ctx.Err(); err != nil {
		return scene.Sample{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.steps) {
		return scene.Sample{}, errors.New("scripted sampler exhausted")
	}
	step := s.steps[s.idx]
	s.idx++
	return step.sample, step.err
}

type eventStep struct {
	event scene.Event
	err   error
}

type chanEventSource struct {
	ch chan eventStep
}

func newChanEventSource(buf int) *chanEventSource {
	return &chanEventSource{ch: make(chan eventStep, buf)}
}

func (s *chanEventSource) Next(ctx context.Context) (scene.Event, error) {
	select {
	case <-ctx.Done():
		return scene.Event{}, ctx.Err()
	case step, ok := <-s.ch:
		if !ok {
			return scene.Event{}, io.EOF
		}
		if step.err != nil {
			return scene.Event{}, step.err
		}
		return step.event, nil
	}
}

type stageError struct {
	stage string
	err   error
}

func TestRunnerDrivesSampleEventAndExportUnderRace(t *testing.T) {
	base := coordBaseTime()
	id := scene.Identity{Target: "svc-a", RunID: "run-1"}

	ring, err := NewRing(8)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	exp := newSyncExporter()
	coord, err := NewCoordinator(ring, exp, CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 8,
		MaxSeenKeys:    8,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	sampler := &scriptedSampler{
		steps: []sampleStep{
			{sample: coordSample(base, 0, id, 100, 10)},
			{sample: coordSample(base, 10, id, 200, 20)},
			{sample: coordSample(base, 20, id, 300, 30)},
		},
	}
	events := newChanEventSource(4)
	tickCh := make(chan time.Time)
	decCh := make(chan EventDecision, 4)
	errCh := make(chan stageError, 4)

	runner, err := NewRunner(coord, sampler, events, RunnerConfig{
		TickCh: tickCh,
		Now:    func() time.Time { return base },
		OnDecision: func(_ scene.Event, dec EventDecision) {
			decCh <- dec
		},
		OnError: func(stage string, err error) {
			errCh <- stageError{stage: stage, err: err}
		},
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- runner.Run(ctx)
	}()

	// Pre-window ticks: 00s and 10s.
	tickCh <- base
	tickCh <- base.Add(10 * time.Second)

	// Alert at 10s -> window [00s, 20s]. Wait for OnEvent decision before next tick.
	events.ch <- eventStep{event: coordEvent(base, 10, "alert-1", id)}
	select {
	case dec := <-decCh:
		if dec != DecisionStarted {
			t.Fatalf("event decision = %q, want %q", dec, DecisionStarted)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event decision")
	}

	// Post-window tick at 20s -> must deliver sample 20s before Tick(20s) and export all 3 points.
	tickCh <- base.Add(20 * time.Second)

	select {
	case rec := <-exp.exportedCh:
		if rec.event.Key != "alert-1" {
			t.Fatalf("exported event key = %q, want alert-1", rec.event.Key)
		}
		if len(rec.sel.Samples) != 3 || len(rec.sel.Gaps) != 0 {
			t.Fatalf("exported %d samples and %d gaps, want 3 samples and 0 gaps", len(rec.sel.Samples), len(rec.sel.Gaps))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for export")
	}

	cancel()
	select {
	case err := <-runErrCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Run shutdown")
	}

	select {
	case se := <-errCh:
		t.Fatalf("unexpected non-fatal stage error: %+v", se)
	default:
	}
}

func TestRunnerTransientSampleErrorDoesNotKillLoop(t *testing.T) {
	base := coordBaseTime()
	id := scene.Identity{Target: "svc-a", RunID: "run-1"}

	ring, err := NewRing(8)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	exp := newSyncExporter()
	coord, err := NewCoordinator(ring, exp, CoordinatorConfig{
		PreWindow:      0,
		PostWindow:     20 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 8,
		MaxSeenKeys:    8,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	sampleBoom := errors.New("transient /proc read failure")
	sampler := &scriptedSampler{
		steps: []sampleStep{
			{sample: coordSample(base, 0, id, 100, 10)},
			{err: sampleBoom},
			{sample: coordSample(base, 20, id, 300, 30)},
		},
	}
	events := newChanEventSource(2)
	tickCh := make(chan time.Time)
	decCh := make(chan EventDecision, 2)
	errCh := make(chan stageError, 4)

	var clockMu sync.Mutex
	curNow := base
	setNow := func(tm time.Time) {
		clockMu.Lock()
		curNow = tm
		clockMu.Unlock()
	}
	getNow := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return curNow
	}

	runner, err := NewRunner(coord, sampler, events, RunnerConfig{
		TickCh: tickCh,
		Now:    getNow,
		OnDecision: func(_ scene.Event, dec EventDecision) {
			decCh <- dec
		},
		OnError: func(stage string, err error) {
			errCh <- stageError{stage: stage, err: err}
		},
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- runner.Run(ctx)
	}()

	// 00s sample succeeds; alert opens [00s, 20s].
	tickCh <- base
	events.ch <- eventStep{event: coordEvent(base, 0, "alert-gap", id)}
	select {
	case dec := <-decCh:
		if dec != DecisionStarted {
			t.Fatalf("decision = %q, want started", dec)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for decision")
	}

	// 10s sample fails -> Runner must report "sample" error and stay alive.
	setNow(base.Add(10 * time.Second))
	tickCh <- base.Add(10 * time.Second)
	select {
	case se := <-errCh:
		if se.stage != "sample" || !errors.Is(se.err, sampleBoom) {
			t.Fatalf("got stage error %+v, want stage=sample err=%v", se, sampleBoom)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for sample error callback")
	}

	// 20s sample succeeds -> window finishes with an honest internal gap [00s, 20s].
	setNow(base.Add(20 * time.Second))
	tickCh <- base.Add(20 * time.Second)
	select {
	case rec := <-exp.exportedCh:
		if len(rec.sel.Samples) != 2 {
			t.Fatalf("got %d samples, want 2 (00s and 20s)", len(rec.sel.Samples))
		}
		if len(rec.sel.Gaps) != 1 || rec.sel.Gaps[0].Kind != "internal" {
			t.Fatalf("expected 1 internal gap for missing 10s sample, got %+v", rec.sel.Gaps)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for export after transient sample error")
	}

	cancel()
	select {
	case err := <-runErrCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Run shutdown")
	}
}

func TestRunnerEventSourceErrorCancelsAndReturns(t *testing.T) {
	base := coordBaseTime()
	ring, err := NewRing(4)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	exp := newSyncExporter()
	coord, err := NewCoordinator(ring, exp, CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 4,
		MaxSeenKeys:    4,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	fatalErr := errors.New("notify-receiver listener failed")
	events := newChanEventSource(1)
	events.ch <- eventStep{err: fatalErr}

	runner, err := NewRunner(coord, &scriptedSampler{}, events, RunnerConfig{
		TickCh: make(chan time.Time),
		Now:    func() time.Time { return base },
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	err = runner.Run(context.Background())
	if !errors.Is(err, fatalErr) {
		t.Fatalf("Run error = %v, want %v", err, fatalErr)
	}
}

// delayedCancelSource blocks inside Next until the runner cancels its context.
// The short sleep after cancellation makes an early Run return observable: a
// runner that does not join its reader goroutine will return before this source
// closes exited.
type delayedCancelSource struct {
	entered  chan struct{}
	exited   chan struct{}
	enterOne sync.Once
	exitOne  sync.Once
}

func newDelayedCancelSource() *delayedCancelSource {
	return &delayedCancelSource{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
}

func (s *delayedCancelSource) Next(ctx context.Context) (scene.Event, error) {
	s.enterOne.Do(func() { close(s.entered) })
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)
	s.exitOne.Do(func() { close(s.exited) })
	return scene.Event{}, ctx.Err()
}

// TestLearnerRunnerCounterexample 用来击穿常见的错误 Go 并发 Runner 实现，建议覆盖以下场景之一或全部：
//
// 场景 A（取消不泄漏、不早退）：
//
//	构造一个自定义 EventSource，在 Next(ctx) 内部先阻塞等待 <-ctx.Done()，
//	醒来后稍微停顿（如 20ms）再关闭一个 readerExited 通道并返回 ctx.Err()。
//	调用 cancel() 停止 Runner 后，验证：
//	1. Run 返回 context.Canceled；
//	2. Run 返回的瞬间，readerExited 已经处于关闭状态（证明 Run 确实等待了读取协程退出，没有早退或泄漏）。
//
// 场景 B（EventSource 返回 io.EOF 后不空转也不丢弃后窗导出）：
//
//	EventSource 发出一条 10s 告警后立即关闭通道返回 io.EOF；
//	验证 Runner 不会因此提前退出或反复收到空事件报错，随后注入 20s 节拍仍能正常完成导出，最后 cancel() 干净退出。
func TestLearnerRunnerCounterexample(t *testing.T) {
	t.Run("cancel joins event reader", func(t *testing.T) {
		base := coordBaseTime()
		ring, err := NewRing(4)
		if err != nil {
			t.Fatalf("NewRing: %v", err)
		}
		coord, err := NewCoordinator(ring, newSyncExporter(), CoordinatorConfig{
			PreWindow:      10 * time.Second,
			PostWindow:     10 * time.Second,
			MaxGap:         10 * time.Second,
			MaxTaskSamples: 4,
			MaxSeenKeys:    4,
		})
		if err != nil {
			t.Fatalf("NewCoordinator: %v", err)
		}

		src := newDelayedCancelSource()
		runner, err := NewRunner(coord, &scriptedSampler{}, src, RunnerConfig{
			TickCh: make(chan time.Time),
			Now:    func() time.Time { return base },
		})
		if err != nil {
			t.Fatalf("NewRunner: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- runner.Run(ctx)
		}()

		// Wait until the reader goroutine is actually blocked in Next.
		select {
		case <-src.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("event reader did not enter Next")
		}

		cancel()
		select {
		case err := <-runErrCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Run did not return after cancel")
		}

		// Run's defer must wait for the reader goroutine, which closes exited
		// before it returns. A default here means Run returned early.
		select {
		case <-src.exited:
		default:
			t.Fatalf("Run returned before the event reader goroutine exited")
		}
	})

	t.Run("io.EOF keeps runner alive for post-window export", func(t *testing.T) {
		base := coordBaseTime()
		id := scene.Identity{Target: "svc-a", RunID: "run-1"}

		ring, err := NewRing(8)
		if err != nil {
			t.Fatalf("NewRing: %v", err)
		}
		exp := newSyncExporter()
		coord, err := NewCoordinator(ring, exp, CoordinatorConfig{
			PreWindow:      10 * time.Second,
			PostWindow:     10 * time.Second,
			MaxGap:         10 * time.Second,
			MaxTaskSamples: 8,
			MaxSeenKeys:    8,
		})
		if err != nil {
			t.Fatalf("NewCoordinator: %v", err)
		}

		sampler := &scriptedSampler{
			steps: []sampleStep{
				{sample: coordSample(base, 0, id, 100, 10)},
				{sample: coordSample(base, 10, id, 200, 20)},
				{sample: coordSample(base, 20, id, 300, 30)},
			},
		}
		events := newChanEventSource(1)
		tickCh := make(chan time.Time)
		decCh := make(chan EventDecision, 4)
		errCh := make(chan stageError, 16)

		runner, err := NewRunner(coord, sampler, events, RunnerConfig{
			TickCh: tickCh,
			Now:    func() time.Time { return base },
			OnDecision: func(_ scene.Event, dec EventDecision) {
				decCh <- dec
			},
			OnError: func(stage string, err error) {
				errCh <- stageError{stage: stage, err: err}
			},
		})
		if err != nil {
			t.Fatalf("NewRunner: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		runErrCh := make(chan error, 1)
		go func() {
			runErrCh <- runner.Run(ctx)
		}()

		// Pre-window samples 00s and 10s land in the ring before the alert.
		tickCh <- base
		tickCh <- base.Add(10 * time.Second)

		// Alert at 10s, then the source ends with io.EOF. The runner must not
		// treat EOF as fatal and must keep serving ticks.
		events.ch <- eventStep{event: coordEvent(base, 10, "alert-eof", id)}
		close(events.ch)
		select {
		case dec := <-decCh:
			if dec != DecisionStarted {
				t.Fatalf("event decision = %q, want %q", dec, DecisionStarted)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event decision")
		}

		// Post-window tick at 20s finishes [00s, 20s] with all three samples.
		select {
		case tickCh <- base.Add(20 * time.Second):
		case <-time.After(2 * time.Second):
			t.Fatalf("runner stopped consuming ticks after io.EOF")
		}

		select {
		case rec := <-exp.exportedCh:
			if rec.event.Key != "alert-eof" {
				t.Fatalf("exported event key = %q, want alert-eof", rec.event.Key)
			}
			if len(rec.sel.Samples) != 3 || len(rec.sel.Gaps) != 0 {
				t.Fatalf("exported %d samples and %d gaps, want 3 samples and 0 gaps", len(rec.sel.Samples), len(rec.sel.Gaps))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for export after io.EOF")
		}

		cancel()
		select {
		case err := <-runErrCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for Run shutdown")
		}

		// A runner that failed to disable the closed evCh branch would spin and
		// report repeated empty-event errors.
		select {
		case se := <-errCh:
			t.Fatalf("unexpected stage error after io.EOF: %+v", se)
		default:
		}
	})
}
