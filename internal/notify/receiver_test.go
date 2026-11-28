package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
	"witness-go/internal/collector"
	"witness-go/internal/exporter"
	"witness-go/internal/pipeline"
	"witness-go/internal/scene"
)

func TestExtractLogicalTarget(t *testing.T) {
	got, err := ExtractLogicalTarget(N9EPayload{
		TagsMap:     map[string]string{"instance": "http://test-http:8080/healthz"},
		TargetIdent: "fallback-host",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "http://test-http:8080/healthz" {
		t.Fatalf("got %q, want tags_map.instance", got)
	}

	got, err = ExtractLogicalTarget(N9EPayload{TargetIdent: "node-01"})
	if err != nil || got != "node-01" {
		t.Fatalf("fallback failed: got (%q, %v)", got, err)
	}

	if _, err := ExtractLogicalTarget(N9EPayload{}); err == nil {
		t.Fatal("expected error when both instance and target_ident are empty")
	}
}

func TestReceiverSourcePollsAndAdvancesCursor(t *testing.T) {
	var mu sync.Mutex
	all := []ReceiverRecord{
		{
			Seq:           1,
			ReceivedAtUTC: "2026-11-28T10:00:10Z",
			Payload: &N9EPayload{
				ID:               101,
				Hash:             "alert-hash-other",
				IsRecovered:      false,
				FirstTriggerTime: 1795831200,
				TriggerTime:      1795831200,
				TagsMap:          map[string]string{"instance": "http://other-svc:8080/healthz"},
			},
		},
		{
			Seq:           2,
			ReceivedAtUTC: "2026-11-28T10:00:12Z",
			Payload: &N9EPayload{
				ID:               102,
				Hash:             "alert-hash-main",
				IsRecovered:      true, // recovery must be skipped
				FirstTriggerTime: 1795831100,
				TriggerTime:      1795831100,
				TagsMap:          map[string]string{"instance": "http://test-http:8080/healthz"},
			},
		},
		{
			Seq:           3,
			ReceivedAtUTC: "2026-11-28T10:00:20Z",
			Payload: &N9EPayload{
				ID:               103,
				Hash:             "alert-hash-main",
				IsRecovered:      false,
				FirstTriggerTime: 1795831215,
				TriggerTime:      1795831215,
				NotifyCurNumber:  1,
				TagsMap:          map[string]string{"instance": "http://test-http:8080/healthz"},
			},
		},
	}

	var seenQueries []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		since, _ := strconv.ParseInt(r.URL.Query().Get("since_seq"), 10, 64)
		mu.Lock()
		seenQueries = append(seenQueries, since)
		var out []ReceiverRecord
		for _, rec := range all {
			if rec.Seq > since {
				out = append(out, rec)
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(eventsEnvelope{Count: len(out), Events: out})
	}))
	defer srv.Close()

	src, err := NewReceiverSource(ReceiverConfig{
		BaseURL:      srv.URL,
		PollInterval: 10 * time.Millisecond,
		Client:       srv.Client(),
		Resolve: func(_ context.Context, logicalTarget string) (scene.Identity, bool, error) {
			if logicalTarget != "http://test-http:8080/healthz" {
				return scene.Identity{}, false, nil
			}
			return scene.Identity{Target: logicalTarget, RunID: "cid-abc:pid-42"}, true, nil
		},
	})
	if err != nil {
		t.Fatalf("NewReceiverSource failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ev, err := src.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if ev.Key != "alert-hash-main:1795831215" {
		t.Fatalf("event key = %q, want %q", ev.Key, "alert-hash-main:1795831215")
	}
	if ev.Identity.Target != "http://test-http:8080/healthz" || ev.Identity.RunID != "cid-abc:pid-42" {
		t.Fatalf("unexpected identity: %+v", ev.Identity)
	}
	if ev.SourceTime == nil || ev.SourceTime.Unix() != 1795831215 {
		t.Fatalf("unexpected source time: %v", ev.SourceTime)
	}

	// Second Next should poll with since_seq=3 and exit cleanly on ctx cancel.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer waitCancel()
	if _, err := src.Next(waitCtx); err == nil {
		t.Fatal("expected context deadline error when no new events arrive")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seenQueries) < 2 || seenQueries[len(seenQueries)-1] != 3 {
		t.Fatalf("expected cursor to advance to since_seq=3, got queries=%v", seenQueries)
	}
}

func TestOnlinePipelineEndToEndWithReceiverAndLinuxSampler(t *testing.T) {
	dir := t.TempDir()
	cpuMaxPath := filepath.Join(dir, "cpu.max")
	cpuStatPath := filepath.Join(dir, "cpu.stat")
	hostPSIPath := filepath.Join(dir, "cpu.psi")
	outPath := filepath.Join(dir, "live-export.json")

	writeFixture := func(quota, usage, throttled, nr string) {
		t.Helper()
		if err := os.WriteFile(cpuMaxPath, []byte(quota+" 100000\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		stat := "usage_usec " + usage + "\nnr_throttled " + nr + "\nthrottled_usec " + throttled + "\n"
		if err := os.WriteFile(cpuStatPath, []byte(stat), 0o600); err != nil {
			t.Fatal(err)
		}
		psi := "some avg10=1.20 avg60=0.80 avg300=0.50 total=123456\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
		if err := os.WriteFile(hostPSIPath, []byte(psi), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeFixture("30000", "100000", "20000", "5")

	baseTime := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	curTime := baseTime
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return curTime
	}

	var mu sync.Mutex
	var records []ReceiverRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since_seq"), 10, 64)
		mu.Lock()
		var out []ReceiverRecord
		for _, rec := range records {
			if rec.Seq > since {
				out = append(out, rec)
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(eventsEnvelope{Count: len(out), Events: out})
	}))
	defer srv.Close()

	id := scene.Identity{Target: "http://test-http:8080/healthz", RunID: "cid-live:pid-99"}
	rawSampler, err := collector.NewLinuxSampler(collector.SourceConfig{
		IdentityFunc: func(context.Context) (scene.Identity, error) { return id, nil },
		CPUMaxPath:   cpuMaxPath,
		CPUStatPath:  cpuStatPath,
		HostPSIPath:  hostPSIPath,
		Now:          nowFn,
	})
	if err != nil {
		t.Fatalf("NewLinuxSampler: %v", err)
	}
	sampledCh := make(chan scene.Sample, 1)
	sampler := samplerFunc(func(ctx context.Context) (scene.Sample, error) {
		sample, err := rawSampler.Sample(ctx)
		if err == nil {
			sampledCh <- sample
		}
		return sample, err
	})

	src, err := NewReceiverSource(ReceiverConfig{
		BaseURL:      srv.URL,
		PollInterval: 5 * time.Millisecond,
		Client:       srv.Client(),
		Now:          nowFn,
		Resolve: func(_ context.Context, logicalTarget string) (scene.Identity, bool, error) {
			if logicalTarget == id.Target {
				return id, true, nil
			}
			return scene.Identity{}, false, nil
		},
	})
	if err != nil {
		t.Fatalf("NewReceiverSource: %v", err)
	}

	fileExp, err := exporter.NewFileExporter(outPath, "e2e httptest simulation", nowFn)
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	exportedCh := make(chan struct{}, 1)
	wrappedExp := exporterFunc(func(ctx context.Context, ev scene.Event, sel scene.Selection) error {
		if err := fileExp.Write(ctx, ev, sel); err != nil {
			return err
		}
		select {
		case exportedCh <- struct{}{}:
		default:
		}
		return nil
	})

	ring, err := pipeline.NewRing(16)
	if err != nil {
		t.Fatal(err)
	}
	coord, err := pipeline.NewCoordinator(ring, wrappedExp, pipeline.CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         10 * time.Second,
		MaxTaskSamples: 16,
		MaxSeenKeys:    16,
	})
	if err != nil {
		t.Fatal(err)
	}

	tickCh := make(chan time.Time)
	startedCh := make(chan pipeline.EventDecision, 2)
	runner, err := pipeline.NewRunner(coord, sampler, src, pipeline.RunnerConfig{
		TickCh: tickCh,
		Now:    nowFn,
		OnDecision: func(_ scene.Event, dec pipeline.EventDecision) {
			startedCh <- dec
		},
		OnError: func(stage string, err error) {
			t.Errorf("unexpected runner error at %s: %v", stage, err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- runner.Run(runCtx)
	}()

	advanceTick := func(offset time.Duration, usage, throttled, nr string) {
		t.Helper()
		writeFixture("30000", usage, throttled, nr)
		clockMu.Lock()
		curTime = baseTime.Add(offset)
		ts := curTime
		clockMu.Unlock()
		tickCh <- ts
		select {
		case <-sampledCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for sampler at offset %s", offset)
		}
	}

	// Pre-window samples at t=0s and t=10s.
	advanceTick(0, "100000", "20000", "5")
	advanceTick(10*time.Second, "180000", "70000", "12")

	// Alert arrives at t=10s (window [0s, 20s]).
	mu.Lock()
	records = append(records, ReceiverRecord{
		Seq:           1,
		ReceivedAtUTC: baseTime.Add(10 * time.Second).Format(time.RFC3339Nano),
		Payload: &N9EPayload{
			ID:               501,
			Hash:             "cpu-throttle-hash",
			IsRecovered:      false,
			FirstTriggerTime: baseTime.Add(5 * time.Second).Unix(),
			TriggerTime:      baseTime.Add(10 * time.Second).Unix(),
			TagsMap:          map[string]string{"instance": id.Target},
		},
	})
	mu.Unlock()

	select {
	case dec := <-startedCh:
		if dec != pipeline.DecisionStarted {
			t.Fatalf("got decision %q, want started", dec)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for alert event to start window task")
	}

	// Post-window sample at t=20s finishes and triggers atomic export.
	advanceTick(20*time.Second, "260000", "140000", "25")

	select {
	case <-exportedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for export")
	}

	cancel()
	if err := <-runDone; err != context.Canceled {
		t.Fatalf("runner exited with %v, want context.Canceled", err)
	}

	got, err := exporter.ReadFile(outPath)
	if err != nil {
		t.Fatalf("exporter.ReadFile failed: %v", err)
	}
	if len(got.Selection.Samples) != 3 {
		t.Fatalf("exported sample count = %d, want 3", len(got.Selection.Samples))
	}
	if len(got.Selection.Gaps) != 0 {
		t.Fatalf("expected 0 gaps, got %+v", got.Selection.Gaps)
	}
	lastCPU := got.Selection.Samples[2].CPU
	if lastCPU == nil || !lastCPU.DeltaValid || lastCPU.DeltaThrottledUS != 70000 {
		t.Fatalf("unexpected last sample CPU delta: %+v", lastCPU)
	}
}

type samplerFunc func(context.Context) (scene.Sample, error)

func (f samplerFunc) Sample(ctx context.Context) (scene.Sample, error) {
	return f(ctx)
}

type exporterFunc func(context.Context, scene.Event, scene.Selection) error

func (f exporterFunc) Write(ctx context.Context, ev scene.Event, sel scene.Selection) error {
	return f(ctx, ev, sel)
}

// TestLearnerNotifyCounterexample 用反例击穿天真的通知转换/去重实现：
//   - Key 掺入 seq/notify_cur_number/trigger_time：同轮故障的重发通知会得到不同 Key，
//     Coordinator 无法去重，同一现场被重复导出；
//   - 仅用 hash 去重：几分钟后同规则的新一轮故障会被 seen 表误吞，永不触发抓取；
//   - 忽略 is_recovered：恢复通知会被当成新故障触发多余的抓取。
func TestLearnerNotifyCounterexample(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	const target = "http://test-http:8080/healthz"
	const alertHash = "cpu-throttle-hash"
	id := scene.Identity{Target: target, RunID: "cid-abc:pid-42"}
	resolve := func(_ context.Context, logicalTarget string) (scene.Identity, bool, error) {
		if logicalTarget != target {
			return scene.Identity{}, false, nil
		}
		return id, true, nil
	}
	now := func() time.Time { return base }

	// notifyRecord 构造一条触发类通知。同一轮故障的重发通知里，seq、notify_cur_number
	// 和 trigger_time（最近一次评估时间）都会前进，只有 first_trigger_time 稳定不变。
	notifyRecord := func(seq, firstTrigger, trigger, notifyNum int64) ReceiverRecord {
		return ReceiverRecord{
			Seq:           seq,
			ReceivedAtUTC: base.Add(time.Duration(seq) * time.Second).Format(time.RFC3339Nano),
			Payload: &N9EPayload{
				ID:               500 + seq,
				Hash:             alertHash,
				TriggerTime:      trigger,
				FirstTriggerTime: firstTrigger,
				NotifyCurNumber:  notifyNum,
				TagsMap:          map[string]string{"instance": target},
			},
		}
	}

	t.Run("same hash: episode retry dedups, new episode starts", func(t *testing.T) {
		var exports []scene.Event
		exp := exporterFunc(func(_ context.Context, ev scene.Event, _ scene.Selection) error {
			exports = append(exports, ev)
			return nil
		})
		ring, err := pipeline.NewRing(8)
		if err != nil {
			t.Fatal(err)
		}
		coord, err := pipeline.NewCoordinator(ring, exp, pipeline.CoordinatorConfig{
			PreWindow:      2 * time.Second,
			PostWindow:     2 * time.Second,
			MaxGap:         10 * time.Second,
			MaxTaskSamples: 8,
			MaxSeenKeys:    8,
		})
		if err != nil {
			t.Fatal(err)
		}

		const episodeA int64 = 1795831215
		recFirst := notifyRecord(1, episodeA, episodeA, 1)               // 首次触发
		recRetry := notifyRecord(7, episodeA, episodeA+45, 3)            // 同一轮故障的第 3 次重发
		recNewEpisode := notifyRecord(20, episodeA+600, episodeA+600, 1) // 10 分钟后的新一轮故障

		evFirst, ok, err := TranslateRecord(ctx, recFirst, resolve, now)
		if err != nil || !ok {
			t.Fatalf("translate first notify: ok=%v err=%v", ok, err)
		}
		evRetry, ok, err := TranslateRecord(ctx, recRetry, resolve, now)
		if err != nil || !ok {
			t.Fatalf("translate retry notify: ok=%v err=%v", ok, err)
		}
		evNew, ok, err := TranslateRecord(ctx, recNewEpisode, resolve, now)
		if err != nil || !ok {
			t.Fatalf("translate new episode: ok=%v err=%v", ok, err)
		}
		if evRetry.Key != evFirst.Key {
			t.Fatalf("same-episode retry must reuse Key: got %q and %q", evFirst.Key, evRetry.Key)
		}
		if evNew.Key == evFirst.Key {
			t.Fatalf("new episode must get a new Key, got %q twice", evFirst.Key)
		}

		// runEpisode 在事件窗口内投两个样本并把时钟推进到窗口 End，驱动一次完整导出。
		runEpisode := func(ev scene.Event) {
			t.Helper()
			for _, at := range []time.Time{ev.ReceivedAt.Add(-1 * time.Second), ev.ReceivedAt.Add(1 * time.Second)} {
				if err := coord.OnSample(ctx, scene.Sample{Identity: id, At: at}); err != nil {
					t.Fatalf("OnSample(%v): %v", at, err)
				}
			}
			if exported, err := coord.Tick(ctx, ev.ReceivedAt.Add(2*time.Second)); err != nil || !exported {
				t.Fatalf("Tick at window end: exported=%v err=%v", exported, err)
			}
		}

		if dec, err := coord.OnEvent(ctx, evFirst); err != nil || dec != pipeline.DecisionStarted {
			t.Fatalf("first notify: decision=%q err=%v, want %q", dec, err, pipeline.DecisionStarted)
		}
		if dec, err := coord.OnEvent(ctx, evRetry); err != nil || dec != pipeline.DecisionDuplicate {
			t.Fatalf("retry while task active: decision=%q err=%v, want %q", dec, err, pipeline.DecisionDuplicate)
		}
		runEpisode(evFirst)
		if dec, err := coord.OnEvent(ctx, evRetry); err != nil || dec != pipeline.DecisionDuplicate {
			t.Fatalf("retry after export: decision=%q err=%v, want %q", dec, err, pipeline.DecisionDuplicate)
		}
		if len(exports) != 1 {
			t.Fatalf("same episode exported %d times, want exactly 1", len(exports))
		}

		// 仅用 hash 去重的实现在这里会返回 DecisionDuplicate，吞掉新一轮故障。
		if dec, err := coord.OnEvent(ctx, evNew); err != nil || dec != pipeline.DecisionStarted {
			t.Fatalf("new episode: decision=%q err=%v, want %q", dec, err, pipeline.DecisionStarted)
		}
		runEpisode(evNew)
		if len(exports) != 2 {
			t.Fatalf("episodes exported %d times, want 2", len(exports))
		}
		if exports[0].Key == exports[1].Key {
			t.Fatalf("two episodes must export different keys, got %q twice", exports[0].Key)
		}
	})

	t.Run("recovery notification is skipped", func(t *testing.T) {
		const episodeA int64 = 1795831215
		recRecovered := notifyRecord(9, episodeA, episodeA+90, 3)
		recRecovered.Payload.IsRecovered = true
		ev, ok, err := TranslateRecord(ctx, recRecovered, resolve, now)
		if err != nil {
			t.Fatalf("recovery notify must be skipped without error, got %v", err)
		}
		if ok || ev != (scene.Event{}) {
			t.Fatalf("recovery notify must not trigger capture: ok=%v event=%+v", ok, ev)
		}
	})
}
