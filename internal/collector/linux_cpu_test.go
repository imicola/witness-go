package collector

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
	"witness-go/internal/exporter"
	"witness-go/internal/pipeline"
	"witness-go/internal/scene"
)

func TestParseCPUPSI(t *testing.T) {
	t.Run("some and full", func(t *testing.T) {
		raw := "some avg10=12.34 avg60=5.67 avg300=1.23 total=98765432\nfull avg10=0.50 avg60=0.10 avg300=0.00 total=4567\n"
		got, err := ParseCPUPSI(raw)
		if err != nil {
			t.Fatalf("ParseCPUPSI returned unexpected error: %v", err)
		}
		if got.SomeAvg10 != 12.34 || got.SomeAvg60 != 5.67 || got.SomeAvg300 != 1.23 || got.SomeTotalUS != 98765432 {
			t.Fatalf("unexpected some metrics: %+v", got)
		}
		if !got.HasFull || got.FullAvg10 != 0.50 || got.FullTotalUS != 4567 {
			t.Fatalf("unexpected full metrics: %+v", got)
		}
	})

	t.Run("some only kernel format", func(t *testing.T) {
		raw := "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
		got, err := ParseCPUPSI(raw)
		if err != nil {
			t.Fatalf("ParseCPUPSI failed on some-only format: %v", err)
		}
		if got.HasFull {
			t.Fatalf("expected HasFull=false when full line is absent")
		}
	})

	t.Run("rejects malformed inputs", func(t *testing.T) {
		badCases := []string{
			"",
			"full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
			"some avg10=-1.00 avg60=0.00 avg300=0.00 total=0\n",
			"some avg10=NaN avg60=0.00 avg300=0.00 total=0\n",
			"some avg10=0.00 avg60=0.00 total=0\n",
			"some avg10=0.00 avg60=0.00 avg300=0.00 total=-5\n",
			"some avg10=0.00 avg60=0.00 avg300=0.00 total=1\nsome avg10=0.00 avg60=0.00 avg300=0.00 total=2\n",
		}
		for i, raw := range badCases {
			if _, err := ParseCPUPSI(raw); err == nil {
				t.Fatalf("case %d: expected error for malformed PSI %q, got nil", i, raw)
			}
		}
	})
}

func TestParseCPUMax(t *testing.T) {
	t.Run("limited quota from W47 lab", func(t *testing.T) {
		got, err := ParseCPUMax("30000 100000\n")
		if err != nil {
			t.Fatalf("ParseCPUMax failed: %v", err)
		}
		if got.Unlimited || got.QuotaUS != 30000 || got.PeriodUS != 100000 {
			t.Fatalf("unexpected RawCPUMax: %+v", got)
		}
	})

	t.Run("unlimited max quota", func(t *testing.T) {
		got, err := ParseCPUMax("max 100000\n")
		if err != nil {
			t.Fatalf("ParseCPUMax failed on max: %v", err)
		}
		if !got.Unlimited || got.QuotaUS != 0 || got.PeriodUS != 100000 {
			t.Fatalf("unexpected unlimited RawCPUMax: %+v", got)
		}
	})

	t.Run("rejects malformed cpu.max", func(t *testing.T) {
		badCases := []string{
			"",
			"30000",
			"30000 100000 extra",
			"0 100000",
			"30000 0",
			"-100 100000",
			"max -1",
			"abc 100000",
		}
		for i, raw := range badCases {
			if _, err := ParseCPUMax(raw); err == nil {
				t.Fatalf("case %d: expected error for %q, got nil", i, raw)
			}
		}
	})
}

func TestParseCPUStat(t *testing.T) {
	t.Run("valid cgroup v2 cpu.stat with extra kernel keys", func(t *testing.T) {
		raw := "" +
			"usage_usec 24000000\n" +
			"user_usec 20000000\n" +
			"system_usec 4000000\n" +
			"nr_periods 500\n" +
			"nr_throttled 390\n" +
			"throttled_usec 70914680\n"
		got, err := ParseCPUStat(raw)
		if err != nil {
			t.Fatalf("ParseCPUStat failed: %v", err)
		}
		if got.UsageUS != 24000000 || got.NrThrottled != 390 || got.ThrottledUS != 70914680 {
			t.Fatalf("unexpected RawCPUStat: %+v", got)
		}
	})

	t.Run("rejects missing required keys, duplicates, and bad values", func(t *testing.T) {
		badCases := []string{
			"",
			"usage_usec 100\nnr_throttled 1\n", // missing throttled_usec
			"usage_usec 100\nthrottled_usec 10\n",
			"nr_throttled 1\nthrottled_usec 10\n",
			"usage_usec -1\nnr_throttled 1\nthrottled_usec 10\n",
			"usage_usec 100\nusage_usec 200\nnr_throttled 1\nthrottled_usec 10\n",
			"usage_usec 100 extra\nnr_throttled 1\nthrottled_usec 10\n",
		}
		for i, raw := range badCases {
			if _, err := ParseCPUStat(raw); err == nil {
				t.Fatalf("case %d: expected error for %q, got nil", i, raw)
			}
		}
	})
}

func TestDeltaTrackerNormalProgression(t *testing.T) {
	base := time.Date(2026, 11, 26, 16, 0, 0, 0, time.UTC)
	id := scene.Identity{Target: "test-http", RunID: "container-a"}
	quota := RawCPUMax{QuotaUS: 30000, PeriodUS: 100000}

	dt := NewDeltaTracker()

	first, err := dt.Observe(id, base, quota, RawCPUStat{
		UsageUS:     10000000,
		NrThrottled: 69,
		ThrottledUS: 12723559,
	})
	if err != nil {
		t.Fatalf("first Observe failed: %v", err)
	}
	if first.DeltaValid || first.DeltaReason != DeltaReasonInitial {
		t.Fatalf("first sample should establish baseline with initial reason, got valid=%v reason=%q", first.DeltaValid, first.DeltaReason)
	}
	if first.QuotaUS != 30000 || first.PeriodUS != 100000 || first.QuotaUnlimited {
		t.Fatalf("quota fields not copied: %+v", first)
	}

	second, err := dt.Observe(id, base.Add(30*time.Second), quota, RawCPUStat{
		UsageUS:     19000000,
		NrThrottled: 390,
		ThrottledUS: 70914680,
	})
	if err != nil {
		t.Fatalf("second Observe failed: %v", err)
	}
	if !second.DeltaValid || second.DeltaReason != "" {
		t.Fatalf("second sample should have valid delta, got valid=%v reason=%q", second.DeltaValid, second.DeltaReason)
	}
	if second.DeltaUsageUS != 9000000 || second.DeltaNrThrottled != 321 || second.DeltaThrottledUS != 58191121 {
		t.Fatalf("unexpected deltas: %+v", second)
	}
}

func TestLinuxSamplerEndToEndWithCoordinatorAndExporter(t *testing.T) {
	dir := t.TempDir()
	cpuMaxPath := filepath.Join(dir, "cpu.max")
	cpuStatPath := filepath.Join(dir, "cpu.stat")
	psiPath := filepath.Join(dir, "cpu.psi")
	exportPath := filepath.Join(dir, "scene-export.json")

	writeFile := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	base := time.Date(2026, 11, 26, 16, 10, 0, 0, time.UTC)
	now := base
	id := scene.Identity{Target: "test-http", RunID: "cid-1001"}

	sampler, err := NewLinuxSampler(SourceConfig{
		IdentityFunc: func(context.Context) (scene.Identity, error) { return id, nil },
		CPUMaxPath:   cpuMaxPath,
		CPUStatPath:  cpuStatPath,
		HostPSIPath:  psiPath,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewLinuxSampler: %v", err)
	}

	ring, err := pipeline.NewRing(16)
	if err != nil {
		t.Fatalf("NewRing: %v", err)
	}
	exp, err := exporter.NewFileExporter(exportPath, "windows-fixture-collector-test", func() time.Time { return base.Add(40 * time.Second) })
	if err != nil {
		t.Fatalf("NewFileExporter: %v", err)
	}
	coord, err := pipeline.NewCoordinator(ring, exp, pipeline.CoordinatorConfig{
		PreWindow:      10 * time.Second,
		PostWindow:     10 * time.Second,
		MaxGap:         15 * time.Second,
		MaxTaskSamples: 16,
		MaxSeenKeys:    8,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	ctx := context.Background()

	// Sample 1 at T+00s (pre-window baseline)
	writeFile(cpuMaxPath, "30000 100000\n")
	writeFile(cpuStatPath, "usage_usec 1000\nnr_throttled 10\nthrottled_usec 5000\n")
	writeFile(psiPath, "some avg10=1.50 avg60=0.80 avg300=0.20 total=1000\n")
	s1, err := sampler.Sample(ctx)
	if err != nil {
		t.Fatalf("sample 1: %v", err)
	}
	if err := coord.OnSample(ctx, s1); err != nil {
		t.Fatalf("OnSample 1: %v", err)
	}

	// Alert arrives at T+10s -> window [T+00s, T+20s]
	dec, err := coord.OnEvent(ctx, scene.Event{
		Key:        "n9e-alert-1",
		Identity:   id,
		ReceivedAt: base.Add(10 * time.Second),
	})
	if err != nil || dec != pipeline.DecisionStarted {
		t.Fatalf("OnEvent: dec=%q err=%v", dec, err)
	}

	// Sample 2 at T+20s (throttling increases under quota)
	now = base.Add(20 * time.Second)
	writeFile(cpuStatPath, "usage_usec 7000\nnr_throttled 45\nthrottled_usec 35000\n")
	writeFile(psiPath, "some avg10=4.25 avg60=1.20 avg300=0.40 total=2500\n")
	s2, err := sampler.Sample(ctx)
	if err != nil {
		t.Fatalf("sample 2: %v", err)
	}
	if err := coord.OnSample(ctx, s2); err != nil {
		t.Fatalf("OnSample 2: %v", err)
	}

	exported, err := coord.Tick(ctx, base.Add(20*time.Second))
	if err != nil || !exported {
		t.Fatalf("Tick export failed: exported=%v err=%v", exported, err)
	}

	rec, err := exporter.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("exporter.ReadFile failed: %v", err)
	}
	if len(rec.Selection.Samples) != 2 {
		t.Fatalf("expected 2 exported samples, got %d", len(rec.Selection.Samples))
	}
	last := rec.Selection.Samples[1]
	if last.CPU == nil || !last.CPU.DeltaValid || last.CPU.DeltaThrottledUS != 30000 || last.CPU.DeltaNrThrottled != 35 {
		t.Fatalf("unexpected exported CPU delta: %+v", last.CPU)
	}
	if last.PSI == nil || last.PSI.SomeAvg10 != 4.25 || last.PSI.SomeTotalUS != 2500 {
		t.Fatalf("unexpected exported PSI: %+v", last.PSI)
	}
}

// TestLearnerDeltaCounterexample 是击穿错误 DeltaTracker 实现的反例测试，覆盖两个高危场景：
//  1. 跨 RunID 伪正增量反例：同一 Target 从 run-1 切换到 run-2（模拟容器重建），
//     且 run-2 的累计计数碰巧比 run-1 更大（例如 usage_usec 从 500 变成 900）。
//     如果错误实现只靠 curr < prev 防负增量而不核对 RunID，就会静默算出虚假的 DeltaUsageUS=400 且 DeltaValid=true。
//     正确实现必须返回 DeltaValid=false, DeltaReason=DeltaReasonRunChanged，并且下一次 run-2 续采时以 900 为新基线正确求差。
//  2. 同 RunID 计数回退防下溢/防伪造零反例：同一 Target + 同一 RunID 出现任一累计计数回退
//     （例如 throttled_usec 从 5000 回退到 100，或时间戳倒退）。
//     正确实现必须返回 DeltaValid=false, DeltaReason=DeltaReasonCounterReset，
//     绝不能无符号下溢成极大数，也不能把 DeltaValid 标成 true 且把差值伪造成 0；并且之后同 RunID 继续递增时应基于新基线恢复 DeltaValid=true。
func TestLearnerDeltaCounterexample(t *testing.T) {
	base := time.Date(2026, 11, 26, 16, 10, 0, 0, time.UTC)

	// 显式接收子测试的 t，让失败归属到具体子测试，而不是让整个测试直接终止。
	writeFile := func(t *testing.T, path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	t.Run("run change must not fabricate a positive delta", func(t *testing.T) {
		dir := t.TempDir()
		cpuMaxPath := filepath.Join(dir, "cpu.max")
		cpuStatPath := filepath.Join(dir, "cpu.stat")
		psiPath := filepath.Join(dir, "cpu.psi")

		// NewLinuxSampler 要求三个路径非空，Sample 每拍都会读全部三个文件；
		// 即使本测试只断言 CPU 增量语义，psi 固定内容也必须存在且可解析。
		now := base
		id := scene.Identity{Target: "test-http", RunID: "run-1"}
		sampler, err := NewLinuxSampler(SourceConfig{
			IdentityFunc: func(context.Context) (scene.Identity, error) { return id, nil },
			CPUMaxPath:   cpuMaxPath,
			CPUStatPath:  cpuStatPath,
			HostPSIPath:  psiPath,
			Now:          func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewLinuxSampler: %v", err)
		}
		writeFile(t, cpuMaxPath, "30000 100000\n")
		writeFile(t, psiPath, "some avg10=1.00 avg60=0.50 avg300=0.10 total=1000\n")
		ctx := context.Background()

		// 第 1 拍（run-1）：建立基线，usage_usec=500。
		writeFile(t, cpuStatPath, "usage_usec 500\nnr_throttled 2\nthrottled_usec 500\n")
		s1, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 1: %v", err)
		}
		if s1.CPU == nil || s1.CPU.DeltaValid || s1.CPU.DeltaReason != DeltaReasonInitial {
			t.Fatalf("sample 1 should be the initial baseline, got %+v", s1.CPU)
		}

		// 第 2 拍：容器重建，RunID 切到 run-2，且 run-2 的累计计数碰巧都比 run-1 大
		// （usage_usec 500 -> 900）。只看 curr < prev 而不核对 RunID 的错误实现，
		// 会静默算出 DeltaUsageUS=400 并标 DeltaValid=true。
		id = scene.Identity{Target: "test-http", RunID: "run-2"}
		now = base.Add(30 * time.Second)
		writeFile(t, cpuStatPath, "usage_usec 900\nnr_throttled 4\nthrottled_usec 900\n")
		s2, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 2: %v", err)
		}
		if s2.CPU == nil || s2.CPU.DeltaValid || s2.CPU.DeltaReason != DeltaReasonRunChanged {
			t.Fatalf("cross-run sample must be invalid with reason %q, got %+v", DeltaReasonRunChanged, s2.CPU)
		}
		if s2.CPU.DeltaUsageUS != 0 || s2.CPU.DeltaNrThrottled != 0 || s2.CPU.DeltaThrottledUS != 0 {
			t.Fatalf("cross-run sample must not fabricate deltas: %+v", s2.CPU)
		}

		// 第 3 拍（仍是 run-2）：必须以 900 为新基线求差（1700-900=800）；
		// 错误地沿用 run-1 的 500 基线会算出 1200。
		now = base.Add(60 * time.Second)
		writeFile(t, cpuStatPath, "usage_usec 1700\nnr_throttled 6\nthrottled_usec 1500\n")
		s3, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 3: %v", err)
		}
		if s3.CPU == nil || !s3.CPU.DeltaValid || s3.CPU.DeltaReason != "" {
			t.Fatalf("run-2 continuation should recover a valid delta, got %+v", s3.CPU)
		}
		if s3.CPU.DeltaUsageUS != 800 || s3.CPU.DeltaNrThrottled != 2 || s3.CPU.DeltaThrottledUS != 600 {
			t.Fatalf("run-2 deltas must be computed from the run-2 baseline: %+v", s3.CPU)
		}
	})

	t.Run("counter regression must not underflow or fake a zero delta", func(t *testing.T) {
		dir := t.TempDir()
		cpuMaxPath := filepath.Join(dir, "cpu.max")
		cpuStatPath := filepath.Join(dir, "cpu.stat")
		psiPath := filepath.Join(dir, "cpu.psi")

		now := base
		id := scene.Identity{Target: "test-http", RunID: "run-1"}
		sampler, err := NewLinuxSampler(SourceConfig{
			IdentityFunc: func(context.Context) (scene.Identity, error) { return id, nil },
			CPUMaxPath:   cpuMaxPath,
			CPUStatPath:  cpuStatPath,
			HostPSIPath:  psiPath,
			Now:          func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("NewLinuxSampler: %v", err)
		}
		writeFile(t, cpuMaxPath, "30000 100000\n")
		writeFile(t, psiPath, "some avg10=1.00 avg60=0.50 avg300=0.10 total=1000\n")
		ctx := context.Background()

		// 第 1 拍（run-1）：基线 usage=1000, nr_throttled=5, throttled_usec=5000。
		writeFile(t, cpuStatPath, "usage_usec 1000\nnr_throttled 5\nthrottled_usec 5000\n")
		s1, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 1: %v", err)
		}
		if s1.CPU == nil || s1.CPU.DeltaValid || s1.CPU.DeltaReason != DeltaReasonInitial {
			t.Fatalf("sample 1 should be the initial baseline, got %+v", s1.CPU)
		}

		// 第 2 拍（同 RunID）：throttled_usec 从 5000 回退到 100，而 usage/nr 仍在上涨。
		// 正确实现必须判 counter_reset：DeltaValid=false 且三个增量字段保持 0。
		// 无符号直接相减会下溢成 2^64 级别的极大数；把差值伪造成 0 却标 DeltaValid=true 同样不可接受。
		now = base.Add(30 * time.Second)
		writeFile(t, cpuStatPath, "usage_usec 1500\nnr_throttled 6\nthrottled_usec 100\n")
		s2, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 2: %v", err)
		}
		if s2.CPU == nil || s2.CPU.DeltaValid || s2.CPU.DeltaReason != DeltaReasonCounterReset {
			t.Fatalf("regressed counter must be invalid with reason %q, got %+v", DeltaReasonCounterReset, s2.CPU)
		}
		if s2.CPU.DeltaUsageUS != 0 || s2.CPU.DeltaNrThrottled != 0 || s2.CPU.DeltaThrottledUS != 0 {
			t.Fatalf("regressed counter must not underflow or fabricate deltas: %+v", s2.CPU)
		}

		// 第 3 拍（同 RunID）：throttled_usec 从回退后的 100 涨到 600，
		// 必须以 100 为新基线恢复 DeltaValid=true（600-100=500），而不是从 5000 求差。
		now = base.Add(60 * time.Second)
		writeFile(t, cpuStatPath, "usage_usec 2000\nnr_throttled 8\nthrottled_usec 600\n")
		s3, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 3: %v", err)
		}
		if s3.CPU == nil || !s3.CPU.DeltaValid || s3.CPU.DeltaReason != "" {
			t.Fatalf("monotonic continuation should recover a valid delta, got %+v", s3.CPU)
		}
		if s3.CPU.DeltaUsageUS != 500 || s3.CPU.DeltaNrThrottled != 2 || s3.CPU.DeltaThrottledUS != 500 {
			t.Fatalf("deltas after reset must come from the new baseline: %+v", s3.CPU)
		}

		// 第 4 拍（同 RunID，计数全部继续上涨，但时间戳倒退）：
		// 时间戳倒退与计数回退同属 counter_reset，必须拒绝而不产出增量。
		now = base.Add(45 * time.Second)
		writeFile(t, cpuStatPath, "usage_usec 2500\nnr_throttled 9\nthrottled_usec 700\n")
		s4, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 4: %v", err)
		}
		if s4.CPU == nil || s4.CPU.DeltaValid || s4.CPU.DeltaReason != DeltaReasonCounterReset {
			t.Fatalf("backward timestamp must be invalid with reason %q, got %+v", DeltaReasonCounterReset, s4.CPU)
		}
		if s4.CPU.DeltaUsageUS != 0 || s4.CPU.DeltaNrThrottled != 0 || s4.CPU.DeltaThrottledUS != 0 {
			t.Fatalf("backward timestamp must not fabricate deltas: %+v", s4.CPU)
		}

		// 第 5 拍：时间恢复正常递增，基于第 4 拍的新基线恢复 DeltaValid=true。
		now = base.Add(75 * time.Second)
		writeFile(t, cpuStatPath, "usage_usec 3000\nnr_throttled 10\nthrottled_usec 900\n")
		s5, err := sampler.Sample(ctx)
		if err != nil {
			t.Fatalf("sample 5: %v", err)
		}
		if s5.CPU == nil || !s5.CPU.DeltaValid || s5.CPU.DeltaReason != "" {
			t.Fatalf("recovery sample should have a valid delta, got %+v", s5.CPU)
		}
		if s5.CPU.DeltaUsageUS != 500 || s5.CPU.DeltaNrThrottled != 1 || s5.CPU.DeltaThrottledUS != 200 {
			t.Fatalf("deltas after the backward timestamp must come from the new baseline: %+v", s5.CPU)
		}
	})
}
