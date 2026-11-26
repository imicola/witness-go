// Package collector reads Linux cgroup v2 CPU files and host CPU PSI files,
// and computes monotonic per-RunID deltas without fabricating zeros on reset.
package collector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"witness-go/internal/pipeline"
	"witness-go/internal/scene"
)

var _ pipeline.Sampler = (*LinuxSampler)(nil)

// ErrNotImplemented marks learner-owned parser/tracker methods before implementation.
var ErrNotImplemented = errors.New("TODO(learner): not implemented")

const (
	// DeltaReasonInitial indicates the first sample observed for a logical Target;
	// a baseline is recorded, so no delta can be computed yet.
	DeltaReasonInitial = "initial"
	// DeltaReasonRunChanged indicates the logical Target switched to a new RunID
	// (e.g. container restart/recreate); baseline is reset and cross-run subtraction is forbidden.
	DeltaReasonRunChanged = "run_changed"
	// DeltaReasonCounterReset indicates the same RunID reported a backward timestamp or
	// decreased cumulative counter; baseline is reset without uint64 underflow or fake zero deltas.
	DeltaReasonCounterReset = "counter_reset"
)

// RawCPUMax holds parsed fields from cgroup v2 cpu.max ("<quota> <period>" or "max <period>").
type RawCPUMax struct {
	QuotaUS   uint64
	PeriodUS  uint64
	Unlimited bool
}

// RawCPUStat holds required cumulative counters from cgroup v2 cpu.stat.
type RawCPUStat struct {
	UsageUS     uint64
	NrThrottled uint64
	ThrottledUS uint64
}

type cpuBaseline struct {
	runID       string
	at          time.Time
	usageUS     uint64
	throttledUS uint64
	nrThrottled uint64
}

// DeltaTracker maintains per-Target baselines to compute safe cumulative deltas.
// It is owned by a single serial caller (the sampler owner loop).
type DeltaTracker struct {
	prev map[string]cpuBaseline
}

// NewDeltaTracker constructs an initialized DeltaTracker.
func NewDeltaTracker() *DeltaTracker {
	return &DeltaTracker{
		prev: make(map[string]cpuBaseline),
	}
}

// SourceConfig configures file paths and identity/clock injection for LinuxSampler.
// On Windows tests, paths point to local fixture files; on Linux, they point to
// /sys/fs/cgroup/.../cpu.max, .../cpu.stat, and /proc/pressure/cpu.
type SourceConfig struct {
	IdentityFunc func(context.Context) (scene.Identity, error)
	CPUMaxPath   string
	CPUStatPath  string
	HostPSIPath  string
	Now          func() time.Time
}

// LinuxSampler implements pipeline.Sampler by reading cgroup v2 CPU files and host PSI.
type LinuxSampler struct {
	cfg     SourceConfig
	tracker *DeltaTracker
}

// NewLinuxSampler validates cfg and constructs a LinuxSampler.
func NewLinuxSampler(cfg SourceConfig) (*LinuxSampler, error) {
	if cfg.IdentityFunc == nil {
		return nil, errors.New("identity function must not be nil")
	}
	if cfg.CPUMaxPath == "" || cfg.CPUStatPath == "" || cfg.HostPSIPath == "" {
		return nil, errors.New("cpu.max, cpu.stat, and host psi paths must not be empty")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &LinuxSampler{
		cfg:     cfg,
		tracker: NewDeltaTracker(),
	}, nil
}

// Sample reads one cross-layer snapshot (container cpu.max + cpu.stat + host CPU PSI).
// Any I/O or parse failure returns an error immediately without fabricating zero values.
func (s *LinuxSampler) Sample(ctx context.Context) (scene.Sample, error) {
	if err := ctx.Err(); err != nil {
		return scene.Sample{}, err
	}
	id, err := s.cfg.IdentityFunc(ctx)
	if err != nil {
		return scene.Sample{}, fmt.Errorf("resolve identity: %w", err)
	}
	if id.Target == "" || id.RunID == "" {
		return scene.Sample{}, errors.New("resolved identity must have non-empty target and run_id")
	}

	maxRaw, err := os.ReadFile(s.cfg.CPUMaxPath)
	if err != nil {
		return scene.Sample{}, fmt.Errorf("read cpu.max %q: %w", s.cfg.CPUMaxPath, err)
	}
	statRaw, err := os.ReadFile(s.cfg.CPUStatPath)
	if err != nil {
		return scene.Sample{}, fmt.Errorf("read cpu.stat %q: %w", s.cfg.CPUStatPath, err)
	}
	psiRaw, err := os.ReadFile(s.cfg.HostPSIPath)
	if err != nil {
		return scene.Sample{}, fmt.Errorf("read host cpu psi %q: %w", s.cfg.HostPSIPath, err)
	}

	cpuMax, err := ParseCPUMax(string(maxRaw))
	if err != nil {
		return scene.Sample{}, fmt.Errorf("parse cpu.max: %w", err)
	}
	cpuStat, err := ParseCPUStat(string(statRaw))
	if err != nil {
		return scene.Sample{}, fmt.Errorf("parse cpu.stat: %w", err)
	}
	psi, err := ParseCPUPSI(string(psiRaw))
	if err != nil {
		return scene.Sample{}, fmt.Errorf("parse host cpu psi: %w", err)
	}

	at := s.cfg.Now()
	if at.IsZero() {
		return scene.Sample{}, errors.New("sample clock returned zero time")
	}
	cpu, err := s.tracker.Observe(id, at, cpuMax, cpuStat)
	if err != nil {
		return scene.Sample{}, fmt.Errorf("track cpu delta: %w", err)
	}
	return scene.Sample{
		Identity: id,
		At:       at,
		CPU:      &cpu,
		PSI:      &psi,
	}, nil
}

// ParseCPUPSI 是助手提供的严格文本解析相邻示范，用于解析宿主 /proc/pressure/cpu。
// 典型格式：
//
//	some avg10=12.34 avg60=5.67 avg300=1.23 total=98765432
//	full avg10=0.00 avg60=0.00 avg300=0.00 total=0
//
// 约束：必须包含 some 行，full 行可选（兼容部分不输出 full 行的内核）；
// 拒绝重复行、缺失字段、未知键、负数或 NaN/Inf，绝不静默补零。
func ParseCPUPSI(raw string) (scene.PSI, error) {
	var out scene.PSI
	var seenSome, seenFull bool

	for lineNo, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 5 {
			return scene.PSI{}, fmt.Errorf("line %d: expected 5 fields, got %d", lineNo+1, len(fields))
		}
		kind := fields[0]
		avg10, avg60, avg300, total, err := parsePSILineMetrics(fields[1:])
		if err != nil {
			return scene.PSI{}, fmt.Errorf("line %d (%s): %w", lineNo+1, kind, err)
		}
		switch kind {
		case "some":
			if seenSome {
				return scene.PSI{}, fmt.Errorf("line %d: duplicate some line", lineNo+1)
			}
			seenSome = true
			out.SomeAvg10 = avg10
			out.SomeAvg60 = avg60
			out.SomeAvg300 = avg300
			out.SomeTotalUS = total
		case "full":
			if seenFull {
				return scene.PSI{}, fmt.Errorf("line %d: duplicate full line", lineNo+1)
			}
			seenFull = true
			out.HasFull = true
			out.FullAvg10 = avg10
			out.FullAvg60 = avg60
			out.FullAvg300 = avg300
			out.FullTotalUS = total
		default:
			return scene.PSI{}, fmt.Errorf("line %d: unknown psi line prefix %q", lineNo+1, kind)
		}
	}
	if !seenSome {
		return scene.PSI{}, errors.New("missing required 'some' line in cpu psi")
	}
	return out, nil
}

func parsePSILineMetrics(kvs []string) (avg10, avg60, avg300 float64, total uint64, err error) {
	var gotAvg10, gotAvg60, gotAvg300, gotTotal bool
	for _, token := range kvs {
		key, val, ok := strings.Cut(token, "=")
		if !ok || key == "" || val == "" {
			return 0, 0, 0, 0, fmt.Errorf("invalid key=value token %q", token)
		}
		switch key {
		case "avg10", "avg60", "avg300":
			f, parseErr := strconv.ParseFloat(val, 64)
			if parseErr != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
				return 0, 0, 0, 0, fmt.Errorf("invalid %s value %q", key, val)
			}
			switch key {
			case "avg10":
				if gotAvg10 {
					return 0, 0, 0, 0, errors.New("duplicate avg10")
				}
				gotAvg10 = true
				avg10 = f
			case "avg60":
				if gotAvg60 {
					return 0, 0, 0, 0, errors.New("duplicate avg60")
				}
				gotAvg60 = true
				avg60 = f
			case "avg300":
				if gotAvg300 {
					return 0, 0, 0, 0, errors.New("duplicate avg300")
				}
				gotAvg300 = true
				avg300 = f
			}
		case "total":
			if gotTotal {
				return 0, 0, 0, 0, errors.New("duplicate total")
			}
			u, parseErr := strconv.ParseUint(val, 10, 64)
			if parseErr != nil {
				return 0, 0, 0, 0, fmt.Errorf("invalid total value %q", val)
			}
			gotTotal = true
			total = u
		default:
			return 0, 0, 0, 0, fmt.Errorf("unknown psi key %q", key)
		}
	}
	if !gotAvg10 || !gotAvg60 || !gotAvg300 || !gotTotal {
		return 0, 0, 0, 0, errors.New("missing required psi metric keys")
	}
	return avg10, avg60, avg300, total, nil
}

// ParseCPUMax 解析 cgroup v2 的 cpu.max 文本（例如 "30000 100000\n" 或 "max 100000\n"）。
//
// TODO(学习者):
// 1. 忽略前后空白后按空白切分，必须恰好包含 2 个字段（<quota> <period>），否则返回错误。
// 2. 第 2 个字段 period 必须是正整数（strconv.ParseUint 且 > 0），否则返回错误。
// 3. 第 1 个字段 quota：
//   - 若等于 "max"，返回 RawCPUMax{QuotaUS: 0, PeriodUS: period, Unlimited: true}；
//   - 否则必须是正整数（strconv.ParseUint 且 > 0），返回 RawCPUMax{QuotaUS: quota, PeriodUS: period, Unlimited: false}。
//
// 4. 拒绝空内容、0、负数、乱码或超过 2 个字段的脏数据，绝不静默补零。
func ParseCPUMax(raw string) (RawCPUMax, error) {
	var out RawCPUMax
	if strings.TrimSpace(raw) == "" {
		return out, errors.New("cpu.max content is empty or whitespace")
	}
	fields := strings.Fields(raw)
	if len(fields) != 2 {
		return out, fmt.Errorf("cpu.max expected 2 fields, got %d", len(fields))
	}
	quotaStr, periodStr := fields[0], fields[1]

	periodUS, err := strconv.ParseUint(periodStr, 10, 64)
	if err != nil || periodUS == 0 {
		return out, fmt.Errorf("invalid cpu.max period %q: %w", periodStr, err)
	}
	out.PeriodUS = periodUS

	if quotaStr == "max" {
		out.Unlimited = true
		out.QuotaUS = 0
	} else {
		quotaUS, err := strconv.ParseUint(quotaStr, 10, 64)
		if err != nil || quotaUS == 0 {
			return out, fmt.Errorf("invalid cpu.max quota %q: %w", quotaStr, err)
		}
		out.QuotaUS = quotaUS
		out.Unlimited = false
	}
	return out, nil
}

// ParseCPUStat 解析 cgroup v2 的 cpu.stat 文本，提取 usage_usec、nr_throttled 与 throttled_usec。
//
// TODO(学习者):
// 1. 按行遍历，跳过空白行；每个非空行必须恰好包含 2 个空白分隔字段（<key> <uint64>），否则返回错误。
// 2. 数值必须是非负无符号整数（strconv.ParseUint），遇到非法数字或负数返回错误。
// 3. 允许内核输出其他无关字段（如 user_usec、system_usec、nr_periods），但若同一个 key 重复出现必须返回错误。
// 4. 必须同时出现 usage_usec、nr_throttled、throttled_usec 三个必需键；缺任意一个必须返回错误，绝不静默补零。
func ParseCPUStat(raw string) (RawCPUStat, error) {
	var out RawCPUStat
	seen := make(map[string]bool)
	for lineNo, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return out, fmt.Errorf("line %d: expected 2 fields, got %d", lineNo+1, len(fields))
		}
		key, valStr := fields[0], fields[1]
		if seen[key] {
			return out, fmt.Errorf("line %d: duplicate key %q", lineNo+1, key)
		}
		seen[key] = true

		val, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			return out, fmt.Errorf("line %d: invalid value for key %q: %w", lineNo+1, key, err)
		}

		switch key {
		case "usage_usec":
			out.UsageUS = val
		case "nr_throttled":
			out.NrThrottled = val
		case "throttled_usec":
			out.ThrottledUS = val
		default:
			// Ignore unknown keys, but they must not be duplicates.
		}
	}
	if !seen["usage_usec"] || !seen["nr_throttled"] || !seen["throttled_usec"] {
		return out, fmt.Errorf("missing required keys in cpu.stat")
	}
	return out, nil
}

// Observe 根据当前逻辑 Target 的上一拍基线，计算同一 RunID 下的累计计数增量，并更新基线。
//
// TODO(学习者):
//  1. 校验输入：id.Target、id.RunID 必须非空，at 不能为零值；若 t.prev == nil 请先初始化。
//  2. 先构造带原始累计值与配额字段的 scene.CPU：
//     UsageUS, ThrottledUS, NrThrottled, QuotaUS, PeriodUS, QuotaUnlimited。
//  3. 按 id.Target 查找上一拍基线 prev, ok := t.prev[id.Target]：
//     - 若 !ok（首条样本）：保存当前基线到 t.prev[id.Target]，设置 DeltaValid=false, DeltaReason=DeltaReasonInitial 并返回。
//     - 若 prev.runID != id.RunID（容器重启/重建换了运行实例）：绝不跨 RunID 相减！
//     用当前样本覆盖 t.prev[id.Target] 作为新基线，设置 DeltaValid=false, DeltaReason=DeltaReasonRunChanged 并返回。
//     - 若同 RunID，但时间戳倒退（at.Before(prev.at)）或任一累计计数回退
//     （stat.UsageUS < prev.usageUS || stat.ThrottledUS < prev.throttledUS || stat.NrThrottled < prev.nrThrottled）：
//     绝不无符号下溢，也绝不把增量伪造为 0 并标 DeltaValid=true！
//     用当前样本重设 t.prev[id.Target]，设置 DeltaValid=false, DeltaReason=DeltaReasonCounterReset 并返回。
//     - 否则（同 RunID 且时间不倒退、三项累计计数均单调非减）：
//     计算 DeltaUsageUS、DeltaThrottledUS、DeltaNrThrottled，设置 DeltaValid=true, DeltaReason=""，
//     更新 t.prev[id.Target] 为当前样本并返回。
func (t *DeltaTracker) Observe(id scene.Identity, at time.Time, max RawCPUMax, stat RawCPUStat) (scene.CPU, error) {
	if id.Target == "" || id.RunID == "" {
		return scene.CPU{}, errors.New("identity target and run_id must not be empty")
	}
	if at.IsZero() {
		return scene.CPU{}, errors.New("timestamp at must not be zero")
	}
	if t.prev == nil {
		// lazy init
		t.prev = make(map[string]cpuBaseline)
	}
	res := scene.CPU{
		UsageUS:        stat.UsageUS,
		ThrottledUS:    stat.ThrottledUS,
		NrThrottled:    stat.NrThrottled,
		QuotaUS:        max.QuotaUS,
		PeriodUS:       max.PeriodUS,
		QuotaUnlimited: max.Unlimited,
	}
	prev, ok := t.prev[id.Target]
	if !ok {
		t.prev[id.Target] = cpuBaseline{
			runID:       id.RunID,
			at:          at,
			usageUS:     stat.UsageUS,
			throttledUS: stat.ThrottledUS,
			nrThrottled: stat.NrThrottled,
		}
		res.DeltaValid = false
		res.DeltaReason = DeltaReasonInitial
		return res, nil
	}
	if prev.runID != id.RunID {
		t.prev[id.Target] = cpuBaseline{
			runID:       id.RunID,
			at:          at,
			usageUS:     stat.UsageUS,
			throttledUS: stat.ThrottledUS,
			nrThrottled: stat.NrThrottled,
		}
		res.DeltaValid = false
		res.DeltaReason = DeltaReasonRunChanged
		return res, nil
	}

	if at.Before(prev.at) ||
		stat.UsageUS < prev.usageUS ||
		stat.ThrottledUS < prev.throttledUS ||
		stat.NrThrottled < prev.nrThrottled {
		t.prev[id.Target] = cpuBaseline{
			runID:       id.RunID,
			at:          at,
			usageUS:     stat.UsageUS,
			throttledUS: stat.ThrottledUS,
			nrThrottled: stat.NrThrottled,
		}
		res.DeltaValid = false
		res.DeltaReason = DeltaReasonCounterReset
		return res, nil
	}
	t.prev[id.Target] = cpuBaseline{
		runID:       id.RunID,
		at:          at,
		usageUS:     stat.UsageUS,
		throttledUS: stat.ThrottledUS,
		nrThrottled: stat.NrThrottled,
	}
	res.DeltaValid = true
	res.DeltaReason = ""
	res.DeltaUsageUS = stat.UsageUS - prev.usageUS
	res.DeltaThrottledUS = stat.ThrottledUS - prev.throttledUS
	res.DeltaNrThrottled = stat.NrThrottled - prev.nrThrottled
	return res, nil
}
