// Package notify adapts Nightingale notify-receiver webhook records into
// witness-go scene.Event values via an incremental HTTP polling EventSource.
package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"witness-go/internal/pipeline"
	"witness-go/internal/scene"
)

var _ pipeline.EventSource = (*ReceiverSource)(nil)

// ErrNotImplemented marks learner-owned translation logic before implementation.
var ErrNotImplemented = errors.New("TODO(learner): not implemented")

const maxResponseBytes = 4 << 20 // 4 MiB guard for /events responses

// ReceiverRecord mirrors one entry returned by notify-receiver's GET /events endpoint.
type ReceiverRecord struct {
	Seq           int64       `json:"seq"`
	ReceivedAtUTC string      `json:"received_at_utc"`
	ReceivedAtMs  int64       `json:"received_at_ms"`
	Payload       *N9EPayload `json:"payload"`
}

// N9EPayload captures the Nightingale webhook fields needed for event filtering,
// episode deduplication, target mapping, and source-time preservation.
type N9EPayload struct {
	ID               int64             `json:"id"`
	Hash             string            `json:"hash"`
	RuleID           int64             `json:"rule_id"`
	RuleName         string            `json:"rule_name"`
	IsRecovered      bool              `json:"is_recovered"`
	TriggerTime      int64             `json:"trigger_time"`
	FirstTriggerTime int64             `json:"first_trigger_time"`
	LastEvalTime     int64             `json:"last_eval_time"`
	NotifyCurNumber  int64             `json:"notify_cur_number"`
	TargetIdent      string            `json:"target_ident"`
	TagsMap          map[string]string `json:"tags_map"`
}

type eventsEnvelope struct {
	Count  int              `json:"count"`
	Events []ReceiverRecord `json:"events"`
}

// IdentityResolver maps a logical alert target (e.g. Prometheus instance URL)
// to the live running scene.Identity (Target + RunID).
// Returning matched=false means the alert belongs to another target and should be skipped.
type IdentityResolver func(ctx context.Context, logicalTarget string) (id scene.Identity, matched bool, err error)

// ReceiverConfig configures incremental polling against notify-receiver's /events API.
type ReceiverConfig struct {
	BaseURL      string
	PollInterval time.Duration
	SinceSeq     int64
	Client       *http.Client
	Now          func() time.Time
	Resolve      IdentityResolver
	// OnSkip optionally logs or counts skipped records ("recovered", "unmatched_target", "malformed").
	OnSkip func(seq int64, reason string, err error)
}

// ReceiverSource implements pipeline.EventSource by polling notify-receiver incrementally.
type ReceiverSource struct {
	baseURL      string
	pollInterval time.Duration
	sinceSeq     int64
	client       *http.Client
	now          func() time.Time
	resolve      IdentityResolver
	onSkip       func(seq int64, reason string, err error)
	pending      []scene.Event
}

// NewReceiverSource validates cfg and creates a ReceiverSource.
func NewReceiverSource(cfg ReceiverConfig) (*ReceiverSource, error) {
	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		return nil, errors.New("receiver base URL must not be empty")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid receiver base URL %q", cfg.BaseURL)
	}
	if cfg.PollInterval <= 0 {
		return nil, errors.New("poll interval must be positive")
	}
	if cfg.SinceSeq < 0 {
		return nil, errors.New("since_seq must be non-negative")
	}
	if cfg.Resolve == nil {
		return nil, errors.New("identity resolver must not be nil")
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &ReceiverSource{
		baseURL:      base,
		pollInterval: cfg.PollInterval,
		sinceSeq:     cfg.SinceSeq,
		client:       client,
		now:          nowFn,
		resolve:      cfg.Resolve,
		onSkip:       cfg.OnSkip,
	}, nil
}

// Next blocks until a matching firing alert event arrives or ctx is cancelled.
// Transient poll/malformed record issues are reported via OnSkip without crashing the stream;
// context cancellation exits immediately.
func (s *ReceiverSource) Next(ctx context.Context) (scene.Event, error) {
	for {
		if len(s.pending) > 0 {
			ev := s.pending[0]
			s.pending = s.pending[1:]
			return ev, nil
		}
		if err := ctx.Err(); err != nil {
			return scene.Event{}, err
		}

		records, err := s.fetchSince(ctx, s.sinceSeq)
		if err != nil {
			if ctx.Err() != nil {
				return scene.Event{}, ctx.Err()
			}
			if s.onSkip != nil {
				s.onSkip(s.sinceSeq, "poll_error", err)
			}
		} else {
			for _, rec := range records {
				if rec.Seq > s.sinceSeq {
					s.sinceSeq = rec.Seq
				}
				ev, ok, tErr := TranslateRecord(ctx, rec, s.resolve, s.now)
				if tErr != nil {
					if ctx.Err() != nil {
						return scene.Event{}, ctx.Err()
					}
					if s.onSkip != nil {
						s.onSkip(rec.Seq, "malformed", tErr)
					}
					continue
				}
				if !ok {
					if s.onSkip != nil {
						reason := "filtered"
						if rec.Payload != nil && rec.Payload.IsRecovered {
							reason = "recovered"
						}
						s.onSkip(rec.Seq, reason, nil)
					}
					continue
				}
				s.pending = append(s.pending, ev)
			}
			if len(s.pending) > 0 {
				continue
			}
		}

		timer := time.NewTimer(s.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return scene.Event{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *ReceiverSource) fetchSince(ctx context.Context, sinceSeq int64) ([]ReceiverRecord, error) {
	endpoint := fmt.Sprintf("%s/events?since_seq=%s", s.baseURL, strconv.FormatInt(sinceSeq, 10))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("notify-receiver returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxResponseBytes {
		return nil, errors.New("notify-receiver response exceeded size limit")
	}
	var env eventsEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode notify-receiver events: %w", err)
	}
	return env.Events, nil
}

// ExtractLogicalTarget 是助手提供的相邻示范：从 Nightingale 载荷中提取逻辑监控目标。
// 优先使用 Prometheus 探测/指标标签 tags_map["instance"]，为空时才回退到 target_ident；
// 若两者均为空则明确报错，绝不静默返回空目标或猜测容器名。
func ExtractLogicalTarget(payload N9EPayload) (string, error) {
	if payload.TagsMap != nil {
		if inst := strings.TrimSpace(payload.TagsMap["instance"]); inst != "" {
			return inst, nil
		}
	}
	if ident := strings.TrimSpace(payload.TargetIdent); ident != "" {
		return ident, nil
	}
	return "", errors.New("alert payload missing logical target (tags_map.instance and target_ident are empty)")
}

// ParseReceivedAt 是助手提供的相邻示范：解析通知到达时间。
// 优先解析 notify-receiver 记录的 received_at_utc（RFC3339Nano），
// 缺失时按毫秒戳 received_at_ms 回退；若两者都缺失则使用注入时钟 now()。
func ParseReceivedAt(rec ReceiverRecord, now func() time.Time) (time.Time, error) {
	if raw := strings.TrimSpace(rec.ReceivedAtUTC); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse received_at_utc %q: %w", raw, err)
		}
		return parsed.UTC(), nil
	}
	if rec.ReceivedAtMs > 0 {
		return time.UnixMilli(rec.ReceivedAtMs).UTC(), nil
	}
	if now != nil {
		if t := now(); !t.IsZero() {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errors.New("missing notification received time")
}

// TranslateRecord 将一条来自 notify-receiver 的告警记录翻译为 witness-go 的 scene.Event。
// 返回值语义：
//   - (event, true, nil)：合法的、命中目标的触发类告警，应交给 Runner/Coordinator；
//   - (zero, false, nil)：合法但应主动跳过的记录（如 is_recovered=true 的恢复通知，或非当前监控目标的告警）；
//   - (zero, false, err)：记录缺失关键字段、解析失败或身份解析出错。
//
// TODO(学习者):
//  1. 先检查 ctx.Err()；校验 rec.Payload != nil 且 resolve != nil，否则返回错误。
//  2. 过滤恢复通知：若 rec.Payload.IsRecovered 为 true，直接返回 scene.Event{}, false, nil（绝不当作新故障触发抓取）。
//  3. 提取逻辑目标：调用 ExtractLogicalTarget(*rec.Payload)，失败返回错误。
//  4. 解析运行身份：调用 id, matched, err := resolve(ctx, logicalTarget)：
//     - err != nil 返回错误；
//     - !matched 返回 scene.Event{}, false, nil（跳过其他目标的告警）；
//     - matched 但 id.Target == "" 或 id.RunID == "" 返回错误（不能用空身份放行）。
//  5. 构造稳定去重键 Event.Key：
//     - 取 hash := strings.TrimSpace(rec.Payload.Hash)，为空则返回错误；
//     - 取单次故障周期时间戳 episodeTS：优先用 rec.Payload.FirstTriggerTime（>0），
//     若为 0 则回退到 rec.Payload.TriggerTime（>0）；若两者均 <= 0 返回错误；
//     - 拼装 key := fmt.Sprintf("%s:%d", hash, episodeTS)。
//     （这样同一轮故障的重复提醒/重试 seq 和 notify_cur_number 不同，但 Key 相同可被 Coordinator 去重；
//     而几分钟后同规则的新一轮故障 first_trigger_time 不同，Key 不同不会被误吞。）
//  6. 解析到达时间：调用 ParseReceivedAt(rec, now)，失败返回错误。
//  7. 保留来源时间（非故障起点）：若 rec.Payload.TriggerTime > 0，
//     构造 t := time.Unix(rec.Payload.TriggerTime, 0).UTC() 并赋值给 SourceTime: &t。
//  8. 返回组装好的 scene.Event, true, nil。
func TranslateRecord(ctx context.Context, rec ReceiverRecord, resolve IdentityResolver, now func() time.Time) (scene.Event, bool, error) {
	if ctx.Err() != nil {
		return scene.Event{}, false, ctx.Err()
	}
	if rec.Payload == nil || resolve == nil {
		return scene.Event{}, false, errors.New("missing payload or identity resolver")
	}
	if rec.Payload.IsRecovered {
		return scene.Event{}, false, nil
	}
	logicalTarget, err := ExtractLogicalTarget(*rec.Payload)
	if err != nil {
		return scene.Event{}, false, fmt.Errorf("extract logical target: %w", err)
	}
	id, matched, err := resolve(ctx, logicalTarget)
	if err != nil {
		return scene.Event{}, false, fmt.Errorf("resolve identity: %w", err)
	}
	if !matched {
		return scene.Event{}, false, nil
	}
	if id.Target == "" || id.RunID == "" {
		return scene.Event{}, false, errors.New("resolved identity has empty Target or RunID")
	}
	hash := strings.TrimSpace(rec.Payload.Hash)
	if hash == "" {
		return scene.Event{}, false, errors.New("payload hash is empty")
	}
	epsiodeTS := rec.Payload.FirstTriggerTime
	if epsiodeTS <= 0 {
		epsiodeTS = rec.Payload.TriggerTime
	}
	if epsiodeTS <= 0 {
		return scene.Event{}, false, errors.New("both first_trigger_time and trigger_time are non-positive")
	}
	key := fmt.Sprintf("%s:%d", hash, epsiodeTS)
	receivedAt, err := ParseReceivedAt(rec, now)
	if err != nil {
		return scene.Event{}, false, fmt.Errorf("parse received time: %w", err)
	}
	var sourceTime *time.Time
	if rec.Payload.TriggerTime > 0 {
		t := time.Unix(rec.Payload.TriggerTime, 0).UTC()
		sourceTime = &t
	}
	ev := scene.Event{
		Key:        key,
		Identity:   id,
		ReceivedAt: receivedAt,
		SourceTime: sourceTime,
	}
	return ev, true, nil
}
