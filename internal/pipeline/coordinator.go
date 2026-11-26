package pipeline

import (
	"context"
	"errors"
	"time"
	"witness-go/internal/scene"
)

// ErrNotImplemented marks learner-owned Coordinator methods before implementation.
var ErrNotImplemented = errors.New("TODO(learner): not implemented")

// EventDecision reports how Coordinator handled an incoming notification event.
type EventDecision string

const (
	// DecisionStarted means the event snapshotted the pre-window and started collecting the post-window.
	DecisionStarted EventDecision = "started"
	// DecisionDuplicate means the event key is already active or already exported in this process.
	DecisionDuplicate EventDecision = "duplicate"
	// DecisionBusy means another event with a different key is currently collecting its post-window.
	DecisionBusy EventDecision = "busy"
)

// CoordinatorConfig defines fixed window, gap, and bounded-memory limits for Coordinator.
type CoordinatorConfig struct {
	PreWindow      time.Duration
	PostWindow     time.Duration
	MaxGap         time.Duration
	MaxTaskSamples int
	MaxSeenKeys    int
}

type activeTask struct {
	event     scene.Event
	requested scene.Window
	task      *Task
}

// Coordinator is the single serial owner of Ring, the active Task, and Exporter handoff.
// Because one owner loop drives OnSample, OnEvent, and Tick serially, Ring and Task need no locks.
type Coordinator struct {
	buf       Buffer
	exp       Exporter
	cfg       CoordinatorConfig
	active    *activeTask
	seen      map[string]struct{}
	seenOrder []string
}

// NewCoordinator constructs a Coordinator with validated dependencies and bounded limits.
func NewCoordinator(buf Buffer, exp Exporter, cfg CoordinatorConfig) (*Coordinator, error) {
	if buf == nil {
		return nil, errors.New("buffer must not be nil")
	}
	if exp == nil {
		return nil, errors.New("exporter must not be nil")
	}
	if cfg.PreWindow < 0 || cfg.PostWindow < 0 {
		return nil, errors.New("pre/post windows must be non-negative")
	}
	if cfg.MaxGap <= 0 {
		return nil, errors.New("max gap must be positive")
	}
	if cfg.MaxTaskSamples <= 0 {
		return nil, errors.New("max task samples must be positive")
	}
	if cfg.MaxSeenKeys <= 0 {
		return nil, errors.New("max seen keys must be positive")
	}
	return &Coordinator{
		buf:       buf,
		exp:       exp,
		cfg:       cfg,
		seen:      make(map[string]struct{}, cfg.MaxSeenKeys),
		seenOrder: make([]string, 0, cfg.MaxSeenKeys),
	}, nil
}

// HasActiveTask reports whether an event task is currently collecting its post-window.
func (c *Coordinator) HasActiveTask() bool {
	return c.active != nil
}

// HasSeen reports whether key is currently recorded as exported in the bounded in-process table.
func (c *Coordinator) HasSeen(key string) bool {
	_, ok := c.seen[key]
	return ok
}

// markSeen records key after a successful export, evicting the oldest key if at MaxSeenKeys.
func (c *Coordinator) markSeen(key string) {
	if _, exists := c.seen[key]; exists {
		return
	}
	if len(c.seenOrder) >= c.cfg.MaxSeenKeys {
		oldest := c.seenOrder[0]
		c.seenOrder = c.seenOrder[1:]
		delete(c.seen, oldest)
	}
	c.seen[key] = struct{}{}
	c.seenOrder = append(c.seenOrder, key)
}

func cloneEvent(ev scene.Event) scene.Event {
	copied := ev
	if ev.SourceTime != nil {
		t := *ev.SourceTime
		copied.SourceTime = &t
	}
	return copied
}

// OnEvent 用于校验传入的告警事件，执行进程内去重以及单活跃事件守卫，
// 并在事件被接受时启动一个新的前置+后置时间窗口（pre+post window）任务。
//
// TODO(学习者):
// 1. 首先检查 ctx.Err()。
// 2. 校验 event.Key、event.Identity（Target 和 RunID）是否有效，以及 event.ReceivedAt 是否为非零值。
// 3. 去重处理：如果 event.Key 与 c.active.event.Key 匹配，或者 c.HasSeen(event.Key) 为真，则返回 DecisionDuplicate, nil。
// 4. 单活跃事件守卫：如果当前已有其他任务处于活跃状态，则返回 DecisionBusy, nil。
// 5. 结合 MaxGap 计算请求的时间窗口 [ReceivedAt-PreWindow, ReceivedAt+PostWindow]。
// 6. 调用 Start(c.buf, event, requested, c.cfg.MaxTaskSamples)。若发生错误，保持 c.active 为 nil，且不要执行 markSeen。
// 7. 存储 c.active（包含 cloneEvent(event)、requested 和 task），并返回 DecisionStarted, nil。
func (c *Coordinator) OnEvent(ctx context.Context, event scene.Event) (EventDecision, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if event.Key == "" || event.Identity.Target == "" || event.Identity.RunID == "" || event.ReceivedAt.IsZero() {
		return "", errors.New("invalid event: missing key, identity, or received time")
	}
	if c.active != nil && c.active.event.Key == event.Key {
		return DecisionDuplicate, nil
	}
	if c.HasSeen(event.Key) {
		return DecisionDuplicate, nil
	}
	if c.active != nil {
		return DecisionBusy, nil
	}
	requested := scene.Window{
		Start:  event.ReceivedAt.Add(-c.cfg.PreWindow),
		End:    event.ReceivedAt.Add(c.cfg.PostWindow),
		MaxGap: c.cfg.MaxGap,
	}
	task, err := Start(c.buf, event, requested, c.cfg.MaxTaskSamples)
	if err != nil {
		return "", err
	}
	c.active = &activeTask{
		event:     cloneEvent(event),
		requested: requested,
		task:      task,
	}
	return DecisionStarted, nil
}

// OnSample 将新的采样数据追加到有界缓冲区中；如果当前存在活跃的事件任务，
// 则同时将该采样数据投递给该任务。
//
// TODO(学习者):
//  1. 首先检查 ctx.Err()。
//  2. 调用 c.buf.Append(sample)；如果失败，直接返回错误，不要修改 c.active。
//  3. 如果 c.active != nil，则调用 c.active.task.Offer(sample)。
//     如果 Offer 失败（例如返回 ErrCapacity），将 c.active 置为 nil（不要执行 markSeen！）并返回该错误。
//  4. 返回 nil。
func (c *Coordinator) OnSample(ctx context.Context, sample scene.Sample) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := c.buf.Append(sample); err != nil {
		return err
	}
	if c.active != nil {
		if err := c.active.task.Offer(sample); err != nil {
			c.active = nil
			return err
		}
	}
	return nil
}

// Tick 将协调器时钟推进至 now。当活跃任务到达其时间窗口的 End 时，
// Tick 会结束该任务，通过 Exporter 写入筛选结果，并更新已见（seen）和活跃（active）状态。
// 调用方在调用 Tick(ctx, End) 之前，必须先通过 OnSample 投递所有时间戳位于 End 的采样数据。
//
// TODO(学习者):
//  1. 首先检查 ctx.Err()；若 now 为零值则拒绝（返回错误）。
//  2. 如果 c.active == nil，或者 now 早于 c.active.requested.End，则返回 false, nil。
//  3. 调用 sel, err := c.active.task.Finish(now)。
//     如果 err != nil，将 c.active 置为 nil（不要执行 markSeen！）并返回 false, err。
//  4. 调用 err = c.exp.Write(ctx, c.active.event, sel)，且无论成功与否，之后都要将 c.active 置为 nil。
//  5. 如果 Write 失败，不要调用 c.markSeen，直接返回 false, err。
//  6. 如果 Write 成功，调用 c.markSeen(ev.Key) 并返回 true, nil。
func (c *Coordinator) Tick(ctx context.Context, now time.Time) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if now.IsZero() {
		return false, errors.New("now must not be zero")
	}
	if c.active == nil || now.Before(c.active.requested.End) {
		return false, nil
	}
	sel, err := c.active.task.Finish(now)
	if err != nil {
		c.active = nil
		return false, err
	}
	err = c.exp.Write(ctx, c.active.event, sel)
	ev := c.active.event
	c.active = nil
	if err != nil {
		return false, err
	}
	c.markSeen(ev.Key)
	return true, nil
}
