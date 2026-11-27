package pipeline

import (
	"context"
	"errors"
	"io"
	"time"
	"witness-go/internal/scene"
)

// RunnerConfig configures the concurrent owner loop that drives Coordinator.
type RunnerConfig struct {
	// SampleInterval controls the internal time.NewTicker when TickCh is nil.
	SampleInterval time.Duration
	// TickCh optionally overrides the internal ticker for deterministic tests.
	// In production, leave TickCh nil and set SampleInterval > 0.
	TickCh <-chan time.Time
	// Now provides the clock used for fallback Tick timestamps when a sample fails
	// or returns a zero timestamp. Defaults to time.Now.
	Now func() time.Time
	// OnDecision is optionally called on the owner goroutine after each accepted/deduped/busy event.
	OnDecision func(event scene.Event, decision EventDecision)
	// OnError is optionally called on the owner goroutine for non-fatal stage errors
	// ("sample", "on_sample", "on_event", "tick").
	OnError func(stage string, err error)
}

// Runner bridges a periodic Sampler and a blocking EventSource into a single lock-free
// owner loop that exclusively drives Coordinator.
type Runner struct {
	coord   *Coordinator
	sampler Sampler
	events  EventSource
	cfg     RunnerConfig
}

// NewRunner validates dependencies and builds a Runner.
func NewRunner(coord *Coordinator, sampler Sampler, events EventSource, cfg RunnerConfig) (*Runner, error) {
	if coord == nil {
		return nil, errors.New("coordinator must not be nil")
	}
	if sampler == nil {
		return nil, errors.New("sampler must not be nil")
	}
	if events == nil {
		return nil, errors.New("event source must not be nil")
	}
	if cfg.TickCh == nil && cfg.SampleInterval <= 0 {
		return nil, errors.New("sample interval must be positive when TickCh is nil")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Runner{
		coord:   coord,
		sampler: sampler,
		events:  events,
		cfg:     cfg,
	}, nil
}

func (r *Runner) reportError(stage string, err error) {
	if err != nil && r.cfg.OnError != nil {
		r.cfg.OnError(stage, err)
	}
}

func (r *Runner) tickNow(tickAt time.Time) time.Time {
	if r.cfg.Now != nil {
		if now := r.cfg.Now(); !now.IsZero() {
			return now
		}
	}
	return tickAt
}

// Run 启动单 owner 并发驱动循环，直到 ctx 取消或 EventSource 返回非 io.EOF 的致命错误。
//
// TODO(学习者): （实现时只需补充导入 "io"）
//  1. 入口快速检查：若 ctx.Err() != nil，直接返回 ctx.Err()，不启动子协程。
//  2. 创建可取消子上下文 runCtx, cancel := context.WithCancel(ctx)，以及：
//     - evCh := make(chan scene.Event)（无缓冲即可，让事件读取协程与 owner 顺序交接）
//     - evErrCh := make(chan error, 1)（缓冲 1，避免致命错误发送阻塞）
//     - readerDone := make(chan struct{})（退出屏障，确保 Run 返回前读取协程已完全退出）
//     并使用 defer 或统一收尾路径保证：cancel() 后必须 <-readerDone 再返回。
//  3. 确定采样节拍通道 tickCh：
//     - 若 r.cfg.TickCh != nil，直接使用 r.cfg.TickCh；
//     - 否则 ticker := time.NewTicker(r.cfg.SampleInterval)，defer ticker.Stop()，tickCh = ticker.C。
//  4. 启动唯一的事件读取协程：
//     - defer close(readerDone) 与 defer close(evCh)；
//     - 循环调用 ev, err := r.events.Next(runCtx)：
//     * 若 err != nil：
//     - 若 errors.Is(err, io.EOF) 或 runCtx.Err() != nil，直接 return（正常结束事件流）；
//     - 否则将致命 err 通过 select { case evErrCh <- err: case <-runCtx.Done(): } 发出后 return。
//     * 若 err == nil：
//     - 必须用 select { case evCh <- ev: case <-runCtx.Done(): return } 转发，绝不能裸写 evCh <- ev！
//  5. 在当前 owner 协程执行主 select 循环（只有本协程能调用 r.sampler 与 r.coord）：
//     - case <-ctx.Done():
//     取消 runCtx，等待 <-readerDone，返回 ctx.Err()。
//     - case err := <-evErrCh:
//     取消 runCtx，等待 <-readerDone，返回 err。
//     - case ev, ok := <-evCh:
//     若 !ok，将 evCh = nil（禁用该分支，避免关闭通道空转）并继续；
//     否则调用 dec, err := r.coord.OnEvent(runCtx, ev)：
//     若 err != nil 且 runCtx.Err() == nil，调用 r.reportError("on_event", err)；
//     若 err == nil 且 r.cfg.OnDecision != nil，调用 r.cfg.OnDecision(ev, dec)。
//     - case tickAt, ok := <-tickCh:
//     若 !ok，将 tickCh = nil 并继续；
//     否则按“先 Sample/OnSample，再 Tick”顺序执行：
//     a) sample, err := r.sampler.Sample(runCtx)：
//     - 若 err != nil：若 runCtx.Err() != nil 则按取消收尾返回 ctx.Err()；
//     否则 r.reportError("sample", err)，并仍调用 r.coord.Tick(runCtx, r.tickNow(tickAt))
//     （Tick 失败且未取消时 r.reportError("tick", tickErr)），防止活动任务因单次采样失败永久卡住。
//     - 若 err == nil：
//     先调用 r.coord.OnSample(runCtx, sample)（失败且未取消时 r.reportError("on_sample", err)）；
//     再取 tickNow := sample.At（若为零值则用 r.tickNow(tickAt)）调用 r.coord.Tick(runCtx, tickNow)
//     （失败且未取消时 r.reportError("tick", err)）。
func (r *Runner) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	runCtx, cancel := context.WithCancel(ctx)
	evCh := make(chan scene.Event)
	evErrCh := make(chan error, 1)
	readerDone := make(chan struct{})
	tickCh := r.cfg.TickCh
	defer func() {
		cancel()
		<-readerDone
	}()
	if r.cfg.TickCh == nil {
		ticker := time.NewTicker(r.cfg.SampleInterval)
		defer ticker.Stop()
		tickCh = ticker.C
	}
	go func() {
		defer close(readerDone)
		defer close(evCh)
		for {
			ev, err := r.events.Next(runCtx)
			if err != nil {
				if errors.Is(err, io.EOF) || runCtx.Err() != nil {
					return
				}
				select {
				case evErrCh <- err:
				case <-runCtx.Done():
				}
				return
			}
			select {
			case evCh <- ev:
			case <-runCtx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-evErrCh:
			return err
		case ev, ok := <-evCh:
			if !ok {
				evCh = nil
			} else {
				dec, err := r.coord.OnEvent(runCtx, ev)
				if err != nil && runCtx.Err() == nil {
					r.reportError("on_event", err)
				}
				if err == nil && r.cfg.OnDecision != nil {
					r.cfg.OnDecision(ev, dec)
				}
			}
		case tickAt, ok := <-tickCh:
			if !ok {
				tickCh = nil
			} else {
				sample, err := r.sampler.Sample(runCtx)
				if err != nil {
					if runCtx.Err() != nil {
						return ctx.Err()
					}
					r.reportError("sample", err)
					_, tickErr := r.coord.Tick(runCtx, r.tickNow(tickAt))
					if tickErr != nil && runCtx.Err() == nil {
						r.reportError("tick", tickErr)
					}

				} else {
					err := r.coord.OnSample(runCtx, sample)
					if err != nil && runCtx.Err() == nil {
						r.reportError("on_sample", err)
					}
					tickNow := sample.At
					if tickNow.IsZero() {
						tickNow = r.tickNow(tickAt)
					}
					_, tickErr := r.coord.Tick(runCtx, tickNow)
					if tickErr != nil && runCtx.Err() == nil {
						r.reportError("tick", tickErr)
					}
				}
			}
		}
	}
}
