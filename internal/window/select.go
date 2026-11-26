// Package window owns the learner's point-sample selection rule.
package window

import (
	"errors"
	"sort"
	"time"
	"witness-go/internal/scene"
)

// Select must not mutate input. It receives a completed snapshot, not a live buffer.
// Contract proposal is in README.md; agree on boundary/gap semantics before coding.
func Select(samples []scene.Sample, event scene.Event, requested scene.Window) (scene.Selection, error) {
	// 检查窗口，事件身份与必需时间是否合法。
	if requested.Start.After(requested.End) {
		return scene.Selection{}, errors.New("窗口开始时间晚于结束时间")
	}
	if requested.MaxGap <= 0 {
		return scene.Selection{}, errors.New("窗口最大间隙为负数")
	}
	if event.Identity.Target == "" || event.Identity.RunID == "" {
		return scene.Selection{}, errors.New("事件身份不为空")
	}
	if requested.Start.IsZero() || requested.End.IsZero() {
		return scene.Selection{}, errors.New("窗口开始或结束时间为零")
	}
	if event.ReceivedAt.IsZero() {
		return scene.Selection{}, errors.New("事件接收时间为零")
	}
	for _, s := range samples {
		if s.Identity.RunID == "" || s.Identity.Target == "" {
			return scene.Selection{}, errors.New("样本身份不为空")
		}
		if s.At.IsZero() {
			return scene.Selection{}, errors.New("样本时间戳不为零")
		}
		if s.At.Before(requested.Start) || s.At.After(requested.End) {
			continue
		}
	}
	needSamples := make([]scene.Sample, 0)
	first, last := (*time.Time)(nil), (*time.Time)(nil)
	for _, s := range samples {
		if s.Identity != event.Identity {
			continue
		}
		if s.At.Before(requested.Start) || s.At.After(requested.End) {
			continue
		}
		needSamples = append(needSamples, s.Clone())
		if first == nil || s.At.Before(*first) {
			first = &s.At
		}
		if last == nil || s.At.After(*last) {
			last = &s.At
		}
	}
	res := scene.Selection{
		Requested: requested,
		Samples:   needSamples,
		First:     first,
		Last:      last,
		Gaps:      []scene.Gap{},
	}
	// 按到达时间左端点排序
	sort.Slice(res.Samples, func(i, j int) bool {
		return res.Samples[i].At.Before(res.Samples[j].At)
	})
	// 重复检验
	for i := 1; i < len(res.Samples); i++ {
		if res.Samples[i].At.Equal(res.Samples[i-1].At) {
			return scene.Selection{}, errors.New("样本时间戳重复")
		}
	}
	// 计算缺口
	if len(res.Samples) > 0 {
		prev := res.Samples[0].At
		for i := 1; i < len(res.Samples); i++ {
			curr := res.Samples[i].At
			if curr.Sub(prev) > requested.MaxGap {
				res.Gaps = append(res.Gaps, scene.Gap{
					Kind: "internal",
					From: prev,
					To:   curr,
				})
			}
			prev = curr
		}
	}
	// 计算前导和尾随缺口
	if first != nil && requested.Start.Before(*first) {
		res.Gaps = append(res.Gaps, scene.Gap{
			Kind: "leading",
			From: requested.Start,
			To:   *first,
		})
	}
	if last != nil && requested.End.After(*last) {
		res.Gaps = append(res.Gaps, scene.Gap{
			Kind: "trailing",
			From: *last,
			To:   requested.End,
		})
	}
	if len(res.Samples) == 0 {
		res.Gaps = append(res.Gaps, scene.Gap{
			Kind: "no_samples",
			From: requested.Start,
			To:   requested.End,
		})
	}
	return res, nil
}
