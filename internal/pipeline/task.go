package pipeline

import (
	"errors"
	"time"
	"witness-go/internal/scene"
	"witness-go/internal/window"
)

var (
	ErrNotReady = errors.New("event window has not ended")
	ErrCapacity = errors.New("event sample capacity exceeded")
	ErrFinished = errors.New("event task already finished")
)

// Task holds one event's independent samples, owned by one serial caller.
// A failed task stays failed. A not-ready task can still receive samples.
type Task struct {
	event      scene.Event
	requested  scene.Window
	maxSamples int
	samples    []scene.Sample
	failure    error
	finished   bool
}

// Start snapshots now, validates with window.Select, and retains its Samples.
// Initial gaps are provisional; do not export them before the deadline.
func Start(buffer Buffer, event scene.Event, requested scene.Window, maxSamples int) (*Task, error) {
	var failure error = nil
	if buffer == nil {
		failure = errors.New("nil buffer")
		return nil, failure
	}
	if maxSamples <= 0 {
		failure = errors.New("maxSamples must be positive")
		return nil, failure
	}
	if event.SourceTime != nil {
		sourceTime := *event.SourceTime
		event.SourceTime = &sourceTime
	}
	Task := &Task{
		event:      event,
		requested:  requested,
		maxSamples: maxSamples,
		samples:    make([]scene.Sample, 0, maxSamples),
		finished:   false,
		failure:    failure,
	}
	samples := buffer.Snapshot()
	selection, err := window.Select(samples, event, requested)
	if err != nil {
		Task.failure = err
		return nil, err
	}
	if len(selection.Samples) > maxSamples {
		Task.failure = ErrCapacity
		return nil, ErrCapacity
	}
	Task.samples = selection.Samples
	return Task, nil
}

// Offer receives only newly dispatched samples, each exactly once after Start.
// Validate identity/time. Ignore other identities/out-of-window points.
// Clone accepted samples; capacity overflow sets failure and returns ErrCapacity.
func (t *Task) Offer(s scene.Sample) error {
	// TODO(learner): state guards, validate, filter, capacity check, clone and append.
	if t.finished {
		return ErrFinished
	}
	if t.failure != nil {
		return t.failure
	}
	if s.At.IsZero() {
		t.failure = errors.New("sample.At is zero")
		return t.failure
	}
	if s.Identity.RunID == "" || s.Identity.Target == "" {
		t.failure = errors.New("sample.Identity is incomplete")
		return t.failure
	}
	if s.Identity != t.event.Identity {
		return nil
	}
	if s.At.Before(t.requested.Start) || s.At.After(t.requested.End) {
		return nil
	}
	if t.maxSamples <= len(t.samples) {
		t.failure = ErrCapacity
		return t.failure
	}
	t.samples = append(t.samples, s.Clone())

	return nil
}

// Finish uses injected time. At End, dispatch that sample before calling Finish.
// If now is before End return ErrNotReady without finishing the task.
// Otherwise call window.Select(t.samples, t.event, t.requested).
func (t *Task) Finish(now time.Time) (scene.Selection, error) {
	// TODO(learner): reject zero time, failed/finished state; readiness; Select; finish.
	if now.IsZero() {
		return scene.Selection{}, errors.New("now is zero")
	}
	if t.failure != nil {
		return scene.Selection{}, t.failure
	}
	if t.finished {
		return scene.Selection{}, ErrFinished
	}
	if now.Before(t.requested.End) {
		return scene.Selection{}, ErrNotReady
	}
	selection, err := window.Select(t.samples, t.event, t.requested)
	if err != nil {
		t.failure = err
		return scene.Selection{}, err
	}
	t.finished = true
	return selection, nil
}
