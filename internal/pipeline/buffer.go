package pipeline

import (
	"errors"
	"witness-go/internal/scene"
)

// Ring is owned by one serial caller. It is not concurrency safe.
// next is the next slot to write; count is the number of valid slots.
type Ring struct {
	slots []scene.Sample
	next  int
	count int
}

func NewRing(capacity int) (*Ring, error) {
	if capacity <= 0 {
		return nil, errors.New("buffer capacity must be positive")
	}
	return &Ring{slots: make([]scene.Sample, capacity)}, nil
}

// Append takes its own copy, including CPU. Overwrite the oldest slot when full.
func (r *Ring) Append(s scene.Sample) error {
	if s.At.IsZero() {
		return errors.New("sample.At is zero")
	}
	if s.Identity.RunID == "" || s.Identity.Target == "" {
		return errors.New("sample.Identity is incomplete")
	}
	r.slots[r.next] = s.Clone()
	r.next = (r.next + 1) % len(r.slots)
	if r.count < len(r.slots) {
		r.count++
	}
	return nil
}

// Snapshot returns owned samples in append order, not physical array order.
func (r *Ring) Snapshot() []scene.Sample {
	samples := make([]scene.Sample, r.count)
	oldest := (r.next - r.count + len(r.slots)) % len(r.slots)
	for i := 0; i < r.count; i++ {
		samples[i] = r.slots[(oldest+i)%len(r.slots)].Clone()
	}
	return samples
}
