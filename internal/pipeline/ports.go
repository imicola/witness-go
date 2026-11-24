// Package pipeline defines later assembly boundaries; no online loop is wired yet.
package pipeline

import (
	"context"
	"witness-go/internal/scene"
)

type Sampler interface {
	Sample(context.Context) (scene.Sample, error)
}

// Snapshot must return owned data including copied pointer values.
type Buffer interface {
	Append(scene.Sample) error
	Snapshot() []scene.Sample
}

type EventSource interface {
	Next(context.Context) (scene.Event, error)
}

type Exporter interface {
	Write(context.Context, scene.Event, scene.Selection) error
}

// Replay reports evidence coverage; CPU diagnosis and probe recovery are distinct.
type Replayer interface {
	Replay(context.Context, scene.Event, scene.Selection) (string, error)
}
