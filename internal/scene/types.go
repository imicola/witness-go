package scene

import "time"

// Identity separates a logical target from one running counter source.
type Identity struct {
	Target string `json:"target"`
	RunID  string `json:"run_id"`
}

// Sample is a point observation. Request intervals need a separate adapter.
type Sample struct {
	Identity Identity  `json:"identity"`
	At       time.Time `json:"at"`
	CPU      *CPU      `json:"cpu,omitempty"`
}

type CPU struct {
	UsageUS     uint64 `json:"usage_us"`
	ThrottledUS uint64 `json:"throttled_us"`
}

type Event struct {
	Key        string    `json:"key"`
	Identity   Identity  `json:"identity"`
	ReceivedAt time.Time `json:"received_at"`
	// SourceTime preserves a source field; it is not the fault onset.
	SourceTime *time.Time `json:"source_time,omitempty"`
}

// Window is a proposed closed interval [Start, End], for point samples only.
type Window struct {
	Start  time.Time     `json:"start"`
	End    time.Time     `json:"end"`
	MaxGap time.Duration `json:"max_gap_ns"`
}

type Gap struct {
	Kind string    `json:"kind"` // leading, internal, trailing, no_samples
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type Selection struct {
	Requested Window     `json:"requested"`
	Samples   []Sample   `json:"samples"`
	First     *time.Time `json:"first,omitempty"`
	Last      *time.Time `json:"last,omitempty"`
	Gaps      []Gap      `json:"gaps"`
}

type Fixture struct {
	Provenance string   `json:"provenance"`
	Event      Event    `json:"event"`
	Window     Window   `json:"window"`
	Samples    []Sample `json:"samples"`
}

const ExportSchemaVersion = "v1"

// Export is the self-contained artifact written after a window task finishes.
// Keep Provenance explicit so synthetic fixtures are never mistaken for live runs.
type Export struct {
	SchemaVersion string    `json:"schema_version"`
	Provenance    string    `json:"provenance"`
	ExportedAt    time.Time `json:"exported_at"`
	Event         Event     `json:"event"`
	Selection     Selection `json:"selection"`
}
