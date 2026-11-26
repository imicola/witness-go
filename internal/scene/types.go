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
	PSI      *PSI      `json:"psi,omitempty"`
}

// Clone returns a deep copy of Sample so buffer/task/selection ownership stays isolated.
func (s Sample) Clone() Sample {
	copied := s
	if s.CPU != nil {
		cpu := *s.CPU
		copied.CPU = &cpu
	}
	if s.PSI != nil {
		psi := *s.PSI
		copied.PSI = &psi
	}
	return copied
}

type CPU struct {
	UsageUS     uint64 `json:"usage_us"`
	ThrottledUS uint64 `json:"throttled_us"`
	NrThrottled uint64 `json:"nr_throttled,omitempty"`
	// QuotaUS / PeriodUS come from cgroup v2 cpu.max; QuotaUnlimited is true when quota is "max".
	QuotaUS        uint64 `json:"quota_us,omitempty"`
	PeriodUS       uint64 `json:"period_us,omitempty"`
	QuotaUnlimited bool   `json:"quota_unlimited,omitempty"`

	// Delta fields are valid only when computed from monotonic cumulative counters
	// of the same (Target, RunID). Never fabricate zero deltas on initial, run change, or reset.
	DeltaValid       bool   `json:"delta_valid,omitempty"`
	DeltaUsageUS     uint64 `json:"delta_usage_us,omitempty"`
	DeltaThrottledUS uint64 `json:"delta_throttled_us,omitempty"`
	DeltaNrThrottled uint64 `json:"delta_nr_throttled,omitempty"`
	DeltaReason      string `json:"delta_reason,omitempty"`
}

// PSI captures host CPU Pressure Stall Information from /proc/pressure/cpu.
type PSI struct {
	SomeAvg10   float64 `json:"some_avg10"`
	SomeAvg60   float64 `json:"some_avg60"`
	SomeAvg300  float64 `json:"some_avg300"`
	SomeTotalUS uint64  `json:"some_total_us"`
	HasFull     bool    `json:"has_full,omitempty"`
	FullAvg10   float64 `json:"full_avg10,omitempty"`
	FullAvg60   float64 `json:"full_avg60,omitempty"`
	FullAvg300  float64 `json:"full_avg300,omitempty"`
	FullTotalUS uint64  `json:"full_total_us,omitempty"`
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
