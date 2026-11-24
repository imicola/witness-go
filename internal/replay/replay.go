package replay

import "time"

type Target struct {
	Job      string
	Instance string
}

// Sample represents one observation with the original scrape timestamp.
type Sample struct {
	Target    Target
	ScrapedAt time.Time
	Value     int
}

type Verdict string

const (
	InsufficientEvidence Verdict = "insufficient evidence"
	ProbeFailed          Verdict = "probe failed"
	PathRecovered        Verdict = "probe path recovered"
)

// Judge evaluates the samples for the expected target at evaluatedAt.
// A nil sample means the instant query returned no sample for that metric.
func Judge(target Target, evaluatedAt time.Time, maxAge time.Duration, up, probe *Sample) Verdict {
	// TODO: Check whether the two samples support a verdict.
	if up == nil || probe == nil {
		return InsufficientEvidence
	}

	if up.ScrapedAt.Equal(probe.ScrapedAt) != true {
		return InsufficientEvidence
	}

	if up.Target != target || probe.Target != target{
		return InsufficientEvidence
	}

	age := evaluatedAt.Sub(up.ScrapedAt)
	if age > maxAge || age < 0 {
		return InsufficientEvidence
	}
	if up.Value == 1 {
		if probe.Value == 0 {
			return ProbeFailed
		}
		if probe.Value == 1 {
			return PathRecovered
		}
	}
	return InsufficientEvidence
}
