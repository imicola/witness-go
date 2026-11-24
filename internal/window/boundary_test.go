package window

import (
	"witness-go/internal/scene"
	"testing"
	"time"
)

func TestSelectRejectsInvalidInput(t *testing.T) {
	at := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	id := scene.Identity{Target: "a", RunID: "one"}
	event := scene.Event{Identity: id, ReceivedAt: at}
	w := scene.Window{Start: at, End: at.Add(40 * time.Second), MaxGap: 10 * time.Second}
	cases := []struct {
		name    string
		event   scene.Event
		w       scene.Window
		samples []scene.Sample
	}{
		{"reversed", event, scene.Window{Start: w.End, End: w.Start, MaxGap: w.MaxGap}, nil},
		{"zero_gap", event, scene.Window{Start: w.Start, End: w.End}, nil},
		{"negative_gap", event, scene.Window{Start: w.Start, End: w.End, MaxGap: -time.Second}, nil},
		{"empty_identity", scene.Event{ReceivedAt: at}, w, nil},
		{"zero_received", scene.Event{Identity: id}, w, nil},
		{"zero_window_time", event, scene.Window{End: w.End, MaxGap: w.MaxGap}, nil},
		{"zero_sample_time", event, w, []scene.Sample{{Identity: id}}},
		{"duplicates", event, w, []scene.Sample{{Identity: id, At: at}, {Identity: id, At: at}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Select(c.samples, c.event, c.w)
			if err == nil {
				t.Error("expected error; got nil")
			}
		})
	}
}

func TestSelectReportsCoverageAndGaps(t *testing.T) {
	at := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	id := scene.Identity{Target: "a", RunID: "one"}
	event := scene.Event{Identity: id, ReceivedAt: at}
	w := scene.Window{Start: at, End: at.Add(40 * time.Second), MaxGap: 10 * time.Second}
	sample := func(sec int, run string) scene.Sample {
		return scene.Sample{Identity: scene.Identity{Target: "a", RunID: run}, At: at.Add(time.Duration(sec) * time.Second)}
	}
	in := []scene.Sample{sample(35, "one"), sample(15, "one"), sample(5, "one"), sample(20, "two"), sample(-1, "one"), sample(41, "one")}
	out, err := Select(in, event, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Samples) != 3 || !out.Samples[0].At.Equal(at.Add(5*time.Second)) || !in[0].At.Equal(at.Add(35*time.Second)) {
		t.Fatal("filter/sort/input changed")
	}
	want := map[string][2]int{"leading": {0, 5}, "internal": {15, 35}, "trailing": {35, 40}}
	if len(out.Gaps) != 3 {
		t.Fatal(out.Gaps)
	}
	for _, g := range out.Gaps {
		v, ok := want[g.Kind]
		if !ok || !g.From.Equal(at.Add(time.Duration(v[0])*time.Second)) || !g.To.Equal(at.Add(time.Duration(v[1])*time.Second)) {
			t.Fatal(g)
		}
	}
	empty, err := Select(nil, event, w)
	if err != nil {
		t.Fatal(err)
	}
	if empty.First != nil || empty.Last != nil || len(empty.Gaps) != 1 || empty.Gaps[0].Kind != "no_samples" {
		t.Fatal(empty)
	}
	full, err := Select([]scene.Sample{sample(0, "one"), sample(10, "one"), sample(20, "one"), sample(30, "one"), sample(40, "one")}, event, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Samples) != 5 || len(full.Gaps) != 0 {
		t.Fatal(full)
	}
}

func TestSelectCopiesCPU(t *testing.T) {
	at := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	id := scene.Identity{Target: "a", RunID: "one"}
	in := []scene.Sample{{Identity: id, At: at, CPU: &scene.CPU{UsageUS: 7}}}
	out, err := Select(in, scene.Event{Identity: id, ReceivedAt: at}, scene.Window{Start: at, End: at, MaxGap: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	out.Samples[0].CPU.UsageUS = 99
	if in[0].CPU.UsageUS != 7 {
		t.Fatal("changing result changed input CPU")
	}
}
