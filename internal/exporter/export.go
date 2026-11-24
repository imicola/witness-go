// Package exporter persists finished window selections atomically and
// strictly re-reads exported artifacts for offline replay.
package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"witness-go/internal/pipeline"
	"witness-go/internal/scene"
	"witness-go/internal/window"
	"os"
	"path/filepath"
	"time"
)

var _ pipeline.Exporter = (*FileExporter)(nil)

var ErrNotImplemented = errors.New("TODO(learner): not implemented")

// FileExporter writes one scene.Export JSON file to Path.
// It owns the temporary file lifecycle during Write.
type FileExporter struct {
	Path       string
	Provenance string
	Now        func() time.Time
}

// NewFileExporter constructs a FileExporter. Validate non-empty path and provenance.
func NewFileExporter(path, provenance string, now func() time.Time) (*FileExporter, error) {
	if path == "" {
		return nil, errors.New("export path must not be empty")
	}
	if provenance == "" {
		return nil, errors.New("provenance must not be empty")
	}
	if now == nil {
		now = time.Now
	}
	return &FileExporter{
		Path:       path,
		Provenance: provenance,
		Now:        now,
	}, nil
}

// Write 构建一个 scene.Export 并将其原子性地持久化到 e.Path。
// TODO(learner):
// 1. 在开始 I/O 操作前检查 ctx.Err()。
// 2. 在写入前校验 event（Identity、ReceivedAt）和 selection（Requested、Gaps 非 nil 等）。
// 3. 构建 scene.Export，设置 SchemaVersion=scene.ExportSchemaVersion、Provenance=e.Provenance、ExportedAt=e.Now()。
// 4. 在 filepath.Dir(e.Path)（与目标文件同一文件系统）中创建临时文件，并注册失败时的清理逻辑。
// 5. 编码 JSON，显式检查 tmp.Close() 的错误，随后执行 os.Rename(tmpPath, e.Path)。
// 6. 发生任何失败时，删除临时文件，保持原有的目标文件完好无损，并返回错误。
func (e *FileExporter) Write(ctx context.Context, event scene.Event, sel scene.Selection) error {
	if event.Identity.Target == "" || event.Identity.RunID == "" || event.ReceivedAt.IsZero() {
		return errors.New("event missing required fields")
	}
	if sel.Requested.Start.IsZero() || sel.Requested.End.IsZero() || sel.Requested.MaxGap <= 0 {
		return errors.New("selection.requested missing required fields")
	}
	if sel.Gaps == nil {
		return errors.New("selection.gaps must not be nil")
	}
	if sel.Requested.Start.After(sel.Requested.End) {
		return errors.New("selection.requested start is after end")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Dir(e.Path)
	tmp, err := os.CreateTemp(dir, "export-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(scene.Export{
		SchemaVersion: scene.ExportSchemaVersion,
		Provenance:    e.Provenance,
		ExportedAt:    e.Now(),
		Event:         event,
		Selection:     sel,
	}); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, e.Path); err != nil {
		return err
	}
	keep = true
	return nil
}

// Read 从 r 中严格解码并校验导出的 JSON 工件（artifact）。
// TODO(learner):
// 1. 使用 json.NewDecoder(r) 并开启 DisallowUnknownFields() 进行解码，随后拒绝末尾多余的 JSON token（!= io.EOF）。
// 2. 校验 SchemaVersion（必须与 scene.ExportSchemaVersion 匹配）、Provenance、ExportedAt、Event 以及 Requested 时间窗口。
// 3. 校验 Selection 的语义一致性：
//   - Samples 必须与 Event.Identity 匹配，落在 [Requested.Start, Requested.End] 区间内，保持严格递增（无重复项），且具有非零的 At 时间戳。
//   - 当 len(Samples) == 0 时：First 和 Last 必须为 nil，且 Gaps 必须包含单个与 [Requested.Start, Requested.End] 匹配的 "no_samples" 间隙记录。
//   - 当 len(Samples) > 0 时：First/Last 必须分别与 Samples[0].At / Samples[len-1].At 匹配，且 Gaps 必须为非 nil 并与首部（leading）、内部（internal）和尾部（trailing）边界保持一致。
func sameSampleSeq(a, b []scene.Sample) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Identity != b[i].Identity || !a[i].At.Equal(b[i].At) {
			return false
		}
		if (a[i].CPU == nil) != (b[i].CPU == nil) {
			return false
		}
		if a[i].CPU != nil && *a[i].CPU != *b[i].CPU {
			return false
		}
	}
	return true
}

func sameTime(a, b *time.Time) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || a.Equal(*b)
}

func sameGaps(a, b []scene.Gap) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || !a[i].From.Equal(b[i].From) || !a[i].To.Equal(b[i].To) {
			return false
		}
	}
	return true
}

func Read(r io.Reader) (scene.Export, error) {
	var rec scene.Export
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return scene.Export{}, fmt.Errorf("failed to decode export JSON: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return rec, fmt.Errorf("unexpected extra JSON after export: %w", err)
	}
	if rec.SchemaVersion == "" || rec.Provenance == "" || rec.ExportedAt.IsZero() {
		return rec, errors.New("export missing required fields")
	}
	if rec.SchemaVersion != scene.ExportSchemaVersion {
		return rec, fmt.Errorf("export schema version mismatch: got %q, want %q", rec.SchemaVersion, scene.ExportSchemaVersion)
	}
	isSorted := func(samples []scene.Sample) bool {
		for i := 1; i < len(samples); i++ {
			if !samples[i].At.After(samples[i-1].At) {
				return false
			}
		}
		return true
	}
	if !isSorted(rec.Selection.Samples) {
		return rec, errors.New("selection.samples must be sorted by At in strictly increasing order")
	}
	want, err := window.Select(rec.Selection.Samples, rec.Event, rec.Selection.Requested)
	if err != nil {
		return rec, fmt.Errorf("selection validation failed: %w", err)
	}
	// 以 window.Select 的规范输出为准，拒绝 Samples/First/Last/Gaps 与事件或窗口矛盾的手改工件。
	if !sameSampleSeq(rec.Selection.Samples, want.Samples) ||
		!sameTime(rec.Selection.First, want.First) ||
		!sameTime(rec.Selection.Last, want.Last) ||
		!sameGaps(rec.Selection.Gaps, want.Gaps) {
		return rec, errors.New("selection inconsistent with event and requested window")
	}
	return rec, nil
}

// ReadFile opens path and delegates strict decoding/validation to Read.
func ReadFile(path string) (scene.Export, error) {
	if path == "" {
		return scene.Export{}, errors.New("export file path must not be empty")
	}
	f, err := os.Open(path)
	if err != nil {
		return scene.Export{}, err
	}
	defer f.Close()
	return Read(f)
}

// Summary is a structured view of a re-read export artifact for offline inspection.
type Summary struct {
	SchemaVersion string     `json:"schema_version"`
	Provenance    string     `json:"provenance"`
	Target        string     `json:"target"`
	RunID         string     `json:"run_id"`
	SampleCount   int        `json:"sample_count"`
	First         *time.Time `json:"first,omitempty"`
	Last          *time.Time `json:"last,omitempty"`
	GapKinds      []string   `json:"gap_kinds"`
}

// Summarize 从已校验的 Export 中提取简明的离线审查摘要。
// TODO(learner): 从 exp 中填充 Summary 字段（保持 GapKinds 非 nil，例如没有间隙时使用空切片）。
func Summarize(exp scene.Export) (Summary, error) {
	summary := Summary{
		SchemaVersion: exp.SchemaVersion,
		Provenance:    exp.Provenance,
		Target:        exp.Event.Identity.Target,
		RunID:         exp.Event.Identity.RunID,
		SampleCount:   len(exp.Selection.Samples),
	}
	if len(exp.Selection.Samples) > 0 {
		summary.First = exp.Selection.First
		summary.Last = exp.Selection.Last
	}
	summary.GapKinds = make([]string, len(exp.Selection.Gaps))
	for i, gap := range exp.Selection.Gaps {
		summary.GapKinds[i] = gap.Kind
	}
	return summary, nil
}
