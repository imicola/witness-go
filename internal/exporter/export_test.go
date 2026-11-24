package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"witness-go/internal/scene"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func ptrTime(t ...time.Time) *time.Time {
	if len(t) == 0 {
		return nil
	}
	return &t[0]
}

var recGood = scene.Export{
	SchemaVersion: scene.ExportSchemaVersion,
	Provenance:    "test",
	ExportedAt:    time.Unix(0, 0),
	Event: scene.Event{
		Key: "test",
		Identity: scene.Identity{
			Target: "a",
			RunID:  "run-a",
		},
		ReceivedAt: time.Unix(0, 0),
	},
	Selection: scene.Selection{
		Requested: scene.Window{
			Start:  time.Unix(0, 0),
			End:    time.Unix(30, 0),
			MaxGap: time.Second,
		},
		Samples: []scene.Sample{
			{
				Identity: scene.Identity{
					Target: "a",
					RunID:  "run-a",
				},
				At: time.Unix(10, 0),
				CPU: &scene.CPU{
					UsageUS:     100,
					ThrottledUS: 500,
				},
			},
			{
				Identity: scene.Identity{
					Target: "a",
					RunID:  "run-a",
				},
				At: time.Unix(20, 0),
			},
		},
		First: ptrTime(time.Unix(10, 0)),
		Last:  ptrTime(time.Unix(20, 0)),
		// 与 window.Select 的生产顺序一致：internal、leading、trailing。
		Gaps: []scene.Gap{
			{
				Kind: "internal",
				From: time.Unix(10, 0),
				To:   time.Unix(20, 0),
			},
			{
				Kind: "leading",
				From: time.Unix(0, 0),
				To:   time.Unix(10, 0),
			},
			{
				Kind: "trailing",
				From: time.Unix(20, 0),
				To:   time.Unix(30, 0),
			},
		},
	},
}

func Judge(rec scene.Export, good scene.Export) error {
	if rec.SchemaVersion != good.SchemaVersion {
		return errors.New("schema version mismatch")
	}
	if rec.Provenance != good.Provenance {
		return errors.New("provenance mismatch")
	}
	if !rec.ExportedAt.Equal(good.ExportedAt) {
		return errors.New("exported_at mismatch")
	}
	if rec.Event.Key != good.Event.Key ||
		rec.Event.Identity != good.Event.Identity ||
		!rec.Event.ReceivedAt.Equal(good.Event.ReceivedAt) {
		return errors.New("event mismatch")
	}
	requestedEqual := func(a, b scene.Window) bool {
		return a.Start.Equal(b.Start) && a.End.Equal(b.End) && a.MaxGap == b.MaxGap
	}
	if !requestedEqual(rec.Selection.Requested, good.Selection.Requested) {
		return errors.New("selection.requested mismatch")
	}
	if len(rec.Selection.Samples) != len(good.Selection.Samples) {
		return errors.New("selection.samples length mismatch")
	}
	deepEqual := func(a, b scene.Sample) bool {
		if a.Identity != b.Identity || !a.At.Equal(b.At) {
			return false
		}
		if (a.CPU == nil) != (b.CPU == nil) {
			return false
		}
		if a.CPU != nil && b.CPU != nil {
			if *a.CPU != *b.CPU {
				return false
			}
		}
		return true
	}
	for i := range rec.Selection.Samples {
		if !deepEqual(rec.Selection.Samples[i], good.Selection.Samples[i]) {
			return errors.New("selection.samples content mismatch")
		}
	}
	if (rec.Selection.First == nil) != (good.Selection.First == nil) ||
		(rec.Selection.First != nil && !rec.Selection.First.Equal(*good.Selection.First)) {
		return errors.New("selection.first mismatch")
	}
	if (rec.Selection.Last == nil) != (good.Selection.Last == nil) ||
		(rec.Selection.Last != nil && !rec.Selection.Last.Equal(*good.Selection.Last)) {
		return errors.New("selection.last mismatch")
	}
	if len(rec.Selection.Gaps) != len(good.Selection.Gaps) {
		return errors.New("selection.gaps length mismatch")
	}
	for i := range rec.Selection.Gaps {
		gapsEqual := func(a, b scene.Gap) bool {
			return a.Kind == b.Kind && a.From.Equal(b.From) && a.To.Equal(b.To)
		}
		if !gapsEqual(rec.Selection.Gaps[i], good.Selection.Gaps[i]) {
			return errors.New("selection.gaps content mismatch")
		}
	}
	return nil
}

func TestFileExporterRoundTrip(t *testing.T) {
	// t.Skip("TODO(learner): 实现 Write 与 ReadFile 后补齐正常导出与重读闭环断言（含 SchemaVersion/Provenance/First/Last/Gaps）")
	path := t.TempDir() + "/export.json"
	provenance := recGood.Provenance
	Now := time.Unix(0, 0)
	e, err := NewFileExporter(path, provenance, func() time.Time { return Now })
	if err != nil {
		t.Fatalf("NewFileExporter failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := e.Write(ctx, recGood.Event, recGood.Selection); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	recRead, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if err := Judge(recRead, recGood); err != nil {
		t.Fatalf("ReadFile result mismatch: %v", err)
	}
}

func TestFileExporterAtomicFailurePreservesTarget(t *testing.T) {
	// t.Skip("TODO(learner): 构造已有目标文件，触发 Write 失败（如 ctx 取消、校验失败或不可写场景），验证返回 error、原文件未被破坏且目录无残留临时文件")
	path := t.TempDir() + "/export.json"
	// 与 recGood 语义一致的合法导出；时间用 UTC 表示，Judge 以 instant 比较不受时区影响。
	baseline := `{
  "schema_version": "v1",
  "provenance": "test",
  "exported_at": "1970-01-01T00:00:00Z",
  "event": {
    "key": "test",
    "identity": {
      "target": "a",
      "run_id": "run-a"
    },
    "received_at": "1970-01-01T00:00:00Z"
  },
  "selection": {
    "requested": {
      "start": "1970-01-01T00:00:00Z",
      "end": "1970-01-01T00:00:30Z",
      "max_gap_ns": 1000000000
    },
    "samples": [
      {
        "identity": {
          "target": "a",
          "run_id": "run-a"
        },
        "at": "1970-01-01T00:00:10Z",
        "cpu": {
          "usage_us": 100,
          "throttled_us": 500
        }
      },
      {
        "identity": {
          "target": "a",
          "run_id": "run-a"
        },
        "at": "1970-01-01T00:00:20Z"
      }
    ],
    "first": "1970-01-01T00:00:10Z",
    "last": "1970-01-01T00:00:20Z",
    "gaps": [
      {
        "kind": "internal",
        "from": "1970-01-01T00:00:10Z",
        "to": "1970-01-01T00:00:20Z"
      },
      {
        "kind": "leading",
        "from": "1970-01-01T00:00:00Z",
        "to": "1970-01-01T00:00:10Z"
      },
      {
        "kind": "trailing",
        "from": "1970-01-01T00:00:20Z",
        "to": "1970-01-01T00:00:30Z"
      }
    ]
  }
}
`

	if err := os.WriteFile(path, []byte(baseline), 0644); err != nil {
		t.Fatalf("WriteFile baseline failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// ctx 取消触发 write 失败
	cancel()
	provenance := recGood.Provenance
	Now := time.Unix(0, 0)
	e, err := NewFileExporter(path, provenance, func() time.Time { return Now })
	if err != nil {
		t.Fatalf("NewFileExporter failed: %v", err)
	}
	err = e.Write(ctx, recGood.Event, recGood.Selection)
	if err == nil {
		t.Fatalf("Write should have failed due to context cancellation")
	}
	// 包装函数，验证源路径无残余文件
	check := func() error {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(data) != baseline {
			return errors.New("baseline file content changed")
		}
		dir := filepath.Dir(path)
		files, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, f := range files {
			if f.Name() != filepath.Base(path) {
				return errors.New("unexpected file in directory: " + f.Name())
			}
		}
		return nil
	}
	if err := check(); err != nil {
		t.Fatalf("Post-failure check failed: %v", err)
	}
	// 不合法的 event 也应触发失败
	recBad := recGood
	recBad.Event.Identity.Target = ""
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	err = e.Write(ctx2, recBad.Event, recBad.Selection)
	if err == nil {
		t.Fatalf("Write should have failed due to invalid event")
	}
	if err := check(); err != nil {
		t.Fatalf("Post-failure check failed: %v", err)
	}
}

// goodExportJSON 返回 recGood 的合法 JSON，作为表驱动测试的基线。
func goodExportJSON(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(recGood)
	if err != nil {
		t.Fatalf("marshal recGood: %v", err)
	}
	return string(b)
}

// mutatedExportJSON 在 recGood 的副本上整体替换字段后序列化；调用方只做整字段赋值，避免改动共享的切片元素。
func mutatedExportJSON(t *testing.T, mutate func(*scene.Export)) string {
	t.Helper()
	rec := recGood
	mutate(&rec)
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal mutated export: %v", err)
	}
	return string(b)
}

// exportJSONWithExtraField 在合法 JSON 的指定对象层级插入未知字段。
func exportJSONWithExtraField(t *testing.T, path []string, key string, value any) string {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(goodExportJSON(t)), &obj); err != nil {
		t.Fatalf("unmarshal recGood JSON: %v", err)
	}
	cur := obj
	for _, p := range path {
		next, ok := cur[p].(map[string]any)
		if !ok {
			t.Fatalf("JSON path %v is not an object", path)
		}
		cur = next
	}
	cur[key] = value
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal JSON with extra field: %v", err)
	}
	return string(b)
}

func TestReadRejectsMalformedOrInconsistentExport(t *testing.T) {
	noSamples := func(r *scene.Export) {
		r.Selection.Samples = nil
		r.Selection.First = nil
		r.Selection.Last = nil
		r.Selection.Gaps = []scene.Gap{{
			Kind: "no_samples",
			From: r.Selection.Requested.Start,
			To:   r.Selection.Requested.End,
		}}
	}

	tests := []struct {
		name    string
		input   func(*testing.T) string
		wantErr bool
	}{
		{
			name:  "合法基线",
			input: goodExportJSON,
		},
		{
			name: "无样本的合法导出",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, noSamples)
			},
		},
		{
			name: "未知顶层字段",
			input: func(t *testing.T) string {
				return exportJSONWithExtraField(t, nil, "unexpected", true)
			},
			wantErr: true,
		},
		{
			name: "未知嵌套字段",
			input: func(t *testing.T) string {
				return exportJSONWithExtraField(t, []string{"event"}, "unexpected", 1)
			},
			wantErr: true,
		},
		{
			name: "尾随第二个 JSON 文档",
			input: func(t *testing.T) string {
				return goodExportJSON(t) + "\n{}\n"
			},
			wantErr: true,
		},
		{
			name: "尾随合法 JSON 值",
			input: func(t *testing.T) string {
				return goodExportJSON(t) + " null"
			},
			wantErr: true,
		},
		{
			name: "非 JSON 输入",
			input: func(t *testing.T) string {
				return "not json"
			},
			wantErr: true,
		},
		{
			name: "空输入",
			input: func(t *testing.T) string {
				return ""
			},
			wantErr: true,
		},
		{
			name: "缺 SchemaVersion",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) { r.SchemaVersion = "" })
			},
			wantErr: true,
		},
		{
			name: "缺 Provenance",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) { r.Provenance = "" })
			},
			wantErr: true,
		},
		{
			name: "零值 ExportedAt",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) { r.ExportedAt = time.Time{} })
			},
			wantErr: true,
		},
		{
			name: "无样本但 First 非 nil",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					noSamples(r)
					r.Selection.First = ptrTime(time.Unix(10, 0))
				})
			},
			wantErr: true,
		},
		{
			name: "无样本但 Last 非 nil",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					noSamples(r)
					r.Selection.Last = ptrTime(time.Unix(20, 0))
				})
			},
			wantErr: true,
		},
		{
			name: "无样本但 Gaps 为空",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					noSamples(r)
					r.Selection.Gaps = nil
				})
			},
			wantErr: true,
		},
		{
			name: "无样本但 Gaps 不是 no_samples",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					noSamples(r)
					r.Selection.Gaps = []scene.Gap{{
						Kind: "leading",
						From: r.Selection.Requested.Start,
						To:   time.Unix(10, 0),
					}}
				})
			},
			wantErr: true,
		},
		{
			name: "有样本但 First 为 nil",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) { r.Selection.First = nil })
			},
			wantErr: true,
		},
		{
			name: "有样本但 Last 为 nil",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) { r.Selection.Last = nil })
			},
			wantErr: true,
		},
		{
			name: "SchemaVersion 不匹配",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) { r.SchemaVersion = "v2" })
			},
			wantErr: true,
		},
		{
			name: "Event.Identity 不匹配",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					r.Event.Identity.Target = "other"
				})
			},
			wantErr: true,
		},
		{
			name: "样板时间混乱",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					r.Selection.Requested.Start = time.Unix(30, 0)
					r.Selection.Requested.End = time.Unix(0, 0)
				})
			},
			wantErr: true,
		},
		{
			name: "样本Gaps被删空",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					r.Selection.Gaps = nil
				})
			},
			wantErr: true,
		},
		{
			name: "样板顺序乱序",
			input: func(t *testing.T) string {
				return mutatedExportJSON(t, func(r *scene.Export) {
					r.Selection.Samples[0], r.Selection.Samples[1] = r.Selection.Samples[1], r.Selection.Samples[0]
				})
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := Read(strings.NewReader(tc.input(t)))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Read accepted malformed input, got %+v", rec)
				}
				return
			}
			if err != nil {
				t.Fatalf("Read rejected legal export: %v", err)
			}
		})
	}
}

func TestLearnerExportCounterexample(t *testing.T) {
	// t.Skip("待本人选择一个坏实现（如半截文件残留、吞掉 Close 错误、或重读放过未知字段/覆盖矛盾）并编写能击穿它的输入及断言；不要仅取消 Skip")
	// 字段完全正确且非空，但是 Gaps 中的 internal 缺口与 leading 缺口顺序颠倒，导致重读时被判定为不一致。
	bad := `{
  "schema_version": "v1",
  "provenance": "test",
  "exported_at": "1970-01-01T00:00:00Z",
  "event": {
    "key": "test",
    "identity": {
      "target": "a",
      "run_id": "run-a"
    },
    "received_at": "1970-01-01T00:00:00Z"
  },
  "selection": {
    "requested": {
      "start": "1970-01-01T00:00:00Z",
      "end": "1970-01-01T00:00:30Z",
      "max_gap_ns": 1000000000
    },
    "samples": [
      {
        "identity": {
          "target": "a",
          "run_id": "run-a"
        },
        "at": "1970-01-01T00:00:10Z",
        "cpu": {
          "usage_us": 100,
          "throttled_us": 500
        }
      },
      {
        "identity": {
          "target": "a",
          "run_id": "run-a"
        },
        "at": "1970-01-01T00:00:20Z"
      }
    ],
    "first": "1970-01-01T00:00:10Z",
    "last": "1970-01-01T00:00:20Z",
    "gaps": [
      {
        "kind": "leading",
        "from": "1970-01-01T00:00:10Z",
        "to": "1970-01-01T00:00:20Z"
      },
      {
        "kind": "internal",
        "from": "1970-01-01T00:00:00Z",
        "to": "1970-01-01T00:00:10Z"
      },
      {
        "kind": "trailing",
        "from": "1970-01-01T00:00:20Z",
        "to": "1970-01-01T00:00:30Z"
      }
    ]
  }
}
`

	path := t.TempDir() + "/export.json"
	if err := os.WriteFile(path, []byte(bad), 0644); err != nil {
		t.Fatalf("WriteFile bad export failed: %v", err)
	}
	_, err := ReadFile(path)
	if err == nil {
		t.Fatalf("缺口类型与时间段语义错乱（leading/internal 错标）")
	}
}
