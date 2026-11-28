package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"witness-go/internal/collector"
	"witness-go/internal/exporter"
	"witness-go/internal/fixture"
	"witness-go/internal/notify"
	"witness-go/internal/pipeline"
	"witness-go/internal/scene"
	"witness-go/internal/window"
)

type observingExporter struct {
	inner     pipeline.Exporter
	outPath   string
	onSuccess func(ev scene.Event, sel scene.Selection)
}

func (o *observingExporter) Write(ctx context.Context, ev scene.Event, sel scene.Selection) error {
	if err := o.inner.Write(ctx, ev, sel); err != nil {
		return err
	}
	if o.onSuccess != nil {
		o.onSuccess(ev, sel)
	}
	return nil
}

func run() error {
	path := flag.String("fixture", "testdata/window-demo.json", "离线构造夹具")
	mode := flag.String("mode", "inspect", "inspect、select、export、replay 或 run")
	outPath := flag.String("out", "", "导出 JSON 输出路径（mode=export 或 mode=run 时使用）")
	exportPath := flag.String("export", "", "待重读的导出 JSON 路径（mode=replay 时使用）")

	// Online mode (-mode run) flags for Linux cgroup v2 + host PSI + Nightingale notify-receiver.
	receiverURL := flag.String("receiver-url", "http://127.0.0.1:18090", "Nightingale notify-receiver HTTP 地址")
	pollInterval := flag.Duration("poll-interval", 500*time.Millisecond, "notify-receiver 轮询间隔")
	sinceSeq := flag.Int64("since-seq", 0, "忽略 seq <= since-seq 的历史通知")
	target := flag.String("target", "", "逻辑监控目标（需与告警 tags_map.instance 一致）")
	runID := flag.String("run-id", "", "当前容器运行身份标识（例如 <container-id>:<pid>）")
	cpuMaxPath := flag.String("cpu-max", "", "容器 cgroup v2 cpu.max 路径")
	cpuStatPath := flag.String("cpu-stat", "", "容器 cgroup v2 cpu.stat 路径")
	hostPSIPath := flag.String("host-psi", "/proc/pressure/cpu", "宿主 CPU PSI 路径")
	sampleInterval := flag.Duration("sample-interval", time.Second, "持续采样间隔")
	preWindow := flag.Duration("pre", 20*time.Second, "告警前窗时长")
	postWindow := flag.Duration("post", 15*time.Second, "告警后窗时长")
	maxGap := flag.Duration("max-gap", 5*time.Second, "允许的最大相邻样本间隔")
	bufferCap := flag.Int("buffer-cap", 120, "环形缓冲容量（样本条数）")
	maxTaskSamples := flag.Int("max-task-samples", 120, "单次事件任务最大样本条数")
	maxSeenKeys := flag.Int("max-seen-keys", 64, "同进程去重表最大容量")
	provenance := flag.String("provenance", "live linux cgroup v2 + host psi + nightingale notify-receiver", "导出工件来源标识")
	stopAfterExport := flag.Bool("stop-after-export", true, "完成首份告警现场导出后优雅退出")
	flag.Parse()

	if *mode == "run" {
		return runOnline(onlineOptions{
			receiverURL:     *receiverURL,
			pollInterval:    *pollInterval,
			sinceSeq:        *sinceSeq,
			target:          *target,
			runID:           *runID,
			cpuMaxPath:      *cpuMaxPath,
			cpuStatPath:     *cpuStatPath,
			hostPSIPath:     *hostPSIPath,
			sampleInterval:  *sampleInterval,
			preWindow:       *preWindow,
			postWindow:      *postWindow,
			maxGap:          *maxGap,
			bufferCap:       *bufferCap,
			maxTaskSamples:  *maxTaskSamples,
			maxSeenKeys:     *maxSeenKeys,
			provenance:      *provenance,
			outPath:         *outPath,
			stopAfterExport: *stopAfterExport,
		})
	}

	var output any
	switch *mode {
	case "inspect", "select", "export":
		f, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer f.Close()
		input, err := fixture.Read(f)
		if err != nil {
			return err
		}
		if *mode == "inspect" {
			output = input
			break
		}
		result, err := window.Select(input.Samples, input.Event, input.Window)
		if err != nil {
			return err
		}
		if *mode == "select" {
			output = result
			break
		}
		exp, err := exporter.NewFileExporter(*outPath, input.Provenance, time.Now)
		if err != nil {
			return err
		}
		if err := exp.Write(context.Background(), input.Event, result); err != nil {
			return err
		}
		output = map[string]string{
			"exported":   *outPath,
			"provenance": input.Provenance,
		}
	case "replay":
		rec, err := exporter.ReadFile(*exportPath)
		if err != nil {
			return err
		}
		summary, err := exporter.Summarize(rec)
		if err != nil {
			return err
		}
		output = summary
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

type onlineOptions struct {
	receiverURL     string
	pollInterval    time.Duration
	sinceSeq        int64
	target          string
	runID           string
	cpuMaxPath      string
	cpuStatPath     string
	hostPSIPath     string
	sampleInterval  time.Duration
	preWindow       time.Duration
	postWindow      time.Duration
	maxGap          time.Duration
	bufferCap       int
	maxTaskSamples  int
	maxSeenKeys     int
	provenance      string
	outPath         string
	stopAfterExport bool
}

func runOnline(opts onlineOptions) error {
	if opts.target == "" || opts.runID == "" {
		return errors.New("mode=run requires -target and -run-id")
	}
	id := scene.Identity{Target: opts.target, RunID: opts.runID}

	sampler, err := collector.NewLinuxSampler(collector.SourceConfig{
		IdentityFunc: func(context.Context) (scene.Identity, error) { return id, nil },
		CPUMaxPath:   opts.cpuMaxPath,
		CPUStatPath:  opts.cpuStatPath,
		HostPSIPath:  opts.hostPSIPath,
		Now:          time.Now,
	})
	if err != nil {
		return fmt.Errorf("init linux sampler: %w", err)
	}

	events, err := notify.NewReceiverSource(notify.ReceiverConfig{
		BaseURL:      opts.receiverURL,
		PollInterval: opts.pollInterval,
		SinceSeq:     opts.sinceSeq,
		Now:          time.Now,
		Resolve: func(_ context.Context, logicalTarget string) (scene.Identity, bool, error) {
			if logicalTarget != opts.target {
				return scene.Identity{}, false, nil
			}
			return id, true, nil
		},
		OnSkip: func(seq int64, reason string, err error) {
			if err != nil {
				log.Printf("notify skip seq=%d reason=%s err=%v", seq, reason, err)
			} else {
				log.Printf("notify skip seq=%d reason=%s", seq, reason)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("init notify receiver source: %w", err)
	}

	fileExp, err := exporter.NewFileExporter(opts.outPath, opts.provenance, time.Now)
	if err != nil {
		return fmt.Errorf("init file exporter: %w", err)
	}

	sigCtx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSig()
	runCtx, cancel := context.WithCancel(sigCtx)
	defer cancel()

	var exportedCount int
	obsExp := &observingExporter{
		inner:   fileExp,
		outPath: opts.outPath,
		onSuccess: func(ev scene.Event, sel scene.Selection) {
			exportedCount++
			log.Printf("exported key=%s target=%s run_id=%s samples=%d gaps=%d path=%s",
				ev.Key, ev.Identity.Target, ev.Identity.RunID, len(sel.Samples), len(sel.Gaps), opts.outPath)
			if opts.stopAfterExport {
				cancel()
			}
		},
	}

	ring, err := pipeline.NewRing(opts.bufferCap)
	if err != nil {
		return fmt.Errorf("init ring buffer: %w", err)
	}
	coord, err := pipeline.NewCoordinator(ring, obsExp, pipeline.CoordinatorConfig{
		PreWindow:      opts.preWindow,
		PostWindow:     opts.postWindow,
		MaxGap:         opts.maxGap,
		MaxTaskSamples: opts.maxTaskSamples,
		MaxSeenKeys:    opts.maxSeenKeys,
	})
	if err != nil {
		return fmt.Errorf("init coordinator: %w", err)
	}

	runner, err := pipeline.NewRunner(coord, sampler, events, pipeline.RunnerConfig{
		SampleInterval: opts.sampleInterval,
		Now:            time.Now,
		OnDecision: func(ev scene.Event, dec pipeline.EventDecision) {
			log.Printf("event decision=%s key=%s target=%s run_id=%s received_at=%s",
				dec, ev.Key, ev.Identity.Target, ev.Identity.RunID, ev.ReceivedAt.Format(time.RFC3339Nano))
		},
		OnError: func(stage string, err error) {
			log.Printf("runner error stage=%s err=%v", stage, err)
		},
	})
	if err != nil {
		return fmt.Errorf("init runner: %w", err)
	}

	log.Printf("witness-go online runner started: target=%s run_id=%s receiver=%s since_seq=%d interval=%s pre=%s post=%s out=%s",
		opts.target, opts.runID, opts.receiverURL, opts.sinceSeq, opts.sampleInterval, opts.preWindow, opts.postWindow, opts.outPath)

	err = runner.Run(runCtx)
	if errors.Is(err, context.Canceled) && (exportedCount > 0 || sigCtx.Err() != nil) {
		log.Printf("witness-go online runner stopped cleanly (exports=%d)", exportedCount)
		return nil
	}
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
