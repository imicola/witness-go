package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"witness-go/internal/exporter"
	"witness-go/internal/fixture"
	"witness-go/internal/window"
	"os"
	"time"
)

func run() error {
	path := flag.String("fixture", "testdata/window-demo.json", "离线构造夹具")
	mode := flag.String("mode", "inspect", "inspect、select、export 或 replay")
	outPath := flag.String("out", "", "导出 JSON 输出路径（mode=export 时使用）")
	exportPath := flag.String("export", "", "待重读的导出 JSON 路径（mode=replay 时使用）")
	flag.Parse()

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

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
