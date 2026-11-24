package replay

import (
	"testing"
	"time"
)

func TestJudge(t *testing.T) {
	target := Target{
		Job:      "job1",
		Instance: "http_test",
	}

	beijing := time.FixedZone("UTC+8", 8*60*60)
	quereTime := time.Date(2026, time.November, 18, 16, 43, 58, 0, beijing)

	up := &Sample{
		Target:    target,
		ScrapedAt: quereTime,
		Value:     1,
	}

	probe := &Sample{
		Target:    target,
		ScrapedAt: quereTime,
		Value:     1,
	}
	evaluatedTime := quereTime.Add(40 * time.Second)

	if res := Judge(target, evaluatedTime, 30*time.Second, up, probe); res != InsufficientEvidence {
		t.Errorf("Judge() = %v, want %v", res, InsufficientEvidence)
	}
}

func TestJudgePathRecovered(t *testing.T) {
	target := Target{
		Job:      "job1",
		Instance: "http_test",
	}
	beijing := time.FixedZone("UTC+8", 8*60*60)
	quereTime := time.Date(2026, time.November, 18, 16, 47, 50, 0, beijing)

	up := &Sample{
		Target:    target,
		ScrapedAt: quereTime,
		Value:     1,
	}

	probe := &Sample{
		Target:    target,
		ScrapedAt: quereTime,
		Value:     1,
	}
	evaluatedTime := time.Date(2026, time.November, 18, 16, 47, 58, 0, beijing)

	if res := Judge(target, evaluatedTime, 30*time.Second, up, probe); res != PathRecovered {
		t.Errorf("Judge() = %v, want %v", res, PathRecovered)
	}

}

func TestJudgeMissingSample(t *testing.T) {
	target := Target{
		Job:      "job1",
		Instance: "http_test",
	}
	beijing := time.FixedZone("UTC+8", 8*60*60)
	evaluatedTime := time.Date(2026, time.November, 18, 16, 43, 58, 0, beijing)

	// 探测器停止后的现场：up 抓取失败(0)，probe 无样本。
	up := &Sample{
		Target:    target,
		ScrapedAt: time.Date(2026, time.November, 18, 16, 43, 50, 0, beijing),
		Value:     0,
	}
	if res := Judge(target, evaluatedTime, 30*time.Second, up, nil); res != InsufficientEvidence {
		t.Errorf("Judge() = %v, want %v", res, InsufficientEvidence)
	}
}

func TestJudgeFreshUpProbeFailed(t *testing.T) {
	target := Target{
		Job:      "job1",
		Instance: "http_test",
	}

	beijing := time.FixedZone("UTC+8", 8*60*60)
	evaluatedTime := time.Date(2026, time.November, 18, 16, 47, 58, 0, beijing)
	scrapedTime := evaluatedTime.Add(-5 * time.Second)

	// 新鲜的 up=1, probe_success=0
	up := &Sample{
		Target:    target,
		ScrapedAt: scrapedTime,
		Value:     1,
	}

	probe := &Sample{
		Target:    target,
		ScrapedAt: scrapedTime,
		Value:     0,
	}

	if res := Judge(target, evaluatedTime, 30*time.Second, up, probe); res != ProbeFailed {
		t.Errorf("Judge() = %v, want %v", res, ProbeFailed)
	}
}

func TestJudgeDifferentRounds(t *testing.T) {
	target := Target{
		Job:      "job1",
		Instance: "http_test",
	}

	beijing := time.FixedZone("UTC+8", 8*60*60)
	evaluatedTime := time.Date(2026, time.November, 18, 12, 0, 20, 0, beijing)

	// 两条样本都在 30 秒内、目标正确、值均为 1，只有抓取时间不同（不同轮）。
	up := &Sample{
		Target:    target,
		ScrapedAt: time.Date(2026, time.November, 18, 12, 0, 10, 0, beijing),
		Value:     1,
	}

	probe := &Sample{
		Target:    target,
		ScrapedAt: time.Date(2026, time.November, 18, 12, 0, 0, 0, beijing),
		Value:     1,
	}

	if res := Judge(target, evaluatedTime, 30*time.Second, up, probe); res != InsufficientEvidence {
		t.Errorf("Judge() = %v, want %v", res, InsufficientEvidence)
	}
}
