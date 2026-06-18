package goadmin

import (
	"testing"
	"time"
)

func TestSummarizeShadowResultsCountsCommandsTargetsAndMismatches(t *testing.T) {
	summary := SummarizeShadowResults([]ShadowResult{
		{
			Command: "topicList",
			Diffs: []ShadowDiff{
				{Target: "native", Matched: true, Duration: 10 * time.Millisecond},
				{Target: "sidecar", Matched: false, Duration: 30 * time.Millisecond},
			},
		},
		{
			Command: "clusterList",
			Diffs: []ShadowDiff{
				{Target: "native", Matched: false, Duration: 20 * time.Millisecond},
			},
		},
	})

	if summary.Commands != 2 {
		t.Fatalf("expected two commands, got %#v", summary)
	}
	if summary.Targets != 3 {
		t.Fatalf("expected three target comparisons, got %#v", summary)
	}
	if summary.Mismatches != 2 {
		t.Fatalf("expected two mismatches, got %#v", summary)
	}
	if summary.ByTarget["native"].Mismatches != 1 {
		t.Fatalf("expected native mismatch count to be 1, got %#v", summary.ByTarget["native"])
	}
	if summary.ByTarget["sidecar"].Mismatches != 1 {
		t.Fatalf("expected sidecar mismatch count to be 1, got %#v", summary.ByTarget["sidecar"])
	}
}

func TestSummarizeShadowResultsComputesTargetLatencyStats(t *testing.T) {
	summary := SummarizeShadowResults([]ShadowResult{
		{Command: "a", Diffs: []ShadowDiff{{Target: "native", Matched: true, Duration: 10 * time.Millisecond}}},
		{Command: "b", Diffs: []ShadowDiff{{Target: "native", Matched: true, Duration: 20 * time.Millisecond}}},
		{Command: "c", Diffs: []ShadowDiff{{Target: "native", Matched: true, Duration: 30 * time.Millisecond}}},
		{Command: "d", Diffs: []ShadowDiff{{Target: "native", Matched: true, Duration: 40 * time.Millisecond}}},
	})

	stats := summary.ByTarget["native"]
	if stats.Samples != 4 {
		t.Fatalf("expected four samples, got %#v", stats)
	}
	if stats.AvgDuration != 25*time.Millisecond {
		t.Fatalf("expected avg 25ms, got %s", stats.AvgDuration)
	}
	if stats.MaxDuration != 40*time.Millisecond {
		t.Fatalf("expected max 40ms, got %s", stats.MaxDuration)
	}
	if stats.P95Duration != 40*time.Millisecond {
		t.Fatalf("expected nearest-rank p95 40ms, got %s", stats.P95Duration)
	}
}

func TestSummarizeShadowResultsUsesDefensiveCopies(t *testing.T) {
	results := []ShadowResult{{
		Command: "topicStatus",
		Diffs:   []ShadowDiff{{Target: "native", Matched: true, Duration: 10 * time.Millisecond}},
	}}

	summary := SummarizeShadowResults(results)
	stats := summary.ByTarget["native"]
	stats.Samples = 99
	summary.ByTarget["native"] = stats

	again := SummarizeShadowResults(results)
	if again.ByTarget["native"].Samples != 1 {
		t.Fatalf("expected fresh summary map, got %#v", again.ByTarget["native"])
	}
}
