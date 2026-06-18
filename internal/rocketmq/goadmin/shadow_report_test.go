package goadmin

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewShadowReportRecordsSizesErrorsAndDurations(t *testing.T) {
	result := ShadowResult{
		Command: "topicStatus",
		Args:    []string{"topicStatus", "-t", "订单Topic"},
		Primary: ShadowTargetResult{
			Name:     "official",
			Stdout:   "primary输出\n",
			Stderr:   "primary警告",
			Error:    "exit status 1",
			Duration: 1500 * time.Millisecond,
		},
		Targets: []ShadowTargetResult{{
			Name:     "native",
			Stdout:   "native输出\n",
			Stderr:   "native警告",
			Error:    "native failed",
			Duration: 25 * time.Millisecond,
		}},
		Diffs: []ShadowDiff{{
			Target:          "native",
			Matched:         false,
			StdoutDifferent: true,
			StderrDifferent: true,
			ErrorDifferent:  true,
			Duration:        25 * time.Millisecond,
		}},
	}

	report := NewShadowReport(result)

	if report.Command != "topicStatus" {
		t.Fatalf("expected command to be copied, got %#v", report)
	}
	if len(report.Args) != 3 || report.Args[2] != "订单Topic" {
		t.Fatalf("expected args to be copied, got %#v", report.Args)
	}
	if report.Primary.Name != "official" {
		t.Fatalf("expected primary name to be copied, got %#v", report.Primary)
	}
	if report.Primary.StdoutBytes != len([]byte("primary输出\n")) {
		t.Fatalf("expected primary stdout byte count, got %#v", report.Primary)
	}
	if report.Primary.StderrBytes != len([]byte("primary警告")) {
		t.Fatalf("expected primary stderr byte count, got %#v", report.Primary)
	}
	if report.Primary.Error != "exit status 1" || report.Primary.DurationMs != 1500 {
		t.Fatalf("expected primary error and duration, got %#v", report.Primary)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("expected one target, got %#v", report.Targets)
	}
	if report.Targets[0].Name != "native" || report.Targets[0].StdoutBytes != len([]byte("native输出\n")) || report.Targets[0].StderrBytes != len([]byte("native警告")) {
		t.Fatalf("expected target byte sizes, got %#v", report.Targets[0])
	}
	if report.Targets[0].Error != "native failed" || report.Targets[0].DurationMs != 25 {
		t.Fatalf("expected target error and duration, got %#v", report.Targets[0])
	}
	if len(report.Diffs) != 1 {
		t.Fatalf("expected one diff, got %#v", report.Diffs)
	}
	diff := report.Diffs[0]
	if diff.Target != "native" || diff.Matched || !diff.StdoutDifferent || !diff.StderrDifferent || !diff.ErrorDifferent || diff.DurationMs != 25 {
		t.Fatalf("expected diff flags and duration, got %#v", diff)
	}
}

func TestMarshalShadowReportJSONLineIsStableAndOmitsOutputText(t *testing.T) {
	result := ShadowResult{
		Command: "clusterList",
		Args:    []string{"clusterList", "-n", "127.0.0.1:9876"},
		Primary: ShadowTargetResult{
			Name:     "official",
			Stdout:   "BROKER SECRET STDOUT\n",
			Stderr:   "SECRET STDERR",
			Duration: 2 * time.Millisecond,
		},
		Targets: []ShadowTargetResult{{
			Name:     "sidecar",
			Stdout:   "BROKER SECRET STDOUT changed\n",
			Stderr:   "SECRET STDERR changed",
			Error:    "exit status 1",
			Duration: 4 * time.Millisecond,
		}},
		Diffs: []ShadowDiff{{
			Target:          "sidecar",
			Matched:         false,
			StdoutDifferent: true,
			ErrorDifferent:  true,
			Duration:        4 * time.Millisecond,
		}},
	}

	line, err := MarshalShadowReportJSONLine(result)
	if err != nil {
		t.Fatalf("expected JSON line to marshal, got %v", err)
	}

	expected := `{"command":"clusterList","args":["clusterList","-n","127.0.0.1:9876"],"primary":{"name":"official","stdout_bytes":21,"stderr_bytes":13,"error":"","duration_ms":2},"targets":[{"name":"sidecar","stdout_bytes":29,"stderr_bytes":21,"error":"exit status 1","duration_ms":4}],"diffs":[{"target":"sidecar","matched":false,"stdout_different":true,"stderr_different":false,"error_different":true,"duration_ms":4}]}`
	if string(line) != expected+"\n" {
		t.Fatalf("unexpected JSON line:\n%s", line)
	}
	for _, forbidden := range []string{"BROKER SECRET STDOUT", "SECRET STDERR"} {
		if strings.Contains(string(line), forbidden) {
			t.Fatalf("JSON line leaked raw output %q: %s", forbidden, line)
		}
	}
	var decoded ShadowReport
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatalf("expected JSON line to decode into ShadowReport, got %v", err)
	}
	if decoded.Targets[0].StdoutBytes != 29 || decoded.Targets[0].StderrBytes != 21 {
		t.Fatalf("expected decoded byte counts, got %#v", decoded.Targets[0])
	}
}

func TestMarshalShadowBatchJSONLinesKeepsOrderAndOmitsOutputText(t *testing.T) {
	batch := ShadowBatchResult{
		Results: []ShadowResult{
			{
				Command: "topicList",
				Primary: ShadowTargetResult{
					Name:   "official",
					Stdout: "SECRET TOPIC OUTPUT",
				},
			},
			{
				Command: "clusterList",
				Primary: ShadowTargetResult{
					Name:   "official",
					Stdout: "SECRET CLUSTER OUTPUT",
				},
			},
		},
	}

	lines, err := MarshalShadowBatchJSONLines(batch)
	if err != nil {
		t.Fatalf("expected batch JSON lines to marshal, got %v", err)
	}
	text := string(lines)
	if !strings.Contains(text, `"command":"topicList"`) || !strings.Contains(text, `"command":"clusterList"`) {
		t.Fatalf("expected both commands in JSON lines, got %s", text)
	}
	if strings.Index(text, `"command":"topicList"`) > strings.Index(text, `"command":"clusterList"`) {
		t.Fatalf("expected JSON lines to keep result order, got %s", text)
	}
	if strings.Count(text, "\n") != 2 {
		t.Fatalf("expected one JSON line per result, got %q", text)
	}
	for _, forbidden := range []string{"SECRET TOPIC OUTPUT", "SECRET CLUSTER OUTPUT"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("batch JSON lines leaked raw output %q: %s", forbidden, text)
		}
	}
}

func TestMarshalShadowBatchJSONLinesReturnsEmptyBytesForEmptyBatch(t *testing.T) {
	lines, err := MarshalShadowBatchJSONLines(ShadowBatchResult{})
	if err != nil {
		t.Fatalf("expected empty batch to marshal without error, got %v", err)
	}
	if lines == nil {
		t.Fatal("expected empty batch to return a non-nil empty byte slice")
	}
	if len(lines) != 0 {
		t.Fatalf("expected empty batch to produce no JSONL bytes, got %q", lines)
	}
}

func TestMarshalShadowBatchJSONLinesRejectsUnknownDiffTarget(t *testing.T) {
	batch := ShadowBatchResult{
		Results: []ShadowResult{{
			Command: "topicStatus",
			Primary: ShadowTargetResult{
				Name: "official",
			},
			Targets: []ShadowTargetResult{{
				Name: "sidecar",
			}},
			Diffs: []ShadowDiff{{
				Target:          "missing-target",
				StdoutDifferent: true,
			}},
		}},
	}

	lines, err := MarshalShadowBatchJSONLines(batch)
	if err == nil {
		t.Fatalf("expected unknown diff target to fail, got JSONL %s", lines)
	}
	if len(lines) != 0 {
		t.Fatalf("expected invalid batch to return no partial JSONL bytes, got %s", lines)
	}
	for _, want := range []string{"topicStatus", "missing-target", "sidecar"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error %q to mention %q", err.Error(), want)
		}
	}
}

func TestMarshalShadowBatchJSONLinesEscapesControlTextIntoOneLinePerResult(t *testing.T) {
	batch := ShadowBatchResult{
		Results: []ShadowResult{{
			Command: "topic\nStatus",
			Args:    []string{"topicStatus", "-t", "订单\nTopic"},
			Primary: ShadowTargetResult{
				Name:     "official",
				Stdout:   "PRIMARY RAW STDOUT\nsecond line",
				Stderr:   "PRIMARY RAW STDERR\r\nsecond line",
				Error:    "primary error\nsecond line",
				Duration: 7 * time.Millisecond,
			},
			Targets: []ShadowTargetResult{{
				Name:     "sidecar",
				Stdout:   "TARGET RAW STDOUT\nsecond line",
				Stderr:   "TARGET RAW STDERR\r\nsecond line",
				Error:    "target error\r\nsecond line",
				Duration: 9 * time.Millisecond,
			}},
			Diffs: []ShadowDiff{{
				Target:          "sidecar",
				StdoutDifferent: true,
				StderrDifferent: true,
				ErrorDifferent:  true,
				Duration:        9 * time.Millisecond,
			}},
		}},
	}

	lines, err := MarshalShadowBatchJSONLines(batch)
	if err != nil {
		t.Fatalf("expected control text to marshal safely, got %v", err)
	}
	text := string(lines)
	if strings.Count(text, "\n") != 1 || !strings.HasSuffix(text, "\n") {
		t.Fatalf("expected exactly one trailing JSONL newline, got %q", text)
	}
	body := strings.TrimSuffix(text, "\n")
	if strings.Contains(body, "\n") || strings.Contains(body, "\r") {
		t.Fatalf("expected JSON body to contain no literal line breaks, got %q", body)
	}
	for _, want := range []string{"topic\\nStatus", "订单\\nTopic", "primary error\\nsecond line", "target error\\r\\nsecond line"} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected JSON body to contain escaped text %q, got %q", want, body)
		}
	}
	for _, forbidden := range []string{"PRIMARY RAW STDOUT", "PRIMARY RAW STDERR", "TARGET RAW STDOUT", "TARGET RAW STDERR"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("JSON body leaked raw output %q: %s", forbidden, body)
		}
	}
	var decoded ShadowReport
	if err := json.Unmarshal(lines, &decoded); err != nil {
		t.Fatalf("expected escaped JSONL line to decode, got %v", err)
	}
	if decoded.Command != "topic\nStatus" || decoded.Args[2] != "订单\nTopic" {
		t.Fatalf("expected decoded command and args to preserve control text, got %#v", decoded)
	}
	if decoded.Primary.Error != "primary error\nsecond line" || decoded.Targets[0].Error != "target error\r\nsecond line" {
		t.Fatalf("expected decoded errors to preserve control text, got primary=%q target=%q", decoded.Primary.Error, decoded.Targets[0].Error)
	}
}

func TestNewShadowBatchSummaryReportHandlesEmptyBatch(t *testing.T) {
	report := NewShadowBatchSummaryReport(ShadowBatchResult{})

	if report.Total != 0 || report.Executed != 0 || report.Skipped != 0 {
		t.Fatalf("expected empty counters, got %#v", report)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("expected no errors, got %#v", report.Errors)
	}
	if report.Summary.Commands != 0 || report.Summary.Targets != 0 || report.Summary.Mismatches != 0 {
		t.Fatalf("expected empty summary, got %#v", report.Summary)
	}
	if report.Summary.ByTarget == nil {
		t.Fatal("expected empty batch summary to expose a non-nil by_target map")
	}
	if len(report.Summary.ByTarget) != 0 {
		t.Fatalf("expected empty by_target map, got %#v", report.Summary.ByTarget)
	}
}

func TestNewShadowBatchSummaryReportRecordsBatchErrors(t *testing.T) {
	batch := ShadowBatchResult{
		TotalSamples:    3,
		ExecutedSamples: 1,
		SkippedSamples:  2,
		Errors: []error{
			errors.New("invalid shadow plan"),
			errors.New("missing target provider"),
		},
	}

	report := NewShadowBatchSummaryReport(batch)

	if report.Total != 3 || report.Executed != 1 || report.Skipped != 2 {
		t.Fatalf("expected batch counters to be copied, got %#v", report)
	}
	if len(report.Errors) != 2 {
		t.Fatalf("expected two errors, got %#v", report.Errors)
	}
	if report.Errors[0] != "invalid shadow plan" || report.Errors[1] != "missing target provider" {
		t.Fatalf("expected error strings to be copied in order, got %#v", report.Errors)
	}
}

func TestNewShadowBatchSummaryReportIncludesTargetP95Stats(t *testing.T) {
	batch := ShadowBatchResult{
		TotalSamples:    4,
		ExecutedSamples: 3,
		SkippedSamples:  1,
		Summary: SummarizeShadowResults([]ShadowResult{
			{Diffs: []ShadowDiff{
				{Target: "native", Matched: true, Duration: 10 * time.Millisecond},
				{Target: "sidecar", Matched: true, Duration: 40 * time.Millisecond},
			}},
			{Diffs: []ShadowDiff{
				{Target: "native", Matched: true, Duration: 20 * time.Millisecond},
				{Target: "sidecar", Matched: false, Duration: 60 * time.Millisecond},
			}},
			{Diffs: []ShadowDiff{
				{Target: "native", Matched: true, Duration: 30 * time.Millisecond},
			}},
		}),
	}

	report := NewShadowBatchSummaryReport(batch)

	if report.Summary.Commands != 3 || report.Summary.Targets != 5 || report.Summary.Mismatches != 1 {
		t.Fatalf("expected summary counters, got %#v", report.Summary)
	}
	native := report.Summary.ByTarget["native"]
	if native.Samples != 3 || native.Mismatches != 0 || native.AvgDurationMs != 20 || native.MaxDurationMs != 30 || native.P95DurationMs != 30 {
		t.Fatalf("expected native latency stats, got %#v", native)
	}
	sidecar := report.Summary.ByTarget["sidecar"]
	if sidecar.Samples != 2 || sidecar.Mismatches != 1 || sidecar.AvgDurationMs != 50 || sidecar.MaxDurationMs != 60 || sidecar.P95DurationMs != 60 {
		t.Fatalf("expected sidecar latency stats, got %#v", sidecar)
	}
}

func TestMarshalShadowBatchSummaryJSONLineIsSingleLineAndOmitsOutputText(t *testing.T) {
	batch := ShadowBatchResult{
		TotalSamples:    1,
		ExecutedSamples: 1,
		Results: []ShadowResult{{
			Command: "clusterList",
			Primary: ShadowTargetResult{
				Name:   "official",
				Stdout: "SECRET PRIMARY STDOUT",
				Stderr: "SECRET PRIMARY STDERR",
			},
			Targets: []ShadowTargetResult{{
				Name:   "native",
				Stdout: "SECRET TARGET STDOUT",
				Stderr: "SECRET TARGET STDERR",
			}},
			Diffs: []ShadowDiff{{
				Target:   "native",
				Matched:  false,
				Duration: 15 * time.Millisecond,
			}},
		}},
		Summary: SummarizeShadowResults([]ShadowResult{{
			Diffs: []ShadowDiff{{
				Target:   "native",
				Matched:  false,
				Duration: 15 * time.Millisecond,
			}},
		}}),
		Errors: []error{errors.New("batch error\nsecond line")},
	}

	line, err := MarshalShadowBatchSummaryJSONLine(batch)
	if err != nil {
		t.Fatalf("expected summary JSON line to marshal, got %v", err)
	}
	text := string(line)
	if strings.Count(text, "\n") != 1 || !strings.HasSuffix(text, "\n") {
		t.Fatalf("expected exactly one trailing JSONL newline, got %q", text)
	}
	body := strings.TrimSuffix(text, "\n")
	if strings.Contains(body, "\n") || strings.Contains(body, "\r") {
		t.Fatalf("expected JSON body to contain no literal line breaks, got %q", body)
	}
	for _, forbidden := range []string{"SECRET PRIMARY STDOUT", "SECRET PRIMARY STDERR", "SECRET TARGET STDOUT", "SECRET TARGET STDERR"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("summary JSON line leaked raw output %q: %s", forbidden, body)
		}
	}
	expected := `{"total":1,"executed":1,"skipped":0,"errors":["batch error\nsecond line"],"summary":{"commands":1,"targets":1,"mismatches":1,"by_target":{"native":{"samples":1,"mismatches":1,"avg_duration_ms":15,"max_duration_ms":15,"p95_duration_ms":15}}}}`
	if text != expected+"\n" {
		t.Fatalf("unexpected summary JSON line:\n%s", text)
	}
	var decoded ShadowBatchSummaryReport
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatalf("expected summary JSON line to decode, got %v", err)
	}
	if decoded.Summary.ByTarget["native"].P95DurationMs != 15 {
		t.Fatalf("expected decoded p95, got %#v", decoded.Summary.ByTarget["native"])
	}
}

func TestMarshalShadowBatchPlanJSONLineRecordsDryRunPlan(t *testing.T) {
	plan := PlanShadowBatch([]ShadowSample{
		{
			Name:       "topic-list",
			Args:       []string{"topicList", "-n", "127.0.0.1:9876"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
			MinSamples: 1,
		},
		{
			Name:       "known-message",
			Args:       []string{"queryMsgById", "-i", "<message-id>"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderSidecar, ShadowProviderNative, ShadowProviderAuto},
			MinSamples: 93,
			Notes:      "needs real message fixture",
		},
	})

	line, err := MarshalShadowBatchPlanJSONLine(plan)
	if err != nil {
		t.Fatalf("expected plan JSON line to marshal, got %v", err)
	}
	text := string(line)
	if strings.Count(text, "\n") != 1 || !strings.HasSuffix(text, "\n") {
		t.Fatalf("expected exactly one trailing JSONL newline, got %q", text)
	}
	body := strings.TrimSuffix(text, "\n")
	for _, want := range []string{
		`"total":2`,
		`"executable":1`,
		`"skipped":1`,
		`"name":"topic-list"`,
		`"args":["topicList","-n","127.0.0.1:9876"]`,
		`"providers":["official","native"]`,
		`"placeholder":"<message-id>"`,
		`"reason":"sample \"known-message\" contains placeholder \"<message-id>\""`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected plan JSON body to contain %q, got %s", want, body)
		}
	}
	var decoded ShadowBatchPlanReport
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatalf("expected plan JSON line to decode, got %v", err)
	}
	if decoded.Total != 2 || decoded.Executable != 1 || decoded.Skipped != 1 {
		t.Fatalf("expected decoded counters, got %#v", decoded)
	}
	if len(decoded.ExecutableSamples) != 1 || decoded.ExecutableSamples[0].Name != "topic-list" {
		t.Fatalf("expected decoded executable sample, got %#v", decoded.ExecutableSamples)
	}
	if len(decoded.SkippedSamples) != 1 || decoded.SkippedSamples[0].Placeholder != "<message-id>" {
		t.Fatalf("expected decoded skipped sample, got %#v", decoded.SkippedSamples)
	}
}

func TestNewShadowBatchPlanReportCopiesPlanAndErrors(t *testing.T) {
	plan := ShadowBatchPlan{
		TotalSamples:      2,
		ExecutableSamples: 1,
		SkippedSamples:    1,
		Executable: []ShadowSample{{
			Name:       "topic-list",
			Args:       []string{"topicList"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
			MinSamples: 1,
		}},
		Skipped: []ShadowSkippedSample{{
			Sample: ShadowSample{
				Name:       "known-message",
				Args:       []string{"queryMsgById", "-i", "<message-id>"},
				Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
				MinSamples: 93,
			},
			Placeholder: "<message-id>",
			Reason:      "sample \"known-message\" contains placeholder \"<message-id>\"",
		}},
		Errors: []error{errors.New("invalid plan\nsecond line")},
	}

	report := NewShadowBatchPlanReport(plan)
	plan.Executable[0].Args[0] = "mutated"
	plan.Skipped[0].Sample.Providers[1] = ShadowProviderSidecar

	if report.Total != 2 || report.Executable != 1 || report.Skipped != 1 {
		t.Fatalf("expected counters to be copied, got %#v", report)
	}
	if report.ExecutableSamples[0].Args[0] != "topicList" {
		t.Fatalf("expected executable args to be deep-copied, got %#v", report.ExecutableSamples[0].Args)
	}
	if report.SkippedSamples[0].Sample.Providers[1] != ShadowProviderNative {
		t.Fatalf("expected skipped sample providers to be deep-copied, got %#v", report.SkippedSamples[0].Sample.Providers)
	}
	if len(report.Errors) != 1 || report.Errors[0] != "invalid plan\nsecond line" {
		t.Fatalf("expected error text to be copied, got %#v", report.Errors)
	}
}
