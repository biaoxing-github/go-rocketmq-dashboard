package goadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/config"
	nativeadmin "rocketmq-go-dashboard/internal/rocketmq/goadmin"
)

func TestRunNoArgsDelegatesOfficialHelp(t *testing.T) {
	runner := &recordingRunner{output: "official help\n"}
	var stdout bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
	}, nil)

	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if stdout.String() != "official help\n" {
		t.Fatalf("expected official help passthrough, got %q", stdout.String())
	}
	if len(runner.calls) != 1 || len(runner.calls[0]) != 0 {
		t.Fatalf("expected empty args to reach official mqadmin, got %#v", runner.calls)
	}
}

func TestRunM6ShadowPlanPrintsDefaultDryRunJSONL(t *testing.T) {
	runner := &recordingRunner{err: errors.New("m6 shadow plan must not invoke injected runner")}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-plan"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q", code, stderr.String())
	}
	text := stdout.String()
	if strings.Count(text, "\n") != 1 || !strings.HasSuffix(text, "\n") {
		t.Fatalf("expected exactly one JSONL line, got %q", text)
	}
	expectedLine, err := nativeadmin.MarshalShadowBatchPlanJSONLine(nativeadmin.PlanShadowBatch(nativeadmin.DefaultM6ShadowPlan()))
	if err != nil {
		t.Fatalf("expected default M6 plan to marshal, got %v", err)
	}
	if text != string(expectedLine) {
		t.Fatalf("stdout mismatch\nexpected=%q\nactual=%q", string(expectedLine), text)
	}
	if !strings.Contains(text, `"skipped_samples"`) && !strings.Contains(text, `"skipped":`) {
		t.Fatalf("expected skipped sample visibility in plan JSON, got %q", text)
	}
	if !strings.Contains(text, "<known-message-id>") {
		t.Fatalf("expected placeholder visibility in plan JSON, got %q", text)
	}
	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &decoded); err != nil {
		t.Fatalf("expected valid JSON object, got %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected injected runner to stay unused, got %#v", runner.calls)
	}
}

func TestRunM6ShadowPlanDoesNotInvokeNativeRunner(t *testing.T) {
	originalNativeRunner := nativeCommandRunner
	nativeCalled := false
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		nativeCalled = true
		return "", true, errors.New("m6 shadow plan must not invoke native runner")
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "native",
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-plan"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q", code, stderr.String())
	}
	if nativeCalled {
		t.Fatalf("expected native runner to stay unused")
	}
	if !strings.Contains(stdout.String(), "<known-message-id>") {
		t.Fatalf("expected placeholder visibility in plan JSON, got %q", stdout.String())
	}
}

func TestRunM6ShadowPlanAppliesFixtureJSONWithoutInvokingRunners(t *testing.T) {
	originalNativeRunner := nativeCommandRunner
	nativeCalled := false
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		nativeCalled = true
		return "", true, errors.New("m6 shadow fixture plan must not invoke native runner")
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	runner := &recordingRunner{err: errors.New("m6 shadow fixture plan must not invoke injected runner")}
	fixtures := `{"samples":[{"name":"known-message","args":["queryMsgById","-i","AC18000300002A9F0000000000000000"]}]}`
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "native",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-fixtures", fixtures, "--m6-shadow-plan"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q", code, stderr.String())
	}
	if nativeCalled {
		t.Fatalf("expected native runner to stay unused")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected injected runner to stay unused, got %#v", runner.calls)
	}
	var decoded nativeadmin.ShadowBatchPlanReport
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &decoded); err != nil {
		t.Fatalf("expected valid JSON object, got %v", err)
	}
	if decoded.Total != 96 || decoded.Executable != 1 || decoded.Skipped != 95 {
		t.Fatalf("unexpected plan counts: %#v", decoded)
	}
	if len(decoded.ExecutableSamples) != 1 || decoded.ExecutableSamples[0].Name != "known-message" {
		t.Fatalf("expected known-message executable sample, got %#v", decoded.ExecutableSamples)
	}
	if strings.Contains(stdout.String(), "<known-message-id>") {
		t.Fatalf("expected known-message placeholder to be replaced, got %q", stdout.String())
	}
}

func TestRunM6ShadowPlanReadsFixtureJSONFileAfterPlanFlag(t *testing.T) {
	fixturePath := filepath.Join(t.TempDir(), "m6-shadow-fixtures.json")
	if err := os.WriteFile(fixturePath, []byte(`{"samples":[{"name":"known-message","args":["queryMsgById","-i","AC18000300002A9F0000000000000000"]}]}`), 0o600); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     &recordingRunner{err: errors.New("fixture file dry-run must not call runner")},
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-plan", "--m6-shadow-fixtures-file", fixturePath})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q", code, stderr.String())
	}
	var decoded nativeadmin.ShadowBatchPlanReport
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &decoded); err != nil {
		t.Fatalf("expected valid JSON object, got %v", err)
	}
	if decoded.Total != 96 || decoded.Executable != 1 || decoded.Skipped != 95 {
		t.Fatalf("expected fixture file to make one sample executable, got %#v", decoded)
	}
}

func TestRunM6ShadowRunExecutesConcreteFixturesAndPrintsSummary(t *testing.T) {
	fixtures := `{"samples":[{"name":"known-message","args":["queryMsgById","-i","AC18000300002A9F0000000000000000"]}]}`
	runner := &recordingRunner{output: "message detail\n"}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected result line plus summary line, got %d lines: %q", len(lines), stdout.String())
	}
	var report nativeadmin.ShadowReport
	if err := json.Unmarshal([]byte(lines[0]), &report); err != nil {
		t.Fatalf("expected first line to be shadow report JSON, got %v", err)
	}
	if report.Command != "queryMsgById" || report.Primary.Name != "official" {
		t.Fatalf("unexpected report: %#v", report)
	}
	if len(report.Diffs) != 3 {
		t.Fatalf("expected sidecar/native/auto diffs, got %#v", report.Diffs)
	}
	for _, diff := range report.Diffs {
		if !diff.Matched {
			t.Fatalf("expected matching diff, got %#v", diff)
		}
	}
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[1]), &summary); err != nil {
		t.Fatalf("expected second line to be summary JSON, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	expectedCall := []string{"queryMsgById", "-n", "127.0.0.1:9876", "-i", "AC18000300002A9F0000000000000000"}
	if len(runner.calls) != 4 {
		t.Fatalf("expected official plus three targets to run, got %#v", runner.calls)
	}
	for _, call := range runner.calls {
		if !reflect.DeepEqual(call, expectedCall) {
			t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expectedCall, call)
		}
	}
}

func TestRunM6ShadowRunExecutesMessageChainFixture(t *testing.T) {
	fixtures := `{"samples":[{"name":"message-chain-cold","args":["messageChain","-t","sample_notice_topic","-k","user-10001","-b","0","-e","9223372036854775807","-m","1","--traceTopic","RMQ_SYS_TRACE_TOPIC"]}]}`
	runner := &mappedRecordingRunner{outputsByCommand: map[string]string{
		"queryMsgByKey":     "0AE97A6A00017F3CA64A23D49A900003 3 10240\n",
		"topicStatus":       messageChainTopicStatusOutputForTest("broker-a", 3, 0, 10241),
		"queryMsgByOffset":  messageChainDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": "",
	}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected result line plus summary line, got %d lines: %q", len(lines), stdout.String())
	}
	var report nativeadmin.ShadowReport
	if err := json.Unmarshal([]byte(lines[0]), &report); err != nil {
		t.Fatalf("expected first line to be shadow report JSON, got %v", err)
	}
	if report.Command != "messageChain" || report.Primary.Name != "official" || report.Primary.Error != "" {
		t.Fatalf("unexpected messageChain report: %#v", report)
	}
	for _, diff := range report.Diffs {
		if !diff.Matched {
			t.Fatalf("expected matching messageChain diff, got %#v", diff)
		}
	}
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[1]), &summary); err != nil {
		t.Fatalf("expected summary JSON, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected messageChain summary: %#v", summary)
	}
	if runner.countCommand("messageChain") != 0 {
		t.Fatalf("messageChain must be expanded before hitting mqadmin runner, calls=%#v", runner.commands())
	}
	if report.Primary.StdoutBytes == 0 {
		t.Fatalf("expected primary messageChain stdout bytes to be recorded, got %#v", report.Primary)
	}
	if runner.countCommand("queryMsgByKey") != 4 || runner.countCommand("topicStatus") != 4 || runner.countCommand("queryMsgByOffset") != 4 || runner.countCommand("queryMsgTraceById") != 4 {
		t.Fatalf("expected four provider paths to execute typed message chain commands, calls=%#v", runner.commands())
	}
	keyCall := runner.firstCommand("queryMsgByKey")
	if begin := stringArgForCLITest(t, keyCall, "-b"); begin != "0" {
		t.Fatalf("expected explicit begin timestamp, got %s in %#v", begin, keyCall)
	}
	if end := stringArgForCLITest(t, keyCall, "-e"); end != "9223372036854775807" {
		t.Fatalf("expected explicit end timestamp, got %s in %#v", end, keyCall)
	}
	if maxNum := stringArgForCLITest(t, keyCall, "-m"); maxNum != "1" {
		t.Fatalf("expected explicit maxNum=1, got %s in %#v", maxNum, keyCall)
	}
	traceCall := runner.firstCommand("queryMsgTraceById")
	if traceTopic := stringArgForCLITest(t, traceCall, "-t"); traceTopic != "RMQ_SYS_TRACE_TOPIC" {
		t.Fatalf("expected explicit trace topic, got %s in %#v", traceTopic, traceCall)
	}
}

func TestRunM6ShadowRunExecutesMessageChainMessageIDTraceFixture(t *testing.T) {
	fixtures := `{"samples":[{"name":"message-chain-warm","args":["messageChain","-t","sample_notice_topic","-i","7F00000100002A9F00000000000123AB","-g","CG_NOTICE","-b","0","-e","9223372036854775807","--traceTopic","RMQ_SYS_TRACE_TOPIC"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"queryMsgById":      messageChainDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
			"queryMsgTraceById": messageChainTraceSuccessOutputForTest("CG_NOTICE"),
		},
		errByCommand: map[string]error{
			"messageChain":     errors.New("messageChain must be expanded before hitting mqadmin runner"),
			"queryMsgByKey":    errors.New("message id fixture must not search by key"),
			"topicStatus":      errors.New("message id fixture must not resolve key candidate broker"),
			"queryMsgByOffset": errors.New("message id fixture must not use offset detail path"),
			"consumerProgress": errors.New("trace success for the same group must skip consumerProgress"),
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q calls=%#v", code, stderr.String(), stdout.String(), runner.commands())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected result line plus summary line, got %d lines: %q", len(lines), stdout.String())
	}
	var report nativeadmin.ShadowReport
	if err := json.Unmarshal([]byte(lines[0]), &report); err != nil {
		t.Fatalf("expected first line to be shadow report JSON, got %v", err)
	}
	if report.Command != "messageChain" || report.Primary.Name != "official" || report.Primary.Error != "" {
		t.Fatalf("unexpected messageChain report: %#v", report)
	}
	for _, diff := range report.Diffs {
		if !diff.Matched {
			t.Fatalf("expected matching messageChain diff, got %#v", diff)
		}
	}
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[1]), &summary); err != nil {
		t.Fatalf("expected summary JSON, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected messageChain summary: %#v", summary)
	}
	if runner.countCommand("queryMsgById") != 4 || runner.countCommand("queryMsgTraceById") != 4 {
		t.Fatalf("expected four provider paths to execute id detail and trace, calls=%#v", runner.commands())
	}
	for _, command := range []string{"messageChain", "queryMsgByKey", "topicStatus", "queryMsgByOffset", "consumerProgress"} {
		if runner.countCommand(command) != 0 {
			t.Fatalf("expected %s to stay unused, calls=%#v", command, runner.commands())
		}
	}
	detailCall := runner.firstCommand("queryMsgById")
	if messageID := stringArgForCLITest(t, detailCall, "-i"); messageID != "7F00000100002A9F00000000000123AB" {
		t.Fatalf("expected queryMsgById to use supplied OffsetID, got %s in %#v", messageID, detailCall)
	}
	traceCall := runner.firstCommand("queryMsgTraceById")
	if messageID := stringArgForCLITest(t, traceCall, "-i"); messageID != "0AE97A6A00017F3CA64A23D49A900003" {
		t.Fatalf("expected trace query to use detail UNIQ_KEY, got %s in %#v", messageID, traceCall)
	}
	if traceTopic := stringArgForCLITest(t, traceCall, "-t"); traceTopic != "RMQ_SYS_TRACE_TOPIC" {
		t.Fatalf("expected explicit trace topic, got %s in %#v", traceTopic, traceCall)
	}
}

func TestRunM6ShadowRunPassesConcurrencyToBatchExecution(t *testing.T) {
	fixtures := `{"samples":[
		{"name":"command-smoke","args":["slow"]},
		{"name":"command-smoke","args":["medium"]},
		{"name":"command-smoke","args":["fast"]}
	]}`
	runner := &timedShadowRecordingRunner{
		delayByCommand: map[string]time.Duration{
			"slow":   40 * time.Millisecond,
			"medium": 40 * time.Millisecond,
			"fast":   40 * time.Millisecond,
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-concurrency", "3", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q", code, stderr.String())
	}
	if runner.MaxActive() < 3 {
		t.Fatalf("expected runner to observe concurrent batch execution, max active=%d", runner.MaxActive())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected three result lines plus summary line, got %d lines: %q", len(lines), stdout.String())
	}
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 3 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected summary after concurrent run: %#v", summary)
	}
}

func TestRunM6ShadowRunRestoresWritePermBeforeEachWipeWritePermProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"wipe-write-perm","args":["wipeWritePerm","-n","127.0.0.1:9876","-b","broker-a"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"addWritePerm":  "add write perm of broker[broker-a] in name server[127.0.0.1:9876] OK, 31\n",
			"wipeWritePerm": "wipe write perm of broker[broker-a] in name server[127.0.0.1:9876] OK, 31\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"addWritePerm", "wipeWritePerm", "addWritePerm",
		"addWritePerm", "wipeWritePerm", "addWritePerm",
		"addWritePerm", "wipeWritePerm", "addWritePerm",
		"addWritePerm", "wipeWritePerm", "addWritePerm",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected write permission restoration before every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected wipeWritePerm summary: %#v", summary)
	}
}

func TestRunM6ShadowRunPreparesAndRestoresWritePermAroundEachAddWritePermProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"add-write-perm","args":["addWritePerm","-n","127.0.0.1:9876","-b","broker-a"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"addWritePerm":  "add write perm of broker[broker-a] in name server[127.0.0.1:9876] OK, 31\n",
			"wipeWritePerm": "wipe write perm of broker[broker-a] in name server[127.0.0.1:9876] OK, 31\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"wipeWritePerm", "addWritePerm", "addWritePerm",
		"wipeWritePerm", "addWritePerm", "addWritePerm",
		"wipeWritePerm", "addWritePerm", "addWritePerm",
		"wipeWritePerm", "addWritePerm", "addWritePerm",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected write permission preparation and restoration around every addWritePerm provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected addWritePerm summary: %#v", summary)
	}
}

func TestRunM6ShadowRunPreparesAndCleansDeleteKvConfigAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"delete-kv-config","args":["deleteKvConfig","-n","127.0.0.1:9876","-s","GoadminM6ShadowKV","-k","key-a"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateKvConfig": "create or update kv config to namespace success.\n",
			"deleteKvConfig": "delete kv config from namespace success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateKvConfig", "deleteKvConfig", "deleteKvConfig",
		"updateKvConfig", "deleteKvConfig", "deleteKvConfig",
		"updateKvConfig", "deleteKvConfig", "deleteKvConfig",
		"updateKvConfig", "deleteKvConfig", "deleteKvConfig",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected KV preparation before and cleanup after every deleteKvConfig provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "updateKvConfig":
			if value := stringArgForCLITest(t, call, "-v"); value != "m6-shadow-delete-kv-prepare" {
				t.Fatalf("expected updateKvConfig prepare value, got %q in %#v", value, call)
			}
		case "deleteKvConfig":
			if namespace := stringArgForCLITest(t, call, "-s"); namespace != "GoadminM6ShadowKV" {
				t.Fatalf("expected namespace to be copied, got %q in %#v", namespace, call)
			}
			if key := stringArgForCLITest(t, call, "-k"); key != "key-a" {
				t.Fatalf("expected key to be copied, got %q in %#v", key, call)
			}
		default:
			t.Fatalf("unexpected command in KV hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected deleteKvConfig summary: %#v", summary)
	}
}

// TestRunM6ShadowRunPreparesAndCleansUpdateOrderConfDeleteAroundEachProvider 验证每路 provider 删除真实存在的 orderConf 并清理残留。
func TestRunM6ShadowRunPreparesAndCleansUpdateOrderConfDeleteAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-order-conf-delete","args":["updateOrderConf","-n","127.0.0.1:9876","-m","delete","-t","GoadminM6128OrderDelete"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateOrderConf": "delete orderConf success. topic=[GoadminM6128OrderDelete]",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected orderConf seed and cleanup around every delete provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if topic := stringArgForCLITest(t, call, "-t"); topic != "GoadminM6128OrderDelete" {
			t.Fatalf("expected topic to be copied, got %q in %#v", topic, call)
		}
		expectedMethod := "delete"
		if index%3 == 0 {
			expectedMethod = "put"
			if value := stringArgForCLITest(t, call, "-v"); value != "m6-shadow-order-conf:1" {
				t.Fatalf("expected orderConf seed value, got %q in %#v", value, call)
			}
		}
		if method := stringArgForCLITest(t, call, "-m"); method != expectedMethod {
			t.Fatalf("expected method %s at call %d, got %q in %#v", expectedMethod, index, method, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateOrderConf delete summary: %#v", summary)
	}
}

// TestRunM6ShadowRunCleansUpdateOrderConfPutAroundEachProvider 验证每路 provider 写入前后都删除同一 orderConf key。
func TestRunM6ShadowRunCleansUpdateOrderConfPutAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-order-conf-put","args":["updateOrderConf","-n","127.0.0.1:9876","-m","put","-t","GoadminM6129OrderPut","-v","broker-a:4"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateOrderConf": "update orderConf success. topic=[GoadminM6129OrderPut], orderConf=[broker-a:4]",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
		"updateOrderConf", "updateOrderConf", "updateOrderConf",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected orderConf cleanup around every put provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if topic := stringArgForCLITest(t, call, "-t"); topic != "GoadminM6129OrderPut" {
			t.Fatalf("expected topic to be copied, got %q in %#v", topic, call)
		}
		expectedMethod := "delete"
		if index%3 == 1 {
			expectedMethod = "put"
			if value := stringArgForCLITest(t, call, "-v"); value != "broker-a:4" {
				t.Fatalf("expected orderConf target value, got %q in %#v", value, call)
			}
		}
		if method := stringArgForCLITest(t, call, "-m"); method != expectedMethod {
			t.Fatalf("expected method %s at call %d, got %q in %#v", expectedMethod, index, method, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateOrderConf put summary: %#v", summary)
	}
}

func TestRunM6ShadowRunResetsUpdateUserTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-user","args":["updateUser","-b","127.0.0.1:31102","-u","goadmin-m6-update-user","-s","disable"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateUser": "update user to 127.0.0.1:31102 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateUser", "updateUser", "updateUser",
		"updateUser", "updateUser", "updateUser",
		"updateUser", "updateUser", "updateUser",
		"updateUser", "updateUser", "updateUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected updateUser baseline reset around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if call[0] != "updateUser" {
			t.Fatalf("unexpected command in updateUser hook run: %#v", call)
		}
		if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31102" {
			t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
		}
		if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-update-user" {
			t.Fatalf("expected username to be copied, got %q in %#v", username, call)
		}
		status := stringArgForCLITest(t, call, "-s")
		if index%3 == 1 {
			if status != "disable" {
				t.Fatalf("expected provider update to preserve fixture status disable, got %q in %#v", status, call)
			}
			continue
		}
		if status != "enable" {
			t.Fatalf("expected before/after reset status enable, got %q in %#v", status, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateUser summary: %#v", summary)
	}
}

func TestRunM6ShadowRunResetsUpdateNamesrvConfigAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-namesrv-config","args":["updateNamesrvConfig","-n","127.0.0.1:9876","-k","m6ShadowNamesrvConfig","-v","m6-71-target"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateNamesrvConfig": "update name server config success![127.0.0.1:9876]\nm6ShadowNamesrvConfig : m6-71-target\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateNamesrvConfig", "updateNamesrvConfig", "updateNamesrvConfig",
		"updateNamesrvConfig", "updateNamesrvConfig", "updateNamesrvConfig",
		"updateNamesrvConfig", "updateNamesrvConfig", "updateNamesrvConfig",
		"updateNamesrvConfig", "updateNamesrvConfig", "updateNamesrvConfig",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected updateNamesrvConfig baseline reset around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if call[0] != "updateNamesrvConfig" {
			t.Fatalf("unexpected command in updateNamesrvConfig hook run: %#v", call)
		}
		if key := stringArgForCLITest(t, call, "-k"); key != "m6ShadowNamesrvConfig" {
			t.Fatalf("expected config key to be copied, got %q in %#v", key, call)
		}
		value := stringArgForCLITest(t, call, "-v")
		if index%3 == 1 {
			if value != "m6-71-target" {
				t.Fatalf("expected target value on provider command, got %q in %#v", value, call)
			}
			continue
		}
		if value != "m6-shadow-namesrv-baseline" {
			t.Fatalf("expected baseline reset value, got %q in %#v", value, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateNamesrvConfig summary: %#v", summary)
	}
}

func TestRunM6ShadowRunResetsUpdateBrokerConfigAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-broker-config","args":["updateBrokerConfig","-b","127.0.0.1:10911","-k","m6ShadowBrokerConfig","-v","m6-72-target"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateBrokerConfig": "update broker config success, 127.0.0.1:10911\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateBrokerConfig", "updateBrokerConfig", "updateBrokerConfig",
		"updateBrokerConfig", "updateBrokerConfig", "updateBrokerConfig",
		"updateBrokerConfig", "updateBrokerConfig", "updateBrokerConfig",
		"updateBrokerConfig", "updateBrokerConfig", "updateBrokerConfig",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected updateBrokerConfig baseline reset around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if call[0] != "updateBrokerConfig" {
			t.Fatalf("unexpected command in updateBrokerConfig hook run: %#v", call)
		}
		if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:10911" {
			t.Fatalf("expected broker addr to be copied, got %q in %#v", broker, call)
		}
		if key := stringArgForCLITest(t, call, "-k"); key != "m6ShadowBrokerConfig" {
			t.Fatalf("expected config key to be copied, got %q in %#v", key, call)
		}
		value := stringArgForCLITest(t, call, "-v")
		if index%3 == 1 {
			if value != "m6-72-target" {
				t.Fatalf("expected target value on provider command, got %q in %#v", value, call)
			}
			continue
		}
		if value != "m6-shadow-broker-baseline" {
			t.Fatalf("expected broker baseline reset value, got %q in %#v", value, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateBrokerConfig summary: %#v", summary)
	}
}

func TestRunM6ShadowRunCleansUpdateColdDataFlowCtrGroupConfigAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-cold-data-flow-ctr-group-config","args":["updateColdDataFlowCtrGroupConfig","-b","127.0.0.1:10911","-g","M673ColdGroup","-v","123"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateColdDataFlowCtrGroupConfig": "updateColdDataFlowCtrGroupConfig success, 127.0.0.1:10911\n",
			"removeColdDataFlowCtrGroupConfig": "remove broker cold read threshold success, 127.0.0.1:10911\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"removeColdDataFlowCtrGroupConfig", "updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
		"removeColdDataFlowCtrGroupConfig", "updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
		"removeColdDataFlowCtrGroupConfig", "updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
		"removeColdDataFlowCtrGroupConfig", "updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected cold data flow threshold cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:10911" {
			t.Fatalf("expected broker addr to be copied, got %q in %#v", broker, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "M673ColdGroup" {
			t.Fatalf("expected consumer group to be copied, got %q in %#v", group, call)
		}
		if index%3 == 1 {
			if call[0] != "updateColdDataFlowCtrGroupConfig" {
				t.Fatalf("expected provider command to be updateColdDataFlowCtrGroupConfig, got %#v", call)
			}
			if threshold := stringArgForCLITest(t, call, "-v"); threshold != "123" {
				t.Fatalf("expected target threshold to be preserved, got %q in %#v", threshold, call)
			}
			continue
		}
		if call[0] != "removeColdDataFlowCtrGroupConfig" {
			t.Fatalf("expected cleanup command to be removeColdDataFlowCtrGroupConfig, got %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateColdDataFlowCtrGroupConfig summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsRemoveColdDataFlowCtrGroupConfigAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"remove-cold-data-flow-ctr-group-config","args":["removeColdDataFlowCtrGroupConfig","-b","127.0.0.1:10911","-g","M674ColdGroup"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateColdDataFlowCtrGroupConfig": "updateColdDataFlowCtrGroupConfig success, 127.0.0.1:10911\n",
			"removeColdDataFlowCtrGroupConfig": "remove broker cold read threshold success, 127.0.0.1:10911\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
		"updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
		"updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
		"updateColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig", "removeColdDataFlowCtrGroupConfig",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected cold data flow threshold seed and cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:10911" {
			t.Fatalf("expected broker addr to be copied, got %q in %#v", broker, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "M674ColdGroup" {
			t.Fatalf("expected consumer group to be copied, got %q in %#v", group, call)
		}
		if index%3 == 0 {
			if call[0] != "updateColdDataFlowCtrGroupConfig" {
				t.Fatalf("expected seed command to be updateColdDataFlowCtrGroupConfig, got %#v", call)
			}
			if threshold := stringArgForCLITest(t, call, "-v"); threshold != "m6-shadow-cold-flow-threshold" {
				t.Fatalf("expected seed threshold, got %q in %#v", threshold, call)
			}
			continue
		}
		if call[0] != "removeColdDataFlowCtrGroupConfig" {
			t.Fatalf("expected provider or cleanup command to be removeColdDataFlowCtrGroupConfig, got %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected removeColdDataFlowCtrGroupConfig summary: %#v", summary)
	}
}

func TestRunM6ShadowRunCleansUpdateTopicAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-topic","args":["updateTopic","-n","127.0.0.1:9876","-c","DefaultCluster","-t","M676UpdateTopic"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateTopic": "create topic to 127.0.0.1:10911 success.\nTopicConfig [topicName=M676UpdateTopic, readQueueNums=8, writeQueueNums=8, perm=RW-, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}]\n",
			"deleteTopic": "delete topic [M676UpdateTopic] from cluster [DefaultCluster] success.\ndelete topic [M676UpdateTopic] from NameServer success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteTopic", "updateTopic", "deleteTopic",
		"deleteTopic", "updateTopic", "deleteTopic",
		"deleteTopic", "updateTopic", "deleteTopic",
		"deleteTopic", "updateTopic", "deleteTopic",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected updateTopic cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
			t.Fatalf("expected cluster to be copied, got %q in %#v", cluster, call)
		}
		if topic := stringArgForCLITest(t, call, "-t"); topic != "M676UpdateTopic" {
			t.Fatalf("expected topic to be copied, got %q in %#v", topic, call)
		}
		if index%3 == 1 {
			if call[0] != "updateTopic" {
				t.Fatalf("expected provider command to be updateTopic, got %#v", call)
			}
			continue
		}
		if call[0] != "deleteTopic" {
			t.Fatalf("expected cleanup command to be deleteTopic, got %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateTopic summary: %#v", summary)
	}
}

// TestRunM6ShadowRunRestoresUpdateTopicPermAroundEachProvider 验证四路 provider 都从 perm=6 开始并恢复到 perm=6。
func TestRunM6ShadowRunRestoresUpdateTopicPermAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-topic-perm","args":["updateTopicPerm","-n","127.0.0.1:9876","-b","127.0.0.1:10911","-t","M6127UpdateTopicPerm","-p","4"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateTopicPerm": "updateTopicPerm topic [M6127UpdateTopicPerm] from 6 to 4 in 127.0.0.1:10911 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateTopicPerm", "updateTopicPerm", "updateTopicPerm",
		"updateTopicPerm", "updateTopicPerm", "updateTopicPerm",
		"updateTopicPerm", "updateTopicPerm", "updateTopicPerm",
		"updateTopicPerm", "updateTopicPerm", "updateTopicPerm",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected permission restore around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:10911" {
			t.Fatalf("expected broker to be copied, got %q in %#v", broker, call)
		}
		if topic := stringArgForCLITest(t, call, "-t"); topic != "M6127UpdateTopicPerm" {
			t.Fatalf("expected topic to be copied, got %q in %#v", topic, call)
		}
		expectedPerm := "6"
		if index%3 == 1 {
			expectedPerm = "4"
		}
		if perm := stringArgForCLITest(t, call, "-p"); perm != expectedPerm {
			t.Fatalf("expected perm %s at call %d, got %q in %#v", expectedPerm, index, perm, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateTopicPerm summary: %#v", summary)
	}
}

// TestRunM6ShadowRunRestoresSetConsumeModeAroundEachProvider 验证四路 provider 都从 POP/q=1 基线执行 PULL/q=0 并恢复。
func TestRunM6ShadowRunRestoresSetConsumeModeAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"set-consume-mode","args":["setConsumeMode","-n","127.0.0.1:9876","-c","DefaultCluster","-t","M6130ModeTopic","-g","M6130ModeGroup","-m","PULL","-q","0"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"setConsumeMode": "set consume mode to 127.0.0.1:10911 success.\ntopic[M6130ModeTopic] group[M6130ModeGroup] consume mode[PULL] popShareQueueNum[0]\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"setConsumeMode", "setConsumeMode", "setConsumeMode",
		"setConsumeMode", "setConsumeMode", "setConsumeMode",
		"setConsumeMode", "setConsumeMode", "setConsumeMode",
		"setConsumeMode", "setConsumeMode", "setConsumeMode",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected mode restore around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
			t.Fatalf("expected cluster to be copied, got %q in %#v", cluster, call)
		}
		if topic := stringArgForCLITest(t, call, "-t"); topic != "M6130ModeTopic" {
			t.Fatalf("expected topic to be copied, got %q in %#v", topic, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "M6130ModeGroup" {
			t.Fatalf("expected group to be copied, got %q in %#v", group, call)
		}
		expectedMode, expectedQueue := "POP", "1"
		if index%3 == 1 {
			expectedMode, expectedQueue = "PULL", "0"
		}
		if mode := stringArgForCLITest(t, call, "-m"); mode != expectedMode {
			t.Fatalf("expected mode %s at call %d, got %q in %#v", expectedMode, index, mode, call)
		}
		if queue := stringArgForCLITest(t, call, "-q"); queue != expectedQueue {
			t.Fatalf("expected queue %s at call %d, got %q in %#v", expectedQueue, index, queue, call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected setConsumeMode summary: %#v", summary)
	}
}

// TestRunM6ShadowRunPreparesAndCleansResetOffsetSpecifiedQueueAroundEachProvider 验证四路 provider 均执行集群级组清理、重建、指定队列重置和最终清理。
func TestRunM6ShadowRunPreparesAndCleansResetOffsetSpecifiedQueueAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"reset-offset-by-time-specified-queue","args":["resetOffsetByTime","-n","127.0.0.1:9876","-g","GoadminM6131ResetOffset","-t","GoadminQueryKeyTest","-s","-1","-f","false","-c","DefaultCluster","-b","127.0.0.1:10911","-q","0","-o","2262883"]}]}`
	runner := &mappedRecordingRunner{outputsByCommand: map[string]string{
		"deleteSubGroup":    "delete subscription group success.\n",
		"updateSubGroup":    "create subscription group success.\n",
		"resetOffsetByTime": "reset consumer offset to 2262883\n",
	}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	expected := make([]string, 0, 16)
	for index := 0; index < 4; index++ {
		expected = append(expected, "deleteSubGroup", "updateSubGroup", "resetOffsetByTime", "deleteSubGroup")
	}
	commands := runner.commandNames()
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected resetOffsetByTime lifecycle around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "GoadminM6131ResetOffset" {
			t.Fatalf("expected group to be copied, got %q in %#v", group, call)
		}
		switch index % 4 {
		case 0, 3:
			if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
				t.Fatalf("expected cluster-scoped cleanup, got %q in %#v", cluster, call)
			}
			if removeOffset := stringArgForCLITest(t, call, "-r"); removeOffset != "true" {
				t.Fatalf("expected deleteSubGroup -r true, got %q in %#v", removeOffset, call)
			}
		case 1:
			if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
				t.Fatalf("expected cluster-scoped group setup, got %q in %#v", cluster, call)
			}
		case 2:
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:10911" {
				t.Fatalf("expected target broker to be preserved, got %q in %#v", broker, call)
			}
			if queue := stringArgForCLITest(t, call, "-q"); queue != "0" {
				t.Fatalf("expected target queue to be preserved, got %q in %#v", queue, call)
			}
			if offset := stringArgForCLITest(t, call, "-o"); offset != "2262883" {
				t.Fatalf("expected target offset to be preserved, got %q in %#v", offset, call)
			}
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected resetOffsetByTime summary: %#v", summary)
	}
}

// TestRunM6ShadowRunPreparesAndCleansResetOffsetAllQueuesAroundEachProvider 验证四路 provider 均执行集群级组清理、重建、全队列重置和最终清理。
func TestRunM6ShadowRunPreparesAndCleansResetOffsetAllQueuesAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"reset-offset-by-time-all-queues","args":["resetOffsetByTime","-n","127.0.0.1:9876","-g","GoadminM6132ResetAll","-t","GoadminQueryKeyTest","-s","-1","-f","false","-c","DefaultCluster"]}]}`
	runner := &mappedRecordingRunner{outputsByCommand: map[string]string{
		"deleteSubGroup":    "delete subscription group success.\n",
		"updateSubGroup":    "create subscription group success.\n",
		"resetOffsetByTime": "reset all consumer offsets\n",
	}}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	expected := make([]string, 0, 16)
	for index := 0; index < 4; index++ {
		expected = append(expected, "deleteSubGroup", "updateSubGroup", "resetOffsetByTime", "deleteSubGroup")
	}
	commands := runner.commandNames()
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected all-queue reset lifecycle around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "GoadminM6132ResetAll" {
			t.Fatalf("expected group to be copied, got %q in %#v", group, call)
		}
		switch index % 4 {
		case 0, 3:
			if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
				t.Fatalf("expected cluster-scoped cleanup, got %q in %#v", cluster, call)
			}
			if removeOffset := stringArgForCLITest(t, call, "-r"); removeOffset != "true" {
				t.Fatalf("expected deleteSubGroup -r true, got %q in %#v", removeOffset, call)
			}
		case 1:
			if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
				t.Fatalf("expected cluster-scoped group setup, got %q in %#v", cluster, call)
			}
		case 2:
			joined := " " + strings.Join(call, " ") + " "
			if strings.Contains(joined, " -b ") || strings.Contains(joined, " -q ") || strings.Contains(joined, " -o ") {
				t.Fatalf("expected all-queue reset without broker/queue/offset selectors, got %#v", call)
			}
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected all-queue reset summary: %#v", summary)
	}
}

func TestRunM6ShadowRunCleansUpdateTopicListAroundEachProvider(t *testing.T) {
	fixturePath := filepath.Join(t.TempDir(), "topics.json")
	if err := os.WriteFile(fixturePath, []byte(`[{"topicName":"M6126BatchTopicA"},{"topicName":"M6126BatchTopicB"}]`), 0o600); err != nil {
		t.Fatalf("write topic fixture: %v", err)
	}
	fixtures := fmt.Sprintf(`{"samples":[{"name":"update-topic-list","args":["updateTopicList","-n","127.0.0.1:9876","-c","DefaultCluster","-f",%q]}]}`, fixturePath)
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateTopicList": "submit batch of topic config to 127.0.0.1:10911 success, please check the result later.\n",
			"deleteTopic":     "delete topic success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	expectedProviderCalls := []string{"deleteTopic", "deleteTopic", "updateTopicList", "deleteTopic", "deleteTopic"}
	commands := runner.commandNames()
	if len(commands) != len(expectedProviderCalls)*4 {
		t.Fatalf("expected cleanup around four providers, got commands=%#v calls=%#v", commands, runner.commands())
	}
	for provider := 0; provider < 4; provider++ {
		start := provider * len(expectedProviderCalls)
		if !reflect.DeepEqual(commands[start:start+len(expectedProviderCalls)], expectedProviderCalls) {
			t.Fatalf("unexpected provider %d command sequence: %#v", provider, commands[start:start+len(expectedProviderCalls)])
		}
		calls := runner.commands()[start : start+len(expectedProviderCalls)]
		for index, expectedTopic := range []string{"M6126BatchTopicA", "M6126BatchTopicB"} {
			if topic := stringArgForCLITest(t, calls[index], "-t"); topic != expectedTopic {
				t.Fatalf("expected before cleanup topic %q, got %q in %#v", expectedTopic, topic, calls[index])
			}
			if topic := stringArgForCLITest(t, calls[index+3], "-t"); topic != expectedTopic {
				t.Fatalf("expected after cleanup topic %q, got %q in %#v", expectedTopic, topic, calls[index+3])
			}
		}
	}
}

func TestRunM6ShadowRunCleansUpdateSubGroupAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-sub-group","args":["updateSubGroup","-n","127.0.0.1:9876","-c","DefaultCluster","-g","M677UpdateSubGroup"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateSubGroup": "create subscription group to 127.0.0.1:10911 success.\nSubscriptionGroupConfig{groupName=M677UpdateSubGroup, consumeEnable=true, consumeFromMinEnable=false, consumeBroadcastEnable=false, consumeMessageOrderly=false, retryQueueNums=1, retryMaxTimes=16, groupRetryPolicy=GroupRetryPolicy{type=CUSTOMIZED, exponentialRetryPolicy=null, customizedRetryPolicy=null}, brokerId=0, whichBrokerWhenConsumeSlowly=1, notifyConsumerIdsChangedEnable=true, groupSysFlag=0, consumeTimeoutMinute=15, subscriptionDataSet=null, attributes={}}\n",
			"deleteSubGroup": "delete subscription group [M677UpdateSubGroup] from broker [127.0.0.1:10911] in cluster [DefaultCluster] success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteSubGroup", "updateSubGroup", "deleteSubGroup",
		"deleteSubGroup", "updateSubGroup", "deleteSubGroup",
		"deleteSubGroup", "updateSubGroup", "deleteSubGroup",
		"deleteSubGroup", "updateSubGroup", "deleteSubGroup",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected updateSubGroup cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
			t.Fatalf("expected cluster to be copied, got %q in %#v", cluster, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "M677UpdateSubGroup" {
			t.Fatalf("expected group to be copied, got %q in %#v", group, call)
		}
		if index%3 == 1 {
			if call[0] != "updateSubGroup" {
				t.Fatalf("expected provider command to be updateSubGroup, got %#v", call)
			}
			continue
		}
		if call[0] != "deleteSubGroup" {
			t.Fatalf("expected cleanup command to be deleteSubGroup, got %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateSubGroup summary: %#v", summary)
	}
}

func TestRunM6ShadowRunCleansUpdateSubGroupListAroundEachProvider(t *testing.T) {
	fixturePath := filepath.Join(t.TempDir(), "subscription-groups.json")
	if err := os.WriteFile(fixturePath, []byte(`[{"groupName":"M6125BatchGroupA"},{"groupName":"M6125BatchGroupB"}]`), 0o600); err != nil {
		t.Fatalf("write subscription group fixture: %v", err)
	}
	fixtures := fmt.Sprintf(`{"samples":[{"name":"update-sub-group-list","args":["updateSubGroupList","-n","127.0.0.1:9876","-b","127.0.0.1:10911","-f",%q]}]}`, fixturePath)
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateSubGroupList": "submit batch of group config to 127.0.0.1:10911 success, please check the result later.\n",
			"deleteSubGroup":     "delete subscription group success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures, "--m6-shadow-concurrency", "1"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	expectedProviderCalls := []string{"deleteSubGroup", "deleteSubGroup", "updateSubGroupList", "deleteSubGroup", "deleteSubGroup"}
	commands := runner.commandNames()
	if len(commands) != len(expectedProviderCalls)*4 {
		t.Fatalf("expected cleanup around four providers, got commands=%#v calls=%#v", commands, runner.commands())
	}
	for provider := 0; provider < 4; provider++ {
		start := provider * len(expectedProviderCalls)
		if !reflect.DeepEqual(commands[start:start+len(expectedProviderCalls)], expectedProviderCalls) {
			t.Fatalf("unexpected provider %d command sequence: %#v", provider, commands[start:start+len(expectedProviderCalls)])
		}
		calls := runner.commands()[start : start+len(expectedProviderCalls)]
		for index, expectedGroup := range []string{"M6125BatchGroupA", "M6125BatchGroupB"} {
			if group := stringArgForCLITest(t, calls[index], "-g"); group != expectedGroup {
				t.Fatalf("expected before cleanup group %q, got %q in %#v", expectedGroup, group, calls[index])
			}
			if group := stringArgForCLITest(t, calls[index+3], "-g"); group != expectedGroup {
				t.Fatalf("expected after cleanup group %q, got %q in %#v", expectedGroup, group, calls[index+3])
			}
		}
	}
}

func TestRunM6ShadowRunSeedsDeleteSubGroupAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"delete-sub-group","args":["deleteSubGroup","-n","127.0.0.1:9876","-c","DefaultCluster","-g","M678DeleteSubGroup"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateSubGroup": "create subscription group to 127.0.0.1:10911 success.\nSubscriptionGroupConfig{groupName=M678DeleteSubGroup, consumeEnable=true, consumeFromMinEnable=false, consumeBroadcastEnable=false, consumeMessageOrderly=false, retryQueueNums=1, retryMaxTimes=16, groupRetryPolicy=GroupRetryPolicy{type=CUSTOMIZED, exponentialRetryPolicy=null, customizedRetryPolicy=null}, brokerId=0, whichBrokerWhenConsumeSlowly=1, notifyConsumerIdsChangedEnable=true, groupSysFlag=0, consumeTimeoutMinute=15, subscriptionDataSet=null, attributes={}}\n",
			"deleteSubGroup": "delete subscription group [M678DeleteSubGroup] from broker [127.0.0.1:10911] in cluster [DefaultCluster] success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateSubGroup", "deleteSubGroup", "deleteSubGroup",
		"updateSubGroup", "deleteSubGroup", "deleteSubGroup",
		"updateSubGroup", "deleteSubGroup", "deleteSubGroup",
		"updateSubGroup", "deleteSubGroup", "deleteSubGroup",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected deleteSubGroup setup and cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
			t.Fatalf("expected cluster to be copied, got %q in %#v", cluster, call)
		}
		if group := stringArgForCLITest(t, call, "-g"); group != "M678DeleteSubGroup" {
			t.Fatalf("expected group to be copied, got %q in %#v", group, call)
		}
		if index%3 == 0 {
			if call[0] != "updateSubGroup" {
				t.Fatalf("expected setup command to be updateSubGroup, got %#v", call)
			}
			continue
		}
		if call[0] != "deleteSubGroup" {
			t.Fatalf("expected target or cleanup command to be deleteSubGroup, got %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected deleteSubGroup summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsDeleteTopicAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"delete-topic","args":["deleteTopic","-n","127.0.0.1:9876","-c","DefaultCluster","-t","M675DeleteTopic"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"updateTopic": "create topic to 127.0.0.1:10911 success.\nTopicConfig [topicName=M675DeleteTopic, readQueueNums=8, writeQueueNums=8, perm=RW-, topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}]\n",
			"deleteTopic": "delete topic [M675DeleteTopic] from cluster [DefaultCluster] success.\ndelete topic [M675DeleteTopic] from NameServer success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"updateTopic", "deleteTopic", "deleteTopic",
		"updateTopic", "deleteTopic", "deleteTopic",
		"updateTopic", "deleteTopic", "deleteTopic",
		"updateTopic", "deleteTopic", "deleteTopic",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected deleteTopic topic seed and cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for index, call := range runner.commands() {
		if nameServer := stringArgForCLITest(t, call, "-n"); nameServer != "127.0.0.1:9876" {
			t.Fatalf("expected namesrv to be copied, got %q in %#v", nameServer, call)
		}
		if cluster := stringArgForCLITest(t, call, "-c"); cluster != "DefaultCluster" {
			t.Fatalf("expected cluster to be copied, got %q in %#v", cluster, call)
		}
		if topic := stringArgForCLITest(t, call, "-t"); topic != "M675DeleteTopic" {
			t.Fatalf("expected topic to be copied, got %q in %#v", topic, call)
		}
		if index%3 == 0 {
			if call[0] != "updateTopic" {
				t.Fatalf("expected seed command to be updateTopic, got %#v", call)
			}
			continue
		}
		if call[0] != "deleteTopic" {
			t.Fatalf("expected provider or cleanup command to be deleteTopic, got %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected deleteTopic summary: %#v", summary)
	}
}

func TestRunM6ShadowRunDeletesCreateUserTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"create-user","args":["createUser","-b","127.0.0.1:31092","-u","goadmin-m6-create-user","-p","seed-pass","-t","Super"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31092 success.\n",
			"createUser": "create user to 127.0.0.1:31092 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "createUser", "deleteUser",
		"deleteUser", "createUser", "deleteUser",
		"deleteUser", "createUser", "deleteUser",
		"deleteUser", "createUser", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected createUser target cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31092" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-create-user" {
				t.Fatalf("expected username to be copied, got %q in %#v", username, call)
			}
		case "createUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31092" {
				t.Fatalf("expected target broker to be preserved, got %q in %#v", broker, call)
			}
			if password := stringArgForCLITest(t, call, "-p"); password != "seed-pass" {
				t.Fatalf("expected password to be preserved, got %q in %#v", password, call)
			}
			if userType := stringArgForCLITest(t, call, "-t"); userType != "Super" {
				t.Fatalf("expected user type to be preserved, got %q in %#v", userType, call)
			}
		default:
			t.Fatalf("unexpected command in createUser hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected createUser summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsAndDeletesCreateAclTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"create-acl","args":["createAcl","-b","127.0.0.1:31182","-s","User:goadmin-m6-create-acl","-r","Topic:GoadminM679CreateAclTopic","-a","Pub","-d","Allow","-i","10.67.9.1"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31182 success.\n",
			"createUser": "create user to 127.0.0.1:31182 success.\n",
			"createAcl":  "create acl to 127.0.0.1:31182 success.\n",
			"deleteAcl":  "delete acl to 127.0.0.1:31182 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected createAcl user seed and acl cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31182" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-create-acl" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
		case "createUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31182" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-create-acl" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
			if password := stringArgForCLITest(t, call, "-p"); password != "m6-shadow-acl-pass" {
				t.Fatalf("expected deterministic seed password, got %q in %#v", password, call)
			}
			if userType := stringArgForCLITest(t, call, "-t"); userType != "Super" {
				t.Fatalf("expected super user type for ACL seed user, got %q in %#v", userType, call)
			}
		case "createAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31182" {
				t.Fatalf("expected target broker to be preserved, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-create-acl" {
				t.Fatalf("expected subject to be preserved, got %q in %#v", subject, call)
			}
			if sourceIP := stringArgForCLITest(t, call, "-i"); sourceIP != "10.67.9.1" {
				t.Fatalf("expected source IP to be preserved, got %q in %#v", sourceIP, call)
			}
		case "deleteAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31182" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-create-acl" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
			if resource := stringArgForCLITest(t, call, "-r"); resource != "Topic:GoadminM679CreateAclTopic" {
				t.Fatalf("expected resource to be copied, got %q in %#v", resource, call)
			}
		default:
			t.Fatalf("unexpected command in createAcl hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected createAcl summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsUpdatesAndDeletesUpdateAclTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"update-acl","args":["updateAcl","-b","127.0.0.1:31183","-s","User:goadmin-m6-update-acl","-r","Topic:GoadminM680UpdateAclTopic","-a","Sub","-d","Deny","-i","10.68.0.1"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31183 success.\n",
			"createUser": "create user to 127.0.0.1:31183 success.\n",
			"createAcl":  "create acl to 127.0.0.1:31183 success.\n",
			"updateAcl":  "update acl to 127.0.0.1:31183 success.\n",
			"deleteAcl":  "delete acl to 127.0.0.1:31183 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "createUser", "createAcl", "updateAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "updateAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "updateAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "updateAcl", "deleteAcl", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected updateAcl user and baseline seed plus cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31183" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-update-acl" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
		case "createUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31183" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-update-acl" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
			if password := stringArgForCLITest(t, call, "-p"); password != "m6-shadow-acl-pass" {
				t.Fatalf("expected deterministic seed password, got %q in %#v", password, call)
			}
			if userType := stringArgForCLITest(t, call, "-t"); userType != "Super" {
				t.Fatalf("expected super user type for ACL seed user, got %q in %#v", userType, call)
			}
		case "createAcl":
			if action := stringArgForCLITest(t, call, "-a"); action != "Pub" {
				t.Fatalf("expected updateAcl baseline action Pub, got %q in %#v", action, call)
			}
			if decision := stringArgForCLITest(t, call, "-d"); decision != "Allow" {
				t.Fatalf("expected updateAcl baseline decision Allow, got %q in %#v", decision, call)
			}
		case "updateAcl":
			if action := stringArgForCLITest(t, call, "-a"); action != "Sub" {
				t.Fatalf("expected fixture action Sub, got %q in %#v", action, call)
			}
			if decision := stringArgForCLITest(t, call, "-d"); decision != "Deny" {
				t.Fatalf("expected fixture decision Deny, got %q in %#v", decision, call)
			}
		case "deleteAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31183" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-update-acl" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
			if resource := stringArgForCLITest(t, call, "-r"); resource != "Topic:GoadminM680UpdateAclTopic" {
				t.Fatalf("expected resource to be copied, got %q in %#v", resource, call)
			}
		default:
			t.Fatalf("unexpected command in updateAcl hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected updateAcl summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsAndDeletesDeleteAclTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"delete-acl","args":["deleteAcl","-b","127.0.0.1:31184","-s","User:goadmin-m6-delete-acl","-r","Topic:GoadminM681DeleteAclTopic"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31184 success.\n",
			"createUser": "create user to 127.0.0.1:31184 success.\n",
			"createAcl":  "create acl to 127.0.0.1:31184 success.\n",
			"deleteAcl":  "delete acl to 127.0.0.1:31184 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "deleteAcl", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected deleteAcl user and baseline seed plus user cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31184" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-delete-acl" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
		case "createUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31184" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-delete-acl" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
			if password := stringArgForCLITest(t, call, "-p"); password != "m6-shadow-acl-pass" {
				t.Fatalf("expected deterministic seed password, got %q in %#v", password, call)
			}
			if userType := stringArgForCLITest(t, call, "-t"); userType != "Super" {
				t.Fatalf("expected super user type for ACL seed user, got %q in %#v", userType, call)
			}
		case "createAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31184" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-delete-acl" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
			if resource := stringArgForCLITest(t, call, "-r"); resource != "Topic:GoadminM681DeleteAclTopic" {
				t.Fatalf("expected resource to be copied, got %q in %#v", resource, call)
			}
			if action := stringArgForCLITest(t, call, "-a"); action != "Pub" {
				t.Fatalf("expected deleteAcl baseline action Pub, got %q in %#v", action, call)
			}
			if decision := stringArgForCLITest(t, call, "-d"); decision != "Allow" {
				t.Fatalf("expected deleteAcl baseline decision Allow, got %q in %#v", decision, call)
			}
		case "deleteAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31184" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-delete-acl" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
			if resource := stringArgForCLITest(t, call, "-r"); resource != "Topic:GoadminM681DeleteAclTopic" {
				t.Fatalf("expected resource to be copied, got %q in %#v", resource, call)
			}
		default:
			t.Fatalf("unexpected command in deleteAcl hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected deleteAcl summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsListsAndDeletesListAclTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"list-acl","args":["listAcl","-b","127.0.0.1:31185","-s","User:goadmin-m6-list-acl-user"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31185 success.\n",
			"createUser": "create user to 127.0.0.1:31185 success.\n",
			"createAcl":  "create acl to 127.0.0.1:31185 success.\n",
			"listAcl":    "Subject: User:goadmin-m6-list-acl-user\nResource: Topic:goadmin-m6-list-acl-topic\n",
			"deleteAcl":  "delete acl to 127.0.0.1:31185 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "createUser", "createAcl", "listAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "listAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "listAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "listAcl", "deleteAcl", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected listAcl user and baseline seed plus ACL/user cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31185" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-list-acl-user" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
		case "createUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31185" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-list-acl-user" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
			if password := stringArgForCLITest(t, call, "-p"); password != "m6-shadow-acl-pass" {
				t.Fatalf("expected deterministic seed password, got %q in %#v", password, call)
			}
			if userType := stringArgForCLITest(t, call, "-t"); userType != "Super" {
				t.Fatalf("expected super user type for ACL seed user, got %q in %#v", userType, call)
			}
		case "createAcl", "deleteAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31185" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-list-acl-user" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
			if resource := stringArgForCLITest(t, call, "-r"); resource != "Topic:goadmin-m6-list-acl-topic" {
				t.Fatalf("expected derived resource to be copied, got %q in %#v", resource, call)
			}
			if call[0] == "createAcl" {
				if action := stringArgForCLITest(t, call, "-a"); action != "Pub" {
					t.Fatalf("expected listAcl baseline action Pub, got %q in %#v", action, call)
				}
				if decision := stringArgForCLITest(t, call, "-d"); decision != "Allow" {
					t.Fatalf("expected listAcl baseline decision Allow, got %q in %#v", decision, call)
				}
			}
		case "listAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31185" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-list-acl-user" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
		default:
			t.Fatalf("unexpected command in listAcl hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected listAcl summary: %#v", summary)
	}
}

func TestRunM6ShadowRunSeedsGetsAndDeletesGetAclTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"get-acl","args":["getAcl","-b","127.0.0.1:31186","-s","User:goadmin-m6-get-acl-user"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31186 success.\n",
			"createUser": "create user to 127.0.0.1:31186 success.\n",
			"createAcl":  "create acl to 127.0.0.1:31186 success.\n",
			"getAcl":     "Subject: User:goadmin-m6-get-acl-user\nResource: Topic:goadmin-m6-get-acl-topic\n",
			"deleteAcl":  "delete acl to 127.0.0.1:31186 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "createUser", "createAcl", "getAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "getAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "getAcl", "deleteAcl", "deleteUser",
		"deleteUser", "createUser", "createAcl", "getAcl", "deleteAcl", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected getAcl user and baseline seed plus ACL/user cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31186" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-get-acl-user" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
		case "createUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31186" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-get-acl-user" {
				t.Fatalf("expected username to be copied from subject, got %q in %#v", username, call)
			}
			if password := stringArgForCLITest(t, call, "-p"); password != "m6-shadow-acl-pass" {
				t.Fatalf("expected deterministic seed password, got %q in %#v", password, call)
			}
			if userType := stringArgForCLITest(t, call, "-t"); userType != "Super" {
				t.Fatalf("expected super user type for ACL seed user, got %q in %#v", userType, call)
			}
		case "createAcl", "deleteAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31186" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-get-acl-user" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
			if resource := stringArgForCLITest(t, call, "-r"); resource != "Topic:goadmin-m6-get-acl-topic" {
				t.Fatalf("expected derived resource to be copied, got %q in %#v", resource, call)
			}
			if call[0] == "createAcl" {
				if action := stringArgForCLITest(t, call, "-a"); action != "Pub" {
					t.Fatalf("expected getAcl baseline action Pub, got %q in %#v", action, call)
				}
				if decision := stringArgForCLITest(t, call, "-d"); decision != "Allow" {
					t.Fatalf("expected getAcl baseline decision Allow, got %q in %#v", decision, call)
				}
			}
		case "getAcl":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31186" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if subject := stringArgForCLITest(t, call, "-s"); subject != "User:goadmin-m6-get-acl-user" {
				t.Fatalf("expected subject to be copied, got %q in %#v", subject, call)
			}
		default:
			t.Fatalf("unexpected command in getAcl hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected getAcl summary: %#v", summary)
	}
}

func TestRunM6ShadowRunDeletesCopyUserTargetAroundEachProvider(t *testing.T) {
	fixtures := `{"samples":[{"name":"copy-user","args":["copyUser","-f","127.0.0.1:31072","-t","127.0.0.1:31082","-u","goadmin-m6-copy-user"]}]}`
	runner := &mappedRecordingRunner{
		outputsByCommand: map[string]string{
			"deleteUser": "delete user to 127.0.0.1:31082 success.\n",
			"copyUser":   "copy user of goadmin-m6-copy-user from 127.0.0.1:31072 to 127.0.0.1:31082 success.\n",
		},
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-fixtures", fixtures})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d stderr=%q stdout=%q", code, stderr.String(), stdout.String())
	}
	commands := runner.commandNames()
	expected := []string{
		"deleteUser", "copyUser", "deleteUser",
		"deleteUser", "copyUser", "deleteUser",
		"deleteUser", "copyUser", "deleteUser",
		"deleteUser", "copyUser", "deleteUser",
	}
	if !reflect.DeepEqual(commands, expected) {
		t.Fatalf("expected copyUser target cleanup around every provider\nexpected=%#v\nactual=%#v\ncalls=%#v", expected, commands, runner.commands())
	}
	for _, call := range runner.commands() {
		switch call[0] {
		case "deleteUser":
			if broker := stringArgForCLITest(t, call, "-b"); broker != "127.0.0.1:31082" {
				t.Fatalf("expected target broker to be copied, got %q in %#v", broker, call)
			}
			if username := stringArgForCLITest(t, call, "-u"); username != "goadmin-m6-copy-user" {
				t.Fatalf("expected username to be copied, got %q in %#v", username, call)
			}
		case "copyUser":
			if source := stringArgForCLITest(t, call, "-f"); source != "127.0.0.1:31072" {
				t.Fatalf("expected source broker to be preserved, got %q in %#v", source, call)
			}
			if target := stringArgForCLITest(t, call, "-t"); target != "127.0.0.1:31082" {
				t.Fatalf("expected target broker to be preserved, got %q in %#v", target, call)
			}
		default:
			t.Fatalf("unexpected command in copyUser hook run: %#v", call)
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var summary nativeadmin.ShadowBatchSummaryReport
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatalf("expected summary JSON line, got %v", err)
	}
	if summary.Executed != 1 || summary.Skipped != 95 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected copyUser summary: %#v", summary)
	}
}

func TestRunM6ShadowCommandCapturesExportConfigsArtifact(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "export")
	runner := &recordingRunner{
		output: "export " + filepath.ToSlash(filepath.Join(outputDir, "configs.json")) + " success",
		onCall: func(callCount int) {
			if err := os.MkdirAll(outputDir, 0o755); err != nil {
				t.Fatalf("mkdir export dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(outputDir, "configs.json"), []byte(`{"cluster":"DefaultCluster"}`), 0o644); err != nil {
				t.Fatalf("write export configs artifact: %v", err)
			}
		},
	}

	output, err := runM6ShadowCommand(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
	}, []string{"exportConfigs", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-f", outputDir})

	if err != nil {
		t.Fatalf("expected exportConfigs shadow command to pass, got %v", err)
	}
	if output.Stdout != runner.output {
		t.Fatalf("stdout mismatch: %#v", output)
	}
	if output.Artifacts["configs.json"] != `{"cluster":"DefaultCluster"}` {
		t.Fatalf("expected configs.json artifact to be captured, got %#v", output.Artifacts)
	}
}

func TestRunM6ShadowCommandCapturesExportMetadataArtifact(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "export")
	runner := &recordingRunner{
		output: "export " + filepath.ToSlash(filepath.Join(outputDir, "metadata.json")) + " success\n",
		onCall: func(callCount int) {
			if err := os.MkdirAll(outputDir, 0o755); err != nil {
				t.Fatalf("mkdir export dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(outputDir, "metadata.json"), []byte(`{"exportTime":1782480000123}`), 0o644); err != nil {
				t.Fatalf("write export metadata artifact: %v", err)
			}
		},
	}

	output, err := runM6ShadowCommand(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
	}, []string{"exportMetadata", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-f", outputDir})

	if err != nil {
		t.Fatalf("expected exportMetadata shadow command to pass, got %v", err)
	}
	if output.Artifacts["metadata.json"] != `{"exportTime":1782480000123}` {
		t.Fatalf("expected metadata.json artifact to be captured, got %#v", output.Artifacts)
	}
}

func TestRunM6ShadowCommandCapturesExportMetricsArtifact(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "export")
	runner := &recordingRunner{
		output: "export " + filepath.ToSlash(filepath.Join(outputDir, "metrics.json")) + " success\n",
		onCall: func(callCount int) {
			if err := os.MkdirAll(outputDir, 0o755); err != nil {
				t.Fatalf("mkdir export dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(outputDir, "metrics.json"), []byte(`{"totalData":{}}`), 0o644); err != nil {
				t.Fatalf("write export metrics artifact: %v", err)
			}
		},
	}

	output, err := runM6ShadowCommand(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
	}, []string{"exportMetrics", "-n", "127.0.0.1:9876", "-c", "DefaultCluster", "-f", outputDir})

	if err != nil {
		t.Fatalf("expected exportMetrics shadow command to pass, got %v", err)
	}
	if output.Artifacts["metrics.json"] != `{"totalData":{}}` {
		t.Fatalf("expected metrics.json artifact to be captured, got %#v", output.Artifacts)
	}
}

func TestRunM6ShadowCommandTreatsGetBrokerEpochControllerModeErrorAsOfficialStderr(t *testing.T) {
	officialStderr := nativeadmin.OfficialGetBrokerEpochControllerModeStderr("this request only for controllerMode ")
	runner := &recordingRunner{
		err: errors.New("mqadmin 命令输出异常: org.apache.rocketmq.tools.command.SubCommandException: GetBrokerEpochSubCommand command failed"),
	}

	output, err := runM6ShadowCommand(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
	}, []string{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"})
	if err != nil {
		t.Fatalf("expected M6 shadow to preserve official zero-exit stderr, got %v", err)
	}
	if output.Stdout != "" {
		t.Fatalf("expected empty stdout, got %q", output.Stdout)
	}
	if output.Stderr != officialStderr {
		t.Fatalf("unexpected stderr\nwant=%q\n got=%q", officialStderr, output.Stderr)
	}
}

func TestRunM6ShadowCommandMapsQueryMsgTraceByIdNoMessageOfficialResultToPrimaryError(t *testing.T) {
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		expectedArgs := []string{"queryMsgTraceById", "-n", "127.0.0.1:9876", "-i", "MISSING-TRACE", "-t", "RMQ_SYS_TRACE_TOPIC", "-b", "0", "-e", "9223372036854775807"}
		if !reflect.DeepEqual(args, expectedArgs) {
			t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expectedArgs, args)
		}
		return "", true, &nativeadmin.OfficialCommandResult{
			ExitCode: 0,
			Stderr:   queryMsgTraceByIDNoMessageStderrForM6ShadowTest(),
		}
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()

	output, err := runM6ShadowCommand(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "native",
	}, []string{"queryMsgTraceById", "-n", "127.0.0.1:9876", "-i", "MISSING-TRACE", "-t", "RMQ_SYS_TRACE_TOPIC", "-b", "0", "-e", "9223372036854775807"})

	expectedError := "mqadmin 命令输出异常: org.apache.rocketmq.tools.command.SubCommandException: QueryMsgTraceByIdSubCommandcommand failed"
	if err == nil || err.Error() != expectedError {
		t.Fatalf("expected primary-compatible error %q, got %T %v", expectedError, err, err)
	}
	if output.Stdout != "" || output.Stderr != "" {
		t.Fatalf("expected empty comparable streams, got stdout=%q stderr=%q", output.Stdout, output.Stderr)
	}
}

func queryMsgTraceByIDNoMessageStderrForM6ShadowTest() string {
	return "org.apache.rocketmq.tools.command.SubCommandException: QueryMsgTraceByIdSubCommandcommand failed\n" +
		"\tat org.apache.rocketmq.tools.command.message.QueryMsgTraceByIdSubCommand.execute(QueryMsgTraceByIdSubCommand.java:110)\n" +
		"Caused by: org.apache.rocketmq.client.exception.MQClientException: CODE: 208  DESC: query message by key finished, but no message.\n"
}

func TestRunM6ShadowRunRejectsPlaceholderOnlyPlan(t *testing.T) {
	runner := &recordingRunner{err: errors.New("placeholder plan must not call runner")}
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &bytes.Buffer{},
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run"})

	if code == 0 {
		t.Fatalf("expected placeholder-only shadow run to fail")
	}
	if !strings.Contains(stderr.String(), "no executable M6 shadow samples") {
		t.Fatalf("expected no executable samples error, got %q", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected runner to stay unused, got %#v", runner.calls)
	}
}

func TestRunM6ShadowRunRejectsInvalidConcurrency(t *testing.T) {
	runner := &recordingRunner{err: errors.New("invalid concurrency must not call runner")}
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &bytes.Buffer{},
		Stderr:     &stderr,
	}, []string{"--m6-shadow-run", "--m6-shadow-concurrency", "0"})

	if code == 0 {
		t.Fatalf("expected invalid concurrency to fail")
	}
	if !strings.Contains(stderr.String(), "invalid --m6-shadow-concurrency") {
		t.Fatalf("expected concurrency parse error, got %q", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected runner to stay unused, got %#v", runner.calls)
	}
}

func TestParseM6ShadowRunFlagsParsesConcurrency(t *testing.T) {
	options := Options{}

	fixturesJSON, fixturesFile, concurrency, err := parseM6ShadowRunFlags(&options, []string{
		"--m6-shadow-run",
		"--m6-shadow-concurrency", "4",
		"--transport", "sidecar",
		"--namesrv", "ns-a:9876",
		"--sidecar-addr", "http://127.0.0.1:18091",
		"--timeout-ms", "60000",
		"--m6-shadow-fixtures", `{"samples":[]}`,
	})
	if err != nil {
		t.Fatalf("expected flags to parse, got %v", err)
	}
	if fixturesJSON != `{"samples":[]}` || fixturesFile != "" {
		t.Fatalf("unexpected fixtures parse result: json=%q file=%q", fixturesJSON, fixturesFile)
	}
	if concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", concurrency)
	}
	if options.Transport != "sidecar" || options.NameServer != "ns-a:9876" || options.SidecarAddr != "http://127.0.0.1:18091" {
		t.Fatalf("unexpected parsed options: %#v", options)
	}
	if options.Timeout != 60*time.Second || options.SidecarTimeout != 60*time.Second {
		t.Fatalf("expected timeout override to propagate, got timeout=%s sidecarTimeout=%s", options.Timeout, options.SidecarTimeout)
	}
}

func TestRunInjectsNameServerAfterSubCommand(t *testing.T) {
	runner := &recordingRunner{output: "cluster\n"}
	var stdout bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"clusterList"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	expected := [][]string{{"clusterList", "-n", "127.0.0.1:9876"}}
	if !reflect.DeepEqual(runner.calls, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, runner.calls)
	}
}

func TestRunKeepsExplicitNameServer(t *testing.T) {
	runner := &recordingRunner{output: "topic\n"}

	code := Run(context.Background(), Options{
		NameServer: "default:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &bytes.Buffer{},
	}, []string{"topicList", "-n", "custom:9876"})

	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	expected := [][]string{{"topicList", "-n", "custom:9876"}}
	if !reflect.DeepEqual(runner.calls, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, runner.calls)
	}
}

func TestRunDoesNotInjectNameServerForRocksDBConfigToJsonBrokerMode(t *testing.T) {
	runner := &recordingRunner{output: "broker export done."}

	code := Run(context.Background(), Options{
		NameServer: "default:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &bytes.Buffer{},
	}, []string{"rocksDBConfigToJson", "-b", "127.0.0.1:10911", "-t", "topics"})

	if code != 0 {
		t.Fatalf("expected zero exit code")
	}
	expected := [][]string{{"rocksDBConfigToJson", "-b", "127.0.0.1:10911", "-t", "topics"}}
	if !reflect.DeepEqual(runner.calls, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, runner.calls)
	}
}

func TestRunDoesNotInjectNameServerForRocksDBConfigToJsonLocalMode(t *testing.T) {
	runner := &recordingRunner{output: "Use [local mode] load rocksdb to print or export file \n"}

	code := Run(context.Background(), Options{
		NameServer: "default:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &bytes.Buffer{},
	}, []string{"rocksDBConfigToJson", "-p", "/tmp/metadata", "-t", "topics"})

	if code != 0 {
		t.Fatalf("expected zero exit code")
	}
	expected := [][]string{{"rocksDBConfigToJson", "-p", "/tmp/metadata", "-t", "topics"}}
	if !reflect.DeepEqual(runner.calls, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, runner.calls)
	}
}

func TestRunDoesNotInjectNameServerForBrokerContainerCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "addBroker",
			args: []string{"addBroker", "-c", "127.0.0.1:30911", "-b", "/tmp/broker.conf"},
		},
		{
			name: "removeBroker",
			args: []string{"removeBroker", "-c", "127.0.0.1:30911", "-b", "DefaultCluster:broker-a:0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &recordingRunner{output: "ok\n"}
			code := Run(context.Background(), Options{
				NameServer: "default:9876",
				Transport:  "process",
				Runner:     runner,
				Stdout:     &bytes.Buffer{},
			}, tt.args)
			if code != 0 {
				t.Fatalf("expected zero exit code")
			}
			expected := [][]string{tt.args}
			if !reflect.DeepEqual(runner.calls, expected) {
				t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, runner.calls)
			}
		})
	}
}

func TestRunDoesNotInjectNameServerForOfficialNullNameServerTopicCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "topicRoute",
			args: []string{"topicRoute", "-t", "TopicTest"},
		},
		{
			name: "topicClusterList",
			args: []string{"topicClusterList", "-t", "TopicTest"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &recordingRunner{output: "official stderr shape is checked in native tests"}
			code := Run(context.Background(), Options{
				NameServer: "default:9876",
				Transport:  "process",
				Runner:     runner,
				Stdout:     &bytes.Buffer{},
			}, tt.args)
			if code != 0 {
				t.Fatalf("expected zero exit code")
			}
			expected := [][]string{tt.args}
			if !reflect.DeepEqual(runner.calls, expected) {
				t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, runner.calls)
			}
		})
	}
}

func TestNormalizeOptionsUsesCommandTimeoutForSidecarByDefault(t *testing.T) {
	options := normalizeOptions(Options{
		Transport: "sidecar",
		Timeout:   42 * time.Second,
	})

	if options.SidecarTimeout != 42*time.Second {
		t.Fatalf("expected sidecar timeout to follow command timeout, got %s", options.SidecarTimeout)
	}
}

func TestNormalizeOptionsKeepsExplicitSidecarTimeout(t *testing.T) {
	options := normalizeOptions(Options{
		Transport:      "sidecar",
		Timeout:        42 * time.Second,
		SidecarTimeout: 5 * time.Second,
	})

	if options.SidecarTimeout != 5*time.Second {
		t.Fatalf("expected explicit sidecar timeout, got %s", options.SidecarTimeout)
	}
}

func TestOptionsFromConfigUsesRequestTimeoutForImplicitSidecarTimeout(t *testing.T) {
	original, hadOriginal := os.LookupEnv("RMQD_ADMIN_SIDECAR_TIMEOUT_MS")
	if err := os.Unsetenv("RMQD_ADMIN_SIDECAR_TIMEOUT_MS"); err != nil {
		t.Fatalf("unset sidecar timeout env: %v", err)
	}
	defer func() {
		if hadOriginal {
			if err := os.Setenv("RMQD_ADMIN_SIDECAR_TIMEOUT_MS", original); err != nil {
				t.Fatalf("restore sidecar timeout env: %v", err)
			}
			return
		}
		if err := os.Unsetenv("RMQD_ADMIN_SIDECAR_TIMEOUT_MS"); err != nil {
			t.Fatalf("restore empty sidecar timeout env: %v", err)
		}
	}()

	options := OptionsFromConfig(config.Config{
		RequestTimeout:      60 * time.Second,
		AdminSidecarTimeout: 3 * time.Second,
	})

	if options.SidecarTimeout != 60*time.Second {
		t.Fatalf("expected implicit sidecar timeout to follow request timeout, got %s", options.SidecarTimeout)
	}
}

func TestOptionsFromConfigKeepsExplicitSidecarTimeoutEnv(t *testing.T) {
	t.Setenv("RMQD_ADMIN_SIDECAR_TIMEOUT_MS", "5000")

	options := OptionsFromConfig(config.Config{
		RequestTimeout:      60 * time.Second,
		AdminSidecarTimeout: 5 * time.Second,
	})

	if options.SidecarTimeout != 5*time.Second {
		t.Fatalf("expected explicit sidecar timeout from config, got %s", options.SidecarTimeout)
	}
}

func TestParseGlobalFlagsTimeoutOverridesSidecarTimeout(t *testing.T) {
	options := Options{
		SidecarTimeout: 3 * time.Second,
		Stderr:         &bytes.Buffer{},
	}

	args, exitCode, handled := parseGlobalFlags(&options, []string{"--timeout-ms", "60000", "topicList"})
	if handled || exitCode != 0 {
		t.Fatalf("expected command args to continue, handled=%v exit=%d", handled, exitCode)
	}
	if !reflect.DeepEqual(args, []string{"topicList"}) {
		t.Fatalf("unexpected args %#v", args)
	}
	if options.Timeout != 60*time.Second {
		t.Fatalf("expected command timeout from flag, got %s", options.Timeout)
	}
	if options.SidecarTimeout != 60*time.Second {
		t.Fatalf("expected --timeout-ms to override sidecar timeout, got %s", options.SidecarTimeout)
	}
}

func TestRunReportsCommandErrorsToStderr(t *testing.T) {
	runner := &recordingRunner{err: errors.New("boom")}
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stderr:     &stderr,
	}, []string{"topicList"})

	if code == 0 {
		t.Fatalf("expected non-zero exit code")
	}
	if !strings.Contains(stderr.String(), "boom") {
		t.Fatalf("expected stderr to contain command error, got %q", stderr.String())
	}
}

func TestRunClusterListIntervalRepeatsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &recordingRunner{
		outputs: []string{"first\n", "second\n"},
		onCall: func(callCount int) {
			if callCount == 2 {
				cancel()
			}
		},
	}
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"clusterList", "-i", "0", "-c", "DefaultCluster"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expectedCalls := [][]string{
		{"clusterList", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"},
		{"clusterList", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"},
	}
	if !reflect.DeepEqual(runner.calls, expectedCalls) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expectedCalls, runner.calls)
	}
	if stdout.String() != "first\nsecond\n" {
		t.Fatalf("stdout mismatch: %q", stdout.String())
	}
}

func TestRunClusterRTIntervalRepeatsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	nativeCalls := [][]string{}
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		nativeCalls = append(nativeCalls, append([]string(nil), args...))
		if len(nativeCalls) == 2 {
			cancel()
		}
		if len(nativeCalls) == 1 {
			return "first\n", true, nil
		}
		return "second\n", true, nil
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	runner := &recordingRunner{
		err: errors.New("clusterRT interval must not call buffered official runner"),
	}
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "sidecar",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"clusterRT", "-i", "0", "-a", "2", "-s", "16", "-c", "DefaultCluster"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expectedCalls := [][]string{
		{"clusterRT", "-n", "127.0.0.1:9876", "-a", "2", "-s", "16", "-c", "DefaultCluster"},
		{"clusterRT", "-n", "127.0.0.1:9876", "-a", "2", "-s", "16", "-c", "DefaultCluster"},
	}
	if !reflect.DeepEqual(nativeCalls, expectedCalls) {
		t.Fatalf("native args mismatch\nexpected=%#v\nactual=%#v", expectedCalls, nativeCalls)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected buffered official runner to stay unused, got %#v", runner.calls)
	}
	if stdout.String() != "first\nsecond\n" {
		t.Fatalf("stdout mismatch: %q", stdout.String())
	}
}

func TestRunClusterRTProcessIntervalUsesNativeSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	nativeCalls := [][]string{}
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		nativeCalls = append(nativeCalls, append([]string(nil), args...))
		cancel()
		return "snapshot\n", true, nil
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	runner := &recordingRunner{
		err: errors.New("clusterRT process interval must not call infinite official runner"),
	}
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"clusterRT", "-a", "2", "-s", "16", "-c", "DefaultCluster"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expectedCalls := [][]string{
		{"clusterRT", "-n", "127.0.0.1:9876", "-a", "2", "-s", "16", "-c", "DefaultCluster"},
	}
	if !reflect.DeepEqual(nativeCalls, expectedCalls) {
		t.Fatalf("native args mismatch\nexpected=%#v\nactual=%#v", expectedCalls, nativeCalls)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected process runner to stay unused, got %#v", runner.calls)
	}
	if stdout.String() != "snapshot\n" {
		t.Fatalf("stdout mismatch: %q", stdout.String())
	}
}

func TestRunClusterRTIntervalPrintsTableHeaderOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	header := "#Cluster Name             #Broker Name              #RT   #successCount  #failCount\n"
	originalNativeRunner := nativeCommandRunner
	callCount := 0
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		if strings.Contains(strings.Join(args, " "), "-i") {
			t.Fatalf("expected interval flag stripped, got %#v", args)
		}
		callCount++
		if callCount == 2 {
			cancel()
			return header + "DefaultCluster            broker-a                  2.00      2                 0\n", true, nil
		}
		return header + "DefaultCluster            broker-a                  1.00      2                 0\n", true, nil
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner: &recordingRunner{
			err: errors.New("clusterRT interval must not call buffered official runner"),
		},
		Stdout: &stdout,
	}, []string{"clusterRT", "-i", "0"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	if count := strings.Count(stdout.String(), "#Cluster Name"); count != 1 {
		t.Fatalf("expected clusterRT table header once, count=%d stdout=%q", count, stdout.String())
	}
	if !strings.Contains(stdout.String(), "1.00") || !strings.Contains(stdout.String(), "2.00") {
		t.Fatalf("expected both clusterRT rows, got %q", stdout.String())
	}
}

func TestRunClusterRTIntervalKeepsTlogRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	originalNativeRunner := nativeCommandRunner
	callCount := 0
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		callCount++
		if callCount == 2 {
			cancel()
			return "2026-06-17 12:00:01|room-a|DefaultCluster|broker-a|2\n", true, nil
		}
		return "2026-06-17 12:00:00|room-a|DefaultCluster|broker-a|1\n", true, nil
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "sidecar",
		Runner: &recordingRunner{
			err: errors.New("clusterRT interval must not call buffered official runner"),
		},
		Stdout: &stdout,
	}, []string{"clusterRT", "-i", "0", "-p", "true", "-m", "room-a"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expected := "2026-06-17 12:00:00|room-a|DefaultCluster|broker-a|1\n" +
		"2026-06-17 12:00:01|room-a|DefaultCluster|broker-a|2\n"
	if stdout.String() != expected {
		t.Fatalf("stdout mismatch\nexpected=%q\nactual=%q", expected, stdout.String())
	}
}

func TestRunClusterRTIntervalPropagatesNativeUnsupported(t *testing.T) {
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		return "", false, nil
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "sidecar",
		Runner: &recordingRunner{
			output: "official output should not be used\n",
		},
		Stdout: &bytes.Buffer{},
		Stderr: &stderr,
	}, []string{"clusterRT", "-i", "0"})

	if code == 0 {
		t.Fatalf("expected clusterRT interval to fail when native snapshot is unavailable")
	}
	if !strings.Contains(stderr.String(), "native transport does not support") {
		t.Fatalf("expected native unsupported error, got %q", stderr.String())
	}
}

func TestRunClusterRTIntervalRejectsNativeSnapshotError(t *testing.T) {
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		return "", true, errors.New("snapshot failed")
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner: &recordingRunner{
			output: "official output should not be used\n",
		},
		Stdout: &bytes.Buffer{},
		Stderr: &stderr,
	}, []string{"clusterRT", "-i", "0"})

	if code == 0 {
		t.Fatalf("expected clusterRT interval to fail when native snapshot fails")
	}
	if !strings.Contains(stderr.String(), "snapshot failed") {
		t.Fatalf("expected native snapshot error, got %q", stderr.String())
	}
}

func TestRunGetBrokerEpochIntervalRepeatsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &recordingRunner{
		outputs: []string{"epoch-one\n", "epoch-two\n"},
		onCall: func(callCount int) {
			if callCount == 2 {
				cancel()
			}
		},
	}
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"getBrokerEpoch", "-b", "broker-a", "-i", "0"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expectedCalls := [][]string{
		{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-b", "broker-a"},
		{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-b", "broker-a"},
	}
	if !reflect.DeepEqual(runner.calls, expectedCalls) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expectedCalls, runner.calls)
	}
	if stdout.String() != "epoch-one\nepoch-two\n" {
		t.Fatalf("stdout mismatch: %q", stdout.String())
	}
}

func TestIntervalCommandGetBrokerEpochEmptyIntervalUsesOfficialDefault(t *testing.T) {
	interval, stripped, intervalMode, err := intervalCommand([]string{
		"getBrokerEpoch",
		"-n", "127.0.0.1:9876",
		"-c", "GoadminEpochIntervalCluster",
		"--interval=",
	})

	if err != nil {
		t.Fatalf("expected empty interval to use official default, got error %v", err)
	}
	if !intervalMode {
		t.Fatalf("expected getBrokerEpoch --interval= to enter interval mode")
	}
	if interval != 3*time.Second {
		t.Fatalf("expected official getBrokerEpoch default interval 3s, got %v", interval)
	}
	expected := []string{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-c", "GoadminEpochIntervalCluster"}
	if !reflect.DeepEqual(stripped, expected) {
		t.Fatalf("stripped args mismatch\nexpected=%#v\nactual=%#v", expected, stripped)
	}
}

func TestRunGetSyncStateSetIntervalRepeatsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &recordingRunner{
		outputs: []string{"sync-one\n", "sync-two\n"},
		onCall: func(callCount int) {
			if callCount == 2 {
				cancel()
			}
		},
	}
	var stdout bytes.Buffer

	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "process",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"getSyncStateSet", "-a", "127.0.0.1:9878", "-c", "GoadminSyncIntervalCluster", "-i", "0"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expectedCalls := [][]string{
		{"getSyncStateSet", "-n", "127.0.0.1:9876", "-a", "127.0.0.1:9878", "-c", "GoadminSyncIntervalCluster"},
		{"getSyncStateSet", "-n", "127.0.0.1:9876", "-a", "127.0.0.1:9878", "-c", "GoadminSyncIntervalCluster"},
	}
	if !reflect.DeepEqual(runner.calls, expectedCalls) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expectedCalls, runner.calls)
	}
	if stdout.String() != "sync-one\nsync-two\n" {
		t.Fatalf("stdout mismatch: %q", stdout.String())
	}
}

func TestRunHAStatusIntervalRepeatsUntilContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &recordingRunner{
		outputs: []string{"ha-one\n", "ha-two\n"},
		onCall: func(callCount int) {
			if callCount == 2 {
				cancel()
			}
		},
	}
	var stdout bytes.Buffer
	code := Run(ctx, Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "native",
		Runner:     runner,
		Stdout:     &stdout,
	}, []string{"haStatus", "-c", "DefaultCluster", "-i", "0"})

	if code != 0 {
		t.Fatalf("expected exit 0 after context cancellation, got %d", code)
	}
	expectedCalls := [][]string{
		{"haStatus", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"},
		{"haStatus", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"},
	}
	if !reflect.DeepEqual(runner.calls, expectedCalls) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expectedCalls, runner.calls)
	}
	if stdout.String() != "ha-one\nha-two\n" {
		t.Fatalf("stdout mismatch: %q", stdout.String())
	}
}

func TestIntervalCommandHAStatusEmptyIntervalUsesOfficialDefault(t *testing.T) {
	interval, stripped, intervalMode, err := intervalCommand([]string{
		"haStatus",
		"-n", "127.0.0.1:9876",
		"-c", "DefaultCluster",
		"--interval=",
	})

	if err != nil {
		t.Fatalf("expected empty interval to use official default, got error %v", err)
	}
	if !intervalMode {
		t.Fatalf("expected haStatus --interval= to enter interval mode")
	}
	if interval != 3*time.Second {
		t.Fatalf("expected official haStatus default interval 3s, got %v", interval)
	}
	expected := []string{"haStatus", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"}
	if !reflect.DeepEqual(stripped, expected) {
		t.Fatalf("stripped args mismatch\nexpected=%#v\nactual=%#v", expected, stripped)
	}
}

func TestIntervalCommandGetSyncStateSetEmptyIntervalUsesOfficialDefault(t *testing.T) {
	interval, stripped, intervalMode, err := intervalCommand([]string{
		"getSyncStateSet",
		"-n", "127.0.0.1:9876",
		"-a", "127.0.0.1:9878",
		"-c", "GoadminSyncIntervalCluster",
		"--interval=",
	})

	if err != nil {
		t.Fatalf("expected empty interval to use official default, got error %v", err)
	}
	if !intervalMode {
		t.Fatalf("expected getSyncStateSet --interval= to enter interval mode")
	}
	if interval != 3*time.Second {
		t.Fatalf("expected official getSyncStateSet default interval 3s, got %v", interval)
	}
	expected := []string{"getSyncStateSet", "-n", "127.0.0.1:9876", "-a", "127.0.0.1:9878", "-c", "GoadminSyncIntervalCluster"}
	if !reflect.DeepEqual(stripped, expected) {
		t.Fatalf("stripped args mismatch\nexpected=%#v\nactual=%#v", expected, stripped)
	}
}

func TestRunStartMonitoringWaitsAfterSilentRunnerReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runnerCalled := make(chan struct{})
	done := make(chan int, 1)
	runner := &recordingRunner{
		onCall: func(callCount int) {
			if callCount == 1 {
				close(runnerCalled)
			}
		},
	}
	var stdout bytes.Buffer

	go func() {
		done <- Run(ctx, Options{
			NameServer: "127.0.0.1:9876",
			Transport:  "process",
			Runner:     runner,
			Stdout:     &stdout,
			Stderr:     &bytes.Buffer{},
		}, []string{"startMonitoring"})
	}()

	select {
	case <-runnerCalled:
	case <-time.After(time.Second):
		t.Fatalf("expected startMonitoring runner to be called")
	}
	select {
	case code := <-done:
		t.Fatalf("startMonitoring returned before context cancel, code=%d", code)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("expected exit 0 after context cancellation, got %d", code)
		}
	case <-time.After(time.Second):
		t.Fatalf("startMonitoring did not return after context cancel")
	}
	if stdout.String() != "" {
		t.Fatalf("expected startMonitoring stdout to stay empty, got %q", stdout.String())
	}
}

func TestRunNativeTransportRejectsUnsupportedCommand(t *testing.T) {
	var stderr bytes.Buffer

	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "native",
		Stderr:     &stderr,
		Stdout:     &bytes.Buffer{},
	}, []string{"unsupportedNativeCommand", "-t", "TopicA"})

	if code == 0 {
		t.Fatalf("expected native unsupported command to fail")
	}
	if !strings.Contains(stderr.String(), "does not support") {
		t.Fatalf("expected unsupported command error, got %q", stderr.String())
	}
}

func TestRunNativeTransportPrintsOfficialParserStreams(t *testing.T) {
	for _, transport := range []string{"native", "auto", "sidecar", "process"} {
		t.Run(transport, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			runner := &recordingRunner{err: errors.New("parser preflight should run before transport")}

			code := Run(context.Background(), Options{
				NameServer:  "ns-a:9876",
				Transport:   transport,
				Runner:      runner,
				SidecarAddr: "http://127.0.0.1:18091",
				Stdout:      &stdout,
				Stderr:      &stderr,
			}, []string{"cloneGroupOffset", "-s", "src-group", "-d", "dest-group", "-t", "TopicTest", "-o"})

			if code == 0 {
				t.Fatalf("expected parser error to fail")
			}
			if !strings.HasPrefix(stdout.String(), "usage: mqadmin cloneGroupOffset") {
				t.Fatalf("expected official usage on stdout, got %q", stdout.String())
			}
			if stderr.String() != "Missing argument for option: o\n" {
				t.Fatalf("expected raw official parser stderr, got %q", stderr.String())
			}
			if len(runner.calls) != 0 {
				t.Fatalf("expected parser preflight to avoid runner calls, got %#v", runner.calls)
			}
		})
	}
}

func TestRunNativeTransportPrintsOfficialSuccessStderr(t *testing.T) {
	officialStderr := nativeadmin.OfficialGetBrokerEpochControllerModeStderr("this request only for controllerMode ")
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		return "", true, &nativeadmin.OfficialCommandResult{
			ExitCode: 0,
			Stdout:   "",
			Stderr:   officialStderr,
		}
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "native",
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"})

	if code != 0 {
		t.Fatalf("expected official-compatible exit 0, got %d stderr=%q", code, stderr.String())
	}
	if stdout.String() != "" {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
	if stderr.String() != officialStderr {
		t.Fatalf("unexpected stderr\nwant=%q\n got=%q", officialStderr, stderr.String())
	}
	if strings.Contains(stderr.String(), "goadmin:") {
		t.Fatalf("official stderr must not receive goadmin prefix: %q", stderr.String())
	}
}

func TestRunAutoTransportDoesNotFallbackAfterOfficialSuccessStderr(t *testing.T) {
	officialStderr := nativeadmin.OfficialGetBrokerEpochControllerModeStderr("this request only for controllerMode ")
	originalNativeRunner := nativeCommandRunner
	nativeCommandRunner = func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		return "", true, &nativeadmin.OfficialCommandResult{
			ExitCode: 0,
			Stderr:   officialStderr,
		}
	}
	defer func() {
		nativeCommandRunner = originalNativeRunner
	}()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(context.Background(), Options{
		NameServer: "127.0.0.1:9876",
		Transport:  "auto",
		Stdout:     &stdout,
		Stderr:     &stderr,
	}, []string{"getBrokerEpoch", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"})

	if code != 0 {
		t.Fatalf("expected auto to preserve official-compatible exit 0, got %d stderr=%q", code, stderr.String())
	}
	if stdout.String() != "" || stderr.String() != officialStderr {
		t.Fatalf("unexpected streams stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunSidecarTransportPreservesOfficialNullNameServerTopicRoute(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(context.Background(), Options{
		Transport:      "sidecar",
		SidecarAddr:    "http://127.0.0.1:1",
		SidecarTimeout: time.Millisecond,
		Stdout:         &stdout,
		Stderr:         &stderr,
	}, []string{"topicRoute", "-t", "TopicTest"})

	if code != 0 {
		t.Fatalf("expected official-compatible exit 0, got %d stderr=%q", code, stderr.String())
	}
	if stdout.String() != "" {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "TopicRouteSubCommand command failed") || !strings.Contains(stderr.String(), "connect to null failed") {
		t.Fatalf("expected official connect-null stderr, got %q", stderr.String())
	}
	if strings.Contains(stderr.String(), "goadmin:") {
		t.Fatalf("official stderr must not receive goadmin prefix: %q", stderr.String())
	}
}

type recordingRunner struct {
	output  string
	outputs []string
	err     error
	mutex   sync.Mutex
	calls   [][]string
	onCall  func(callCount int)
}

type timedShadowRecordingRunner struct {
	delayByCommand map[string]time.Duration
	mutex          sync.Mutex
	active         int
	maxActive      int
	calls          [][]string
}

type mappedRecordingRunner struct {
	outputsByCommand map[string]string
	errByCommand     map[string]error
	mutex            sync.Mutex
	calls            [][]string
}

func (r *recordingRunner) Run(ctx context.Context, args ...string) (string, error) {
	r.mutex.Lock()
	r.calls = append(r.calls, append([]string(nil), args...))
	callCount := len(r.calls)
	r.mutex.Unlock()
	if r.onCall != nil {
		r.onCall(callCount)
	}
	if len(r.outputs) > 0 {
		index := callCount - 1
		if index >= len(r.outputs) {
			index = len(r.outputs) - 1
		}
		return r.outputs[index], r.err
	}
	return r.output, r.err
}

func (r *mappedRecordingRunner) Run(ctx context.Context, args ...string) (string, error) {
	r.mutex.Lock()
	r.calls = append(r.calls, append([]string(nil), args...))
	r.mutex.Unlock()
	command := ""
	if len(args) > 0 {
		command = args[0]
	}
	if r.errByCommand != nil {
		if err := r.errByCommand[command]; err != nil {
			return "", err
		}
	}
	if r.outputsByCommand != nil {
		return r.outputsByCommand[command], nil
	}
	return "", nil
}

func (r *mappedRecordingRunner) countCommand(command string) int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	count := 0
	for _, call := range r.calls {
		if len(call) > 0 && call[0] == command {
			count++
		}
	}
	return count
}

func (r *mappedRecordingRunner) firstCommand(command string) []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, call := range r.calls {
		if len(call) > 0 && call[0] == command {
			return append([]string(nil), call...)
		}
	}
	return nil
}

func (r *mappedRecordingRunner) commands() [][]string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	calls := make([][]string, len(r.calls))
	for index, call := range r.calls {
		calls[index] = append([]string(nil), call...)
	}
	return calls
}

func (r *mappedRecordingRunner) commandNames() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	commands := make([]string, 0, len(r.calls))
	for _, call := range r.calls {
		if len(call) == 0 {
			commands = append(commands, "")
			continue
		}
		commands = append(commands, call[0])
	}
	return commands
}

func (r *timedShadowRecordingRunner) Run(ctx context.Context, args ...string) (string, error) {
	r.mutex.Lock()
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.calls = append(r.calls, append([]string(nil), args...))
	r.mutex.Unlock()

	command := ""
	if len(args) > 0 {
		command = args[0]
	}
	delay := r.delayByCommand[command]
	if delay <= 0 {
		delay = 10 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		r.leave()
		return "", ctx.Err()
	case <-timer.C:
	}
	r.leave()
	return "provider-" + command + "\n", nil
}

func (r *timedShadowRecordingRunner) leave() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.active--
}

func (r *timedShadowRecordingRunner) MaxActive() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.maxActive
}

func TestParseM6ShadowRunFlagsRejectsInvalidConcurrency(t *testing.T) {
	options := Options{}
	for _, value := range []string{"0", "-1", "abc"} {
		t.Run("value_"+strconv.Quote(value), func(t *testing.T) {
			_, _, _, err := parseM6ShadowRunFlags(&options, []string{"--m6-shadow-run", "--m6-shadow-concurrency", value})
			if err == nil || !strings.Contains(err.Error(), "invalid --m6-shadow-concurrency") {
				t.Fatalf("expected invalid concurrency error for %q, got %v", value, err)
			}
		})
	}
}

func messageChainDetailOutputForTest(messageID string, topic string, queueID int, queueOffset int64) string {
	return `OffsetID:            ` + messageID + `
Topic:               ` + topic + `
Tags:                [notice]
Keys:                [user-10001]
Queue ID:            ` + strconv.Itoa(queueID) + `
Queue Offset:        ` + strconv.FormatInt(queueOffset, 10) + `
Reconsume Times:     0
Born Timestamp:      2026-06-06 19:48:01
Born Host:           10.0.0.8:51111
Store Timestamp:     2026-06-06 19:48:02
Store Host:          127.0.0.1:10911
Properties:          {MSG_REGION=DefaultRegion, UNIQ_KEY=0AE97A6A00017F3CA64A23D49A900003, TRACE_ON=true}
Message Body:        {"assessmentId":10001,"status":"created"}`
}

func messageChainTraceSuccessOutputForTest(consumerGroup string) string {
	return `#Type      #ProducerGroup       #ClientHost          #SendTime            #CostTimes #Status
Pub        PG_NOTICE            10.0.0.8             2026-06-06 19:48:01  12ms       success

#Type      #ConsumerGroup       #ClientHost          #ConsumerTime        #CostTimes #Status
Sub        ` + consumerGroup + `            10.0.0.9             2026-06-06 19:48:05  18ms       success`
}

func messageChainTopicStatusOutputForTest(brokerName string, queueID int, minOffset int64, maxOffset int64) string {
	return "#Broker Name                      #QID  #Min Offset           #Max Offset             #Last Updated\n" +
		brokerName + "                          " + strconv.Itoa(queueID) + "     " + strconv.FormatInt(minOffset, 10) + "                     " + strconv.FormatInt(maxOffset, 10) + "                  2026-06-05 16:20:48,715\n"
}

func stringArgForCLITest(t *testing.T, args []string, name string) string {
	t.Helper()
	for index, arg := range args {
		if arg == name && index+1 < len(args) {
			return args[index+1]
		}
	}
	t.Fatalf("arg %s not found in %#v", name, args)
	return ""
}
