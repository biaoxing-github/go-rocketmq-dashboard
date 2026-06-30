package goadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	if decoded.Total != 61 || decoded.Executable != 1 || decoded.Skipped != 60 {
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
	if decoded.Total != 61 || decoded.Executable != 1 || decoded.Skipped != 60 {
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
	if summary.Executed != 1 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
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
	if summary.Executed != 1 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
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
	if summary.Executed != 1 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
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
	if summary.Executed != 3 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
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
	if summary.Executed != 1 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
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
	if summary.Executed != 1 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
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
	if summary.Executed != 1 || summary.Skipped != 60 || summary.Summary.Mismatches != 0 {
		t.Fatalf("unexpected deleteKvConfig summary: %#v", summary)
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
