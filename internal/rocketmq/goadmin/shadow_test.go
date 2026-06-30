package goadmin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type shadowRunnerFunc func(context.Context, []string) (ShadowOutput, error)

func (fn shadowRunnerFunc) RunShadow(ctx context.Context, args []string) (ShadowOutput, error) {
	return fn(ctx, args)
}

func TestRunShadowCompareMatchesIdenticalTargets(t *testing.T) {
	result := RunShadowCompare(context.Background(), []string{"topicStatus", "-t", "TopicTest"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner("stdout\n", "stderr\n", nil),
	}, []ShadowTarget{{
		Name:   "native",
		Runner: fixedShadowRunner("stdout\n", "stderr\n", nil),
	}}, nil)

	if result.Command != "topicStatus" {
		t.Fatalf("expected command to be recorded, got %q", result.Command)
	}
	if len(result.Args) != 3 || result.Args[2] != "TopicTest" {
		t.Fatalf("expected args to be recorded, got %#v", result.Args)
	}
	if result.Primary.Name != "official" || result.Primary.Stdout != "stdout\n" || result.Primary.Stderr != "stderr\n" || result.Primary.Error != "" {
		t.Fatalf("unexpected primary result: %#v", result.Primary)
	}
	if len(result.Diffs) != 1 {
		t.Fatalf("expected one diff entry, got %#v", result.Diffs)
	}
	diff := result.Diffs[0]
	if diff.Target != "native" || !diff.Matched || diff.StdoutDifferent || diff.StderrDifferent || diff.ErrorDifferent {
		t.Fatalf("expected native to match exactly, got %#v", diff)
	}
}

func TestRunShadowCompareReportsMultipleTargetDiffs(t *testing.T) {
	result := RunShadowCompare(context.Background(), []string{"clusterList"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner("broker-a\n", "", nil),
	}, []ShadowTarget{
		{Name: "sidecar", Runner: fixedShadowRunner("broker-b\n", "", nil)},
		{Name: "native", Runner: fixedShadowRunner("broker-a\n", "warn\n", nil)},
	}, nil)

	if len(result.Diffs) != 2 {
		t.Fatalf("expected two diff entries, got %#v", result.Diffs)
	}
	if result.Diffs[0].Target != "sidecar" || result.Diffs[0].Matched || !result.Diffs[0].StdoutDifferent || result.Diffs[0].StderrDifferent || result.Diffs[0].ErrorDifferent {
		t.Fatalf("expected sidecar stdout diff, got %#v", result.Diffs[0])
	}
	if result.Diffs[1].Target != "native" || result.Diffs[1].Matched || result.Diffs[1].StdoutDifferent || !result.Diffs[1].StderrDifferent || result.Diffs[1].ErrorDifferent {
		t.Fatalf("expected native stderr diff, got %#v", result.Diffs[1])
	}
}

func TestRunShadowCompareReportsArtifactDiff(t *testing.T) {
	result := RunShadowCompare(context.Background(), []string{"exportConfigs", "-f", "/tmp/m6-export"}, ShadowTarget{
		Name: "official",
		Runner: shadowRunnerFunc(func(ctx context.Context, args []string) (ShadowOutput, error) {
			return ShadowOutput{
				Stdout:    "export /tmp/m6-export/configs.json success",
				Artifacts: map[string]string{"configs.json": `{"broker":"official"}`},
			}, nil
		}),
	}, []ShadowTarget{{
		Name: "native",
		Runner: shadowRunnerFunc(func(ctx context.Context, args []string) (ShadowOutput, error) {
			return ShadowOutput{
				Stdout:    "export /tmp/m6-export/configs.json success",
				Artifacts: map[string]string{"configs.json": `{"broker":"native"}`},
			}, nil
		}),
	}}, nil)

	if len(result.Diffs) != 1 || result.Diffs[0].Matched || !result.Diffs[0].ArtifactsDifferent {
		t.Fatalf("expected artifact diff to be reported, got %#v", result.Diffs)
	}
}

func TestRunShadowCompareRunsTargetsConcurrentlyAndKeepsOrder(t *testing.T) {
	targetRunner := newConcurrentBatchRunner(map[string]string{
		"clusterList": "broker-a\n",
	}, map[string]time.Duration{
		"clusterList": 50 * time.Millisecond,
	})

	result := RunShadowCompare(context.Background(), []string{"clusterList"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner("broker-a\n", "", nil),
	}, []ShadowTarget{
		{Name: "sidecar", Runner: targetRunner},
		{Name: "native", Runner: targetRunner},
		{Name: "auto", Runner: targetRunner},
	}, nil)

	if targetRunner.MaxActive() < 2 {
		t.Fatalf("expected target providers to run concurrently, max active=%d", targetRunner.MaxActive())
	}
	if len(result.Targets) != 3 || result.Targets[0].Name != "sidecar" || result.Targets[1].Name != "native" || result.Targets[2].Name != "auto" {
		t.Fatalf("expected target results to keep target order, got %#v", result.Targets)
	}
	if len(result.Diffs) != 3 || result.Diffs[0].Target != "sidecar" || result.Diffs[1].Target != "native" || result.Diffs[2].Target != "auto" {
		t.Fatalf("expected diffs to keep target order, got %#v", result.Diffs)
	}
}

func TestRunShadowCompareWithOptionsRunsTargetsSerially(t *testing.T) {
	targetRunner := newConcurrentBatchRunner(map[string]string{
		"queryMsgById": "message\n",
	}, map[string]time.Duration{
		"queryMsgById": 20 * time.Millisecond,
	})

	result := RunShadowCompareWithOptions(context.Background(), []string{"queryMsgById"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner("message\n", "", nil),
	}, []ShadowTarget{
		{Name: "sidecar", Runner: targetRunner},
		{Name: "native", Runner: targetRunner},
		{Name: "auto", Runner: targetRunner},
	}, nil, true)

	if targetRunner.MaxActive() != 1 {
		t.Fatalf("expected serial target execution, max active=%d", targetRunner.MaxActive())
	}
	if len(result.Targets) != 3 || result.Targets[0].Name != "sidecar" || result.Targets[1].Name != "native" || result.Targets[2].Name != "auto" {
		t.Fatalf("expected serial target results to keep target order, got %#v", result.Targets)
	}
	if len(result.Diffs) != 3 || !result.Diffs[0].Matched || !result.Diffs[1].Matched || !result.Diffs[2].Matched {
		t.Fatalf("expected serial target diffs to match, got %#v", result.Diffs)
	}
}

func TestRunShadowCompareRunsBeforeRunBeforeProvider(t *testing.T) {
	events := make([]string, 0, 6)
	hook := func(prefix string, name string) func(context.Context, []string) error {
		return func(ctx context.Context, args []string) error {
			events = append(events, prefix+"-"+name+"-"+args[0])
			return nil
		}
	}
	runner := func(name string) ShadowRunner {
		return shadowRunnerFunc(func(ctx context.Context, args []string) (ShadowOutput, error) {
			events = append(events, "run-"+name+"-"+args[0])
			return ShadowOutput{Stdout: "ok\n"}, nil
		})
	}

	result := RunShadowCompareWithOptions(context.Background(), []string{"wipeWritePerm", "-b", "broker-a"}, ShadowTarget{
		Name:      "official",
		Runner:    runner("official"),
		BeforeRun: hook("before", "official"),
		AfterRun:  hook("after", "official"),
	}, []ShadowTarget{{
		Name:      "native",
		Runner:    runner("native"),
		BeforeRun: hook("before", "native"),
		AfterRun:  hook("after", "native"),
	}}, nil, true)

	if len(result.Diffs) != 1 || !result.Diffs[0].Matched {
		t.Fatalf("expected target to match after before-run hook, got %#v", result.Diffs)
	}
	expected := "before-official-wipeWritePerm,run-official-wipeWritePerm,after-official-wipeWritePerm,before-native-wipeWritePerm,run-native-wipeWritePerm,after-native-wipeWritePerm"
	if strings.Join(events, ",") != expected {
		t.Fatalf("unexpected before/run order\nexpected=%s\nactual=%s", expected, strings.Join(events, ","))
	}
}

func TestRunShadowCompareNormalizerRemovesDynamicOutput(t *testing.T) {
	normalizer := func(output ShadowOutput) ShadowOutput {
		output.Stdout = strings.ReplaceAll(output.Stdout, "timestamp=111", "timestamp=<dynamic>")
		output.Stdout = strings.ReplaceAll(output.Stdout, "timestamp=222", "timestamp=<dynamic>")
		return output
	}

	result := RunShadowCompare(context.Background(), []string{"queryMsgById"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner("timestamp=111\nbody\n", "", nil),
	}, []ShadowTarget{{
		Name:   "native",
		Runner: fixedShadowRunner("timestamp=222\nbody\n", "", nil),
	}}, normalizer)

	if len(result.Diffs) != 1 || !result.Diffs[0].Matched {
		t.Fatalf("expected normalized outputs to match, got %#v", result.Diffs)
	}
}

func TestRunShadowCompareNormalizesBrokerStatusDynamicStdoutByCommand(t *testing.T) {
	officialStdout := "putTps                          : 1.0 2.0 3.0\n" +
		"runtime                         : 120 seconds\n" +
		"timerReadBehind                 : 1\n" +
		"brokerVersionDesc               : V5_3_2\n"
	nativeStdout := "putTps                          : 9.0 8.0 7.0\n" +
		"runtime                         : 121 seconds\n" +
		"timerReadBehind                 : 0\n" +
		"brokerVersionDesc               : V5_3_2\n"

	result := RunShadowCompare(context.Background(), []string{"brokerStatus", "-b", "127.0.0.1:10911"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner(officialStdout, "", nil),
	}, []ShadowTarget{{
		Name:   "native",
		Runner: fixedShadowRunner(nativeStdout, "", nil),
	}}, DefaultM6ShadowNormalizer())

	if len(result.Diffs) != 1 || !result.Diffs[0].Matched {
		t.Fatalf("expected brokerStatus dynamic fields to normalize by command, got %#v", result.Diffs)
	}
}

func TestRunShadowCompareDoesNotNormalizeBrokerStatusDynamicStdoutForOtherCommands(t *testing.T) {
	officialStdout := "putTps                          : 1.0 2.0 3.0\n"
	nativeStdout := "putTps                          : 9.0 8.0 7.0\n"

	result := RunShadowCompare(context.Background(), []string{"topicStatus", "-t", "TopicTest"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner(officialStdout, "", nil),
	}, []ShadowTarget{{
		Name:   "native",
		Runner: fixedShadowRunner(nativeStdout, "", nil),
	}}, DefaultM6ShadowNormalizer())

	if len(result.Diffs) != 1 || result.Diffs[0].Matched || !result.Diffs[0].StdoutDifferent {
		t.Fatalf("expected non-brokerStatus command to preserve stdout difference, got %#v", result.Diffs)
	}
}

func TestRunShadowCompareReportsRunnerErrorDiff(t *testing.T) {
	result := RunShadowCompare(context.Background(), []string{"consumerStatus"}, ShadowTarget{
		Name:   "official",
		Runner: fixedShadowRunner("", "", errors.New("exit status 1")),
	}, []ShadowTarget{
		{Name: "native", Runner: fixedShadowRunner("", "", nil)},
		{Name: "sidecar", Runner: fixedShadowRunner("", "", errors.New("exit status 1"))},
	}, nil)

	if len(result.Diffs) != 2 {
		t.Fatalf("expected two diff entries, got %#v", result.Diffs)
	}
	if result.Primary.Error != "exit status 1" {
		t.Fatalf("expected primary error text to be recorded, got %#v", result.Primary)
	}
	if result.Diffs[0].Target != "native" || result.Diffs[0].Matched || !result.Diffs[0].ErrorDifferent {
		t.Fatalf("expected native error diff, got %#v", result.Diffs[0])
	}
	if result.Diffs[1].Target != "sidecar" || !result.Diffs[1].Matched || result.Diffs[1].ErrorDifferent {
		t.Fatalf("expected sidecar error to match, got %#v", result.Diffs[1])
	}
}

func fixedShadowRunner(stdout string, stderr string, err error) ShadowRunner {
	return shadowRunnerFunc(func(ctx context.Context, args []string) (ShadowOutput, error) {
		return ShadowOutput{Stdout: stdout, Stderr: stderr}, err
	})
}
