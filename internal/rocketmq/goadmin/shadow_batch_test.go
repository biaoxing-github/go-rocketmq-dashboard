package goadmin

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunShadowBatchExecutesConcreteSamples(t *testing.T) {
	official := batchRecordingRunner{stdoutByCommand: map[string]string{
		"topicList":   "official topics\n",
		"clusterList": "clusters\n",
	}}
	native := batchRecordingRunner{stdoutByCommand: map[string]string{
		"topicList":   "native topics\n",
		"clusterList": "clusters\n",
	}}

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary: ShadowTarget{Name: "official", Runner: &official},
		Targets: []ShadowTarget{{Name: "native", Runner: &native}},
		Samples: []ShadowSample{
			{Name: "topic-list", Args: []string{"topicList"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
			{Name: "placeholder", Args: []string{"queryMsgById", "-i", "<message-id>"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
			{Name: "cluster-list", Args: []string{"clusterList"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
		},
	})

	if result.TotalSamples != 3 {
		t.Fatalf("expected total samples to include placeholders, got %#v", result)
	}
	if result.ExecutedSamples != 2 {
		t.Fatalf("expected two concrete samples to execute, got %#v", result)
	}
	if result.SkippedSamples != 1 {
		t.Fatalf("expected one placeholder sample to be skipped, got %#v", result)
	}
	if len(result.Results) != 2 {
		t.Fatalf("expected two shadow results, got %#v", result.Results)
	}
	if result.Results[0].Command != "topicList" || result.Results[1].Command != "clusterList" {
		t.Fatalf("unexpected command order: %#v", result.Results)
	}
	if result.Summary.Commands != 2 || result.Summary.Mismatches != 1 {
		t.Fatalf("expected summary to aggregate executed results, got %#v", result.Summary)
	}
}

func TestRunShadowBatchRecordsValidationErrorWithoutExecuting(t *testing.T) {
	official := batchRecordingRunner{stdoutByCommand: map[string]string{"topicList": "topics\n"}}

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary: ShadowTarget{Name: "official", Runner: &official},
		Samples: []ShadowSample{{
			Name:       "missing-official",
			Args:       []string{"topicList"},
			Providers:  []ShadowProviderMode{ShadowProviderNative},
			MinSamples: 1,
		}},
	})

	if len(result.Errors) != 1 {
		t.Fatalf("expected validation error, got %#v", result)
	}
	if result.ExecutedSamples != 0 || len(official.calls) != 0 {
		t.Fatalf("expected invalid batch to skip execution, result=%#v calls=%#v", result, official.calls)
	}
}

func TestPlanShadowBatchClassifiesExecutableAndSkippedSamples(t *testing.T) {
	plan := PlanShadowBatch([]ShadowSample{
		{Name: "topic-list", Args: []string{"topicList"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
		{Name: "known-message", Args: []string{"queryMsgById", "-i", "<message-id>", "-k", "<message-key>"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
		{Name: "cluster-list", Args: []string{"clusterList"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
		{Name: "recent-topic", Args: []string{"queryMsgByKey", "-t", "TopicTest", "-k", "<message-key>"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
	})

	if plan.TotalSamples != 4 || plan.ExecutableSamples != 2 || plan.SkippedSamples != 2 {
		t.Fatalf("unexpected plan counts: %#v", plan)
	}
	if len(plan.Executable) != 2 {
		t.Fatalf("expected two executable samples, got %#v", plan.Executable)
	}
	executableNames := []string{plan.Executable[0].Name, plan.Executable[1].Name}
	expectedExecutableNames := []string{"topic-list", "cluster-list"}
	if !reflect.DeepEqual(executableNames, expectedExecutableNames) {
		t.Fatalf("expected executable sample order, expected=%#v actual=%#v", expectedExecutableNames, executableNames)
	}
	if len(plan.Skipped) != 2 || plan.Skipped[0].Sample.Name != "known-message" || plan.Skipped[1].Sample.Name != "recent-topic" {
		t.Fatalf("expected placeholder skip details, got %#v", plan.Skipped)
	}
	if plan.Skipped[0].Placeholder != "<message-id>" || !strings.Contains(plan.Skipped[0].Reason, "<message-id>") {
		t.Fatalf("expected skip reason to mention first placeholder, got %#v", plan.Skipped[0])
	}
}

func TestPlanShadowBatchCopiesSamplesForCallerMutationSafety(t *testing.T) {
	samples := []ShadowSample{
		{Name: "topic-list", Args: []string{"topicList"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
		{Name: "known-message", Args: []string{"queryMsgById", "-i", "<message-id>"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
	}

	plan := PlanShadowBatch(samples)
	if len(plan.Executable) != 1 || len(plan.Skipped) != 1 {
		t.Fatalf("expected one executable and one skipped sample, got %#v", plan)
	}

	samples[0].Args[0] = "sourceMutatedTopicList"
	samples[0].Providers[0] = ShadowProviderNative
	samples[1].Args[0] = "sourceMutatedQueryMsgById"
	samples[1].Providers[0] = ShadowProviderNative

	if plan.Executable[0].Args[0] != "topicList" || plan.Executable[0].Providers[0] != ShadowProviderOfficial {
		t.Fatalf("original executable sample mutation leaked into plan: %#v", plan.Executable[0])
	}
	if plan.Skipped[0].Sample.Args[0] != "queryMsgById" || plan.Skipped[0].Sample.Providers[0] != ShadowProviderOfficial {
		t.Fatalf("original skipped sample mutation leaked into plan: %#v", plan.Skipped[0])
	}

	plan.Executable[0].Args[0] = "mutatedTopicList"
	plan.Executable[0].Providers[0] = ShadowProviderNative
	plan.Skipped[0].Sample.Args[0] = "mutatedQueryMsgById"
	plan.Skipped[0].Sample.Providers[0] = ShadowProviderNative

	if samples[0].Args[0] != "sourceMutatedTopicList" || samples[0].Providers[0] != ShadowProviderNative {
		t.Fatalf("executable sample mutation leaked into original sample: %#v", samples[0])
	}
	if samples[1].Args[0] != "sourceMutatedQueryMsgById" || samples[1].Providers[0] != ShadowProviderNative {
		t.Fatalf("skipped sample mutation leaked into original sample: %#v", samples[1])
	}
}

func TestPlanShadowBatchReportsDefaultPlanAsDryRunOnly(t *testing.T) {
	plan := PlanShadowBatch(DefaultM6ShadowPlan())

	if len(plan.Errors) != 0 {
		t.Fatalf("default plan should be structurally valid, got %#v", plan.Errors)
	}
	if plan.TotalSamples == 0 || plan.ExecutableSamples != 0 || plan.SkippedSamples != plan.TotalSamples {
		t.Fatalf("expected default plan to be entirely skipped until placeholders are filled, got %#v", plan)
	}
	if len(plan.Executable) != 0 || len(plan.Skipped) != plan.TotalSamples {
		t.Fatalf("expected only skipped details for default plan, got %#v", plan)
	}
}

func TestRunShadowBatchUsesNormalizerAndContext(t *testing.T) {
	official := batchRecordingRunner{stdoutByCommand: map[string]string{"topicList": "timestamp=1\n"}}
	native := batchRecordingRunner{stdoutByCommand: map[string]string{"topicList": "timestamp=2\n"}}
	normalizer := ReplaceShadowText("timestamp=1", "timestamp=<dynamic>")
	normalizer = ComposeShadowNormalizers(normalizer, ReplaceShadowText("timestamp=2", "timestamp=<dynamic>"))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := RunShadowBatch(ctx, ShadowBatch{
		Primary:    ShadowTarget{Name: "official", Runner: &official},
		Targets:    []ShadowTarget{{Name: "native", Runner: &native}},
		Normalizer: normalizer,
		Samples: []ShadowSample{{
			Name:       "topic-list",
			Args:       []string{"topicList"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
			MinSamples: 1,
		}},
	})

	if len(result.Results) != 1 || len(result.Results[0].Diffs) != 1 || !result.Results[0].Diffs[0].Matched {
		t.Fatalf("expected normalized batch result to match, got %#v", result.Results)
	}
}

func TestRunShadowBatchUsesDefaultM6ShadowNormalizer(t *testing.T) {
	official := batchRecordingRunner{
		stdoutByCommand: map[string]string{"queryMsgById": "bornTimestamp=1781749000000 queueOffset=100 took 91ms\n"},
		stderrByCommand: map[string]string{"queryMsgById": "2026-06-18 10:01:02.123 cost=91ms\n"},
	}
	native := batchRecordingRunner{
		stdoutByCommand: map[string]string{"queryMsgById": "bornTimestamp=1781749009999 queueOffset=101 took 12ms\n"},
		stderrByCommand: map[string]string{"queryMsgById": "2026-06-18 10:01:03.456 cost=12ms\n"},
	}

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary:    ShadowTarget{Name: "official", Runner: &official},
		Targets:    []ShadowTarget{{Name: "native", Runner: &native}},
		Normalizer: DefaultM6ShadowNormalizer(),
		Samples: []ShadowSample{{
			Name:       "known-message",
			Args:       []string{"queryMsgById", "-i", "fixture-message-id"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
			MinSamples: 1,
		}},
	})

	if len(result.Results) != 1 || len(result.Results[0].Diffs) != 1 || !result.Results[0].Diffs[0].Matched {
		t.Fatalf("expected default M6 normalizer to hide dynamic fields, got %#v", result.Results)
	}
}

func TestRunShadowBatchMaxConcurrencyKeepsResultOrder(t *testing.T) {
	official := newConcurrentBatchRunner(map[string]string{
		"slow":   "same\n",
		"medium": "same\n",
		"fast":   "same\n",
	}, map[string]time.Duration{
		"slow":   20 * time.Millisecond,
		"medium": 20 * time.Millisecond,
		"fast":   20 * time.Millisecond,
	})
	native := newConcurrentBatchRunner(map[string]string{
		"slow":   "same\n",
		"medium": "same\n",
		"fast":   "same\n",
	}, map[string]time.Duration{
		"slow":   50 * time.Millisecond,
		"medium": 50 * time.Millisecond,
		"fast":   50 * time.Millisecond,
	})

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary:        ShadowTarget{Name: "official", Runner: official},
		Targets:        []ShadowTarget{{Name: "native", Runner: native}},
		MaxConcurrency: 3,
		Samples: []ShadowSample{
			{Name: "slow", Args: []string{"slow"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
			{Name: "medium", Args: []string{"medium"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
			{Name: "fast", Args: []string{"fast"}, Providers: []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative}, MinSamples: 1},
		},
	})

	if result.ExecutedSamples != 3 || len(result.Results) != 3 {
		t.Fatalf("expected three executed samples, got %#v", result)
	}
	commands := []string{result.Results[0].Command, result.Results[1].Command, result.Results[2].Command}
	expectedOrder := []string{"slow", "medium", "fast"}
	if !reflect.DeepEqual(commands, expectedOrder) {
		t.Fatalf("expected result order to follow sample order, expected=%#v actual=%#v", expectedOrder, commands)
	}
	if official.MaxActive() < 2 || native.MaxActive() < 2 {
		t.Fatalf("expected concurrent execution, official max=%d native max=%d", official.MaxActive(), native.MaxActive())
	}
}

func TestRunShadowBatchFiltersTargetsBySampleProviders(t *testing.T) {
	official := batchRecordingRunner{stdoutByCommand: map[string]string{"topicList": "topics\n"}}
	native := batchRecordingRunner{stdoutByCommand: map[string]string{"topicList": "topics\n"}}
	sidecar := batchRecordingRunner{stdoutByCommand: map[string]string{"topicList": "sidecar topics\n"}}

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary: ShadowTarget{Name: "official", Runner: &official},
		Targets: []ShadowTarget{
			{Name: "native", Runner: &native},
			{Name: "sidecar", Runner: &sidecar},
		},
		Samples: []ShadowSample{{
			Name:       "native-only",
			Args:       []string{"topicList"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
			MinSamples: 1,
		}},
	})

	if len(result.Results) != 1 || len(result.Results[0].Targets) != 1 || result.Results[0].Targets[0].Name != "native" {
		t.Fatalf("expected only native target to run, got %#v", result.Results)
	}
	if len(native.calls) != 1 {
		t.Fatalf("expected native to be called once, got %#v", native.calls)
	}
	if len(sidecar.calls) != 0 {
		t.Fatalf("expected sidecar to be filtered out, got %#v", sidecar.calls)
	}
}

func TestRunShadowBatchRecordsRunnerErrorsInResults(t *testing.T) {
	official := batchRecordingRunner{errByCommand: map[string]error{"topicList": errors.New("official failed")}}
	native := batchRecordingRunner{}

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary: ShadowTarget{Name: "official", Runner: &official},
		Targets: []ShadowTarget{{Name: "native", Runner: &native}},
		Samples: []ShadowSample{{
			Name:       "topic-list",
			Args:       []string{"topicList"},
			Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
			MinSamples: 1,
		}},
	})

	if len(result.Errors) != 0 {
		t.Fatalf("runner errors should stay in shadow result, got batch errors %#v", result.Errors)
	}
	if len(result.Results) != 1 || result.Results[0].Primary.Error != "official failed" {
		t.Fatalf("expected primary runner error in result, got %#v", result.Results)
	}
	if result.Summary.Mismatches != 1 {
		t.Fatalf("expected summary mismatch for runner error difference, got %#v", result.Summary)
	}
}

func TestRunShadowBatchDefaultPlanIsDryRunSafe(t *testing.T) {
	official := batchRecordingRunner{}
	native := batchRecordingRunner{}

	result := RunShadowBatch(context.Background(), ShadowBatch{
		Primary: ShadowTarget{Name: "official", Runner: &official},
		Targets: []ShadowTarget{{Name: "native", Runner: &native}},
		Samples: DefaultM6ShadowPlan(),
	})

	if len(result.Errors) != 0 {
		t.Fatalf("expected default plan to be structurally valid, got %#v", result.Errors)
	}
	if result.ExecutedSamples != 0 {
		t.Fatalf("default plan contains placeholders and must not execute commands, got %#v", result)
	}
	if result.SkippedSamples != result.TotalSamples || result.TotalSamples == 0 {
		t.Fatalf("expected all default samples to be skipped, got %#v", result)
	}
	if len(official.calls) != 0 || len(native.calls) != 0 {
		t.Fatalf("default plan must not call runners before placeholders are filled, official=%#v native=%#v", official.calls, native.calls)
	}
}

type batchRecordingRunner struct {
	stdoutByCommand map[string]string
	stderrByCommand map[string]string
	errByCommand    map[string]error
	calls           [][]string
}

type concurrentBatchRunner struct {
	stdoutByCommand map[string]string
	delayByCommand  map[string]time.Duration
	mutex           sync.Mutex
	active          int
	maxActive       int
}

func newConcurrentBatchRunner(stdoutByCommand map[string]string, delayByCommand map[string]time.Duration) *concurrentBatchRunner {
	return &concurrentBatchRunner{
		stdoutByCommand: stdoutByCommand,
		delayByCommand:  delayByCommand,
	}
}

func (runner *concurrentBatchRunner) RunShadow(ctx context.Context, args []string) (ShadowOutput, error) {
	command := ""
	if len(args) > 0 {
		command = args[0]
	}
	runner.enter()
	defer runner.leave()
	timer := time.NewTimer(runner.delayByCommand[command])
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ShadowOutput{}, ctx.Err()
	case <-timer.C:
		return ShadowOutput{Stdout: runner.stdoutByCommand[command]}, nil
	}
}

func (runner *concurrentBatchRunner) enter() {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	runner.active++
	if runner.active > runner.maxActive {
		runner.maxActive = runner.active
	}
}

func (runner *concurrentBatchRunner) leave() {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	runner.active--
}

func (runner *concurrentBatchRunner) MaxActive() int {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	return runner.maxActive
}

func (runner *batchRecordingRunner) RunShadow(ctx context.Context, args []string) (ShadowOutput, error) {
	runner.calls = append(runner.calls, append([]string(nil), args...))
	command := ""
	if len(args) > 0 {
		command = args[0]
	}
	return ShadowOutput{
		Stdout: runner.stdoutByCommand[command],
		Stderr: runner.stderrByCommand[command],
	}, runner.errByCommand[command]
}
