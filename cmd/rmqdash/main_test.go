package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"slices"
	"strings"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/config"
	"rocketmq-go-dashboard/internal/rocketmq"
	goadminshadow "rocketmq-go-dashboard/internal/rocketmq/goadmin"
)

func TestAdminSidecarJavaArgsUsesToolsLogbackConfig(t *testing.T) {
	args := adminSidecarJavaArgs(config.Config{
		AdminSidecarMainClass: "dev.codex.rocketmq.AdminSidecar",
		AdminSidecarAddr:      "127.0.0.1:18091",
	}, "/app/rocketmq-admin-sidecar:/opt/rocketmq/lib/*")

	expected := "-Drmq.logback.configurationFile=/opt/rocketmq/conf/rmq.tools.logback.xml"
	if !slices.Contains(args, expected) {
		t.Fatalf("expected sidecar java args to include %q, got %#v", expected, args)
	}
}

func TestMQAdminProviderConfigForModeUsesProcessByDefault(t *testing.T) {
	cfg := config.Config{
		AdminProvider:  "mqadmin",
		RequestTimeout: time.Second,
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if provider == nil {
		t.Fatalf("expected provider")
	}
	if provider.SidecarEnabled {
		t.Fatalf("mqadmin provider must use process runner, got sidecar enabled")
	}
	if provider.CommandRunner != nil {
		t.Fatalf("mqadmin provider must not inject goadmin command runner")
	}
}

func TestMQAdminProviderConfigForModeUsesProcessPrimaryWithGoAdminShadow(t *testing.T) {
	cfg := config.Config{
		AdminProvider:    "mqadmin",
		NameServer:       "127.0.0.1:9876",
		GoAdminShadow:    true,
		GoAdminTimeout:   1500 * time.Millisecond,
		RequestTimeout:   60 * time.Second,
		AdminSidecarAddr: "127.0.0.1:18091",
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if provider.SidecarEnabled {
		t.Fatalf("mqadmin shadow phase must keep process primary, got sidecar enabled")
	}
	shadowRunner, ok := provider.CommandRunner.(shadowingCommandRunner)
	if !ok {
		t.Fatalf("expected mqadmin provider to wrap process primary with shadowingCommandRunner, got %T", provider.CommandRunner)
	}
	if shadowRunner.primaryName != "process" {
		t.Fatalf("expected process primary name, got %q", shadowRunner.primaryName)
	}
	if len(shadowRunner.shadows) != 1 || shadowRunner.shadows[0].Name != "native" {
		t.Fatalf("expected only native shadow target for mqadmin primary, got %#v", shadowRunner.shadows)
	}
}

func TestMQAdminProviderConfigForModeUsesSidecar(t *testing.T) {
	cfg := config.Config{
		AdminProvider:       "sidecar",
		AdminSidecarAddr:    "127.0.0.1:18091",
		AdminSidecarTimeout: 2 * time.Second,
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if !provider.SidecarEnabled {
		t.Fatalf("sidecar provider must enable sidecar")
	}
	if provider.CommandRunner != nil {
		t.Fatalf("sidecar provider must not inject goadmin command runner")
	}
	if provider.SidecarAddr != "127.0.0.1:18091" {
		t.Fatalf("unexpected sidecar addr %q", provider.SidecarAddr)
	}
}

func TestMQAdminProviderConfigForModeUsesGoAdminRunner(t *testing.T) {
	cfg := config.Config{
		AdminProvider:    "goadmin",
		NameServer:       "127.0.0.1:9876",
		GoAdminTimeout:   1500 * time.Millisecond,
		RequestTimeout:   60 * time.Second,
		AdminSidecarAddr: "127.0.0.1:18091",
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if provider.SidecarEnabled {
		t.Fatalf("goadmin provider should use command runner, not MQAdminProvider sidecar")
	}
	if provider.CommandRunner == nil {
		t.Fatalf("goadmin provider must inject command runner")
	}
}

func TestMQAdminProviderConfigForModeWrapsGoAdminRunnerWhenShadowEnabled(t *testing.T) {
	cfg := config.Config{
		AdminProvider:    "goadmin",
		NameServer:       "127.0.0.1:9876",
		GoAdminShadow:    true,
		GoAdminTimeout:   1500 * time.Millisecond,
		RequestTimeout:   60 * time.Second,
		AdminSidecarAddr: "127.0.0.1:18091",
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if _, ok := provider.CommandRunner.(shadowingCommandRunner); !ok {
		t.Fatalf("expected goadmin provider to wrap command runner with shadowingCommandRunner, got %T", provider.CommandRunner)
	}
}

func TestShadowingCommandRunnerUsesDefaultM6NormalizerForProductionWiring(t *testing.T) {
	tests := []struct {
		name          string
		adminProvider string
		primaryName   string
	}{
		{name: "goadmin native primary", adminProvider: "goadmin", primaryName: "native"},
		{name: "explicit auto primary", adminProvider: "auto", primaryName: "auto"},
		{name: "default auto primary", adminProvider: "", primaryName: "auto"},
		{name: "process primary", adminProvider: "mqadmin", primaryName: "process"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Config{
				AdminProvider:    tt.adminProvider,
				NameServer:       "127.0.0.1:9876",
				GoAdminShadow:    true,
				GoAdminTimeout:   1500 * time.Millisecond,
				RequestTimeout:   60 * time.Second,
				AdminSidecarAddr: "127.0.0.1:18091",
			}

			provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
			shadowRunner, ok := provider.CommandRunner.(shadowingCommandRunner)
			if !ok {
				t.Fatalf("expected shadowingCommandRunner, got %T", provider.CommandRunner)
			}
			if shadowRunner.primaryName != tt.primaryName {
				t.Fatalf("expected primary %q, got %q", tt.primaryName, shadowRunner.primaryName)
			}
			if shadowRunner.normalizer == nil {
				t.Fatalf("expected production shadow runner to set DefaultM6ShadowNormalizer")
			}

			normalized := shadowRunner.normalizer(goadminshadow.ShadowOutput{
				Stdout: "time=2026-06-18 12:34:56.789 took 25ms queueOffset=123 brokerOffset=456 bornTimestamp=1710000000000",
				Stderr: "duration=41ms consumerOffset=789 storeTimestamp=1710000000001",
			})
			if strings.Contains(normalized.Stdout, "2026-06-18 12:34:56.789") ||
				strings.Contains(normalized.Stdout, "25ms") ||
				strings.Contains(normalized.Stdout, "queueOffset=123") ||
				strings.Contains(normalized.Stdout, "brokerOffset=456") ||
				strings.Contains(normalized.Stdout, "bornTimestamp=1710000000000") ||
				strings.Contains(normalized.Stderr, "duration=41ms") ||
				strings.Contains(normalized.Stderr, "consumerOffset=789") ||
				strings.Contains(normalized.Stderr, "storeTimestamp=1710000000001") {
				t.Fatalf("expected M6 dynamic fields to be normalized, got stdout=%q stderr=%q", normalized.Stdout, normalized.Stderr)
			}
		})
	}
}

func TestGoAdminShadowTargetsSkipSidecarWhenSidecarDisabled(t *testing.T) {
	cfg := config.Config{
		GoAdminTimeout:      time.Second,
		RequestTimeout:      60 * time.Second,
		AdminSidecarEnabled: false,
		AdminSidecarAddr:    "127.0.0.1:18091",
	}

	targets := goAdminShadowTargets(cfg, "127.0.0.1:9876", "native")

	for _, target := range targets {
		if target.Name == "sidecar" {
			t.Fatalf("expected disabled sidecar to be excluded from shadow targets, got %#v", targets)
		}
	}
	if !shadowTargetsContain(targets, "process") || !shadowTargetsContain(targets, "auto") {
		t.Fatalf("expected process and auto targets to remain, got %#v", targets)
	}
}

func TestGoAdminShadowTargetsIncludeSidecarWhenSidecarEnabled(t *testing.T) {
	cfg := config.Config{
		GoAdminTimeout:      time.Second,
		RequestTimeout:      60 * time.Second,
		AdminSidecarEnabled: true,
		AdminSidecarAddr:    "127.0.0.1:18091",
	}

	targets := goAdminShadowTargets(cfg, "127.0.0.1:9876", "native")

	if !shadowTargetsContain(targets, "sidecar") {
		t.Fatalf("expected enabled sidecar to be included, got %#v", targets)
	}
}

func TestMQAdminProviderConfigForModeUsesAutoRunner(t *testing.T) {
	cfg := config.Config{
		AdminProvider:       "auto",
		NameServer:          "127.0.0.1:9876",
		GoAdminTimeout:      time.Second,
		AdminSidecarAddr:    "127.0.0.1:18091",
		AdminSidecarTimeout: 2 * time.Second,
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if provider.CommandRunner == nil {
		t.Fatalf("auto provider must inject goadmin command runner")
	}
}

func TestMQAdminProviderConfigForModeDefaultsToAutoRunner(t *testing.T) {
	cfg := config.Config{
		NameServer:          "127.0.0.1:9876",
		GoAdminTimeout:      time.Second,
		AdminSidecarAddr:    "127.0.0.1:18091",
		AdminSidecarTimeout: 2 * time.Second,
	}

	provider := mqAdminProviderForMode(cfg, "127.0.0.1:9876")
	if provider.CommandRunner == nil {
		t.Fatalf("empty admin provider must default to goadmin auto runner")
	}
	if provider.SidecarEnabled {
		t.Fatalf("auto provider should use goadmin runner rather than direct MQAdminProvider sidecar")
	}
}

func TestShadowingCommandRunnerReturnsPrimaryAndReportsShadowDiff(t *testing.T) {
	reported := make(chan goadminshadow.ShadowResult, 1)
	runner := shadowingCommandRunner{
		primary: rocketmq.CommandRunnerFunc(func(ctx context.Context, args ...string) (string, error) {
			return "primary-output", nil
		}),
		timeout: time.Second,
		shadows: []goadminshadow.ShadowTarget{{
			Name: "sidecar",
			Runner: shadowRunnerFunc(func(ctx context.Context, args []string) (goadminshadow.ShadowOutput, error) {
				return goadminshadow.ShadowOutput{Stdout: "shadow-output"}, errors.New("shadow failed")
			}),
		}},
		report: func(result goadminshadow.ShadowResult) {
			reported <- result
		},
	}

	output, err := runner.Run(context.Background(), "topicList", "-n", "127.0.0.1:9876")
	if err != nil {
		t.Fatalf("expected primary error to be returned only, got %v", err)
	}
	if output != "primary-output" {
		t.Fatalf("expected primary output, got %q", output)
	}

	select {
	case result := <-reported:
		if result.Command != "topicList" {
			t.Fatalf("expected command topicList, got %q", result.Command)
		}
		if len(result.Diffs) != 1 || result.Diffs[0].Matched {
			t.Fatalf("expected one mismatched shadow diff, got %#v", result.Diffs)
		}
		if len(result.Targets) != 1 || result.Targets[0].Error != "shadow failed" {
			t.Fatalf("expected shadow error to be recorded, got %#v", result.Targets)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected shadow report")
	}
}

func TestReportGoAdminShadowResultLogsJSONSummaryWithoutRawOutput(t *testing.T) {
	var logs bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	defer func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	}()
	log.SetOutput(&logs)
	log.SetFlags(0)

	reportGoAdminShadowResult(goadminshadow.ShadowResult{
		Command: "topicList",
		Args:    []string{"topicList", "-n", "127.0.0.1:9876"},
		Primary: goadminshadow.ShadowTargetResult{
			Name:   "official",
			Stdout: "SECRET OFFICIAL STDOUT",
		},
		Targets: []goadminshadow.ShadowTargetResult{{
			Name:   "native",
			Stdout: "SECRET NATIVE STDOUT",
		}},
		Diffs: []goadminshadow.ShadowDiff{{
			Target:          "native",
			Matched:         false,
			StdoutDifferent: true,
			Duration:        25 * time.Millisecond,
		}},
	})

	output := logs.String()
	if !strings.Contains(output, `"command":"topicList"`) || !strings.Contains(output, `"stdout_bytes"`) {
		t.Fatalf("expected JSON shadow summary in log, got %q", output)
	}
	if strings.Contains(output, "SECRET OFFICIAL STDOUT") || strings.Contains(output, "SECRET NATIVE STDOUT") {
		t.Fatalf("shadow log leaked raw output: %q", output)
	}
}

func shadowTargetsContain(targets []goadminshadow.ShadowTarget, name string) bool {
	for _, target := range targets {
		if target.Name == name {
			return true
		}
	}
	return false
}

type shadowRunnerFunc func(ctx context.Context, args []string) (goadminshadow.ShadowOutput, error)

func (f shadowRunnerFunc) RunShadow(ctx context.Context, args []string) (goadminshadow.ShadowOutput, error) {
	return f(ctx, args)
}
