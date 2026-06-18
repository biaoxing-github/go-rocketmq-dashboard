package goadmin

import (
	"strings"
	"testing"
)

func TestDefaultM6ShadowPlanIsValid(t *testing.T) {
	samples := DefaultM6ShadowPlan()
	if err := ValidateShadowPlan(samples); err != nil {
		t.Fatalf("expected default M6 shadow plan to be valid: %v", err)
	}

	required := map[string]bool{
		"command-smoke":        false,
		"known-message":        false,
		"recent-topic-message": false,
		"message-chain-cold":   false,
		"message-chain-warm":   false,
	}
	for _, sample := range samples {
		if _, ok := required[sample.Name]; ok {
			required[sample.Name] = true
		}
	}
	for name, found := range required {
		if !found {
			t.Fatalf("expected default plan to include %s, got %#v", name, samples)
		}
	}
}

func TestValidateShadowPlanRequiresOfficialProvider(t *testing.T) {
	err := ValidateShadowPlan([]ShadowSample{{
		Name:       "missing-official",
		Args:       []string{"clusterList"},
		Providers:  []ShadowProviderMode{ShadowProviderNative},
		MinSamples: 1,
	}})

	if err == nil || !strings.Contains(err.Error(), "official") {
		t.Fatalf("expected official provider error, got %v", err)
	}
}

func TestValidateShadowPlanRequiresEnoughP95Samples(t *testing.T) {
	err := ValidateShadowPlan([]ShadowSample{{
		Name:       "p95-too-small",
		Args:       []string{"messageChain", "-t", "TopicTest", "-k", "MessageKey"},
		Providers:  []ShadowProviderMode{ShadowProviderOfficial, ShadowProviderNative},
		MinSamples: 19,
		RequireP95: true,
	}})

	if err == nil || !strings.Contains(err.Error(), "MinSamples") {
		t.Fatalf("expected p95 MinSamples error, got %v", err)
	}
}

func TestDefaultM6ShadowPlanReturnsDefensiveCopies(t *testing.T) {
	first := DefaultM6ShadowPlan()
	if len(first) == 0 {
		t.Fatal("expected non-empty default plan")
	}
	first[0].Args[0] = "mutated-command"
	first[0].Providers[0] = ShadowProviderAuto

	second := DefaultM6ShadowPlan()
	if second[0].Args[0] == "mutated-command" {
		t.Fatalf("expected args to be copied between default plan calls, got %#v", second[0].Args)
	}
	if second[0].Providers[0] == ShadowProviderAuto {
		t.Fatalf("expected providers to be copied between default plan calls, got %#v", second[0].Providers)
	}
}

func TestApplyShadowFixtureOverridesMarksConcreteSamplesExecutable(t *testing.T) {
	samples, err := ApplyShadowFixtureOverrides(DefaultM6ShadowPlan(), ShadowFixtureOverrides{
		Samples: []ShadowSampleFixture{
			{
				Name: "known-message",
				Args: []string{"queryMsgById", "-i",
					"AC18000300002A9F0000000000000000"},
			},
			{
				Name: "recent-topic-message",
				Args: []string{"queryMsgByKey", "-t",
					"GoadminQueryKeyTest", "-k", "goadmin-query-key"},
			},
		},
	})
	if err != nil {
		t.Fatalf("apply fixture overrides: %v", err)
	}

	plan := PlanShadowBatch(samples)

	if plan.ExecutableSamples != 2 || plan.SkippedSamples != 3 {
		t.Fatalf("expected 2 executable and 3 skipped samples, got executable=%d skipped=%d plan=%#v",
			plan.ExecutableSamples, plan.SkippedSamples, plan)
	}
	if plan.Executable[0].Name != "known-message" {
		t.Fatalf("expected known-message executable sample first, got %#v", plan.Executable)
	}
	if strings.Contains(strings.Join(plan.Executable[0].Args, " "), "<known-message-id>") {
		t.Fatalf("expected known-message placeholder to be replaced, got %#v", plan.Executable[0].Args)
	}
	if DefaultM6ShadowPlan()[1].Args[2] != "<known-message-id>" {
		t.Fatalf("expected default plan to remain unchanged")
	}
}

func TestApplyShadowFixtureOverridesRejectsUnknownSample(t *testing.T) {
	_, err := ApplyShadowFixtureOverrides(DefaultM6ShadowPlan(), ShadowFixtureOverrides{
		Samples: []ShadowSampleFixture{{
			Name: "not-in-default-plan",
			Args: []string{"clusterList"},
		}},
	})

	if err == nil || !strings.Contains(err.Error(), "not-in-default-plan") {
		t.Fatalf("expected unknown sample error, got %v", err)
	}
}
