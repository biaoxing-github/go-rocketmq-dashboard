package goadmin

import (
	"strings"
	"testing"
)

func TestComposeShadowNormalizersRunsInOrder(t *testing.T) {
	normalizer := ComposeShadowNormalizers(
		ReplaceShadowText("step=1", "step=2"),
		ReplaceShadowText("step=2", "step=3"),
	)

	output := normalizer(ShadowOutput{
		Stdout: "stdout step=1\n",
		Stderr: "stderr step=1\n",
	})

	if output.Stdout != "stdout step=3\n" {
		t.Fatalf("expected stdout normalizers to run in order, got %q", output.Stdout)
	}
	if output.Stderr != "stderr step=3\n" {
		t.Fatalf("expected stderr normalizers to run in order, got %q", output.Stderr)
	}
}

func TestReplaceShadowTextRewritesStdoutAndStderr(t *testing.T) {
	normalizer := ReplaceShadowText("cost=123ms", "cost=<dynamic>")

	output := normalizer(ShadowOutput{
		Stdout: "ok cost=123ms\n",
		Stderr: "warn cost=123ms\n",
	})

	if output.Stdout != "ok cost=<dynamic>\n" {
		t.Fatalf("expected stdout text to be replaced, got %q", output.Stdout)
	}
	if output.Stderr != "warn cost=<dynamic>\n" {
		t.Fatalf("expected stderr text to be replaced, got %q", output.Stderr)
	}
}

func TestReplaceShadowRegexpRewritesStdoutAndStderr(t *testing.T) {
	normalizer := ReplaceShadowRegexp(`cost=\d+ms`, "cost=<dynamic>")

	output := normalizer(ShadowOutput{
		Stdout: "ok cost=123ms\n",
		Stderr: "warn cost=456ms\n",
	})

	if output.Stdout != "ok cost=<dynamic>\n" {
		t.Fatalf("expected stdout regexp text to be replaced, got %q", output.Stdout)
	}
	if output.Stderr != "warn cost=<dynamic>\n" {
		t.Fatalf("expected stderr regexp text to be replaced, got %q", output.Stderr)
	}
}

func TestComposeShadowNormalizersNilAndEmptyKeepOutput(t *testing.T) {
	input := ShadowOutput{
		Stdout: "stdout\n",
		Stderr: "stderr\n",
	}

	if output := ComposeShadowNormalizers()(input); output != input {
		t.Fatalf("expected empty normalizer chain to keep output, got %#v", output)
	}
	if output := ComposeShadowNormalizers(nil)(input); output != input {
		t.Fatalf("expected nil normalizer to keep output, got %#v", output)
	}
}

func TestNormalizeShadowLinesAppliesFunctionPerLine(t *testing.T) {
	normalizer := NormalizeShadowLines(func(line string) string {
		return strings.TrimSpace(line)
	})

	output := normalizer(ShadowOutput{
		Stdout: "  a  \n  b  \n",
		Stderr: "  warn  \n",
	})

	if output.Stdout != "a\nb\n" {
		t.Fatalf("expected stdout lines to be normalized, got %q", output.Stdout)
	}
	if output.Stderr != "warn\n" {
		t.Fatalf("expected stderr lines to be normalized, got %q", output.Stderr)
	}
}

func TestComposeShadowNormalizersRunsRegexpNormalizerInOrder(t *testing.T) {
	normalizer := ComposeShadowNormalizers(
		ReplaceShadowRegexp(`broker-\d+`, "broker-<id>"),
		ReplaceShadowText("broker-<id>", "broker"),
	)

	output := normalizer(ShadowOutput{
		Stdout: "broker-101\n",
		Stderr: "broker-202\n",
	})

	if output.Stdout != "broker\n" {
		t.Fatalf("expected stdout regexp normalizer to compose in order, got %q", output.Stdout)
	}
	if output.Stderr != "broker\n" {
		t.Fatalf("expected stderr regexp normalizer to compose in order, got %q", output.Stderr)
	}
}

func TestDefaultM6ShadowNormalizerRemovesCommonDynamicFields(t *testing.T) {
	normalizer := DefaultM6ShadowNormalizer()

	output := normalizer(ShadowOutput{
		Stdout: "timestamp=1781747000123 bornTimestamp=1781747000456 cost=123ms elapsed=45ms queueOffset=789 commitOffset=456\n#Consumer Offset #LastTime\n",
		Stderr: "WARN request took 67ms at 2026-06-18 09:55:01.123\n",
	})

	expectedStdout := "timestamp=<dynamic> bornTimestamp=<dynamic> cost=<duration> elapsed=<duration> queueOffset=<offset> commitOffset=<offset>\n#Consumer Offset #LastTime\n"
	expectedStderr := "WARN request took <duration> at <datetime>\n"
	if output.Stdout != expectedStdout {
		t.Fatalf("unexpected normalized stdout\nexpected=%q\nactual=%q", expectedStdout, output.Stdout)
	}
	if output.Stderr != expectedStderr {
		t.Fatalf("unexpected normalized stderr\nexpected=%q\nactual=%q", expectedStderr, output.Stderr)
	}
}
