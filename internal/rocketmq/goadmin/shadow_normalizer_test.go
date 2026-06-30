package goadmin

import (
	"reflect"
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

	if output := ComposeShadowNormalizers()(input); !reflect.DeepEqual(output, input) {
		t.Fatalf("expected empty normalizer chain to keep output, got %#v", output)
	}
	if output := ComposeShadowNormalizers(nil)(input); !reflect.DeepEqual(output, input) {
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

func TestDefaultM6ShadowNormalizerRemovesExportMetadataArtifactTime(t *testing.T) {
	normalizer := DefaultM6ShadowNormalizer()

	output := normalizer(ShadowOutput{
		Artifacts: map[string]string{
			"metadata.json": "{\n\t\"exportTime\":1782480000123,\n\t\"topicConfigTable\":{}\n}",
		},
	})

	expected := "{\n\t\"exportTime\":<dynamic>,\n\t\"topicConfigTable\":{}\n}"
	if output.Artifacts["metadata.json"] != expected {
		t.Fatalf("unexpected normalized metadata artifact\nexpected=%q\nactual=%q", expected, output.Artifacts["metadata.json"])
	}
}

func TestNormalizeBrokerStatusShadowOutputKeepsOnlyRuntimeFieldsDynamic(t *testing.T) {
	output := normalizeBrokerStatusShadowOutput(ShadowOutput{
		Stdout: "putTps                          : 1.0 2.0 3.0\n" +
			"runtime                         : 120 seconds\n" +
			"192.168.0.10:10911        getTransferredTps              : 4.0 5.0 6.0\n" +
			"timerReadBehind                 : 1\n" +
			"brokerVersionDesc               : V5_3_2\n",
		Stderr: "brokerStatus warn 123\n",
	})

	expectedStdout := "putTps                          : <dynamic>\n" +
		"runtime                         : <dynamic>\n" +
		"192.168.0.10:10911        getTransferredTps              : <dynamic>\n" +
		"timerReadBehind                 : <dynamic>\n" +
		"brokerVersionDesc               : V5_3_2\n"
	if output.Stdout != expectedStdout {
		t.Fatalf("unexpected brokerStatus normalized stdout\nexpected=%q\nactual=%q", expectedStdout, output.Stdout)
	}
	if output.Stderr != "brokerStatus warn 123\n" {
		t.Fatalf("expected brokerStatus normalizer to keep stderr unchanged, got %q", output.Stderr)
	}
}

func TestNormalizeShadowOutputForProducerHidesLastUpdateTimestamp(t *testing.T) {
	first := normalizeShadowOutputForCommand("producer", shadowComparableOutput{
		Stdout: "#Group #ClientID #Version #LastUpdate\n" +
			"GoadminBenchmarkProducer PID_1 JAVA V5_3_2 lastUpdateTimestamp=1782561006123\n",
	}, DefaultM6ShadowNormalizer())
	second := normalizeShadowOutputForCommand("producer", shadowComparableOutput{
		Stdout: "#Group #ClientID #Version #LastUpdate\n" +
			"GoadminBenchmarkProducer PID_1 JAVA V5_3_2 lastUpdateTimestamp=1782561010456\n",
	}, DefaultM6ShadowNormalizer())

	expected := "#Group #ClientID #Version #LastUpdate\n" +
		"GoadminBenchmarkProducer PID_1 JAVA V5_3_2 lastUpdateTimestamp=<dynamic>\n"
	if first.Stdout != expected {
		t.Fatalf("unexpected producer normalized stdout\nexpected=%q\nactual=%q", expected, first.Stdout)
	}
	if first.Stdout != second.Stdout {
		t.Fatalf("expected producer timestamps to normalize equally\nfirst=%q\nsecond=%q", first.Stdout, second.Stdout)
	}
}
