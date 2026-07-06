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

func TestNormalizeShadowOutputForCheckMsgSendRTHidesOnlyRTColumns(t *testing.T) {
	first := normalizeShadowOutputForCommand("checkMsgSendRT", shadowComparableOutput{
		Stdout: "#Broker Name                      #QID  #Send Result            #RT\n" +
			"broker-a                          0     true                    8\n" +
			"broker-a                          1     true                    13\n" +
			"Avg RT: 12.50\n",
	}, DefaultM6ShadowNormalizer())
	second := normalizeShadowOutputForCommand("checkMsgSendRT", shadowComparableOutput{
		Stdout: "#Broker Name                      #QID  #Send Result            #RT\n" +
			"broker-a                          0     true                    123\n" +
			"broker-a                          1     true                    4\n" +
			"Avg RT: 4.00\n",
	}, DefaultM6ShadowNormalizer())

	expected := "#Broker Name                      #QID  #Send Result            #RT\n" +
		"broker-a                          0     true                    <rt>\n" +
		"broker-a                          1     true                    <rt>\n" +
		"Avg RT: <rt>\n"
	if first.Stdout != expected {
		t.Fatalf("unexpected checkMsgSendRT normalized stdout\nexpected=%q\nactual=%q", expected, first.Stdout)
	}
	if first.Stdout != second.Stdout {
		t.Fatalf("expected checkMsgSendRT RT values to normalize equally\nfirst=%q\nsecond=%q", first.Stdout, second.Stdout)
	}

	unchanged := normalizeShadowOutputForCommand("topicStatus", shadowComparableOutput{Stdout: "Avg RT: 12.50\n"}, DefaultM6ShadowNormalizer())
	if unchanged.Stdout != "Avg RT: 12.50\n" {
		t.Fatalf("expected non-checkMsgSendRT command to preserve Avg RT text, got %q", unchanged.Stdout)
	}
}

func TestNormalizeShadowOutputForSendMsgStatusHidesDynamicSendResultFields(t *testing.T) {
	first := normalizeShadowOutputForCommand("sendMsgStatus", shadowComparableOutput{
		Stdout: "rt=7ms, SendResult=SendResult [sendStatus=SEND_OK, msgId=AC180002018F1152471119E69A780001, offsetMsgId=AC18000400002A9F000000005740AA95, messageQueue=MessageQueue [topic=55924048bd08, brokerName=55924048bd08, queueId=0], queueOffset=155225, recallHandle=null]\n",
	}, DefaultM6ShadowNormalizer())
	second := normalizeShadowOutputForCommand("sendMsgStatus", shadowComparableOutput{
		Stdout: "rt=3ms, SendResult=SendResult [sendStatus=SEND_OK, msgId=AC18000201C11152471119E6A92F0001, offsetMsgId=AC18000400002A9F000000005740AD44, messageQueue=MessageQueue [topic=55924048bd08, brokerName=55924048bd08, queueId=0], queueOffset=155228, recallHandle=null]\n",
	}, DefaultM6ShadowNormalizer())

	expected := "rt=<rt>ms, SendResult=SendResult [sendStatus=SEND_OK, msgId=<msgId>, offsetMsgId=<offsetMsgId>, messageQueue=MessageQueue [topic=55924048bd08, brokerName=55924048bd08, queueId=0], queueOffset=<offset>, recallHandle=null]\n"
	if first.Stdout != expected {
		t.Fatalf("unexpected sendMsgStatus normalized stdout\nexpected=%q\nactual=%q", expected, first.Stdout)
	}
	if first.Stdout != second.Stdout {
		t.Fatalf("expected sendMsgStatus dynamic fields to normalize equally\nfirst=%q\nsecond=%q", first.Stdout, second.Stdout)
	}

	unchanged := normalizeShadowOutputForCommand("sendMessage", shadowComparableOutput{Stdout: "rt=7ms, msgId=AC180002018F1152471119E69A780001\n"}, DefaultM6ShadowNormalizer())
	if unchanged.Stdout != "rt=7ms, msgId=AC180002018F1152471119E69A780001\n" {
		t.Fatalf("expected non-sendMsgStatus command to preserve sendMsgStatus-like text, got %q", unchanged.Stdout)
	}
}

func TestNormalizeShadowOutputForSendMessageHidesOnlyResultRowMsgID(t *testing.T) {
	first := normalizeShadowOutputForCommand("sendMessage", shadowComparableOutput{
		Stdout: "#Broker Name                      #QID  #Send Result            #MsgId\n" +
			"55924048bd08                      0     SEND_OK                 AC180002013C1152471119F2CF470000\n",
	}, DefaultM6ShadowNormalizer())
	second := normalizeShadowOutputForCommand("sendMessage", shadowComparableOutput{
		Stdout: "#Broker Name                      #QID  #Send Result            #MsgId\n" +
			"55924048bd08                      0     SEND_OK                 AC180002013C1152471119F2D1230001\n",
	}, DefaultM6ShadowNormalizer())

	expected := "#Broker Name                      #QID  #Send Result            #MsgId\n" +
		"55924048bd08                      0     SEND_OK                 <msgId>\n"
	if first.Stdout != expected {
		t.Fatalf("unexpected sendMessage normalized stdout\nexpected=%q\nactual=%q", expected, first.Stdout)
	}
	if first.Stdout != second.Stdout {
		t.Fatalf("expected sendMessage MsgId values to normalize equally\nfirst=%q\nsecond=%q", first.Stdout, second.Stdout)
	}

	unchanged := normalizeShadowOutputForCommand("sendMessage", shadowComparableOutput{Stdout: "rt=7ms, msgId=AC180002018F1152471119E69A780001\n"}, DefaultM6ShadowNormalizer())
	if unchanged.Stdout != "rt=7ms, msgId=AC180002018F1152471119E69A780001\n" {
		t.Fatalf("expected sendMessage normalizer to preserve non-table msgId text, got %q", unchanged.Stdout)
	}
}
