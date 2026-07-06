package rocketmq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMQAdminCommandFailureDetectsZeroExitException(t *testing.T) {
	output := `org.apache.rocketmq.tools.command.SubCommandException: TopicStatusSubCommand command failed
Caused by: org.apache.rocketmq.remoting.exception.RemotingTimeoutException: wait response on the channel </127.0.0.1:9876> timeout, 4936(ms)`

	if failure := mqadminCommandFailure(output); failure == "" {
		t.Fatalf("expected zero-exit command exception to be detected")
	}
}

func TestMQAdminCommandFailureKeepsMixedTableOutput(t *testing.T) {
	output := `org.apache.rocketmq.tools.command.SubCommandException: ConsumerProgressSubCommand command failed
#Group                                                            #Count  #Version                 #Type  #Model          #TPS     #Diff Total
sample-order-events-consumer             1       V5_3_2                   PUSH   CLUSTERING      0        0`

	if failure := mqadminCommandFailure(output); failure != "" {
		t.Fatalf("expected mixed table output to stay parseable, got %s", failure)
	}
}

func TestMQAdminJavaArgsForcesUTF8ConsoleEncoding(t *testing.T) {
	args := mqadminJavaArgs("rocketmq-tools.jar", []string{"queryMsgByOffset", "-f", "UTF-8"})
	expectedPrefix := []string{
		"-Dfile.encoding=UTF-8",
		"-Dsun.stdout.encoding=UTF-8",
		"-Dsun.stderr.encoding=UTF-8",
		"-cp",
		"rocketmq-tools.jar",
		"org.apache.rocketmq.tools.command.MQAdminStartup",
	}
	if len(args) < len(expectedPrefix) {
		t.Fatalf("expected java args to include utf-8 prefix, got %#v", args)
	}
	for index, expected := range expectedPrefix {
		if args[index] != expected {
			t.Fatalf("java arg %d mismatch: expected %q got %q in %#v", index, expected, args[index], args)
		}
	}
}

func TestTraceMissingDetailHidesMQAdminSubCommandException(t *testing.T) {
	err := &commandErrorForTest{message: "mqadmin 命令输出异常: org.apache.rocketmq.tools.command.SubCommandException: QueryMsgTraceByIdSubCommandcommand failed"}

	detail := traceMissingDetail(err)

	if detail == "" {
		t.Fatalf("expected trace missing detail")
	}
	if strings.Contains(detail, "SubCommandException") || strings.Contains(detail, "QueryMsgTraceByIdSubCommand") {
		t.Fatalf("expected user-facing trace detail, got %q", detail)
	}
	if !strings.Contains(detail, "traceEnable") || !strings.Contains(detail, "保留窗口") {
		t.Fatalf("expected detail to explain trace boundary, got %q", detail)
	}
}

func TestMessageChainUsesOfficialTraceMissingDetailForEmptyTraceOutput(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"queryMsgByOffset":  messageDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": "",
	})
	provider := &MQAdminProvider{
		NameServer:      "127.0.0.1:9876",
		CommandRunner:   runner,
		MessageCacheTTL: time.Hour,
	}

	chain, err := provider.MessageChain(context.Background(), MessageQuery{
		Topic:          "sample_notice_topic",
		MessageID:      "7F00000100002A9F00000000000123AB",
		BrokerName:     "broker-a",
		QueueID:        3,
		QueueOffset:    10240,
		HasQueueOffset: true,
	})
	if err != nil {
		t.Fatalf("MessageChain returned error: %v", err)
	}
	if len(chain.Steps) != 2 || chain.Steps[1].Stage != "TRACE_MISSING" {
		t.Fatalf("expected trace missing step, got %#v", chain.Steps)
	}
	detail := chain.Steps[1].Detail
	if !strings.Contains(detail, "traceEnable") || !strings.Contains(detail, "保留窗口") || !strings.Contains(detail, "Consumer 位点") {
		t.Fatalf("expected official trace missing explanation, got %q", detail)
	}
}

func TestTraceQueryMessageIDPrefersRocketMQUniqKey(t *testing.T) {
	message := MessageDetail{
		MessageID:      "ACA8015D00002A9F00000000975A7838",
		TraceMessageID: "0AE97A6A00017F3CA64A23D49A900003",
	}

	if got := traceQueryMessageID(message); got != message.TraceMessageID {
		t.Fatalf("expected trace query to use UNIQ_KEY, got %s", got)
	}
}

func TestTraceQueryMessageIDFallsBackToBrokerMessageID(t *testing.T) {
	message := MessageDetail{MessageID: "ACA8015D00002A9F00000000975A7838"}

	if got := traceQueryMessageID(message); got != message.MessageID {
		t.Fatalf("expected trace query to fall back to broker message id, got %s", got)
	}
}

func TestMessageChainUsesOffsetDetailAndSkipsConsumerProgressWhenTraceConsumed(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"queryMsgByOffset": messageDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": `#Type      #ProducerGroup       #ClientHost          #SendTime            #CostTimes #Status
Pub        PG_NOTICE            10.0.0.8             2026-06-06 19:48:01  12ms       success

#Type      #ConsumerGroup       #ClientHost          #ConsumerTime        #CostTimes #Status
Sub        CG_NOTICE            10.0.0.9             2026-06-06 19:48:05  18ms       success`,
	})
	provider := &MQAdminProvider{
		NameServer:       "127.0.0.1:9876",
		CommandRunner:    runner,
		MessageCacheTTL:  time.Hour,
		SidecarEnabled:   false,
		SidecarTransport: nil,
	}

	chain, err := provider.MessageChain(context.Background(), MessageQuery{
		Topic:          "sample_notice_topic",
		MessageID:      "7F00000100002A9F00000000000123AB",
		BrokerName:     "broker-a",
		QueueID:        3,
		QueueOffset:    10240,
		HasQueueOffset: true,
		ConsumerGroup:  "CG_NOTICE",
	})
	if err != nil {
		t.Fatalf("MessageChain returned error: %v", err)
	}
	if chain.OverallStatus != "CONSUME_SUCCESS" {
		t.Fatalf("expected consume success from trace, got %#v", chain)
	}
	if runner.countCommand("queryMsgByOffset") != 1 || runner.countCommand("queryMsgById") != 0 {
		t.Fatalf("expected offset detail path, calls=%#v", runner.commands())
	}
	if runner.countCommand("consumerProgress") != 0 {
		t.Fatalf("expected consumerProgress to be skipped when trace has same group consume success, calls=%#v", runner.commands())
	}
}

func TestMessageChainCachesDetailTraceAndConsumerProgress(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"queryMsgById": messageDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": `#Type      #ProducerGroup       #ClientHost          #SendTime            #CostTimes #Status
Pub        PG_NOTICE            10.0.0.8             2026-06-06 19:48:01  12ms       success`,
		"consumerProgress": `#Topic              #Broker Name  #QID  #Broker Offset  #Consumer Offset  #Client IP           #Diff  #Inflight  #LastTime
sample_notice_topic broker-a 3 10241 10239 10.0.0.9 2 0 2026-06-06 19:49:00`,
	})
	provider := &MQAdminProvider{
		NameServer:      "127.0.0.1:9876",
		CommandRunner:   runner,
		MessageCacheTTL: time.Hour,
	}
	query := MessageQuery{
		Topic:         "sample_notice_topic",
		MessageID:     "7F00000100002A9F00000000000123AB",
		ConsumerGroup: "CG_NOTICE",
	}

	if _, err := provider.MessageChain(context.Background(), query); err != nil {
		t.Fatalf("first MessageChain returned error: %v", err)
	}
	if _, err := provider.MessageChain(context.Background(), query); err != nil {
		t.Fatalf("second MessageChain returned error: %v", err)
	}
	if runner.countCommand("queryMsgById") != 1 || runner.countCommand("queryMsgTraceById") != 1 || runner.countCommand("consumerProgress") != 1 {
		t.Fatalf("expected detail/trace/progress caches to avoid duplicate mqadmin calls, calls=%#v", runner.commands())
	}
}

func TestClusterFeaturesCollectsBrokerAndNameServerConfigs(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"clusterList": `#Cluster Name           #Broker Name            #BID  #Addr                  #Version              #InTPS(LOAD)                   #OutTPS(LOAD)  #Timer(Progress)        #PCWait(ms)  #Hour         #SPACE    #ACTIVATED
DefaultCluster          broker-a                0     127.0.0.1:10911     V5_3_2                 0.00(0,0ms)               0.00(0,0ms|N,Nms)  0-0(0.0w, 0.0, 0.0)               0  1446.72       0.1200          true`,
		"topicList": `RMQ_SYS_TRANS_HALF_TOPIC
RMQ_SYS_TRANS_OP_HALF_TOPIC
RMQ_SYS_TRACE_TOPIC
sample_notice_topic`,
		"getBrokerConfig": `============127.0.0.1:10911============
brokerName                                        =  broker-a
brokerRole                                        =  ASYNC_MASTER
transactionCheckInterval                          =  30000
traceTopicEnable                                  =  true
autoCreateTopicEnable                             =  false`,
		"getNamesrvConfig": `============127.0.0.1:9876============
rocketmqHome                                      =  /opt/rocketmq
clusterTest                                       =  false`,
	})
	provider := &MQAdminProvider{NameServer: "127.0.0.1:9876", CommandRunner: runner}

	report, err := provider.ClusterFeatures(context.Background())
	if err != nil {
		t.Fatalf("ClusterFeatures returned error: %v", err)
	}
	if report.BrokerCount != 1 || len(report.BrokerConfigs) != 1 || len(report.NameServerConfigs) != 1 {
		t.Fatalf("unexpected feature report: %#v", report)
	}
	capabilities := make(map[string]FeatureCapability)
	for _, capability := range report.Capabilities {
		capabilities[capability.Key] = capability
	}
	if capabilities["transaction"].Status != "supported" || capabilities["trace"].Status != "enabled" || capabilities["autoCreateTopic"].Status != "disabled" {
		t.Fatalf("unexpected capabilities: %#v", report.Capabilities)
	}
	if runner.countCommand("getBrokerConfig") != 1 || runner.countCommand("getNamesrvConfig") != 1 {
		t.Fatalf("expected config commands, calls=%#v", runner.commands())
	}
	brokerCall := runner.firstCommand("getBrokerConfig")
	if broker := stringArgForTest(t, brokerCall, "-b"); broker != "127.0.0.1:10911" {
		t.Fatalf("expected broker config address, got %s in %#v", broker, brokerCall)
	}
	nameServerCall := runner.firstCommand("getNamesrvConfig")
	if nameServer := stringArgForTest(t, nameServerCall, "-n"); nameServer != "127.0.0.1:9876" {
		t.Fatalf("expected namesrv config address, got %s in %#v", nameServer, nameServerCall)
	}
}

func TestSearchMessageByKeyUsesNarrowDefaultWindow(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"queryMsgByKey":     "7F00000100002A9F00000000000123AB 3 10240\n",
		"topicStatus":       topicStatusOutputForTest("broker-a", 3, 0, 10241),
		"queryMsgByOffset":  messageDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": "",
	})
	provider := &MQAdminProvider{NameServer: "127.0.0.1:9876", CommandRunner: runner}

	_, err := provider.MessageChain(context.Background(), MessageQuery{
		Topic: "sample_notice_topic",
		Key:   "user-10001",
	})
	if err != nil {
		t.Fatalf("MessageChain returned error: %v", err)
	}
	call := runner.firstCommand("queryMsgByKey")
	begin := int64ArgForTest(t, call, "-b")
	end := int64ArgForTest(t, call, "-e")
	if end-begin > (2*time.Hour + time.Minute).Milliseconds() {
		t.Fatalf("expected key search default window to stay near 2h, begin=%d end=%d diff=%d", begin, end, end-begin)
	}
	if maxNum := stringArgForTest(t, call, "-m"); maxNum != "16" {
		t.Fatalf("expected default key search maxNum=16, got %s in %#v", maxNum, call)
	}
}

func TestMessageChainKeyQueryUsesCandidateQueueOffsetForDetail(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"queryMsgByKey":     "0AE97A6A00017F3CA64A23D49A900003 3 10240\n",
		"topicStatus":       topicStatusOutputForTest("broker-a", 3, 0, 10241),
		"queryMsgByOffset":  messageDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": "",
	})
	runner.errByCommand["queryMsgById"] = errors.New("queryMsgById should not be called for key candidate detail")
	provider := &MQAdminProvider{NameServer: "127.0.0.1:9876", CommandRunner: runner}

	chain, err := provider.MessageChain(context.Background(), MessageQuery{
		Topic: "sample_notice_topic",
		Key:   "user-10001",
	})
	if err != nil {
		t.Fatalf("MessageChain returned error: %v", err)
	}
	if chain.MessageID != "7F00000100002A9F00000000000123AB" {
		t.Fatalf("expected detail OffsetID from queryMsgByOffset, got %s", chain.MessageID)
	}
	if len(chain.Candidates) != 1 || chain.Candidates[0].MessageID != "0AE97A6A00017F3CA64A23D49A900003" {
		t.Fatalf("expected original key candidate to remain visible, got %#v", chain.Candidates)
	}
	if runner.countCommand("queryMsgById") != 0 {
		t.Fatalf("expected queryMsgById to be skipped, calls=%#v", runner.commands())
	}
	offsetCall := runner.firstCommand("queryMsgByOffset")
	if broker := stringArgForTest(t, offsetCall, "-b"); broker != "broker-a" {
		t.Fatalf("expected broker-a from topicStatus, got %s in %#v", broker, offsetCall)
	}
	if queue := stringArgForTest(t, offsetCall, "-i"); queue != "3" {
		t.Fatalf("expected queue 3 from key candidate, got %s in %#v", queue, offsetCall)
	}
	if offset := stringArgForTest(t, offsetCall, "-o"); offset != "10240" {
		t.Fatalf("expected offset 10240 from key candidate, got %s in %#v", offset, offsetCall)
	}
}

func TestMessageChainKeyQueryUsesExplicitWindowMaxNumAndTraceTopic(t *testing.T) {
	runner := newRecordingCommandRunner(map[string]string{
		"queryMsgByKey":     "7F00000100002A9F00000000000123AB 3 10240\n",
		"topicStatus":       topicStatusOutputForTest("broker-a", 3, 0, 10241),
		"queryMsgByOffset":  messageDetailOutputForTest("7F00000100002A9F00000000000123AB", "sample_notice_topic", 3, 10240),
		"queryMsgTraceById": "",
		"consumerProgress":  "#Topic              #Broker Name  #QID  #Broker Offset  #Consumer Offset  #Client IP           #Diff  #Inflight  #LastTime\n",
	})
	provider := &MQAdminProvider{NameServer: "127.0.0.1:9876", CommandRunner: runner}

	_, err := provider.MessageChain(context.Background(), MessageQuery{
		Topic:          "sample_notice_topic",
		Key:            "user-10001",
		ConsumerGroup:  "CG_NOTICE",
		TraceTopic:     "RMQ_SYS_TRACE_TOPIC",
		BeginTimestamp: 0,
		EndTimestamp:   9223372036854775807,
		MaxNum:         1,
	})
	if err != nil {
		t.Fatalf("MessageChain returned error: %v", err)
	}

	keyCall := runner.firstCommand("queryMsgByKey")
	if begin := stringArgForTest(t, keyCall, "-b"); begin != "0" {
		t.Fatalf("expected explicit begin timestamp, got %s in %#v", begin, keyCall)
	}
	if end := stringArgForTest(t, keyCall, "-e"); end != "9223372036854775807" {
		t.Fatalf("expected explicit end timestamp, got %s in %#v", end, keyCall)
	}
	if maxNum := stringArgForTest(t, keyCall, "-m"); maxNum != "1" {
		t.Fatalf("expected explicit maxNum=1, got %s in %#v", maxNum, keyCall)
	}
	traceCall := runner.firstCommand("queryMsgTraceById")
	if traceTopic := stringArgForTest(t, traceCall, "-t"); traceTopic != "RMQ_SYS_TRACE_TOPIC" {
		t.Fatalf("expected explicit trace topic, got %s in %#v", traceTopic, traceCall)
	}
	progressCall := runner.firstCommand("consumerProgress")
	if group := stringArgForTest(t, progressCall, "-g"); group != "CG_NOTICE" {
		t.Fatalf("expected consumer group fallback, got %s in %#v", group, progressCall)
	}
}

func TestRunUsesSidecarBeforeProcessRunnerAndFallsBackWhenUnavailable(t *testing.T) {
	fallback := newRecordingCommandRunner(map[string]string{"clusterList": "fallback-output"})
	provider := &MQAdminProvider{
		NameServer:       "127.0.0.1:9876",
		CommandRunner:    fallback,
		SidecarEnabled:   true,
		SidecarTransport: commandRunnerFunc(func(ctx context.Context, args ...string) (string, error) { return "sidecar-output", nil }),
		SidecarTimeout:   time.Second,
		MessageCacheTTL:  time.Hour,
		SidecarAddr:      "127.0.0.1:18091",
		SidecarMainClass: "dev.codex.rocketmq.AdminSidecar",
		SidecarClasspath: "sidecar.jar",
	}

	output, err := provider.run(context.Background(), "clusterList", "-n", "127.0.0.1:9876")
	if err != nil || output != "sidecar-output" {
		t.Fatalf("expected sidecar output, output=%q err=%v", output, err)
	}
	if fallback.countCommand("clusterList") != 0 {
		t.Fatalf("expected process fallback to stay unused, calls=%#v", fallback.commands())
	}

	provider.SidecarTransport = commandRunnerFunc(func(ctx context.Context, args ...string) (string, error) {
		return "", errSidecarUnavailable
	})
	output, err = provider.run(context.Background(), "clusterList", "-n", "127.0.0.1:9876")
	if err != nil || output != "fallback-output" {
		t.Fatalf("expected fallback output after sidecar unavailable, output=%q err=%v", output, err)
	}
	if fallback.countCommand("clusterList") != 1 {
		t.Fatalf("expected one fallback command, calls=%#v", fallback.commands())
	}
}

func TestAdminSidecarClientPostsRunRequest(t *testing.T) {
	var received []string
	workDir := t.TempDir()
	originalWorkDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get original cwd: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("chdir temp cwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(originalWorkDir); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/run" || r.Method != http.MethodPost {
			t.Fatalf("unexpected sidecar request %s %s", r.Method, r.URL.Path)
		}
		var body adminSidecarRunRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode sidecar request: %v", err)
		}
		received = body.Args
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":"sidecar-output","files":[{"path":"1781664999000/client-a","content":"running-info"}]}`))
	}))
	defer server.Close()

	client := AdminSidecarClient{BaseURL: server.URL, HTTPClient: server.Client()}
	output, err := client.Run(context.Background(), "queryMsgById", "-n", "127.0.0.1:9876")
	if err != nil {
		t.Fatalf("sidecar run: %v", err)
	}
	if output != "sidecar-output" {
		t.Fatalf("unexpected sidecar output %q", output)
	}
	expected := []string{"queryMsgById", "-n", "127.0.0.1:9876"}
	if !reflect.DeepEqual(received, expected) {
		t.Fatalf("sidecar args mismatch\nexpected=%#v\nactual=%#v", expected, received)
	}
	written, err := os.ReadFile("1781664999000/client-a")
	if err != nil {
		t.Fatalf("read sidecar file output: %v", err)
	}
	if string(written) != "running-info" {
		t.Fatalf("sidecar file content mismatch: %q", written)
	}
}

type commandErrorForTest struct {
	message string
}

func (e *commandErrorForTest) Error() string {
	return e.message
}

type commandRunnerFunc func(ctx context.Context, args ...string) (string, error)

func (f commandRunnerFunc) Run(ctx context.Context, args ...string) (string, error) {
	return f(ctx, args...)
}

type recordingCommandRunner struct {
	outputByCommand map[string]string
	errByCommand    map[string]error
	calls           [][]string
}

func newRecordingCommandRunner(outputByCommand map[string]string) *recordingCommandRunner {
	return &recordingCommandRunner{outputByCommand: outputByCommand, errByCommand: make(map[string]error)}
}

func (r *recordingCommandRunner) Run(ctx context.Context, args ...string) (string, error) {
	copied := append([]string(nil), args...)
	r.calls = append(r.calls, copied)
	if len(args) == 0 {
		return "", errors.New("empty command")
	}
	command := args[0]
	if err := r.errByCommand[command]; err != nil {
		return "", err
	}
	return r.outputByCommand[command], nil
}

func (r *recordingCommandRunner) countCommand(command string) int {
	count := 0
	for _, call := range r.calls {
		if len(call) > 0 && call[0] == command {
			count++
		}
	}
	return count
}

func (r *recordingCommandRunner) firstCommand(command string) []string {
	for _, call := range r.calls {
		if len(call) > 0 && call[0] == command {
			return call
		}
	}
	return nil
}

func (r *recordingCommandRunner) commands() [][]string {
	return append([][]string(nil), r.calls...)
}

func messageDetailOutputForTest(messageID string, topic string, queueID int, queueOffset int64) string {
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

func topicStatusOutputForTest(brokerName string, queueID int, minOffset int64, maxOffset int64) string {
	return "#Broker Name                      #QID  #Min Offset           #Max Offset             #Last Updated\n" +
		brokerName + "                          " + strconv.Itoa(queueID) + "     " + strconv.FormatInt(minOffset, 10) + "                     " + strconv.FormatInt(maxOffset, 10) + "                  2026-06-05 16:20:48,715\n"
}

func stringArgForTest(t *testing.T, args []string, name string) string {
	t.Helper()
	for index, arg := range args {
		if arg == name && index+1 < len(args) {
			return args[index+1]
		}
	}
	t.Fatalf("arg %s not found in %#v", name, args)
	return ""
}

func int64ArgForTest(t *testing.T, args []string, name string) int64 {
	t.Helper()
	value, err := strconv.ParseInt(stringArgForTest(t, args, name), 10, 64)
	if err != nil {
		t.Fatalf("parse arg %s: %v", name, err)
	}
	return value
}

func TestCollectTopicMessagesByOffsetReusesPreviousOffsets(t *testing.T) {
	query := MessageBrowseQuery{Topic: "codex_topic", QueueID: -1, Limit: 3}
	rows := []TopicStatusRow{
		{BrokerName: "broker-a", QueueID: 0, MinOffset: 0, MaxOffset: 3},
	}
	previous := TopicMessages{
		Topic: "codex_topic",
		Rows: []MessageDetail{
			{
				MessageID:      "cached-2",
				Topic:          "codex_topic",
				BrokerName:     "broker-a",
				QueueID:        0,
				QueueOffset:    2,
				StoreTimestamp: 300,
			},
		},
	}
	fetchedOffsets := make([]int64, 0)
	result, err := collectTopicMessagesByOffset(context.Background(), query, rows, previous, func(ctx context.Context, topic string, brokerName string, queueID int, offset int64) (MessageDetail, error) {
		fetchedOffsets = append(fetchedOffsets, offset)
		return MessageDetail{
			MessageID:      "fetched-" + strconv.FormatInt(offset, 10),
			Topic:          topic,
			BrokerName:     brokerName,
			QueueID:        queueID,
			QueueOffset:    offset,
			StoreTimestamp: 100 + offset,
		}, nil
	})
	if err != nil {
		t.Fatalf("collect messages: %v", err)
	}
	if !reflect.DeepEqual(fetchedOffsets, []int64{1, 0}) {
		t.Fatalf("expected only missing offsets to be fetched, got %#v", fetchedOffsets)
	}
	if result.ScannedOffsets != 3 || result.ReusedOffsets != 1 || result.FetchedOffsets != 2 {
		t.Fatalf("unexpected offset counters: %#v", result)
	}
	if len(result.Rows) != 3 || result.Rows[0].MessageID != "cached-2" {
		t.Fatalf("expected cached newest message to be reused in rows, got %#v", result.Rows)
	}
}

func TestCollectTopicMessagesByOffsetKeepsBrokerQueueFilter(t *testing.T) {
	query := MessageBrowseQuery{Topic: "codex_topic", BrokerName: "broker-a", QueueID: 1, Limit: 2}
	rows := []TopicStatusRow{
		{BrokerName: "broker-a", QueueID: 1, MinOffset: 3, MaxOffset: 6},
	}
	fetched := make([]string, 0)
	result, err := collectTopicMessagesByOffset(context.Background(), query, rows, TopicMessages{}, func(ctx context.Context, topic string, brokerName string, queueID int, offset int64) (MessageDetail, error) {
		fetched = append(fetched, brokerName+"/"+strconv.Itoa(queueID)+"/"+strconv.FormatInt(offset, 10))
		return MessageDetail{
			MessageID:      "msg-" + strconv.FormatInt(offset, 10),
			Topic:          topic,
			BrokerName:     brokerName,
			QueueID:        queueID,
			QueueOffset:    offset,
			StoreTimestamp: 100 + offset,
		}, nil
	})
	if err != nil {
		t.Fatalf("collect messages: %v", err)
	}
	if !reflect.DeepEqual(fetched, []string{"broker-a/1/5", "broker-a/1/4"}) {
		t.Fatalf("expected only the filtered queue latest offsets, got %#v", fetched)
	}
	if result.BrokerName != "broker-a" || result.QueueID != 1 || result.Limit != 2 {
		t.Fatalf("expected result to keep browse filter, got %#v", result)
	}
	if len(result.Rows) != 2 || result.Rows[0].QueueID != 1 || result.Rows[1].QueueOffset != 4 {
		t.Fatalf("unexpected filtered rows: %#v", result.Rows)
	}
}

func TestBuildUpsertTopicArgsUsesClusterTargetAndQueueOptions(t *testing.T) {
	args, err := buildUpsertTopicArgs("127.0.0.1:9876", TopicConfigMutation{
		Topic:          "codex_topic",
		ClusterName:    "DefaultCluster",
		ReadQueueNums:  4,
		WriteQueueNums: 4,
		Perm:           6,
		Order:          true,
		Attributes:     "+message.type=NORMAL",
	})
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	expected := []string{
		"updateTopic",
		"-n", "127.0.0.1:9876",
		"-t", "codex_topic",
		"-r", "4",
		"-w", "4",
		"-p", "6",
		"-a", "+message.type=NORMAL",
		"-o", "true",
		"-c", "DefaultCluster",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, args)
	}
}

func TestBuildUpsertTopicArgsUsesBrokerTargetAndDefaults(t *testing.T) {
	args, err := buildUpsertTopicArgs("127.0.0.1:9876", TopicConfigMutation{
		Topic:      "codex_topic",
		BrokerAddr: "127.0.0.1:10911",
	})
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	expected := []string{
		"updateTopic",
		"-n", "127.0.0.1:9876",
		"-t", "codex_topic",
		"-r", "8",
		"-w", "8",
		"-p", "6",
		"-b", "127.0.0.1:10911",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, args)
	}
}

func TestBuildDeleteTopicArgsRequiresClusterTarget(t *testing.T) {
	args, err := buildDeleteTopicArgs("127.0.0.1:9876", TopicDeleteRequest{
		Topic:       "codex_topic",
		ClusterName: "DefaultCluster",
	})
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	expected := []string{"deleteTopic", "-n", "127.0.0.1:9876", "-t", "codex_topic", "-c", "DefaultCluster"}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, args)
	}
}

func TestBuildSendTopicMessageArgsUsesOfficialOptions(t *testing.T) {
	queueID := 1
	args, err := buildSendTopicMessageArgs("127.0.0.1:9876", TopicMessageSendRequest{
		Topic:       "codex_topic",
		Body:        "{\"hello\":\"rocketmq\"}",
		Keys:        "codex-key",
		Tags:        "qa",
		BrokerName:  "broker-a",
		QueueID:     &queueID,
		TraceEnable: true,
	})
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	expected := []string{
		"sendMessage",
		"-n", "127.0.0.1:9876",
		"-t", "codex_topic",
		"-p", "{\"hello\":\"rocketmq\"}",
		"-k", "codex-key",
		"-c", "qa",
		"-b", "broker-a",
		"-i", "1",
		"-m", "true",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, args)
	}
}

func TestParseSendTopicMessageResultExtractsMessageID(t *testing.T) {
	output := `#Broker Name                       #QID  #Send Result           #MsgId
broker-a                          1     SEND_OK               ACA8015D00002A9F00000000971701F5`

	result := parseSendTopicMessageResult("codex_topic", output)
	if result.MessageID != "ACA8015D00002A9F00000000971701F5" || result.BrokerName != "broker-a" || result.QueueID != 1 {
		t.Fatalf("unexpected send result: %#v", result)
	}
	if result.SendStatus != "SEND_OK" {
		t.Fatalf("unexpected send status: %#v", result)
	}
}

func TestBuildResetConsumerOffsetArgsUsesTimestampAndForce(t *testing.T) {
	args, err := buildResetConsumerOffsetArgs("127.0.0.1:9876", ConsumerOffsetResetRequest{
		Group:     "codex-group",
		Topic:     "codex_topic",
		Timestamp: "now",
		Force:     true,
	})
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	expected := []string{
		"resetOffsetByTime",
		"-n", "127.0.0.1:9876",
		"-g", "codex-group",
		"-t", "codex_topic",
		"-s", "now",
		"-f", "true",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, args)
	}
}

func TestBuildResetConsumerOffsetArgsUsesQueueTarget(t *testing.T) {
	queueID := 2
	offset := int64(42)
	args, err := buildResetConsumerOffsetArgs("127.0.0.1:9876", ConsumerOffsetResetRequest{
		Group:      "codex-group",
		Topic:      "codex_topic",
		Timestamp:  "1717651200000",
		Force:      false,
		BrokerAddr: "127.0.0.1:10911",
		QueueID:    &queueID,
		Offset:     &offset,
	})
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	expected := []string{
		"resetOffsetByTime",
		"-n", "127.0.0.1:9876",
		"-g", "codex-group",
		"-t", "codex_topic",
		"-s", "1717651200000",
		"-f", "false",
		"-b", "127.0.0.1:10911",
		"-q", "2",
		"-o", "42",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("args mismatch\nexpected=%#v\nactual=%#v", expected, args)
	}
}

func TestBuildUpsertTopicArgsRejectsAmbiguousTarget(t *testing.T) {
	_, err := buildUpsertTopicArgs("127.0.0.1:9876", TopicConfigMutation{
		Topic:       "codex_topic",
		ClusterName: "DefaultCluster",
		BrokerAddr:  "127.0.0.1:10911",
	})
	if err == nil {
		t.Fatalf("expected ambiguous target error")
	}
}
