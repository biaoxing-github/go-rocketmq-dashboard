package rocketmq

import (
	"strings"
	"testing"
)

func TestParseClusterListReadsBrokerVersionAndAddress(t *testing.T) {
	output := `#Cluster Name           #Broker Name            #BID  #Addr                  #Version              #InTPS(LOAD)                   #OutTPS(LOAD)  #Timer(Progress)        #PCWait(ms)  #Hour         #SPACE    #ACTIVATED
DefaultCluster          broker-a                0     127.0.0.1:10911     V5_2_0                 0.00(0,0ms)               0.00(0,0ms|N,Nms)  0-0(0.0w, 0.0, 0.0)               0  1446.72       0.1200          true`

	clusters, err := ParseClusterList(output)
	if err != nil {
		t.Fatalf("ParseClusterList returned error: %v", err)
	}

	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters))
	}
	if clusters[0].Name != "DefaultCluster" {
		t.Fatalf("cluster name mismatch: %s", clusters[0].Name)
	}
	if len(clusters[0].Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(clusters[0].Brokers))
	}

	broker := clusters[0].Brokers[0]
	if broker.Name != "broker-a" || broker.Address != "127.0.0.1:10911" || broker.Version != "V5_2_0" {
		t.Fatalf("unexpected broker parsed: %#v", broker)
	}
	if !broker.Activated {
		t.Fatalf("expected broker activated")
	}
}

func TestBuildMessageStatusChainOrdersLifecycleSteps(t *testing.T) {
	message := MessageDetail{
		MessageID:      "7F00000100002A9F00000000000123AB",
		Topic:          "sample_notice_topic",
		Keys:           []string{"user-10001"},
		StoreTimestamp: 1717651200000,
	}
	traces := []TraceEvent{
		{Stage: "SEND_SUCCESS", Group: "sample-producer", Timestamp: 1717651200000, Detail: "消息已写入 broker-a"},
		{Stage: "CONSUME_SUCCESS", Group: "sample-notice-consumer", Timestamp: 1717651200900, Detail: "消费成功"},
	}
	offsets := []ConsumerState{
		{Group: "sample-notice-consumer", Topic: "sample_notice_topic", Status: "CONSUMED", Lag: 0},
	}

	chain := BuildMessageStatusChain(message, traces, offsets)

	if chain.MessageID != message.MessageID {
		t.Fatalf("message id mismatch: %s", chain.MessageID)
	}
	if len(chain.Steps) != 3 {
		t.Fatalf("expected stored, send trace and consume trace steps, got %d", len(chain.Steps))
	}
	if chain.Steps[0].Stage != "STORED" || chain.Steps[1].Stage != "SEND_SUCCESS" || chain.Steps[2].Stage != "CONSUME_SUCCESS" {
		t.Fatalf("unexpected chain order: %#v", chain.Steps)
	}
	if chain.OverallStatus != "CONSUME_SUCCESS" {
		t.Fatalf("expected CONSUME_SUCCESS overall status, got %s", chain.OverallStatus)
	}
}

func TestBuildMessageStatusChainIgnoresTraceMissingForOverallStatus(t *testing.T) {
	message := MessageDetail{
		MessageID:      "7F00000100002A9F00000000000123AB",
		Topic:          "sample_notice_topic",
		StoreTimestamp: 1717651200000,
	}
	traces := []TraceEvent{
		{Stage: "TRACE_MISSING", Group: "trace", Timestamp: 1717651200000, Detail: "Trace 数据不可用"},
	}

	chain := BuildMessageStatusChain(message, traces, nil)

	if chain.OverallStatus != "STORED" {
		t.Fatalf("expected STORED overall status when only trace is missing, got %s", chain.OverallStatus)
	}
	if len(chain.Steps) != 2 || chain.Steps[1].Stage != "TRACE_MISSING" {
		t.Fatalf("expected stored plus trace missing warning, got %#v", chain.Steps)
	}
}

func TestParseTopicListClassifiesRetryDlqAndSystemTopics(t *testing.T) {
	output := `%RETRY%mb-consumer-group
RMQ_SYS_TRANS_HALF_TOPIC
BenchmarkTest
TBW102
sample_order_events_topic
%DLQ%EVENT_MESSAGES_TOPIC_CONSUMER`

	topics := ParseTopicList(output)

	if len(topics) != 6 {
		t.Fatalf("expected 6 topics, got %d", len(topics))
	}
	cases := map[string]string{
		"%RETRY%mb-consumer-group":           "retry",
		"%DLQ%EVENT_MESSAGES_TOPIC_CONSUMER": "dlq",
		"RMQ_SYS_TRANS_HALF_TOPIC":           "system",
		"TBW102":                             "system",
		"BenchmarkTest":                      "normal",
		"sample_order_events_topic":          "normal",
	}
	for _, topic := range topics {
		if cases[topic.Name] != topic.Kind {
			t.Fatalf("topic %s kind mismatch: got %s want %s", topic.Name, topic.Kind, cases[topic.Name])
		}
	}
}

func TestParseConfigSectionsReadsBrokerAndNameServerConfig(t *testing.T) {
	output := `============Master: 127.0.0.1:10911============
brokerName                                        =  broker-a
traceTopicEnable                                  =  true
transactionCheckInterval=30000

============127.0.0.1:9876============
rocketmqHome                                      =  /opt/rocketmq
clusterTest=false`

	sections, err := ParseConfigSections(output)
	if err != nil {
		t.Fatalf("ParseConfigSections returned error: %v", err)
	}
	if len(sections) != 2 {
		t.Fatalf("expected 2 sections, got %#v", sections)
	}
	if sections[0].Header != "Master: 127.0.0.1:10911" || sections[0].Entries[1].Key != "traceTopicEnable" || sections[0].Entries[1].Value != "true" {
		t.Fatalf("unexpected broker config section: %#v", sections[0])
	}
	if sections[1].Header != "127.0.0.1:9876" || sections[1].Entries[1].Key != "clusterTest" || sections[1].Entries[1].Value != "false" {
		t.Fatalf("unexpected namesrv config section: %#v", sections[1])
	}
}

func TestBuildClusterFeatureReportInfersTransactionsAndTrace(t *testing.T) {
	clusters := []Cluster{{
		Name: "DefaultCluster",
		Brokers: []Broker{{
			Cluster: "DefaultCluster",
			Name:    "broker-a",
			ID:      "0",
			Address: "127.0.0.1:10911",
			Version: "V5_3_2",
		}},
	}}
	topics := []Topic{
		{Name: "RMQ_SYS_TRANS_HALF_TOPIC", Kind: "system"},
		{Name: "RMQ_SYS_TRANS_OP_HALF_TOPIC", Kind: "system"},
		{Name: "RMQ_SYS_TRACE_TOPIC", Kind: "system"},
		{Name: "SCHEDULE_TOPIC_XXXX", Kind: "system"},
	}
	config := BrokerConfigSnapshotFromEntries(clusters[0].Brokers[0], []ConfigEntry{
		{Key: "brokerRole", Value: "ASYNC_MASTER"},
		{Key: "transactionCheckInterval", Value: "30000"},
		{Key: "traceTopicEnable", Value: "true"},
		{Key: "autoCreateTopicEnable", Value: "false"},
	})

	report := BuildClusterFeatureReport("127.0.0.1:9876", clusters, topics, []BrokerConfigSnapshot{config}, nil, nil)

	if report.BrokerCount != 1 || report.SystemTopicCount != 4 || len(report.BrokerConfigs[0].Highlights) == 0 {
		t.Fatalf("unexpected feature report summary: %#v", report)
	}
	capabilities := make(map[string]FeatureCapability)
	for _, capability := range report.Capabilities {
		capabilities[capability.Key] = capability
	}
	if capabilities["transaction"].Status != "supported" {
		t.Fatalf("expected transaction support, got %#v", capabilities["transaction"])
	}
	if capabilities["trace"].Status != "enabled" {
		t.Fatalf("expected trace enabled, got %#v", capabilities["trace"])
	}
	if capabilities["autoCreateTopic"].Status != "disabled" {
		t.Fatalf("expected autoCreateTopic disabled, got %#v", capabilities["autoCreateTopic"])
	}
}

func TestBuildClusterFeatureReportAddsCommonChineseConfigPanels(t *testing.T) {
	clusters := []Cluster{{
		Name: "DefaultCluster",
		Brokers: []Broker{{
			Cluster: "DefaultCluster",
			Name:    "broker-a",
			ID:      "0",
			Address: "127.0.0.1:10911",
			Version: "V5_3_2",
		}},
	}}
	config := BrokerConfigSnapshotFromEntries(clusters[0].Brokers[0], []ConfigEntry{
		{Key: "brokerRole", Value: "ASYNC_MASTER"},
		{Key: "flushDiskType", Value: "ASYNC_FLUSH"},
		{Key: "transactionCheckInterval", Value: "30000"},
		{Key: "transactionCheckMax", Value: "15"},
		{Key: "autoCreateTopicEnable", Value: "false"},
		{Key: "traceTopicEnable", Value: "true"},
	})

	report := BuildClusterFeatureReport("127.0.0.1:9876", clusters, nil, []BrokerConfigSnapshot{config}, nil, nil)

	items := commonConfigItemsForTest(report.CommonConfigPanels)
	if items["transactionCheckInterval"].Label != "事务回查间隔" || !strings.Contains(items["transactionCheckInterval"].Description, "事务半消息") {
		t.Fatalf("expected transactionCheckInterval Chinese explanation, got %#v", items["transactionCheckInterval"])
	}
	if items["autoCreateTopicEnable"].Status != "disabled" || !strings.Contains(items["autoCreateTopicEnable"].Impact, "自动创建") {
		t.Fatalf("expected autoCreateTopicEnable disabled interpretation, got %#v", items["autoCreateTopicEnable"])
	}
	if items["traceTopicEnable"].Status != "enabled" {
		t.Fatalf("expected traceTopicEnable enabled status, got %#v", items["traceTopicEnable"])
	}
}

func TestBuildTransactionRuntimeReportSummarizesQueuesAndOperationSamples(t *testing.T) {
	halfStatus := TopicStatus{
		Topic:             "RMQ_SYS_TRANS_HALF_TOPIC",
		TotalQueues:       1,
		TotalMessageCount: 4,
		MinOffsetTotal:    10,
		MaxOffsetTotal:    14,
		Rows: []TopicStatusRow{{
			BrokerName:   "broker-a",
			QueueID:      0,
			MinOffset:    10,
			MaxOffset:    14,
			MessageCount: 4,
			LastUpdated:  "2026-07-06 10:00:00,000",
		}},
	}
	opStatus := TopicStatus{
		Topic:             "RMQ_SYS_TRANS_OP_HALF_TOPIC",
		TotalQueues:       1,
		TotalMessageCount: 3,
		MinOffsetTotal:    20,
		MaxOffsetTotal:    23,
		Rows: []TopicStatusRow{{
			BrokerName:   "broker-a",
			QueueID:      0,
			MinOffset:    20,
			MaxOffset:    23,
			MessageCount: 3,
			LastUpdated:  "2026-07-06 10:01:00,000",
		}},
	}
	operations := []MessageDetail{
		{MessageID: "commit-msg", Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", BrokerName: "broker-a", QueueID: 0, QueueOffset: 22, StoreTimestamp: 1783303260000, BodyPreview: "COMMIT_MESSAGE"},
		{MessageID: "rollback-msg", Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", BrokerName: "broker-a", QueueID: 0, QueueOffset: 21, StoreTimestamp: 1783303200000, BodyPreview: "ROLLBACK_MESSAGE"},
		{MessageID: "remove-msg", Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", BrokerName: "broker-a", QueueID: 0, QueueOffset: 20, StoreTimestamp: 1783303140000, BodyPreview: "d"},
	}

	report := BuildTransactionRuntimeReport(&halfStatus, &opStatus, operations, nil)

	if !report.Supported || report.HalfTopic.TotalMessageCount != 4 || report.OpTopic.TotalMessageCount != 3 {
		t.Fatalf("expected transaction topic status summary, got %#v", report)
	}
	if report.CommitCount != 1 || report.RollbackCount != 1 || report.CleanupCount != 1 || report.UnknownCount != 0 {
		t.Fatalf("expected operation counts, got %#v", report)
	}
	if len(report.RecentOperations) != 3 || report.RecentOperations[1].Operation != "rollback" {
		t.Fatalf("expected classified operation samples, got %#v", report.RecentOperations)
	}
}

func commonConfigItemsForTest(panels []CommonConfigPanel) map[string]CommonConfigItem {
	items := make(map[string]CommonConfigItem)
	for _, panel := range panels {
		for _, item := range panel.Items {
			items[item.Key] = item
		}
	}
	return items
}

func TestParseTopicRouteReadsJsonRouteData(t *testing.T) {
	output := `{
  "queueDatas": [
    {
      "brokerName": "broker-a",
      "readQueueNums": 4,
      "writeQueueNums": 4,
      "perm": 6,
      "topicSysFlag": 0
    }
  ],
  "brokerDatas": [
    {
      "cluster": "DefaultCluster",
      "brokerName": "broker-a",
      "brokerAddrs": {
        "0": "127.0.0.1:10911"
      }
    }
  ]
}`

	route, err := ParseTopicRoute("sample_order_events_topic", output)
	if err != nil {
		t.Fatalf("ParseTopicRoute returned error: %v", err)
	}
	if route.Topic != "sample_order_events_topic" || route.TotalReadQueues != 4 || route.TotalWriteQueues != 4 {
		t.Fatalf("unexpected route summary: %#v", route)
	}
	if len(route.Queues) != 1 || route.Queues[0].PermissionLabel != "RW" {
		t.Fatalf("unexpected queue route: %#v", route.Queues)
	}
	if len(route.Brokers) != 1 || route.Brokers[0].Addrs["0"] != "127.0.0.1:10911" {
		t.Fatalf("unexpected broker route: %#v", route.Brokers)
	}
}

func TestParseTopicRouteReadsRocketMQ52UnquotedBrokerId(t *testing.T) {
	output := `{
	"brokerDatas":[
		{
			"brokerAddrs":{0:"127.0.0.1:10911"
			},
			"brokerName":"broker-a",
			"cluster":"DefaultCluster",
			"enableActingMaster":false
		}
	],
	"filterServerTable":{},
	"queueDatas":[
		{
			"brokerName":"broker-a",
			"perm":6,
			"readQueueNums":4,
			"topicSysFlag":0,
			"writeQueueNums":4
		}
	]
}`

	route, err := ParseTopicRoute("sample_order_events_topic", output)
	if err != nil {
		t.Fatalf("ParseTopicRoute returned error: %v", err)
	}
	if len(route.Brokers) != 1 || route.Brokers[0].Addrs["0"] != "127.0.0.1:10911" {
		t.Fatalf("unexpected broker route: %#v", route.Brokers)
	}
	if route.TotalReadQueues != 4 || route.TotalWriteQueues != 4 {
		t.Fatalf("unexpected queue totals: %#v", route)
	}
}

func TestParseTopicStatusReadsQueueOffsetsAndLastUpdated(t *testing.T) {
	output := `Java HotSpot(TM) 64-Bit Server VM warning: Option UseConcMarkSweepGC was deprecated by: 9
#Broker Name                      #QID  #Min Offset           #Max Offset             #Last Updated
broker-a                          0     0                     1                       2026-06-05 16:20:48,715
broker-a                          1     0                     2                       2026-06-06 23:45:34,278
broker-a                          2     0                     0
broker-a                          3     0                     2                       2026-06-05 22:04:56,759`

	status, err := ParseTopicStatus("sample_order_events_topic", output)
	if err != nil {
		t.Fatalf("ParseTopicStatus returned error: %v", err)
	}
	if status.Topic != "sample_order_events_topic" || status.TotalQueues != 4 {
		t.Fatalf("unexpected status summary: %#v", status)
	}
	if status.TotalMessageCount != 5 || status.MaxOffsetTotal != 5 || status.MinOffsetTotal != 0 {
		t.Fatalf("unexpected topic totals: %#v", status)
	}
	if status.Rows[1].QueueID != 1 || status.Rows[1].MessageCount != 2 || status.Rows[1].LastUpdated != "2026-06-06 23:45:34,278" {
		t.Fatalf("unexpected second queue row: %#v", status.Rows[1])
	}
	if status.Rows[2].QueueID != 2 || status.Rows[2].LastUpdated != "" {
		t.Fatalf("expected empty last updated to be preserved, got %#v", status.Rows[2])
	}
}

func TestParseTopicStatusReportsCommandException(t *testing.T) {
	output := `org.apache.rocketmq.tools.command.SubCommandException: TopicStatusSubCommand command failed
Caused by: org.apache.rocketmq.remoting.exception.RemotingTimeoutException: wait response on the channel </127.0.0.1:9876> timeout, 4936(ms)`

	_, err := ParseTopicStatus("sample_order_events_topic", output)
	if err == nil || !strings.Contains(err.Error(), "TopicStatusSubCommand command failed") {
		t.Fatalf("expected command failure summary, got %v", err)
	}
}

func TestParseConsumerProgressHandlesOnlineAndOfflineRows(t *testing.T) {
	output := `#Group                                                            #Count  #Version                 #Type  #Model          #TPS     #Diff Total
sample-offline-consumer                                               0       OFFLINE                                         0        22653
sample-order-events-consumer             1       V5_3_2                   PUSH   CLUSTERING      0        0`

	groups, err := ParseConsumerProgress(output)
	if err != nil {
		t.Fatalf("ParseConsumerProgress returned error: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].Name != "sample-offline-consumer" || groups[0].Online || groups[0].DiffTotal != 22653 {
		t.Fatalf("unexpected offline group: %#v", groups[0])
	}
	if groups[1].Name != "sample-order-events-consumer" || !groups[1].Online || groups[1].Version != "V5_3_2" || groups[1].Model != "CLUSTERING" {
		t.Fatalf("unexpected online group: %#v", groups[1])
	}
}

func TestParseConsumerProgressSkipsCommandNoiseRows(t *testing.T) {
	output := `org.apache.rocketmq.tools.command.SubCommandException: ConsumerProgressSubCommand command failed
	at org.apache.rocketmq.tools.command.consumer.ConsumerProgressSubCommand.execute(ConsumerProgressSubCommand.java:207)
#Group                                                            #Count  #Version                 #Type  #Model          #TPS     #Diff Total
sample-offline-consumer                                               0       OFFLINE                                         0        22653
sample-order-events-consumer             1       V5_3_2                   PUSH   CLUSTERING      0        0`

	groups, err := ParseConsumerProgress(output)
	if err != nil {
		t.Fatalf("ParseConsumerProgress returned error: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 valid groups after filtering noise, got %d", len(groups))
	}
	if groups[0].Name != "sample-offline-consumer" || groups[1].Name != "sample-order-events-consumer" {
		t.Fatalf("unexpected groups: %#v", groups)
	}
}

func TestParseConsumerConnectionReadsConnectionsAndSubscriptions(t *testing.T) {
	output := `#ClientId                            #ClientAddr            #Language  #Version
127.0.0.1@1#9790766882943522    127.0.0.1:44832     JAVA       V5_3_2

Below is subscription:
#Topic               #SubExpression
sample_order_events_topic order_created
%RETRY%sample-order-events-consumer *

ConsumeType: CONSUME_PASSIVELY
MessageModel: CLUSTERING
ConsumeFromWhere: CONSUME_FROM_LAST_OFFSET`

	connection, err := ParseConsumerConnection(output)
	if err != nil {
		t.Fatalf("ParseConsumerConnection returned error: %v", err)
	}
	if len(connection.Connections) != 1 || connection.Connections[0].ClientAddr != "127.0.0.1:44832" {
		t.Fatalf("unexpected connections: %#v", connection.Connections)
	}
	if len(connection.Subscriptions) != 2 || connection.Subscriptions[0].Expression != "order_created" {
		t.Fatalf("unexpected subscriptions: %#v", connection.Subscriptions)
	}
	if connection.ConsumeType != "CONSUME_PASSIVELY" || connection.MessageModel != "CLUSTERING" || connection.ConsumeFromWhere != "CONSUME_FROM_LAST_OFFSET" {
		t.Fatalf("unexpected connection metadata: %#v", connection)
	}
}

func TestParseConsumerProgressDetailReadsQueueRowsAndTotals(t *testing.T) {
	output := `#Topic                                                            #Broker Name                      #QID  #Broker Offset        #Consumer Offset      #Client IP           #Diff                #Inflight           #LastTime
sample_order_events_topic                           broker-a                          0     1                     1                     127.0.0.1       0                    0                    2026-06-05 16:20:48
sample_order_events_topic                           broker-a                          1     1                     1                     127.0.0.1       0                    0                    2026-06-05 19:11:19
sample_order_events_topic                           broker-a                          2     0                     0                     127.0.0.1       0                    0                    N/A

Consume TPS: 0.00
Consume Diff Total: 0
Consume Inflight Total: 0`

	progress, err := ParseConsumerProgressDetail(output)
	if err != nil {
		t.Fatalf("ParseConsumerProgressDetail returned error: %v", err)
	}
	if len(progress.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %#v", progress.Rows)
	}
	if progress.Rows[0].ClientIP != "127.0.0.1" || progress.Rows[0].LastTime != "2026-06-05 16:20:48" {
		t.Fatalf("unexpected first row: %#v", progress.Rows[0])
	}
	if progress.ConsumeTPS != 0 || progress.DiffTotal != 0 || progress.InflightTotal != 0 {
		t.Fatalf("unexpected totals: %#v", progress)
	}
}

func TestParseMessageDetailReadsQueryMsgByIdOutput(t *testing.T) {
	output := `OffsetID:            7F00000100002A9F00000000000123AB
Topic:               sample_notice_topic
Tags:                [notice]
Keys:                [user-10001]
Queue ID:            3
Queue Offset:        10240
CommitLog Offset:    2533979436
Reconsume Times:     1
Born Timestamp:      2026-06-06 19:48:01
Store Timestamp:     2026-06-06 19:48:02
Born Host:           10.0.0.8:51111
Store Host:          127.0.0.1:10911
Properties:          {MSG_REGION=DefaultRegion, UNIQ_KEY=0AE97A6A00017F3CA64A23D49A900003, TRACE_ON=true, traceparent=00-f548684403498fc90ec7ccc4ead087c7-02c2b68d0a05b7ee-00}
Message Body:        {"assessmentId":10001,"status":"created"}`

	message, err := ParseMessageDetail(output)
	if err != nil {
		t.Fatalf("ParseMessageDetail returned error: %v", err)
	}
	if message.MessageID != "7F00000100002A9F00000000000123AB" || message.Topic != "sample_notice_topic" {
		t.Fatalf("unexpected message identity: %#v", message)
	}
	if len(message.Keys) != 1 || message.Keys[0] != "user-10001" {
		t.Fatalf("unexpected keys: %#v", message.Keys)
	}
	if message.QueueID != 3 || message.QueueOffset != 10240 || message.ReconsumeTimes != 1 {
		t.Fatalf("unexpected queue fields: %#v", message)
	}
	if message.StoreTimestamp <= 0 || message.BornHost != "10.0.0.8:51111" || message.StoreHost != "127.0.0.1:10911" {
		t.Fatalf("unexpected host/time fields: %#v", message)
	}
	if message.BodyPreview != `{"assessmentId":10001,"status":"created"}` {
		t.Fatalf("unexpected body preview: %s", message.BodyPreview)
	}
	if message.TraceMessageID != "0AE97A6A00017F3CA64A23D49A900003" {
		t.Fatalf("unexpected trace message id: %s", message.TraceMessageID)
	}
	if message.TraceParent != "00-f548684403498fc90ec7ccc4ead087c7-02c2b68d0a05b7ee-00" {
		t.Fatalf("unexpected traceparent: %s", message.TraceParent)
	}
}

func TestParseMessageDetailSkipsNullKeys(t *testing.T) {
	output := `OffsetID:            7F00000100002A9F00000000000123AB
Topic:               sample_notice_topic
Keys:                [null]
Queue ID:            3
Queue Offset:        10240
Reconsume Times:     0
Store Timestamp:     2026-06-06 19:48:02
Message Body:        {}`

	message, err := ParseMessageDetail(output)
	if err != nil {
		t.Fatalf("ParseMessageDetail returned error: %v", err)
	}
	if len(message.Keys) != 0 {
		t.Fatalf("expected null key to be ignored, got %#v", message.Keys)
	}
}

func TestParseMessageDetailKeepsChineseBodyUTF8WhenTruncated(t *testing.T) {
	longName := strings.Repeat("脑", 260)
	output := `OffsetID:            7F00000100002A9F00000000000123AB
Topic:               sample_notice_topic
Keys:                [user-10001]
Queue ID:            3
Queue Offset:        10240
Reconsume Times:     0
Store Timestamp:     2026-06-06 19:48:02
Message Body:        {"qaName":"` + longName + `"}`

	message, err := ParseMessageDetail(output)
	if err != nil {
		t.Fatalf("ParseMessageDetail returned error: %v", err)
	}
	if strings.ContainsRune(message.BodyPreview, '\uFFFD') {
		t.Fatalf("body preview contains replacement rune: %q", message.BodyPreview)
	}
	if len([]rune(message.BodyPreview)) > 512 {
		t.Fatalf("expected body preview to be capped at 512 runes, got %d", len([]rune(message.BodyPreview)))
	}
}

func TestParseMessageSearchResultsReadsQueryMsgByKeyOutput(t *testing.T) {
	output := `#Message ID                                          #QID                                  #Offset
7F00000100002A9F00000000000123AB                       3                                    10240`

	results, err := ParseMessageSearchResults(output)
	if err != nil {
		t.Fatalf("ParseMessageSearchResults returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one message search result, got %d", len(results))
	}
	if results[0].MessageID != "7F00000100002A9F00000000000123AB" || results[0].QueueID != 3 || results[0].QueueOffset != 10240 {
		t.Fatalf("unexpected search result: %#v", results[0])
	}
}

func TestParseTraceEventsReadsPubAndSubRows(t *testing.T) {
	output := `#Type      #ProducerGroup       #ClientHost          #SendTime            #CostTimes #Status
Pub        PG_NOTICE            10.0.0.8             2026-06-06 19:48:01  12ms       success

#Type      #ConsumerGroup       #ClientHost          #ConsumerTime        #CostTimes #Status
Sub        CG_NOTICE            10.0.0.9             2026-06-06 19:48:05  18ms       success`

	events, err := ParseTraceEvents(output)
	if err != nil {
		t.Fatalf("ParseTraceEvents returned error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected two trace events, got %d", len(events))
	}
	if events[0].Stage != "SEND_SUCCESS" || events[0].Group != "PG_NOTICE" {
		t.Fatalf("unexpected producer trace: %#v", events[0])
	}
	if events[1].Stage != "CONSUME_SUCCESS" || events[1].Group != "CG_NOTICE" {
		t.Fatalf("unexpected consumer trace: %#v", events[1])
	}
}

func TestParseConsumerStatesReadsQueueLagRows(t *testing.T) {
	output := `#Topic              #Broker Name  #QID  #Broker Offset  #Consumer Offset  #Client IP           #Diff  #Inflight  #LastTime
sample_metrics_topic     broker-a      3     10241           10239              10.0.0.9             2      0          2026-06-06 19:49:00
Consume TPS: 12.30
Consume Diff Total: 2
Consume Inflight Total: 0`

	states, err := ParseConsumerStates("CG_NOTICE", output)
	if err != nil {
		t.Fatalf("ParseConsumerStates returned error: %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected one consumer state, got %d", len(states))
	}
	if states[0].Group != "CG_NOTICE" || states[0].Topic != "sample_metrics_topic" || states[0].Status != "PENDING" || states[0].Lag != 2 {
		t.Fatalf("unexpected consumer state: %#v", states[0])
	}
}
