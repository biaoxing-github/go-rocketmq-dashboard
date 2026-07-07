package rocketmq

import (
	"context"
	"errors"
	"time"
)

// SampleProvider 只用于本地 UI 预览或 mqadmin 不可用时的开发验证，不作为生产默认 Provider。
type SampleProvider struct{}

// ClusterList 返回固定样例，帮助前端在没有 MQ 连接时继续开发布局。
func (p SampleProvider) ClusterList(ctx context.Context) ([]Cluster, error) {
	return []Cluster{{
		Name: "DefaultCluster",
		Brokers: []Broker{{
			Cluster:   "DefaultCluster",
			Name:      "broker-a",
			ID:        "0",
			Address:   "127.0.0.1:10911",
			Version:   "V5_2_0",
			InTPS:     "0.00(0,0ms)",
			OutTPS:    "0.00(0,0ms|N,Nms)",
			Activated: true,
		}},
	}}, nil
}

// BrokerStatus 返回样例 Broker 运行指标，帮助 Broker 面在无 mqadmin 时展示摘要和指标表。
func (p SampleProvider) BrokerStatus(ctx context.Context, brokerAddr string) (BrokerStatus, error) {
	if brokerAddr == "" {
		brokerAddr = "127.0.0.1:10911"
	}
	return BrokerStatus{
		BrokerAddr:         brokerAddr,
		BrokerVersionDesc:  "V5_2_0",
		BrokerRole:         "ASYNC_MASTER",
		BootTimestamp:      "2026-06-07 09:28:00",
		PutTps:             "1.00 0.50 0.10",
		GetFoundTps:        "0.80 0.40 0.08",
		GetTotalTps:        "0.90 0.50 0.10",
		CommitLogCapacity:  "Total : 100 GiB, Free : 88 GiB.",
		DispatchBehind:     "0",
		RuntimeDescription: "ASYNC_MASTER · V5_2_0 · PUT 1.00 0.50 0.10 · GET 0.80 0.40 0.08",
		Metrics: []BrokerRuntimeMetric{
			{Key: "brokerRole", Value: "ASYNC_MASTER"},
			{Key: "brokerVersionDesc", Value: "V5_2_0"},
			{Key: "bootTimestamp", Value: "2026-06-07 09:28:00"},
			{Key: "putTps", Value: "1.00 0.50 0.10"},
			{Key: "getFoundTps", Value: "0.80 0.40 0.08"},
			{Key: "getTotalTps", Value: "0.90 0.50 0.10"},
			{Key: "commitLogDirCapacity", Value: "Total : 100 GiB, Free : 88 GiB."},
			{Key: "dispatchBehindBytes", Value: "0"},
		},
	}, nil
}

// ClusterFeatures 返回样例能力画像，帮助前端在无 MQ 环境下展示配置页布局。
func (p SampleProvider) ClusterFeatures(ctx context.Context) (ClusterFeatureReport, error) {
	clusters, _ := p.ClusterList(ctx)
	topics, _ := p.TopicList(ctx)
	topics = append(topics,
		Topic{Name: "RMQ_SYS_TRANS_HALF_TOPIC", Kind: "system"},
		Topic{Name: "RMQ_SYS_TRANS_OP_HALF_TOPIC", Kind: "system"},
		Topic{Name: "RMQ_SYS_TRACE_TOPIC", Kind: "system"},
		Topic{Name: "SCHEDULE_TOPIC_XXXX", Kind: "system"},
		Topic{Name: "rmq_sys_wheel_timer", Kind: "system"},
	)
	entries := []ConfigEntry{
		{Key: "brokerClusterName", Value: "DefaultCluster"},
		{Key: "brokerName", Value: "broker-a"},
		{Key: "brokerId", Value: "0"},
		{Key: "brokerRole", Value: "ASYNC_MASTER"},
		{Key: "brokerPermission", Value: "6"},
		{Key: "flushDiskType", Value: "ASYNC_FLUSH"},
		{Key: "messageDelayLevel", Value: "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"},
		{Key: "transactionCheckInterval", Value: "30000"},
		{Key: "transactionCheckMax", Value: "15"},
		{Key: "traceTopicEnable", Value: "true"},
		{Key: "traceOn", Value: "true"},
		{Key: "msgTraceTopicName", Value: "RMQ_SYS_TRACE_TOPIC"},
		{Key: "timerStopEnqueue", Value: "false"},
		{Key: "autoCreateTopicEnable", Value: "true"},
		{Key: "autoCreateSubscriptionGroup", Value: "true"},
		{Key: "slaveReadEnable", Value: "false"},
		{Key: "useTLS", Value: "false"},
		{Key: "authenticationEnabled", Value: "false"},
		{Key: "authorizationEnabled", Value: "false"},
		{Key: "popInvisibleTime", Value: "60000"},
	}
	brokerConfigs := []BrokerConfigSnapshot{
		BrokerConfigSnapshotFromEntries(clusters[0].Brokers[0], entries),
	}
	nameServerConfigs := []NameServerConfigSnapshot{{
		NameServer: "127.0.0.1:9876",
		Entries: []ConfigEntry{
			{Key: "rocketmqHome", Value: "/opt/rocketmq"},
			{Key: "clusterTest", Value: "false"},
		},
	}}
	report := BuildClusterFeatureReport("127.0.0.1:9876", clusters, topics, brokerConfigs, nameServerConfigs, nil)
	halfStatus := sampleTransactionTopicStatus("RMQ_SYS_TRANS_HALF_TOPIC", 18, 7, "2026-07-06 10:08:00,000")
	opStatus := sampleTransactionTopicStatus("RMQ_SYS_TRANS_OP_HALF_TOPIC", 42, 12, "2026-07-06 10:10:00,000")
	halfMessages := []MessageDetail{
		{MessageID: "sample-trans-half-oldest", Topic: "RMQ_SYS_TRANS_HALF_TOPIC", BrokerName: "broker-a", QueueID: 0, QueueOffset: 11, StoreTimestamp: time.Now().Add(-38 * time.Minute).UnixMilli(), BodyPreview: "{\"orderId\":\"T10001\"}"},
		{MessageID: "sample-trans-half-newer", Topic: "RMQ_SYS_TRANS_HALF_TOPIC", BrokerName: "broker-a", QueueID: 1, QueueOffset: 13, StoreTimestamp: time.Now().Add(-8 * time.Minute).UnixMilli(), BodyPreview: "{\"orderId\":\"T10002\"}"},
	}
	report.TransactionRuntime = BuildTransactionRuntimeReport(&halfStatus, &opStatus, halfMessages, []MessageDetail{
		{MessageID: "sample-trans-op-commit", Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", BrokerName: "broker-a", QueueID: 0, QueueOffset: 41, StoreTimestamp: time.Now().Add(-2 * time.Minute).UnixMilli(), BodyPreview: "COMMIT_MESSAGE"},
		{MessageID: "sample-trans-op-rollback", Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", BrokerName: "broker-a", QueueID: 1, QueueOffset: 40, StoreTimestamp: time.Now().Add(-6 * time.Minute).UnixMilli(), BodyPreview: "ROLLBACK_MESSAGE"},
		{MessageID: "sample-trans-op-cleanup", Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", BrokerName: "broker-a", QueueID: 2, QueueOffset: 39, StoreTimestamp: time.Now().Add(-12 * time.Minute).UnixMilli(), BodyPreview: "d"},
	}, BuildTransactionConsumerImpact([]ConsumerGroup{
		{Name: "sample-order-events-consumer", Online: true, DiffTotal: 12},
	}, topics), nil)
	return report, nil
}

// TopicList 返回样例 Topic 列表。
func (p SampleProvider) TopicList(ctx context.Context) ([]Topic, error) {
	return []Topic{
		{Name: "sample_notice_topic", Kind: "normal"},
		{Name: "sample_order_events_topic", Kind: "normal"},
		{Name: "%RETRY%sample-order-events-consumer", Kind: "retry"},
		{Name: "%DLQ%sample-order-events-consumer", Kind: "dlq"},
	}, nil
}

// TopicRoute 返回样例 Topic 路由，帮助 Topic 页面在无 mqadmin 时展示路由详情布局。
func (p SampleProvider) TopicRoute(ctx context.Context, topic string) (TopicRoute, error) {
	return TopicRoute{
		Topic:            topic,
		TotalReadQueues:  4,
		TotalWriteQueues: 4,
		Queues: []TopicQueueRoute{{
			BrokerName:      "broker-a",
			ReadQueueNums:   4,
			WriteQueueNums:  4,
			Perm:            6,
			PermissionLabel: "RW",
			TopicSysFlag:    0,
		}},
		Brokers: []TopicBrokerRoute{{
			Cluster:    "DefaultCluster",
			BrokerName: "broker-a",
			Addrs:      map[string]string{"0": "127.0.0.1:10911"},
		}},
	}, nil
}

// TopicStatus 返回样例 Topic 队列状态，帮助 Topic 页面在无 mqadmin 时展示位点明细。
func (p SampleProvider) TopicStatus(ctx context.Context, topic string) (TopicStatus, error) {
	return TopicStatus{
		Topic:             topic,
		TotalQueues:       4,
		TotalMessageCount: 5,
		Rows: []TopicStatusRow{
			{BrokerName: "broker-a", QueueID: 0, MinOffset: 0, MaxOffset: 1, MessageCount: 1, LastUpdated: "2026-06-05 16:20:48,715"},
			{BrokerName: "broker-a", QueueID: 1, MinOffset: 0, MaxOffset: 2, MessageCount: 2, LastUpdated: "2026-06-06 23:45:34,278"},
			{BrokerName: "broker-a", QueueID: 2, MinOffset: 0, MaxOffset: 0, MessageCount: 0, LastUpdated: ""},
			{BrokerName: "broker-a", QueueID: 3, MinOffset: 0, MaxOffset: 2, MessageCount: 2, LastUpdated: "2026-06-05 22:04:56,759"},
		},
	}, nil
}

func sampleTransactionTopicStatus(topic string, maxOffset int64, count int64, lastUpdated string) TopicStatus {
	return TopicStatus{
		Topic:             topic,
		TotalQueues:       3,
		TotalMessageCount: count,
		MinOffsetTotal:    maxOffset - count,
		MaxOffsetTotal:    maxOffset,
		Rows: []TopicStatusRow{
			{BrokerName: "broker-a", QueueID: 0, MinOffset: maxOffset - count, MaxOffset: maxOffset - count + 2, MessageCount: 2, LastUpdated: lastUpdated},
			{BrokerName: "broker-a", QueueID: 1, MinOffset: maxOffset - count + 2, MaxOffset: maxOffset - count + 5, MessageCount: 3, LastUpdated: lastUpdated},
			{BrokerName: "broker-a", QueueID: 2, MinOffset: maxOffset - count + 5, MaxOffset: maxOffset, MessageCount: count - 5, LastUpdated: lastUpdated},
		},
	}
}

// TopicMessages 返回样例 Topic 消息，帮助前端验证“Topic -> 消息 -> 链路”的点击流程。
func (p SampleProvider) TopicMessages(ctx context.Context, query MessageBrowseQuery) (TopicMessages, error) {
	topic := query.Topic
	if topic == "" {
		topic = "sample_notice_topic"
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 12
	}
	now := time.Now().UnixMilli()
	rows := []MessageDetail{
		{
			MessageID:      "sample-message-001",
			Topic:          topic,
			BrokerName:     "broker-a",
			Keys:           []string{"sample-key-001"},
			StoreTimestamp: now - 1200,
			QueueID:        0,
			QueueOffset:    2,
			StoreHost:      "127.0.0.1:10911",
			BodyPreview:    "{\"event\":\"order_created\"}",
		},
		{
			MessageID:      "sample-message-002",
			Topic:          topic,
			BrokerName:     "broker-a",
			Keys:           []string{"sample-key-002"},
			StoreTimestamp: now - 8200,
			QueueID:        1,
			QueueOffset:    1,
			StoreHost:      "127.0.0.1:10911",
			BodyPreview:    "{\"event\":\"notice_created\"}",
		},
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return TopicMessages{
		Topic:          topic,
		BrokerName:     query.BrokerName,
		QueueID:        query.QueueID,
		Limit:          limit,
		ScannedOffsets: len(rows),
		Rows:           rows,
	}, nil
}

// UpsertTopic 返回样例成功结果，便于无 mqadmin 环境下验证写表单和响应结构。
func (p SampleProvider) UpsertTopic(ctx context.Context, request TopicConfigMutation) (TopicMutationResult, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return TopicMutationResult{}, err
	}
	return TopicMutationResult{
		Topic:     request.Topic,
		Operation: "upsertTopic",
		Target:    request.TargetLabel(),
		Output:    "sample provider topic updated",
	}, nil
}

// DeleteTopic 返回样例成功结果，便于无 mqadmin 环境下验证删除流程。
func (p SampleProvider) DeleteTopic(ctx context.Context, request TopicDeleteRequest) (TopicMutationResult, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return TopicMutationResult{}, err
	}
	return TopicMutationResult{
		Topic:     request.Topic,
		Operation: "deleteTopic",
		Target:    request.ClusterName,
		Output:    "sample provider topic deleted",
	}, nil
}

// SendTopicMessage 返回样例发送结果，便于无 mqadmin 环境下验证发消息表单和链路跳转。
func (p SampleProvider) SendTopicMessage(ctx context.Context, request TopicMessageSendRequest) (TopicMessageSendResult, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return TopicMessageSendResult{}, err
	}
	return TopicMessageSendResult{
		Topic:      request.Topic,
		Operation:  "sendMessage",
		BrokerName: "broker-a",
		QueueID:    0,
		SendStatus: "SEND_OK",
		MessageID:  "sample-sent-message-001",
		Output:     "sample provider message sent",
	}, nil
}

// ConsumerGroups 返回样例消费者组列表。
func (p SampleProvider) ConsumerGroups(ctx context.Context) ([]ConsumerGroup, error) {
	return []ConsumerGroup{
		{Name: "sample-order-events-consumer", Count: 1, Version: "V5_3_2", Type: "PUSH", Model: "CLUSTERING", TPS: "0", DiffTotal: 0, Online: true},
		{Name: "sample-offline-consumer", Count: 0, Version: "OFFLINE", TPS: "0", DiffTotal: 22653, Online: false},
	}, nil
}

// ConsumerDetail 返回样例消费者详情，帮助 Consumer 页面在无 mqadmin 时展示连接和位点结构。
func (p SampleProvider) ConsumerDetail(ctx context.Context, group string, topic string) (ConsumerDetail, error) {
	if topic == "" {
		topic = "sample_order_events_topic"
	}
	return ConsumerDetail{
		Group:            group,
		Topic:            topic,
		ConsumeType:      "CONSUME_PASSIVELY",
		MessageModel:     "CLUSTERING",
		ConsumeFromWhere: "CONSUME_FROM_LAST_OFFSET",
		Connections: []ConsumerConnection{{
			ClientID:   "127.0.0.1@1#sample",
			ClientAddr: "127.0.0.1:44832",
			Language:   "JAVA",
			Version:    "V5_3_2",
		}},
		Subscriptions: []ConsumerSubscription{{
			Topic:      topic,
			Expression: "order_created",
		}},
		ProgressRows: []ConsumerProgressRow{{
			Topic:          topic,
			BrokerName:     "broker-a",
			QueueID:        0,
			BrokerOffset:   1,
			ConsumerOffset: 1,
			ClientIP:       "127.0.0.1",
			Diff:           0,
			Inflight:       0,
			LastTime:       "2026-06-05 16:20:48",
		}},
	}, nil
}

// ResetConsumerOffset 返回样例消费点重置结果，便于无 mqadmin 环境下验证写操作回显。
func (p SampleProvider) ResetConsumerOffset(ctx context.Context, request ConsumerOffsetResetRequest) (ConsumerOffsetResetResult, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return ConsumerOffsetResetResult{}, err
	}
	return ConsumerOffsetResetResult{
		Group:     request.Group,
		Topic:     request.Topic,
		Operation: "resetOffsetByTime",
		Timestamp: request.Timestamp,
		Target:    request.TargetLabel(),
		Output:    "sample provider consumer offset reset",
	}, nil
}

// MessageChain 返回一条示例链路，便于前端链路视图先成型。
func (p SampleProvider) MessageChain(ctx context.Context, query MessageQuery) (MessageStatusChain, error) {
	if query.MessageID == "" && query.Key == "" {
		return MessageStatusChain{}, errors.New("messageId 或 key 至少传一个")
	}
	messageID := query.MessageID
	if messageID == "" {
		messageID = "sample-" + query.Key
	}
	now := time.Now().Add(-1200 * time.Millisecond).UnixMilli()
	return BuildMessageStatusChain(
		MessageDetail{
			MessageID:      messageID,
			Topic:          query.Topic,
			Keys:           []string{query.Key},
			StoreTimestamp: now,
		},
		[]TraceEvent{{Stage: "SEND_SUCCESS", Group: "sample-producer", Timestamp: now + 100, Detail: "样例消息写入 broker-a"}},
		[]ConsumerState{{Group: "sample-consumer", Topic: query.Topic, Status: "CONSUMED", Lag: 0}},
	), nil
}
