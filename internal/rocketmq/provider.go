package rocketmq

import "context"

// Provider 定义 Dashboard 后端需要的 RocketMQ 管理能力。
// 读操作走快照缓存，写操作直接执行官方 mqadmin 命令并在 HTTP 层触发相关快照刷新。
type Provider interface {
	ClusterList(ctx context.Context) ([]Cluster, error)
	BrokerStatus(ctx context.Context, brokerAddr string) (BrokerStatus, error)
	TopicList(ctx context.Context) ([]Topic, error)
	TopicRoute(ctx context.Context, topic string) (TopicRoute, error)
	TopicStatus(ctx context.Context, topic string) (TopicStatus, error)
	TopicMessages(ctx context.Context, query MessageBrowseQuery) (TopicMessages, error)
	UpsertTopic(ctx context.Context, request TopicConfigMutation) (TopicMutationResult, error)
	DeleteTopic(ctx context.Context, request TopicDeleteRequest) (TopicMutationResult, error)
	SendTopicMessage(ctx context.Context, request TopicMessageSendRequest) (TopicMessageSendResult, error)
	ConsumerGroups(ctx context.Context) ([]ConsumerGroup, error)
	ConsumerDetail(ctx context.Context, group string, topic string) (ConsumerDetail, error)
	ResetConsumerOffset(ctx context.Context, request ConsumerOffsetResetRequest) (ConsumerOffsetResetResult, error)
	MessageChain(ctx context.Context, query MessageQuery) (MessageStatusChain, error)
}

// MessageBrowseQuery 表示 Topic 消息浏览入口，按队列水位回查保留窗口内的最近消息。
type MessageBrowseQuery struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueID    int    `json:"queueId"`
	Limit      int    `json:"limit"`
}

// MessageQuery 表示消息链路查询入口，支持 messageId、key、trace 和消费者组进度组合查询。
type MessageQuery struct {
	MessageID      string `json:"messageId"`
	Topic          string `json:"topic"`
	Key            string `json:"key"`
	BrokerName     string `json:"brokerName"`
	QueueID        int    `json:"queueId"`
	QueueOffset    int64  `json:"queueOffset"`
	HasQueueOffset bool   `json:"hasQueueOffset"`
	ConsumerGroup  string `json:"consumerGroup"`
	TraceTopic     string `json:"traceTopic"`
	BeginTimestamp int64  `json:"beginTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp"`
	MaxNum         int    `json:"maxNum"`
}
