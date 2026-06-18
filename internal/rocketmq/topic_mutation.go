package rocketmq

import (
	"errors"
	"strconv"
	"strings"
)

// TopicConfigMutation 表示 updateTopic 命令需要的 Topic 配置变更。
type TopicConfigMutation struct {
	// Topic 是要创建或更新的 Topic 名称，对应 mqadmin updateTopic -t。
	Topic string `json:"topic"`
	// ClusterName 表示按集群批量创建或更新 Topic，对应 mqadmin updateTopic -c。
	ClusterName string `json:"clusterName"`
	// BrokerAddr 表示按单个 Broker 地址创建或更新 Topic，对应 mqadmin updateTopic -b。
	BrokerAddr string `json:"brokerAddr"`
	// ReadQueueNums 是读队列数量，未传或小于等于 0 时使用 RocketMQ 控制台常用默认值 8。
	ReadQueueNums int `json:"readQueueNums"`
	// WriteQueueNums 是写队列数量，未传或小于等于 0 时使用 RocketMQ 控制台常用默认值 8。
	WriteQueueNums int `json:"writeQueueNums"`
	// Perm 是 Topic 权限位，2 表示写、4 表示读、6 表示读写。
	Perm int `json:"perm"`
	// Order 表示是否创建顺序 Topic，对应 mqadmin updateTopic -o true。
	Order bool `json:"order"`
	// Unit 表示是否设置 unit Topic，对应 mqadmin updateTopic -u true。
	Unit bool `json:"unit"`
	// HasUnitSub 表示是否允许 unit 订阅，对应 mqadmin updateTopic -s true。
	HasUnitSub bool `json:"hasUnitSub"`
	// Attributes 是 RocketMQ 5.x Topic 属性表达式，例如 +message.type=NORMAL。
	Attributes string `json:"attributes"`
}

// TopicDeleteRequest 表示 deleteTopic 命令需要的删除参数。
type TopicDeleteRequest struct {
	// Topic 是要删除的 Topic 名称，对应 mqadmin deleteTopic -t。
	Topic string `json:"topic"`
	// ClusterName 是删除目标集群，对应 mqadmin deleteTopic -c。
	ClusterName string `json:"clusterName"`
}

// TopicMutationResult 表示 Topic 写操作完成后返回给前端的执行摘要。
type TopicMutationResult struct {
	// Topic 是本次写操作影响的 Topic 名称。
	Topic string `json:"topic"`
	// Operation 是执行的逻辑操作，例如 upsertTopic 或 deleteTopic。
	Operation string `json:"operation"`
	// Target 是本次写操作的目标集群或 Broker 地址。
	Target string `json:"target"`
	// Output 是 mqadmin 返回的原始摘要，便于用户排查写操作失败或确认生效。
	Output string `json:"output"`
}

// TopicMessageSendRequest 表示 sendMessage 命令需要的消息发送参数。
type TopicMessageSendRequest struct {
	// Topic 是目标 Topic 名称，对应 mqadmin sendMessage -t。
	Topic string `json:"topic"`
	// Body 是 UTF-8 消息体，对应 mqadmin sendMessage -p。
	Body string `json:"body"`
	// Keys 是消息 key，对应 mqadmin sendMessage -k。
	Keys string `json:"keys"`
	// Tags 是消息 tags，对应 mqadmin sendMessage -c。
	Tags string `json:"tags"`
	// BrokerName 是定向发送的 Broker 名称，对应 mqadmin sendMessage -b。
	BrokerName string `json:"brokerName"`
	// QueueID 是定向发送的队列 ID，对应 mqadmin sendMessage -i；为空表示交给 RocketMQ 路由。
	QueueID *int `json:"queueId,omitempty"`
	// TraceEnable 表示本次发送是否开启消息轨迹，对应 mqadmin sendMessage -m true。
	TraceEnable bool `json:"traceEnable"`
}

// TopicMessageSendResult 表示发送消息后的 mqadmin 摘要，包含可直接查询链路的 messageId。
type TopicMessageSendResult struct {
	// Topic 是本次消息写入的 Topic。
	Topic string `json:"topic"`
	// Operation 是本次写操作名称，固定为 sendMessage。
	Operation string `json:"operation"`
	// BrokerName 是 mqadmin 返回的实际写入 Broker 名称。
	BrokerName string `json:"brokerName"`
	// QueueID 是 mqadmin 返回的实际写入队列 ID。
	QueueID int `json:"queueId"`
	// SendStatus 是 RocketMQ producer 返回的发送状态。
	SendStatus string `json:"sendStatus"`
	// MessageID 是 RocketMQ 返回的消息 ID，前端用它继续查看消息链路。
	MessageID string `json:"messageId"`
	// Output 是 mqadmin 返回的原始表格摘要，便于排查发送结果。
	Output string `json:"output"`
}

// ConsumerOffsetResetRequest 表示 resetOffsetByTime 命令需要的消费点重置参数。
type ConsumerOffsetResetRequest struct {
	// Group 是消费者组名称，对应 mqadmin resetOffsetByTime -g。
	Group string `json:"group"`
	// Topic 是消费者组订阅的 Topic，对应 mqadmin resetOffsetByTime -t。
	Topic string `json:"topic"`
	// Timestamp 是重置目标时间，支持 now、毫秒时间戳或 yyyy-MM-dd#HH:mm:ss:SSS，对应 -s。
	Timestamp string `json:"timestamp"`
	// Force 表示是否强制按时间回滚，对应 mqadmin resetOffsetByTime -f。
	Force bool `json:"force"`
	// BrokerAddr 是单队列重置时的 Broker 地址，对应 mqadmin resetOffsetByTime -b。
	BrokerAddr string `json:"brokerAddr"`
	// QueueID 是单队列重置时的队列 ID，对应 mqadmin resetOffsetByTime -q。
	QueueID *int `json:"queueId,omitempty"`
	// Offset 是单队列重置时的期望位点，对应 mqadmin resetOffsetByTime -o。
	Offset *int64 `json:"offset,omitempty"`
}

// ConsumerOffsetResetResult 表示消费点重置完成后的执行摘要。
type ConsumerOffsetResetResult struct {
	// Group 是本次重置的消费者组。
	Group string `json:"group"`
	// Topic 是本次重置的 Topic。
	Topic string `json:"topic"`
	// Operation 是本次写操作名称，固定为 resetOffsetByTime。
	Operation string `json:"operation"`
	// Timestamp 是最终传给 mqadmin 的目标时间。
	Timestamp string `json:"timestamp"`
	// Target 是本次重置范围，可能是整个 Topic 或某个 Broker/Queue。
	Target string `json:"target"`
	// Output 是 mqadmin 返回的原始表格摘要，便于确认每个队列的位点。
	Output string `json:"output"`
}

// Normalized 返回去空格并补齐默认值后的 Topic 创建/更新请求。
func (r TopicConfigMutation) Normalized() TopicConfigMutation {
	r.Topic = strings.TrimSpace(r.Topic)
	r.ClusterName = strings.TrimSpace(r.ClusterName)
	r.BrokerAddr = strings.TrimSpace(r.BrokerAddr)
	r.Attributes = strings.TrimSpace(r.Attributes)
	if r.ReadQueueNums <= 0 {
		r.ReadQueueNums = 8
	}
	if r.WriteQueueNums <= 0 {
		r.WriteQueueNums = 8
	}
	if r.Perm == 0 {
		r.Perm = 6
	}
	return r
}

// Validate 校验 updateTopic 所需的 Topic、目标和权限位，保证错误在发起 mqadmin 前暴露。
func (r TopicConfigMutation) Validate() error {
	r = r.Normalized()
	if r.Topic == "" {
		return errors.New("topic 必填")
	}
	hasCluster := r.ClusterName != ""
	hasBroker := r.BrokerAddr != ""
	if hasCluster == hasBroker {
		return errors.New("clusterName 和 brokerAddr 必须且只能填写一个")
	}
	if r.Perm != 2 && r.Perm != 4 && r.Perm != 6 {
		return errors.New("perm 仅支持 2、4、6")
	}
	return nil
}

// TargetLabel 返回用户可读的写操作目标，优先展示集群，其次展示 Broker 地址。
func (r TopicConfigMutation) TargetLabel() string {
	r = r.Normalized()
	if r.ClusterName != "" {
		return r.ClusterName
	}
	return r.BrokerAddr
}

// Normalized 返回去空格后的 Topic 删除请求。
func (r TopicDeleteRequest) Normalized() TopicDeleteRequest {
	r.Topic = strings.TrimSpace(r.Topic)
	r.ClusterName = strings.TrimSpace(r.ClusterName)
	return r
}

// Validate 校验 deleteTopic 所需的 Topic 和集群目标，RocketMQ 5.3.2 deleteTopic 只接受 -c。
func (r TopicDeleteRequest) Validate() error {
	r = r.Normalized()
	if r.Topic == "" {
		return errors.New("topic 必填")
	}
	if r.ClusterName == "" {
		return errors.New("clusterName 必填")
	}
	return nil
}

// Normalized 返回去空格后的消息发送请求，保留消息体原文不做裁剪。
func (r TopicMessageSendRequest) Normalized() TopicMessageSendRequest {
	r.Topic = strings.TrimSpace(r.Topic)
	r.Keys = strings.TrimSpace(r.Keys)
	r.Tags = strings.TrimSpace(r.Tags)
	r.BrokerName = strings.TrimSpace(r.BrokerName)
	return r
}

// Validate 校验 sendMessage 所需的 Topic、消息体和定向队列参数。
func (r TopicMessageSendRequest) Validate() error {
	r = r.Normalized()
	if r.Topic == "" {
		return errors.New("topic 必填")
	}
	if strings.TrimSpace(r.Body) == "" {
		return errors.New("body 必填")
	}
	if r.QueueID != nil {
		if *r.QueueID < 0 {
			return errors.New("queueId 不能小于 0")
		}
		if r.BrokerName == "" {
			return errors.New("指定 queueId 时 brokerName 必填")
		}
	}
	return nil
}

// TargetLabel 返回消息发送范围，未指定 Broker 时表示由 RocketMQ 路由选择队列。
func (r TopicMessageSendRequest) TargetLabel() string {
	r = r.Normalized()
	if r.BrokerName == "" {
		return "route"
	}
	if r.QueueID == nil {
		return r.BrokerName
	}
	return r.BrokerName + "/Q" + strconv.Itoa(*r.QueueID)
}

// Normalized 返回去空格并补齐默认时间后的消费点重置请求。
func (r ConsumerOffsetResetRequest) Normalized() ConsumerOffsetResetRequest {
	r.Group = strings.TrimSpace(r.Group)
	r.Topic = strings.TrimSpace(r.Topic)
	r.Timestamp = strings.TrimSpace(r.Timestamp)
	r.BrokerAddr = strings.TrimSpace(r.BrokerAddr)
	if r.Timestamp == "" {
		r.Timestamp = "now"
	}
	return r
}

// Validate 校验 resetOffsetByTime 所需的消费者组、Topic 和单队列参数组合。
func (r ConsumerOffsetResetRequest) Validate() error {
	r = r.Normalized()
	if r.Group == "" {
		return errors.New("group 必填")
	}
	if r.Topic == "" {
		return errors.New("topic 必填")
	}
	if r.QueueID != nil && *r.QueueID < 0 {
		return errors.New("queueId 不能小于 0")
	}
	hasBroker := r.BrokerAddr != ""
	hasQueue := r.QueueID != nil
	if hasBroker != hasQueue {
		return errors.New("brokerAddr 和 queueId 必须同时填写")
	}
	if r.Offset != nil && (!hasBroker || !hasQueue) {
		return errors.New("指定 offset 时 brokerAddr 和 queueId 必填")
	}
	return nil
}

// TargetLabel 返回消费点重置范围，未指定队列时表示重置整个 Group/Topic。
func (r ConsumerOffsetResetRequest) TargetLabel() string {
	r = r.Normalized()
	if r.BrokerAddr == "" || r.QueueID == nil {
		return "group/topic"
	}
	return r.BrokerAddr + "/Q" + strconv.Itoa(*r.QueueID)
}
