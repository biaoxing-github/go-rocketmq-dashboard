package rocketmq

import "sort"

// BuildMessageStatusChain 将消息详情、轨迹事件和消费者状态拼成前端可读的状态链路。
func BuildMessageStatusChain(message MessageDetail, traces []TraceEvent, offsets []ConsumerState) MessageStatusChain {
	steps := make([]MessageStatusStep, 0, 1+len(traces)+len(offsets))
	consumeTraceByGroup := make(map[string]bool)
	for _, event := range traces {
		if isConsumeStage(event.Stage) {
			consumeTraceByGroup[event.Group] = true
		}
	}

	steps = append(steps, MessageStatusStep{
		Stage:     "STORED",
		Label:     "Broker 已存储",
		Timestamp: message.StoreTimestamp,
		Detail:    "消息已经写入 Broker 存储队列",
		Health:    "ok",
	})

	for _, event := range traces {
		steps = append(steps, MessageStatusStep{
			Stage:     event.Stage,
			Label:     labelForStage(event.Stage),
			Group:     event.Group,
			Timestamp: event.Timestamp,
			Detail:    event.Detail,
			Health:    healthForStage(event.Stage),
		})
	}

	for _, state := range offsets {
		// 已经有同组消费 Trace 时，优先展示带真实消费时间的 Trace，避免 Consumer 位点重复刷屏。
		if consumeTraceByGroup[state.Group] {
			continue
		}
		steps = append(steps, MessageStatusStep{
			Stage:     state.Status,
			Label:     labelForStage(state.Status),
			Group:     state.Group,
			Timestamp: latestTimestamp(message.StoreTimestamp, traces),
			Detail:    "消费者组当前堆积: " + formatLag(state.Lag),
			Health:    healthForStage(state.Status),
		})
	}

	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].Timestamp == steps[j].Timestamp {
			return stepRank(steps[i].Stage) < stepRank(steps[j].Stage)
		}
		return steps[i].Timestamp < steps[j].Timestamp
	})

	return MessageStatusChain{
		MessageID:     message.MessageID,
		Topic:         message.Topic,
		Keys:          message.Keys,
		Detail:        message,
		OverallStatus: overallStatusFromSteps(steps),
		Steps:         steps,
	}
}

// overallStatusFromSteps 从最终时间线反推主状态，但把 TRACE_MISSING 视为旁路告警而不是业务终态。
func overallStatusFromSteps(steps []MessageStatusStep) string {
	for index := len(steps) - 1; index >= 0; index-- {
		if steps[index].Stage == "TRACE_MISSING" {
			continue
		}
		return steps[index].Stage
	}
	return "STORED"
}

func isConsumeStage(stage string) bool {
	return stage == "CONSUME_SUCCESS" || stage == "CONSUME_FAILED" || stage == "CONSUMED"
}

func latestTimestamp(seed int64, traces []TraceEvent) int64 {
	latest := seed
	for _, event := range traces {
		if event.Timestamp > latest {
			latest = event.Timestamp
		}
	}
	return latest
}

func labelForStage(stage string) string {
	switch stage {
	case "STORED":
		return "Broker 已存储"
	case "SEND_SUCCESS":
		return "发送成功"
	case "SEND_FAILED":
		return "发送失败"
	case "CONSUME_SUCCESS", "CONSUMED":
		return "消费成功"
	case "CONSUME_FAILED":
		return "消费失败"
	case "RETRY":
		return "等待重试"
	case "DLQ":
		return "进入死信队列"
	case "PENDING":
		return "等待消费"
	case "TRACE_MISSING":
		return "轨迹缺失"
	case "CONSUMER_PROGRESS_FAILED":
		return "消费进度查询失败"
	default:
		return stage
	}
}

func healthForStage(stage string) string {
	switch stage {
	case "SEND_FAILED", "CONSUME_FAILED", "DLQ":
		return "danger"
	case "RETRY", "PENDING", "TRACE_MISSING", "CONSUMER_PROGRESS_FAILED":
		return "warning"
	default:
		return "ok"
	}
}

func stepRank(stage string) int {
	switch stage {
	case "STORED":
		return 0
	case "SEND_SUCCESS", "SEND_FAILED":
		return 1
	case "CONSUME_SUCCESS", "CONSUME_FAILED":
		return 2
	default:
		return 3
	}
}

func formatLag(lag int64) string {
	if lag == 0 {
		return "0"
	}
	return stringFromInt(lag)
}

func stringFromInt(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	buf := make([]byte, 0, 20)
	for value > 0 {
		buf = append(buf, byte('0'+value%10))
		value /= 10
	}
	if negative {
		buf = append(buf, '-')
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}
