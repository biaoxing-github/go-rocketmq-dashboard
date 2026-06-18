package rocketmq

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var unquotedNumericObjectKeyPattern = regexp.MustCompile(`([{\[,]\s*)(\d+)(\s*:)`)

// ParseClusterList 将 mqadmin clusterList 文本解析成结构化集群数据。
func ParseClusterList(output string) ([]Cluster, error) {
	lines := strings.Split(output, "\n")
	clusterMap := make(map[string][]Broker)
	clusterOrder := make([]string, 0)

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 12 {
			return nil, fmt.Errorf("clusterList 行字段不足: %s", line)
		}

		clusterName := fields[0]
		if _, ok := clusterMap[clusterName]; !ok {
			clusterOrder = append(clusterOrder, clusterName)
		}

		broker := Broker{
			Cluster:   clusterName,
			Name:      fields[1],
			ID:        fields[2],
			Address:   fields[3],
			Version:   fields[4],
			InTPS:     fields[5],
			OutTPS:    fields[6],
			Activated: strings.EqualFold(fields[len(fields)-1], "true"),
		}
		clusterMap[clusterName] = append(clusterMap[clusterName], broker)
	}

	clusters := make([]Cluster, 0, len(clusterOrder))
	for _, name := range clusterOrder {
		clusters = append(clusters, Cluster{Name: name, Brokers: clusterMap[name]})
	}
	return clusters, nil
}

// ParseBrokerStatus 将 mqadmin brokerStatus 的 key/value 输出解析成摘要和完整指标表。
func ParseBrokerStatus(brokerAddr string, output string) (BrokerStatus, error) {
	status := BrokerStatus{
		BrokerAddr: strings.TrimSpace(brokerAddr),
		Metrics:    make([]BrokerRuntimeMetric, 0),
	}
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "Java ") || strings.HasPrefix(line, "OpenJDK") || strings.HasPrefix(line, "Picked up ") {
			continue
		}
		key, value, ok := parseBrokerStatusLine(line)
		if !ok {
			continue
		}
		status.Metrics = append(status.Metrics, BrokerRuntimeMetric{Key: key, Value: value})
		switch key {
		case "brokerVersionDesc":
			status.BrokerVersionDesc = value
		case "brokerRole":
			status.BrokerRole = value
		case "bootTimestamp":
			status.BootTimestamp = value
		case "putTps":
			status.PutTps = value
		case "getFoundTps":
			status.GetFoundTps = value
		case "getTotalTps":
			status.GetTotalTps = value
		case "commitLogDirCapacity":
			status.CommitLogCapacity = value
		case "dispatchBehindBytes":
			status.DispatchBehind = value
		}
	}
	if len(status.Metrics) == 0 {
		if summary := mqadminFailureSummary(output); summary != "" {
			return BrokerStatus{}, fmt.Errorf("brokerStatus 命令失败: %s", summary)
		}
		return BrokerStatus{}, fmt.Errorf("brokerStatus 未解析到运行指标")
	}
	status.RuntimeDescription = brokerRuntimeDescription(status)
	return status, nil
}

// parseBrokerStatusLine 兼容 brokerStatus 单 Broker 和 cluster 模式输出，保留真实 key/value。
func parseBrokerStatusLine(line string) (string, string, bool) {
	index := strings.Index(line, ": ")
	if index < 0 {
		index = strings.LastIndex(line, ":")
	}
	if index < 0 {
		return "", "", false
	}
	keyPart := strings.TrimSpace(line[:index])
	value := strings.TrimSpace(line[index+1:])
	if keyPart == "" || value == "" {
		return "", "", false
	}
	fields := strings.Fields(keyPart)
	key := fields[len(fields)-1]
	if key == "" || strings.Contains(key, ".") || strings.Contains(key, "/") {
		return "", "", false
	}
	return key, value, true
}

func brokerRuntimeDescription(status BrokerStatus) string {
	parts := make([]string, 0, 4)
	if status.BrokerRole != "" {
		parts = append(parts, status.BrokerRole)
	}
	if status.BrokerVersionDesc != "" {
		parts = append(parts, status.BrokerVersionDesc)
	}
	if status.PutTps != "" {
		parts = append(parts, "PUT "+status.PutTps)
	}
	if status.GetFoundTps != "" {
		parts = append(parts, "GET "+status.GetFoundTps)
	}
	return strings.Join(parts, " · ")
}

// ParseTopicList 将 mqadmin topicList 输出解析成 Topic 列表，并标注 Topic 类型。
func ParseTopicList(output string) []Topic {
	lines := strings.Split(output, "\n")
	topics := make([]Topic, 0, len(lines))
	seen := make(map[string]bool)
	for _, raw := range lines {
		name := strings.TrimSpace(raw)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		topics = append(topics, Topic{Name: name, Kind: classifyTopic(name)})
	}
	return topics
}

// ParseTopicRoute 将 mqadmin topicRoute 默认 JSON 输出解析成前端可扫描的路由详情。
func ParseTopicRoute(topic string, output string) (TopicRoute, error) {
	var raw struct {
		QueueDatas []struct {
			BrokerName     string `json:"brokerName"`
			ReadQueueNums  int    `json:"readQueueNums"`
			WriteQueueNums int    `json:"writeQueueNums"`
			Perm           int    `json:"perm"`
			TopicSysFlag   int    `json:"topicSysFlag"`
		} `json:"queueDatas"`
		BrokerDatas []struct {
			Cluster     string            `json:"cluster"`
			BrokerName  string            `json:"brokerName"`
			BrokerAddrs map[string]string `json:"brokerAddrs"`
		} `json:"brokerDatas"`
	}
	normalizedOutput := normalizeTopicRouteJSON(output)
	if err := json.Unmarshal([]byte(normalizedOutput), &raw); err != nil {
		return TopicRoute{}, fmt.Errorf("解析 topicRoute JSON 失败: %w", err)
	}

	route := TopicRoute{
		Topic:   topic,
		Queues:  make([]TopicQueueRoute, 0, len(raw.QueueDatas)),
		Brokers: make([]TopicBrokerRoute, 0, len(raw.BrokerDatas)),
	}
	for _, queue := range raw.QueueDatas {
		route.TotalReadQueues += queue.ReadQueueNums
		route.TotalWriteQueues += queue.WriteQueueNums
		route.Queues = append(route.Queues, TopicQueueRoute{
			BrokerName:      queue.BrokerName,
			ReadQueueNums:   queue.ReadQueueNums,
			WriteQueueNums:  queue.WriteQueueNums,
			Perm:            queue.Perm,
			PermissionLabel: permissionLabel(queue.Perm),
			TopicSysFlag:    queue.TopicSysFlag,
		})
	}
	for _, broker := range raw.BrokerDatas {
		route.Brokers = append(route.Brokers, TopicBrokerRoute{
			Cluster:    broker.Cluster,
			BrokerName: broker.BrokerName,
			Addrs:      broker.BrokerAddrs,
		})
	}
	if len(route.Queues) == 0 && len(route.Brokers) == 0 {
		return TopicRoute{}, fmt.Errorf("topicRoute 未解析到路由数据")
	}
	return route, nil
}

// ParseTopicStatus 将 mqadmin topicStatus 输出解析成队列水位明细，并计算 Topic 总消息数。
func ParseTopicStatus(topic string, output string) (TopicStatus, error) {
	lines := strings.Split(output, "\n")
	status := TopicStatus{
		Topic: strings.TrimSpace(topic),
		Rows:  make([]TopicStatusRow, 0),
	}

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "org.apache.") || strings.HasPrefix(line, "at ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		queueID, err := strconv.Atoi(fields[1])
		if err != nil {
			// RocketMQ tools 在部分 JVM 环境会把 warning 输出混在表格前面，QID 不是数字时视为命令噪声。
			continue
		}
		minOffset, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return TopicStatus{}, fmt.Errorf("解析 topicStatus min offset 失败: %w", err)
		}
		maxOffset, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return TopicStatus{}, fmt.Errorf("解析 topicStatus max offset 失败: %w", err)
		}
		messageCount := maxOffset - minOffset
		if messageCount < 0 {
			messageCount = 0
		}
		row := TopicStatusRow{
			BrokerName:   fields[0],
			QueueID:      queueID,
			MinOffset:    minOffset,
			MaxOffset:    maxOffset,
			MessageCount: messageCount,
			LastUpdated:  strings.Join(fields[4:], " "),
		}
		status.Rows = append(status.Rows, row)
		status.TotalQueues += 1
		status.MinOffsetTotal += minOffset
		status.MaxOffsetTotal += maxOffset
		status.TotalMessageCount += messageCount
	}

	if len(status.Rows) == 0 {
		if summary := mqadminFailureSummary(output); summary != "" {
			return TopicStatus{}, fmt.Errorf("topicStatus 命令失败: %s", summary)
		}
		return TopicStatus{}, fmt.Errorf("topicStatus 未解析到队列状态")
	}
	return status, nil
}

// mqadminFailureSummary 从 RocketMQ tools 的零退出异常输出里提取最关键的一行，避免前端只看到解析失败。
func mqadminFailureSummary(output string) string {
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.Contains(line, "SubCommandException") || strings.Contains(line, "Caused by:") || strings.Contains(line, "RemotingTimeoutException") {
			return line
		}
	}
	return ""
}

// normalizeTopicRouteJSON 兼容 RocketMQ 5.2.0 topicRoute 输出中 brokerAddrs 的数字 key 未加引号格式。
func normalizeTopicRouteJSON(output string) string {
	trimmed := strings.TrimSpace(output)
	return unquotedNumericObjectKeyPattern.ReplaceAllString(trimmed, `${1}"${2}"${3}`)
}

// ParseConsumerProgress 将 mqadmin consumerProgress 输出解析成消费者组列表。
func ParseConsumerProgress(output string) ([]ConsumerGroup, error) {
	lines := strings.Split(output, "\n")
	groups := make([]ConsumerGroup, 0, len(lines))
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if !looksLikeConsumerSummaryRow(fields) {
			// consumerProgress 会把部分组的查询异常写入同一段输出；这里只接受官方汇总表数据行。
			continue
		}

		count, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("解析 consumer count 失败: %w", err)
		}

		group := ConsumerGroup{Name: fields[0], Count: count}
		if fields[2] == "OFFLINE" {
			group.Version = "OFFLINE"
			group.Online = false
			group.TPS = fields[3]
			group.DiffTotal = parseLastInt(fields)
		} else {
			if len(fields) < 7 {
				return nil, fmt.Errorf("consumerProgress 在线行字段不足: %s", line)
			}
			group.Version = fields[2]
			group.Type = fields[3]
			group.Model = fields[4]
			group.TPS = fields[5]
			group.DiffTotal = parseLastInt(fields)
			group.Online = true
		}
		groups = append(groups, group)
	}
	if len(groups) == 0 {
		return nil, fmt.Errorf("consumerProgress 未解析到消费者组")
	}
	return groups, nil
}

// ParseConsumerConnection 将 mqadmin consumerConnection 输出解析成连接、订阅和基础消费信息。
func ParseConsumerConnection(output string) (ConsumerConnectionSnapshot, error) {
	lines := strings.Split(output, "\n")
	snapshot := ConsumerConnectionSnapshot{}
	mode := "connections"

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, "Below is subscription:"):
			mode = "subscriptions"
			continue
		case strings.HasPrefix(line, "ConsumeType:"):
			snapshot.ConsumeType = strings.TrimSpace(strings.TrimPrefix(line, "ConsumeType:"))
			continue
		case strings.HasPrefix(line, "MessageModel:"):
			snapshot.MessageModel = strings.TrimSpace(strings.TrimPrefix(line, "MessageModel:"))
			continue
		case strings.HasPrefix(line, "ConsumeFromWhere:"):
			snapshot.ConsumeFromWhere = strings.TrimSpace(strings.TrimPrefix(line, "ConsumeFromWhere:"))
			continue
		case strings.HasPrefix(line, "#ClientId") || strings.HasPrefix(line, "#Topic") || strings.HasPrefix(line, "#"):
			continue
		}

		fields := strings.Fields(line)
		if mode == "connections" {
			if len(fields) < 4 {
				continue
			}
			snapshot.Connections = append(snapshot.Connections, ConsumerConnection{
				ClientID:   fields[0],
				ClientAddr: fields[1],
				Language:   fields[2],
				Version:    fields[3],
			})
			continue
		}
		if len(fields) < 2 {
			continue
		}
		snapshot.Subscriptions = append(snapshot.Subscriptions, ConsumerSubscription{
			Topic:      fields[0],
			Expression: strings.Join(fields[1:], " "),
		})
	}

	if len(snapshot.Connections) == 0 && len(snapshot.Subscriptions) == 0 && snapshot.ConsumeType == "" && snapshot.MessageModel == "" && snapshot.ConsumeFromWhere == "" {
		return ConsumerConnectionSnapshot{}, fmt.Errorf("consumerConnection 未解析到有效数据")
	}
	return snapshot, nil
}

// ParseConsumerProgressDetail 将 consumerProgress -g -t -s true 输出解析成队列明细和汇总指标。
func ParseConsumerProgressDetail(output string) (ConsumerProgressDetail, error) {
	lines := strings.Split(output, "\n")
	detail := ConsumerProgressDetail{}
	var sumDiff int64
	var sumInflight int64
	var hasDiffTotal bool
	var hasInflightTotal bool
	var hasConsumeTPS bool

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "org.apache.") || strings.HasPrefix(line, "at ") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "Consume TPS:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "Consume TPS:"))
			tps, err := strconv.ParseFloat(value, 64)
			if err == nil {
				detail.ConsumeTPS = tps
				hasConsumeTPS = true
			}
			continue
		case strings.HasPrefix(line, "Consume Diff Total:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "Consume Diff Total:"))
			total, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				detail.DiffTotal = total
				hasDiffTotal = true
			}
			continue
		case strings.HasPrefix(line, "Consume Inflight Total:"):
			value := strings.TrimSpace(strings.TrimPrefix(line, "Consume Inflight Total:"))
			total, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				detail.InflightTotal = total
				hasInflightTotal = true
			}
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 9 {
			continue
		}
		queueID, err := strconv.Atoi(fields[2])
		if err != nil {
			return ConsumerProgressDetail{}, fmt.Errorf("解析 consumerProgress queue id 失败: %w", err)
		}
		brokerOffset, err := strconv.ParseInt(fields[3], 10, 64)
		if err != nil {
			return ConsumerProgressDetail{}, fmt.Errorf("解析 consumerProgress broker offset 失败: %w", err)
		}
		consumerOffset, err := strconv.ParseInt(fields[4], 10, 64)
		if err != nil {
			return ConsumerProgressDetail{}, fmt.Errorf("解析 consumerProgress consumer offset 失败: %w", err)
		}
		diff, err := strconv.ParseInt(fields[6], 10, 64)
		if err != nil {
			return ConsumerProgressDetail{}, fmt.Errorf("解析 consumerProgress diff 失败: %w", err)
		}
		inflight, err := strconv.ParseInt(fields[7], 10, 64)
		if err != nil {
			return ConsumerProgressDetail{}, fmt.Errorf("解析 consumerProgress inflight 失败: %w", err)
		}
		row := ConsumerProgressRow{
			Topic:          fields[0],
			BrokerName:     fields[1],
			QueueID:        queueID,
			BrokerOffset:   brokerOffset,
			ConsumerOffset: consumerOffset,
			ClientIP:       fields[5],
			Diff:           diff,
			Inflight:       inflight,
			LastTime:       strings.Join(fields[8:], " "),
		}
		detail.Rows = append(detail.Rows, row)
		sumDiff += diff
		sumInflight += inflight
	}

	if !hasConsumeTPS {
		detail.ConsumeTPS = 0
	}
	if !hasDiffTotal {
		detail.DiffTotal = sumDiff
	}
	if !hasInflightTotal {
		detail.InflightTotal = sumInflight
	}
	if len(detail.Rows) == 0 {
		return ConsumerProgressDetail{}, fmt.Errorf("consumerProgress 未解析到队列明细")
	}
	return detail, nil
}

// ParseMessageDetail 将 mqadmin queryMsgById 输出解析成单条消息详情。
func ParseMessageDetail(output string) (MessageDetail, error) {
	values := parseColonLines(output)
	messageID := firstNonEmpty(values["OffsetID"], values["Message ID"], values["MsgId"])
	topic := values["Topic"]
	if messageID == "" || topic == "" {
		return MessageDetail{}, fmt.Errorf("queryMsgById 输出缺少消息 ID 或 Topic")
	}

	queueID, err := parseInt(values["Queue ID"])
	if err != nil {
		return MessageDetail{}, fmt.Errorf("解析 Queue ID 失败: %w", err)
	}
	queueOffset, err := parseInt64(values["Queue Offset"])
	if err != nil {
		return MessageDetail{}, fmt.Errorf("解析 Queue Offset 失败: %w", err)
	}
	reconsumeTimes, err := parseInt(values["Reconsume Times"])
	if err != nil {
		return MessageDetail{}, fmt.Errorf("解析 Reconsume Times 失败: %w", err)
	}
	properties := parseMessageProperties(values["Properties"])

	return MessageDetail{
		MessageID:      messageID,
		Topic:          topic,
		Keys:           parseBracketList(values["Keys"]),
		TraceMessageID: properties["UNIQ_KEY"],
		TraceParent:    properties["traceparent"],
		StoreTimestamp: parseRocketMQTime(values["Store Timestamp"]),
		QueueID:        queueID,
		QueueOffset:    queueOffset,
		ReconsumeTimes: reconsumeTimes,
		BornHost:       values["Born Host"],
		StoreHost:      values["Store Host"],
		BodyPreview:    truncateBody(values["Message Body"]),
	}, nil
}

func parseMessageProperties(raw string) map[string]string {
	properties := make(map[string]string)
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "{")
	raw = strings.TrimSuffix(raw, "}")
	if raw == "" {
		return properties
	}
	for _, part := range strings.Split(raw, ",") {
		pair := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := strings.TrimSpace(pair[0])
		value := strings.TrimSpace(pair[1])
		if key != "" {
			properties[key] = value
		}
	}
	return properties
}

// ParseMessageSearchResults 将 mqadmin queryMsgByKey 输出解析成候选消息列表。
func ParseMessageSearchResults(output string) ([]MessageSearchResult, error) {
	lines := strings.Split(output, "\n")
	results := make([]MessageSearchResult, 0)
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, fmt.Errorf("queryMsgByKey 行字段不足: %s", line)
		}
		queueID, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("解析消息 Queue ID 失败: %w", err)
		}
		queueOffset, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("解析消息 Queue Offset 失败: %w", err)
		}
		results = append(results, MessageSearchResult{
			MessageID:   fields[0],
			QueueID:     queueID,
			QueueOffset: queueOffset,
		})
	}
	return results, nil
}

// ParseTraceEvents 将 mqadmin queryMsgTraceById 输出解析成发送和消费轨迹事件。
func ParseTraceEvents(output string) ([]TraceEvent, error) {
	lines := strings.Split(output, "\n")
	events := make([]TraceEvent, 0)
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			return nil, fmt.Errorf("queryMsgTraceById 行字段不足: %s", line)
		}
		traceType := fields[0]
		stage := stageFromTrace(traceType, fields[len(fields)-1])
		group := fields[1]
		clientHost := fields[2]
		timeText := fields[3] + " " + fields[4]
		cost := fields[5]
		status := fields[len(fields)-1]
		events = append(events, TraceEvent{
			Stage:     stage,
			Group:     group,
			Timestamp: parseRocketMQTime(timeText),
			Detail:    fmt.Sprintf("%s host=%s cost=%s status=%s", traceType, clientHost, cost, status),
		})
	}
	return events, nil
}

// ParseConsumerStates 将指定消费者组的 queue 级消费进度解析成链路状态判断。
func ParseConsumerStates(group string, output string) ([]ConsumerState, error) {
	lines := strings.Split(output, "\n")
	states := make([]ConsumerState, 0)
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "Consume ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 7 {
			return nil, fmt.Errorf("consumerProgress 明细行字段不足: %s", line)
		}
		lag, err := parseConsumerDiff(fields)
		if err != nil {
			return nil, fmt.Errorf("解析 consumer diff 失败: %w", err)
		}
		status := "CONSUMED"
		if lag > 0 {
			status = "PENDING"
		}
		states = append(states, ConsumerState{
			Group:  group,
			Topic:  fields[0],
			Status: status,
			Lag:    lag,
		})
	}
	return states, nil
}

func parseConsumerDiff(fields []string) (int64, error) {
	intsFromRight := make([]int64, 0, 2)
	for i := len(fields) - 1; i >= 0; i-- {
		value, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			continue
		}
		intsFromRight = append(intsFromRight, value)
		if len(intsFromRight) == 2 {
			return intsFromRight[1], nil
		}
	}
	return 0, fmt.Errorf("未找到 consumer diff 字段")
}

func classifyTopic(name string) string {
	switch {
	case strings.HasPrefix(name, "%RETRY%"):
		return "retry"
	case strings.HasPrefix(name, "%DLQ%"):
		return "dlq"
	case strings.HasPrefix(name, "RMQ_SYS_"),
		strings.HasPrefix(name, "SCHEDULE_TOPIC_"),
		strings.HasPrefix(name, "rmq_sys_"),
		name == "TBW102",
		name == "SELF_TEST_TOPIC",
		name == "OFFSET_MOVED_EVENT":
		return "system"
	default:
		return "normal"
	}
}

// looksLikeConsumerSummaryRow 只放行 consumerProgress 官方汇总表行，过滤命令异常和日志噪声。
func looksLikeConsumerSummaryRow(fields []string) bool {
	if len(fields) < 5 {
		return false
	}
	if _, err := strconv.ParseInt(fields[1], 10, 64); err != nil {
		return false
	}
	if fields[2] == "OFFLINE" {
		_, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
		return err == nil
	}
	if len(fields) < 7 {
		return false
	}
	if !strings.HasPrefix(fields[2], "V") {
		return false
	}
	_, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	return err == nil
}

// permissionLabel 将 RocketMQ Topic 权限位转成运维可直接扫读的读写标签。
func permissionLabel(perm int) string {
	labels := make([]string, 0, 2)
	if perm&0x4 != 0 {
		labels = append(labels, "R")
	}
	if perm&0x2 != 0 {
		labels = append(labels, "W")
	}
	if len(labels) == 0 {
		return "NONE"
	}
	return strings.Join(labels, "")
}

func parseLastInt(fields []string) int64 {
	if len(fields) == 0 {
		return 0
	}
	value, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func parseColonLines(output string) map[string]string {
	values := make(map[string]string)
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		index := strings.Index(line, ":")
		if index <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:index])
		value := strings.TrimSpace(line[index+1:])
		values[key] = value
	}
	return values
}

func parseBracketList(value string) []string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" && !strings.EqualFold(item, "null") {
			items = append(items, item)
		}
	}
	return items
}

func parseRocketMQTime(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04:05.000"} {
		parsed, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func parseInt(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	return strconv.Atoi(strings.TrimSpace(value))
}

func parseInt64(value string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	return strconv.ParseInt(strings.TrimSpace(value), 10, 64)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func truncateBody(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= 512 {
		return value
	}
	return string(runes[:512])
}

func stageFromTrace(traceType string, status string) string {
	success := strings.EqualFold(status, "success") || strings.EqualFold(status, "true")
	switch traceType {
	case "Pub":
		if success {
			return "SEND_SUCCESS"
		}
		return "SEND_FAILED"
	case "Sub", "SubAfter":
		if success {
			return "CONSUME_SUCCESS"
		}
		return "CONSUME_FAILED"
	default:
		return traceType
	}
}
