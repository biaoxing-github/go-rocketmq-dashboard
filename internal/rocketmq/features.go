package rocketmq

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

var brokerConfigHighlightKeys = []string{
	"brokerClusterName",
	"brokerName",
	"brokerId",
	"brokerRole",
	"flushDiskType",
	"brokerPermission",
	"listenPort",
	"brokerIP1",
	"fileReservedTime",
	"maxMessageSize",
	"messageDelayLevel",
	"transactionCheckInterval",
	"transactionTimeOut",
	"transactionTimeout",
	"transactionCheckMax",
	"msgTraceTopicName",
	"traceOn",
	"traceTopicEnable",
	"timerStopEnqueue",
	"autoCreateTopicEnable",
	"autoCreateSubscriptionGroup",
	"slaveReadEnable",
	"useTLS",
	"aclEnable",
	"authenticationEnabled",
	"authorizationEnabled",
	"enableControllerMode",
	"controllerAddr",
	"allAckInSyncStateSet",
}

type commonConfigDefinition struct {
	key         string
	category    string
	label       string
	description string
	impact      string
}

var commonConfigDefinitions = []commonConfigDefinition{
	{key: "brokerClusterName", category: "Broker 基础", label: "所属集群", description: "Broker 注册到 NameServer 时声明的集群名称。", impact: "用于判断 Topic 路由和 Broker 分组是否符合预期。"},
	{key: "brokerName", category: "Broker 基础", label: "Broker 名称", description: "同一主从复制组共享的 Broker 名称。", impact: "排查主从、路由和消费位点时通常先按该名称定位。"},
	{key: "brokerId", category: "Broker 基础", label: "Broker ID", description: "0 通常表示 Master，非 0 通常表示 Slave。", impact: "用于识别当前节点角色和读写流量是否落到预期节点。"},
	{key: "brokerRole", category: "Broker 基础", label: "Broker 角色", description: "声明 Broker 是同步 Master、异步 Master 还是 Slave。", impact: "影响写入确认、复制链路和故障切换判断。"},
	{key: "flushDiskType", category: "Broker 基础", label: "刷盘方式", description: "控制 CommitLog 同步刷盘或异步刷盘。", impact: "同步刷盘更重视落盘确认，异步刷盘更重视吞吐。"},
	{key: "brokerPermission", category: "Broker 基础", label: "Broker 权限", description: "Broker 对读写请求开放的权限位。", impact: "权限异常会直接导致生产或消费侧读写失败。"},
	{key: "listenPort", category: "Broker 基础", label: "监听端口", description: "Broker 对客户端和管理命令开放的端口。", impact: "用于核对网络、容器映射和客户端连接目标。"},
	{key: "brokerIP1", category: "Broker 基础", label: "Broker 地址", description: "Broker 对外注册的主要 IP。", impact: "地址不正确会导致客户端或管理命令连接失败。"},
	{key: "autoCreateTopicEnable", category: "Topic 与订阅", label: "自动创建 Topic", description: "允许生产端通过默认模板自动创建新 Topic。", impact: "开启后支持自动创建 Topic，关闭后需要提前创建 Topic，能减少误写 Topic。"},
	{key: "autoCreateSubscriptionGroup", category: "Topic 与订阅", label: "自动创建订阅组", description: "允许消费组首次连接时自动创建订阅组。", impact: "关闭后需要提前创建消费组，能减少误拼 group 自动生效。"},
	{key: "messageDelayLevel", category: "消息类型", label: "延时级别", description: "RocketMQ 4.x 延时消息支持的固定延迟级别。", impact: "业务只能选择这里声明的延迟档位，缺失会影响延时消息投递。"},
	{key: "transactionCheckInterval", category: "事务消息", label: "事务回查间隔", description: "Broker 扫描事务半消息并触发回查的基础间隔。", impact: "间隔越短回查越快，但 Broker 与生产者的回查压力越高。"},
	{key: "transactionTimeOut", category: "事务消息", label: "事务超时时间", description: "事务半消息超过该时间后会进入回查流程。", impact: "设置过短可能频繁回查，过长会延迟未决事务的处理。"},
	{key: "transactionTimeout", category: "事务消息", label: "事务超时时间", description: "RocketMQ 部分版本使用的事务超时配置键。", impact: "与 transactionTimeOut 语义相近，用于判断未决事务多久后被回查。"},
	{key: "transactionCheckMax", category: "事务消息", label: "最大回查次数", description: "同一事务半消息最多允许被 Broker 回查的次数。", impact: "超过次数后事务可能进入异常处理，需要重点关注。"},
	{key: "traceOn", category: "可观测", label: "消息 Trace 开关", description: "控制 Broker 或客户端链路是否记录消息轨迹。", impact: "开启后更容易排查发送和消费链路，但会增加 Trace Topic 写入。"},
	{key: "traceTopicEnable", category: "可观测", label: "Trace Topic 开关", description: "控制系统 Trace Topic 是否启用。", impact: "关闭后页面无法通过 Trace 直接展示消息消费链路。"},
	{key: "msgTraceTopicName", category: "可观测", label: "Trace Topic 名称", description: "消息轨迹写入的 Topic 名称。", impact: "自定义后排查时需要用该 Topic 查询轨迹。"},
	{key: "timerWheelEnable", category: "定时消息", label: "Timer Wheel", description: "RocketMQ 5.x 定时消息时间轮开关。", impact: "开启后可使用更灵活的定时消息能力。"},
	{key: "timerStopEnqueue", category: "定时消息", label: "暂停定时入队", description: "控制定时消息是否停止进入普通消费队列。", impact: "为 true 时定时消息可能停留在定时系统内，业务消费会延迟。"},
	{key: "timerMaxDelaySec", category: "定时消息", label: "最大定时秒数", description: "允许定时消息延迟的最大秒数。", impact: "超过该值的定时投递请求会被限制或拒绝。"},
	{key: "popInvisibleTime", category: "消费模型", label: "POP 隐身时间", description: "POP 消费模式下消息被取走后的不可见时间。", impact: "影响 POP 消费失败后的重新可见速度和重复消费窗口。"},
	{key: "slaveReadEnable", category: "高可用", label: "从节点读取", description: "允许消费者从 Slave 读取消息。", impact: "可分摊读取压力，也会影响故障或延迟场景下的消费路径。"},
	{key: "enableControllerMode", category: "高可用", label: "Controller 模式", description: "启用 RocketMQ Controller 管理 Broker 主从切换。", impact: "开启后主从管理依赖 Controller 配置，需要关注 Controller 可用性。"},
	{key: "controllerAddr", category: "高可用", label: "Controller 地址", description: "Broker 连接的 Controller 地址。", impact: "地址错误会影响自动选主和高可用管理。"},
	{key: "allAckInSyncStateSet", category: "高可用", label: "同步副本全确认", description: "要求同步状态集合内副本全部确认。", impact: "可提升复制一致性要求，但会提高写入确认成本。"},
	{key: "fileReservedTime", category: "存储", label: "文件保留时间", description: "CommitLog 和消费队列文件保留的小时数。", impact: "决定消息可回溯窗口，过短会影响补偿和排查。"},
	{key: "diskMaxUsedSpaceRatio", category: "存储", label: "磁盘水位阈值", description: "磁盘使用率超过该比例后 Broker 会进入保护状态。", impact: "阈值过高可能压缩磁盘余量，过低可能提前限制写入。"},
	{key: "maxMessageSize", category: "存储", label: "最大消息大小", description: "单条消息允许的最大字节数。", impact: "超过限制的业务消息会发送失败。"},
	{key: "useTLS", category: "连接与认证", label: "TLS", description: "控制客户端和 Broker 连接是否使用 TLS。", impact: "客户端配置必须与 Broker 保持一致，否则连接失败。"},
	{key: "aclEnable", category: "连接与认证", label: "ACL", description: "RocketMQ 访问控制开关。", impact: "开启后客户端和管理命令需要携带正确访问凭据。"},
	{key: "authenticationEnabled", category: "连接与认证", label: "认证", description: "RocketMQ 5.x 认证开关。", impact: "开启后未认证请求会被拒绝。"},
	{key: "authorizationEnabled", category: "连接与认证", label: "授权", description: "RocketMQ 5.x 授权开关。", impact: "开启后账号权限会影响 Topic 和消费组操作。"},
}

var knownSystemTopics = []FeatureTopic{
	{Name: "RMQ_SYS_TRANS_HALF_TOPIC", Label: "事务半消息", Kind: "transaction", Detail: "事务消息 prepare/half 消息"},
	{Name: "RMQ_SYS_TRANS_OP_HALF_TOPIC", Label: "事务操作消息", Kind: "transaction", Detail: "事务提交/回滚操作消息"},
	{Name: "RMQ_SYS_TRACE_TOPIC", Label: "消息 Trace", Kind: "trace", Detail: "客户端消息轨迹"},
	{Name: "SCHEDULE_TOPIC_XXXX", Label: "延时消息", Kind: "delay", Detail: "RocketMQ 延时级别消息"},
	{Name: "rmq_sys_wheel_timer", Label: "定时消息", Kind: "timer", Detail: "RocketMQ 5.x timer wheel"},
	{Name: "TBW102", Label: "自动创建 Topic 模板", Kind: "system", Detail: "autoCreateTopicEnable 使用的默认 Topic"},
	{Name: "SELF_TEST_TOPIC", Label: "自检 Topic", Kind: "system", Detail: "Broker 自检与探测"},
	{Name: "OFFSET_MOVED_EVENT", Label: "Offset 迁移事件", Kind: "system", Detail: "消费位点迁移事件"},
}

// BuildClusterFeatureReport 根据已采集的 Topic 和配置快照生成能力画像。
func BuildClusterFeatureReport(nameServer string, clusters []Cluster, topics []Topic, brokerConfigs []BrokerConfigSnapshot, nameServerConfigs []NameServerConfigSnapshot, warnings []string) ClusterFeatureReport {
	report := ClusterFeatureReport{
		NameServer:           strings.TrimSpace(nameServer),
		GeneratedAtUnixMilli: time.Now().UnixMilli(),
		ClusterCount:         len(clusters),
		TopicCount:           len(topics),
		BrokerConfigs:        append([]BrokerConfigSnapshot(nil), brokerConfigs...),
		NameServerConfigs:    append([]NameServerConfigSnapshot(nil), nameServerConfigs...),
		Warnings:             append([]string(nil), warnings...),
	}
	for _, cluster := range clusters {
		report.BrokerCount += len(cluster.Brokers)
	}
	report.SystemTopics = buildFeatureTopics(topics)
	report.SystemTopicCount = countPresentFeatureTopics(report.SystemTopics)
	report.Capabilities = buildFeatureCapabilities(report.SystemTopics, brokerConfigs)
	report.CommonConfigPanels = buildCommonConfigPanels(brokerConfigs)
	report.TransactionRuntime = BuildTransactionRuntimeReport(nil, nil, nil, nil)
	return report
}

// BrokerConfigSnapshotFromEntries 将 Broker 基础信息和配置 entries 合成页面展示快照。
func BrokerConfigSnapshotFromEntries(broker Broker, entries []ConfigEntry) BrokerConfigSnapshot {
	snapshot := BrokerConfigSnapshot{
		Cluster:    broker.Cluster,
		BrokerName: broker.Name,
		BrokerID:   broker.ID,
		BrokerAddr: broker.Address,
		Version:    broker.Version,
		Entries:    append([]ConfigEntry(nil), entries...),
		Highlights: configHighlights(entries),
	}
	if role := configEntryValue(entries, "brokerRole"); role != "" {
		snapshot.Role = role
	}
	if cluster := configEntryValue(entries, "brokerClusterName"); snapshot.Cluster == "" && cluster != "" {
		snapshot.Cluster = cluster
	}
	if name := configEntryValue(entries, "brokerName"); snapshot.BrokerName == "" && name != "" {
		snapshot.BrokerName = name
	}
	if id := configEntryValue(entries, "brokerId"); snapshot.BrokerID == "" && id != "" {
		snapshot.BrokerID = id
	}
	return snapshot
}

func buildFeatureTopics(topics []Topic) []FeatureTopic {
	present := make(map[string]Topic, len(topics))
	for _, topic := range topics {
		present[topic.Name] = topic
	}
	result := make([]FeatureTopic, 0, len(knownSystemTopics)+len(topics))
	known := make(map[string]bool, len(knownSystemTopics))
	for _, topic := range knownSystemTopics {
		topic.Present = present[topic.Name].Name != ""
		known[topic.Name] = true
		result = append(result, topic)
	}
	for _, topic := range topics {
		if known[topic.Name] || topic.Kind != "system" {
			continue
		}
		result = append(result, FeatureTopic{
			Name:    topic.Name,
			Label:   topic.Name,
			Kind:    "system",
			Present: true,
			Detail:  "NameServer 返回的系统 Topic",
		})
	}
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].Present != result[right].Present {
			return result[left].Present
		}
		return result[left].Name < result[right].Name
	})
	return result
}

func countPresentFeatureTopics(topics []FeatureTopic) int {
	count := 0
	for _, topic := range topics {
		if topic.Present {
			count++
		}
	}
	return count
}

func buildFeatureCapabilities(topics []FeatureTopic, brokerConfigs []BrokerConfigSnapshot) []FeatureCapability {
	return []FeatureCapability{
		transactionCapability(topics, brokerConfigs),
		traceCapability(topics, brokerConfigs),
		delayCapability(topics, brokerConfigs),
		timerCapability(topics, brokerConfigs),
		boolConfigCapability("autoCreateTopic", "自动创建 Topic", "Topic", []string{"autoCreateTopicEnable"}, brokerConfigs),
		boolConfigCapability("autoCreateSubscriptionGroup", "自动创建订阅组", "Consumer", []string{"autoCreateSubscriptionGroup"}, brokerConfigs),
		boolConfigCapability("tls", "TLS", "安全", []string{"useTLS"}, brokerConfigs),
		aclCapability(brokerConfigs),
		boolConfigCapability("slaveRead", "从节点读取", "高可用", []string{"slaveReadEnable"}, brokerConfigs),
		controllerCapability(brokerConfigs),
		popCapability(brokerConfigs),
		keyPresenceCapability("coldData", "冷数据流控", "存储", brokerConfigs, func(key string) bool {
			return strings.Contains(strings.ToLower(key), "cold")
		}),
		keyPresenceCapability("rocksdb", "RocksDB 配置", "存储", brokerConfigs, func(key string) bool {
			return strings.Contains(strings.ToLower(key), "rocksdb")
		}),
	}
}

// BuildTransactionRuntimeReport 汇总事务半消息 Topic、操作消息 Topic 和近期操作样本。
func BuildTransactionRuntimeReport(halfStatus *TopicStatus, opStatus *TopicStatus, operations []MessageDetail, warnings []string) TransactionRuntimeReport {
	report := TransactionRuntimeReport{
		Detail:   "未采集到事务系统 Topic 运行态",
		Warnings: uniqueStrings(warnings),
	}
	report.HalfTopic = transactionTopicRuntime("RMQ_SYS_TRANS_HALF_TOPIC", "事务半消息", halfStatus)
	report.OpTopic = transactionTopicRuntime("RMQ_SYS_TRANS_OP_HALF_TOPIC", "事务操作消息", opStatus)
	report.Supported = report.HalfTopic.Present && report.OpTopic.Present
	if report.Supported {
		report.Detail = "已采集事务半消息与操作消息 Topic 的队列水位"
	} else if report.HalfTopic.Present || report.OpTopic.Present {
		report.Detail = "仅采集到部分事务系统 Topic 运行态"
	}
	for _, message := range operations {
		sample := transactionOperationSample(message)
		report.RecentOperations = append(report.RecentOperations, sample)
		switch sample.Operation {
		case "commit":
			report.CommitCount++
		case "rollback":
			report.RollbackCount++
		case "cleanup":
			report.CleanupCount++
		default:
			report.UnknownCount++
		}
	}
	if report.OpTopic.Present && (report.CleanupCount > 0 || report.UnknownCount > 0) {
		report.Warnings = uniqueStrings(append(report.Warnings, "事务操作 Topic 的清理标记可能同时来自提交和回滚，未识别样本不作为精确回滚数量。"))
	}
	return report
}

func transactionTopicRuntime(topic string, label string, status *TopicStatus) TransactionTopicRuntime {
	runtime := TransactionTopicRuntime{
		Topic: topic,
		Label: label,
		Rows:  make([]TopicStatusRow, 0),
	}
	if status == nil {
		return runtime
	}
	runtime.Present = true
	runtime.Topic = firstNonEmpty(status.Topic, topic)
	runtime.TotalQueues = status.TotalQueues
	runtime.TotalMessageCount = status.TotalMessageCount
	runtime.MinOffsetTotal = status.MinOffsetTotal
	runtime.MaxOffsetTotal = status.MaxOffsetTotal
	runtime.Rows = append([]TopicStatusRow(nil), status.Rows...)
	for _, row := range status.Rows {
		if row.LastUpdated > runtime.LatestUpdated {
			runtime.LatestUpdated = row.LastUpdated
		}
	}
	return runtime
}

func transactionOperationSample(message MessageDetail) TransactionOperationSample {
	operation, label, evidence := classifyTransactionOperation(message)
	return TransactionOperationSample{
		MessageID:      message.MessageID,
		Operation:      operation,
		OperationLabel: label,
		BrokerName:     message.BrokerName,
		QueueID:        message.QueueID,
		QueueOffset:    message.QueueOffset,
		StoreTimestamp: message.StoreTimestamp,
		Keys:           append([]string(nil), message.Keys...),
		BodyPreview:    message.BodyPreview,
		Evidence:       evidence,
	}
}

func classifyTransactionOperation(message MessageDetail) (string, string, []string) {
	text := strings.ToLower(strings.Join(append([]string{message.BodyPreview, message.MessageID, message.TraceMessageID}, message.Keys...), " "))
	switch {
	case strings.Contains(text, "rollback") || strings.Contains(text, "commit_or_rollback=12") || strings.Contains(text, "commitorrollback=12"):
		return "rollback", "回滚", []string{"样本文本包含 rollback 或回滚标志"}
	case strings.Contains(text, "commit_message") || strings.Contains(text, "transaction_commit") || strings.Contains(text, "commit_or_rollback=8") || strings.Contains(text, "commitorrollback=8"):
		return "commit", "提交", []string{"样本文本包含 commit 或提交标志"}
	case strings.TrimSpace(strings.ToLower(message.BodyPreview)) == "d" || strings.Contains(text, "remove") || strings.Contains(text, "cleanup"):
		return "cleanup", "清理标记", []string{"样本仅能识别为事务半消息清理标记"}
	default:
		return "unknown", "未识别", []string{"样本缺少可直接区分提交或回滚的文本"}
	}
}

func buildCommonConfigPanels(brokerConfigs []BrokerConfigSnapshot) []CommonConfigPanel {
	panels := make([]CommonConfigPanel, 0)
	panelIndex := make(map[string]int)
	for _, definition := range commonConfigDefinitions {
		item, ok := buildCommonConfigItem(definition, brokerConfigs)
		if !ok {
			continue
		}
		index, exists := panelIndex[definition.category]
		if !exists {
			panelIndex[definition.category] = len(panels)
			panels = append(panels, CommonConfigPanel{Category: definition.category})
			index = len(panels) - 1
		}
		panels[index].Items = append(panels[index].Items, item)
	}
	return panels
}

func buildCommonConfigItem(definition commonConfigDefinition, brokerConfigs []BrokerConfigSnapshot) (CommonConfigItem, bool) {
	values := make([]string, 0)
	evidence := make([]string, 0)
	for _, broker := range brokerConfigs {
		for _, entry := range broker.Entries {
			if !strings.EqualFold(entry.Key, definition.key) || strings.TrimSpace(entry.Value) == "" {
				continue
			}
			values = append(values, strings.TrimSpace(entry.Value))
			evidence = append(evidence, formatConfigEvidence(broker, entry))
		}
	}
	values = uniqueStrings(values)
	if len(values) == 0 {
		return CommonConfigItem{}, false
	}
	return CommonConfigItem{
		Key:         definition.key,
		Label:       definition.label,
		Value:       commonConfigValueText(values),
		Status:      commonConfigStatus(values),
		Description: definition.description,
		Impact:      definition.impact,
		Evidence:    uniqueStrings(evidence),
	}, true
}

func commonConfigValueText(values []string) string {
	if len(values) == 1 {
		return values[0]
	}
	return "不同 Broker 不一致: " + strings.Join(values, " / ")
}

func commonConfigStatus(values []string) string {
	trueSeen := false
	falseSeen := false
	boolSeen := false
	for _, value := range values {
		parsed, ok := parseConfigBool(value)
		if !ok {
			continue
		}
		boolSeen = true
		if parsed {
			trueSeen = true
		} else {
			falseSeen = true
		}
	}
	switch {
	case boolSeen && trueSeen && falseSeen:
		return "mixed"
	case boolSeen && trueSeen:
		return "enabled"
	case boolSeen && falseSeen:
		return "disabled"
	case len(values) > 1:
		return "mixed"
	default:
		return "configured"
	}
}

func transactionCapability(topics []FeatureTopic, brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	evidence := make([]string, 0)
	half := featureTopicPresent(topics, "RMQ_SYS_TRANS_HALF_TOPIC")
	op := featureTopicPresent(topics, "RMQ_SYS_TRANS_OP_HALF_TOPIC")
	if half {
		evidence = append(evidence, "Topic RMQ_SYS_TRANS_HALF_TOPIC 已注册")
	}
	if op {
		evidence = append(evidence, "Topic RMQ_SYS_TRANS_OP_HALF_TOPIC 已注册")
	}
	evidence = append(evidence, configEvidenceForKeys(brokerConfigs, []string{"transactionCheckInterval", "transactionTimeOut", "transactionTimeout", "transactionCheckMax"}, 6)...)
	status := "unknown"
	detail := "未发现完整事务系统 Topic"
	if half && op {
		status = "supported"
		detail = "已发现事务半消息与操作消息 Topic"
	} else if len(evidence) > 0 {
		status = "partial"
		detail = "发现部分事务配置或系统 Topic"
	}
	return capability("transaction", "事务消息", "消息类型", status, detail, evidence)
}

func traceCapability(topics []FeatureTopic, brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	topic := featureTopicPresent(topics, "RMQ_SYS_TRACE_TOPIC")
	state, evidence := boolConfigState(brokerConfigs, []string{"traceTopicEnable", "traceOn"})
	evidence = append(evidence, configEvidenceForKeys(brokerConfigs, []string{"msgTraceTopicName"}, 4)...)
	if topic {
		evidence = append(evidence, "Topic RMQ_SYS_TRACE_TOPIC 已注册")
	}
	status := "unknown"
	detail := "未发现 Trace 开启证据"
	switch {
	case topic || state == "enabled":
		status = "enabled"
		detail = "Trace Topic 或 Broker Trace 开关已开启"
	case state == "disabled":
		status = "disabled"
		detail = "Broker Trace 开关显示为关闭"
	}
	return capability("trace", "消息 Trace", "可观测", status, detail, evidence)
}

func delayCapability(topics []FeatureTopic, brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	evidence := configEvidenceForKeys(brokerConfigs, []string{"messageDelayLevel"}, 4)
	if featureTopicPresent(topics, "SCHEDULE_TOPIC_XXXX") {
		evidence = append(evidence, "Topic SCHEDULE_TOPIC_XXXX 已注册")
	}
	status := "unknown"
	detail := "未发现延时消息 Topic 或延时级别配置"
	if len(evidence) > 0 {
		status = "supported"
		detail = "已发现延时级别配置或系统 Topic"
	}
	return capability("delay", "延时消息", "消息类型", status, detail, evidence)
}

func timerCapability(topics []FeatureTopic, brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	evidence := configEvidenceForKeys(brokerConfigs, []string{"timerStopEnqueue", "timerWheelEnable", "timerMaxDelaySec", "timerPrecisionMs"}, 6)
	evidence = append(evidence, configEvidenceForKeyPrefix(brokerConfigs, "timer", 6)...)
	if featureTopicPresent(topics, "rmq_sys_wheel_timer") {
		evidence = append(evidence, "Topic rmq_sys_wheel_timer 已注册")
	}
	status := "unknown"
	detail := "未发现 timer wheel 配置"
	if len(evidence) > 0 {
		status = "supported"
		detail = "已发现定时消息相关配置"
	}
	if state, _ := boolConfigState(brokerConfigs, []string{"timerStopEnqueue"}); state == "enabled" {
		status = "warning"
		detail = "timerStopEnqueue 为 true，定时消息入队可能被暂停"
	}
	return capability("timer", "定时消息", "消息类型", status, detail, uniqueStrings(evidence))
}

func aclCapability(brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	keys := []string{"aclEnable", "enableAcl", "authenticationEnabled", "authorizationEnabled"}
	state, evidence := boolConfigState(brokerConfigs, keys)
	status := "unknown"
	detail := "未发现 ACL/Auth 配置项"
	switch state {
	case "enabled":
		status = "enabled"
		detail = "发现 ACL/Auth 开关为 true"
	case "disabled":
		status = "disabled"
		detail = "ACL/Auth 开关显示为关闭"
	}
	return capability("acl", "ACL / Auth", "安全", status, detail, evidence)
}

func controllerCapability(brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	state, evidence := boolConfigState(brokerConfigs, []string{"enableControllerMode", "allAckInSyncStateSet"})
	evidence = append(evidence, configEvidenceForKeys(brokerConfigs, []string{"controllerAddr", "controllerDLegerGroup", "controllerDLegerPeers"}, 5)...)
	status := "unknown"
	detail := "未发现 Controller/同步复制配置"
	if len(evidence) > 0 {
		status = "supported"
		detail = "发现 Controller 或同步复制相关配置"
	}
	if state == "enabled" {
		status = "enabled"
	}
	return capability("controller", "Controller / 同步复制", "高可用", status, detail, uniqueStrings(evidence))
}

func popCapability(brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	return keyPresenceCapability("pop", "POP 消费", "Consumer", brokerConfigs, func(key string) bool {
		lower := strings.ToLower(key)
		return strings.HasPrefix(lower, "pop") || strings.Contains(lower, "pop")
	})
}

func boolConfigCapability(key string, label string, category string, configKeys []string, brokerConfigs []BrokerConfigSnapshot) FeatureCapability {
	state, evidence := boolConfigState(brokerConfigs, configKeys)
	status := "unknown"
	detail := "未发现配置项"
	switch state {
	case "enabled":
		status = "enabled"
		detail = "至少一个 Broker 配置为 true"
	case "disabled":
		status = "disabled"
		detail = "Broker 配置显示为 false"
	case "mixed":
		status = "partial"
		detail = "不同 Broker 配置不一致"
	}
	return capability(key, label, category, status, detail, evidence)
}

func keyPresenceCapability(key string, label string, category string, brokerConfigs []BrokerConfigSnapshot, match func(string) bool) FeatureCapability {
	evidence := make([]string, 0)
	for _, broker := range brokerConfigs {
		for _, entry := range broker.Entries {
			if !match(entry.Key) {
				continue
			}
			evidence = append(evidence, formatConfigEvidence(broker, entry))
			if len(evidence) >= 8 {
				break
			}
		}
		if len(evidence) >= 8 {
			break
		}
	}
	status := "unknown"
	detail := "未发现相关配置项"
	if len(evidence) > 0 {
		status = "supported"
		detail = "发现相关配置项"
	}
	return capability(key, label, category, status, detail, uniqueStrings(evidence))
}

func capability(key string, label string, category string, status string, detail string, evidence []string) FeatureCapability {
	return FeatureCapability{
		Key:      key,
		Label:    label,
		Category: category,
		Status:   status,
		Detail:   detail,
		Evidence: uniqueStrings(evidence),
	}
}

func featureTopicPresent(topics []FeatureTopic, name string) bool {
	for _, topic := range topics {
		if topic.Name == name && topic.Present {
			return true
		}
	}
	return false
}

func configHighlights(entries []ConfigEntry) []ConfigEntry {
	result := make([]ConfigEntry, 0, len(brokerConfigHighlightKeys))
	seen := make(map[string]bool)
	for _, key := range brokerConfigHighlightKeys {
		if value := configEntryValue(entries, key); value != "" {
			result = append(result, ConfigEntry{Key: key, Value: value})
			seen[strings.ToLower(key)] = true
		}
	}
	for _, entry := range entries {
		lower := strings.ToLower(entry.Key)
		if seen[lower] {
			continue
		}
		if strings.Contains(lower, "transaction") || strings.Contains(lower, "trace") || strings.HasPrefix(lower, "timer") || strings.Contains(lower, "acl") {
			result = append(result, entry)
			seen[lower] = true
		}
	}
	return result
}

func configEntryValue(entries []ConfigEntry, key string) string {
	for _, entry := range entries {
		if strings.EqualFold(entry.Key, key) {
			return strings.TrimSpace(entry.Value)
		}
	}
	return ""
}

func boolConfigState(brokerConfigs []BrokerConfigSnapshot, keys []string) (string, []string) {
	trueSeen := false
	falseSeen := false
	evidence := make([]string, 0)
	for _, broker := range brokerConfigs {
		for _, entry := range broker.Entries {
			if !configKeyIn(entry.Key, keys) {
				continue
			}
			value := strings.TrimSpace(entry.Value)
			if value == "" {
				continue
			}
			parsed, ok := parseConfigBool(value)
			if !ok {
				continue
			}
			if parsed {
				trueSeen = true
			} else {
				falseSeen = true
			}
			evidence = append(evidence, formatConfigEvidence(broker, entry))
			if len(evidence) >= 8 {
				break
			}
		}
	}
	switch {
	case trueSeen && falseSeen:
		return "mixed", uniqueStrings(evidence)
	case trueSeen:
		return "enabled", uniqueStrings(evidence)
	case falseSeen:
		return "disabled", uniqueStrings(evidence)
	default:
		return "unknown", uniqueStrings(evidence)
	}
}

func configEvidenceForKeys(brokerConfigs []BrokerConfigSnapshot, keys []string, limit int) []string {
	evidence := make([]string, 0)
	for _, broker := range brokerConfigs {
		for _, entry := range broker.Entries {
			if !configKeyIn(entry.Key, keys) || strings.TrimSpace(entry.Value) == "" {
				continue
			}
			evidence = append(evidence, formatConfigEvidence(broker, entry))
			if len(evidence) >= limit {
				return uniqueStrings(evidence)
			}
		}
	}
	return uniqueStrings(evidence)
}

func configEvidenceForKeyPrefix(brokerConfigs []BrokerConfigSnapshot, prefix string, limit int) []string {
	evidence := make([]string, 0)
	for _, broker := range brokerConfigs {
		for _, entry := range broker.Entries {
			if !strings.HasPrefix(strings.ToLower(entry.Key), strings.ToLower(prefix)) || strings.TrimSpace(entry.Value) == "" {
				continue
			}
			evidence = append(evidence, formatConfigEvidence(broker, entry))
			if len(evidence) >= limit {
				return uniqueStrings(evidence)
			}
		}
	}
	return uniqueStrings(evidence)
}

func configKeyIn(key string, keys []string) bool {
	for _, candidate := range keys {
		if strings.EqualFold(key, candidate) {
			return true
		}
	}
	return false
}

func parseConfigBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "y", "on":
		return true, true
	case "false", "0", "no", "n", "off":
		return false, true
	default:
		return false, false
	}
}

func formatConfigEvidence(broker BrokerConfigSnapshot, entry ConfigEntry) string {
	source := broker.BrokerName
	if source == "" {
		source = broker.BrokerAddr
	}
	if source == "" {
		source = "broker"
	}
	return fmt.Sprintf("%s.%s=%s", source, entry.Key, entry.Value)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}
