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
