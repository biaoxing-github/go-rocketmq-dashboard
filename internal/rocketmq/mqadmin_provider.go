package rocketmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errSidecarUnavailable = errors.New("admin sidecar unavailable")

// CommandRunner 表示一次 RocketMQ 官方管理命令执行入口，可由 mqadmin 进程或常驻 Java sidecar 承接。
type CommandRunner interface {
	Run(ctx context.Context, args ...string) (string, error)
}

// CommandRunnerFunc 让函数可直接作为 CommandRunner 使用，测试和 fallback 组合更轻量。
type CommandRunnerFunc func(ctx context.Context, args ...string) (string, error)

func (f CommandRunnerFunc) Run(ctx context.Context, args ...string) (string, error) {
	return f(ctx, args...)
}

// MQAdminProvider 通过 RocketMQ 官方 tools 执行只读管理命令，作为 JVM sidecar 落地前的真实线上 Provider。
type MQAdminProvider struct {
	NameServer       string
	JavaPath         string
	MavenRepository  string
	Classpath        string
	ClasspathFile    string
	Version          string
	Timeout          time.Duration
	CommandRunner    CommandRunner
	SidecarEnabled   bool
	SidecarAddr      string
	SidecarClasspath string
	SidecarMainClass string
	SidecarTimeout   time.Duration
	SidecarTransport CommandRunner
	MessageCacheTTL  time.Duration
	// TopicMetadataCacheTTL 控制 exportMetadata 结果缓存时间，Dashboard 内写操作会主动清空缓存。
	TopicMetadataCacheTTL time.Duration

	cacheOnce             sync.Once
	messageDetailCache    *providerTTLCache[MessageDetail]
	messageTraceCache     *providerTTLCache[[]TraceEvent]
	consumerStateCache    *providerTTLCache[[]ConsumerState]
	topicMessageTypeCache *providerTTLCache[map[string]string]
}

type providerTTLCache[T any] struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]providerTTLCacheEntry[T]
}

type providerTTLCacheEntry[T any] struct {
	value T
	at    time.Time
}

func newProviderTTLCache[T any](ttl time.Duration) *providerTTLCache[T] {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	return &providerTTLCache[T]{ttl: ttl, entries: make(map[string]providerTTLCacheEntry[T])}
}

func (c *providerTTLCache[T]) get(key string, now time.Time) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || now.After(entry.at.Add(c.ttl)) {
		var zero T
		return zero, false
	}
	return entry.value, true
}

func (c *providerTTLCache[T]) set(key string, value T, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = providerTTLCacheEntry[T]{value: value, at: now}
}

func (c *providerTTLCache[T]) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.entries)
}

func (p *MQAdminProvider) ensureMessageCaches() {
	p.cacheOnce.Do(func() {
		ttl := p.MessageCacheTTL
		if ttl <= 0 {
			ttl = 30 * time.Minute
		}
		p.messageDetailCache = newProviderTTLCache[MessageDetail](ttl)
		p.messageTraceCache = newProviderTTLCache[[]TraceEvent](ttl)
		p.consumerStateCache = newProviderTTLCache[[]ConsumerState](ttl)
		topicMetadataTTL := p.TopicMetadataCacheTTL
		if topicMetadataTTL <= 0 {
			topicMetadataTTL = 30 * time.Second
		}
		p.topicMessageTypeCache = newProviderTTLCache[map[string]string](topicMetadataTTL)
	})
}

// ClusterList 读取 RocketMQ broker 自报的集群、地址和版本信息。
func (p *MQAdminProvider) ClusterList(ctx context.Context) ([]Cluster, error) {
	output, err := p.run(ctx, "clusterList", "-n", p.NameServer)
	if err != nil {
		return nil, err
	}
	return ParseClusterList(output)
}

// BrokerStatus 读取指定 Broker 地址的运行时指标，直接访问 Broker，适合 NameServer topic 路由抖动时继续排障。
func (p *MQAdminProvider) BrokerStatus(ctx context.Context, brokerAddr string) (BrokerStatus, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	if brokerAddr == "" {
		return BrokerStatus{}, errors.New("brokerAddr 必填")
	}
	output, err := p.run(ctx, "brokerStatus", "-b", brokerAddr)
	if err != nil {
		return BrokerStatus{}, err
	}
	return ParseBrokerStatus(brokerAddr, output)
}

// TopicList 读取 nameserver 上已注册的 Topic 列表。
func (p *MQAdminProvider) TopicList(ctx context.Context) ([]Topic, error) {
	output, err := p.run(ctx, "topicList", "-n", p.NameServer)
	if err != nil {
		return nil, err
	}
	topics := ParseTopicList(output)
	messageTypes, err := p.topicMessageTypes(ctx)
	if err != nil {
		return topics, nil
	}
	for index := range topics {
		if messageType := strings.TrimSpace(messageTypes[topics[index].Name]); messageType != "" {
			topics[index].MessageType = messageType
		}
	}
	return topics, nil
}

// topicMessageTypes 通过官方 exportMetadata 读取所有集群的 Topic 属性，并用短缓存避免频繁导出元数据。
func (p *MQAdminProvider) topicMessageTypes(ctx context.Context) (map[string]string, error) {
	p.ensureMessageCaches()
	cacheKey := strings.TrimSpace(p.NameServer)
	if cached, ok := p.topicMessageTypeCache.get(cacheKey, time.Now()); ok {
		return cached, nil
	}

	clusters, err := p.ClusterList(ctx)
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "rmqdash-topic-metadata-")
	if err != nil {
		return nil, fmt.Errorf("创建 Topic 元数据临时目录失败: %w", err)
	}
	defer os.RemoveAll(root)

	messageTypes := make(map[string]string)
	seenClusters := make(map[string]bool)
	for index, cluster := range clusters {
		clusterName := strings.TrimSpace(cluster.Name)
		if clusterName == "" || seenClusters[clusterName] {
			continue
		}
		seenClusters[clusterName] = true
		targetDir := filepath.Join(root, strconv.Itoa(index))
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			return nil, fmt.Errorf("创建集群 %s 的 Topic 元数据目录失败: %w", clusterName, err)
		}
		if _, err := p.run(ctx,
			"exportMetadata",
			"-n", p.NameServer,
			"-c", clusterName,
			"-t",
			"-f", targetDir,
		); err != nil {
			return nil, err
		}
		metadata, err := os.ReadFile(filepath.Join(targetDir, "topic.json"))
		if err != nil {
			return nil, fmt.Errorf("读取集群 %s 的 Topic 元数据失败: %w", clusterName, err)
		}
		clusterTypes, err := ParseTopicMessageTypes(string(metadata))
		if err != nil {
			return nil, err
		}
		for topic, messageType := range clusterTypes {
			messageType = strings.ToUpper(strings.TrimSpace(messageType))
			if current := messageTypes[topic]; current != "" && current != messageType {
				messageTypes[topic] = "MIXED"
				continue
			}
			messageTypes[topic] = messageType
		}
	}
	if len(seenClusters) == 0 {
		return nil, errors.New("clusterList 未返回可导出 Topic 元数据的集群")
	}
	p.topicMessageTypeCache.set(cacheKey, messageTypes, time.Now())
	return messageTypes, nil
}

func (p *MQAdminProvider) clearTopicMessageTypeCache() {
	p.ensureMessageCaches()
	p.topicMessageTypeCache.clear()
}

// TopicRoute 读取指定 Topic 的 Broker 路由、读写队列和权限分布。
func (p *MQAdminProvider) TopicRoute(ctx context.Context, topic string) (TopicRoute, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return TopicRoute{}, errors.New("topic 必填")
	}
	output, err := p.run(ctx, "topicRoute", "-n", p.NameServer, "-t", topic)
	if err != nil {
		return TopicRoute{}, err
	}
	return ParseTopicRoute(topic, output)
}

// TopicStatus 读取指定 Topic 的队列位点、水位和最后写入时间。
func (p *MQAdminProvider) TopicStatus(ctx context.Context, topic string) (TopicStatus, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return TopicStatus{}, errors.New("topic 必填")
	}
	output, err := p.run(ctx, "topicStatus", "-n", p.NameServer, "-t", topic)
	if err != nil {
		return TopicStatus{}, err
	}
	return ParseTopicStatus(topic, output)
}

// TopicMessages 基于 topicStatus 的队列水位，用官方 queryMsgByOffset 回查保留窗口内最近消息。
func (p *MQAdminProvider) TopicMessages(ctx context.Context, query MessageBrowseQuery) (TopicMessages, error) {
	return p.topicMessages(ctx, query, TopicMessages{})
}

// TopicMessagesIncremental 基于上一轮快照复用已查过的队列位点，刷新时只对缺失 offset 回源。
func (p *MQAdminProvider) TopicMessagesIncremental(ctx context.Context, query MessageBrowseQuery, previous TopicMessages) (TopicMessages, error) {
	return p.topicMessages(ctx, query, previous)
}

func (p *MQAdminProvider) topicMessages(ctx context.Context, query MessageBrowseQuery, previous TopicMessages) (TopicMessages, error) {
	query = normalizeBrowseQuery(query)
	if query.Topic == "" {
		return TopicMessages{}, errors.New("topic 必填")
	}

	status, err := p.TopicStatus(ctx, query.Topic)
	if err != nil {
		return TopicMessages{}, err
	}
	rows := filterBrowseQueues(status.Rows, query)
	if len(rows) == 0 {
		return TopicMessages{}, errors.New("未找到可浏览的 Topic 队列")
	}

	return collectTopicMessagesByOffset(ctx, query, rows, previous, p.messageByOffset)
}

type messageByOffsetFunc func(ctx context.Context, topic string, brokerName string, queueID int, offset int64) (MessageDetail, error)

// collectTopicMessagesByOffset 按 topicStatus 位点倒序收集消息，并复用旧快照中已经回查过的 offset。
func collectTopicMessagesByOffset(ctx context.Context, query MessageBrowseQuery, rows []TopicStatusRow, previous TopicMessages, messageByOffset messageByOffsetFunc) (TopicMessages, error) {
	query = normalizeBrowseQuery(query)
	cachedMessages := previousTopicMessagesByOffset(previous)
	result := TopicMessages{
		Topic:      query.Topic,
		BrokerName: query.BrokerName,
		QueueID:    query.QueueID,
		Limit:      query.Limit,
		Rows:       make([]MessageDetail, 0, query.Limit),
		Warnings:   make([]string, 0),
	}
	perQueueLimit := perQueueBrowseLimit(query.Limit, len(rows))
	for _, row := range rows {
		scannedInQueue := 0
		for offset := row.MaxOffset - 1; offset >= row.MinOffset && scannedInQueue < perQueueLimit; offset-- {
			result.ScannedOffsets++
			scannedInQueue++
			cacheKey := messageOffsetCacheKey(query.Topic, row.BrokerName, row.QueueID, offset)
			if cached, ok := cachedMessages[cacheKey]; ok {
				result.ReusedOffsets++
				result.Rows = append(result.Rows, cached)
				continue
			}
			result.FetchedOffsets++
			message, err := messageByOffset(ctx, query.Topic, row.BrokerName, row.QueueID, offset)
			if err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s/%d/%d: %s", row.BrokerName, row.QueueID, offset, err.Error()))
				continue
			}
			result.Rows = append(result.Rows, message)
		}
	}
	sort.SliceStable(result.Rows, func(left int, right int) bool {
		return result.Rows[left].StoreTimestamp > result.Rows[right].StoreTimestamp
	})
	if len(result.Rows) > query.Limit {
		result.Rows = result.Rows[:query.Limit]
	}
	if len(result.Rows) == 0 {
		return result, errors.New("未回查到可展示消息")
	}
	return result, nil
}

// UpsertTopic 使用官方 updateTopic 命令创建或更新 Topic，支持集群级和 Broker 级目标。
func (p *MQAdminProvider) UpsertTopic(ctx context.Context, request TopicConfigMutation) (TopicMutationResult, error) {
	request = request.Normalized()
	args, err := buildUpsertTopicArgs(p.NameServer, request)
	if err != nil {
		return TopicMutationResult{}, err
	}
	output, err := p.run(ctx, args...)
	if err != nil {
		return TopicMutationResult{}, err
	}
	p.clearTopicMessageTypeCache()
	return TopicMutationResult{
		Topic:     request.Topic,
		Operation: "upsertTopic",
		Target:    request.TargetLabel(),
		Output:    strings.TrimSpace(output),
	}, nil
}

// DeleteTopic 使用官方 deleteTopic 命令从集群和 NameServer 删除 Topic。
func (p *MQAdminProvider) DeleteTopic(ctx context.Context, request TopicDeleteRequest) (TopicMutationResult, error) {
	request = request.Normalized()
	args, err := buildDeleteTopicArgs(p.NameServer, request)
	if err != nil {
		return TopicMutationResult{}, err
	}
	output, err := p.run(ctx, args...)
	if err != nil {
		return TopicMutationResult{}, err
	}
	p.clearTopicMessageTypeCache()
	return TopicMutationResult{
		Topic:     request.Topic,
		Operation: "deleteTopic",
		Target:    request.ClusterName,
		Output:    strings.TrimSpace(output),
	}, nil
}

// SendTopicMessage 使用官方 sendMessage 命令向 Topic 写入一条消息，成功后返回可继续查链路的 messageId。
func (p *MQAdminProvider) SendTopicMessage(ctx context.Context, request TopicMessageSendRequest) (TopicMessageSendResult, error) {
	request = request.Normalized()
	args, err := buildSendTopicMessageArgs(p.NameServer, request)
	if err != nil {
		return TopicMessageSendResult{}, err
	}
	output, err := p.run(ctx, args...)
	if err != nil {
		return TopicMessageSendResult{}, err
	}
	result := parseSendTopicMessageResult(request.Topic, output)
	result.Operation = "sendMessage"
	result.Output = strings.TrimSpace(output)
	return result, nil
}

// ConsumerGroups 读取消费者组在线状态和堆积概览。
func (p *MQAdminProvider) ConsumerGroups(ctx context.Context) ([]ConsumerGroup, error) {
	output, err := p.run(ctx, "consumerProgress", "-n", p.NameServer)
	if err != nil {
		return nil, err
	}
	return ParseConsumerProgress(output)
}

// ConsumerDetail 读取指定消费者组的连接、订阅和队列进度详情。
func (p *MQAdminProvider) ConsumerDetail(ctx context.Context, group string, topic string) (ConsumerDetail, error) {
	group = strings.TrimSpace(group)
	topic = strings.TrimSpace(topic)
	if group == "" {
		return ConsumerDetail{}, errors.New("group 必填")
	}

	connectionSnapshot, err := p.consumerConnection(ctx, group)
	if err != nil {
		return ConsumerDetail{}, err
	}

	selectedTopic := topic
	if selectedTopic == "" {
		selectedTopic = chooseConsumerTopic(connectionSnapshot.Subscriptions)
	}
	detail := ConsumerDetail{
		Group:            group,
		Topic:            selectedTopic,
		ConsumeType:      connectionSnapshot.ConsumeType,
		MessageModel:     connectionSnapshot.MessageModel,
		ConsumeFromWhere: connectionSnapshot.ConsumeFromWhere,
		Connections:      connectionSnapshot.Connections,
		Subscriptions:    connectionSnapshot.Subscriptions,
	}
	if selectedTopic == "" {
		detail.ProgressError = "topic 必填"
		return detail, nil
	}

	progress, err := p.consumerProgressDetail(ctx, group, selectedTopic)
	if err != nil {
		detail.ProgressError = err.Error()
		return detail, nil
	}
	detail.ProgressRows = progress.Rows
	detail.ConsumeTPS = progress.ConsumeTPS
	detail.DiffTotal = progress.DiffTotal
	detail.InflightTotal = progress.InflightTotal
	return detail, nil
}

// ResetConsumerOffset 使用官方 resetOffsetByTime 命令重置消费者组消费点。
func (p *MQAdminProvider) ResetConsumerOffset(ctx context.Context, request ConsumerOffsetResetRequest) (ConsumerOffsetResetResult, error) {
	request = request.Normalized()
	args, err := buildResetConsumerOffsetArgs(p.NameServer, request)
	if err != nil {
		return ConsumerOffsetResetResult{}, err
	}
	output, err := p.run(ctx, args...)
	if err != nil {
		return ConsumerOffsetResetResult{}, err
	}
	return ConsumerOffsetResetResult{
		Group:     request.Group,
		Topic:     request.Topic,
		Operation: "resetOffsetByTime",
		Timestamp: request.Timestamp,
		Target:    request.TargetLabel(),
		Output:    strings.TrimSpace(output),
	}, nil
}

// RunCommand 执行一次官方 mqadmin 子命令，供 goadmin CLI 保持完整 mqadmin 兼容面。
func (p *MQAdminProvider) RunCommand(ctx context.Context, args ...string) (string, error) {
	return p.run(ctx, args...)
}

// ClusterFeatures 汇总当前 NameServer 发现到的系统 Topic、Broker 配置和可推断能力。
func (p *MQAdminProvider) ClusterFeatures(ctx context.Context) (ClusterFeatureReport, error) {
	clusters, err := p.ClusterList(ctx)
	if err != nil {
		return ClusterFeatureReport{}, err
	}
	warnings := make([]string, 0)
	topics, err := p.TopicList(ctx)
	if err != nil {
		warnings = append(warnings, "Topic 列表读取失败: "+err.Error())
	}

	brokerConfigs := make([]BrokerConfigSnapshot, 0)
	for _, cluster := range clusters {
		for _, broker := range cluster.Brokers {
			if strings.TrimSpace(broker.Address) == "" {
				continue
			}
			output, err := p.run(ctx, "getBrokerConfig", "-b", broker.Address)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("%s 配置读取失败: %s", broker.Address, err.Error()))
				continue
			}
			sections, err := ParseConfigSections(output)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("%s 配置解析失败: %s", broker.Address, err.Error()))
				continue
			}
			entries := flattenConfigSections(sections)
			brokerConfigs = append(brokerConfigs, BrokerConfigSnapshotFromEntries(broker, entries))
		}
	}

	nameServerConfigs := []NameServerConfigSnapshot(nil)
	if strings.TrimSpace(p.NameServer) != "" {
		output, err := p.run(ctx, "getNamesrvConfig", "-n", p.NameServer)
		if err != nil {
			warnings = append(warnings, "NameServer 配置读取失败: "+err.Error())
		} else {
			nameServerConfigs, err = ParseNameServerConfigs(output)
			if err != nil {
				warnings = append(warnings, "NameServer 配置解析失败: "+err.Error())
			}
		}
	}

	transactionRuntime, transactionWarnings := p.clusterTransactionRuntime(ctx, topics)
	warnings = append(warnings, transactionWarnings...)
	report := BuildClusterFeatureReport(p.NameServer, clusters, topics, brokerConfigs, nameServerConfigs, warnings)
	report.TransactionRuntime = transactionRuntime
	return report, nil
}

func flattenConfigSections(sections []ConfigSection) []ConfigEntry {
	entries := make([]ConfigEntry, 0)
	for _, section := range sections {
		entries = append(entries, section.Entries...)
	}
	return entries
}

func (p *MQAdminProvider) clusterTransactionRuntime(ctx context.Context, topics []Topic) (TransactionRuntimeReport, []string) {
	warnings := make([]string, 0)
	var halfStatus *TopicStatus
	var opStatus *TopicStatus
	if topicListContains(topics, "RMQ_SYS_TRANS_HALF_TOPIC") {
		status, err := p.TopicStatus(ctx, "RMQ_SYS_TRANS_HALF_TOPIC")
		if err != nil {
			warnings = append(warnings, "事务半消息 Topic 水位读取失败: "+err.Error())
		} else {
			halfStatus = &status
		}
	}
	if topicListContains(topics, "RMQ_SYS_TRANS_OP_HALF_TOPIC") {
		status, err := p.TopicStatus(ctx, "RMQ_SYS_TRANS_OP_HALF_TOPIC")
		if err != nil {
			warnings = append(warnings, "事务操作消息 Topic 水位读取失败: "+err.Error())
		} else {
			opStatus = &status
		}
	}
	halfMessages := []MessageDetail(nil)
	if halfStatus != nil && halfStatus.TotalMessageCount > 0 {
		messages, err := collectOldestTopicMessagesByOffset(ctx, MessageBrowseQuery{Topic: "RMQ_SYS_TRANS_HALF_TOPIC", QueueID: -1, Limit: 8}, halfStatus.Rows, p.messageByOffset)
		if err != nil {
			warnings = append(warnings, "事务半消息待决样本读取失败: "+err.Error())
		} else {
			halfMessages = messages.Rows
			warnings = append(warnings, messages.Warnings...)
		}
	}
	operations := []MessageDetail(nil)
	if opStatus != nil && opStatus.TotalMessageCount > 0 {
		messages, err := collectTopicMessagesByOffset(ctx, MessageBrowseQuery{Topic: "RMQ_SYS_TRANS_OP_HALF_TOPIC", QueueID: -1, Limit: 12}, opStatus.Rows, TopicMessages{}, p.messageByOffset)
		if err != nil {
			warnings = append(warnings, "事务操作消息样本读取失败: "+err.Error())
		} else {
			operations = messages.Rows
			warnings = append(warnings, messages.Warnings...)
		}
	}
	consumerImpact := BuildTransactionConsumerImpact(nil, topics)
	groups, err := p.ConsumerGroups(ctx)
	if err != nil {
		warnings = append(warnings, "消费组汇总读取失败: "+err.Error())
		consumerImpact.Warnings = append(consumerImpact.Warnings, err.Error())
	} else {
		consumerImpact = BuildTransactionConsumerImpact(groups, topics)
	}
	return BuildTransactionRuntimeReport(halfStatus, opStatus, halfMessages, operations, consumerImpact, warnings), warnings
}

// collectOldestTopicMessagesByOffset 按队列最小位点向后采样，专门用于估算半消息最老待决时间。
func collectOldestTopicMessagesByOffset(ctx context.Context, query MessageBrowseQuery, rows []TopicStatusRow, messageByOffset messageByOffsetFunc) (TopicMessages, error) {
	query = normalizeBrowseQuery(query)
	result := TopicMessages{
		Topic:      query.Topic,
		BrokerName: query.BrokerName,
		QueueID:    query.QueueID,
		Limit:      query.Limit,
		Rows:       make([]MessageDetail, 0, query.Limit),
		Warnings:   make([]string, 0),
	}
	perQueueLimit := perQueueBrowseLimit(query.Limit, len(rows))
	for _, row := range rows {
		scannedInQueue := 0
		for offset := row.MinOffset; offset < row.MaxOffset && scannedInQueue < perQueueLimit; offset++ {
			result.ScannedOffsets++
			scannedInQueue++
			result.FetchedOffsets++
			message, err := messageByOffset(ctx, query.Topic, row.BrokerName, row.QueueID, offset)
			if err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("%s/%d/%d: %s", row.BrokerName, row.QueueID, offset, err.Error()))
				continue
			}
			result.Rows = append(result.Rows, message)
		}
	}
	sort.SliceStable(result.Rows, func(left int, right int) bool {
		return result.Rows[left].StoreTimestamp < result.Rows[right].StoreTimestamp
	})
	if len(result.Rows) > query.Limit {
		result.Rows = result.Rows[:query.Limit]
	}
	if len(result.Rows) == 0 {
		return result, errors.New("未回查到可展示消息")
	}
	return result, nil
}

func topicListContains(topics []Topic, name string) bool {
	for _, topic := range topics {
		if topic.Name == name {
			return true
		}
	}
	return false
}

// MessageChain 查询单条消息详情，并叠加 trace 与指定消费者组位点，形成可视化状态链路。
func (p *MQAdminProvider) MessageChain(ctx context.Context, query MessageQuery) (MessageStatusChain, error) {
	query.Topic = strings.TrimSpace(query.Topic)
	query.MessageID = strings.TrimSpace(query.MessageID)
	query.Key = strings.TrimSpace(query.Key)
	query.BrokerName = strings.TrimSpace(query.BrokerName)
	query.ConsumerGroup = strings.TrimSpace(query.ConsumerGroup)
	query.TraceTopic = strings.TrimSpace(query.TraceTopic)
	if query.Topic == "" {
		return MessageStatusChain{}, errors.New("topic 必填")
	}
	if query.MessageID == "" && query.Key == "" {
		return MessageStatusChain{}, errors.New("messageId 或 key 至少传一个")
	}

	candidates := make([]MessageSearchResult, 0)
	messageID := query.MessageID
	var keyCandidate *MessageSearchResult
	if messageID == "" && query.Key != "" {
		results, err := p.searchMessageByKey(ctx, query)
		if err != nil {
			return MessageStatusChain{}, err
		}
		if len(results) == 0 {
			return MessageStatusChain{}, errors.New("未查询到匹配 key 的消息")
		}
		candidates = results
		keyCandidate = &results[0]
		messageID = results[0].MessageID
	}
	query.MessageID = messageID

	var message MessageDetail
	var err error
	if query.HasQueueOffset {
		message, err = p.cachedMessageByOffset(ctx, query)
	} else if keyCandidate != nil {
		message, err = p.cachedMessageByKeyCandidate(ctx, query, *keyCandidate)
	} else {
		message, err = p.cachedMessageDetail(ctx, query.Topic, messageID)
	}
	if err != nil {
		return MessageStatusChain{}, err
	}
	if len(message.Keys) == 0 && query.Key != "" {
		message.Keys = compactKeys(query.Key)
	}

	traces := p.traceEvents(ctx, query, message)
	consumerStates := []ConsumerState(nil)
	if !traceHasConsumerSuccess(query.ConsumerGroup, traces) {
		consumerStates = p.consumerStates(ctx, query)
	}
	chain := BuildMessageStatusChain(message, traces, consumerStates)
	chain.Candidates = candidates
	return chain, nil
}

func compactKeys(key string) []string {
	if key == "" {
		return nil
	}
	return []string{key}
}

// cachedMessageByKeyCandidate 用 queryMsgByKey 返回的队列位点回查消息详情，避免把候选 UNIQ_KEY 误当成 OffsetID。
func (p *MQAdminProvider) cachedMessageByKeyCandidate(ctx context.Context, query MessageQuery, candidate MessageSearchResult) (MessageDetail, error) {
	detailQuery := query
	detailQuery.QueueID = candidate.QueueID
	detailQuery.QueueOffset = candidate.QueueOffset
	detailQuery.HasQueueOffset = true
	rows, err := p.topicStatusRowsForKeyCandidate(ctx, detailQuery.Topic, detailQuery.BrokerName, candidate)
	if err != nil {
		return MessageDetail{}, err
	}
	var lastErr error
	for _, row := range rows {
		detailQuery.BrokerName = row.BrokerName
		message, err := p.cachedMessageByOffset(ctx, detailQuery)
		if err != nil {
			lastErr = err
			continue
		}
		if messageMatchesKeyCandidate(message, query.Key, candidate) {
			return message, nil
		}
		lastErr = fmt.Errorf("key 候选消息不匹配: broker=%s queue=%d offset=%d", row.BrokerName, candidate.QueueID, candidate.QueueOffset)
	}
	if lastErr != nil {
		return MessageDetail{}, lastErr
	}
	return MessageDetail{}, errors.New("未找到 key 候选消息所在队列")
}

// topicStatusRowsForKeyCandidate 根据候选 queueId/offset 找到真实 Broker；显式 brokerName 优先用于用户已知队列来源的场景。
func (p *MQAdminProvider) topicStatusRowsForKeyCandidate(ctx context.Context, topic string, brokerName string, candidate MessageSearchResult) ([]TopicStatusRow, error) {
	if strings.TrimSpace(brokerName) != "" {
		return []TopicStatusRow{{BrokerName: strings.TrimSpace(brokerName), QueueID: candidate.QueueID, MinOffset: candidate.QueueOffset, MaxOffset: candidate.QueueOffset + 1}}, nil
	}
	status, err := p.TopicStatus(ctx, topic)
	if err != nil {
		return nil, err
	}
	rows := make([]TopicStatusRow, 0, 1)
	for _, row := range status.Rows {
		if row.QueueID != candidate.QueueID {
			continue
		}
		if candidate.QueueOffset < row.MinOffset || candidate.QueueOffset >= row.MaxOffset {
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("未找到 key 候选消息所在队列: topic=%s queueId=%d offset=%d", topic, candidate.QueueID, candidate.QueueOffset)
	}
	return rows, nil
}

// messageMatchesKeyCandidate 校验回查到的 offset 详情仍属于当前 key 候选，防止多 Broker 同 queueId 时误取其它消息。
func messageMatchesKeyCandidate(message MessageDetail, key string, candidate MessageSearchResult) bool {
	if message.MessageID == candidate.MessageID || message.TraceMessageID == candidate.MessageID {
		return true
	}
	for _, value := range message.Keys {
		if value == key {
			return true
		}
	}
	return key == ""
}

func (p *MQAdminProvider) searchMessageByKey(ctx context.Context, query MessageQuery) ([]MessageSearchResult, error) {
	begin, end := keySearchWindow(query)
	maxNum := query.MaxNum
	if maxNum <= 0 {
		maxNum = 16
	}
	if maxNum > 64 {
		maxNum = 64
	}
	output, err := p.run(ctx,
		"queryMsgByKey",
		"-n", p.NameServer,
		"-t", query.Topic,
		"-k", query.Key,
		"-b", fmt.Sprintf("%d", begin),
		"-e", fmt.Sprintf("%d", end),
		"-m", fmt.Sprintf("%d", maxNum),
	)
	if err != nil {
		return nil, err
	}
	return ParseMessageSearchResults(output)
}

func (p *MQAdminProvider) cachedMessageDetail(ctx context.Context, topic string, messageID string) (MessageDetail, error) {
	p.ensureMessageCaches()
	key := messageDetailCacheKey(topic, messageID)
	if cached, ok := p.messageDetailCache.get(key, time.Now()); ok {
		return cached, nil
	}
	message, err := p.messageDetail(ctx, topic, messageID)
	if err != nil {
		return MessageDetail{}, err
	}
	p.messageDetailCache.set(key, message, time.Now())
	return message, nil
}

func (p *MQAdminProvider) messageDetail(ctx context.Context, topic string, messageID string) (MessageDetail, error) {
	output, err := p.run(ctx,
		"queryMsgById",
		"-n", p.NameServer,
		"-t", topic,
		"-i", messageID,
		"-f", "UTF-8",
	)
	if err != nil {
		return MessageDetail{}, err
	}
	return ParseMessageDetail(output)
}

func (p *MQAdminProvider) cachedMessageByOffset(ctx context.Context, query MessageQuery) (MessageDetail, error) {
	p.ensureMessageCaches()
	key := messageOffsetCacheKey(query.Topic, query.BrokerName, query.QueueID, query.QueueOffset)
	if cached, ok := p.messageDetailCache.get(key, time.Now()); ok {
		return cached, nil
	}
	message, err := p.messageByOffset(ctx, query.Topic, query.BrokerName, query.QueueID, query.QueueOffset)
	if err != nil {
		return MessageDetail{}, err
	}
	p.messageDetailCache.set(key, message, time.Now())
	if message.MessageID != "" {
		p.messageDetailCache.set(messageDetailCacheKey(query.Topic, message.MessageID), message, time.Now())
	}
	return message, nil
}

func (p *MQAdminProvider) messageByOffset(ctx context.Context, topic string, brokerName string, queueID int, offset int64) (MessageDetail, error) {
	output, err := p.run(ctx,
		"queryMsgByOffset",
		"-n", p.NameServer,
		"-t", topic,
		"-b", brokerName,
		"-i", fmt.Sprintf("%d", queueID),
		"-o", fmt.Sprintf("%d", offset),
		"-f", "UTF-8",
	)
	if err != nil {
		return MessageDetail{}, err
	}
	message, err := ParseMessageDetail(output)
	if err != nil {
		return MessageDetail{}, err
	}
	message.BrokerName = brokerName
	message.QueueID = queueID
	message.QueueOffset = offset
	return message, nil
}

func (p *MQAdminProvider) consumerConnection(ctx context.Context, group string) (ConsumerConnectionSnapshot, error) {
	output, err := p.run(ctx, "consumerConnection", "-n", p.NameServer, "-g", group)
	if err != nil {
		return ConsumerConnectionSnapshot{}, err
	}
	return ParseConsumerConnection(output)
}

func (p *MQAdminProvider) consumerProgressDetail(ctx context.Context, group string, topic string) (ConsumerProgressDetail, error) {
	output, err := p.run(ctx,
		"consumerProgress",
		"-n", p.NameServer,
		"-g", group,
		"-t", topic,
		"-s", "true",
	)
	if err != nil {
		return ConsumerProgressDetail{}, err
	}
	return ParseConsumerProgressDetail(output)
}

func (p *MQAdminProvider) traceEvents(ctx context.Context, query MessageQuery, message MessageDetail) []TraceEvent {
	begin, end := traceQueryWindow(query, message)
	p.ensureMessageCaches()
	cacheKey := traceEventsCacheKey(query, message, begin, end)
	if cached, ok := p.messageTraceCache.get(cacheKey, time.Now()); ok {
		return cached
	}
	args := []string{
		"queryMsgTraceById",
		"-n", p.NameServer,
		"-i", traceQueryMessageID(message),
		"-b", fmt.Sprintf("%d", begin),
		"-e", fmt.Sprintf("%d", end),
		"-c", "64",
	}
	if query.TraceTopic != "" {
		args = append(args, "-t", query.TraceTopic)
	}
	output, err := p.run(ctx, args...)
	if err != nil {
		events := []TraceEvent{{
			Stage:     "TRACE_MISSING",
			Group:     "trace",
			Timestamp: fallbackTraceTimestamp(message),
			Detail:    traceMissingDetail(err),
		}}
		p.messageTraceCache.set(cacheKey, events, time.Now())
		return events
	}
	events, err := ParseTraceEvents(output)
	if err != nil || len(events) == 0 {
		detail := traceMissingDetail(nil)
		if err != nil {
			detail = "trace 输出解析失败: " + err.Error()
		}
		events := []TraceEvent{{
			Stage:     "TRACE_MISSING",
			Group:     "trace",
			Timestamp: fallbackTraceTimestamp(message),
			Detail:    detail,
		}}
		p.messageTraceCache.set(cacheKey, events, time.Now())
		return events
	}
	p.messageTraceCache.set(cacheKey, events, time.Now())
	return events
}

func traceQueryMessageID(message MessageDetail) string {
	return firstNonEmpty(message.TraceMessageID, message.MessageID)
}

// traceMissingDetail 将 RocketMQ tools 的 trace 子命令异常收敛成用户可理解的边界说明。
func traceMissingDetail(err error) string {
	if err == nil {
		return "Trace 数据不可用：生产端未开启 traceEnable、trace topic 未保留，或当前消息已超过 trace 保留窗口；Broker 存储和 Consumer 位点仍可继续参考。"
	}
	message := err.Error()
	if strings.Contains(message, "QueryMsgTraceByIdSubCommand") || strings.Contains(message, "SubCommandException") {
		return "Trace 数据不可用：生产端未开启 traceEnable、trace topic 未保留，或当前消息已超过 trace 保留窗口；Broker 存储和 Consumer 位点仍可继续参考。"
	}
	return "Trace 数据不可用：" + mqadminFailureSummary(message)
}

// fallbackTraceTimestamp 让 trace 缺失告警贴近消息存储时间，避免当前时间把告警排成业务终态。
func fallbackTraceTimestamp(message MessageDetail) int64 {
	if message.StoreTimestamp > 0 {
		return message.StoreTimestamp
	}
	return time.Now().UnixMilli()
}

func (p *MQAdminProvider) consumerStates(ctx context.Context, query MessageQuery) []ConsumerState {
	if query.ConsumerGroup == "" {
		return nil
	}
	p.ensureMessageCaches()
	cacheKey := consumerStatesCacheKey(query)
	if cached, ok := p.consumerStateCache.get(cacheKey, time.Now()); ok {
		return cached
	}
	output, err := p.run(ctx,
		"consumerProgress",
		"-n", p.NameServer,
		"-g", query.ConsumerGroup,
		"-t", query.Topic,
		"-s", "true",
	)
	if err != nil {
		states := []ConsumerState{{
			Group:  query.ConsumerGroup,
			Topic:  query.Topic,
			Status: "CONSUMER_PROGRESS_FAILED",
			Lag:    0,
		}}
		p.consumerStateCache.set(cacheKey, states, time.Now())
		return states
	}
	states, err := ParseConsumerStates(query.ConsumerGroup, output)
	if err != nil {
		states := []ConsumerState{{
			Group:  query.ConsumerGroup,
			Topic:  query.Topic,
			Status: "CONSUMER_PROGRESS_FAILED",
			Lag:    0,
		}}
		p.consumerStateCache.set(cacheKey, states, time.Now())
		return states
	}
	p.consumerStateCache.set(cacheKey, states, time.Now())
	return states
}

func chooseConsumerTopic(subscriptions []ConsumerSubscription) string {
	for _, subscription := range subscriptions {
		if subscription.Topic == "" {
			continue
		}
		if !strings.HasPrefix(subscription.Topic, "%RETRY%") {
			return subscription.Topic
		}
	}
	if len(subscriptions) > 0 {
		return subscriptions[0].Topic
	}
	return ""
}

func normalizeBrowseQuery(query MessageBrowseQuery) MessageBrowseQuery {
	query.Topic = strings.TrimSpace(query.Topic)
	query.BrokerName = strings.TrimSpace(query.BrokerName)
	if query.QueueID < 0 {
		query.QueueID = -1
	}
	if query.Limit <= 0 {
		query.Limit = 12
	}
	if query.Limit > 24 {
		query.Limit = 24
	}
	return query
}

func filterBrowseQueues(rows []TopicStatusRow, query MessageBrowseQuery) []TopicStatusRow {
	filtered := make([]TopicStatusRow, 0, len(rows))
	for _, row := range rows {
		if row.MaxOffset <= row.MinOffset {
			continue
		}
		if query.BrokerName != "" && row.BrokerName != query.BrokerName {
			continue
		}
		if query.QueueID >= 0 && row.QueueID != query.QueueID {
			continue
		}
		filtered = append(filtered, row)
	}
	sort.SliceStable(filtered, func(left int, right int) bool {
		if filtered[left].LastUpdated != filtered[right].LastUpdated {
			return filtered[left].LastUpdated > filtered[right].LastUpdated
		}
		return filtered[left].MaxOffset > filtered[right].MaxOffset
	})
	return filtered
}

func perQueueBrowseLimit(limit int, queueCount int) int {
	if queueCount <= 0 {
		return limit
	}
	value := (limit + queueCount - 1) / queueCount
	if value < 1 {
		return 1
	}
	return value
}

func previousTopicMessagesByOffset(previous TopicMessages) map[string]MessageDetail {
	messages := make(map[string]MessageDetail, len(previous.Rows))
	for _, message := range previous.Rows {
		if message.Topic == "" || message.BrokerName == "" {
			continue
		}
		key := messageOffsetCacheKey(message.Topic, message.BrokerName, message.QueueID, message.QueueOffset)
		messages[key] = message
	}
	return messages
}

func messageOffsetCacheKey(topic string, brokerName string, queueID int, offset int64) string {
	return strings.Join([]string{
		topic,
		brokerName,
		strconv.Itoa(queueID),
		strconv.FormatInt(offset, 10),
	}, "\x00")
}

// buildUpsertTopicArgs 生成 mqadmin updateTopic 参数，保持和 RocketMQ 官方命令选项一一对应。
func buildUpsertTopicArgs(nameServer string, request TopicConfigMutation) ([]string, error) {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	args := []string{
		"updateTopic",
		"-n", nameServer,
		"-t", request.Topic,
		"-r", fmt.Sprintf("%d", request.ReadQueueNums),
		"-w", fmt.Sprintf("%d", request.WriteQueueNums),
		"-p", fmt.Sprintf("%d", request.Perm),
	}
	if request.Attributes != "" {
		args = append(args, "-a", request.Attributes)
	}
	if request.Order {
		args = append(args, "-o", "true")
	}
	if request.Unit {
		args = append(args, "-u", "true")
	}
	if request.HasUnitSub {
		args = append(args, "-s", "true")
	}
	if request.BrokerAddr != "" {
		args = append(args, "-b", request.BrokerAddr)
	} else {
		args = append(args, "-c", request.ClusterName)
	}
	return args, nil
}

// buildDeleteTopicArgs 生成 mqadmin deleteTopic 参数；RocketMQ 5.3.2 删除 Topic 要求集群目标。
func buildDeleteTopicArgs(nameServer string, request TopicDeleteRequest) ([]string, error) {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	return []string{"deleteTopic", "-n", nameServer, "-t", request.Topic, "-c", request.ClusterName}, nil
}

// buildSendTopicMessageArgs 生成 mqadmin sendMessage 参数，保持和 RocketMQ 官方命令选项一一对应。
func buildSendTopicMessageArgs(nameServer string, request TopicMessageSendRequest) ([]string, error) {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	args := []string{
		"sendMessage",
		"-n", nameServer,
		"-t", request.Topic,
		"-p", request.Body,
	}
	if request.Keys != "" {
		args = append(args, "-k", request.Keys)
	}
	if request.Tags != "" {
		args = append(args, "-c", request.Tags)
	}
	if request.BrokerName != "" {
		args = append(args, "-b", request.BrokerName)
	}
	if request.QueueID != nil {
		args = append(args, "-i", fmt.Sprintf("%d", *request.QueueID))
	}
	if request.TraceEnable {
		args = append(args, "-m", "true")
	}
	return args, nil
}

// buildResetConsumerOffsetArgs 生成 mqadmin resetOffsetByTime 参数，支持整个 Group/Topic 和单队列重置。
func buildResetConsumerOffsetArgs(nameServer string, request ConsumerOffsetResetRequest) ([]string, error) {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	args := []string{
		"resetOffsetByTime",
		"-n", nameServer,
		"-g", request.Group,
		"-t", request.Topic,
		"-s", request.Timestamp,
		"-f", fmt.Sprintf("%t", request.Force),
	}
	if request.BrokerAddr != "" && request.QueueID != nil {
		args = append(args, "-b", request.BrokerAddr, "-q", fmt.Sprintf("%d", *request.QueueID))
	}
	if request.Offset != nil {
		args = append(args, "-o", fmt.Sprintf("%d", *request.Offset))
	}
	return args, nil
}

// parseSendTopicMessageResult 从 sendMessage 表格输出中提取 Broker、队列、状态和 messageId。
func parseSendTopicMessageResult(topic string, output string) TopicMessageSendResult {
	result := TopicMessageSendResult{Topic: topic, QueueID: -1}
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		result.BrokerName = fields[0]
		if queueID, err := strconv.Atoi(fields[1]); err == nil {
			result.QueueID = queueID
		}
		result.SendStatus = fields[2]
		result.MessageID = fields[3]
	}
	return result
}

func messageDetailCacheKey(topic string, messageID string) string {
	return strings.Join([]string{"id", topic, messageID}, "\x00")
}

func traceEventsCacheKey(query MessageQuery, message MessageDetail, begin int64, end int64) string {
	return strings.Join([]string{
		query.Topic,
		traceQueryMessageID(message),
		query.TraceTopic,
		strconv.FormatInt(begin, 10),
		strconv.FormatInt(end, 10),
	}, "\x00")
}

func consumerStatesCacheKey(query MessageQuery) string {
	return strings.Join([]string{query.ConsumerGroup, query.Topic}, "\x00")
}

func traceHasConsumerSuccess(group string, traces []TraceEvent) bool {
	group = strings.TrimSpace(group)
	if group == "" {
		return true
	}
	for _, event := range traces {
		if event.Group != group {
			continue
		}
		if event.Stage == "CONSUME_SUCCESS" || event.Stage == "CONSUMED" {
			return true
		}
	}
	return false
}

func normalizeQueryWindow(begin int64, end int64) (int64, int64) {
	now := time.Now().UnixMilli()
	if end <= 0 {
		end = now
	}
	if begin <= 0 || begin > end {
		begin = end - 24*time.Hour.Milliseconds()
	}
	return begin, end
}

func keySearchWindow(query MessageQuery) (int64, int64) {
	if query.BeginTimestamp > 0 || query.EndTimestamp > 0 {
		return explicitQueryWindow(query.BeginTimestamp, query.EndTimestamp)
	}
	end := time.Now().UnixMilli()
	begin := end - 2*time.Hour.Milliseconds()
	return begin, end
}

func traceQueryWindow(query MessageQuery, message MessageDetail) (int64, int64) {
	if query.BeginTimestamp > 0 || query.EndTimestamp > 0 {
		return explicitQueryWindow(query.BeginTimestamp, query.EndTimestamp)
	}
	if message.StoreTimestamp > 0 {
		return message.StoreTimestamp - 30*time.Minute.Milliseconds(), message.StoreTimestamp + 30*time.Minute.Milliseconds()
	}
	return normalizeQueryWindow(0, 0)
}

// explicitQueryWindow 保留用户显式给出的 begin=0，用于查询历史 fixture 的全量时间窗。
func explicitQueryWindow(begin int64, end int64) (int64, int64) {
	if end <= 0 {
		end = time.Now().UnixMilli()
	}
	if begin < 0 {
		begin = 0
	}
	if begin > end {
		begin = end
	}
	return begin, end
}

func (p *MQAdminProvider) run(ctx context.Context, args ...string) (string, error) {
	if p.SidecarEnabled {
		if sidecar := p.sidecarRunner(); sidecar != nil {
			output, err := sidecar.Run(ctx, args...)
			if err == nil {
				return validateMQAdminOutput(output)
			}
			if !errors.Is(err, errSidecarUnavailable) {
				return "", err
			}
		}
	}
	runner := p.CommandRunner
	if runner != nil {
		output, err := runner.Run(ctx, args...)
		if err != nil {
			return "", err
		}
		return validateMQAdminOutput(output)
	}
	output, err := p.runProcess(ctx, args...)
	if err != nil {
		return "", err
	}
	return validateMQAdminOutput(output)
}

func (p *MQAdminProvider) sidecarRunner() CommandRunner {
	if p.SidecarTransport != nil {
		return p.SidecarTransport
	}
	addr := strings.TrimSpace(p.SidecarAddr)
	if addr == "" {
		return nil
	}
	timeout := p.SidecarTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return AdminSidecarClient{
		BaseURL: addr,
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func validateMQAdminOutput(output string) (string, error) {
	if failure := mqadminCommandFailure(output); failure != "" {
		return "", fmt.Errorf("mqadmin 命令输出异常: %s", failure)
	}
	return output, nil
}

func (p *MQAdminProvider) runProcess(ctx context.Context, args ...string) (string, error) {
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cp, err := p.classpath()
	if err != nil {
		return "", err
	}

	javaPath := p.JavaPath
	if javaPath == "" {
		javaPath = "java"
	}

	commandArgs := mqadminJavaArgs(cp, args)
	cmd := exec.CommandContext(cmdCtx, javaPath, commandArgs...)
	output, err := cmd.CombinedOutput()
	if cmdCtx.Err() != nil {
		return "", fmt.Errorf("mqadmin 命令超时: %w", cmdCtx.Err())
	}
	if err != nil {
		return "", fmt.Errorf("mqadmin 命令失败: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

type adminSidecarRunRequest struct {
	// Args 是传给官方 MQAdminStartup 的子命令参数列表，首项为 mqadmin 子命令名。
	Args []string `json:"args"`
}

type adminSidecarRunResponse struct {
	// Output 是官方 mqadmin 捕获到的 stdout/stderr 合并文本，必须原样返回给调用方。
	Output string `json:"output"`
	// Error 是 sidecar 执行阶段的异常类型和消息，非空时 Go 端按命令失败处理。
	Error string `json:"error,omitempty"`
	// Files 保存官方命令产生的相对路径文件内容，由 Go 端写回自己的当前工作目录。
	Files []adminSidecarOutputFile `json:"files,omitempty"`
}

type adminSidecarOutputFile struct {
	// Path 是官方命令输出表格中声明的相对文件路径，禁止 sidecar 返回绝对路径。
	Path string `json:"path"`
	// Content 是该相对文件的完整文本内容，Go 端落盘后复刻官方本地文件副作用。
	Content string `json:"content"`
}

// AdminSidecarClient 调用常驻 Java sidecar 的 /run 接口，让 Go 热路径复用同一个 RocketMQ tools JVM。
type AdminSidecarClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c AdminSidecarClient) Run(ctx context.Context, args ...string) (string, error) {
	baseURL, err := normalizeSidecarBaseURL(c.BaseURL)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(adminSidecarRunRequest{Args: args})
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/run", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errSidecarUnavailable, err)
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return "", fmt.Errorf("%w: %v", errSidecarUnavailable, readErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("%w: http %d: %s", errSidecarUnavailable, response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	var payload adminSidecarRunResponse
	if err := json.Unmarshal(responseBody, &payload); err != nil {
		return "", fmt.Errorf("%w: decode response: %v", errSidecarUnavailable, err)
	}
	if payload.Error != "" {
		return "", fmt.Errorf("admin sidecar command failed: %s: %s", payload.Error, strings.TrimSpace(payload.Output))
	}
	if err := writeSidecarOutputFiles(payload.Files); err != nil {
		return "", err
	}
	return payload.Output, nil
}

// writeSidecarOutputFiles 将 sidecar 返回的相对文件写入当前工作目录，复刻官方 mqadmin 的本地文件副作用。
func writeSidecarOutputFiles(files []adminSidecarOutputFile) error {
	for _, file := range files {
		relativePath := filepath.Clean(strings.TrimSpace(file.Path))
		if relativePath == "." || filepath.IsAbs(relativePath) || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) || relativePath == ".." {
			return fmt.Errorf("admin sidecar output file path invalid: %q", file.Path)
		}
		parent := filepath.Dir(relativePath)
		if parent != "." {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return err
			}
		}
		if err := os.WriteFile(relativePath, []byte(file.Content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func normalizeSidecarBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errSidecarUnavailable
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("%w: invalid sidecar addr %q", errSidecarUnavailable, raw)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

// mqadminJavaArgs 统一生成 Java 启动参数，并强制 stdout/stderr 使用 UTF-8，避免 Windows 默认编码污染消息体中文。
func mqadminJavaArgs(classpath string, args []string) []string {
	commandArgs := []string{
		"-Dfile.encoding=UTF-8",
		"-Dsun.stdout.encoding=UTF-8",
		"-Dsun.stderr.encoding=UTF-8",
		"-cp",
		classpath,
		"org.apache.rocketmq.tools.command.MQAdminStartup",
	}
	return append(commandArgs, args...)
}

// mqadminCommandFailure 识别 RocketMQ tools 零退出但只输出异常栈的情况，避免后续解析器误判为数据格式问题。
func mqadminCommandFailure(output string) string {
	if !strings.Contains(output, "SubCommandException") && !strings.Contains(output, "Caused by:") {
		return ""
	}
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "#") {
			return ""
		}
	}
	return mqadminFailureSummary(output)
}

func (p *MQAdminProvider) classpath() (string, error) {
	if p.Classpath != "" {
		return p.mergeCommandClasspath(p.Classpath), nil
	}
	if p.ClasspathFile != "" {
		content, err := os.ReadFile(p.ClasspathFile)
		if err != nil {
			return "", fmt.Errorf("读取 mqadmin classpath 文件失败: %w", err)
		}
		value := strings.TrimSpace(string(content))
		if value != "" {
			return p.mergeCommandClasspath(value), nil
		}
	}

	repo := p.MavenRepository
	if repo == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		repo = filepath.Join(home, ".m2", "repository")
	}

	version := p.Version
	if version == "" {
		version = "5.3.2"
	}

	parts := []string{
		rocketmqJar(repo, "rocketmq-tools", version),
		rocketmqJar(repo, "rocketmq-client", version),
		rocketmqJar(repo, "rocketmq-acl", version),
		rocketmqJar(repo, "rocketmq-remoting", version),
		rocketmqJar(repo, "rocketmq-common", version),
		rocketmqJar(repo, "rocketmq-srvutil", version),
		mavenJar(repo, "commons-cli", "commons-cli", "1.5.0"),
		mavenJar(repo, "commons-validator", "commons-validator", "1.7"),
		mavenJar(repo, "commons-beanutils", "commons-beanutils", "1.9.4"),
		mavenJar(repo, "commons-digester", "commons-digester", "2.1"),
		mavenJar(repo, "commons-collections", "commons-collections", "3.2.2"),
		jar(repo, "com", "alibaba", "fastjson", "1.2.83", "fastjson-1.2.83.jar"),
		jar(repo, "com", "alibaba", "fastjson2", "fastjson2", "2.0.43", "fastjson2-2.0.43.jar"),
		jar(repo, "org", "apache", "commons", "commons-lang3", "3.12.0", "commons-lang3-3.12.0.jar"),
		jar(repo, "org", "yaml", "snakeyaml", "2.2", "snakeyaml-2.2.jar"),
		jar(repo, "com", "google", "guava", "guava", "31.1-jre", "guava-31.1-jre.jar"),
		jar(repo, "com", "google", "guava", "failureaccess", "1.0.1", "failureaccess-1.0.1.jar"),
		jar(repo, "com", "github", "luben", "zstd-jni", "1.5.2-2", "zstd-jni-1.5.2-2.jar"),
		jar(repo, "org", "lz4", "lz4-java", "1.8.0", "lz4-java-1.8.0.jar"),
		jar(repo, "com", "google", "protobuf", "protobuf-java", "3.20.1", "protobuf-java-3.20.1.jar"),
		jar(repo, "com", "google", "protobuf", "protobuf-java-util", "3.20.1", "protobuf-java-util-3.20.1.jar"),
		jar(repo, "com", "google", "code", "gson", "gson", "2.10.1", "gson-2.10.1.jar"),
		jar(repo, "io", "github", "aliyunmq", "rocketmq-slf4j-api", "1.0.1", "rocketmq-slf4j-api-1.0.1.jar"),
		jar(repo, "io", "github", "aliyunmq", "rocketmq-logback-classic", "1.0.1", "rocketmq-logback-classic-1.0.1.jar"),
		jar(repo, "org", "slf4j", "slf4j-api", "1.7.36", "slf4j-api-1.7.36.jar"),
		jar(repo, "ch", "qos", "logback", "logback-classic", "1.2.12", "logback-classic-1.2.12.jar"),
		jar(repo, "ch", "qos", "logback", "logback-core", "1.2.12", "logback-core-1.2.12.jar"),
	}
	parts = append(parts, nettyJars(repo, "4.1.113.Final")...)

	existing := make([]string, 0, len(parts))
	for _, part := range parts {
		if _, err := os.Stat(part); err == nil {
			existing = append(existing, part)
		}
	}
	if len(existing) < 8 {
		return "", fmt.Errorf("RocketMQ tools classpath 不完整，已找到 %d 个 jar，请先执行项目 README 中的依赖预热命令", len(existing))
	}
	return strings.Join(existing, string(os.PathListSeparator)), nil
}

func (p *MQAdminProvider) mergeCommandClasspath(base string) string {
	addons := p.commandClasspathAddons()
	if addons == "" {
		return base
	}
	return base + string(os.PathListSeparator) + addons
}

func (p *MQAdminProvider) commandClasspathAddons() string {
	repo := p.MavenRepository
	if repo == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		repo = filepath.Join(home, ".m2", "repository")
	}

	version := p.Version
	if version == "" {
		version = "5.3.2"
	}

	parts := []string{
		rocketmqJar(repo, "rocketmq-tools", version),
		mavenJar(repo, "commons-cli", "commons-cli", "1.5.0"),
		mavenJar(repo, "commons-validator", "commons-validator", "1.7"),
		mavenJar(repo, "commons-beanutils", "commons-beanutils", "1.9.4"),
		mavenJar(repo, "commons-digester", "commons-digester", "2.1"),
		mavenJar(repo, "commons-collections", "commons-collections", "3.2.2"),
		jar(repo, "com", "alibaba", "fastjson2", "fastjson2", "2.0.43", "fastjson2-2.0.43.jar"),
		jar(repo, "com", "google", "guava", "guava", "31.1-jre", "guava-31.1-jre.jar"),
		jar(repo, "com", "google", "guava", "failureaccess", "1.0.1", "failureaccess-1.0.1.jar"),
		jar(repo, "com", "github", "luben", "zstd-jni", "1.5.2-2", "zstd-jni-1.5.2-2.jar"),
		jar(repo, "org", "lz4", "lz4-java", "1.8.0", "lz4-java-1.8.0.jar"),
		jar(repo, "com", "google", "protobuf", "protobuf-java", "3.20.1", "protobuf-java-3.20.1.jar"),
		jar(repo, "com", "google", "protobuf", "protobuf-java-util", "3.20.1", "protobuf-java-util-3.20.1.jar"),
		jar(repo, "com", "google", "code", "gson", "gson", "2.10.1", "gson-2.10.1.jar"),
		jar(repo, "io", "github", "aliyunmq", "rocketmq-slf4j-api", "1.0.1", "rocketmq-slf4j-api-1.0.1.jar"),
		jar(repo, "io", "github", "aliyunmq", "rocketmq-logback-classic", "1.0.1", "rocketmq-logback-classic-1.0.1.jar"),
	}
	parts = append(parts, nettyJars(repo, "4.1.113.Final")...)
	existing := make([]string, 0, len(parts))
	for _, part := range parts {
		if _, err := os.Stat(part); err == nil {
			existing = append(existing, part)
		}
	}
	return strings.Join(existing, string(os.PathListSeparator))
}

func rocketmqJar(repo, artifact, version string) string {
	return jar(repo, "org", "apache", "rocketmq", artifact, version, artifact+"-"+version+".jar")
}

func mavenJar(repo string, group string, artifact string, version string) string {
	return jar(repo, group, artifact, version, artifact+"-"+version+".jar")
}

func nettyJars(repo string, version string) []string {
	artifacts := []string{
		"netty-buffer",
		"netty-codec",
		"netty-codec-dns",
		"netty-codec-haproxy",
		"netty-codec-http",
		"netty-codec-http2",
		"netty-common",
		"netty-handler",
		"netty-handler-proxy",
		"netty-resolver",
		"netty-resolver-dns",
		"netty-transport",
		"netty-transport-classes-epoll",
		"netty-transport-classes-kqueue",
		"netty-transport-native-unix-common",
	}
	paths := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		paths = append(paths, jar(repo, "io", "netty", artifact, version, artifact+"-"+version+".jar"))
	}
	return paths
}

func jar(repo string, parts ...string) string {
	return filepath.Join(append([]string{repo}, parts...)...)
}
