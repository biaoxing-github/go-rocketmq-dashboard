package rocketmq

// Cluster 表示 RocketMQ 集群，聚合同一集群下的 Broker 节点。
type Cluster struct {
	Name    string   `json:"name"`
	Brokers []Broker `json:"brokers"`
}

// Broker 表示 RocketMQ Broker 的运行状态，字段来自 mqadmin clusterList 输出。
type Broker struct {
	Cluster   string `json:"cluster"`
	Name      string `json:"name"`
	ID        string `json:"id"`
	Address   string `json:"address"`
	Version   string `json:"version"`
	InTPS     string `json:"inTps"`
	OutTPS    string `json:"outTps"`
	Activated bool   `json:"activated"`
}

// BrokerStatus 表示 Broker 运行时指标，来自 mqadmin brokerStatus 的 key/value 输出。
type BrokerStatus struct {
	// BrokerAddr 是当前 Broker 的直连地址，页面用它区分不同 Broker 的状态快照。
	BrokerAddr string `json:"brokerAddr"`
	// BrokerVersionDesc 是 brokerStatus 输出里的版本描述，通常用于排查版本差异。
	BrokerVersionDesc string `json:"brokerVersionDesc"`
	// BrokerRole 表示 Broker 当前角色，例如 MASTER、SLAVE 或 ASYNC_MASTER。
	BrokerRole string `json:"brokerRole"`
	// BootTimestamp 是 Broker 启动时间，便于判断重启和漂移。
	BootTimestamp string `json:"bootTimestamp"`
	// PutTps 表示写入 TPS 的原始输出。
	PutTps string `json:"putTps"`
	// GetFoundTps 表示命中读取 TPS 的原始输出。
	GetFoundTps string `json:"getFoundTps"`
	// GetTotalTps 表示读取总 TPS 的原始输出。
	GetTotalTps string `json:"getTotalTps"`
	// CommitLogCapacity 表示 commitlog 磁盘容量或剩余容量描述。
	CommitLogCapacity string `json:"commitLogCapacity"`
	// DispatchBehind 表示消息分发积压字节数或相关描述。
	DispatchBehind string `json:"dispatchBehind"`
	// RuntimeDescription 是前端用于展示的摘要文案。
	RuntimeDescription string `json:"runtimeDescription"`
	// Metrics 保留 brokerStatus 的完整指标列表，便于前端和后续排障继续扩展。
	Metrics []BrokerRuntimeMetric `json:"metrics"`
}

// BrokerRuntimeMetric 表示 brokerStatus 输出中的一行运行时指标。
type BrokerRuntimeMetric struct {
	// Key 是 brokerStatus 输出的指标名称。
	Key string `json:"key"`
	// Value 是对应指标的原始值。
	Value string `json:"value"`
}

// ConfigEntry 表示 RocketMQ 配置输出中的一项 key/value。
type ConfigEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ConfigSection 表示 getBrokerConfig/getNamesrvConfig 输出中的一个配置段。
type ConfigSection struct {
	Header  string        `json:"header"`
	Entries []ConfigEntry `json:"entries"`
}

// ClusterFeatureReport 汇总当前 NameServer 发现到的 Broker 配置、系统 Topic 和能力判断。
type ClusterFeatureReport struct {
	NameServer           string                     `json:"nameServer"`
	GeneratedAtUnixMilli int64                      `json:"generatedAtUnixMilli"`
	ClusterCount         int                        `json:"clusterCount"`
	BrokerCount          int                        `json:"brokerCount"`
	TopicCount           int                        `json:"topicCount"`
	SystemTopicCount     int                        `json:"systemTopicCount"`
	Capabilities         []FeatureCapability        `json:"capabilities"`
	SystemTopics         []FeatureTopic             `json:"systemTopics"`
	TransactionRuntime   TransactionRuntimeReport   `json:"transactionRuntime"`
	CommonConfigPanels   []CommonConfigPanel        `json:"commonConfigPanels"`
	BrokerConfigs        []BrokerConfigSnapshot     `json:"brokerConfigs"`
	NameServerConfigs    []NameServerConfigSnapshot `json:"nameServerConfigs"`
	Warnings             []string                   `json:"warnings"`
}

// FeatureCapability 表示一个 RocketMQ 能力或开关的当前推断状态。
type FeatureCapability struct {
	Key      string   `json:"key"`
	Label    string   `json:"label"`
	Category string   `json:"category"`
	Status   string   `json:"status"`
	Detail   string   `json:"detail"`
	Evidence []string `json:"evidence"`
}

// TransactionRuntimeReport 表示事务系统 Topic 的队列水位、操作消息样本和可识别提交/回滚证据。
type TransactionRuntimeReport struct {
	// Supported 表示半消息 Topic 与操作消息 Topic 是否都能从当前 NameServer 查到运行态。
	Supported bool `json:"supported"`
	// Detail 是给页面展示的事务运行态说明。
	Detail string `json:"detail"`
	// HalfTopic 是 RMQ_SYS_TRANS_HALF_TOPIC 的队列水位摘要。
	HalfTopic TransactionTopicRuntime `json:"halfTopic"`
	// OpTopic 是 RMQ_SYS_TRANS_OP_HALF_TOPIC 的队列水位摘要。
	OpTopic TransactionTopicRuntime `json:"opTopic"`
	// CommitCount 是近期操作样本里能明确识别为提交的数量。
	CommitCount int `json:"commitCount"`
	// RollbackCount 是近期操作样本里能明确识别为回滚的数量。
	RollbackCount int `json:"rollbackCount"`
	// CleanupCount 是近期操作样本里只能识别为半消息清理标记的数量。
	CleanupCount int `json:"cleanupCount"`
	// UnknownCount 是近期操作样本里无法识别操作语义的数量。
	UnknownCount int `json:"unknownCount"`
	// RecentOperations 是从操作消息 Topic 最近位点回查到的样本。
	RecentOperations []TransactionOperationSample `json:"recentOperations"`
	// Warnings 记录事务运行态采集或语义识别的非致命问题。
	Warnings []string `json:"warnings"`
}

// TransactionTopicRuntime 表示一个事务系统 Topic 的聚合队列水位。
type TransactionTopicRuntime struct {
	// Topic 是事务系统 Topic 名称。
	Topic string `json:"topic"`
	// Label 是页面展示的中文名称。
	Label string `json:"label"`
	// Present 表示该 Topic 的 topicStatus 是否采集成功。
	Present bool `json:"present"`
	// TotalQueues 是该 Topic 的队列数量。
	TotalQueues int `json:"totalQueues"`
	// TotalMessageCount 是所有队列 maxOffset-minOffset 的合计。
	TotalMessageCount int64 `json:"totalMessageCount"`
	// MinOffsetTotal 是所有队列最小位点合计。
	MinOffsetTotal int64 `json:"minOffsetTotal"`
	// MaxOffsetTotal 是所有队列最大位点合计。
	MaxOffsetTotal int64 `json:"maxOffsetTotal"`
	// LatestUpdated 是各队列最后更新时间里的最大值文本。
	LatestUpdated string `json:"latestUpdated"`
	// Rows 是原始队列水位行，用于页面展开查看。
	Rows []TopicStatusRow `json:"rows"`
}

// TransactionOperationSample 表示一条事务操作消息样本及其分类结果。
type TransactionOperationSample struct {
	// MessageID 是操作消息的 OffsetID 或消息 ID。
	MessageID string `json:"messageId"`
	// Operation 是机器可读分类：commit、rollback、cleanup 或 unknown。
	Operation string `json:"operation"`
	// OperationLabel 是页面展示的中文分类。
	OperationLabel string `json:"operationLabel"`
	// BrokerName 是样本所在 Broker。
	BrokerName string `json:"brokerName"`
	// QueueID 是样本所在队列 ID。
	QueueID int `json:"queueId"`
	// QueueOffset 是样本所在队列位点。
	QueueOffset int64 `json:"queueOffset"`
	// StoreTimestamp 是 Broker 存储时间戳。
	StoreTimestamp int64 `json:"storeTimestamp"`
	// Keys 是操作消息携带的 key 列表。
	Keys []string `json:"keys"`
	// BodyPreview 是操作消息体预览。
	BodyPreview string `json:"bodyPreview"`
	// Evidence 是用于解释分类依据的短文本。
	Evidence []string `json:"evidence"`
}

// CommonConfigPanel 按中文业务类别聚合常用 Broker 配置。
type CommonConfigPanel struct {
	// Category 是中文配置分类。
	Category string `json:"category"`
	// Items 是该分类下已在 Broker 配置中出现的常用配置项。
	Items []CommonConfigItem `json:"items"`
}

// CommonConfigItem 表示一个常用配置的中文说明、当前值和影响。
type CommonConfigItem struct {
	// Key 是 RocketMQ 原始配置键。
	Key string `json:"key"`
	// Label 是配置项中文名。
	Label string `json:"label"`
	// Value 是聚合后的当前配置值。
	Value string `json:"value"`
	// Status 是机器可读状态：enabled、disabled、mixed、configured 或 unknown。
	Status string `json:"status"`
	// Description 说明配置项控制的能力或行为。
	Description string `json:"description"`
	// Impact 说明该配置对日常运维和业务行为的影响。
	Impact string `json:"impact"`
	// Evidence 保留各 Broker 的实际 key=value 来源。
	Evidence []string `json:"evidence"`
}

// FeatureTopic 表示一个系统 Topic 是否在当前 NameServer 可见。
type FeatureTopic struct {
	Name    string `json:"name"`
	Label   string `json:"label"`
	Kind    string `json:"kind"`
	Present bool   `json:"present"`
	Detail  string `json:"detail"`
}

// BrokerConfigSnapshot 保留单个 Broker 的完整配置和常用关键配置。
type BrokerConfigSnapshot struct {
	Cluster    string        `json:"cluster"`
	BrokerName string        `json:"brokerName"`
	BrokerID   string        `json:"brokerId"`
	BrokerAddr string        `json:"brokerAddr"`
	Role       string        `json:"role"`
	Version    string        `json:"version"`
	Entries    []ConfigEntry `json:"entries"`
	Highlights []ConfigEntry `json:"highlights"`
}

// NameServerConfigSnapshot 保留单个 NameServer 返回的完整配置。
type NameServerConfigSnapshot struct {
	NameServer string        `json:"nameServer"`
	Entries    []ConfigEntry `json:"entries"`
}

// Topic 表示 RocketMQ Topic 列表项，Kind 用于前端区分普通、重试、死信和系统 Topic。
type Topic struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// TopicStatus 表示一个 Topic 当前每个队列的位点状态，来自 mqadmin topicStatus 输出。
type TopicStatus struct {
	Topic             string           `json:"topic"`
	TotalQueues       int              `json:"totalQueues"`
	TotalMessageCount int64            `json:"totalMessageCount"`
	MinOffsetTotal    int64            `json:"minOffsetTotal"`
	MaxOffsetTotal    int64            `json:"maxOffsetTotal"`
	Rows              []TopicStatusRow `json:"rows"`
}

// TopicStatusRow 表示 Topic 在某个 Broker 队列上的最小位点、最大位点和最后写入时间。
type TopicStatusRow struct {
	BrokerName   string `json:"brokerName"`
	QueueID      int    `json:"queueId"`
	MinOffset    int64  `json:"minOffset"`
	MaxOffset    int64  `json:"maxOffset"`
	MessageCount int64  `json:"messageCount"`
	LastUpdated  string `json:"lastUpdated"`
}

// TopicRoute 表示一个 Topic 在 Broker 上的路由分布，来自 mqadmin topicRoute JSON 输出。
type TopicRoute struct {
	Topic            string             `json:"topic"`
	TotalReadQueues  int                `json:"totalReadQueues"`
	TotalWriteQueues int                `json:"totalWriteQueues"`
	Queues           []TopicQueueRoute  `json:"queues"`
	Brokers          []TopicBrokerRoute `json:"brokers"`
}

// TopicQueueRoute 表示某个 Broker 对指定 Topic 承载的读写队列数量和权限位。
type TopicQueueRoute struct {
	BrokerName      string `json:"brokerName"`
	ReadQueueNums   int    `json:"readQueueNums"`
	WriteQueueNums  int    `json:"writeQueueNums"`
	Perm            int    `json:"perm"`
	PermissionLabel string `json:"permissionLabel"`
	TopicSysFlag    int    `json:"topicSysFlag"`
}

// TopicBrokerRoute 表示 Topic 路由里的 Broker 地址表，Addrs 保留 brokerId 到地址的映射。
type TopicBrokerRoute struct {
	Cluster    string            `json:"cluster"`
	BrokerName string            `json:"brokerName"`
	Addrs      map[string]string `json:"addrs"`
}

// ConsumerGroup 表示消费者组的在线状态、协议版本和堆积量。
type ConsumerGroup struct {
	Name      string `json:"name"`
	Count     int64  `json:"count"`
	Version   string `json:"version"`
	Type      string `json:"type"`
	Model     string `json:"model"`
	TPS       string `json:"tps"`
	DiffTotal int64  `json:"diffTotal"`
	Online    bool   `json:"online"`
}

// ConsumerDetail 表示 Consumer 页点击某个消费者组后展示的连接、订阅和队列进度详情。
type ConsumerDetail struct {
	Group            string                 `json:"group"`
	Topic            string                 `json:"topic"`
	ConsumeType      string                 `json:"consumeType"`
	MessageModel     string                 `json:"messageModel"`
	ConsumeFromWhere string                 `json:"consumeFromWhere"`
	Connections      []ConsumerConnection   `json:"connections"`
	Subscriptions    []ConsumerSubscription `json:"subscriptions"`
	ProgressRows     []ConsumerProgressRow  `json:"progressRows"`
	ConsumeTPS       float64                `json:"consumeTps"`
	DiffTotal        int64                  `json:"diffTotal"`
	InflightTotal    int64                  `json:"inflightTotal"`
	ConnectionError  string                 `json:"connectionError,omitempty"`
	ProgressError    string                 `json:"progressError,omitempty"`
}

// ConsumerConnection 表示消费者组内一个在线客户端连接。
type ConsumerConnection struct {
	ClientID   string `json:"clientId"`
	ClientAddr string `json:"clientAddr"`
	Language   string `json:"language"`
	Version    string `json:"version"`
}

// ConsumerSubscription 表示消费者组订阅的 Topic 和 tag/filter 表达式。
type ConsumerSubscription struct {
	Topic      string `json:"topic"`
	Expression string `json:"expression"`
}

// ConsumerProgressRow 表示消费者组在一个队列上的 broker/consumer 位点和堆积量。
type ConsumerProgressRow struct {
	Topic          string `json:"topic"`
	BrokerName     string `json:"brokerName"`
	QueueID        int    `json:"queueId"`
	BrokerOffset   int64  `json:"brokerOffset"`
	ConsumerOffset int64  `json:"consumerOffset"`
	ClientIP       string `json:"clientIp"`
	Diff           int64  `json:"diff"`
	Inflight       int64  `json:"inflight"`
	LastTime       string `json:"lastTime"`
}

// ConsumerConnectionSnapshot 是 consumerConnection 命令解析出的连接和订阅摘要。
type ConsumerConnectionSnapshot struct {
	Connections      []ConsumerConnection   `json:"connections"`
	Subscriptions    []ConsumerSubscription `json:"subscriptions"`
	ConsumeType      string                 `json:"consumeType"`
	MessageModel     string                 `json:"messageModel"`
	ConsumeFromWhere string                 `json:"consumeFromWhere"`
}

// ConsumerProgressDetail 是 consumerProgress -g 明细命令解析出的队列位点和汇总指标。
type ConsumerProgressDetail struct {
	Rows          []ConsumerProgressRow `json:"rows"`
	ConsumeTPS    float64               `json:"consumeTps"`
	DiffTotal     int64                 `json:"diffTotal"`
	InflightTotal int64                 `json:"inflightTotal"`
}

// MessageDetail 表示单条消息的基础元数据，后续由查询消息和轨迹查询共同填充。
type MessageDetail struct {
	MessageID string `json:"messageId"`
	Topic     string `json:"topic"`
	// BrokerName 是按队列位点浏览消息时的来源 Broker 名称，messageId 回查无法提供时保持为空。
	BrokerName string   `json:"brokerName"`
	Keys       []string `json:"keys"`
	// TraceMessageID 是 RocketMQ 消息属性里的 UNIQ_KEY，queryMsgTraceById 需要用它而不是 Broker OffsetID 查询 Trace。
	TraceMessageID string `json:"traceMessageId,omitempty"`
	// TraceParent 是业务消息透传的 W3C traceparent，便于把 MQ 链路和应用链路排障关联起来。
	TraceParent    string `json:"traceParent,omitempty"`
	StoreTimestamp int64  `json:"storeTimestamp"`
	QueueID        int    `json:"queueId"`
	QueueOffset    int64  `json:"queueOffset"`
	ReconsumeTimes int    `json:"reconsumeTimes"`
	BornHost       string `json:"bornHost"`
	StoreHost      string `json:"storeHost"`
	BodyPreview    string `json:"bodyPreview"`
}

// MessageSearchResult 表示按 key 查询到的候选消息位置，详情页会继续按 messageId 回查完整消息。
type MessageSearchResult struct {
	MessageID   string `json:"messageId"`
	QueueID     int    `json:"queueId"`
	QueueOffset int64  `json:"queueOffset"`
}

// TopicMessages 表示某个 Topic 在保留窗口内可回查到的消息列表。
type TopicMessages struct {
	// Topic 是当前消息浏览的 Topic 名称。
	Topic string `json:"topic"`
	// BrokerName 是用户指定的 Broker 名称，未指定时表示跨队列聚合浏览。
	BrokerName string `json:"brokerName"`
	// QueueID 是用户指定的队列 ID，-1 表示跨队列聚合浏览。
	QueueID int `json:"queueId"`
	// Limit 是本次最多返回的消息数量。
	Limit int `json:"limit"`
	// ScannedOffsets 是本次实际尝试回查的队列位点数量。
	ScannedOffsets int `json:"scannedOffsets"`
	// FetchedOffsets 是本次真实调用 mqadmin queryMsgByOffset 的位点数量。
	FetchedOffsets int `json:"fetchedOffsets"`
	// ReusedOffsets 是本次从上一轮快照复用的位点数量。
	ReusedOffsets int `json:"reusedOffsets"`
	// Rows 是可用于点击查看链路的消息明细。
	Rows []MessageDetail `json:"rows"`
	// Warnings 记录部分队列或位点无法回查时的摘要，避免少量失败吞掉已拿到的消息。
	Warnings []string `json:"warnings"`
}

// TraceEvent 表示消息生命周期中的发送、投递或消费轨迹事件。
type TraceEvent struct {
	Stage     string `json:"stage"`
	Group     string `json:"group"`
	Timestamp int64  `json:"timestamp"`
	Detail    string `json:"detail"`
}

// ConsumerState 表示消费者组在指定 Topic 上对消息的消费状态判断。
type ConsumerState struct {
	Group  string `json:"group"`
	Topic  string `json:"topic"`
	Status string `json:"status"`
	Lag    int64  `json:"lag"`
}

// MessageStatusStep 是前端链路时间线中的一个节点。
type MessageStatusStep struct {
	Stage     string `json:"stage"`
	Label     string `json:"label"`
	Group     string `json:"group,omitempty"`
	Timestamp int64  `json:"timestamp"`
	Detail    string `json:"detail"`
	Health    string `json:"health"`
}

// MessageStatusChain 汇总单条消息从入库到消费完成的可视化状态链路。
type MessageStatusChain struct {
	MessageID     string                `json:"messageId"`
	Topic         string                `json:"topic"`
	Keys          []string              `json:"keys"`
	Detail        MessageDetail         `json:"detail"`
	Candidates    []MessageSearchResult `json:"candidates"`
	OverallStatus string                `json:"overallStatus"`
	Steps         []MessageStatusStep   `json:"steps"`
}
