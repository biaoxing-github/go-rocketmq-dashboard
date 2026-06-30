package goadmin

import (
	"fmt"
	"strings"
)

// ShadowProviderMode 表示 M6 shadow 验证计划中的 provider 执行路径。
type ShadowProviderMode string

const (
	// ShadowProviderOfficial 表示官方 mqadmin JVM 命令路径，作为差异比较基准。
	ShadowProviderOfficial ShadowProviderMode = "official"
	// ShadowProviderSidecar 表示常驻 JVM sidecar 路径，用于验证热进程输出一致性。
	ShadowProviderSidecar ShadowProviderMode = "sidecar"
	// ShadowProviderNative 表示 Go native remoting 路径，用于验证高性能原生实现。
	ShadowProviderNative ShadowProviderMode = "native"
	// ShadowProviderAuto 表示 auto provider 选择路径，用于验证生产默认路由效果。
	ShadowProviderAuto ShadowProviderMode = "auto"
)

// ShadowSample 描述 M6 后续批量 shadow/provider 验证的一类样本，不直接执行真实命令。
type ShadowSample struct {
	// Name 是样本类别名称，例如 command-smoke 或 message-chain-warm。
	Name string
	// Args 是传给 goadmin/mqadmin 的命令参数模板。
	Args []string
	// Providers 是本样本需要对照的 provider 路径集合。
	Providers []ShadowProviderMode
	// SerialTargets 表示同一样本内部的 shadow target 需要按顺序执行，避免固定文件路径等副作用互相覆盖。
	SerialTargets bool
	// MinSamples 是该类别至少需要采集的样本数量。
	MinSamples int
	// RequireP95 表示该样本需要在后续真实验证中统计 p95 延迟。
	RequireP95 bool
	// Notes 记录样本选择依据和后续采集注意事项。
	Notes string
}

// ShadowFixtureOverrides 保存 M6 dry-run 样本的真实参数覆盖列表，用于把默认模板样本转换成可执行样本。
type ShadowFixtureOverrides struct {
	// Samples 是按默认样本名称匹配的参数覆盖；同一名称可出现多次以展开多组真实样本。
	Samples []ShadowSampleFixture `json:"samples"`
}

// ShadowSampleFixture 描述一个默认 shadow 样本的完整命令参数覆盖。
type ShadowSampleFixture struct {
	// Name 是要覆盖的默认样本名称，例如 known-message 或 message-chain-warm。
	Name string `json:"name"`
	// Args 是完整 goadmin/mqadmin 命令参数；覆盖后不能再包含 <...> 占位符才会被 dry-run 判为 executable。
	Args []string `json:"args"`
	// Repeat 表示将同一条 fixture 展开为多条 concrete sample；未设置或小于 1 时按 1 条处理。
	Repeat int `json:"repeat,omitempty"`
	// SerialTargets 为 true 时强制本条 fixture 内 provider 串行执行，用于隔离固定输出文件等副作用。
	SerialTargets bool `json:"serialTargets,omitempty"`
}

var defaultM6ShadowSamples = []ShadowSample{
	{
		Name:       "command-smoke",
		Args:       []string{"<command>", "<args>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 93,
		Notes:      "覆盖已枚举的 93 个官方命令名，只验证退出码和 stdout/stderr 基线，不在计划层执行命令。",
	},
	{
		Name:          "known-message",
		Args:          []string{"queryMsgById", "-i", "<known-message-id>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    93,
		Notes:         "对已知消息样本做 official/sidecar/native/auto 输出差异比较；queryMsgById 会写固定消息体文件路径，因此同一样本内 target 需要串行执行。",
	},
	{
		Name:          "unique-key-message",
		Args:          []string{"queryMsgByUniqueKey", "-n", "<unique-key-message-namesrv>", "-t", "<unique-key-message-topic>", "-i", "<unique-key-message-id>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "对固定 topic/UNIQ_KEY 查询官方 queryMsgByUniqueKey 消息详情；命令会写固定消息体文件路径，因此同一样本内 target 需要串行执行并采集 warm p95。",
	},
	{
		Name:       "query-msg-trace-by-id",
		Args:       []string{"queryMsgTraceById", "-n", "<trace-by-id-namesrv>", "-i", "<trace-by-id-message-id>", "-t", "<trace-by-id-trace-topic>", "-b", "<trace-by-id-begin-timestamp>", "-e", "<trace-by-id-end-timestamp>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "Read a fixed trace row with official queryMsgTraceById; no output artifact, suitable for warm p95 strict diff.",
	},
	{
		Name:          "offset-message",
		Args:          []string{"queryMsgByOffset", "-t", "<offset-topic>", "-b", "<offset-broker-name>", "-i", "<offset-queue-id>", "-o", "<offset-queue-offset>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		Notes:         "对固定 topic/broker/queue/offset 的消息详情做 official/sidecar/native/auto 输出差异比较；queryMsgByOffset 同样会写固定消息体文件路径，因此同一样本内 target 需要串行执行。",
	},
	{
		Name:       "recent-topic-message",
		Args:       []string{"queryMsgByKey", "-t", "<topic>", "-k", "<recent-message-key>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		Notes:      "抽取最近 Topic 消息，验证动态消息查询输出在四路 provider 下保持一致。",
	},
	{
		Name:       "topic-status",
		Args:       []string{"topicStatus", "-n", "<topic-status-namesrv>", "-t", "<topic-status-topic>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 查询官方 topicStatus 队列位点表；只读 Broker/NameServer 元数据，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "topic-route",
		Args:       []string{"topicRoute", "-t", "<topic-route-topic>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 查询官方 topicRoute 路由 JSON；只读 NameServer 元数据，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "topic-cluster-list",
		Args:       []string{"topicClusterList", "-t", "<topic-cluster-topic>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 查询官方 topicClusterList 集群列表；只读路由和集群元数据，不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "topic-list",
		Args:       []string{"topicList", "-n", "<topic-list-namesrv>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取官方 topicList 全量主题列表；只读 NameServer 元数据，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "cluster-list",
		Args:       []string{"clusterList", "-n", "<cluster-list-namesrv>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取官方 clusterList broker 元数据；只读集群状态，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "cluster-list-more-stats",
		Args:       []string{"clusterList", "-n", "<cluster-list-more-stats-namesrv>", "-m"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取官方 clusterList -m 昨日/今日消息量扩展统计；只读集群计数器，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "stats-all",
		Args:       []string{"statsAll", "-n", "<stats-all-namesrv>", "-t", "<stats-all-topic>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 查询官方 statsAll 统计表；只读 Topic/Broker/Consumer 位点和统计数据，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "consumer-progress",
		Args:       []string{"consumerProgress", "-n", "<consumer-progress-namesrv>", "-g", "<consumer-progress-group>", "-t", "<consumer-progress-topic>", "-c", "<consumer-progress-cluster>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 group/topic/cluster 查询官方 consumerProgress 明细；只读消费位点，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "consumer-connection",
		Args:       []string{"consumerConnection", "-n", "<consumer-connection-namesrv>", "-g", "<consumer-connection-group>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定在线 consumer group 查询官方 consumerConnection 连接和订阅表；只读 Broker 客户端连接状态，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "list-user",
		Args:       []string{"listUser", "-b", "<list-user-broker-addr>", "-f", "<list-user-filter>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 auth broker 和 filter 查询官方 listUser 用户表；只读 RocketMQ 5.x auth metadata，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-user",
		Args:       []string{"getUser", "-b", "<get-user-broker-addr>", "-u", "<get-user-username>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 auth broker 和 username 查询官方 getUser 单用户详情；只读 RocketMQ 5.x auth metadata，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "list-acl",
		Args:       []string{"listAcl", "-b", "<list-acl-broker-addr>", "-s", "<list-acl-subject>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 auth broker 与 subject 查询官方 listAcl ACL 表；fixture 需先预置同一 subject 的 ACL，shadow 样本自身只执行 listAcl 只读路径，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-acl",
		Args:       []string{"getAcl", "-b", "<get-acl-broker-addr>", "-s", "<get-acl-subject>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 auth broker 与 subject 查询官方 getAcl ACL 详情；fixture 需先预置同一 subject 的 ACL，shadow 样本自身只执行 getAcl 只读路径，适合样本级并发采集 p95。",
	},
	{
		Name:       "controller-metadata",
		Args:       []string{"getControllerMetaData", "-a", "<controller-metadata-address>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 standalone controller 查询官方 getControllerMetaData 元数据；只读 Controller header，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "controller-config",
		Args:       []string{"getControllerConfig", "-a", "<controller-config-address>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 standalone controller 查询官方 getControllerConfig 配置表；只读 Controller properties，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-broker-config",
		Args:       []string{"getBrokerConfig", "-b", "<broker-config-broker-addr>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 查询官方 getBrokerConfig 配置表；只读 Broker 配置，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-broker-config-c",
		Args:       []string{"getBrokerConfig", "-n", "<broker-config-namesrv>", "-c", "<broker-config-cluster>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 cluster 查询官方 getBrokerConfig 集群配置表；只读 Broker 配置，不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-namesrv-config",
		Args:       []string{"getNamesrvConfig", "-n", "<namesrv-config-namesrv>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 NameServer 查询官方 getNamesrvConfig 配置表；只读 NameServer 配置，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-consumer-config",
		Args:       []string{"getConsumerConfig", "-n", "<consumer-config-namesrv>", "-g", "<consumer-config-group>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 consumer group 查询官方 getConsumerConfig 订阅组配置；只读 Broker 配置，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-cold-ctr-info",
		Args:       []string{"getColdDataFlowCtrInfo", "-n", "<cold-ctr-namesrv>", "-b", "<cold-ctr-broker-addr>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 查询官方 getColdDataFlowCtrInfo 冷数据流控表；只读 Broker 配置和运行态，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-cold-ctr-info-c",
		Args:       []string{"getColdDataFlowCtrInfo", "-n", "<cold-ctr-namesrv>", "-c", "<cold-ctr-cluster>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 cluster 查询官方 getColdDataFlowCtrInfo 冷数据流控表；只读 Broker 配置和运行态，不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "allocate-mq",
		Args:       []string{"allocateMQ", "-n", "<allocate-mq-namesrv>", "-t", "<allocate-mq-topic>", "-i", "<allocate-mq-ip-list>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 和 consumer ip list 计算官方 allocateMQ 队列分配；只读 TopicRoute，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "broker-status",
		Args:       []string{"brokerStatus", "-b", "<broker-status-broker-addr>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 查询官方 brokerStatus runtime 表；只读 Broker 运行态，TPS/runtime 字段在 shadow 比较层按 brokerStatus 命令白名单归一化。",
	},
	{
		Name:       "broker-status-c",
		Args:       []string{"brokerStatus", "-n", "<broker-status-namesrv>", "-c", "<broker-status-cluster>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 cluster 查询官方 brokerStatus runtime 表；只读 Broker 运行态，TPS/runtime 字段在 shadow 比较层按 brokerStatus 命令白名单归一化。",
	},
	{
		Name:       "print-message",
		Args:       []string{"printMsg", "-t", "<print-topic>", "-b", "<print-begin-timestamp>", "-e", "<print-end-timestamp>", "-d", "false"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按 Topic 全队列扫描官方 printMsg 输出，适合作为只读 warm 样本并行采集 official/sidecar/native/auto 的 p95 延迟。",
	},
	{
		Name:       "print-message-queue",
		Args:       []string{"printMsgByQueue", "-t", "<print-topic>", "-a", "<print-broker-name>", "-i", "<print-queue-id>", "-b", "<print-begin-timestamp>", "-e", "<print-end-timestamp>", "-p", "true", "-d", "false"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker/queue 范围采样官方 printMsgByQueue 输出；属于只读消息浏览命令，可做样本级并发和四路 provider 热路径 p95 对照。",
	},
	{
		Name:       "consume-message",
		Args:       []string{"consumeMessage", "-t", "<consume-topic>", "-b", "<consume-broker-name>", "-i", "<consume-queue-id>", "-o", "<consume-offset>", "-c", "<consume-count>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic/broker/queue/offset 拉取官方 consumeMessage 输出；属于只读消息拉取命令，可做样本级并发和四路 provider warm p95 对照。",
	},
	{
		Name:       "query-consume-queue",
		Args:       []string{"queryCq", "-t", "<cq-topic>", "-q", "<cq-queue-id>", "-i", "<cq-index>", "-c", "<cq-count>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic/queue/index 查询 consumequeue 明细；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:          "check-rocksdb-cq-write-progress",
		Args:          []string{"checkRocksdbCqWriteProgress", "-n", "<rocksdb-cq-namesrv>", "-c", "<rocksdb-cq-cluster>", "-t", "<rocksdb-cq-topic>", "-cf", "<rocksdb-cq-check-store-time>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "按固定 cluster/topic 触发官方 checkRocksdbCqWriteProgress 检查；命令不写本地 artifact，但会触发 Broker 侧检查日志，因此同一样本内 target 串行执行。",
	},
	{
		Name:          "dump-compaction-log",
		Args:          []string{"dumpCompactionLog", "-f", "<dump-compaction-log-file>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "读取固定 compaction log 文件并按官方 dumpCompactionLog 输出 MessageExt；官方以 rw 方式打开文件，因此同一样本内 target 串行执行。",
	},
	{
		Name:       "export-pop-record",
		Args:       []string{"exportPopRecord", "-n", "<export-pop-record-namesrv>", "-c", "<export-pop-record-cluster>", "-d", "false"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 cluster 执行官方 exportPopRecord dry-run 输出；-d false 只打印 dryRun=true，不触发真实 POP 记录导出，适合样本级并发采集 p95。",
	},
	{
		Name:       "export-pop-record-b",
		Args:       []string{"exportPopRecord", "-n", "<export-pop-record-b-namesrv>", "-b", "<export-pop-record-b-broker-addr>", "-d", "false"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 执行官方 exportPopRecord dry-run 输出；-d false 只打印 dryRun=true，不触发真实 POP 记录导出，适合样本级并发采集 p95。",
	},
	{
		Name:       "producer",
		Args:       []string{"producer", "-n", "<producer-namesrv>", "-b", "<producer-broker-addr>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 查询官方 producer 在线连接表；只读 Broker 客户端连接状态，不写固定输出文件，lastUpdateTimestamp 在 producer 命令白名单归一化后采集 p95。",
	},
	{
		Name:       "producer-connection",
		Args:       []string{"producerConnection", "-n", "<producer-connection-namesrv>", "-g", "<producer-connection-group>", "-t", "<producer-connection-topic>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 和 producer group 查询官方 producerConnection 连接表；只读 Broker 客户端连接状态，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "rocksdb-config-to-json-local",
		Args:       []string{"rocksDBConfigToJson", "-p", "<rocksdb-config-local-path>", "-t", "<rocksdb-config-local-type>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取固定本地 RocksDB metadata fixture 并按官方 rocksDBConfigToJson local mode 输出；只读本地 fixture，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "rocksdb-config-to-json-groups-local",
		Args:       []string{"rocksDBConfigToJson", "-p", "<rocksdb-config-groups-local-path>", "-t", "<rocksdb-config-groups-local-type>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取固定本地 RocksDB metadata fixture 的 subscriptionGroups 分支并按官方 rocksDBConfigToJson local mode 输出；只读本地 fixture，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "rocksdb-config-to-json-offsets-local",
		Args:       []string{"rocksDBConfigToJson", "-p", "<rocksdb-config-offsets-local-path>", "-t", "<rocksdb-config-offsets-local-type>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取固定本地 RocksDB metadata fixture 的 consumerOffsets 分支并按官方 rocksDBConfigToJson local mode 输出；只读本地 fixture，不写固定输出文件，适合样本级并发采集 p95。",
	},
	{
		Name:       "export-metadata-in-rocksdb-local",
		Args:       []string{"exportMetadataInRocksDB", "-p", "<export-metadata-rocksdb-local-path>", "-t", "<export-metadata-rocksdb-local-type>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "读取固定本地 RocksDB metadata fixture 并按官方 exportMetadataInRocksDB raw 模式输出；只读本地 fixture，不访问 NameServer/Broker，适合样本级并发采集 p95。",
	},
	{
		Name:       "broker-consume-stats",
		Args:       []string{"brokerConsumeStats", "-b", "<broker-consume-stats-broker>", "-t", "<broker-consume-stats-timeout-ms>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 查询官方 brokerConsumeStats 消费堆积输出；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "ha-status",
		Args:       []string{"haStatus", "-b", "<ha-broker-addr>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 broker 查询官方 haStatus 主从复制状态；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "ha-status-c",
		Args:       []string{"haStatus", "-n", "<ha-cluster-namesrv>", "-c", "<ha-cluster-name>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 cluster 查询官方 haStatus 主从复制状态；只读 NameServer 与 Broker HA 元数据，不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-broker-epoch",
		Args:       []string{"getBrokerEpoch", "-n", "<epoch-namesrv>", "-b", "<epoch-broker-name>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 controller-mode broker 读取官方 getBrokerEpoch epoch 缓存；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-broker-epoch-c",
		Args:       []string{"getBrokerEpoch", "-n", "<epoch-namesrv>", "-c", "<epoch-cluster-name>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按独立 controller-mode cluster 读取官方 getBrokerEpoch 集群分支输出；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-sync-state-set",
		Args:       []string{"getSyncStateSet", "-a", "<sync-controller-addr>", "-b", "<sync-broker-name>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 controller-mode broker 读取官方 getSyncStateSet 同步状态集；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:       "get-sync-state-set-c",
		Args:       []string{"getSyncStateSet", "-n", "<sync-namesrv>", "-a", "<sync-controller-addr>", "-c", "<sync-cluster-name>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按独立 controller-mode cluster 读取官方 getSyncStateSet 集群分支输出；不写固定输出文件且不修改集群状态，适合样本级并发采集 p95。",
	},
	{
		Name:          "export-configs",
		Args:          []string{"exportConfigs", "-n", "<export-configs-namesrv>", "-c", "<export-configs-cluster>", "-f", "<export-configs-output-dir>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "按固定 cluster 导出官方 exportConfigs 配置文件；命令会写固定 configs.json，shadow 比较会读取文件产物正文，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:          "export-metadata",
		Args:          []string{"exportMetadata", "-n", "<export-metadata-namesrv>", "-c", "<export-metadata-cluster>", "-f", "<export-metadata-output-dir>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "按固定 cluster 导出官方 exportMetadata 元数据文件；命令会写固定 metadata.json，shadow 比较会读取文件产物正文，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:          "export-metrics",
		Args:          []string{"exportMetrics", "-n", "<export-metrics-namesrv>", "-c", "<export-metrics-cluster>", "-f", "<export-metrics-output-dir>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "按固定 cluster 导出官方 exportMetrics 指标文件；命令会写固定 metrics.json，shadow 比较会读取文件产物正文，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:          "wipe-write-perm",
		Args:          []string{"wipeWritePerm", "-n", "<write-perm-namesrv>", "-b", "<write-perm-broker-name>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "对固定 broker 执行官方 wipeWritePerm 写权限擦除；CLI shadow runner 会在每路 provider 前后用 addWritePerm 恢复同一初始状态，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:          "add-write-perm",
		Args:          []string{"addWritePerm", "-n", "<write-perm-namesrv>", "-b", "<write-perm-broker-name>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "对固定 broker 执行官方 addWritePerm 写权限恢复；CLI shadow runner 会在每路 provider 前用 wipeWritePerm 准备同一初始状态，并在 provider 后用 addWritePerm 恢复 broker 可写，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:          "update-kv-config",
		Args:          []string{"updateKvConfig", "-n", "<kv-config-namesrv>", "-s", "<kv-config-namespace>", "-k", "<kv-config-key>", "-v", "<kv-config-value>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "向固定 NameServer namespace/key 写入官方 updateKvConfig KV 配置；样本会修改共享 NameServer KV 状态，真实验证必须使用隔离 key 并在 run 后用 deleteKvConfig 清理，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:          "delete-kv-config",
		Args:          []string{"deleteKvConfig", "-n", "<kv-config-namesrv>", "-s", "<kv-config-namespace>", "-k", "<kv-config-key>"},
		Providers:     defaultShadowProviders(),
		SerialTargets: true,
		MinSamples:    20,
		RequireP95:    true,
		Notes:         "删除固定 NameServer namespace/key 的官方 deleteKvConfig KV 配置；CLI shadow runner 会在每路 provider 前用 updateKvConfig 预置同一 KV，并在 provider 后再次 deleteKvConfig 清理，因此同一样本内 target 必须串行执行。",
	},
	{
		Name:       "update-order-conf-get",
		Args:       []string{"updateOrderConf", "-n", "<order-conf-namesrv>", "-m", "get", "-t", "<order-conf-topic>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "按固定 topic 读取官方 ORDER_TOPIC_CONFIG orderConf；fixture 需先预置同一 topic 的 orderConf，shadow 样本自身只执行 get 只读路径，适合样本级并发采集 p95。",
	},
	{
		Name:       "message-chain-cold",
		Args:       []string{"messageChain", "-t", "<topic>", "-k", "<message-key>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "冷路径样本用于统计 /api/message-chain 首次查询延迟和输出一致性。",
	},
	{
		Name:       "message-chain-warm",
		Args:       []string{"messageChain", "-t", "<topic>", "-k", "<message-key>"},
		Providers:  defaultShadowProviders(),
		MinSamples: 20,
		RequireP95: true,
		Notes:      "热路径样本用于统计 /api/message-chain 复用 provider 后的 p95 延迟。",
	},
}

// DefaultM6ShadowPlan 返回 M6 批量验证的默认样本计划副本，调用方修改返回值不会污染全局模板。
func DefaultM6ShadowPlan() []ShadowSample {
	return cloneShadowSamples(defaultM6ShadowSamples)
}

// ApplyShadowFixtureOverrides 将真实 fixture 参数合入默认 shadow 样本；未提供覆盖的样本仍保留占位模板。
func ApplyShadowFixtureOverrides(samples []ShadowSample, overrides ShadowFixtureOverrides) ([]ShadowSample, error) {
	base := cloneShadowSamples(samples)
	if len(overrides.Samples) == 0 {
		return base, nil
	}

	templates := make(map[string]ShadowSample, len(base))
	for _, sample := range base {
		templates[sample.Name] = sample
	}
	grouped := make(map[string][]ShadowSampleFixture, len(overrides.Samples))
	for index, fixture := range overrides.Samples {
		name := strings.TrimSpace(fixture.Name)
		if name == "" {
			return nil, fmt.Errorf("shadow fixture %d name is empty", index)
		}
		if len(fixture.Args) == 0 {
			return nil, fmt.Errorf("shadow fixture %q args is empty", name)
		}
		if _, ok := templates[name]; !ok {
			return nil, fmt.Errorf("shadow fixture %q does not match any default sample", name)
		}
		fixture.Name = name
		fixture.Args = append([]string(nil), fixture.Args...)
		grouped[name] = append(grouped[name], fixture)
	}

	merged := make([]ShadowSample, 0, len(base)+len(overrides.Samples))
	for _, sample := range base {
		fixtures := grouped[sample.Name]
		if len(fixtures) == 0 {
			merged = append(merged, sample)
			continue
		}
		for _, fixture := range fixtures {
			repeat := fixture.Repeat
			if repeat < 1 {
				repeat = 1
			}
			for index := 0; index < repeat; index++ {
				concrete := sample
				concrete.Args = append([]string(nil), fixture.Args...)
				if fixture.SerialTargets {
					concrete.SerialTargets = true
				}
				merged = append(merged, concrete)
			}
		}
	}
	return merged, nil
}

// ValidateShadowPlan 检查 shadow 样本计划是否满足 M6 批量验证的最低结构约束。
func ValidateShadowPlan(samples []ShadowSample) error {
	for index, sample := range samples {
		name := strings.TrimSpace(sample.Name)
		if name == "" {
			return fmt.Errorf("shadow sample %d name is empty", index)
		}
		if len(sample.Args) == 0 {
			return fmt.Errorf("shadow sample %q args is empty", name)
		}
		if err := validateShadowProviders(name, sample.Providers); err != nil {
			return err
		}
		if sample.MinSamples <= 0 {
			return fmt.Errorf("shadow sample %q MinSamples must be greater than 0", name)
		}
		if sample.RequireP95 && sample.MinSamples < 20 {
			return fmt.Errorf("shadow sample %q MinSamples must be at least 20 when RequireP95 is true", name)
		}
	}
	return nil
}

func validateShadowProviders(sampleName string, providers []ShadowProviderMode) error {
	hasOfficial := false
	hasShadowProvider := false
	for _, provider := range providers {
		switch provider {
		case ShadowProviderOfficial:
			hasOfficial = true
		case ShadowProviderSidecar, ShadowProviderNative, ShadowProviderAuto:
			hasShadowProvider = true
		}
	}
	if !hasOfficial {
		return fmt.Errorf("shadow sample %q providers must include official", sampleName)
	}
	if !hasShadowProvider {
		return fmt.Errorf("shadow sample %q providers must include at least one shadow provider", sampleName)
	}
	return nil
}

func defaultShadowProviders() []ShadowProviderMode {
	return []ShadowProviderMode{
		ShadowProviderOfficial,
		ShadowProviderSidecar,
		ShadowProviderNative,
		ShadowProviderAuto,
	}
}

func cloneShadowSamples(samples []ShadowSample) []ShadowSample {
	cloned := make([]ShadowSample, len(samples))
	for i, sample := range samples {
		cloned[i] = sample
		cloned[i].Args = append([]string(nil), sample.Args...)
		cloned[i].Providers = append([]ShadowProviderMode(nil), sample.Providers...)
	}
	return cloned
}
