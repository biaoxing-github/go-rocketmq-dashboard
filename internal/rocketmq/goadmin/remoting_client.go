package goadmin

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/aalhour/rockyardkv"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"
)

const (
	// RocketMQ Remoting JSON 序列化类型标记，对应 SerializeType.JSON。
	serializeTypeJSON byte = 0
	// RocketMQ Remoting 二进制 header 序列化类型标记，对应 SerializeType.ROCKETMQ。
	serializeTypeRocketMQ byte = 1
	// PULL_MESSAGE 对应 RocketMQ RequestCode 11。
	requestCodePullMessage = 11
	// SEARCH_OFFSET_BY_TIMESTAMP 对应 RocketMQ RequestCode 29。
	requestCodeSearchOffsetByTimestamp = 29
	// GET_MAX_OFFSET 对应 RocketMQ RequestCode 30。
	requestCodeGetMaxOffset = 30
	// GET_MIN_OFFSET 对应 RocketMQ RequestCode 31。
	requestCodeGetMinOffset = 31
	// VIEW_MESSAGE_BY_ID 对应 RocketMQ RequestCode 33。
	requestCodeViewMessageByID = 33
	// UPDATE_AND_CREATE_TOPIC 对应 RocketMQ RequestCode 17。
	requestCodeUpdateAndCreateTopic = 17
	// UPDATE_AND_CREATE_TOPIC_LIST 对应 RocketMQ RequestCode 18。
	requestCodeUpdateAndCreateTopicList = 18
	// GET_TOPIC_CONFIG 对应 RocketMQ RequestCode 351，用于读取静态 Topic 的 mapping 配置。
	requestCodeGetTopicConfig = 351
	// UPDATE_AND_CREATE_STATIC_TOPIC 对应 RocketMQ RequestCode 513。
	requestCodeUpdateAndCreateStaticTopic = 513
	// UPDATE_BROKER_CONFIG 对应 RocketMQ RequestCode 25。
	requestCodeUpdateBrokerConfig = 25
	// GET_BROKER_CONFIG 对应 RocketMQ RequestCode 26。
	requestCodeGetBrokerConfig = 26
	// GET_ALL_TOPIC_CONFIG 对应 RocketMQ RequestCode 21。
	requestCodeGetAllTopicConfig = 21
	// GET_BROKER_RUNTIME_INFO 对应 RocketMQ RequestCode 28。
	requestCodeGetBrokerRuntimeInfo = 28
	// GET_NAMESRV_CONFIG 对应 RocketMQ RequestCode 319。
	requestCodeGetNamesrvConfig = 319
	// UPDATE_NAMESRV_CONFIG 对应 RocketMQ RequestCode 318。
	requestCodeUpdateNamesrvConfig = 318
	// GET_SYSTEM_TOPIC_LIST_FROM_BROKER 对应 RocketMQ RequestCode 305。
	requestCodeGetSystemTopicListFromBroker = 305
	// GET_ROUTEINFO_BY_TOPIC 对应 RocketMQ RequestCode 105。
	requestCodeGetRouteInfoByTopic = 105
	// GET_BROKER_CLUSTER_INFO 对应 RocketMQ RequestCode 106。
	requestCodeGetBrokerClusterInfo = 106
	// SEND_MESSAGE_V2 对应 RocketMQ RequestCode 310；官方默认 sendSmartMsg=true。
	requestCodeSendMessageV2 = 310
	// SET_MESSAGE_REQUEST_MODE 对应 RocketMQ RequestCode 401。
	requestCodeSetMessageRequestMode = 401
	// GET_TOPIC_STATS_INFO 对应 RocketMQ RequestCode 202。
	requestCodeGetTopicStatsInfo = 202
	// GET_CONSUMER_CONNECTION_LIST 对应 RocketMQ RequestCode 203。
	requestCodeGetConsumerConnectionList = 203
	// GET_PRODUCER_CONNECTION_LIST 对应 RocketMQ RequestCode 204。
	requestCodeGetProducerConnectionList = 204
	// WIPE_WRITE_PERM_OF_BROKER 对应 RocketMQ RequestCode 205。
	requestCodeWipeWritePermOfBroker = 205
	// ADD_WRITE_PERM_OF_BROKER 对应 RocketMQ RequestCode 327。
	requestCodeAddWritePermOfBroker = 327
	// GET_ALL_PRODUCER_INFO 对应 RocketMQ RequestCode 328。
	requestCodeGetAllProducerInfo = 328
	// DELETE_EXPIRED_COMMITLOG 对应 RocketMQ RequestCode 329。
	requestCodeDeleteExpiredCommitLog = 329
	// UPDATE_AND_CREATE_ACL_CONFIG 对应 RocketMQ RequestCode 50，用于更新历史 plain_acl.yml 账号配置。
	requestCodeUpdateAndCreateAclConfig = 50
	// DELETE_ACL_CONFIG 对应 RocketMQ RequestCode 51，用于删除历史 plain_acl.yml 账号配置。
	requestCodeDeleteAclConfig = 51
	// GET_BROKER_CLUSTER_ACL_INFO 对应 RocketMQ RequestCode 52。
	requestCodeGetBrokerClusterAclInfo = 52
	// UPDATE_GLOBAL_WHITE_ADDRS_CONFIG 对应 RocketMQ RequestCode 53，用于更新 plain_acl.yml 全局白名单。
	requestCodeUpdateGlobalWhiteAddrsConfig = 53
	// AUTH_CREATE_USER 对应 RocketMQ RequestCode 3001，用于创建 5.x auth 用户。
	requestCodeAuthCreateUser = 3001
	// AUTH_UPDATE_USER 对应 RocketMQ RequestCode 3002，用于更新 5.x auth 用户。
	requestCodeAuthUpdateUser = 3002
	// AUTH_DELETE_USER 对应 RocketMQ RequestCode 3003，用于删除 5.x auth 用户。
	requestCodeAuthDeleteUser = 3003
	// AUTH_GET_USER 对应 RocketMQ RequestCode 3004，用于读取单个 5.x auth 用户。
	requestCodeAuthGetUser = 3004
	// AUTH_LIST_USER 对应 RocketMQ RequestCode 3005，用于列出 5.x auth 用户。
	requestCodeAuthListUser = 3005
	// AUTH_CREATE_ACL 对应 RocketMQ RequestCode 3006，用于创建 5.x ACL。
	requestCodeAuthCreateAcl = 3006
	// AUTH_UPDATE_ACL 对应 RocketMQ RequestCode 3007，用于更新 5.x ACL。
	requestCodeAuthUpdateAcl = 3007
	// AUTH_DELETE_ACL 对应 RocketMQ RequestCode 3008，用于删除 5.x ACL。
	requestCodeAuthDeleteAcl = 3008
	// AUTH_GET_ACL 对应 RocketMQ RequestCode 3009，用于读取单个 5.x ACL。
	requestCodeAuthGetAcl = 3009
	// AUTH_LIST_ACL 对应 RocketMQ RequestCode 3010，用于列出 5.x ACL。
	requestCodeAuthListAcl = 3010
	// authClusterNotFoundOutput 复刻 CommandUtil.fetchMasterAndSlaveAddrByClusterName 缺失集群时的 stdout。
	authClusterNotFoundOutput = "[error] Make sure the specified clusterName exists or the name server connected to is correct."
	// UPDATE_COLD_DATA_FLOW_CTR_CONFIG 对应 RocketMQ RequestCode 2001。
	requestCodeUpdateColdDataFlowCtrConfig = 2001
	// REMOVE_COLD_DATA_FLOW_CTR_CONFIG 对应 RocketMQ RequestCode 2002。
	requestCodeRemoveColdDataFlowCtrConfig = 2002
	// GET_COLD_DATA_FLOW_CTR_INFO 对应 RocketMQ RequestCode 2003。
	requestCodeGetColdDataFlowCtrInfo = 2003
	// SET_COMMITLOG_READ_MODE 对应 RocketMQ RequestCode 2004。
	requestCodeSetCommitLogReadMode = 2004
	// CLEAN_EXPIRED_CONSUMEQUEUE 对应 RocketMQ RequestCode 306。
	requestCodeCleanExpiredConsumeQueue = 306
	// CLEAN_UNUSED_TOPIC 对应 RocketMQ RequestCode 316。
	requestCodeCleanUnusedTopic = 316
	// GET_CONSUMER_RUNNING_INFO 对应 RocketMQ RequestCode 307。
	requestCodeGetConsumerRunningInfo = 307
	// CONSUME_MESSAGE_DIRECTLY 对应 RocketMQ RequestCode 309。
	requestCodeConsumeMessageDirectly = 309
	// GET_SUBSCRIPTIONGROUP_CONFIG 对应 RocketMQ RequestCode 352。
	requestCodeGetSubscriptionGroupConfig = 352
	// GET_ALL_SUBSCRIPTIONGROUP_CONFIG 对应 RocketMQ RequestCode 201。
	requestCodeGetAllSubscriptionGroupConfig = 201
	// UPDATE_AND_CREATE_SUBSCRIPTIONGROUP 对应 RocketMQ RequestCode 200。
	requestCodeUpdateAndCreateSubscriptionGroup = 200
	// UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST 对应 RocketMQ RequestCode 225。
	requestCodeUpdateAndCreateSubscriptionGroupList = 225
	// DELETE_SUBSCRIPTIONGROUP 对应 RocketMQ RequestCode 207。
	requestCodeDeleteSubscriptionGroup = 207
	// GET_ALL_TOPIC_LIST_FROM_NAMESERVER 对应 RocketMQ RequestCode 206。
	requestCodeGetAllTopicListFromNameServer = 206
	// GET_CONSUME_STATS 对应 RocketMQ RequestCode 208。
	requestCodeGetConsumeStats = 208
	// VIEW_BROKER_STATS_DATA 对应 RocketMQ RequestCode 315。
	requestCodeViewBrokerStatsData = 315
	// GET_BROKER_CONSUME_STATS 对应 RocketMQ RequestCode 317。
	requestCodeGetBrokerConsumeStats = 317
	// GET_BROKER_HA_STATUS 对应 RocketMQ RequestCode 907。
	requestCodeGetBrokerHAStatus = 907
	// ADD_BROKER 对应 RocketMQ RequestCode 902，用于向 BrokerContainer 添加指定配置文件的 Broker。
	requestCodeAddBroker = 902
	// REMOVE_BROKER 对应 RocketMQ RequestCode 903，用于从 BrokerContainer 移除指定 Broker。
	requestCodeRemoveBroker = 903
	// RESET_MASTER_FLUSH_OFFSET 对应 RocketMQ RequestCode 908。
	requestCodeResetMasterFlushOffset = 908
	// CHECK_ROCKSDB_CQ_WRITE_PROGRESS 对应 RocketMQ RequestCode 354。
	requestCodeCheckRocksdbCqWriteProgress = 354
	// EXPORT_ROCKSDB_CONFIG_TO_JSON 对应 RocketMQ RequestCode 355。
	requestCodeExportRocksDBConfigToJson = 355
	// EXPORT_POP_RECORD 对应 RocketMQ RequestCode 200056。
	requestCodeExportPopRecord = 200056
	// PUT_KV_CONFIG 对应 RocketMQ RequestCode 100。
	requestCodePutKVConfig = 100
	// GET_KV_CONFIG 对应 RocketMQ RequestCode 101。
	requestCodeGetKVConfig = 101
	// DELETE_KV_CONFIG 对应 RocketMQ RequestCode 102。
	requestCodeDeleteKVConfig = 102
	// DELETE_TOPIC_IN_BROKER 对应 RocketMQ RequestCode 215。
	requestCodeDeleteTopicInBroker = 215
	// DELETE_TOPIC_IN_NAMESRV 对应 RocketMQ RequestCode 216。
	requestCodeDeleteTopicInNameServer = 216
	// QUERY_CONSUME_QUEUE 对应 RocketMQ RequestCode 321。
	requestCodeQueryConsumeQueue = 321
	// QUERY_TOPIC_CONSUME_BY_WHO 对应 RocketMQ RequestCode 300。
	requestCodeQueryTopicConsumeByWho = 300
	// INVOKE_BROKER_TO_RESET_OFFSET 对应 RocketMQ RequestCode 222。
	requestCodeInvokeBrokerToResetOffset = 222
	// QUERY_MESSAGE 对应 RocketMQ RequestCode 12。
	requestCodeQueryMessage = 12
	// UPDATE_CONSUMER_OFFSET 对应 RocketMQ RequestCode 15。
	requestCodeUpdateConsumerOffset = 15
	// CONTROLLER_ELECT_MASTER 对应 RocketMQ RequestCode 1002，用于 Controller 触发指定 Broker 选主。
	requestCodeControllerElectMaster = 1002
	// CONTROLLER_GET_METADATA_INFO 对应 RocketMQ RequestCode 1005。
	requestCodeControllerGetMetadataInfo = 1005
	// CONTROLLER_GET_SYNC_STATE_DATA 对应 RocketMQ RequestCode 1006。
	requestCodeControllerGetSyncStateData = 1006
	// GET_BROKER_EPOCH_CACHE 对应 RocketMQ RequestCode 1007，用于读取 controllerMode Broker 的 epoch 缓存。
	requestCodeGetBrokerEpochCache = 1007
	// UPDATE_CONTROLLER_CONFIG 对应 RocketMQ RequestCode 1009。
	requestCodeUpdateControllerConfig = 1009
	// GET_CONTROLLER_CONFIG 对应 RocketMQ RequestCode 1010。
	requestCodeGetControllerConfig = 1010
	// CLEAN_BROKER_DATA 对应 RocketMQ RequestCode 1011，用于清理 Controller 里的 Broker 元数据。
	requestCodeCleanBrokerData = 1011
	// SUCCESS 对应 RocketMQ ResponseCode.SUCCESS。
	responseCodeSuccess = 0
	// FLUSH_DISK_TIMEOUT 对应 RocketMQ ResponseCode.FLUSH_DISK_TIMEOUT。
	responseCodeFlushDiskTimeout = 10
	// FLUSH_SLAVE_TIMEOUT 对应 RocketMQ ResponseCode.FLUSH_SLAVE_TIMEOUT。
	responseCodeFlushSlaveTimeout = 11
	// SLAVE_NOT_AVAILABLE 对应 RocketMQ ResponseCode.SLAVE_NOT_AVAILABLE。
	responseCodeSlaveNotAvailable = 12
	// PULL_NOT_FOUND 对应 RocketMQ ResponseCode.PULL_NOT_FOUND，官方 queryMsgByOffset 对该状态不输出详情。
	responseCodePullNotFound = 19
	// PULL_RETRY_IMMEDIATELY 对应 RocketMQ ResponseCode.PULL_RETRY_IMMEDIATELY，官方 queryMsgByOffset 对该状态不输出详情。
	responseCodePullRetryImmediately = 20
	// PULL_OFFSET_MOVED 对应 RocketMQ ResponseCode.PULL_OFFSET_MOVED，官方 queryMsgByOffset 对该状态不输出详情。
	responseCodePullOffsetMoved = 21
	// TOPIC_NOT_EXIST 对应 RocketMQ ResponseCode.TOPIC_NOT_EXIST，官方静态 Topic 创建会把它视为空旧 mapping。
	responseCodeTopicNotExist = 17
	// CONSUMER_NOT_ONLINE 对应 RocketMQ ResponseCode.CONSUMER_NOT_ONLINE。
	responseCodeConsumerNotOnline = 206
	// BROADCAST_CONSUMPTION 对应 RocketMQ ResponseCode.BROADCAST_CONSUMPTION。
	responseCodeBroadcastConsumption = 213
	// QUERY_MESSAGE 默认结束时间戳，和官方 mqadmin 保持一致。
	defaultQueryMessageEndTimestamp int64 = 1<<63 - 1
	// TOOLS_CONSUMER 是官方 MixAll.TOOLS_CONSUMER_GROUP。
	toolsConsumerGroup = "TOOLS_CONSUMER"
	// PULL_MESSAGE 默认挂起上限，来自 DefaultMQPullConsumer.getBrokerSuspendMaxTimeMillis。
	defaultBrokerSuspendMaxTimeMillis = 20000
	// PULL_MESSAGE 单次最大消息体大小，官方 pullAsyncImpl 对应 Integer.MAX_VALUE。
	defaultPullMaxMsgBytes = 2147483647
	// 官方 QueryMsgByIdSubCommand.createBodyFile 使用的固定消息体目录。
	messageBodyDirectory = "/tmp/rocketmq/msgbodys"
	// RocketMQ 唯一消息查询标记，对应 MixAll.UNIQUE_MSG_QUERY_FLAG。
	uniqueMsgQueryFlag = "_UNIQUE_KEY_QUERY"
	// 官方自动建 Topic key，对应 TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC。
	defaultCreateTopicKey = "TBW102"
	// 官方 DefaultMQProducer.defaultTopicQueueNums 默认值。
	defaultProducerTopicQueueNums = 4
	// TOPIC_PUT_NUMS 是官方 statsAll 查询 Topic 写入量使用的 statsName。
	statsNameTopicPutNums = "TOPIC_PUT_NUMS"
	// GROUP_GET_NUMS 是官方 statsAll 查询消费组拉取量使用的 statsName。
	statsNameGroupGetNums = "GROUP_GET_NUMS"
	// 官方 queryMsgTraceById 默认查询的 trace topic。
	defaultTraceTopic = "RMQ_SYS_TRACE_TOPIC"
	// 官方 MQClientAPIImpl 在 Broker 未返回 MSG_REGION 时使用的 trace region。
	defaultTraceRegionID = "DefaultRegion"
	// exportMetrics 使用当前 RocketMQ tools 包的 MQVersion.CURRENT_VERSION。
	exportMetricsRocketMQVersion = "V5_3_2"
	// RocketMQ 消息版本 v1 的魔数。
	messageMagicCodeV1 = -626843481
	// RocketMQ 消息版本 v2 的魔数。
	messageMagicCodeV2 = -626843477
	// TraceDataEncoder 使用 CONTENT_SPLITOR 分隔同一条 trace 的字段。
	traceContentSplitter = rune(1)
	// TraceDataEncoder 使用 FIELD_SPLITOR 分隔多条 trace context。
	traceFieldSplitter = rune(2)
	// 官方重试 Topic 前缀，对应 MixAll.RETRY_GROUP_TOPIC_PREFIX。
	retryGroupTopicPrefix = "%RETRY%"
	// 官方死信 Topic 前缀，对应 MixAll.DLQ_GROUP_TOPIC_PREFIX。
	dlqGroupTopicPrefix = "%DLQ%"
	// topicList -c 的每 Topic 查询并发度；输出仍按 NameServer topicList 顺序回填。
	topicListClusterConcurrency = 16
	// statsAll 的每 Topic 查询并发度；输出仍按 NameServer topicList 顺序回填。
	statsAllTopicConcurrency = 16
	// CheckRocksdbCqWriteResult.CheckStatus.CHECK_ERROR 的官方枚举值。
	checkRocksdbCqWriteStatusError = 3
)

var nextOpaque atomic.Int32

// remotingCommand 是 RocketMQ RemotingCommand 的 JSON header 子集；body 不进入 header JSON。
type remotingCommand struct {
	// Code 是请求码或响应码。
	Code int `json:"code"`
	// Language 保持官方 Java 客户端口径，NameServer 会按该字段识别调用端语言。
	Language string `json:"language"`
	// Version 是 remoting 协议版本；0 与官方默认值兼容。
	Version int `json:"version"`
	// Opaque 是请求响应关联 ID。
	Opaque int32 `json:"opaque"`
	// Flag 表示请求/响应类型等 RocketMQ 内部标记。
	Flag int `json:"flag"`
	// Remark 是异常响应的可读说明。
	Remark string `json:"remark,omitempty"`
	// ExtFields 保存自定义 header 字段；topicList 请求不需要。
	ExtFields map[string]string `json:"extFields,omitempty"`
	// Body 是 frame header 后面的 payload，不参与 JSON header 编码。
	Body []byte `json:"-"`
}

// Client 通过 RocketMQ Remoting 协议直连 NameServer/Broker 执行只读管理请求。
type Client struct {
	timeout time.Duration
}

// NewClient 创建 Go 原生 Remoting 客户端。
func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{timeout: timeout}
}

// RunCommand 尝试用 Go 原生 Remoting 执行 goadmin 子命令；不支持的命令返回 supported=false。
func RunCommand(ctx context.Context, args []string, timeout time.Duration) (output string, supported bool, err error) {
	return runNativeCommand(ctx, args, NewClient(timeout))
}

// OfficialParserPreflight 复刻官方 MQAdminStartup 在执行子命令前的 Commons CLI 参数解析错误。
func OfficialParserPreflight(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "clonegroupoffset":
		if cloneGroupOffsetOfflineMissingValue(args[1:]) {
			return cloneGroupOffsetUsage(), &OfficialParserError{Stderr: "Missing argument for option: o\n"}
		}
	}
	return "", nil
}

// OfficialParserError 表示官方 mqadmin 在进入子命令 execute 前由 Commons CLI 输出的参数解析错误。
type OfficialParserError struct {
	// Stderr 是官方 parser 写到 stderr 的错误行，CLI 会原样转发。
	Stderr string
}

func (err *OfficialParserError) Error() string {
	return strings.TrimRight(err.Stderr, "\r\n")
}

// OfficialCommandResult 表示官方 mqadmin 会正常退出，但仍需要保留 stdout/stderr 分流的命令结果。
type OfficialCommandResult struct {
	// ExitCode 是官方 mqadmin 进程退出码。
	ExitCode int
	// Stdout 是官方 mqadmin 写到 stdout 的文本。
	Stdout string
	// Stderr 是官方 mqadmin 写到 stderr 的文本。
	Stderr string
}

func (result *OfficialCommandResult) Error() string {
	if result == nil {
		return ""
	}
	if strings.TrimSpace(result.Stderr) != "" {
		return strings.TrimRight(result.Stderr, "\r\n")
	}
	return strings.TrimRight(result.Stdout, "\r\n")
}

func officialGetBrokerEpochControllerModeStderr(remark string) string {
	return "org.apache.rocketmq.tools.command.SubCommandException: GetBrokerEpochSubCommand command failed\n" +
		"\tat org.apache.rocketmq.tools.command.broker.GetBrokerEpochSubCommand.execute(GetBrokerEpochSubCommand.java:87)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main0(MQAdminStartup.java:181)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main(MQAdminStartup.java:131)\n" +
		"Caused by: org.apache.rocketmq.client.exception.MQBrokerException: CODE: 1  DESC: " + remark + "\n" +
		"For more information, please visit the url, https://rocketmq.apache.org/docs/bestPractice/06FAQ\n" +
		"\tat org.apache.rocketmq.client.impl.MQClientAPIImpl.getBrokerEpochCache(MQClientAPIImpl.java:3321)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl.getBrokerEpochCache(DefaultMQAdminExtImpl.java:1892)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExt.getBrokerEpochCache(DefaultMQAdminExt.java:841)\n" +
		"\tat org.apache.rocketmq.tools.command.broker.GetBrokerEpochSubCommand.printData(GetBrokerEpochSubCommand.java:110)\n" +
		"\tat org.apache.rocketmq.tools.command.broker.GetBrokerEpochSubCommand.innerExec(GetBrokerEpochSubCommand.java:102)\n" +
		"\tat org.apache.rocketmq.tools.command.broker.GetBrokerEpochSubCommand.execute(GetBrokerEpochSubCommand.java:84)\n" +
		"\t... 2 more\n"
}

// OfficialGetBrokerEpochControllerModeStderr 返回官方 getBrokerEpoch 在非 controllerMode Broker 上的 stderr。
func OfficialGetBrokerEpochControllerModeStderr(remark string) string {
	return officialGetBrokerEpochControllerModeStderr(remark)
}

func officialTopicRouteMissingNameServerStderr() string {
	return "org.apache.rocketmq.tools.command.SubCommandException: TopicRouteSubCommand command failed\n" +
		"\tat org.apache.rocketmq.tools.command.topic.TopicRouteSubCommand.execute(TopicRouteSubCommand.java:74)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main0(MQAdminStartup.java:181)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main(MQAdminStartup.java:131)\n" +
		"Caused by: org.apache.rocketmq.remoting.exception.RemotingConnectException: connect to null failed\n" +
		"\tat org.apache.rocketmq.remoting.netty.NettyRemotingClient.invokeSync(NettyRemotingClient.java:584)\n" +
		"\tat org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicRouteInfoFromNameServer(MQClientAPIImpl.java:2090)\n" +
		"\tat org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicRouteInfoFromNameServer(MQClientAPIImpl.java:2081)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl.examineTopicRouteInfo(DefaultMQAdminExtImpl.java:602)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExt.examineTopicRouteInfo(DefaultMQAdminExt.java:346)\n" +
		"\tat org.apache.rocketmq.tools.command.topic.TopicRouteSubCommand.execute(TopicRouteSubCommand.java:71)\n" +
		"\t... 2 more\n"
}

// OfficialTopicRouteMissingNameServerStderr 返回官方 topicRoute 未配置 namesrvAddr 时的 stderr。
func OfficialTopicRouteMissingNameServerStderr() string {
	return officialTopicRouteMissingNameServerStderr()
}

func officialTopicClusterListMissingNameServerStderr() string {
	return "org.apache.rocketmq.tools.command.SubCommandException: TopicClusterSubCommand command failed\n" +
		"\tat org.apache.rocketmq.tools.command.topic.TopicClusterSubCommand.execute(TopicClusterSubCommand.java:61)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main0(MQAdminStartup.java:181)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main(MQAdminStartup.java:131)\n" +
		"Caused by: org.apache.rocketmq.remoting.exception.RemotingConnectException: connect to null failed\n" +
		"\tat org.apache.rocketmq.remoting.netty.NettyRemotingClient.invokeSync(NettyRemotingClient.java:584)\n" +
		"\tat org.apache.rocketmq.client.impl.MQClientAPIImpl.getBrokerClusterInfo(MQClientAPIImpl.java:2060)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl.examineBrokerClusterInfo(DefaultMQAdminExtImpl.java:596)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl.getTopicClusterList(DefaultMQAdminExtImpl.java:1655)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExt.getTopicClusterList(DefaultMQAdminExt.java:683)\n" +
		"\tat org.apache.rocketmq.tools.command.topic.TopicClusterSubCommand.execute(TopicClusterSubCommand.java:56)\n" +
		"\t... 2 more\n"
}

// OfficialTopicClusterListMissingNameServerStderr 返回官方 topicClusterList 未配置 namesrvAddr 时的 stderr。
func OfficialTopicClusterListMissingNameServerStderr() string {
	return officialTopicClusterListMissingNameServerStderr()
}

func officialQueryMsgTraceByIDNoMessageStderr() string {
	return "org.apache.rocketmq.tools.command.SubCommandException: QueryMsgTraceByIdSubCommandcommand failed\n" +
		"\tat org.apache.rocketmq.tools.command.message.QueryMsgTraceByIdSubCommand.execute(QueryMsgTraceByIdSubCommand.java:110)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main0(MQAdminStartup.java:181)\n" +
		"\tat org.apache.rocketmq.tools.command.MQAdminStartup.main(MQAdminStartup.java:131)\n" +
		"Caused by: org.apache.rocketmq.client.exception.MQClientException: CODE: 208  DESC: query message by key finished, but no message.\n" +
		"For more information, please visit the url, https://rocketmq.apache.org/docs/bestPractice/06FAQ\n" +
		"\tat org.apache.rocketmq.client.impl.MQAdminImpl.queryMessage(MQAdminImpl.java:482)\n" +
		"\tat org.apache.rocketmq.client.impl.MQAdminImpl.queryMessage(MQAdminImpl.java:282)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl.queryMessage(DefaultMQAdminExtImpl.java:1765)\n" +
		"\tat org.apache.rocketmq.tools.admin.DefaultMQAdminExt.queryMessage(DefaultMQAdminExt.java:155)\n" +
		"\tat org.apache.rocketmq.tools.command.message.QueryMsgTraceByIdSubCommand.queryTraceByMsgId(QueryMsgTraceByIdSubCommand.java:120)\n" +
		"\tat org.apache.rocketmq.tools.command.message.QueryMsgTraceByIdSubCommand.execute(QueryMsgTraceByIdSubCommand.java:108)\n" +
		"\t... 2 more\n"
}

type rocketMQResponseError struct {
	// Code 是 RocketMQ ResponseCode 数值。
	Code int
	// Remark 是 Broker/NameServer 返回的错误描述。
	Remark string
}

func (err *rocketMQResponseError) Error() string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("CODE:%d DESC:%s", err.Code, err.Remark)
}

type topicRouteError struct {
	// Code 是 NameServer 查询 Topic 路由失败时返回的 RocketMQ ResponseCode。
	Code int
	// Remark 是 NameServer 对失败原因的官方描述。
	Remark string
}

func (err *topicRouteError) Error() string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("NameServer topicRoute failed: code=%d remark=%s", err.Code, err.Remark)
}

func (err *topicRouteError) messageTrackExceptionDesc() string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("org.apache.rocketmq.client.exception.MQClientException: CODE: %d  DESC: %s, org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicRouteInfoFromNameServer(MQClientAPIImpl.java:2110)", err.Code, err.Remark)
}

type nativeCommandClient interface {
	TopicList(ctx context.Context, nameServer string) ([]string, error)
	TopicListCluster(ctx context.Context, nameServer string) ([]topicClusterRow, error)
	AllocateMQ(ctx context.Context, nameServer string, topic string, ipList string) ([]allocateMQAssignment, error)
	ClusterList(ctx context.Context, nameServer string, clusterName string) ([]clusterListRow, error)
	ClusterListMoreStats(ctx context.Context, nameServer string, clusterName string) ([]clusterListMoreStatsRow, error)
	BrokerStatus(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerStatusTable, error)
	GetBrokerConfig(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerConfigSection, error)
	UpdateBrokerConfig(ctx context.Context, nameServer string, options updateBrokerConfigOptions) ([]string, error)
	UpdateNamesrvConfig(ctx context.Context, nameServers string, options updateNamesrvConfigOptions) ([]string, error)
	UpdateControllerConfig(ctx context.Context, controllerAddrs string, options updateControllerConfigOptions) ([]string, error)
	WipeWritePerm(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error)
	AddWritePerm(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error)
	SendMessage(ctx context.Context, nameServer string, options sendMessageOptions) (*sendMessageResult, error)
	SendMsgStatus(ctx context.Context, nameServer string, options sendMsgStatusOptions) ([]sendMsgStatusResult, error)
	CheckMsgSendRT(ctx context.Context, nameServer string, options checkMsgSendRTOptions) (*checkMsgSendRTResult, error)
	ClusterRT(ctx context.Context, nameServer string, options clusterRTOptions) (*clusterRTResult, error)
	AddBroker(ctx context.Context, brokerContainerAddr string, options addBrokerOptions) error
	RemoveBroker(ctx context.Context, brokerContainerAddr string, options removeBrokerOptions) error
	ResetMasterFlushOffset(ctx context.Context, brokerAddr string, offset int64) error
	GetBrokerEpoch(ctx context.Context, nameServer string, brokerName string) ([]brokerEpochResult, error)
	GetBrokerEpochByCluster(ctx context.Context, nameServer string, clusterName string) ([]brokerEpochResult, error)
	GetControllerMetaData(ctx context.Context, controllerAddr string) (*controllerMetaData, error)
	GetSyncStateSet(ctx context.Context, controllerAddr string, brokerNames []string) (*syncStateSetResult, error)
	GetSyncStateSetByCluster(ctx context.Context, nameServer string, controllerAddr string, clusterName string) (*syncStateSetResult, error)
	GetControllerConfig(ctx context.Context, controllerAddrs string) ([]namesrvConfigSection, error)
	CleanBrokerMetadata(ctx context.Context, controllerAddr string, options cleanBrokerMetadataOptions) error
	ElectMaster(ctx context.Context, controllerAddr string, options electMasterOptions) (*electMasterResult, error)
	ResetOffsetByTime(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error)
	SkipAccumulatedMessage(ctx context.Context, nameServer string, options skipAccumulatedMessageOptions) ([]skipAccumulatedMessageRow, error)
	ExportConfigs(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error)
	ExportMetrics(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error)
	ExportMetadata(ctx context.Context, nameServer string, options exportMetadataOptions) (*exportMetadataResult, error)
	GetNamesrvConfig(ctx context.Context, nameServers string) ([]namesrvConfigSection, error)
	GetConsumerConfig(ctx context.Context, nameServer string, groupName string) ([]consumerConfigSection, error)
	BrokerConsumeStats(ctx context.Context, brokerAddr string, isOrder bool, timeout time.Duration) (*brokerConsumeStats, error)
	StatsAll(ctx context.Context, nameServer string, topic string, activeOnly bool) ([]statsAllRow, error)
	BrokerHAStatus(ctx context.Context, brokerAddr string) (*haStatusResult, error)
	BrokerHAStatusByCluster(ctx context.Context, nameServer string, clusterName string) ([]haStatusBrokerResult, error)
	ClusterAclConfigVersion(ctx context.Context, nameServer string, clusterName string) ([]clusterAclConfigVersionRow, error)
	SetCommitLogReadAheadMode(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error)
	ListUser(ctx context.Context, nameServer string, brokerAddr string, clusterName string, filter string) ([]listUserRow, error)
	GetUser(ctx context.Context, nameServer string, brokerAddr string, clusterName string, username string) (*listUserRow, error)
	CopyUser(ctx context.Context, sourceBroker string, targetBroker string, usernames string) ([]copyUserResult, error)
	ListAcl(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subjectFilter string, resourceFilter string) ([]aclInfo, error)
	GetAcl(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subject string) ([]aclInfo, error)
	CopyAcl(ctx context.Context, sourceBroker string, targetBroker string, subjects string) ([]copyAclResult, error)
	CreateUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error)
	UpdateUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error)
	DeleteUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error)
	CreateAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error)
	UpdateAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error)
	DeleteAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error)
	UpdateAclConfig(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error)
	DeleteAclConfig(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error)
	UpdateGlobalWhiteAddr(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error)
	CheckRocksdbCqWriteProgress(ctx context.Context, nameServer string, clusterName string, topic string, checkStoreTime int64) ([]checkRocksdbCqWriteProgressRow, error)
	RocksDBConfigToJson(ctx context.Context, nameServer string, brokerAddr string, clusterName string, configTypes []string) error
	ExportPopRecord(ctx context.Context, nameServer string, brokerAddr string, clusterName string, dryRun bool) ([]exportPopRecordRow, error)
	UpdateKvConfig(ctx context.Context, nameServers string, namespace string, key string, value string) error
	GetKvConfig(ctx context.Context, nameServers string, namespace string, key string) (string, error)
	DeleteKvConfig(ctx context.Context, nameServers string, namespace string, key string) error
	UpdateTopicList(ctx context.Context, nameServer string, options updateTopicListOptions) ([]string, error)
	UpdateTopic(ctx context.Context, nameServer string, options updateTopicOptions) (*updateTopicResult, error)
	UpdateStaticTopic(ctx context.Context, nameServer string, options updateStaticTopicOptions) (*updateStaticTopicResult, error)
	RemappingStaticTopic(ctx context.Context, nameServer string, options remappingStaticTopicOptions) (*remappingStaticTopicResult, error)
	UpdateTopicPerm(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error)
	SetConsumeMode(ctx context.Context, nameServer string, options setConsumeModeOptions) (*setConsumeModeResult, error)
	DeleteTopic(ctx context.Context, nameServer string, clusterName string, topic string) error
	UpdateSubGroup(ctx context.Context, nameServer string, options updateSubGroupOptions) (*updateSubGroupResult, error)
	UpdateSubGroupList(ctx context.Context, nameServer string, options updateSubGroupListOptions) ([]string, error)
	DeleteSubGroup(ctx context.Context, nameServer string, options deleteSubGroupOptions) ([]deleteSubGroupResult, error)
	QueryConsumeQueue(ctx context.Context, nameServer string, brokerAddr string, topic string, queueID int, index int64, count int, consumerGroup string) (*queryConsumeQueueResult, error)
	TopicRoute(ctx context.Context, nameServer string, topic string) ([]byte, error)
	TopicStatus(ctx context.Context, nameServer string, topic string) ([]topicStatusEntry, error)
	TopicStatusByCluster(ctx context.Context, nameServer string, topic string, cluster string) ([]topicStatusEntry, error)
	TopicClusterList(ctx context.Context, nameServer string, topic string) ([]string, error)
	ConsumerConnection(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string) (*consumerConnectionDetail, error)
	ConsumerStatus(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (string, error)
	ConsumerStatusList(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string, jstack bool) (string, error)
	CloneGroupOffset(ctx context.Context, nameServer string, srcGroup string, destGroup string, topic string) error
	Producer(ctx context.Context, brokerAddr string) (*producerTableInfo, error)
	UpdateColdDataFlowCtrGroupConfig(ctx context.Context, nameServer string, options coldDataFlowCtrGroupConfigOptions) ([]string, error)
	RemoveColdDataFlowCtrGroupConfig(ctx context.Context, nameServer string, options removeColdDataFlowCtrGroupConfigOptions) ([]string, error)
	CleanExpiredCQ(ctx context.Context, nameServer string, options cleanExpiredCQOptions) (bool, error)
	CleanUnusedTopic(ctx context.Context, nameServer string, options cleanUnusedTopicOptions) (bool, error)
	DeleteExpiredCommitLog(ctx context.Context, nameServer string, options deleteExpiredCommitLogOptions) (bool, error)
	GetColdDataFlowCtrInfo(ctx context.Context, brokerAddr string) (string, error)
	GetColdDataFlowCtrInfoByCluster(ctx context.Context, nameServer string, clusterName string) ([]coldDataFlowCtrInfoSection, error)
	ProducerConnection(ctx context.Context, nameServer string, producerGroup string, topic string) (*producerConnectionDetail, error)
	QueryMessagesByKey(ctx context.Context, nameServer string, topic string, key string, clusterName string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageSearchResult, error)
	QueryMessageByOffset(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error)
	PrintMessages(ctx context.Context, nameServer string, options printMsgOptions) (*printMsgResult, error)
	PrintMessagesByQueue(ctx context.Context, nameServer string, options printMsgByQueueOptions) (*printMsgByQueueResult, error)
	ConsumeMessages(ctx context.Context, nameServer string, options consumeMessageOptions) (*consumeMessageResult, error)
	QueryMessageByID(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error)
	MessageTrackDetail(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error)
	QueryMessageByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error)
	QueryMessagesByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) ([]messageDetail, error)
	ConsumeMessageDirectly(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error)
	ConsumeMessageDirectlyByID(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error)
	ResendMessageByID(ctx context.Context, nameServer string, topic string, clusterName string, msgID string, unitName string) (*queryMsgByIDResendResult, error)
	QueryMessageTraceByID(ctx context.Context, nameServer string, traceTopic string, msgID string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageTraceView, error)
	ConsumerProgress(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error)
	ConsumerProgressWithClientIP(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error)
	ConsumerConnectionSummary(ctx context.Context, nameServer string, consumerGroup string) (*consumerConnectionSummary, error)
	ConsumerProgressSummary(ctx context.Context, nameServer string) ([]consumerProgressSummaryRow, error)
	StartMonitoring(ctx context.Context, nameServer string) error
}

type topicClusterRow struct {
	// ClusterName 是 Topic 所属集群名，对应官方 topicList -c 第一列。
	ClusterName string
	// Topic 是当前 Topic 名称，对应官方 topicList -c 第二列。
	Topic string
	// ConsumerGroup 是订阅该 Topic 的消费组；无消费组时保持空字符串。
	ConsumerGroup string
}

type allocateMQAssignment struct {
	// IP 是官方 allocateMQ -i 传入的消费者标识，作为 JSON result 的 key。
	IP string
	// Queues 是 AllocateMessageQueueAveragely 分配给该消费者的 MessageQueue 列表。
	Queues []messageQueueIdentity
}

type clusterListRow struct {
	// ClusterName 是 Broker 所属集群名，对应官方 clusterList 第一列。
	ClusterName string
	// BrokerName 是 Broker 逻辑名称，对应官方 clusterList 第二列。
	BrokerName string
	// BrokerID 是 Broker 地址表中的 ID，master 通常为 0。
	BrokerID string
	// Addr 是 Broker remoting 地址。
	Addr string
	// Version 是 Broker runtime 返回的版本描述。
	Version string
	// InTPS 是 putTps 字段第一段数值。
	InTPS float64
	// SendThreadPoolQueueSize 是发送线程池队列长度。
	SendThreadPoolQueueSize string
	// SendThreadPoolQueueHeadWaitMS 是发送线程池队头等待毫秒数。
	SendThreadPoolQueueHeadWaitMS string
	// OutTPS 是 getTransferredTps 字段第一段数值。
	OutTPS float64
	// PullThreadPoolQueueSize 是拉取线程池队列长度。
	PullThreadPoolQueueSize string
	// PullThreadPoolQueueHeadWaitMS 是拉取线程池队头等待毫秒数。
	PullThreadPoolQueueHeadWaitMS string
	// AckThreadPoolQueueSize 是 ack 线程池队列长度，缺省按官方显示 N。
	AckThreadPoolQueueSize string
	// AckThreadPoolQueueHeadWaitMS 是 ack 线程池队头等待毫秒数，缺省按官方显示 N。
	AckThreadPoolQueueHeadWaitMS string
	// TimerReadBehind 是定时消息读取滞后量。
	TimerReadBehind int64
	// TimerOffsetBehind 是定时消息 offset 滞后量。
	TimerOffsetBehind int64
	// TimerCongestNum 是定时消息拥塞数量，显示时按官方除以 10000。
	TimerCongestNum int64
	// TimerEnqueueTPS 是定时消息入队 TPS。
	TimerEnqueueTPS float64
	// TimerDequeueTPS 是定时消息出队 TPS。
	TimerDequeueTPS float64
	// PageCacheLockTimeMS 是 page cache 锁等待毫秒数。
	PageCacheLockTimeMS string
	// Hour 是最早消息到当前时间的小时差。
	Hour float64
	// CommitLogDiskRatio 是 commitlog 磁盘使用比例。
	CommitLogDiskRatio float64
	// BrokerActive 表示 Broker runtime 中的 brokerActive 状态。
	BrokerActive bool
}

type clusterAclConfigVersionRow struct {
	// ClusterName 是 ACL 版本信息所属集群名，对应官方第一列。
	ClusterName string
	// BrokerName 是返回 ACL 版本信息的 Broker 逻辑名称，对应官方第二列。
	BrokerName string
	// BrokerAddr 是返回 ACL 版本信息的 Broker 地址，对应官方第三列。
	BrokerAddr string
	// AclFilePath 是 Broker 返回的 ACL 文件路径，对应官方第四列。
	AclFilePath string
	// VersionCounter 是 ACL DataVersion 的 counter 字段。
	VersionCounter int64
	// LastUpdateTime 是 ACL DataVersion 的 timestamp 字段。
	LastUpdateTime time.Time
}

type listUserRow struct {
	// Username 是 RocketMQ 5.x auth 用户名，对应官方 listUser 第一列。
	Username string
	// Password 是 Broker 返回的用户密码显示值，对应官方 listUser 第二列。
	Password string
	// UserType 是用户类型，例如 Super 或 Normal，对应官方 listUser 第三列。
	UserType string
	// UserStatus 是用户状态，例如 enable 或 disable，对应官方 listUser 第四列。
	UserStatus string
	// SourceAddress 是 cluster 分支首个返回非空用户列表的 master broker 地址，仅用于追加官方 success 行。
	SourceAddress string
}

type copyUserResult struct {
	// Username 是已复制的用户名，对应官方 copyUser success 行中的用户字段。
	Username string
	// SourceBroker 是官方 -f/--fromBroker，用于 success 行中的源 Broker 地址。
	SourceBroker string
	// TargetBroker 是官方 -t/--toBroker，用于 success 行中的目标 Broker 地址。
	TargetBroker string
}

type aclInfo struct {
	// Subject 是 ACL 主体，对应官方表格 #Subject 列和 copyAcl success 行。
	Subject string `json:"subject,omitempty"`
	// Policies 是 RocketMQ 5.x AclInfo.policies，按 Broker 返回顺序打印。
	Policies []aclPolicyInfo `json:"policies,omitempty"`
	// SourceAddress 是 cluster 分支当前 ACL 来源 master broker 地址，仅用于保留查询来源。
	SourceAddress string `json:"-"`
}

type aclPolicyInfo struct {
	// PolicyType 是策略类型，对应官方表格 #PolicyType 列。
	PolicyType string `json:"policyType,omitempty"`
	// Entries 是该策略下的资源权限条目，对应官方逐行输出。
	Entries []aclPolicyEntryInfo `json:"entries,omitempty"`
}

type aclPolicyEntryInfo struct {
	// Resource 是 ACL 资源名，对应官方表格 #Resource 列。
	Resource string `json:"resource,omitempty"`
	// Actions 是允许或拒绝的动作列表，对应官方 Java List.toString 输出。
	Actions []string `json:"actions,omitempty"`
	// SourceIps 是来源 IP 列表，对应官方 Java List.toString 输出。
	SourceIps []string `json:"sourceIps,omitempty"`
	// Decision 是 Allow/Deny 等决策，对应官方表格 #Decision 列。
	Decision string `json:"decision,omitempty"`
}

type copyAclResult struct {
	// Subject 是已复制的 ACL 主体，对应官方 copyAcl success 行中的 subject。
	Subject string
	// SourceBroker 是官方 -f/--fromBroker，用于 success 行中的源 Broker 地址。
	SourceBroker string
	// TargetBroker 是官方 -t/--toBroker，用于 success 行中的目标 Broker 地址。
	TargetBroker string
}

type commitLogReadAheadModeSection struct {
	// Header 是官方 setAndPrint 输出的 broker 分隔标题，不包含前导空格和换行。
	Header string
	// Result 是 Broker success remark；官方 Java 在空 remark 时拼接 null 字符串。
	Result string
	// Raw 是集群不存在等由 CommandUtil 直接写出的原始文本。
	Raw string
}

type authUserOptions struct {
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 写入 auth 用户。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群全部 Broker 地址写入 auth 用户。
	ClusterName string
	// Username 是官方 -u/--username，作为请求 header 和 body 的用户名。
	Username string
	// Password 是官方 -p/--password，create 必填，update 可选。
	Password string
	// UserType 是官方 -t/--userType，create/update 可选。
	UserType string
	// UserStatus 是官方 -s/--userStatus，仅 update 可选。
	UserStatus string
	// PasswordSet 表示 CLI 显式选择了 -p/--password，用于复刻 updateUser 的 OptionGroup 单选语义。
	PasswordSet bool
	// UserTypeSet 表示 CLI 显式选择了 -t/--userType，用于只编码官方选中的变更字段。
	UserTypeSet bool
	// UserStatusSet 表示 CLI 显式选择了 -s/--userStatus，用于只编码官方选中的变更字段。
	UserStatusSet bool
}

type authUserBodyMode int

const (
	// authUserBodyNone 表示官方 deleteUser 请求不携带 body。
	authUserBodyNone authUserBodyMode = iota
	// authUserBodyCreate 表示官方 createUser body 只包含 username/password/userType。
	authUserBodyCreate
	// authUserBodyUpdate 表示官方 updateUser body 只包含 username 与被 OptionGroup 选中的单个变更字段。
	authUserBodyUpdate
)

type aclOptions struct {
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 写入 ACL。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群全部 Broker 地址写入 ACL。
	ClusterName string
	// Subject 是官方 -s/--subject，作为请求 header 和 body 的 ACL 主体。
	Subject string
	// Resources 是官方 -r/--resources 按逗号拆分后的资源列表，用于 createAcl/updateAcl body。
	Resources []string
	// Actions 是官方 -a/--actions 按逗号拆分后的动作列表，用于每个 ACL policy entry。
	Actions []string
	// SourceIps 是官方 -i/--sourceIp 按逗号拆分后的来源 IP 列表，未选择时保持 nil 以省略 JSON 字段。
	SourceIps []string
	// Decision 是官方 -d/--decision，写入每个 ACL policy entry。
	Decision string
	// Resource 是官方 deleteAcl -r/--resources 原始字符串；deleteAcl 不按逗号拆分该参数。
	Resource string
}

type aclConfigOptions struct {
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 写入历史 ACL YAML 配置。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群全部 Broker 地址写入历史 ACL YAML 配置。
	ClusterName string
	// AccessKey 是官方 -a/--accessKey，作为账号配置主键和 delete 请求 header。
	AccessKey string
	// SecretKey 是官方 -s/--secretKey，updateAclConfig 必填。
	SecretKey string
	// WhiteRemoteAddress 是官方 -w/--whiteRemoteAddress，写入账号白名单。
	WhiteRemoteAddress string
	// DefaultTopicPerm 是官方 -i/--defaultTopicPerm，写入账号默认 Topic 权限。
	DefaultTopicPerm string
	// DefaultGroupPerm 是官方 -u/--defaultGroupPerm，写入账号默认 Group 权限。
	DefaultGroupPerm string
	// TopicPerms 是官方 -t/--topicPerms 按逗号拆分后的 Topic 权限列表。
	TopicPerms []string
	// TopicPermsSet 表示 CLI 显式传入 -t/--topicPerms，用于复刻 Java null 与列表的输出差异。
	TopicPermsSet bool
	// GroupPerms 是官方 -g/--groupPerms 按逗号拆分后的 Group 权限列表。
	GroupPerms []string
	// GroupPermsSet 表示 CLI 显式传入 -g/--groupPerms，用于复刻 Java null 与列表的输出差异。
	GroupPermsSet bool
	// Admin 是官方 -m/--admin 解析出的管理员标记；未传时保持 Java boolean 默认 false。
	Admin bool
	// AdminSet 表示 CLI 显式传入 -m/--admin，用于区分 formatter 中的默认值来源。
	AdminSet bool
}

type globalWhiteAddrOptions struct {
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 更新全局白名单。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群全部 Broker 地址更新全局白名单。
	ClusterName string
	// GlobalWhiteRemoteAddresses 是官方 -g/--globalWhiteRemoteAddresses，全局白名单地址列表原文。
	GlobalWhiteRemoteAddresses string
	// AclFileFullPath 是官方 -p/--aclFileFullPath，指定要更新的 ACL 文件路径。
	AclFileFullPath string
}

type clusterListMoreStatsRow struct {
	// ClusterName 是 Broker 所属集群名，对应官方 clusterList -m 第一列。
	ClusterName string
	// BrokerName 是 Broker 逻辑名称，对应官方 clusterList -m 第二列。
	BrokerName string
	// InTotalYest 是昨日写入消息总量，由 runtime 今日晨间值减昨日晨间值。
	InTotalYest int64
	// OutTotalYest 是昨日读取消息总量，由 runtime 今日晨间值减昨日晨间值。
	OutTotalYest int64
	// InTotalToday 是今日写入消息总量，由 runtime 当前值减今日晨间值。
	InTotalToday int64
	// OutTotalToday 是今日读取消息总量，由 runtime 当前值减今日晨间值。
	OutTotalToday int64
}

type clusterListBrokerRuntimeStats struct {
	// ClusterName 是当前 Broker 所属集群，用于保持官方输出分组顺序。
	ClusterName string
	// BrokerName 是 Broker 逻辑名称。
	BrokerName string
	// BrokerID 是 Broker 地址表中的 ID。
	BrokerID string
	// Addr 是 Broker remoting 地址。
	Addr string
	// Stats 是 GET_BROKER_RUNTIME_INFO 返回的 KVTable.table。
	Stats map[string]string
}

type brokerStatusTable struct {
	// BrokerAddr 是 cluster 模式下官方输出的 Broker 地址列；单 Broker 模式保持空字符串。
	BrokerAddr string
	// Stats 是 GET_BROKER_RUNTIME_INFO 返回的 KVTable.table，输出前按 key 字典序排序。
	Stats map[string]string
}

type brokerConfigSection struct {
	// Header 是官方 getBrokerConfig 每个 Broker 配置段的分隔行。
	Header string
	// Entries 是按 JDK 21 Properties.entrySet 顺序输出的配置项。
	Entries []brokerConfigEntry
	// Raw 是官方特殊提示行，例如 clusterName 不存在时的错误提示。
	Raw string
}

type brokerConfigEntry struct {
	// Key 是 Broker 配置项名称。
	Key string
	// Value 是 Broker 配置项字符串值。
	Value string
}

type exportConfigsData struct {
	// NameServerSize 是官方 clusterScale.namesrvSize，来源于当前命令连接到的 NameServer 地址数量。
	NameServerSize int
	// MasterBrokerSize 是官方 clusterScale.masterBrokerSize，只统计集群内 master Broker。
	MasterBrokerSize int
	// SlaveBrokerSize 是官方 clusterScale.slaveBrokerSize，统计 master 下的 slave 地址数量。
	SlaveBrokerSize int
	// BrokerConfigs 保存每个 master Broker 被筛选后的配置。
	BrokerConfigs []exportBrokerConfig
}

type exportBrokerConfig struct {
	// BrokerName 是官方 brokerConfigs 对象中的 key，优先取 Broker 配置里的 brokerName。
	BrokerName string
	// Entries 是 Broker 原始配置项，写出前只保留官方 needBrokerProperties 的字段。
	Entries []brokerConfigEntry
}

type exportMetadataOptions struct {
	// ClusterName 是官方 -c/--clusterName，指定按集群合并导出 metadata。
	ClusterName string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 导出 topic 或 subscriptionGroup wrapper。
	BrokerAddr string
	// FilePath 是官方 -f/--filePath，表示导出目录。
	FilePath string
	// TopicOnly 对应官方 -t/--topic，只导出 topic metadata。
	TopicOnly bool
	// SubscriptionGroupOnly 对应官方 -g/--subscriptionGroup，只导出 subscriptionGroup metadata。
	SubscriptionGroupOnly bool
	// SpecialTopic 对应官方 -s/--specialTopic，允许导出 retry 和 dlq topic。
	SpecialTopic bool
	// NowMillis 是测试注入的 exportTime；为 0 时使用当前时间。
	NowMillis int64
}

type exportMetadataResult struct {
	// OutputPath 是本次真实写出的 JSON 文件路径。
	OutputPath string
	// PrintNewline 表示官方该分支 success 文本是否追加换行。
	PrintNewline bool
	// Wrote 表示本次是否写出文件；官方 brokerAddr 但未传 -t/-g 时不会写文件。
	Wrote bool
}

type exportMetadataData struct {
	// ExportTime 是官方 cluster 模式写入的毫秒时间戳。
	ExportTime int64
	// TopicConfigs 是合并后的 topicConfigTable。
	TopicConfigs []exportMetadataTopicConfig
	// SubscriptionGroups 是合并后的 subscriptionGroupTable 原始 JSON pair。
	SubscriptionGroups []orderedJSONPair
	// IncludeTopics 控制是否输出 topicConfigTable。
	IncludeTopics bool
	// IncludeGroups 控制是否输出 subscriptionGroupTable。
	IncludeGroups bool
}

type exportMetadataTopicConfig struct {
	// Name 是 topicConfigTable 的 key。
	Name string
	// Value 是 TopicConfig 的 JSON 对象值。
	Value orderedJSONValue
}

type exportMetricsData struct {
	// Total 保存官方 totalData 中跨 Broker 聚合后的 TPS 与一天消息数。
	Total exportMetricsTotal
	// Reports 保存官方 evaluateReport 中每个 master Broker 的运行评估。
	Reports []exportMetricsBrokerReport
}

type exportMetricsTotal struct {
	// NormalInTps 是普通消息写入 TPS 的集群合计。
	NormalInTps float64
	// NormalOutTps 是普通消息读取 TPS 的集群合计。
	NormalOutTps float64
	// TransInTps 是事务半消息写入 TPS 的集群合计。
	TransInTps float64
	// ScheduleInTps 是延时消息写入 TPS 的集群合计。
	ScheduleInTps float64
	// NormalOneDayInNum 是普通消息一天写入数量的集群合计。
	NormalOneDayInNum int64
	// NormalOneDayOutNum 是普通消息一天读取数量的集群合计。
	NormalOneDayOutNum int64
	// TransOneDayInNum 是事务半消息一天写入数量的集群合计。
	TransOneDayInNum int64
	// ScheduleOneDayInNum 是延时消息一天写入数量的集群合计。
	ScheduleOneDayInNum int64
}

type exportMetricsBrokerReport struct {
	// BrokerName 是 evaluateReport 的 broker key，来源于官方 ClusterInfo brokerName。
	BrokerName string
	// RuntimeEnv 保存 cpuNum 与 totalMemKBytes 等环境指标。
	RuntimeEnv exportMetricsRuntimeEnv
	// RuntimeQuota 保存磁盘比例、TPS、消息量、Topic 与 Group 数量。
	RuntimeQuota exportMetricsRuntimeQuota
	// RuntimeVersion 保存 RocketMQ 版本和在线客户端语言版本集合。
	RuntimeVersion exportMetricsRuntimeVersion
}

type exportMetricsRuntimeEnv struct {
	// CPUNum 对应官方 cpuNum，实际来源是 Broker 配置 clientCallbackExecutorThreads。
	CPUNum string
	// TotalMemKBytes 对应 Broker runtime table 的 totalMemKBytes，空值按 FastJSON 规则省略。
	TotalMemKBytes string
}

type exportMetricsRuntimeQuota struct {
	// CommitLogDiskRatio 对应 runtime table 的 commitLogDiskRatio。
	CommitLogDiskRatio string
	// ConsumeQueueDiskRatio 对应 runtime table 的 consumeQueueDiskRatio。
	ConsumeQueueDiskRatio string
	// NormalInTps 是 putTps 第一段数值。
	NormalInTps float64
	// NormalOutTps 是 getTransferredTps 第一段数值。
	NormalOutTps float64
	// TransInTps 是事务半消息 TOPIC_PUT_NUMS 的分钟 TPS。
	TransInTps float64
	// ScheduleInTps 是延时 Topic TOPIC_PUT_NUMS 的分钟 TPS。
	ScheduleInTps float64
	// NormalOneDayInNum 是 msgPutTotalTodayMorning 减 msgPutTotalYesterdayMorning。
	NormalOneDayInNum int64
	// NormalOneDayOutNum 是 msgGetTotalTodayMorning 减 msgGetTotalYesterdayMorning。
	NormalOneDayOutNum int64
	// TransOneDayInNum 是事务半消息 24 小时写入量。
	TransOneDayInNum int64
	// ScheduleOneDayInNum 是延时 Topic 24 小时写入量。
	ScheduleOneDayInNum int64
	// MessageAverageSize 对应 runtime table 的 putMessageAverageSize。
	MessageAverageSize string
	// TopicSize 是官方 getUserTopicConfig 后的 topicConfigTable 大小。
	TopicSize int
	// GroupSize 是官方 getUserSubscriptionGroup 后的 subscriptionGroupTable 大小。
	GroupSize int
}

type exportMetricsRuntimeVersion struct {
	// RocketMQVersion 是官方 MQVersion.CURRENT_VERSION 的版本描述。
	RocketMQVersion string
	// ClientInfo 是官方去重后的 Language%MQVersion 客户端集合。
	ClientInfo []string
}

type namesrvConfigSection struct {
	// NameServer 是官方 getNamesrvConfig 段标题中的 NameServer 地址。
	NameServer string
	// Entries 是按 JDK 21 Properties.entrySet 顺序输出的配置项。
	Entries []brokerConfigEntry
}

type consumerConfigSection struct {
	// Header 是官方 getConsumerConfig 的 cluster:brokerName 分隔行。
	Header string
	// Entries 是按 SubscriptionGroupConfig 字段声明顺序输出的配置项。
	Entries []consumerConfigEntry
}

type consumerConfigEntry struct {
	// Name 是 SubscriptionGroupConfig 字段名。
	Name string
	// Value 是字段值按官方反射打印规则转换后的字符串。
	Value string
}

type brokerEpochResult struct {
	// ClusterName 是 Broker epoch 缓存里的集群名。
	ClusterName string
	// BrokerName 是 Broker epoch 缓存里的 brokerName。
	BrokerName string
	// BrokerAddr 是本次读取 epoch 缓存的 broker 地址，输出直接使用路由表地址。
	BrokerAddr string
	// BrokerID 是 Broker epoch 缓存里的 brokerId。
	BrokerID int64
	// MaxOffset 是官方打印最后一条 epoch 前覆盖 endOffset 使用的最大位点。
	MaxOffset int64
	// EpochList 是 Broker 返回的 epoch 条目列表，顺序保持 Broker 响应顺序。
	EpochList []epochEntry
}

type epochEntry struct {
	// Epoch 是 controllerMode 下的 epoch 编号。
	Epoch int `json:"epoch"`
	// StartOffset 是该 epoch 起始 commitlog 位点。
	StartOffset int64 `json:"startOffset"`
	// EndOffset 是该 epoch 结束 commitlog 位点；最后一条打印前按官方逻辑覆盖为 MaxOffset。
	EndOffset int64 `json:"endOffset"`
}

type messageSearchResult struct {
	MessageID   string
	QueueID     int
	QueueOffset int64
	Topic       string
	Keys        []string
	UniqKey     string
}

type messageProperty struct {
	// Key 是 RocketMQ message property 名称。
	Key string
	// Value 是 RocketMQ message property 值。
	Value string
}

type messageProperties []messageProperty

func (properties messageProperties) Get(key string) string {
	for _, property := range properties {
		if property.Key == key {
			return property.Value
		}
	}
	return ""
}

func (properties *messageProperties) Set(key string, value string) {
	for index := range *properties {
		if (*properties)[index].Key == key {
			(*properties)[index].Value = value
			return
		}
	}
	*properties = append(*properties, messageProperty{Key: key, Value: value})
}

type messageDetail struct {
	// OffsetMessageID 是 Broker commitlog offset 生成的消息 ID，对应官方 OffsetID。
	OffsetMessageID string
	// DisplayMessageID 是 MessageClientExt.getMsgId()，存在 UNIQ_KEY 时优先使用。
	DisplayMessageID string
	// BrokerName 是 MessageQueue 所属 Broker 名称，printMsgByQueue 的 MessageExt.toString 需要展示。
	BrokerName string
	// Topic 是消息所属 Topic。
	Topic string
	// Tags 是原始 TAGS property；为空时官方打印 [null]。
	Tags string
	// Keys 是原始 KEYS property；为空时官方打印 [null]。
	Keys string
	// QueueID 是消息所在队列编号。
	QueueID int
	// QueueOffset 是消息在队列中的 offset。
	QueueOffset int64
	// CommitLogOffset 是消息在 commitlog 中的物理 offset。
	CommitLogOffset int64
	// StoreSize 是 commitlog 单条消息记录大小，对应 MessageExt.storeSize。
	StoreSize int
	// BodyCRC 是 commitlog 中记录的 body CRC，对应 MessageExt.bodyCRC。
	BodyCRC int32
	// Flag 是消息 flag 字段，对应 Message.flag。
	Flag int32
	// ReconsumeTimes 是消息重投次数。
	ReconsumeTimes int
	// PreparedTransactionOffset 是事务消息预提交物理位点，对应 MessageExt.preparedTransactionOffset。
	PreparedTransactionOffset int64
	// BornTimestamp 是生产端写入时间戳，单位毫秒。
	BornTimestamp int64
	// StoreTimestamp 是 Broker 存储时间戳，单位毫秒。
	StoreTimestamp int64
	// BornHost 是生产端地址，格式与 RemotingHelper.parseSocketAddressAddr 一致。
	BornHost string
	// StoreHost 是 Broker 存储地址，格式与 RemotingHelper.parseSocketAddressAddr 一致。
	StoreHost string
	// SysFlag 是 RocketMQ message sysFlag 原值。
	SysFlag int
	// Properties 是消息 properties，包含 pull 后追加的 MIN_OFFSET/MAX_OFFSET。
	Properties messageProperties
	// Body 是消息体原始字节。
	Body []byte
}

type messageTrack struct {
	// ConsumerGroup 是订阅当前 Topic 的消费组名称。
	ConsumerGroup string
	// TrackType 是官方 MessageTrack.trackType 输出的枚举文本。
	TrackType string
	// ExceptionDesc 是官方 MessageTrack.exceptionDesc，空值打印为 null。
	ExceptionDesc string
}

type printMsgByQueueOptions struct {
	// Topic 是要读取的 Topic，对应官方 -t/--topic。
	Topic string
	// BrokerName 是队列所属 Broker 名称，对应官方 -a/--brokerName。
	BrokerName string
	// QueueID 是要读取的队列编号，对应官方 -i/--queueId。
	QueueID int
	// ConsumerGroup 是 PullMessage 使用的消费组；为空时使用官方 TOOLS_CONSUMER。
	ConsumerGroup string
	// HasBeginTimestamp 表示用户是否显式传入 -b/--beginTimestamp。
	HasBeginTimestamp bool
	// BeginTimestamp 是起始时间戳，单位毫秒；存在时会先 searchOffset。
	BeginTimestamp int64
	// HasEndTimestamp 表示用户是否显式传入 -e/--endTimestamp。
	HasEndTimestamp bool
	// EndTimestamp 是结束时间戳，单位毫秒；存在时会先 searchOffset。
	EndTimestamp int64
	// PrintMessage 表示是否逐条打印消息，默认 false。
	PrintMessage bool
	// PrintBody 表示是否打印消息体文本，默认 false。
	PrintBody bool
	// CharsetName 是消息体转字符串使用的字符集，默认 UTF-8。
	CharsetName string
	// SubExpression 是 pull 订阅表达式，默认 *。
	SubExpression string
	// CalculateByTag 表示是否按 Tag 统计数量，默认 false。
	CalculateByTag bool
}

type consumeMessageOptions struct {
	// Topic 是要消费的 Topic，对应官方 -t/--topic。
	Topic string
	// BrokerName 是指定队列所属 Broker 名称，对应官方 -b/--brokerName。
	BrokerName string
	// QueueID 是指定队列编号，对应官方 -i/--queueId。
	QueueID int
	// HasQueueID 表示用户是否显式传入 -i/--queueId。
	HasQueueID bool
	// Offset 是指定队列消费起点，对应官方 -o/--offset。
	Offset int64
	// HasOffset 表示用户是否显式传入 -o/--offset。
	HasOffset bool
	// ConsumerGroup 是 PullConsumer 的消费组，对应官方 -g/--consumerGroup。
	ConsumerGroup string
	// MessageCount 是本次最多消费的消息数，对应官方 -c/--MessageNumber。
	MessageCount int64
	// HasBeginTimestamp 表示用户是否显式传入 -s/--beginTimestamp。
	HasBeginTimestamp bool
	// BeginTimestamp 是起始时间戳，单位毫秒；存在时会先 searchOffset。
	BeginTimestamp int64
	// HasEndTimestamp 表示用户是否显式传入 -e/--endTimestamp。
	HasEndTimestamp bool
	// EndTimestamp 是结束时间戳，单位毫秒；存在时会先 searchOffset。
	EndTimestamp int64
}

type consumeMessageResult struct {
	// Notices 是官方在拉取前输出的提示行，例如 oldler/older 限量提示。
	Notices []string
	// Messages 是兼容单批输出的消息集合，formatter 会在前面输出 Consume ok。
	Messages []messageDetail
	// Entries 是按官方 pull 循环顺序记录的输出事件。
	Entries []consumeMessageOutputEntry
}

type consumeMessageOutputEntry struct {
	// Notice 是当前队列或批次拉取前的提示行。
	Notice string
	// StatusLine 是非 FOUND 状态下官方输出的状态行。
	StatusLine string
	// Messages 是 FOUND 状态下本批次拉到的消息。
	Messages []messageDetail
}

type printMsgOptions struct {
	// Topic 是要读取的真实 Topic，对应官方 -t/--topic。
	Topic string
	// LMQParentTopic 是 LMQ 场景下用于查询路由的父 Topic，对应官方 -l/--lmqParentTopic。
	LMQParentTopic string
	// HasBeginTimestamp 表示用户是否显式传入 -b/--beginTimestamp。
	HasBeginTimestamp bool
	// BeginTimestamp 是起始时间戳，单位毫秒；存在时会先 searchOffset。
	BeginTimestamp int64
	// HasEndTimestamp 表示用户是否显式传入 -e/--endTimestamp。
	HasEndTimestamp bool
	// EndTimestamp 是结束时间戳，单位毫秒；存在时会先 searchOffset。
	EndTimestamp int64
	// HasPrintBody 表示用户是否显式传入 -d/--printBody；未传时官方默认为 true。
	HasPrintBody bool
	// PrintBody 表示是否打印消息体文本，对应官方 -d/--printBody。
	PrintBody bool
	// CharsetName 是消息体转字符串使用的字符集，默认 UTF-8。
	CharsetName string
	// SubExpression 是 pull 订阅表达式，默认 *。
	SubExpression string
}

type printMsgResult struct {
	// Queues 是按官方 fetchSubscribeMessageQueues 的 Java HashSet 顺序扫描的队列结果。
	Queues []printMsgQueueResult
}

type printMsgQueueResult struct {
	// Queue 是当前扫描的 MessageQueue。
	Queue messageQueueIdentity
	// MinOffset 是当前队列本次扫描的起始 offset，已按 beginTimestamp 修正。
	MinOffset int64
	// MaxOffset 是当前队列本次扫描的结束 offset，已按 endTimestamp 修正。
	MaxOffset int64
	// Messages 是当前队列 pull 到的消息批次内容。
	Messages []messageDetail
}

type printMsgByQueueResult struct {
	// Messages 是按 pull 返回顺序收集的消息列表。
	Messages []messageDetail
}

type printMsgByQueueTagCount struct {
	// Tag 是消息 TAGS property 中的标签值。
	Tag string
	// Count 是该标签出现次数。
	Count int64
}

type consumeMessageDirectlyResult struct {
	// Order 表示消费者是否按顺序消费该消息。
	Order bool `json:"order"`
	// AutoCommit 表示直接消费完成后是否自动提交结果。
	AutoCommit bool `json:"autoCommit"`
	// ConsumeResult 是消费者直接消费返回的结果枚举名称。
	ConsumeResult string `json:"consumeResult"`
	// Remark 是消费者返回的说明；nil 时官方 toString 打印 null。
	Remark *string `json:"remark"`
	// SpentTimeMills 是消费者直接消费耗时，字段名沿用官方拼写。
	SpentTimeMills int64 `json:"spentTimeMills"`
}

type consumeMessageDirectlyUnavailableError struct {
	// ClientID 是官方提示中要展示的消费者客户端 ID。
	ClientID string
	// RunningInfoFailed 表示 getConsumerRunningInfo 阶段失败，官方会先打印一行 runtime info failed。
	RunningInfoFailed bool
	// Cause 是底层查询运行信息时返回的错误。
	Cause error
}

func (err *consumeMessageDirectlyUnavailableError) Error() string {
	return fmt.Sprintf("get consumer info failed or this %s client is not push consumer ,not support direct push", err.ClientID)
}

func (err *consumeMessageDirectlyUnavailableError) Unwrap() error {
	return err.Cause
}

type messageTraceView struct {
	// MsgID 是被追踪的业务消息 ID。
	MsgID string
	// Tags 是 trace body 中记录的消息 tags。
	Tags string
	// Keys 是 trace body 中记录的业务 keys。
	Keys string
	// StoreHost 是生产 trace 中记录的 Broker 存储地址。
	StoreHost string
	// ClientHost 是承载 trace 消息的 BornHost，官方 TraceView 使用 MessageExt.getBornHostString。
	ClientHost string
	// CostTime 是发送或消费耗时，单位毫秒。
	CostTime int
	// MsgType 是 TraceType 名称，例如 Pub 或 SubAfter。
	MsgType string
	// OffsetMessageID 是生产 trace 中记录的 offsetMsgId。
	OffsetMessageID string
	// TimeStamp 是 trace 事件时间戳，单位毫秒。
	TimeStamp int64
	// Topic 是业务消息 Topic。
	Topic string
	// GroupName 是生产者组或消费者组名称。
	GroupName string
	// Status 是官方可见状态 success/failed。
	Status string
}

type topicStatusEntry struct {
	// BrokerName 是队列所在 Broker 名称，对应官方 MessageQueue.brokerName。
	BrokerName string
	// QueueID 是 Topic 队列编号。
	QueueID int
	// MinOffset 是队列最小 offset。
	MinOffset int64
	// MaxOffset 是队列最大 offset。
	MaxOffset int64
	// LastUpdateTimestamp 是 Broker 返回的最后更新时间毫秒时间戳，0 表示官方输出空白。
	LastUpdateTimestamp int64
}

type consumerProgress struct {
	// Entries 是官方 ConsumeStats.offsetTable 解码后的队列位点。
	Entries []consumerProgressEntry
	// ConsumeTPS 是 Broker 返回的消费 TPS 汇总。
	ConsumeTPS float64
}

type consumerProgressEntry struct {
	// Topic 是 MessageQueue.topic。
	Topic string
	// BrokerName 是 MessageQueue.brokerName。
	BrokerName string
	// QueueID 是 MessageQueue.queueId。
	QueueID int
	// BrokerOffset 是 Broker 当前最大消费位点。
	BrokerOffset int64
	// ConsumerOffset 是消费者已提交位点。
	ConsumerOffset int64
	// PullOffset 是消费者拉取位点，用于计算 inflight。
	PullOffset int64
	// LastTimestamp 是最后消费时间戳，0 时官方输出 N/A。
	LastTimestamp int64
	// ClientIP 是 showClientIP 模式下对应消费者客户端 IP；普通模式为空。
	ClientIP string
}

type brokerConsumeStats struct {
	// BrokerAddr 是 Broker 返回的地址字段，对应 ConsumeStatsList.brokerAddr。
	BrokerAddr string
	// TotalDiff 是 Broker 聚合后的总堆积，对应官方 Diff Total。
	TotalDiff int64
	// TotalInflightDiff 是 Broker 返回的 inflight 聚合值，当前 brokerConsumeStats 官方 CLI 不展示。
	TotalInflightDiff int64
	// Groups 按官方 body 中 consumeStatsList 的顺序保存消费组统计。
	Groups []brokerConsumeStatsGroup
}

type brokerConsumeStatsGroup struct {
	// Group 是 consumeStatsList 外层 Map 的消费组名，对应官方 #Group 列。
	Group string
	// Stats 是该消费组下的 ConsumeStats 列表，保留 Broker body 原始列表顺序。
	Stats []consumerProgress
}

type statsAllRow struct {
	// Topic 是官方 statsAll 输出的 Topic 列。
	Topic string
	// ConsumerGroup 是消费组列；无消费者时为空字符串。
	ConsumerGroup string
	// Accumulation 是该消费组在当前 Topic 下的总堆积。
	Accumulation int64
	// InTPS 是 Topic 写入 TPS，来自 TOPIC_PUT_NUMS 的分钟统计。
	InTPS float64
	// OutTPS 是消费组拉取 TPS，来自 GROUP_GET_NUMS 的分钟统计。
	OutTPS float64
	// InMsg24Hour 是 Topic 近 24 小时写入量，按官方 day/hour/minute sum 回退规则计算。
	InMsg24Hour int64
	// OutMsg24Hour 是消费组近 24 小时拉取量，按官方 day/hour/minute sum 回退规则计算。
	OutMsg24Hour int64
	// NoConsumer 表示官方 NO_CONSUMER 行，输出时隐藏消费组和 OutTPS/OutMsg24Hour。
	NoConsumer bool
}

type brokerStatsData struct {
	// StatsMinute 是 Broker 返回的分钟粒度统计。
	StatsMinute brokerStatsItem `json:"statsMinute"`
	// StatsHour 是 Broker 返回的小时粒度统计。
	StatsHour brokerStatsItem `json:"statsHour"`
	// StatsDay 是 Broker 返回的日粒度统计。
	StatsDay brokerStatsItem `json:"statsDay"`
}

type brokerStatsItem struct {
	// Sum 是当前粒度累计消息数。
	Sum int64 `json:"sum"`
	// TPS 是当前粒度吞吐速率。
	TPS float64 `json:"tps"`
	// Avgpt 是 Broker StatsItem 的平均处理时间字段，statsAll 当前不展示。
	Avgpt float64 `json:"avgpt"`
}

type haStatusResult struct {
	// Master 表示当前节点是否为 HA master。
	Master bool
	// MasterCommitLogMaxOffset 是 master 模式下的 commitlog 最大 offset。
	MasterCommitLogMaxOffset int64
	// InSyncSlaveNums 是 master 当前同步副本数量。
	InSyncSlaveNums int
	// HAConnectionInfo 是 master 侧的 slave 连接列表。
	HAConnectionInfo []haConnectionRuntimeInfo
	// HAClientRuntimeInfo 是 slave 侧的运行时信息。
	HAClientRuntimeInfo haClientRuntimeInfo
}

type haConnectionRuntimeInfo struct {
	// Addr 是 slave 地址。
	Addr string
	// SlaveAckOffset 是 slave 已确认的 offset。
	SlaveAckOffset int64
	// Diff 是 slave 与 master 的 offset 差值。
	Diff int64
	// InSync 表示 slave 是否同步。
	InSync bool
	// TransferredByteInSecond 是秒级传输字节数。
	TransferredByteInSecond int64
	// TransferFromWhere 是 slave 的起始传输位置。
	TransferFromWhere int64
}

type haClientRuntimeInfo struct {
	// MasterAddr 是 slave 对应的 master 地址。
	MasterAddr string
	// TransferredByteInSecond 是秒级传输字节数。
	TransferredByteInSecond int64
	// MaxOffset 是 commitlog 最大 offset。
	MaxOffset int64
	// LastReadTimestamp 是最近一次读时间戳。
	LastReadTimestamp int64
	// LastWriteTimestamp 是最近一次写时间戳。
	LastWriteTimestamp int64
	// MasterFlushOffset 是 master 刷盘 offset。
	MasterFlushOffset int64
}

type haStatusBrokerResult struct {
	// Addr 是该条 HA 状态对应的 Broker 地址。
	Addr string
	// Result 是 Broker 返回的 HA 运行时信息。
	Result *haStatusResult
}

type checkRocksdbCqWriteProgressRow struct {
	// BrokerName 是官方输出前缀，来自 brokerAddrTable 的 brokerName。
	BrokerName string
	// CheckError 表示 Broker 返回 CHECK_ERROR，官方会输出 errInfo。
	CheckError bool
	// ErrorInfo 是 CHECK_ERROR 时 Broker 返回的 checkResult 文本。
	ErrorInfo string
}

type exportPopRecordRow struct {
	// BrokerName 是官方输出中的 brokerName；cluster 模式来自 ClusterInfo，broker 模式来自 Broker 配置。
	BrokerName string
	// BrokerAddr 是当前导出命令发送到的 Broker Remoting 地址。
	BrokerAddr string
	// DryRun 表示官方 exportPopRecord 是否跳过实际 Broker 请求；只有传入 -d false 时为 true。
	DryRun bool
	// Err 保存单个 Broker 导出失败；官方会打印错误行但不中断其它 Broker。
	Err error
}

type updateBrokerConfigOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 Broker。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 更新配置。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群批量更新 Broker 配置。
	ClusterName string
	// Key 是官方 -k/--key，表示要更新的 Broker 配置项名称。
	Key string
	// Value 是官方 -v/--value，表示要写入的配置值。
	Value string
	// UpdateAllBroker 是官方 -a/--updateAllBroker，只在 cluster 模式下包含 slave Broker。
	UpdateAllBroker bool
}

type updateNamesrvConfigOptions struct {
	// NameServers 是官方 -n/--namesrvAddr，表示要更新的 NameServer 地址列表。
	NameServers string
	// Key 是官方 -k/--key，表示要更新的 NameServer 配置项名称。
	Key string
	// Value 是官方 -v/--value，表示要写入的配置值。
	Value string
}

type updateControllerConfigOptions struct {
	// ControllerAddrs 是官方 -a/--controllerAddress，表示要更新的 controller 地址列表。
	ControllerAddrs string
	// Key 是官方 -k/--key，表示要更新的 controller 配置项名称。
	Key string
	// Value 是官方 -v/--value，表示要写入的配置值。
	Value string
}

type cleanBrokerMetadataOptions struct {
	// ControllerAddr 是官方 -a/--controllerAddress，表示 controller remoting 地址。
	ControllerAddr string
	// ClusterName 是官方 -c/--clusterName，表示待清理 Broker 所属集群。
	ClusterName string
	// BrokerName 是官方 -bn/--brokerName，表示要清理元数据的 Broker 名称。
	BrokerName string
	// BrokerControllerIDsToClean 是官方 -b/--brokerControllerIdsToClean，表示要清理的 controller brokerId 列表。
	BrokerControllerIDsToClean string
	// CleanLivingBroker 是官方 -l/--cleanLivingBroker，允许清理仍存活的 Broker 元数据。
	CleanLivingBroker bool
}

type electMasterOptions struct {
	// ControllerAddr 是官方 -a/--controllerAddress，表示 controller remoting 地址。
	ControllerAddr string
	// ClusterName 是官方 -c/--clusterName，表示待选主 Broker 所属集群。
	ClusterName string
	// BrokerName 是官方 -bn/--brokerName，表示待选主的副本组名称。
	BrokerName string
	// BrokerID 是官方 -b/--brokerId，表示期望成为 master 的 brokerId。
	BrokerID int64
}

type addBrokerOptions struct {
	// BrokerContainerAddr 是官方 -c/--brokerContainerAddr，表示 BrokerContainer remoting 地址。
	BrokerContainerAddr string
	// BrokerConfigPath 是官方 -b/--brokerConfigPath，表示 BrokerContainer 本机可读取的 broker 配置文件路径。
	BrokerConfigPath string
}

type removeBrokerOptions struct {
	// BrokerContainerAddr 是官方 -c/--brokerContainerAddr，表示 BrokerContainer remoting 地址。
	BrokerContainerAddr string
	// BrokerIdentity 是官方 -b/--brokerIdentity 原始值，格式为 clusterName:brokerName:brokerId。
	BrokerIdentity string
	// ClusterName 是 brokerIdentity 第一段，对应请求头 brokerClusterName。
	ClusterName string
	// BrokerName 是 brokerIdentity 第二段，对应请求头 brokerName。
	BrokerName string
	// BrokerID 是 brokerIdentity 第三段，对应请求头 brokerId。
	BrokerID int64
}

type writePermResult struct {
	// NameServer 是本次写权限变更命中的 NameServer 地址。
	NameServer string
	// Count 是官方响应头中的 wipeTopicCount/addTopicCount。
	Count int
}

type sendMessageOptions struct {
	// Topic 是官方 -t/--topic，表示目标 Topic。
	Topic string
	// Body 是官方 -p/--body，按 UTF-8 原样写入消息体。
	Body string
	// BodyBytes 是内部重发场景使用的原始消息体；非空时优先于 Body。
	BodyBytes []byte
	// Keys 是官方 -k/--key，写入 KEYS 消息属性。
	Keys string
	// Tags 是官方 -c/--tags，写入 TAGS 消息属性。
	Tags string
	// BrokerName 是官方 -b/--broker，只有和 QueueID 同时存在时才定向发送。
	BrokerName string
	// QueueID 是官方 -i/--qid，表示定向发送的队列 ID。
	QueueID int
	// HasQueueID 区分未传 -i 与显式传入 0。
	HasQueueID bool
	// MsgTraceEnable 是官方 -m/--msgTraceEnable；启用后主消息发送成功返回时同步写入 Pub trace。
	MsgTraceEnable bool
	// ProducerGroup 是内部发送使用的 Producer Group；为空时按 sendMessage 官方命令使用当前毫秒时间。
	ProducerGroup string
	// OmitWaitStoreProperty 表示是否省略 WAIT 属性；sendMsgStatus 官方 new Message() 不写 WAIT property。
	OmitWaitStoreProperty bool
	// PreserveProperties 表示按原消息 properties 发送；queryMsgById -s 依赖它保留原 UNIQ_KEY。
	PreserveProperties bool
	// Properties 是 PreserveProperties=true 时写入 SEND_MESSAGE_V2 的原消息 properties。
	Properties messageProperties
	// Flag 是 Message.flag 原值；普通 sendMessage 默认为 0，重发使用原消息 flag。
	Flag int32
	// UnitMode 对应官方 DefaultMQProducer.setUnitName 后的 requestHeader.unitMode。
	UnitMode bool
}

type sendMessageResult struct {
	// Topic 是 SendResult.MessageQueue.topic。
	Topic string
	// BrokerName 是 SendResult.MessageQueue.brokerName。
	BrokerName string
	// QueueID 是 SendResult.MessageQueue.queueId。
	QueueID int
	// SendStatus 是官方 SendStatus 字符串，例如 SEND_OK。
	SendStatus string
	// MessageID 是 SendResult.getMsgId()，即发送前生成并写入 UNIQ_KEY 的客户端消息 ID。
	MessageID string
	// OffsetMessageID 是 SendResult.offsetMsgId，来自 Broker 响应头 msgId。
	OffsetMessageID string
	// QueueOffset 是 SendResult.queueOffset，来自 Broker 响应头 queueOffset。
	QueueOffset int64
}

type queryMsgByIDResendResult struct {
	// OriginalMsgID 是用户传入的原始 msgId，用于官方 resend 前缀输出。
	OriginalMsgID string
	// SendResult 是重发成功后的 SendResult；为空表示官方语义下没有查询到消息。
	SendResult *sendMessageResult
}

type sendMessageTraceContext struct {
	// ProducerGroup 是官方 SendMessageTraceHookImpl 写入 Pub trace 的生产组名。
	ProducerGroup string
	// BrokerAddr 是主消息发送目标 Broker 地址，对应 Pub trace 的 storeHost 字段。
	BrokerAddr string
	// BornTimestamp 是主消息 SEND_MESSAGE_V2 请求头中的 bornTimestamp。
	BornTimestamp int64
	// CostTimeMillis 是主消息请求耗时毫秒，对应 Pub trace 的 costTime 字段。
	CostTimeMillis int
	// BodyLength 是业务消息体字节数，对应 Pub trace 的 bodyLength 字段。
	BodyLength int
	// RegionID 是 SendResult.regionId；Broker 未返回时按官方默认 DefaultRegion。
	RegionID string
	// TraceOn 是 SendResult.traceOn；只有 Broker 明确返回 TRACE_ON=false 才关闭。
	TraceOn bool
}

type sendMsgStatusOptions struct {
	// BrokerName 是官方 -b/--brokerName，同时也是 sendMsgStatus 构造消息时使用的 Topic。
	BrokerName string
	// MessageSize 是官方 -s/--messageSize，用于构造每次计时发送的消息体。
	MessageSize int
	// Count 是官方 -c/--count，表示 warmup 后打印多少次发送结果。
	Count int
}

type sendMsgStatusResult struct {
	// RTMillis 是官方每次发送前后 System.currentTimeMillis 差值。
	RTMillis int64
	// SendResult 是一次 producer.send 返回的官方可见结果字段。
	SendResult sendMessageResult
}

type checkMsgSendRTOptions struct {
	// Topic 是官方 -t/--topic，表示待压测发送响应时间的 Topic。
	Topic string
	// Amount 是官方 -a/--amount，表示发送消息条数，默认 100。
	Amount int
	// Size 是官方 -s/--size，表示每条消息体的字节数，默认 128。
	Size int
}

type checkMsgSendRTRow struct {
	// BrokerName 是本次 MessageQueueSelector 选中的 Broker 名称。
	BrokerName string
	// QueueID 是本次 MessageQueueSelector 选中的队列 ID。
	QueueID int
	// SendSuccess 对应官方 sendSuccess 布尔列，单条发送异常时为 false。
	SendSuccess bool
	// RTMillis 是本次发送的毫秒耗时。
	RTMillis int64
}

type checkMsgSendRTResult struct {
	// Rows 是官方逐次打印的 Broker/QID/Send Result/RT 明细。
	Rows []checkMsgSendRTRow
	// AvgRT 是官方忽略首条 warmup 样本后的平均 RT。
	AvgRT float64
}

type clusterRTOptions struct {
	// ClusterName 是官方 -c/--cluster，空值表示遍历全部集群。
	ClusterName string
	// Amount 是官方 -a/--amount，表示每个 BrokerName topic 的发送次数，默认 100。
	Amount int
	// Size 是官方 -s/--size，表示每条探测消息体字节数，默认 128。
	Size int
	// PrintAsTlog 是官方 -p/--print log，true 时输出 tlog 管道格式。
	PrintAsTlog bool
	// MachineRoom 是官方 -m/--machine room，tlog 输出的机房名，默认 noname。
	MachineRoom string
}

type clusterRTRow struct {
	// ClusterName 是当前 Broker 所属集群，对应官方 #Cluster Name 列。
	ClusterName string
	// BrokerName 是当前被压测的 Broker 名称，同时也是官方发送消息使用的 Topic。
	BrokerName string
	// RT 是忽略首条样本后的平均发送耗时，单位毫秒。
	RT float64
	// SuccessCount 是本轮成功发送次数，对应官方 #successCount。
	SuccessCount int
	// FailCount 是本轮发送失败次数，对应官方 #failCount。
	FailCount int
	// Timestamp 是 tlog 行的采样时间；为空时格式化阶段使用当前 GMT+8 时间。
	Timestamp time.Time
}

type clusterRTResult struct {
	// Rows 是每个 cluster/brokerName 生成的一行 RT 汇总。
	Rows []clusterRTRow
	// Raw 保存官方特殊分支的原始文本，例如指定不存在的 cluster。
	Raw string
}

type controllerMetaData struct {
	// Group 是 controller 所属组名，对应官方 #ControllerGroup。
	Group string
	// ControllerLeaderID 是 controller leader 的节点 ID。
	ControllerLeaderID string
	// ControllerLeaderAddress 是 controller leader 的 remoting 地址。
	ControllerLeaderAddress string
	// IsLeader 是响应头里的本节点 leader 标记；官方 CLI 当前不打印，但保留真实 header 值。
	IsLeader bool
	// Peers 是官方 header 中以分号分隔的 peer 列表。
	Peers string
}

type syncStateSetResult struct {
	// Brokers 按 controller 返回的 brokerName 顺序保存副本状态段。
	Brokers []syncStateSetBrokerInfo
}

type syncStateSetBrokerInfo struct {
	// BrokerName 是 controller 返回的 brokerName。
	BrokerName string
	// MasterBrokerID 是当前 master brokerId。
	MasterBrokerID int64
	// MasterAddress 是当前 master 地址。
	MasterAddress string
	// MasterEpoch 是 controller 中记录的 master epoch。
	MasterEpoch int
	// SyncStateSetEpoch 是同步副本集合 epoch。
	SyncStateSetEpoch int
	// InSyncReplicas 是同步副本列表，数量用于官方 #SyncStateSetNums。
	InSyncReplicas []syncStateSetReplicaIdentity
	// NotInSyncReplicas 是未同步副本列表。
	NotInSyncReplicas []syncStateSetReplicaIdentity
}

type syncStateSetReplicaIdentity struct {
	// BrokerName 是副本所属 brokerName。
	BrokerName string `json:"brokerName"`
	// BrokerID 是副本 brokerId。
	BrokerID int64 `json:"brokerId"`
	// BrokerAddress 是副本地址。
	BrokerAddress string `json:"brokerAddress"`
	// Alive 表示 controller 判断的副本存活状态；nil 时复刻 Java Boolean 的 null 文本。
	Alive *bool `json:"alive"`
}

type electMasterResult struct {
	// ClusterName 是命令入参中的集群名，官方输出直接使用请求值。
	ClusterName string
	// BrokerName 是命令入参中的 brokerName，官方输出直接使用请求值。
	BrokerName string
	// BrokerMasterAddr 是响应 header 的 masterAddress。
	BrokerMasterAddr string
	// MasterEpoch 是响应 header 的 masterEpoch。
	MasterEpoch int
	// SyncStateSetEpoch 是响应 header 的 syncStateSetEpoch。
	SyncStateSetEpoch int
	// BrokerMemberAddrs 保存响应 body 中 brokerAddrs 的有序条目；当前官方成功样本可能为空。
	BrokerMemberAddrs []electMasterBrokerMember
	// BrokerMemberAddrsOK 标记 body 是否成功解出 brokerAddrs 字段，便于测试表达空表是有效响应。
	BrokerMemberAddrsOK bool
}

type electMasterBrokerMember struct {
	// BrokerID 是 BrokerMemberGroup.brokerAddrs 的 key。
	BrokerID int64
	// BrokerAddress 是 BrokerMemberGroup.brokerAddrs 的 value。
	BrokerAddress string
}

type resetOffsetByTimeOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，指定队列分支仍保持与官方参数一致。
	NameServer string
	// Group 是官方 -g/--group，需要已存在订阅组，否则 Broker 会按官方返回组不存在。
	Group string
	// Topic 是官方 -t/--topic，表示要重置消费位点的 Topic。
	Topic string
	// TimestampText 是官方 -s/--timestamp 原始文本，用于 stdout 复刻。
	TimestampText string
	// TimestampMillis 是官方解析后的毫秒时间戳；指定队列未传 -o 时用于搜索目标位点。
	TimestampMillis int64
	// Force 是官方 -f/--force；指定队列 offset 分支不发送该字段，但保留解析结果。
	Force bool
	// ClusterName 是官方 -c/--cluster，timestamp 分支按官方语义作为集群名或 LMQ 父 Topic 使用。
	ClusterName string
	// BrokerAddr 是官方 -b/--broker，指定队列分支直连该 Broker。
	BrokerAddr string
	// QueueID 是官方 -q/--queue，表示目标队列。
	QueueID int
	// ExpectOffset 是官方 -o/--offset，指定队列分支直接使用该 offset，避免时间戳搜索。
	ExpectOffset int64
	// HasQueueID 标识用户显式传入 -q/--queue。
	HasQueueID bool
	// HasExpectOffset 标识用户显式传入 -o/--offset。
	HasExpectOffset bool
	// SpecifiedQueue 表示进入官方 broker+queue 指定队列分支，-o 缺省时按 timestamp 搜索 offset。
	SpecifiedQueue bool
}

type skipAccumulatedMessageOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，用于读取 TopicRoute 并定位 Broker。
	NameServer string
	// Group 是官方 -g/--group，表示要跳过堆积消息的消费组。
	Group string
	// Topic 是官方 -t/--topic，Broker 会将该 Topic 的消费位点重置到当前最大位点。
	Topic string
	// ClusterName 是官方 -c/--cluster；普通 Topic 透传给 Broker，LMQ 或 timer Topic 用它查路由。
	ClusterName string
	// Force 是官方 -f/--force，默认 true，用于决定是否强制回滚到目标时间戳位点。
	Force bool
}

type skipAccumulatedMessageRow struct {
	// Queue 是 Broker 返回的 MessageQueue key，输出列取 brokerName 和 queueId。
	Queue messageQueueIdentity
	// Offset 是 Broker 返回并写入消费组的目标 offset。
	Offset int64
}

type updateTopicOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式与 orderConf 写入需要使用。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 创建或更新 Topic。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量创建或更新 Topic。
	ClusterName string
	// Topic 是官方 -t/--topic，表示要创建或更新的 Topic 名称。
	Topic string
	// ReadQueueNums 是官方 -r/--readQueueNums，默认值为 8。
	ReadQueueNums int
	// WriteQueueNums 是官方 -w/--writeQueueNums，默认值为 8。
	WriteQueueNums int
	// Perm 是官方 -p/--perm，默认值为 6，即 RW-。
	Perm int
	// TopicFilterType 是官方 TopicConfig 默认过滤类型，当前 mqadmin updateTopic 固定为 SINGLE_TAG。
	TopicFilterType string
	// TopicSysFlag 是 -u/--unit 与 -s/--hasUnitSub 组合出来的官方 TopicSysFlag。
	TopicSysFlag int
	// Order 是官方 -o/--order，true 时会额外写 ORDER_TOPIC_CONFIG。
	Order bool
	// Attributes 是官方 -a/--attributes 原始表达式，例如 +a=b,-c。
	Attributes string
}

type updateTopicConfig struct {
	// TopicName 是 TopicConfig.topicName。
	TopicName string `json:"topicName"`
	// ReadQueueNums 是 TopicConfig.readQueueNums。
	ReadQueueNums int `json:"readQueueNums"`
	// WriteQueueNums 是 TopicConfig.writeQueueNums。
	WriteQueueNums int `json:"writeQueueNums"`
	// Perm 是 TopicConfig.perm 的数值形式。
	Perm int `json:"perm"`
	// TopicFilterType 是 TopicConfig.topicFilterType。
	TopicFilterType string `json:"topicFilterType"`
	// TopicSysFlag 是 TopicConfig.topicSysFlag。
	TopicSysFlag int `json:"topicSysFlag"`
	// Order 是 TopicConfig.order。
	Order bool `json:"order"`
	// Attributes 是 TopicConfig.attributes，key 保留官方 + 或 - 前缀。
	Attributes map[string]string `json:"attributes"`
}

type updateTopicListOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 master Broker。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 批量创建或更新 Topic。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量创建或更新 Topic。
	ClusterName string
	// FileName 是官方 -f/--filename，指向 TopicConfig JSON 数组文件。
	FileName string
	// TopicConfigs 是从 FileName 解出的 TopicConfig 列表，对应官方 JSON.parseArray(TopicConfig.class)。
	TopicConfigs []updateTopicConfig
}

type createTopicListRequestBody struct {
	// TopicConfigList 对应官方 CreateTopicListRequestBody.topicConfigList。
	TopicConfigList []updateTopicConfig `json:"topicConfigList"`
}

type updateTopicResult struct {
	// Targets 是官方逐行打印 create topic success 的目标 Broker 地址。
	Targets []string
	// Config 是官方最后打印的 TopicConfig.toString() 数据。
	Config updateTopicConfig
	// OrderConf 是 -o true 时写入 ORDER_TOPIC_CONFIG 的值，用于打印官方 orderConf 行。
	OrderConf string
}

type updateStaticTopicOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，用于读取集群信息并定位 Broker master 地址。
	NameServer string
	// BrokerNames 是官方 -b/--brokers 解析出的目标 Broker 名称列表，按用户传入顺序保留。
	BrokerNames []string
	// ClusterNames 是官方 -c/--clusters 解析出的目标 Cluster 名称列表，按用户传入顺序保留。
	ClusterNames []string
	// Topic 是官方 -t/--topic，表示要创建或更新的静态 Topic 名称。
	Topic string
	// TotalQueueNums 是官方 -qn/--totalQueueNum，表示静态 Topic 的逻辑队列总数。
	TotalQueueNums int
	// MapFile 是官方 -mf/--mapFile；RocketMQ 5.3.2 声明该选项但执行分支检查 f，因此实际不会走文件导入。
	MapFile string
	// ForceReplace 是官方 -fr/--forceReplace，会写入 CreateTopicRequestHeader.force。
	ForceReplace bool
}

type updateStaticTopicResult struct {
	// BeforeFile 是官方 writeToTemp(..., false) 输出的旧 mapping 临时文件路径。
	BeforeFile string
	// AfterFile 是官方 writeToTemp(..., true) 输出的新 mapping 临时文件路径。
	AfterFile string
}

type remappingStaticTopicOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，用于读取集群与旧静态 Topic mapping。
	NameServer string
	// BrokerNames 是官方 -b/--brokers，表示 remap 后逻辑队列允许分布到的 Broker 名称。
	BrokerNames []string
	// ClusterNames 是官方 -c/--clusters，会展开为目标集群内所有 Broker 名称。
	ClusterNames []string
	// Topic 是官方 -t/--topic，表示要重新映射的静态 Topic。
	Topic string
	// MapFile 是官方 -mf/--mapFile；RocketMQ 5.3.2 声明该选项但执行分支检查 f，因此实际不会走文件导入。
	MapFile string
}

type remappingStaticTopicResult struct {
	// BeforeFile 是旧 mapping wrapper 临时文件路径，官方 stdout 第一行打印。
	BeforeFile string
	// AfterFile 是 remapping 后 wrapper 临时文件路径，官方 stdout 第二行仍误写为 old mapping。
	AfterFile string
}

type staticTopicConfigAndQueueMapping struct {
	// TopicName 对应 TopicConfig.topicName。
	TopicName string `json:"topicName"`
	// ReadQueueNums 对应 TopicConfig.readQueueNums。
	ReadQueueNums int `json:"readQueueNums"`
	// WriteQueueNums 对应 TopicConfig.writeQueueNums。
	WriteQueueNums int `json:"writeQueueNums"`
	// Perm 对应 TopicConfig.perm，官方默认 6。
	Perm int `json:"perm"`
	// TopicFilterType 对应 TopicConfig.topicFilterType，官方默认 SINGLE_TAG。
	TopicFilterType string `json:"topicFilterType"`
	// TopicSysFlag 对应 TopicConfig.topicSysFlag，官方默认 0。
	TopicSysFlag int `json:"topicSysFlag"`
	// Order 对应 TopicConfig.order，官方默认 false。
	Order bool `json:"order"`
	// Attributes 对应 TopicConfig.attributes，官方默认空 map。
	Attributes map[string]string `json:"attributes"`
	// MappingDetail 对应 TopicConfigAndQueueMapping.mappingDetail。
	MappingDetail *staticTopicQueueMappingDetail `json:"mappingDetail"`
}

type staticTopicQueueMappingDetail struct {
	// HostedQueues 对应 TopicQueueMappingDetail.hostedQueues，key 是逻辑队列 id。
	HostedQueues map[string][]staticLogicQueueMappingItem `json:"hostedQueues"`
	// Scope 对应 TopicQueueMappingInfo.scope，官方默认 __global__。
	Scope string `json:"scope"`
	// Topic 对应 TopicQueueMappingInfo.topic。
	Topic string `json:"topic"`
	// TotalQueues 对应 TopicQueueMappingInfo.totalQueues。
	TotalQueues int `json:"totalQueues"`
	// BName 对应 TopicQueueMappingInfo.bname，即承载该 mapping 的 Broker 名称。
	BName string `json:"bname"`
	// Epoch 对应 TopicQueueMappingInfo.epoch。
	Epoch int64 `json:"epoch"`
	// Dirty 对应 TopicQueueMappingInfo.dirty，创建路径固定 false。
	Dirty bool `json:"dirty"`
	// CurrIDMap 对应 TopicQueueMappingInfo.currIdMap，Broker 可由 hostedQueues 重建。
	CurrIDMap map[string]int `json:"currIdMap"`
}

type staticLogicQueueMappingItem struct {
	// Gen 对应 LogicQueueMappingItem.gen，初始创建固定 0。
	Gen int `json:"gen"`
	// QueueID 对应 LogicQueueMappingItem.queueId，表示物理队列 id。
	QueueID int `json:"queueId"`
	// BName 对应 LogicQueueMappingItem.bname，表示当前逻辑队列所在 Broker。
	BName string `json:"bname"`
	// LogicOffset 对应 LogicQueueMappingItem.logicOffset，初始创建固定 0。
	LogicOffset int64 `json:"logicOffset"`
	// StartOffset 对应 LogicQueueMappingItem.startOffset，初始创建固定 0。
	StartOffset int64 `json:"startOffset"`
	// EndOffset 对应 LogicQueueMappingItem.endOffset，未结束时为 -1。
	EndOffset int64 `json:"endOffset"`
	// TimeOfStart 对应 LogicQueueMappingItem.timeOfStart，初始创建为 -1。
	TimeOfStart int64 `json:"timeOfStart"`
	// TimeOfEnd 对应 LogicQueueMappingItem.timeOfEnd，未结束时为 -1。
	TimeOfEnd int64 `json:"timeOfEnd"`
}

type staticTopicRemappingDetailWrapper struct {
	// Topic 对应 TopicRemappingDetailWrapper.topic。
	Topic string `json:"topic"`
	// Type 对应 TopicRemappingDetailWrapper.type，创建和扩容路径固定 CREATE_OR_UPDATE。
	Type string `json:"type"`
	// Epoch 对应 TopicRemappingDetailWrapper.epoch，同时参与临时文件名。
	Epoch int64 `json:"epoch"`
	// BrokerConfigMap 对应 TopicRemappingDetailWrapper.brokerConfigMap。
	BrokerConfigMap map[string]staticTopicConfigAndQueueMapping `json:"brokerConfigMap"`
	// BrokerToMapIn 对应 TopicRemappingDetailWrapper.brokerToMapIn，创建路径为空数组。
	BrokerToMapIn []string `json:"brokerToMapIn"`
	// BrokerToMapOut 对应 TopicRemappingDetailWrapper.brokerToMapOut，创建路径为空数组。
	BrokerToMapOut []string `json:"brokerToMapOut"`
}

type updateTopicPermOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，用于读取 TopicRoute 与 cluster master 地址。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 更新 Topic 权限。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量更新 Topic 权限。
	ClusterName string
	// Topic 是官方 -t/--topic，表示要修改权限的 Topic 名称。
	Topic string
	// Perm 是官方 -p/--perm，取值为 2、4 或 6。
	Perm int
}

type updateTopicPermRow struct {
	// OldPerm 是 TopicRoute 中读取到的更新前权限数值。
	OldPerm int
	// NewPerm 是用户通过 -p/--perm 指定的新权限数值。
	NewPerm int
	// BrokerAddr 是实际执行 UPDATE_AND_CREATE_TOPIC 请求的 Broker 地址。
	BrokerAddr string
}

type updateTopicPermResult struct {
	// Rows 是官方逐个 Broker 打印 update topic perm success 的数据。
	Rows []updateTopicPermRow
	// Config 是 broker 模式下官方追加打印的 TopicConfig.toString() 数据。
	Config updateTopicConfig
	// PrintConfig 表示是否按官方 broker 模式在 success 行后追加 TopicConfig 并带句点。
	PrintConfig bool
	// SamePerm 表示 broker 模式下新旧权限相同，官方只打印 noop 文本且不发送更新请求。
	SamePerm bool
	// BrokerNotMaster 表示 broker 模式下传入地址不在 TopicRoute master 地址中，官方打印错误文本但进程仍成功退出。
	BrokerNotMaster bool
}

type setConsumeModeOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 master Broker。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 设置消费模式。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量设置消费模式。
	ClusterName string
	// Topic 是官方 -t/--topicName，表示要设置消费模式的 Topic 名称。
	Topic string
	// GroupName 是官方 -g/--groupName，表示要设置消费模式的消费者组名称。
	GroupName string
	// Mode 是官方 -m/--mode，合法值由 RocketMQ MessageRequestMode 定义为 PULL 或 POP。
	Mode string
	// PopShareQueueNum 是官方 -q/--popShareQueueNum，表示 POP 模式共享队列数量。
	PopShareQueueNum int
}

type setConsumeModeResult struct {
	// Targets 是本次成功发送 SET_MESSAGE_REQUEST_MODE 的 Broker 地址列表。
	Targets []string
	// Topic 是最终输出中的 topic 字段。
	Topic string
	// GroupName 是最终输出中的 group 字段。
	GroupName string
	// Mode 是最终输出中的 consume mode 字段。
	Mode string
	// PopShareQueueNum 是最终输出中的 popShareQueueNum 字段。
	PopShareQueueNum int
}

type setMessageRequestModeBody struct {
	// Topic 是 SetMessageRequestModeRequestBody.topic。
	Topic string `json:"topic"`
	// ConsumerGroup 是 SetMessageRequestModeRequestBody.consumerGroup。
	ConsumerGroup string `json:"consumerGroup"`
	// Mode 是 SetMessageRequestModeRequestBody.mode，官方枚举值为 PULL 或 POP。
	Mode string `json:"mode"`
	// PopShareQueueNum 是 SetMessageRequestModeRequestBody.popShareQueueNum。
	PopShareQueueNum int `json:"popShareQueueNum"`
}

type coldDataFlowCtrGroupConfigOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 master Broker。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 写入冷读流控阈值。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量写入冷读流控阈值。
	ClusterName string
	// ConsumerGroup 是官方 -g/--consumerGroup，作为 Properties key 写入 Broker。
	ConsumerGroup string
	// Threshold 是官方 -v/--threshold，作为 Properties value 写入 Broker。
	Threshold string
}

type removeColdDataFlowCtrGroupConfigOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 master Broker。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 删除冷读流控阈值。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量删除冷读流控阈值。
	ClusterName string
	// ConsumerGroup 是官方 -g/--consumerGroup，作为 Broker 删除请求的 body。
	ConsumerGroup string
}

type cleanExpiredCQOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 Broker 地址。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 清理过期 ConsumeQueue。
	BrokerAddr string
	// ClusterName 是官方 -c/--cluster，指定按集群全部 Broker 地址清理；为空时复刻官方遍历全部集群。
	ClusterName string
}

type cleanUnusedTopicOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 Broker 地址。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 清理未使用 Topic。
	BrokerAddr string
	// ClusterName 是官方 -c/--cluster，指定按集群全部 Broker 地址清理；为空时复刻官方遍历全部集群。
	ClusterName string
}

type deleteExpiredCommitLogOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 Broker 地址。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单个 Broker 删除过期 CommitLog 文件。
	BrokerAddr string
	// ClusterName 是官方 -c/--cluster，指定按集群全部 Broker 地址清理；为空时复刻官方遍历全部集群。
	ClusterName string
}

const defaultGroupRetryPolicyJSON = `{"type":"CUSTOMIZED","exponentialRetryPolicy":null,"customizedRetryPolicy":null}`

// updateTopicListUsage 复刻官方 cluster 分支成功后继续落入 printCommandLineHelp 的输出。
const updateTopicListUsage = "usage: mqadmin updateTopicList -b <arg> | -c <arg>  -f <arg> [-h] [-n <arg>]\n" +
	" -b,--brokerAddr <arg>    create topic to which broker\n" +
	" -c,--clusterName <arg>   create topic to which cluster\n" +
	" -f,--filename <arg>      Path to a file with list of org.apache.rocketmq.common.TopicConfig in json format\n" +
	" -h,--help                Print help\n" +
	" -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'\n"

// updateSubGroupListUsage 复刻官方 cluster 分支成功后继续落入 printCommandLineHelp 的输出。
const updateSubGroupListUsage = "usage: mqadmin updateSubGroupList -b <arg> | -c <arg>  -f <arg> [-h] [-n <arg>]\n" +
	" -b,--brokerAddr <arg>    create groups to which broker\n" +
	" -c,--clusterName <arg>   create groups to which cluster\n" +
	" -f,--filename <arg>      Path to a file with a list of\n" +
	"                          org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig in json\n" +
	"                          format\n" +
	" -h,--help                Print help\n" +
	" -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'\n"

type updateSubGroupOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要使用。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 创建或更新订阅组。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量创建或更新订阅组。
	ClusterName string
	// GroupName 是官方 -g/--groupName，表示订阅组名称。
	GroupName string
	// ConsumeEnable 是官方 -s/--consumeEnable，默认 true。
	ConsumeEnable bool
	// ConsumeFromMinEnable 是官方 -m/--consumeFromMinEnable，命令默认 false。
	ConsumeFromMinEnable bool
	// ConsumeBroadcastEnable 是官方 -d/--consumeBroadcastEnable，命令默认 false。
	ConsumeBroadcastEnable bool
	// ConsumeMessageOrderly 是官方 -o/--consumeMessageOrderly，默认 false。
	ConsumeMessageOrderly bool
	// RetryQueueNums 是官方 -q/--retryQueueNums，默认 1。
	RetryQueueNums int
	// RetryMaxTimes 是官方 -r/--retryMaxTimes，默认 16。
	RetryMaxTimes int
	// GroupRetryPolicy 是官方 -p/--groupRetryPolicy 的 JSON 文本。
	GroupRetryPolicy string
	// BrokerID 是官方 -i/--brokerId，默认 master broker id 0。
	BrokerID int64
	// WhichBrokerWhenConsumeSlowly 是官方 -w/--whichBrokerWhenConsumeSlowly，默认 1。
	WhichBrokerWhenConsumeSlowly int64
	// NotifyConsumerIdsChanged 是官方 -a/--notifyConsumerIdsChanged，默认 true。
	NotifyConsumerIdsChanged bool
	// Attributes 是官方 --attributes 原始表达式，例如 +a=b,-c。
	Attributes string
}

type updateSubGroupListOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要通过它发现 master Broker。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 批量创建或更新订阅组。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量创建或更新订阅组。
	ClusterName string
	// FileName 是官方 -f/--filename，指向 SubscriptionGroupConfig JSON 数组文件。
	FileName string
	// GroupConfigs 是从 FileName 解出的订阅组配置列表，对应官方 JSON.parseArray(SubscriptionGroupConfig.class)。
	GroupConfigs []subscriptionGroupConfig
}

type subscriptionGroupConfig struct {
	// GroupName 是 SubscriptionGroupConfig.groupName。
	GroupName string
	// ConsumeEnable 是 SubscriptionGroupConfig.consumeEnable。
	ConsumeEnable bool
	// ConsumeFromMinEnable 是 SubscriptionGroupConfig.consumeFromMinEnable。
	ConsumeFromMinEnable bool
	// ConsumeBroadcastEnable 是 SubscriptionGroupConfig.consumeBroadcastEnable。
	ConsumeBroadcastEnable bool
	// ConsumeMessageOrderly 是 SubscriptionGroupConfig.consumeMessageOrderly。
	ConsumeMessageOrderly bool
	// RetryQueueNums 是 SubscriptionGroupConfig.retryQueueNums。
	RetryQueueNums int
	// RetryMaxTimes 是 SubscriptionGroupConfig.retryMaxTimes。
	RetryMaxTimes int
	// GroupRetryPolicy 是 SubscriptionGroupConfig.groupRetryPolicy 的 JSON 表示。
	GroupRetryPolicy string
	// BrokerID 是 SubscriptionGroupConfig.brokerId。
	BrokerID int64
	// WhichBrokerWhenConsumeSlowly 是 SubscriptionGroupConfig.whichBrokerWhenConsumeSlowly。
	WhichBrokerWhenConsumeSlowly int64
	// NotifyConsumerIdsChanged 是 SubscriptionGroupConfig.notifyConsumerIdsChangedEnable。
	NotifyConsumerIdsChanged bool
	// GroupSysFlag 是 SubscriptionGroupConfig.groupSysFlag，当前官方 CLI 不提供参数，固定为 0。
	GroupSysFlag int
	// ConsumeTimeoutMinute 是 SubscriptionGroupConfig.consumeTimeoutMinute，官方默认 15。
	ConsumeTimeoutMinute int
	// Attributes 是 SubscriptionGroupConfig.attributes。
	Attributes map[string]string
}

type subscriptionGroupConfigFile struct {
	// GroupName 是 JSON 文件中的 groupName。
	GroupName string `json:"groupName"`
	// ConsumeEnable 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 true。
	ConsumeEnable *bool `json:"consumeEnable"`
	// ConsumeFromMinEnable 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 true。
	ConsumeFromMinEnable *bool `json:"consumeFromMinEnable"`
	// ConsumeBroadcastEnable 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 true。
	ConsumeBroadcastEnable *bool `json:"consumeBroadcastEnable"`
	// ConsumeMessageOrderly 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 false。
	ConsumeMessageOrderly *bool `json:"consumeMessageOrderly"`
	// RetryQueueNums 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 1。
	RetryQueueNums *int `json:"retryQueueNums"`
	// RetryMaxTimes 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 16。
	RetryMaxTimes *int `json:"retryMaxTimes"`
	// GroupRetryPolicy 保留官方 JSON 对象文本，写入 Broker 时原样放回 groupRetryPolicy 字段。
	GroupRetryPolicy json.RawMessage `json:"groupRetryPolicy"`
	// BrokerID 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 0。
	BrokerID *int64 `json:"brokerId"`
	// WhichBrokerWhenConsumeSlowly 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 1。
	WhichBrokerWhenConsumeSlowly *int64 `json:"whichBrokerWhenConsumeSlowly"`
	// NotifyConsumerIdsChanged 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 true。
	NotifyConsumerIdsChanged *bool `json:"notifyConsumerIdsChangedEnable"`
	// GroupSysFlag 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 0。
	GroupSysFlag *int `json:"groupSysFlag"`
	// ConsumeTimeoutMinute 为 nil 时保留 Java SubscriptionGroupConfig 构造默认值 15。
	ConsumeTimeoutMinute *int `json:"consumeTimeoutMinute"`
	// Attributes 是 JSON 文件中的 attributes，nil 时按官方默认空 Map 写入。
	Attributes map[string]string `json:"attributes"`
}

type subscriptionGroupListRequestBody struct {
	// GroupConfigList 对应官方 SubscriptionGroupList.groupConfigList。
	GroupConfigList []json.RawMessage `json:"groupConfigList"`
}

type updateSubGroupResult struct {
	// Targets 是官方逐行打印 create subscription group success 的目标 Broker 地址。
	Targets []string
	// Config 是官方最后打印的 SubscriptionGroupConfig.toString() 数据。
	Config subscriptionGroupConfig
}

type deleteSubGroupOptions struct {
	// NameServer 是官方 -n/--namesrvAddr，cluster 模式需要使用。
	NameServer string
	// BrokerAddr 是官方 -b/--brokerAddr，指定单 Broker 删除订阅组。
	BrokerAddr string
	// ClusterName 是官方 -c/--clusterName，指定按集群 master Broker 批量删除订阅组。
	ClusterName string
	// GroupName 是官方 -g/--groupName，表示订阅组名称。
	GroupName string
	// RemoveOffset 是官方 -r/--removeOffset，传给 cleanOffset header。
	RemoveOffset bool
}

type deleteSubGroupResult struct {
	// BrokerAddr 是实际执行删除的 Broker 地址。
	BrokerAddr string
	// ClusterName 非空时输出官方 cluster 模式 success 文本。
	ClusterName string
}

// TopicConfig 把 CLI options 转为官方 TopicConfig 等价结构。
func (options updateTopicOptions) TopicConfig() updateTopicConfig {
	filterType := strings.TrimSpace(options.TopicFilterType)
	if filterType == "" {
		filterType = "SINGLE_TAG"
	}
	return updateTopicConfig{
		TopicName:       strings.TrimSpace(options.Topic),
		ReadQueueNums:   options.ReadQueueNums,
		WriteQueueNums:  options.WriteQueueNums,
		Perm:            options.Perm,
		TopicFilterType: filterType,
		TopicSysFlag:    options.TopicSysFlag,
		Order:           options.Order,
		Attributes:      parseTopicAttributes(options.Attributes),
	}
}

func (config updateTopicConfig) requestFields() map[string]string {
	return map[string]string{
		"topic":           config.TopicName,
		"defaultTopic":    "TBW102",
		"readQueueNums":   strconv.Itoa(config.ReadQueueNums),
		"writeQueueNums":  strconv.Itoa(config.WriteQueueNums),
		"perm":            strconv.Itoa(config.Perm),
		"topicFilterType": config.TopicFilterType,
		"topicSysFlag":    strconv.Itoa(config.TopicSysFlag),
		"order":           strconv.FormatBool(config.Order),
		"attributes":      formatTopicAttributesHeader(config.Attributes),
	}
}

func normalizeUpdateTopicConfigs(configs []updateTopicConfig) []updateTopicConfig {
	normalized := make([]updateTopicConfig, 0, len(configs))
	for _, config := range configs {
		config.TopicName = strings.TrimSpace(config.TopicName)
		config.TopicFilterType = strings.TrimSpace(config.TopicFilterType)
		if config.Attributes == nil {
			config.Attributes = map[string]string{}
		}
		normalized = append(normalized, config)
	}
	return normalized
}

func readUpdateTopicConfigsFile(fileName string) ([]updateTopicConfig, error) {
	content, err := os.ReadFile(strings.TrimSpace(fileName))
	if err != nil {
		return nil, err
	}
	var configs []updateTopicConfig
	if err := json.Unmarshal(content, &configs); err != nil {
		return nil, err
	}
	return normalizeUpdateTopicConfigs(configs), nil
}

func boolValue(value *bool, defaultValue bool) bool {
	if value == nil {
		return defaultValue
	}
	return *value
}

func intValue(value *int, defaultValue int) int {
	if value == nil {
		return defaultValue
	}
	return *value
}

func int64Value(value *int64, defaultValue int64) int64 {
	if value == nil {
		return defaultValue
	}
	return *value
}

func (config subscriptionGroupConfigFile) subscriptionGroupConfig() subscriptionGroupConfig {
	policy := strings.TrimSpace(string(config.GroupRetryPolicy))
	if policy == "" || policy == "null" {
		policy = defaultGroupRetryPolicyJSON
	}
	attributes := config.Attributes
	if attributes == nil {
		attributes = map[string]string{}
	}
	return subscriptionGroupConfig{
		GroupName:                    strings.TrimSpace(config.GroupName),
		ConsumeEnable:                boolValue(config.ConsumeEnable, true),
		ConsumeFromMinEnable:         boolValue(config.ConsumeFromMinEnable, true),
		ConsumeBroadcastEnable:       boolValue(config.ConsumeBroadcastEnable, true),
		ConsumeMessageOrderly:        boolValue(config.ConsumeMessageOrderly, false),
		RetryQueueNums:               intValue(config.RetryQueueNums, 1),
		RetryMaxTimes:                intValue(config.RetryMaxTimes, 16),
		GroupRetryPolicy:             policy,
		BrokerID:                     int64Value(config.BrokerID, 0),
		WhichBrokerWhenConsumeSlowly: int64Value(config.WhichBrokerWhenConsumeSlowly, 1),
		NotifyConsumerIdsChanged:     boolValue(config.NotifyConsumerIdsChanged, true),
		GroupSysFlag:                 intValue(config.GroupSysFlag, 0),
		ConsumeTimeoutMinute:         intValue(config.ConsumeTimeoutMinute, 15),
		Attributes:                   attributes,
	}
}

func normalizeSubscriptionGroupConfigs(configs []subscriptionGroupConfig) []subscriptionGroupConfig {
	normalized := make([]subscriptionGroupConfig, 0, len(configs))
	for _, config := range configs {
		config.GroupName = strings.TrimSpace(config.GroupName)
		config.GroupRetryPolicy = strings.TrimSpace(config.GroupRetryPolicy)
		if config.GroupRetryPolicy == "" {
			config.GroupRetryPolicy = defaultGroupRetryPolicyJSON
		}
		if config.Attributes == nil {
			config.Attributes = map[string]string{}
		}
		normalized = append(normalized, config)
	}
	return normalized
}

func readSubscriptionGroupConfigsFile(fileName string) ([]subscriptionGroupConfig, error) {
	content, err := os.ReadFile(strings.TrimSpace(fileName))
	if err != nil {
		return nil, err
	}
	var fileConfigs []subscriptionGroupConfigFile
	if err := json.Unmarshal(content, &fileConfigs); err != nil {
		return nil, err
	}
	configs := make([]subscriptionGroupConfig, 0, len(fileConfigs))
	for _, fileConfig := range fileConfigs {
		configs = append(configs, fileConfig.subscriptionGroupConfig())
	}
	return normalizeSubscriptionGroupConfigs(configs), nil
}

// SubscriptionGroupConfig 把 CLI options 转为官方 SubscriptionGroupConfig 等价结构。
func (options updateSubGroupOptions) SubscriptionGroupConfig() subscriptionGroupConfig {
	policy := strings.TrimSpace(options.GroupRetryPolicy)
	if policy == "" {
		policy = defaultGroupRetryPolicyJSON
	}
	return subscriptionGroupConfig{
		GroupName:                    strings.TrimSpace(options.GroupName),
		ConsumeEnable:                options.ConsumeEnable,
		ConsumeFromMinEnable:         options.ConsumeFromMinEnable,
		ConsumeBroadcastEnable:       options.ConsumeBroadcastEnable,
		ConsumeMessageOrderly:        options.ConsumeMessageOrderly,
		RetryQueueNums:               options.RetryQueueNums,
		RetryMaxTimes:                options.RetryMaxTimes,
		GroupRetryPolicy:             policy,
		BrokerID:                     options.BrokerID,
		WhichBrokerWhenConsumeSlowly: options.WhichBrokerWhenConsumeSlowly,
		NotifyConsumerIdsChanged:     options.NotifyConsumerIdsChanged,
		GroupSysFlag:                 0,
		ConsumeTimeoutMinute:         15,
		Attributes:                   parseTopicAttributes(options.Attributes),
	}
}

func (config subscriptionGroupConfig) requestBody() ([]byte, error) {
	var groupRetryPolicy any
	if err := json.Unmarshal([]byte(config.GroupRetryPolicy), &groupRetryPolicy); err != nil {
		return nil, err
	}
	payload := struct {
		GroupName                    string            `json:"groupName"`
		ConsumeEnable                bool              `json:"consumeEnable"`
		ConsumeFromMinEnable         bool              `json:"consumeFromMinEnable"`
		ConsumeBroadcastEnable       bool              `json:"consumeBroadcastEnable"`
		ConsumeMessageOrderly        bool              `json:"consumeMessageOrderly"`
		RetryQueueNums               int               `json:"retryQueueNums"`
		RetryMaxTimes                int               `json:"retryMaxTimes"`
		GroupRetryPolicy             any               `json:"groupRetryPolicy"`
		BrokerID                     int64             `json:"brokerId"`
		WhichBrokerWhenConsumeSlowly int64             `json:"whichBrokerWhenConsumeSlowly"`
		NotifyConsumerIdsChanged     bool              `json:"notifyConsumerIdsChangedEnable"`
		GroupSysFlag                 int               `json:"groupSysFlag"`
		ConsumeTimeoutMinute         int               `json:"consumeTimeoutMinute"`
		SubscriptionDataSet          any               `json:"subscriptionDataSet"`
		Attributes                   map[string]string `json:"attributes"`
	}{
		GroupName:                    config.GroupName,
		ConsumeEnable:                config.ConsumeEnable,
		ConsumeFromMinEnable:         config.ConsumeFromMinEnable,
		ConsumeBroadcastEnable:       config.ConsumeBroadcastEnable,
		ConsumeMessageOrderly:        config.ConsumeMessageOrderly,
		RetryQueueNums:               config.RetryQueueNums,
		RetryMaxTimes:                config.RetryMaxTimes,
		GroupRetryPolicy:             groupRetryPolicy,
		BrokerID:                     config.BrokerID,
		WhichBrokerWhenConsumeSlowly: config.WhichBrokerWhenConsumeSlowly,
		NotifyConsumerIdsChanged:     config.NotifyConsumerIdsChanged,
		GroupSysFlag:                 config.GroupSysFlag,
		ConsumeTimeoutMinute:         config.ConsumeTimeoutMinute,
		SubscriptionDataSet:          nil,
		Attributes:                   config.Attributes,
	}
	return json.Marshal(payload)
}

type queryConsumeQueueResult struct {
	// SubscriptionData 是 Broker 返回的订阅数据原始 JSON，官方 queryCq 在非空时先打印。
	SubscriptionData string
	// FilterData 是 Broker 返回的过滤说明，例如消费者组离线提示。
	FilterData string
	// QueueData 是从 ConsumeQueue 读取到的明细数据，输出时按响应顺序递增 idx。
	QueueData []consumeQueueData
	// MaxQueueIndex 是当前 ConsumeQueue 最大 index。
	MaxQueueIndex int64
	// MinQueueIndex 是当前 ConsumeQueue 最小 index。
	MinQueueIndex int64
}

type consumeQueueData struct {
	// PhysicOffset 是 ConsumeQueue 条目指向的 commitlog 物理偏移。
	PhysicOffset int64
	// PhysicSize 是消息物理大小。
	PhysicSize int
	// TagsCode 是 ConsumeQueue 保存的 tag hash 或扩展地址。
	TagsCode int64
	// ExtendDataJSON 是扩展数据 JSON；为 null 时按官方 toString 输出字符串 null。
	ExtendDataJSON string
	// BitMap 是过滤 bitmap；为 null 时按官方 toString 输出字符串 null。
	BitMap string
	// Eval 表示 Broker 是否命中过滤表达式。
	Eval bool
	// Msg 是可选消息内容；为 null 时按官方 toString 输出字符串 null。
	Msg string
}

type messageQueueIdentity struct {
	// Topic 是 MessageQueue.topic。
	Topic string
	// BrokerName 是 MessageQueue.brokerName。
	BrokerName string
	// QueueID 是 MessageQueue.queueId。
	QueueID int
}

type consumerProgressSummaryRow struct {
	// Group 是消费者组名，由官方从 %RETRY% Topic 反解得到。
	Group string
	// Count 是当前在线连接数。
	Count int
	// Version 是在线客户端最低版本；离线时为 OFFLINE。
	Version string
	// ConsumeType 是 PUSH/PULL；离线时为空。
	ConsumeType string
	// MessageModel 是 CLUSTERING/BROADCASTING；离线或 PULL 时为空。
	MessageModel string
	// ConsumeTPS 是官方将 ConsumeStats.consumeTps 转 int 后展示的值。
	ConsumeTPS int
	// DiffTotal 是消费堆积总量。
	DiffTotal int64
}

type consumerConnectionSummary struct {
	// Count 是当前在线连接数。
	Count int
	// Version 是在线客户端最低版本；离线时为 OFFLINE。
	Version string
	// ConsumeType 是官方 summary 表格展示的 PULL 或 PUSH。
	ConsumeType string
	// MessageModel 是官方 summary 表格展示的 CLUSTERING 或 BROADCASTING。
	MessageModel string
	// ClientIDs 是在线客户端的 clientId 列表，用于 showClientIP 逐个查询运行信息。
	ClientIDs []string
}

type consumerConnectionDetail struct {
	// Connections 是官方 consumerConnection 输出的在线连接表。
	Connections []consumerConnectionEntry
	// Subscriptions 是官方 consumerConnection 输出的订阅表。
	Subscriptions []consumerSubscriptionEntry
	// ConsumeType 是官方输出里的 CONSUME_PASSIVELY/CONSUME_ACTIVELY。
	ConsumeType string
	// MessageModel 是官方输出里的 CLUSTERING/BROADCASTING。
	MessageModel string
	// ConsumeFromWhere 是官方输出里的消费起点。
	ConsumeFromWhere string
}

type consumerConnectionEntry struct {
	// ClientID 是消费者客户端 ID。
	ClientID string
	// ClientAddr 是消费者客户端地址。
	ClientAddr string
	// Language 是客户端语言。
	Language string
	// Version 是客户端版本号。
	Version int
}

type producerConnectionDetail struct {
	// Connections 是官方 producerConnection 输出的在线生产者连接集合。
	Connections []producerConnectionEntry
}

type producerConnectionEntry struct {
	// ClientID 是生产者客户端 ID。
	ClientID string
	// ClientAddr 是生产者客户端地址。
	ClientAddr string
	// Language 是客户端语言。
	Language string
	// Version 是客户端版本号。
	Version int
}

type producerTableInfo struct {
	// Groups 是 Broker 返回的生产者组列表，输出时按官方 Java HashMap keySet 顺序遍历。
	Groups []producerGroupInfo
}

type producerGroupInfo struct {
	// Group 是生产者组名称，对应官方 producer group (...)。
	Group string
	// Producers 是该组下的在线生产者实例列表。
	Producers []producerInfo
}

type producerInfo struct {
	// ClientID 是生产者客户端 ID。
	ClientID string
	// RemoteIP 是 Broker 记录的远端地址，官方 toString 保留形如 /ip:port 的文本。
	RemoteIP string
	// Language 是客户端语言枚举名称。
	Language string
	// Version 是客户端协议版本号。
	Version int
	// LastUpdateTimestamp 是 Broker producer table 中该实例最后更新时间戳。
	LastUpdateTimestamp int64
}

type coldDataFlowCtrInfoSection struct {
	// Header 是官方 getColdDataFlowCtrInfo 在 JSON 前打印的分隔标题，不包含最前导空格和结尾换行。
	Header string
	// BrokerAddr 是实际查询的 Broker remoting 地址，用于空 body 时打印 Broker[...] 提示。
	BrokerAddr string
	// Body 是 Broker 通过 GET_COLD_DATA_FLOW_CTR_INFO 返回的原始 JSON 字符串。
	Body string
}

type consumerSubscriptionEntry struct {
	// Topic 是订阅 Topic。
	Topic string
	// Expression 是订阅表达式。
	Expression string
}

type consumerRunningInfo struct {
	// Properties 保留 ConsumerRunningInfo.properties，并按 Java Properties 枚举顺序输出。
	Properties []orderedStringValue
	// Subscriptions 对应官方 ConsumerRunningInfo.subscriptionSet，输出前按 Topic@SubString 排序。
	Subscriptions []consumerRunningSubscription
	// MQTable 对应官方 mqTable，输出消费位点和 ProcessQueueInfo。
	MQTable []consumerRunningProcessQueue
	// MQPopTable 对应官方 mqPopTable，输出 Pop 消费队列明细。
	MQPopTable []consumerRunningPopQueue
	// StatusTable 对应官方 statusTable，按 Topic 字典序输出 RT/TPS。
	StatusTable []consumerRunningStatus
	// UserConsumerInfo 对应官方 userConsumerInfo，按 key 字典序输出。
	UserConsumerInfo []orderedStringValue
	// Jstack 是 -s/--jstack 时 Consumer 侧返回的线程栈。
	Jstack string
}

type orderedStringValue struct {
	// Key 是 JSON 对象中的原始字段名。
	Key string
	// Value 是按 Java Object.toString 语义转换后的展示值。
	Value string
}

type consumerRunningSubscription struct {
	// Topic 是订阅 Topic。
	Topic string
	// SubString 是订阅表达式。
	SubString string
	// ClassFilterMode 表示是否启用 class filter。
	ClassFilterMode bool
}

type consumerRunningProcessQueue struct {
	// Queue 是 ProcessQueue 所属 MessageQueue。
	Queue messageQueueIdentity
	// Info 是 ProcessQueueInfo 运行时状态。
	Info processQueueInfo
}

type consumerRunningPopQueue struct {
	// Queue 是 PopProcessQueue 所属 MessageQueue。
	Queue messageQueueIdentity
	// Info 是 PopProcessQueueInfo 运行时状态。
	Info popProcessQueueInfo
}

type consumerRunningStatus struct {
	// Topic 是 statusTable 的 Topic key。
	Topic string
	// Status 是 ConsumeStatus 运行时指标。
	Status consumeStatusInfo
}

type processQueueInfo struct {
	// CommitOffset 是当前队列已提交消费位点。
	CommitOffset int64 `json:"commitOffset"`
	// CachedMsgMinOffset 是本地 ProcessQueue 缓存最小位点。
	CachedMsgMinOffset int64 `json:"cachedMsgMinOffset"`
	// CachedMsgMaxOffset 是本地 ProcessQueue 缓存最大位点。
	CachedMsgMaxOffset int64 `json:"cachedMsgMaxOffset"`
	// CachedMsgCount 是缓存消息数量。
	CachedMsgCount int `json:"cachedMsgCount"`
	// CachedMsgSizeInMiB 是缓存消息大小 MiB。
	CachedMsgSizeInMiB int `json:"cachedMsgSizeInMiB"`
	// TransactionMsgMinOffset 是事务消息缓存最小位点。
	TransactionMsgMinOffset int64 `json:"transactionMsgMinOffset"`
	// TransactionMsgMaxOffset 是事务消息缓存最大位点。
	TransactionMsgMaxOffset int64 `json:"transactionMsgMaxOffset"`
	// TransactionMsgCount 是事务消息缓存数量。
	TransactionMsgCount int `json:"transactionMsgCount"`
	// Locked 表示队列是否已加锁。
	Locked bool `json:"locked"`
	// TryUnlockTimes 是尝试解锁次数。
	TryUnlockTimes int64 `json:"tryUnlockTimes"`
	// LastLockTimestamp 是最近锁定时间戳。
	LastLockTimestamp int64 `json:"lastLockTimestamp"`
	// Droped 对应官方字段拼写 droped。
	Droped bool `json:"droped"`
	// LastPullTimestamp 是最近拉取时间戳。
	LastPullTimestamp int64 `json:"lastPullTimestamp"`
	// LastConsumeTimestamp 是最近消费时间戳。
	LastConsumeTimestamp int64 `json:"lastConsumeTimestamp"`
}

type popProcessQueueInfo struct {
	// WaitAckCount 是等待 ACK 的 Pop 消息数。
	WaitAckCount int `json:"waitAckCount"`
	// Droped 对应官方字段拼写 droped。
	Droped bool `json:"droped"`
	// LastPopTimestamp 是最近 Pop 时间戳。
	LastPopTimestamp int64 `json:"lastPopTimestamp"`
}

type consumeStatusInfo struct {
	// PullRT 是拉取 RT。
	PullRT float64 `json:"pullRT"`
	// PullTPS 是拉取 TPS。
	PullTPS float64 `json:"pullTPS"`
	// ConsumeRT 是消费 RT。
	ConsumeRT float64 `json:"consumeRT"`
	// ConsumeOKTPS 是成功消费 TPS。
	ConsumeOKTPS float64 `json:"consumeOKTPS"`
	// ConsumeFailedTPS 是失败消费 TPS。
	ConsumeFailedTPS float64 `json:"consumeFailedTPS"`
	// ConsumeFailedMsgs 是最近一小时失败消息数。
	ConsumeFailedMsgs int64 `json:"consumeFailedMsgs"`
}

// rocketMQVersionDescTable 复现 RocketMQ 5.3.2 MQVersion.Version 的 ordinal 顺序。
var rocketMQVersionDescTable = buildRocketMQVersionDescTable()

func runNativeCommand(ctx context.Context, args []string, client nativeCommandClient) (string, bool, error) {
	if len(args) == 0 {
		return "", false, nil
	}
	command := strings.ToLower(strings.TrimSpace(args[0]))
	switch command {
	case "clusterlist":
		if hasFlag(args[1:], "-i", "--interval") {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if client == nil {
			client = NewClient(0)
		}
		if hasFlag(args[1:], "-m", "--moreStats") {
			rows, err := client.ClusterListMoreStats(ctx, nameServer, clusterName)
			if err != nil {
				return "", true, err
			}
			return formatClusterListMoreStats(rows), true, nil
		}
		rows, err := client.ClusterList(ctx, nameServer, clusterName)
		if err != nil {
			return "", true, err
		}
		return formatClusterList(rows), true, nil
	case "clusteraclconfigversion":
		// RocketMQ 5.3.2 Broker 未返回官方客户端必读的 allAclFileVersion，保持 fallback 承接官方行为。
		return "", false, nil
	case "setcommitlogreadaheadmode":
		mode := strings.TrimSpace(stringArg(args[1:], "-m", "--commitLogReadAheadMode"))
		if mode == "" {
			return "", false, nil
		}
		if mode != "0" && mode != "1" {
			return "set the read mode error; 0 is default, 1 random read\n", true, nil
		}
		brokerAddr := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerAddr"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if brokerAddr == "" && clusterName == "" {
			return "", true, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if brokerAddr == "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		sections, err := client.SetCommitLogReadAheadMode(ctx, nameServer, brokerAddr, clusterName, mode)
		if err != nil {
			return "", true, err
		}
		return formatCommitLogReadAheadMode(sections), true, nil
	case "listuser":
		brokerAddr := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerAddr"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if brokerAddr == "" && clusterName == "" {
			return "", false, nil
		}
		if brokerAddr != "" && clusterName != "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		filter := strings.TrimSpace(stringArg(args[1:], "-f", "--filter"))
		if clusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.ListUser(ctx, nameServer, brokerAddr, clusterName, filter)
		if err != nil {
			return "", true, err
		}
		return formatListUser(rows, clusterName != ""), true, nil
	case "getuser":
		brokerAddr := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerAddr"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if brokerAddr == "" && clusterName == "" {
			return "", false, nil
		}
		if brokerAddr != "" && clusterName != "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		username := strings.TrimSpace(stringArg(args[1:], "-u", "--username"))
		if clusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		row, err := client.GetUser(ctx, nameServer, brokerAddr, clusterName, username)
		if err != nil {
			return "", true, err
		}
		if row == nil {
			return "", true, nil
		}
		return formatListUser([]listUserRow{*row}, false), true, nil
	case "copyuser":
		sourceBroker := strings.TrimSpace(stringArg(args[1:], "-f", "--fromBroker"))
		targetBroker := strings.TrimSpace(stringArg(args[1:], "-t", "--toBroker"))
		if sourceBroker == "" || targetBroker == "" {
			return "", false, nil
		}
		usernames := strings.TrimSpace(stringArg(args[1:], "-u", "--usernames"))
		if client == nil {
			client = NewClient(0)
		}
		results, err := client.CopyUser(ctx, sourceBroker, targetBroker, usernames)
		if err != nil {
			return "", true, err
		}
		return formatCopyUserResults(results), true, nil
	case "listacl":
		brokerAddr := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerAddr"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if brokerAddr == "" && clusterName == "" {
			return "", false, nil
		}
		if brokerAddr != "" && clusterName != "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if clusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		subjectFilter := strings.TrimSpace(stringArg(args[1:], "-s", "--subject"))
		resourceFilter := strings.TrimSpace(stringArg(args[1:], "-r", "--resource"))
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.ListAcl(ctx, nameServer, brokerAddr, clusterName, subjectFilter, resourceFilter)
		if err != nil {
			return "", true, err
		}
		return formatAclInfoTable(rows), true, nil
	case "getacl":
		brokerAddr := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerAddr"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if brokerAddr == "" && clusterName == "" {
			return "", false, nil
		}
		if brokerAddr != "" && clusterName != "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if clusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		subject := strings.TrimSpace(stringArg(args[1:], "-s", "--subject"))
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.GetAcl(ctx, nameServer, brokerAddr, clusterName, subject)
		if err != nil {
			return "", true, err
		}
		return formatAclInfoTable(rows), true, nil
	case "copyacl":
		sourceBroker := strings.TrimSpace(stringArg(args[1:], "-f", "--fromBroker"))
		targetBroker := strings.TrimSpace(stringArg(args[1:], "-t", "--toBroker"))
		if sourceBroker == "" || targetBroker == "" {
			return "", false, nil
		}
		subjects := strings.TrimSpace(stringArg(args[1:], "-s", "--subjects"))
		if client == nil {
			client = NewClient(0)
		}
		results, err := client.CopyAcl(ctx, sourceBroker, targetBroker, subjects)
		if err != nil {
			return "", true, err
		}
		return formatCopyAclResults(results), true, nil
	case "createuser":
		options := parseAuthUserOptions(args[1:])
		if options.BrokerAddr == "" && options.ClusterName == "" {
			return "", false, nil
		}
		if options.BrokerAddr != "" && options.ClusterName != "" {
			return "", false, nil
		}
		if options.Username == "" || options.Password == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.CreateUser(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatAuthUserTargets("create", targets), true, nil
	case "updateuser":
		options := parseAuthUserOptions(args[1:])
		if options.BrokerAddr == "" && options.ClusterName == "" {
			return "", false, nil
		}
		if options.BrokerAddr != "" && options.ClusterName != "" {
			return "", false, nil
		}
		if options.Username == "" || authUserUpdateFieldCount(options) != 1 {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateUser(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatAuthUserTargets("update", targets), true, nil
	case "deleteuser":
		options := parseAuthUserOptions(args[1:])
		if options.BrokerAddr == "" && options.ClusterName == "" {
			return "", false, nil
		}
		if options.BrokerAddr != "" && options.ClusterName != "" {
			return "", false, nil
		}
		if options.Username == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.DeleteUser(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatAuthUserTargets("delete", targets), true, nil
	case "createacl":
		options := parseAclOptions(args[1:])
		if !aclTargetSelected(options) || options.Subject == "" || len(options.Resources) == 0 || len(options.Actions) == 0 || options.Decision == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.CreateAcl(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatAclTargets("create", targets), true, nil
	case "updateacl":
		options := parseAclOptions(args[1:])
		if !aclTargetSelected(options) || options.Subject == "" || len(options.Resources) == 0 || len(options.Actions) == 0 || options.Decision == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateAcl(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatAclTargets("update", targets), true, nil
	case "deleteacl":
		options := parseAclOptions(args[1:])
		if !aclTargetSelected(options) || options.Subject == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.DeleteAcl(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatAclTargets("delete", targets), true, nil
	case "updateaclconfig":
		options := parseAclConfigOptions(args[1:])
		if !aclConfigTargetSelected(options) || options.AccessKey == "" || options.SecretKey == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateAclConfig(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatUpdateAclConfig(targets, options), true, nil
	case "deleteaclconfig", "deleteaccessconfig":
		options := parseAclConfigOptions(args[1:])
		if !aclConfigTargetSelected(options) || options.AccessKey == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.DeleteAclConfig(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatDeleteAclConfig(targets, options.AccessKey), true, nil
	case "updateglobalwhiteaddr":
		options := parseGlobalWhiteAddrOptions(args[1:])
		if !globalWhiteAddrTargetSelected(options) || options.GlobalWhiteRemoteAddresses == "" {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if options.ClusterName != "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateGlobalWhiteAddr(ctx, nameServer, options)
		if err != nil {
			if err.Error() == authClusterNotFoundOutput {
				return authClusterNotFoundOutput, true, nil
			}
			return "", true, err
		}
		return formatGlobalWhiteAddrTargets(targets), true, nil
	case "brokerstatus":
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if brokerAddr == "" && clusterName == "" {
			return "", true, errors.New("BrokerAddr or ClusterName 必填")
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if brokerAddr == "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if brokerAddr != "" {
			clusterName = ""
		}
		if client == nil {
			client = NewClient(0)
		}
		tables, err := client.BrokerStatus(ctx, nameServer, brokerAddr, clusterName)
		if err != nil {
			return "", true, err
		}
		return formatBrokerStatus(tables), true, nil
	case "getbrokerepoch":
		if hasFlag(args[1:], "-i", "--interval") {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		brokerName := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerName"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if nameServer == "" || (brokerName == "" && clusterName == "") {
			return "", false, nil
		}
		if client == nil {
			client = NewClient(0)
		}
		var results []brokerEpochResult
		var err error
		if brokerName != "" {
			results, err = client.GetBrokerEpoch(ctx, nameServer, brokerName)
		} else {
			results, err = client.GetBrokerEpochByCluster(ctx, nameServer, clusterName)
		}
		if err != nil {
			if brokerName == "" {
				var responseErr *rocketMQResponseError
				if errors.As(err, &responseErr) && isGetBrokerEpochControllerModeError(responseErr) {
					return "", true, &OfficialCommandResult{
						ExitCode: 0,
						Stderr:   officialGetBrokerEpochControllerModeStderr(responseErr.Remark),
					}
				}
			}
			return "", true, err
		}
		return formatBrokerEpoch(results), true, nil
	case "getbrokerconfig":
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if brokerAddr == "" && clusterName == "" {
			return "", true, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if brokerAddr == "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if brokerAddr != "" {
			clusterName = ""
		}
		if client == nil {
			client = NewClient(0)
		}
		sections, err := client.GetBrokerConfig(ctx, nameServer, brokerAddr, clusterName)
		if err != nil {
			return "", true, err
		}
		return formatBrokerConfig(sections), true, nil
	case "updatebrokerconfig":
		options := parseUpdateBrokerConfigOptions(args[1:])
		if options.Key == "" || options.Value == "" {
			return "", true, errors.New("Key、Value 必填")
		}
		if options.BrokerAddr == "" && options.ClusterName == "" {
			return "", true, nil
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateBrokerConfig(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateBrokerConfig(targets), true, nil
	case "updatenamesrvconfig":
		options := parseUpdateNamesrvConfigOptions(args[1:])
		if options.Key == "" || options.Value == "" {
			return "", true, errors.New("Key、Value 必填")
		}
		if options.NameServers == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateNamesrvConfig(ctx, options.NameServers, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateNamesrvConfig(targets, options.Key, options.Value), true, nil
	case "updatecontrollerconfig":
		options := parseUpdateControllerConfigOptions(args[1:])
		if options.Key == "" || options.Value == "" {
			return "", true, errors.New("Key、Value 必填")
		}
		if options.ControllerAddrs == "" {
			return "", true, errors.New("ControllerAddress 必填")
		}
		targets, err := client.UpdateControllerConfig(ctx, options.ControllerAddrs, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateControllerConfig(targets, options.Key, options.Value), true, nil
	case "cleanbrokermetadata":
		options, err := parseCleanBrokerMetadataOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if err := client.CleanBrokerMetadata(ctx, options.ControllerAddr, options); err != nil {
			return "", true, err
		}
		return formatCleanBrokerMetadata(options.BrokerName), true, nil
	case "electmaster":
		options, err := parseElectMasterOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		result, err := client.ElectMaster(ctx, options.ControllerAddr, options)
		if err != nil {
			return "", true, err
		}
		return formatElectMaster(result), true, nil
	case "exportconfigs":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if clusterName == "" {
			return "", true, errors.New("ClusterName 必填")
		}
		filePath := stringArg(args[1:], "-f", "--filePath")
		if filePath == "" {
			filePath = "/tmp/rocketmq/export"
		}
		if client == nil {
			client = NewClient(0)
		}
		outputPath, err := client.ExportConfigs(ctx, nameServer, clusterName, filePath)
		if err != nil {
			return "", true, err
		}
		return fmt.Sprintf("export %s success", outputPath), true, nil
	case "exportmetrics":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if clusterName == "" {
			return "", true, errors.New("ClusterName 必填")
		}
		filePath := stringArg(args[1:], "-f", "--filePath")
		if filePath == "" {
			filePath = "/tmp/rocketmq/export"
		}
		if client == nil {
			client = NewClient(0)
		}
		outputPath, err := client.ExportMetrics(ctx, nameServer, clusterName, filePath)
		if err != nil {
			return "", true, err
		}
		return fmt.Sprintf("export %s success", outputPath), true, nil
	case "exportmetadata":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		options := exportMetadataOptions{
			ClusterName:           stringArg(args[1:], "-c", "--clusterName"),
			BrokerAddr:            stringArg(args[1:], "-b", "--brokerAddr"),
			FilePath:              stringArg(args[1:], "-f", "--filePath"),
			TopicOnly:             hasFlag(args[1:], "-t", "--topic"),
			SubscriptionGroupOnly: hasFlag(args[1:], "-g", "--subscriptionGroup"),
			SpecialTopic:          hasFlag(args[1:], "-s", "--specialTopic"),
		}
		if options.FilePath == "" {
			options.FilePath = "/tmp/rocketmq/export"
		}
		if options.BrokerAddr == "" && options.ClusterName == "" {
			return "", true, nil
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.ExportMetadata(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		if result == nil || strings.TrimSpace(result.OutputPath) == "" {
			return "", true, nil
		}
		output := fmt.Sprintf("export %s success", result.OutputPath)
		if result.PrintNewline {
			output += "\n"
		}
		return output, true, nil
	case "getnamesrvconfig":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		sections, err := client.GetNamesrvConfig(ctx, nameServer)
		if err != nil {
			return "", true, err
		}
		return formatNamesrvConfig(sections), true, nil
	case "getconsumerconfig":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		groupName := stringArg(args[1:], "-g", "--groupName")
		if groupName == "" {
			return "", true, errors.New("GroupName 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		sections, err := client.GetConsumerConfig(ctx, nameServer, groupName)
		if err != nil {
			return "", true, err
		}
		return formatConsumerConfig(sections), true, nil
	case "brokerconsumestats":
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		if brokerAddr == "" {
			return "", true, errors.New("BrokerAddr 必填")
		}
		level, err := int64Arg(args[1:], 0, "-l", "--level")
		if err != nil {
			return "", true, err
		}
		timeoutMillis, err := int64Arg(args[1:], 50000, "-t", "--timeoutMillis")
		if err != nil {
			return "", true, err
		}
		isOrder := strings.EqualFold(stringArg(args[1:], "-o", "--order"), "true")
		if client == nil {
			client = NewClient(0)
		}
		stats, err := client.BrokerConsumeStats(ctx, brokerAddr, isOrder, time.Duration(timeoutMillis)*time.Millisecond)
		if err != nil {
			return "", true, err
		}
		return formatBrokerConsumeStats(stats, level), true, nil
	case "statsall":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		activeOnly := hasFlag(args[1:], "-a", "--activeTopic")
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.StatsAll(ctx, nameServer, topic, activeOnly)
		if err != nil {
			return "", true, err
		}
		return formatStatsAll(rows), true, nil
	case "hastatus":
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if _, err := intArg(args[1:], 0, "-i", "--interval"); err != nil {
			return "", true, err
		}
		if brokerAddr == "" && clusterName == "" {
			return "", true, errors.New("BrokerAddr 或 ClusterName 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if brokerAddr != "" {
			status, err := client.BrokerHAStatus(ctx, brokerAddr)
			if err != nil {
				return "", true, err
			}
			return formatHAStatus(brokerAddr, status), true, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		rows, err := client.BrokerHAStatusByCluster(ctx, nameServer, clusterName)
		if err != nil {
			return "", true, err
		}
		var builder strings.Builder
		for _, row := range rows {
			builder.WriteString(formatHAStatus(row.Addr, row.Result))
		}
		return builder.String(), true, nil
	case "checkrocksdbcqwriteprogress":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr", "--nameserverAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--cluster", "--clusterName")
		if clusterName == "" {
			return "", true, errors.New("ClusterName 必填")
		}
		checkStoreTime, err := int64Arg(args[1:], time.Now().Add(-30*24*time.Hour).UnixMilli(), "-cf", "--checkFrom")
		if err != nil {
			return "", true, err
		}
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.CheckRocksdbCqWriteProgress(ctx, nameServer, clusterName, stringArg(args[1:], "-t", "--topic"), checkStoreTime)
		if err != nil {
			return "", true, err
		}
		return formatCheckRocksdbCqWriteProgress(rows), true, nil
	case "rocksdbconfigtojson":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr", "--nameserverAddr")
		clusterName := stringArg(args[1:], "-c", "--cluster", "--clusterName")
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		configTypes, err := parseRocksDBConfigTypes(stringArg(args[1:], "-t", "--configType"))
		if err != nil {
			return "", true, err
		}
		if client == nil {
			client = NewClient(0)
		}
		if nameServer != "" && clusterName != "" {
			if err := client.RocksDBConfigToJson(ctx, nameServer, "", clusterName, configTypes); err != nil {
				return "", true, err
			}
			return "Use [rpc mode] call all brokers in cluster to export to json file \n" +
				"broker export done.", true, nil
		}
		if brokerAddr == "" {
			output, err := rocksDBConfigToJsonLocal(args[1:])
			if err != nil {
				return "", true, err
			}
			return output, true, nil
		}
		if err := client.RocksDBConfigToJson(ctx, "", brokerAddr, "", configTypes); err != nil {
			return "", true, err
		}
		return "Use [rpc mode] call broker to export to json file \n" +
			"broker export done.", true, nil
	case "exportmetadatainrocksdb":
		output, err := exportMetadataInRocksDBLocal(args[1:])
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "exportpoprecord":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if brokerAddr == "" && clusterName == "" {
			return "", true, nil
		}
		if brokerAddr == "" && nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.ExportPopRecord(ctx, nameServer, brokerAddr, clusterName, parseExportPopRecordDryRun(args[1:]))
		if err != nil {
			return "", true, err
		}
		return formatExportPopRecord(rows), true, nil
	case "updatekvconfig":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		namespace := stringArg(args[1:], "-s", "--namespace")
		key := stringArg(args[1:], "-k", "--key")
		value := stringArg(args[1:], "-v", "--value")
		if namespace == "" || key == "" || value == "" {
			return "", true, errors.New("Namespace、Key、Value 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.UpdateKvConfig(ctx, nameServer, namespace, key, value); err != nil {
			return "", true, err
		}
		return "create or update kv config to namespace success.\n", true, nil
	case "deletekvconfig":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		namespace := stringArg(args[1:], "-s", "--namespace")
		key := stringArg(args[1:], "-k", "--key")
		if namespace == "" || key == "" {
			return "", true, errors.New("Namespace、Key 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.DeleteKvConfig(ctx, nameServer, namespace, key); err != nil {
			return "", true, err
		}
		return "delete kv config from namespace success.\n", true, nil
	case "updateorderconf":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		method := strings.TrimSpace(stringArg(args[1:], "-m", "--method"))
		if topic == "" || method == "" {
			return "", true, errors.New("Topic、Method 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		switch method {
		case "get":
			orderConf, err := client.GetKvConfig(ctx, nameServer, "ORDER_TOPIC_CONFIG", topic)
			if err != nil {
				return "", true, err
			}
			return fmt.Sprintf("get orderConf success. topic=[%s], orderConf=[%s] ", topic, orderConf), true, nil
		case "put":
			orderConf := stringArg(args[1:], "-v", "--orderConf")
			if strings.TrimSpace(orderConf) == "" {
				return "", true, errors.New("please set orderConf with option -v.")
			}
			if err := client.UpdateKvConfig(ctx, nameServer, "ORDER_TOPIC_CONFIG", topic, orderConf); err != nil {
				return "", true, err
			}
			return fmt.Sprintf("update orderConf success. topic=[%s], orderConf=[%s]", topic, orderConf), true, nil
		case "delete":
			if err := client.DeleteKvConfig(ctx, nameServer, "ORDER_TOPIC_CONFIG", topic); err != nil {
				return "", true, err
			}
			return fmt.Sprintf("delete orderConf success. topic=[%s]", topic), true, nil
		default:
			return "", true, fmt.Errorf("unsupported updateOrderConf method %s", method)
		}
	case "updatetopiclist":
		options, err := parseUpdateTopicListOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.FileName != "" && len(options.TopicConfigs) == 0 {
			return "", true, nil
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateTopicList(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateTopicList(targets, options.ClusterName != ""), true, nil
	case "updatetopic":
		options, err := parseUpdateTopicOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.UpdateTopic(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateTopic(result), true, nil
	case "updatestatictopic":
		options, err := parseUpdateStaticTopicOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.UpdateStaticTopic(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateStaticTopic(result), true, nil
	case "remappingstatictopic":
		options, err := parseRemappingStaticTopicOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.RemappingStaticTopic(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatRemappingStaticTopic(result), true, nil
	case "updatetopicperm":
		options, err := parseUpdateTopicPermOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.UpdateTopicPerm(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateTopicPerm(result), true, nil
	case "setconsumemode":
		options, err := parseSetConsumeModeOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.SetConsumeMode(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatSetConsumeMode(result), true, nil
	case "updatecolddataflowctrgroupconfig":
		options, err := parseColdDataFlowCtrGroupConfigOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateColdDataFlowCtrGroupConfig(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateColdDataFlowCtrGroupConfig(targets), true, nil
	case "removecolddataflowctrgroupconfig":
		options, err := parseRemoveColdDataFlowCtrGroupConfigOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.RemoveColdDataFlowCtrGroupConfig(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatRemoveColdDataFlowCtrGroupConfig(targets), true, nil
	case "cleanexpiredcq":
		options := parseCleanExpiredCQOptions(args[1:])
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		ok, err := client.CleanExpiredCQ(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatBrokerBooleanResult(ok), true, nil
	case "cleanunusedtopic":
		options := parseCleanUnusedTopicOptions(args[1:])
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		ok, err := client.CleanUnusedTopic(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatBrokerBooleanResult(ok), true, nil
	case "deleteexpiredcommitlog":
		options := parseDeleteExpiredCommitLogOptions(args[1:])
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		ok, err := client.DeleteExpiredCommitLog(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatBrokerBooleanResult(ok), true, nil
	case "wipewriteperm":
		nameServers := stringArg(args[1:], "-n", "--namesrvAddr")
		brokerName := stringArg(args[1:], "-b", "--brokerName")
		if nameServers == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if brokerName == "" {
			return "", true, errors.New("BrokerName 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		results, err := client.WipeWritePerm(ctx, nameServers, brokerName)
		if err != nil {
			return "", true, err
		}
		return formatWipeWritePerm(brokerName, results), true, nil
	case "addwriteperm":
		nameServers := stringArg(args[1:], "-n", "--namesrvAddr")
		brokerName := stringArg(args[1:], "-b", "--brokerName")
		if nameServers == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if brokerName == "" {
			return "", true, errors.New("BrokerName 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		results, err := client.AddWritePerm(ctx, nameServers, brokerName)
		if err != nil {
			return "", true, err
		}
		return formatAddWritePerm(brokerName, results), true, nil
	case "deletetopic":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		topic := stringArg(args[1:], "-t", "--topic")
		if clusterName == "" || topic == "" {
			return "", true, errors.New("ClusterName、Topic 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.DeleteTopic(ctx, nameServer, clusterName, topic); err != nil {
			return "", true, err
		}
		return formatDeleteTopic(clusterName, topic), true, nil
	case "updatesubgroup":
		options, err := parseUpdateSubGroupOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.UpdateSubGroup(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateSubGroup(result), true, nil
	case "updatesubgrouplist":
		options, err := parseUpdateSubGroupListOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		targets, err := client.UpdateSubGroupList(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatUpdateSubGroupList(targets, options.BrokerAddr != ""), true, nil
	case "deletesubgroup":
		options, err := parseDeleteSubGroupOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerAddr == "" && options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.DeleteSubGroup(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatDeleteSubGroup(options.GroupName, rows), true, nil
	case "querycq":
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		if stringArg(args[1:], "-q", "--queue") == "" {
			return "", true, errors.New("Queue 必填")
		}
		queueID, err := intArg(args[1:], 0, "-q", "--queue")
		if err != nil {
			return "", true, err
		}
		if stringArg(args[1:], "-i", "--index") == "" {
			return "", true, errors.New("Index 必填")
		}
		index, err := int64Arg(args[1:], 0, "-i", "--index")
		if err != nil {
			return "", true, err
		}
		count, err := intArg(args[1:], 10, "-c", "--count")
		if err != nil {
			return "", true, err
		}
		brokerAddr := stringArg(args[1:], "-b", "--broker")
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if brokerAddr == "" && nameServer == "" {
			return "", true, errors.New("NameServer 或 Broker 必填")
		}
		consumerGroup := stringArg(args[1:], "-g", "--consumer")
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.QueryConsumeQueue(ctx, nameServer, brokerAddr, topic, queueID, index, count, consumerGroup)
		if err != nil {
			return "", true, err
		}
		return formatQueryConsumeQueue(result, index), true, nil
	case "topiclist":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if hasFlag(args[1:], "-c", "--clusterModel") {
			rows, err := client.TopicListCluster(ctx, nameServer)
			if err != nil {
				return "", true, err
			}
			return formatTopicListCluster(rows), true, nil
		}
		topics, err := client.TopicList(ctx, nameServer)
		if err != nil {
			return "", true, err
		}
		return formatTopicList(topics), true, nil
	case "allocatemq":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		ipList := stringArg(args[1:], "-i", "--ipList")
		if ipList == "" {
			return "", true, errors.New("ipList 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		assignments, err := client.AllocateMQ(ctx, nameServer, topic, ipList)
		if err != nil {
			return "", true, err
		}
		return formatAllocateMQ(assignments), true, nil
	case "topicroute":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		if nameServer == "" {
			return "", true, &OfficialCommandResult{ExitCode: 0, Stderr: officialTopicRouteMissingNameServerStderr()}
		}
		if client == nil {
			client = NewClient(0)
		}
		body, err := client.TopicRoute(ctx, nameServer, topic)
		if err != nil {
			return "", true, err
		}
		var output string
		if hasFlag(args[1:], "-l", "--list") {
			output, err = formatTopicRouteList(body)
		} else {
			output, err = formatTopicRoute(body)
		}
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "topicstatus":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		cluster := strings.TrimSpace(stringArg(args[1:], "-c", "--cluster"))
		var entries []topicStatusEntry
		var err error
		if cluster != "" {
			entries, err = client.TopicStatusByCluster(ctx, nameServer, topic, cluster)
		} else {
			entries, err = client.TopicStatus(ctx, nameServer, topic)
		}
		if err != nil {
			return "", true, err
		}
		return formatTopicStatus(entries), true, nil
	case "topicclusterlist":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		if nameServer == "" {
			return "", true, &OfficialCommandResult{ExitCode: 0, Stderr: officialTopicClusterListMissingNameServerStderr()}
		}
		if client == nil {
			client = NewClient(0)
		}
		clusters, err := client.TopicClusterList(ctx, nameServer, topic)
		if err != nil {
			return "", true, err
		}
		return formatTopicClusterList(clusters), true, nil
	case "consumerprogress":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		consumerGroup := stringArg(args[1:], "-g", "--groupName")
		if consumerGroup == "" {
			rows, err := client.ConsumerProgressSummary(ctx, nameServer)
			if err != nil {
				return "", true, err
			}
			return formatConsumerProgressSummary(rows), true, nil
		}
		topic := stringArg(args[1:], "-t", "--topicName")
		clusterName := stringArg(args[1:], "-c", "--cluster")
		if strings.TrimSpace(topic) == "" {
			// 官方 consumerProgress -g 无 -t 时调用 examineConsumeStats(group)，不会使用 -c。
			clusterName = ""
		}
		if strings.EqualFold(stringArg(args[1:], "-s", "--showClientIP"), "true") {
			progress, err := client.ConsumerProgressWithClientIP(ctx, nameServer, consumerGroup, topic, clusterName)
			if err != nil {
				return "", true, err
			}
			return formatConsumerProgressWithClientIP(progress), true, nil
		}
		progress, err := client.ConsumerProgress(ctx, nameServer, consumerGroup, topic, clusterName)
		if err != nil {
			return "", true, err
		}
		return formatConsumerProgress(progress), true, nil
	case "consumerconnection":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		consumerGroup := stringArg(args[1:], "-g", "--consumerGroup")
		if consumerGroup == "" {
			return "", true, errors.New("ConsumerGroup 必填")
		}
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		if client == nil {
			client = NewClient(0)
		}
		detail, err := client.ConsumerConnection(ctx, nameServer, consumerGroup, brokerAddr)
		if err != nil {
			return "", true, err
		}
		return formatConsumerConnection(detail), true, nil
	case "consumerstatus":
		clientID := stringArg(args[1:], "-i", "--clientId")
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		consumerGroup := stringArg(args[1:], "-g", "--consumerGroup")
		if consumerGroup == "" {
			return "", true, errors.New("ConsumerGroup 必填")
		}
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		if client == nil {
			client = NewClient(0)
		}
		if clientID == "" {
			output, err := client.ConsumerStatusList(ctx, nameServer, consumerGroup, brokerAddr, hasFlag(args[1:], "-s", "--jstack"))
			if err != nil {
				return "", true, err
			}
			return output, true, nil
		}
		// 官方 consumerStatus -i 只把 -b 用于前置连接检查，不传给 getConsumerRunningInfo。
		output, err := client.ConsumerStatus(ctx, nameServer, consumerGroup, clientID, "", hasFlag(args[1:], "-s", "--jstack"))
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "startmonitoring":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.StartMonitoring(ctx, nameServer); err != nil {
			return "", true, err
		}
		return "", true, nil
	case "clonegroupoffset":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if output, err := OfficialParserPreflight(args); err != nil {
			return output, true, err
		}
		srcGroup := stringArg(args[1:], "-s", "--srcGroup")
		destGroup := stringArg(args[1:], "-d", "--destGroup")
		topic := stringArg(args[1:], "-t", "--topic")
		if srcGroup == "" || destGroup == "" || topic == "" {
			return "", true, errors.New("SrcGroup、DestGroup、Topic 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.CloneGroupOffset(ctx, nameServer, srcGroup, destGroup, topic); err != nil {
			return "", true, err
		}
		return formatCloneGroupOffset(srcGroup, destGroup, topic), true, nil
	case "getcolddataflowctrinfo":
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		if client == nil {
			client = NewClient(0)
		}
		if brokerAddr != "" {
			body, err := client.GetColdDataFlowCtrInfo(ctx, brokerAddr)
			if err != nil {
				return "", true, err
			}
			output, err := formatColdDataFlowCtrInfo([]coldDataFlowCtrInfoSection{{
				Header:     fmt.Sprintf("============%s============", brokerAddr),
				BrokerAddr: brokerAddr,
				Body:       body,
			}})
			if err != nil {
				return "", true, err
			}
			return output, true, nil
		}
		clusterName := stringArg(args[1:], "-c", "--clusterName")
		if clusterName == "" {
			return "", true, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		sections, err := client.GetColdDataFlowCtrInfoByCluster(ctx, nameServer, clusterName)
		if err != nil {
			return "", true, err
		}
		output, err := formatColdDataFlowCtrInfo(sections)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "producer":
		brokerAddr := stringArg(args[1:], "-b", "--broker")
		if brokerAddr == "" {
			return "", true, errors.New("BrokerAddr 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		table, err := client.Producer(ctx, brokerAddr)
		if err != nil {
			return "", true, err
		}
		return formatProducerTableInfo(table), true, nil
	case "producerconnection":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		producerGroup := stringArg(args[1:], "-g", "--producerGroup")
		if producerGroup == "" {
			return "", true, errors.New("ProducerGroup 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		detail, err := client.ProducerConnection(ctx, nameServer, producerGroup, topic)
		if err != nil {
			return "", true, err
		}
		return formatProducerConnection(detail), true, nil
	case "sendmessage":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		options, err := parseSendMessageOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.Topic == "" || options.Body == "" {
			return "", true, errors.New("Topic、Body 必填")
		}
		if options.HasQueueID && options.BrokerName == "" {
			return "Broker name must be set if the queue is chosen!", true, nil
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.SendMessage(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatSendMessage(result), true, nil
	case "sendmsgstatus":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		options, err := parseSendMsgStatusOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerName == "" {
			return "", true, errors.New("BrokerName 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		results, err := client.SendMsgStatus(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatSendMsgStatus(results), true, nil
	case "checkmsgsendrt":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		options, err := parseCheckMsgSendRTOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.Topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.CheckMsgSendRT(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatCheckMsgSendRT(result), true, nil
	case "clusterrt":
		if hasFlag(args[1:], "-i", "--interval") {
			return "", false, nil
		}
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		options, err := parseClusterRTOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.ClusterRT(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatClusterRT(result, options), true, nil
	case "resetmasterflushoffset":
		brokerAddr := stringArg(args[1:], "-b", "--brokerAddr")
		if brokerAddr == "" {
			return "", true, errors.New("BrokerAddr 必填")
		}
		rawOffset := strings.TrimSpace(stringArg(args[1:], "-o", "--offset"))
		if rawOffset == "" {
			return "", true, errors.New("Offset 必填")
		}
		offset, err := strconv.ParseInt(rawOffset, 10, 64)
		if err != nil {
			return "", true, fmt.Errorf("解析长整数参数 %q 失败: %w", rawOffset, err)
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.ResetMasterFlushOffset(ctx, brokerAddr, offset); err != nil {
			return "", true, err
		}
		return fmt.Sprintf("reset master flush offset to %d success\n", offset), true, nil
	case "addbroker":
		options, err := parseAddBrokerOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.AddBroker(ctx, options.BrokerContainerAddr, options); err != nil {
			return "", true, err
		}
		return formatAddBroker(options.BrokerContainerAddr), true, nil
	case "removebroker":
		options, err := parseRemoveBrokerOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.BrokerID < 0 {
			return "brokerId can't be negative\n", true, nil
		}
		if client == nil {
			client = NewClient(0)
		}
		if err := client.RemoveBroker(ctx, options.BrokerContainerAddr, options); err != nil {
			return "", true, err
		}
		return formatRemoveBroker(options.BrokerContainerAddr), true, nil
	case "getcontrollermetadata":
		controllerAddr := stringArg(args[1:], "-a", "--controllerAddress")
		if controllerAddr == "" {
			return "", true, errors.New("ControllerAddress 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		meta, err := client.GetControllerMetaData(ctx, controllerAddr)
		if err != nil {
			return "", true, err
		}
		return formatControllerMetaData(meta), true, nil
	case "getsyncstateset":
		if hasFlag(args[1:], "-i", "--interval") {
			return "", false, nil
		}
		controllerAddr := stringArg(args[1:], "-a", "--controllerAddress")
		if controllerAddr == "" {
			return "", true, errors.New("ControllerAddress 必填")
		}
		brokerName := strings.TrimSpace(stringArg(args[1:], "-b", "--brokerName"))
		clusterName := strings.TrimSpace(stringArg(args[1:], "-c", "--clusterName"))
		if brokerName == "" && clusterName == "" {
			return "", false, nil
		}
		var result *syncStateSetResult
		var err error
		if brokerName != "" {
			result, err = client.GetSyncStateSet(ctx, controllerAddr, []string{brokerName})
		} else {
			nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
			if nameServer == "" {
				return "", true, errors.New("NameServer 必填")
			}
			result, err = client.GetSyncStateSetByCluster(ctx, nameServer, controllerAddr, clusterName)
		}
		if err != nil {
			return "", true, err
		}
		return formatSyncStateSet(result), true, nil
	case "getcontrollerconfig":
		controllerAddrs := stringArg(args[1:], "-a", "--controllerAddress")
		if controllerAddrs == "" {
			return "", true, errors.New("ControllerAddress 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		sections, err := client.GetControllerConfig(ctx, controllerAddrs)
		if err != nil {
			return "", true, err
		}
		return formatNamesrvConfig(sections), true, nil
	case "dumpcompactionlog":
		fileName := stringArg(args[1:], "-f", "--file")
		if fileName == "" {
			return "miss dump log file name\n", true, nil
		}
		output, err := dumpCompactionLogFile(fileName)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "resetoffsetbytime":
		options, err := parseResetOffsetByTimeOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.ResetOffsetByTime(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		if options.SpecifiedQueue {
			resetOffset := options.ExpectOffset
			if len(rows) > 0 {
				resetOffset = rows[0].Offset
			}
			return formatResetOffsetByTimeSpecifiedQueue(options, resetOffset), true, nil
		}
		return formatResetOffsetByTimeTimestamp(options, rows), true, nil
	case "skipaccumulatedmessage":
		options, err := parseSkipAccumulatedMessageOptions(args[1:])
		if err != nil {
			return "", true, err
		}
		if options.NameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		if client == nil {
			client = NewClient(0)
		}
		rows, err := client.SkipAccumulatedMessage(ctx, options.NameServer, options)
		if err != nil {
			return "", true, err
		}
		return formatSkipAccumulatedMessage(rows), true, nil
	case "querymsgbykey":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		key := stringArg(args[1:], "-k", "--msgKey")
		if key == "" {
			return "", true, errors.New("Message Key 必填")
		}
		beginTimestamp, err := int64Arg(args[1:], 0, "-b", "--beginTimestamp")
		if err != nil {
			return "", true, err
		}
		endTimestamp, err := int64Arg(args[1:], defaultQueryMessageEndTimestamp, "-e", "--endTimestamp")
		if err != nil {
			return "", true, err
		}
		maxNum, err := intArg(args[1:], 64, "-m", "--maxNum")
		if err != nil {
			return "", true, err
		}
		clusterName := stringArg(args[1:], "-c", "--cluster")
		if client == nil {
			client = NewClient(0)
		}
		results, err := client.QueryMessagesByKey(ctx, nameServer, topic, key, clusterName, beginTimestamp, endTimestamp, maxNum)
		if err != nil {
			return "", true, err
		}
		return formatMessageSearchResults(results), true, nil
	case "querymsgbyoffset":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		brokerName := stringArg(args[1:], "-b", "--brokerName")
		if brokerName == "" {
			return "", true, errors.New("Broker Name 必填")
		}
		queueIDRaw := stringArg(args[1:], "-i", "--queueId")
		if queueIDRaw == "" {
			return "", true, errors.New("Queue Id 必填")
		}
		queueID, err := strconv.Atoi(queueIDRaw)
		if err != nil {
			return "", true, fmt.Errorf("解析整数参数 %q 失败: %w", queueIDRaw, err)
		}
		offsetRaw := stringArg(args[1:], "-o", "--offset")
		if offsetRaw == "" {
			return "", true, errors.New("Queue Offset 必填")
		}
		offset, err := strconv.ParseInt(offsetRaw, 10, 64)
		if err != nil {
			return "", true, fmt.Errorf("解析长整数参数 %q 失败: %w", offsetRaw, err)
		}
		bodyFormat := stringArg(args[1:], "-f", "--bodyFormat")
		if client == nil {
			client = NewClient(0)
		}
		detail, err := client.QueryMessageByOffset(ctx, nameServer, topic, brokerName, queueID, offset)
		if err != nil {
			return "", true, err
		}
		tracks, err := client.MessageTrackDetail(ctx, nameServer, detail)
		if err != nil {
			return "", true, err
		}
		output, err := formatMessageDetailWithTracks(detail, bodyFormat, true, tracks)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "consumemessage":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		options := consumeMessageOptions{
			Topic:         topic,
			BrokerName:    stringArg(args[1:], "-b", "--brokerName"),
			ConsumerGroup: stringArg(args[1:], "-g", "--consumerGroup"),
			MessageCount:  128,
		}
		if countRaw := stringArg(args[1:], "-c", "--MessageNumber"); countRaw != "" {
			messageCount, err := strconv.ParseInt(countRaw, 10, 64)
			if err != nil {
				return "", true, fmt.Errorf("解析长整数参数 %q 失败: %w", countRaw, err)
			}
			if messageCount <= 0 {
				return "Please input a positive messageNumber!", true, nil
			}
			options.MessageCount = messageCount
		}
		if queueIDRaw := stringArg(args[1:], "-i", "--queueId"); queueIDRaw != "" {
			if options.BrokerName == "" {
				return "Please set the brokerName before queueId!", true, nil
			}
			queueID, err := strconv.Atoi(queueIDRaw)
			if err != nil {
				return "", true, fmt.Errorf("解析整数参数 %q 失败: %w", queueIDRaw, err)
			}
			options.QueueID = queueID
			options.HasQueueID = true
		}
		if offsetRaw := stringArg(args[1:], "-o", "--offset"); offsetRaw != "" {
			if !options.HasQueueID {
				return "Please set queueId before offset!", true, nil
			}
			offset, err := strconv.ParseInt(offsetRaw, 10, 64)
			if err != nil {
				return "", true, fmt.Errorf("解析长整数参数 %q 失败: %w", offsetRaw, err)
			}
			options.Offset = offset
			options.HasOffset = true
		}
		now := time.Now().UnixMilli()
		if beginRaw := stringArg(args[1:], "-s", "--beginTimestamp", "--beginTimestamp "); beginRaw != "" {
			beginTimestamp, err := parsePrintMsgByQueueTimestamp(beginRaw)
			if err != nil {
				return "", true, err
			}
			if beginTimestamp > now {
				return "Please set the beginTimestamp before now!", true, nil
			}
			options.HasBeginTimestamp = true
			options.BeginTimestamp = beginTimestamp
		}
		if endRaw := stringArg(args[1:], "-e", "--endTimestamp", "--endTimestamp "); endRaw != "" {
			endTimestamp, err := parsePrintMsgByQueueTimestamp(endRaw)
			if err != nil {
				return "", true, err
			}
			if endTimestamp > now {
				return "Please set the endTimestamp before now!", true, nil
			}
			if options.HasBeginTimestamp && options.BeginTimestamp > endTimestamp {
				return "Please make sure that the beginTimestamp is less than or equal to the endTimestamp", true, nil
			}
			options.HasEndTimestamp = true
			options.EndTimestamp = endTimestamp
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.ConsumeMessages(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		output, err := formatConsumeMessage(result)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "printmsgbyqueue":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		brokerName := stringArg(args[1:], "-a", "--brokerName")
		if brokerName == "" {
			return "", true, errors.New("Broker Name 必填")
		}
		queueIDRaw := stringArg(args[1:], "-i", "--queueId")
		if queueIDRaw == "" {
			return "", true, errors.New("Queue Id 必填")
		}
		queueID, err := strconv.Atoi(queueIDRaw)
		if err != nil {
			return "", true, fmt.Errorf("解析整数参数 %q 失败: %w", queueIDRaw, err)
		}
		options := printMsgByQueueOptions{
			Topic:          topic,
			BrokerName:     brokerName,
			QueueID:        queueID,
			PrintMessage:   strings.EqualFold(stringArg(args[1:], "-p", "--print msg"), "true"),
			PrintBody:      strings.EqualFold(stringArg(args[1:], "-d", "--printBody"), "true"),
			CharsetName:    stringArg(args[1:], "-c", "--charsetName"),
			SubExpression:  stringArg(args[1:], "-s", "--subExpression"),
			CalculateByTag: strings.EqualFold(stringArg(args[1:], "-f", "--calculate"), "true"),
		}
		if beginRaw := stringArg(args[1:], "-b", "--beginTimestamp"); beginRaw != "" {
			beginTimestamp, err := parsePrintMsgByQueueTimestamp(beginRaw)
			if err != nil {
				return "", true, err
			}
			options.HasBeginTimestamp = true
			options.BeginTimestamp = beginTimestamp
		}
		if endRaw := stringArg(args[1:], "-e", "--endTimestamp"); endRaw != "" {
			endTimestamp, err := parsePrintMsgByQueueTimestamp(endRaw)
			if err != nil {
				return "", true, err
			}
			options.HasEndTimestamp = true
			options.EndTimestamp = endTimestamp
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.PrintMessagesByQueue(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		output, err := formatPrintMsgByQueue(result, options)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "printmsg":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		options := printMsgOptions{
			Topic:          topic,
			LMQParentTopic: stringArg(args[1:], "-l", "--lmqParentTopic"),
			CharsetName:    stringArg(args[1:], "-c", "--charsetName"),
			SubExpression:  stringArg(args[1:], "-s", "--subExpression"),
		}
		if printBodyRaw := stringArg(args[1:], "-d", "--printBody"); printBodyRaw != "" {
			options.HasPrintBody = true
			options.PrintBody = strings.EqualFold(printBodyRaw, "true")
		}
		if beginRaw := stringArg(args[1:], "-b", "--beginTimestamp"); beginRaw != "" {
			beginTimestamp, err := parsePrintMsgByQueueTimestamp(beginRaw)
			if err != nil {
				return "", true, err
			}
			options.HasBeginTimestamp = true
			options.BeginTimestamp = beginTimestamp
		}
		if endRaw := stringArg(args[1:], "-e", "--endTimestamp"); endRaw != "" {
			endTimestamp, err := parsePrintMsgByQueueTimestamp(endRaw)
			if err != nil {
				return "", true, err
			}
			options.HasEndTimestamp = true
			options.EndTimestamp = endTimestamp
		}
		if client == nil {
			client = NewClient(0)
		}
		result, err := client.PrintMessages(ctx, nameServer, options)
		if err != nil {
			return "", true, err
		}
		output, err := formatPrintMsg(result, options)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "querymsgbyid":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		msgIDs := splitMessageIDs(stringArg(args[1:], "-i", "--msgId"))
		if len(msgIDs) == 0 {
			return "", true, errors.New("Message Id 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--cluster")
		bodyFormat := stringArg(args[1:], "-f", "--bodyFormat")
		if client == nil {
			client = NewClient(0)
		}
		consumerGroup := strings.TrimSpace(stringArg(args[1:], "-g", "--consumerGroup"))
		clientID := strings.TrimSpace(stringArg(args[1:], "-d", "--clientId"))
		if consumerGroup != "" && clientID != "" {
			var builder strings.Builder
			for _, msgID := range msgIDs {
				result, err := client.ConsumeMessageDirectlyByID(ctx, nameServer, consumerGroup, clientID, topic, clusterName, msgID)
				if unavailable := (*consumeMessageDirectlyUnavailableError)(nil); errors.As(err, &unavailable) {
					builder.WriteString(formatConsumeMessageDirectlyUnavailable(unavailable))
					continue
				}
				if err != nil {
					return "", true, err
				}
				builder.WriteString(formatConsumeMessageDirectlyResult(result))
			}
			return builder.String(), true, nil
		}
		if hasFlag(args[1:], "-s", "--sendMessage") {
			if !boolArg(args[1:], false, "-s", "--sendMessage") {
				return "", true, nil
			}
			unitName := stringArg(args[1:], "-u", "--unitName")
			var builder strings.Builder
			for _, msgID := range msgIDs {
				result, err := client.ResendMessageByID(ctx, nameServer, topic, clusterName, msgID, unitName)
				if err != nil {
					return "", true, err
				}
				builder.WriteString(formatQueryMsgByIDResend(result, msgID))
			}
			return builder.String(), true, nil
		}
		var builder strings.Builder
		for _, msgID := range msgIDs {
			detail, err := client.QueryMessageByID(ctx, nameServer, topic, clusterName, msgID)
			if err != nil {
				return "", true, err
			}
			tracks, err := client.MessageTrackDetail(ctx, nameServer, detail)
			if err != nil {
				return "", true, err
			}
			output, err := formatMessageDetailWithTracks(detail, bodyFormat, true, tracks)
			if err != nil {
				return "", true, err
			}
			builder.WriteString(output)
		}
		return builder.String(), true, nil
	case "querymsgbyuniquekey":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		topic := stringArg(args[1:], "-t", "--topic")
		if topic == "" {
			return "", true, errors.New("Topic 必填")
		}
		msgID := strings.TrimSpace(stringArg(args[1:], "-i", "--msgId"))
		if msgID == "" {
			return "", true, errors.New("Message Id 必填")
		}
		clusterName := stringArg(args[1:], "-c", "--cluster")
		if client == nil {
			client = NewClient(0)
		}
		consumerGroup := strings.TrimSpace(stringArg(args[1:], "-g", "--consumerGroup"))
		clientID := strings.TrimSpace(stringArg(args[1:], "-d", "--clientId"))
		if consumerGroup != "" && clientID != "" {
			result, err := client.ConsumeMessageDirectly(ctx, nameServer, consumerGroup, clientID, topic, clusterName, msgID)
			if unavailable := (*consumeMessageDirectlyUnavailableError)(nil); errors.As(err, &unavailable) {
				return formatConsumeMessageDirectlyUnavailable(unavailable), true, nil
			}
			if err != nil {
				return "", true, err
			}
			return formatConsumeMessageDirectlyResult(result), true, nil
		}
		if hasFlag(args[1:], "-a", "--showAll") {
			details, err := client.QueryMessagesByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
			if err != nil {
				return "", true, err
			}
			output, err := formatMessageDetailsForUniqueKey(details)
			if err != nil {
				return "", true, err
			}
			return output, true, nil
		}
		detail, err := client.QueryMessageByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
		if err != nil {
			return "", true, err
		}
		output, err := formatMessageDetailForUniqueKey(detail)
		if err != nil {
			return "", true, err
		}
		return output, true, nil
	case "querymsgtracebyid":
		nameServer := stringArg(args[1:], "-n", "--namesrvAddr")
		if nameServer == "" {
			return "", true, errors.New("NameServer 必填")
		}
		msgID := strings.TrimSpace(stringArg(args[1:], "-i", "--msgId"))
		if msgID == "" {
			return "", true, errors.New("Message Id 必填")
		}
		traceTopic := strings.TrimSpace(stringArg(args[1:], "-t", "--traceTopic"))
		if traceTopic == "" {
			traceTopic = defaultTraceTopic
		}
		beginTimestamp, err := int64Arg(args[1:], 0, "-b", "--beginTimestamp")
		if err != nil {
			return "", true, err
		}
		endTimestamp, err := int64Arg(args[1:], defaultQueryMessageEndTimestamp, "-e", "--endTimestamp")
		if err != nil {
			return "", true, err
		}
		maxNum, err := intArg(args[1:], 64, "-c", "--maxNum")
		if err != nil {
			return "", true, err
		}
		if client == nil {
			client = NewClient(0)
		}
		views, err := client.QueryMessageTraceByID(ctx, nameServer, traceTopic, msgID, beginTimestamp, endTimestamp, maxNum)
		if err != nil {
			return "", true, err
		}
		if len(views) == 0 {
			return "", true, &OfficialCommandResult{ExitCode: 0, Stderr: officialQueryMsgTraceByIDNoMessageStderr()}
		}
		return formatMessageTrace(views), true, nil
	default:
		return "", false, nil
	}
}

// TopicList 查询 NameServer 上的全部 Topic，行为对应官方 topicList 非 -c 模式。
func (c *Client) TopicList(ctx context.Context, nameServer string) ([]string, error) {
	response, err := c.invokeNameServer(ctx, nameServer, remotingCommand{
		Code:     requestCodeGetAllTopicListFromNameServer,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("NameServer topicList failed: code=%d remark=%s", response.Code, response.Remark)
	}
	return decodeTopicListBody(response.Body)
}

// AllocateMQ 复刻官方 allocateMQ，只读 TopicRoute 后按 AllocateMessageQueueAveragely 计算队列分配。
func (c *Client) AllocateMQ(ctx context.Context, nameServer string, topic string, ipList string) ([]allocateMQAssignment, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	return allocateMQAssignmentsFromRoute(topic, ipList, routeBody)
}

// ClusterList 查询集群 Broker 基础运行信息，行为对应官方 clusterList 默认模式。
func (c *Client) ClusterList(ctx context.Context, nameServer string, clusterName string) ([]clusterListRow, error) {
	brokerStats, err := c.clusterListBrokerRuntimeStats(ctx, nameServer, clusterName)
	if err != nil {
		return nil, err
	}
	rows := make([]clusterListRow, 0, len(brokerStats))
	now := time.Now()
	for _, currentBrokerStats := range brokerStats {
		rows = append(rows, buildClusterListRow(
			currentBrokerStats.ClusterName,
			currentBrokerStats.BrokerName,
			currentBrokerStats.BrokerID,
			currentBrokerStats.Addr,
			currentBrokerStats.Stats,
			now,
		))
	}
	return rows, nil
}

// ClusterListMoreStats 查询集群 Broker 日累计量，行为对应官方 clusterList -m 模式。
func (c *Client) ClusterListMoreStats(ctx context.Context, nameServer string, clusterName string) ([]clusterListMoreStatsRow, error) {
	brokerStats, err := c.clusterListBrokerRuntimeStats(ctx, nameServer, clusterName)
	if err != nil {
		return nil, err
	}
	rows := make([]clusterListMoreStatsRow, 0, len(brokerStats))
	for _, currentBrokerStats := range brokerStats {
		rows = append(rows, buildClusterListMoreStatsRow(
			currentBrokerStats.ClusterName,
			currentBrokerStats.BrokerName,
			currentBrokerStats.Stats,
		))
	}
	return rows, nil
}

// ClusterAclConfigVersion 查询指定集群 master Broker 的 ACL 配置版本，行为对应官方 clusterAclConfigVersion -c。
func (c *Client) ClusterAclConfigVersion(ctx context.Context, nameServer string, clusterName string) ([]clusterAclConfigVersionRow, error) {
	cluster := strings.TrimSpace(clusterName)
	if cluster == "" {
		return nil, errors.New("ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[cluster]...)
	if len(brokerNames) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回 Broker", cluster)
	}
	sort.Strings(brokerNames)
	rows := make([]clusterAclConfigVersionRow, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		brokerRows, err := c.fetchBrokerClusterAclConfigVersion(ctx, masterAddr)
		if err != nil {
			return nil, err
		}
		rows = append(rows, brokerRows...)
	}
	return rows, nil
}

// CreateUser 复现官方 createUser -b/-c：broker 模式直连，cluster 模式向集群内全部 Broker 地址写入。
func (c *Client) CreateUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	return c.writeAuthUser(ctx, nameServer, options, requestCodeAuthCreateUser, authUserBodyCreate, "createUser")
}

// UpdateUser 复现官方 updateUser -b/-c：仅把用户显式传入的 password/type/status 写入 body。
func (c *Client) UpdateUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	return c.writeAuthUser(ctx, nameServer, options, requestCodeAuthUpdateUser, authUserBodyUpdate, "updateUser")
}

// DeleteUser 复现官方 deleteUser -b/-c：请求只带 username header，不携带 body。
func (c *Client) DeleteUser(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	return c.writeAuthUser(ctx, nameServer, options, requestCodeAuthDeleteUser, authUserBodyNone, "deleteUser")
}

// CreateAcl 复现官方 createAcl -b/-c：请求 header 只带 subject，body 使用 AclInfo JSON。
func (c *Client) CreateAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	return c.writeAcl(ctx, nameServer, options, requestCodeAuthCreateAcl, true, "createAcl")
}

// UpdateAcl 复现官方 updateAcl -b/-c：与 createAcl 使用同一 AclInfo body 结构。
func (c *Client) UpdateAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	return c.writeAcl(ctx, nameServer, options, requestCodeAuthUpdateAcl, true, "updateAcl")
}

// DeleteAcl 复现官方 deleteAcl -b/-c：只带 subject header，resource 仅在用户传入 -r 时写入 header。
func (c *Client) DeleteAcl(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	return c.writeAcl(ctx, nameServer, options, requestCodeAuthDeleteAcl, false, "deleteAcl")
}

// UpdateAclConfig 更新历史 plain_acl.yml 账号配置，行为对应官方 updateAclConfig。
func (c *Client) UpdateAclConfig(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
	targets, err := c.aclConfigTargetAddrs(ctx, nameServer, options)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.updateAclConfigAtBroker(ctx, target, options); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// DeleteAclConfig 删除历史 plain_acl.yml 账号配置，行为对应官方 deleteAclConfig。
func (c *Client) DeleteAclConfig(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
	targets, err := c.aclConfigTargetAddrs(ctx, nameServer, options)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.deleteAclConfigAtBroker(ctx, target, options.AccessKey); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// UpdateGlobalWhiteAddr 更新历史 plain_acl.yml 全局白名单，行为对应官方 updateGlobalWhiteAddr。
func (c *Client) UpdateGlobalWhiteAddr(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error) {
	targets, err := c.globalWhiteAddrTargetAddrs(ctx, nameServer, options)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.updateGlobalWhiteAddrAtBroker(ctx, target, options); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// SetCommitLogReadAheadMode 复现官方 setCommitLogReadAheadMode：向 Broker 发送 commitLog readAhead 模式变更请求。
func (c *Client) SetCommitLogReadAheadMode(ctx context.Context, nameServer string, brokerAddr string, clusterName string, mode string) ([]commitLogReadAheadModeSection, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	mode = strings.TrimSpace(mode)
	if brokerAddr != "" {
		result, err := c.setCommitLogReadAheadModeAtBroker(ctx, brokerAddr, mode)
		if err != nil {
			return nil, err
		}
		return []commitLogReadAheadModeSection{{
			Header: fmt.Sprintf("============%s============", brokerAddr),
			Result: result,
		}}, nil
	}
	if clusterName == "" {
		return nil, nil
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return []commitLogReadAheadModeSection{{Raw: authClusterNotFoundOutput}}, nil
	}
	sort.Strings(brokerNames)
	sections := make([]commitLogReadAheadModeSection, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr != "" {
			result, err := c.setCommitLogReadAheadModeAtBroker(ctx, masterAddr, mode)
			if err != nil {
				return nil, err
			}
			sections = append(sections, commitLogReadAheadModeSection{
				Header: fmt.Sprintf("============Master: %s============", masterAddr),
				Result: result,
			})
		}
		for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
			if brokerID == "0" {
				continue
			}
			slaveAddr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
			if slaveAddr == "" {
				continue
			}
			result, err := c.setCommitLogReadAheadModeAtBroker(ctx, slaveAddr, mode)
			if err != nil {
				return nil, err
			}
			sections = append(sections, commitLogReadAheadModeSection{
				Header: fmt.Sprintf("============My Master: %s=====Slave: %s============", masterAddr, slaveAddr),
				Result: result,
			})
		}
	}
	return sections, nil
}

// ListUser 复现官方 listUser -b/-c：broker 模式直连，cluster 模式逐个 master 查询并在首个非空结果处停止。
func (c *Client) ListUser(ctx context.Context, nameServer string, brokerAddr string, clusterName string, filter string) ([]listUserRow, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	filter = strings.TrimSpace(filter)
	if brokerAddr != "" {
		return c.fetchListUser(ctx, brokerAddr, filter)
	}
	if clusterName == "" {
		return nil, errors.New("BrokerAddr or ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("ListUserSubCommand command failed, there is no broker in cluster.")
	}
	sort.Strings(brokerNames)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		rows, err := c.fetchListUser(ctx, masterAddr, filter)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		for index := range rows {
			rows[index].SourceAddress = masterAddr
		}
		return rows, nil
	}
	return nil, nil
}

// GetUser 复现官方 getUser -b/-c：broker 模式直连，cluster 模式逐个 master 查询并在首个命中用户处停止。
func (c *Client) GetUser(ctx context.Context, nameServer string, brokerAddr string, clusterName string, username string) (*listUserRow, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	username = strings.TrimSpace(username)
	if brokerAddr != "" {
		return c.fetchGetUser(ctx, brokerAddr, username)
	}
	if clusterName == "" {
		return nil, errors.New("BrokerAddr or ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("GetUserSubCommand command failed, there is no broker in cluster.")
	}
	sort.Strings(brokerNames)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		row, err := c.fetchGetUser(ctx, masterAddr, username)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		row.SourceAddress = masterAddr
		return row, nil
	}
	return nil, nil
}

// CopyUser 复现官方 copyUser：从源 Broker 读取用户，再按目标 Broker 是否已有用户选择 createUser 或 updateUser。
func (c *Client) CopyUser(ctx context.Context, sourceBroker string, targetBroker string, usernames string) ([]copyUserResult, error) {
	sourceBroker = strings.TrimSpace(sourceBroker)
	targetBroker = strings.TrimSpace(targetBroker)
	usernames = strings.TrimSpace(usernames)
	if sourceBroker == "" || targetBroker == "" {
		return nil, errors.New("FromBroker and ToBroker 必填")
	}
	userInfos := make([]listUserRow, 0)
	if usernames != "" {
		for _, username := range strings.Split(usernames, ",") {
			if username == "" {
				continue
			}
			userInfo, err := c.fetchGetUser(ctx, sourceBroker, username)
			if err != nil {
				return nil, err
			}
			if userInfo != nil {
				userInfos = append(userInfos, *userInfo)
			}
		}
	} else {
		rows, err := c.fetchListUser(ctx, sourceBroker, "")
		if err != nil {
			return nil, err
		}
		userInfos = rows
	}
	if len(userInfos) == 0 {
		return nil, nil
	}
	results := make([]copyUserResult, 0, len(userInfos))
	for _, userInfo := range userInfos {
		existing, err := c.fetchGetUser(ctx, targetBroker, userInfo.Username)
		if err != nil {
			return nil, err
		}
		requestCode := requestCodeAuthCreateUser
		commandName := "createUser"
		if existing != nil {
			requestCode = requestCodeAuthUpdateUser
			commandName = "updateUser"
		}
		if err := c.writeAuthUserInfoAtBroker(ctx, targetBroker, userInfo, requestCode, commandName); err != nil {
			return nil, err
		}
		results = append(results, copyUserResult{
			Username:     userInfo.Username,
			SourceBroker: sourceBroker,
			TargetBroker: targetBroker,
		})
	}
	return results, nil
}

// ListAcl 复现官方 listAcl -b/-c：broker 模式直连，cluster 模式遍历每个 master 并打印非空 ACL。
func (c *Client) ListAcl(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subjectFilter string, resourceFilter string) ([]aclInfo, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	subjectFilter = strings.TrimSpace(subjectFilter)
	resourceFilter = strings.TrimSpace(resourceFilter)
	if brokerAddr != "" {
		return c.fetchListAcl(ctx, brokerAddr, subjectFilter, resourceFilter)
	}
	if clusterName == "" {
		return nil, errors.New("BrokerAddr or ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("ListAclSubCommand command failed, there is no broker in cluster.")
	}
	sort.Strings(brokerNames)
	rows := make([]aclInfo, 0)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		currentRows, err := c.fetchListAcl(ctx, masterAddr, subjectFilter, resourceFilter)
		if err != nil {
			return nil, err
		}
		for i := range currentRows {
			currentRows[i].SourceAddress = masterAddr
		}
		rows = append(rows, currentRows...)
	}
	return rows, nil
}

// GetAcl 复现官方 getAcl -b/-c：broker 模式直连，cluster 模式遍历每个 master 并打印非空 ACL。
func (c *Client) GetAcl(ctx context.Context, nameServer string, brokerAddr string, clusterName string, subject string) ([]aclInfo, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	subject = strings.TrimSpace(subject)
	if brokerAddr != "" {
		row, err := c.fetchGetAcl(ctx, brokerAddr, subject)
		if err != nil || row == nil {
			return nil, err
		}
		return []aclInfo{*row}, nil
	}
	if clusterName == "" {
		return nil, errors.New("BrokerAddr or ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("GetAclSubCommand command failed, there is no broker in cluster.")
	}
	sort.Strings(brokerNames)
	rows := make([]aclInfo, 0)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		row, err := c.fetchGetAcl(ctx, masterAddr, subject)
		if err != nil {
			return nil, err
		}
		if row != nil {
			row.SourceAddress = masterAddr
			rows = append(rows, *row)
		}
	}
	return rows, nil
}

// CopyAcl 复现官方 copyAcl：从源 Broker 读取 ACL，再按目标 Broker 是否已有同名 ACL 选择 createAcl 或 updateAcl。
func (c *Client) CopyAcl(ctx context.Context, sourceBroker string, targetBroker string, subjects string) ([]copyAclResult, error) {
	sourceBroker = strings.TrimSpace(sourceBroker)
	targetBroker = strings.TrimSpace(targetBroker)
	subjects = strings.TrimSpace(subjects)
	if sourceBroker == "" || targetBroker == "" {
		return nil, errors.New("FromBroker and ToBroker 必填")
	}
	aclInfos := make([]aclInfo, 0)
	if subjects != "" {
		for _, subject := range strings.Split(subjects, ",") {
			subject = strings.TrimSpace(subject)
			if subject == "" {
				continue
			}
			acl, err := c.fetchGetAcl(ctx, sourceBroker, subject)
			if err != nil {
				return nil, err
			}
			if acl != nil {
				aclInfos = append(aclInfos, *acl)
			}
		}
	} else {
		rows, err := c.fetchListAcl(ctx, sourceBroker, "", "")
		if err != nil {
			return nil, err
		}
		aclInfos = rows
	}
	if len(aclInfos) == 0 {
		return nil, nil
	}
	results := make([]copyAclResult, 0, len(aclInfos))
	for _, acl := range aclInfos {
		existing, err := c.fetchGetAcl(ctx, targetBroker, acl.Subject)
		if err != nil {
			return nil, err
		}
		requestCode := requestCodeAuthCreateAcl
		commandName := "createAcl"
		if existing != nil {
			requestCode = requestCodeAuthUpdateAcl
			commandName = "updateAcl"
		}
		if err := c.writeAclInfoAtBroker(ctx, targetBroker, acl, requestCode, commandName); err != nil {
			return nil, err
		}
		results = append(results, copyAclResult{
			Subject:      acl.Subject,
			SourceBroker: sourceBroker,
			TargetBroker: targetBroker,
		})
	}
	return results, nil
}

// BrokerStatus 查询 Broker runtime KVTable，行为对应官方 brokerStatus。
func (c *Client) BrokerStatus(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerStatusTable, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	if brokerAddr != "" {
		stats, err := c.fetchBrokerRuntimeStats(ctx, brokerAddr)
		if err != nil {
			return nil, err
		}
		return []brokerStatusTable{{Stats: stats}}, nil
	}
	if clusterName == "" {
		return nil, errors.New("BrokerAddr or ClusterName 必填")
	}
	brokerStats, err := c.clusterListBrokerRuntimeStats(ctx, nameServer, clusterName)
	if err != nil {
		return nil, err
	}
	rows := make([]brokerStatusTable, 0, len(brokerStats))
	for _, currentBrokerStats := range brokerStats {
		rows = append(rows, brokerStatusTable{
			BrokerAddr: currentBrokerStats.Addr,
			Stats:      currentBrokerStats.Stats,
		})
	}
	return rows, nil
}

// GetBrokerConfig 查询 Broker 配置，行为对应官方 getBrokerConfig -b/-c。
func (c *Client) GetBrokerConfig(ctx context.Context, nameServer string, brokerAddr string, clusterName string) ([]brokerConfigSection, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	if brokerAddr != "" {
		entries, err := c.fetchBrokerConfig(ctx, brokerAddr)
		if err != nil {
			return nil, err
		}
		return []brokerConfigSection{{
			Header:  fmt.Sprintf("============%s============", brokerAddr),
			Entries: entries,
		}}, nil
	}
	if clusterName == "" {
		return nil, nil
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return []brokerConfigSection{{Raw: "[error] Make sure the specified clusterName exists or the name server connected to is correct."}}, nil
	}
	sort.Strings(brokerNames)
	sections := make([]brokerConfigSection, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr != "" {
			entries, err := c.fetchBrokerConfig(ctx, masterAddr)
			if err != nil {
				return nil, err
			}
			sections = append(sections, brokerConfigSection{
				Header:  fmt.Sprintf("============Master: %s============", masterAddr),
				Entries: entries,
			})
		}
		for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
			if brokerID == "0" {
				continue
			}
			slaveAddr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
			if slaveAddr == "" {
				continue
			}
			entries, err := c.fetchBrokerConfig(ctx, slaveAddr)
			if err != nil {
				return nil, err
			}
			sections = append(sections, brokerConfigSection{
				Header:  fmt.Sprintf("============My Master: %s=====Slave: %s============", masterAddr, slaveAddr),
				Entries: entries,
			})
		}
	}
	return sections, nil
}

// UpdateBrokerConfig 更新 Broker 动态配置，行为对应官方 updateBrokerConfig。
func (c *Client) UpdateBrokerConfig(ctx context.Context, nameServer string, options updateBrokerConfigOptions) ([]string, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.Key = strings.TrimSpace(options.Key)
	options.Value = strings.TrimSpace(options.Value)
	if options.Key == "" || options.Value == "" {
		return nil, errors.New("Key、Value 必填")
	}
	if options.BrokerAddr != "" {
		if err := c.updateBrokerConfigAtBroker(ctx, options.BrokerAddr, options.Key, options.Value); err != nil {
			return nil, err
		}
		return []string{options.BrokerAddr}, nil
	}
	if options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if options.NameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	targets := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		if options.UpdateAllBroker {
			for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
				addr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
				if addr == "" {
					continue
				}
				if err := c.updateBrokerConfigAtBroker(ctx, addr, options.Key, options.Value); err != nil {
					return nil, err
				}
				targets = append(targets, addr)
			}
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.updateBrokerConfigAtBroker(ctx, masterAddr, options.Key, options.Value); err != nil {
			return nil, err
		}
		targets = append(targets, masterAddr)
	}
	return targets, nil
}

// ExportConfigs 导出集群 master Broker 配置，行为对应官方 exportConfigs。
func (c *Client) ExportConfigs(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error) {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return "", errors.New("NameServer 必填")
	}
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return "", errors.New("ClusterName 必填")
	}
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		filePath = "/tmp/rocketmq/export"
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return "", err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return "", errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	data := exportConfigsData{
		NameServerSize: len(splitNameServers(nameServer)),
	}
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		entries, err := c.fetchBrokerConfig(ctx, masterAddr)
		if err != nil {
			return "", err
		}
		data.MasterBrokerSize++
		for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
			if brokerID == "0" {
				continue
			}
			if strings.TrimSpace(brokerData.BrokerAddrs[brokerID]) != "" {
				data.SlaveBrokerSize++
			}
		}
		configBrokerName := brokerConfigEntryValue(entries, "brokerName")
		if configBrokerName == "" {
			configBrokerName = brokerName
		}
		data.BrokerConfigs = append(data.BrokerConfigs, exportBrokerConfig{
			BrokerName: configBrokerName,
			Entries:    entries,
		})
	}
	outputPath := exportConfigsOutputPath(filePath)
	if err := os.MkdirAll(strings.TrimRight(filePath, `/\`), 0755); err != nil {
		return "", err
	}
	if err := os.WriteFile(outputPath, []byte(formatExportConfigsJSON(data)), 0644); err != nil {
		return "", err
	}
	return outputPath, nil
}

// ExportMetrics 导出集群运行指标，行为对应官方 exportMetrics。
func (c *Client) ExportMetrics(ctx context.Context, nameServer string, clusterName string, filePath string) (string, error) {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return "", errors.New("NameServer 必填")
	}
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return "", errors.New("ClusterName 必填")
	}
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		filePath = "/tmp/rocketmq/export"
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return "", err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return "", errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	data := exportMetricsData{Reports: make([]exportMetricsBrokerReport, 0, len(brokerNames))}
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		report, err := c.exportMetricsBrokerReport(ctx, nameServer, brokerName, masterAddr)
		if err != nil {
			return "", err
		}
		data.Total.add(report.RuntimeQuota)
		data.Reports = append(data.Reports, report)
	}
	outputPath := exportMetricsOutputPath(filePath)
	if err := writeFastJSONFile(outputPath, formatExportMetricsJSON(data)); err != nil {
		return "", err
	}
	return outputPath, nil
}

func (c *Client) exportMetricsBrokerReport(ctx context.Context, nameServer string, brokerName string, brokerAddr string) (exportMetricsBrokerReport, error) {
	stats, err := c.fetchBrokerRuntimeStats(ctx, brokerAddr)
	if err != nil {
		return exportMetricsBrokerReport{}, err
	}
	configs, err := c.fetchBrokerConfig(ctx, brokerAddr)
	if err != nil {
		return exportMetricsBrokerReport{}, err
	}
	subscriptionGroups, err := c.fetchSubscriptionGroupPairs(ctx, brokerAddr)
	if err != nil {
		return exportMetricsBrokerReport{}, err
	}
	topicConfigs, err := c.fetchUserTopicConfigPairs(ctx, brokerAddr, false)
	if err != nil {
		return exportMetricsBrokerReport{}, err
	}
	transStats, _ := c.viewBrokerStatsData(ctx, brokerAddr, statsNameTopicPutNums, "RMQ_SYS_TRANS_HALF_TOPIC")
	scheduleStats, _ := c.viewBrokerStatsData(ctx, brokerAddr, statsNameTopicPutNums, "SCHEDULE_TOPIC_XXXX")
	return exportMetricsBrokerReport{
		BrokerName: strings.TrimSpace(brokerName),
		RuntimeEnv: exportMetricsRuntimeEnv{
			CPUNum:         brokerConfigEntryValue(configs, "clientCallbackExecutorThreads"),
			TotalMemKBytes: stats["totalMemKBytes"],
		},
		RuntimeQuota: c.exportMetricsRuntimeQuota(stats, topicConfigs, subscriptionGroups, transStats, scheduleStats),
		RuntimeVersion: exportMetricsRuntimeVersion{
			RocketMQVersion: exportMetricsRocketMQVersion,
			ClientInfo:      c.exportMetricsClientInfo(ctx, nameServer, brokerAddr, subscriptionGroups),
		},
	}, nil
}

func (c *Client) exportMetricsRuntimeQuota(stats map[string]string, topicConfigs []orderedJSONPair, subscriptionGroups []orderedJSONPair, transStats *brokerStatsData, scheduleStats *brokerStatsData) exportMetricsRuntimeQuota {
	normalIn := parseFirstFloat(stats["putTps"])
	normalOut := parseFirstFloat(stats["getTransferredTps"])
	transIn := exportMetricsStatsMinuteTPS(transStats)
	scheduleIn := exportMetricsStatsMinuteTPS(scheduleStats)
	normalOneDayIn := parseInt64Default(stats["msgPutTotalTodayMorning"], 0) - parseInt64Default(stats["msgPutTotalYesterdayMorning"], 0)
	normalOneDayOut := parseInt64Default(stats["msgGetTotalTodayMorning"], 0) - parseInt64Default(stats["msgGetTotalYesterdayMorning"], 0)
	return exportMetricsRuntimeQuota{
		CommitLogDiskRatio:    stats["commitLogDiskRatio"],
		ConsumeQueueDiskRatio: stats["consumeQueueDiskRatio"],
		NormalInTps:           normalIn,
		NormalOutTps:          normalOut,
		TransInTps:            transIn,
		ScheduleInTps:         scheduleIn,
		NormalOneDayInNum:     normalOneDayIn,
		NormalOneDayOutNum:    normalOneDayOut,
		TransOneDayInNum:      brokerStats24HourSum(transStats),
		ScheduleOneDayInNum:   brokerStats24HourSum(scheduleStats),
		MessageAverageSize:    stats["putMessageAverageSize"],
		TopicSize:             len(topicConfigs),
		GroupSize:             len(subscriptionGroups),
	}
}

func (c *Client) exportMetricsClientInfo(ctx context.Context, nameServer string, brokerAddr string, subscriptionGroups []orderedJSONPair) []string {
	seen := make(map[string]struct{})
	for _, pair := range subscriptionGroups {
		groupName := strings.TrimSpace(pair.Key)
		if groupName == "" {
			continue
		}
		detail, err := c.ConsumerConnection(ctx, nameServer, groupName, brokerAddr)
		if err != nil || detail == nil {
			continue
		}
		for _, connection := range detail.Connections {
			language := strings.TrimSpace(connection.Language)
			if language == "" {
				continue
			}
			seen[language+"%"+rocketMQVersionDesc(connection.Version)] = struct{}{}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	return exportMetricsJavaHashSetStrings(values)
}

func (total *exportMetricsTotal) add(quota exportMetricsRuntimeQuota) {
	total.NormalInTps += quota.NormalInTps
	total.NormalOutTps += quota.NormalOutTps
	total.TransInTps += quota.TransInTps
	total.ScheduleInTps += quota.ScheduleInTps
	total.NormalOneDayInNum += quota.NormalOneDayInNum
	total.NormalOneDayOutNum += quota.NormalOneDayOutNum
	total.TransOneDayInNum += quota.TransOneDayInNum
	total.ScheduleOneDayInNum += quota.ScheduleOneDayInNum
}

// ExportMetadata 导出 Topic 与订阅组元数据，行为对应官方 exportMetadata。
func (c *Client) ExportMetadata(ctx context.Context, nameServer string, options exportMetadataOptions) (*exportMetadataResult, error) {
	options.FilePath = strings.TrimSpace(options.FilePath)
	if options.FilePath == "" {
		options.FilePath = "/tmp/rocketmq/export"
	}
	if options.BrokerAddr != "" {
		return c.exportMetadataByBroker(ctx, options)
	}
	if strings.TrimSpace(options.ClusterName) == "" {
		return &exportMetadataResult{}, nil
	}
	return c.exportMetadataByCluster(ctx, nameServer, options)
}

func (c *Client) exportMetadataByBroker(ctx context.Context, options exportMetadataOptions) (*exportMetadataResult, error) {
	brokerAddr := strings.TrimSpace(options.BrokerAddr)
	if brokerAddr == "" {
		return &exportMetadataResult{}, nil
	}
	if options.TopicOnly {
		value, err := c.fetchUserTopicConfigValue(ctx, brokerAddr, options.SpecialTopic)
		if err != nil {
			return nil, err
		}
		outputPath := exportMetadataOutputPath(options.FilePath, "topic.json")
		if err := writeFastJSONFile(outputPath, formatExportMetadataValue(value, 0)); err != nil {
			return nil, err
		}
		return &exportMetadataResult{OutputPath: outputPath, Wrote: true}, nil
	}
	if options.SubscriptionGroupOnly {
		value, err := c.fetchSubscriptionGroupValue(ctx, brokerAddr)
		if err != nil {
			return nil, err
		}
		outputPath := exportMetadataOutputPath(options.FilePath, "subscriptionGroup.json")
		if err := writeFastJSONFile(outputPath, formatExportMetadataValue(value, 0)); err != nil {
			return nil, err
		}
		return &exportMetadataResult{OutputPath: outputPath, Wrote: true}, nil
	}
	return &exportMetadataResult{}, nil
}

func (c *Client) exportMetadataByCluster(ctx context.Context, nameServer string, options exportMetadataOptions) (*exportMetadataResult, error) {
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[strings.TrimSpace(options.ClusterName)]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	topics := make(map[string]orderedJSONValue)
	groups := make(map[string]orderedJSONValue)
	topicOrder := make([]string, 0)
	groupOrder := make([]string, 0)
	for _, brokerName := range brokerNames {
		brokerData := clusterInfo.BrokerAddrTable[brokerName]
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if !options.SubscriptionGroupOnly {
			topicPairs, err := c.fetchUserTopicConfigPairs(ctx, masterAddr, options.SpecialTopic)
			if err != nil {
				return nil, err
			}
			for _, pair := range topicPairs {
				existing, ok := topics[pair.Key]
				if ok {
					topics[pair.Key] = mergeExportMetadataTopicConfig(existing, pair.Value)
					continue
				}
				topics[pair.Key] = pair.Value
				topicOrder = append(topicOrder, pair.Key)
			}
		}
		if !options.TopicOnly {
			groupPairs, err := c.fetchSubscriptionGroupPairs(ctx, masterAddr)
			if err != nil {
				return nil, err
			}
			for _, pair := range groupPairs {
				existing, ok := groups[pair.Key]
				if ok {
					groups[pair.Key] = mergeExportMetadataSubscriptionGroup(existing, pair.Value)
					continue
				}
				groups[pair.Key] = pair.Value
				groupOrder = append(groupOrder, pair.Key)
			}
		}
	}
	now := options.NowMillis
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	data := exportMetadataData{
		ExportTime:         now,
		TopicConfigs:       exportMetadataTopicConfigsFromOrderedKeys(topics, topicOrder),
		SubscriptionGroups: exportMetadataPairsFromOrderedKeys(groups, groupOrder),
		IncludeTopics:      !options.SubscriptionGroupOnly,
		IncludeGroups:      !options.TopicOnly,
	}
	fileName := "metadata.json"
	if options.TopicOnly {
		fileName = "topic.json"
	} else if options.SubscriptionGroupOnly {
		fileName = "subscriptionGroup.json"
	}
	outputPath := exportMetadataOutputPath(options.FilePath, fileName)
	if err := writeFastJSONFile(outputPath, formatExportMetadataJSON(data)); err != nil {
		return nil, err
	}
	return &exportMetadataResult{OutputPath: outputPath, PrintNewline: true, Wrote: true}, nil
}

// GetNamesrvConfig 查询 NameServer 配置，行为对应官方 getNamesrvConfig。
func (c *Client) GetNamesrvConfig(ctx context.Context, nameServers string) ([]namesrvConfigSection, error) {
	addrs := splitNameServers(nameServers)
	if len(addrs) == 0 {
		return nil, errors.New("NameServer 必填")
	}
	sections := make([]namesrvConfigSection, 0, len(addrs))
	for _, addr := range addrs {
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:     requestCodeGetNamesrvConfig,
			Language: "JAVA",
			Version:  0,
			Opaque:   nextOpaque.Add(1),
			Flag:     0,
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("NameServer %s 返回错误 code=%d remark=%s", addr, response.Code, response.Remark)
		}
		entries, err := decodeBrokerConfigBody(response.Body)
		if err != nil {
			return nil, fmt.Errorf("解析 NameServer 配置失败: %w", err)
		}
		sections = append(sections, namesrvConfigSection{NameServer: addr, Entries: entries})
	}
	return sections, nil
}

// UpdateNamesrvConfig 更新 NameServer 动态配置，行为对应官方 updateNamesrvConfig。
func (c *Client) UpdateNamesrvConfig(ctx context.Context, nameServers string, options updateNamesrvConfigOptions) ([]string, error) {
	options.NameServers = strings.TrimSpace(nameServers)
	options.Key = strings.TrimSpace(options.Key)
	options.Value = strings.TrimSpace(options.Value)
	addrs := splitNameServers(options.NameServers)
	if len(addrs) == 0 {
		return nil, errors.New("NameServer 必填")
	}
	if options.Key == "" || options.Value == "" {
		return nil, errors.New("Key、Value 必填")
	}
	body := []byte(fmt.Sprintf("%s=%s\n", options.Key, options.Value))
	for _, addr := range addrs {
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:     requestCodeUpdateNamesrvConfig,
			Language: "JAVA",
			Version:  0,
			Opaque:   nextOpaque.Add(1),
			Flag:     0,
			Body:     body,
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("NameServer updateNamesrvConfig failed: namesrv=%s code=%d remark=%s", addr, response.Code, response.Remark)
		}
	}
	return addrs, nil
}

// UpdateControllerConfig 更新 Controller 动态配置，行为对应官方 updateControllerConfig。
func (c *Client) UpdateControllerConfig(ctx context.Context, controllerAddrs string, options updateControllerConfigOptions) ([]string, error) {
	options.ControllerAddrs = strings.TrimSpace(controllerAddrs)
	options.Key = strings.TrimSpace(options.Key)
	options.Value = strings.TrimSpace(options.Value)
	addrs := splitControllerAddresses(options.ControllerAddrs)
	if len(addrs) == 0 {
		return nil, errors.New("ControllerAddress 必填")
	}
	if options.Key == "" || options.Value == "" {
		return nil, errors.New("Key、Value 必填")
	}
	body := []byte(fmt.Sprintf("%s=%s\n", options.Key, options.Value))
	for _, addr := range addrs {
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:     requestCodeUpdateControllerConfig,
			Language: "JAVA",
			Version:  0,
			Opaque:   nextOpaque.Add(1),
			Flag:     0,
			Body:     body,
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("Controller updateControllerConfig failed: controller=%s code=%d remark=%s", addr, response.Code, response.Remark)
		}
	}
	return addrs, nil
}

// CleanBrokerMetadata 清理 Controller 上的 Broker 元数据，先按官方流程定位 leader 再发送清理请求。
func (c *Client) CleanBrokerMetadata(ctx context.Context, controllerAddr string, options cleanBrokerMetadataOptions) error {
	options.ControllerAddr = strings.TrimSpace(controllerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.BrokerName = strings.TrimSpace(options.BrokerName)
	options.BrokerControllerIDsToClean = strings.TrimSpace(options.BrokerControllerIDsToClean)
	if options.ControllerAddr == "" {
		return errors.New("ControllerAddress 必填")
	}
	if options.BrokerName == "" {
		return errors.New("BrokerName 必填")
	}
	if !options.CleanLivingBroker && options.ClusterName == "" {
		return errors.New("cleanLivingBroker option is false, clusterName option can not be empty.")
	}
	meta, err := c.GetControllerMetaData(ctx, options.ControllerAddr)
	if err != nil {
		return err
	}
	leaderAddr := strings.TrimSpace(meta.ControllerLeaderAddress)
	if leaderAddr == "" {
		return fmt.Errorf("Controller cleanBrokerMetadata failed: controller=%s empty leader address", options.ControllerAddr)
	}
	fields := map[string]string{
		"brokerName":          options.BrokerName,
		"isCleanLivingBroker": strconv.FormatBool(options.CleanLivingBroker),
		"invokeTime":          strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	if options.ClusterName != "" {
		fields["clusterName"] = options.ClusterName
	}
	if options.BrokerControllerIDsToClean != "" {
		fields["brokerControllerIdsToClean"] = options.BrokerControllerIDsToClean
	}
	response, err := c.invoke(ctx, leaderAddr, remotingCommand{
		Code:      requestCodeCleanBrokerData,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Controller cleanBrokerMetadata failed: controller=%s code=%d remark=%s", leaderAddr, response.Code, response.Remark)
	}
	return nil
}

// ElectMaster 复现官方 electMaster，先定位 Controller leader，再触发指定 brokerId 成为 master。
func (c *Client) ElectMaster(ctx context.Context, controllerAddr string, options electMasterOptions) (*electMasterResult, error) {
	options.ControllerAddr = strings.TrimSpace(controllerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.BrokerName = strings.TrimSpace(options.BrokerName)
	if options.ControllerAddr == "" {
		return nil, errors.New("ControllerAddress 必填")
	}
	if options.ClusterName == "" {
		return nil, errors.New("ClusterName 必填")
	}
	if options.BrokerName == "" {
		return nil, errors.New("BrokerName 必填")
	}
	meta, err := c.GetControllerMetaData(ctx, options.ControllerAddr)
	if err != nil {
		return nil, err
	}
	leaderAddr := strings.TrimSpace(meta.ControllerLeaderAddress)
	if leaderAddr == "" {
		return nil, fmt.Errorf("Controller electMaster failed: controller=%s empty leader address", options.ControllerAddr)
	}
	fields := map[string]string{
		"brokerId":       strconv.FormatInt(options.BrokerID, 10),
		"brokerName":     options.BrokerName,
		"clusterName":    options.ClusterName,
		"designateElect": "true",
		"invokeTime":     strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	response, err := c.invoke(ctx, leaderAddr, remotingCommand{
		Code:      requestCodeControllerElectMaster,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Controller electMaster failed: controller=%s code=%d remark=%s", leaderAddr, response.Code, response.Remark)
	}
	members, membersOK, err := decodeElectMasterBrokerMembers(response.Body)
	if err != nil {
		return nil, err
	}
	return &electMasterResult{
		ClusterName:         options.ClusterName,
		BrokerName:          options.BrokerName,
		BrokerMasterAddr:    strings.TrimSpace(response.ExtFields["masterAddress"]),
		MasterEpoch:         int(parseInt64Default(response.ExtFields["masterEpoch"], 0)),
		SyncStateSetEpoch:   int(parseInt64Default(response.ExtFields["syncStateSetEpoch"], 0)),
		BrokerMemberAddrs:   members,
		BrokerMemberAddrsOK: membersOK,
	}, nil
}

// WipeWritePerm 按官方 wipeWritePerm 语义在每个 NameServer 上擦除指定 Broker 的写权限。
func (c *Client) WipeWritePerm(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error) {
	return c.writePermOnNameServers(ctx, nameServers, brokerName, requestCodeWipeWritePermOfBroker, "wipeTopicCount")
}

// AddWritePerm 按官方 addWritePerm 语义在每个 NameServer 上恢复指定 Broker 的写权限。
func (c *Client) AddWritePerm(ctx context.Context, nameServers string, brokerName string) ([]writePermResult, error) {
	return c.writePermOnNameServers(ctx, nameServers, brokerName, requestCodeAddWritePermOfBroker, "addTopicCount")
}

// writePermOnNameServers 复刻官方对 NameServer 列表逐个发送 brokerName header 并读取计数字段的流程。
func (c *Client) writePermOnNameServers(ctx context.Context, nameServers string, brokerName string, requestCode int, countField string) ([]writePermResult, error) {
	addrs := splitNameServers(nameServers)
	if len(addrs) == 0 {
		return nil, errors.New("NameServer 必填")
	}
	brokerName = strings.TrimSpace(brokerName)
	if brokerName == "" {
		return nil, errors.New("BrokerName 必填")
	}
	results := make([]writePermResult, 0, len(addrs))
	for _, addr := range addrs {
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:      requestCode,
			Language:  "JAVA",
			Version:   0,
			Opaque:    nextOpaque.Add(1),
			Flag:      0,
			ExtFields: map[string]string{"brokerName": brokerName},
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("NameServer write perm failed: namesrv=%s code=%d remark=%s", addr, response.Code, response.Remark)
		}
		count, err := strconv.Atoi(strings.TrimSpace(response.ExtFields[countField]))
		if err != nil {
			return nil, fmt.Errorf("解析 %s 失败: %w", countField, err)
		}
		results = append(results, writePermResult{NameServer: addr, Count: count})
	}
	return results, nil
}

// GetConsumerConfig 查询订阅组配置，行为对应官方 getConsumerConfig。
func (c *Client) GetConsumerConfig(ctx context.Context, nameServer string, groupName string) ([]consumerConfigSection, error) {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return nil, errors.New("GroupName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := sortedBrokerNames(clusterInfo.BrokerAddrTable)
	sections := make([]consumerConfigSection, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData := clusterInfo.BrokerAddrTable[brokerName]
		brokerAddr := brokerData.selectAddr()
		if brokerAddr == "" {
			continue
		}
		entries, err := c.fetchConsumerConfig(ctx, brokerAddr, groupName)
		if err != nil {
			return nil, err
		}
		if entries == nil {
			continue
		}
		clusterName := brokerData.Cluster
		if clusterName == "" {
			clusterName = clusterInfo.clusterNameForBroker(brokerName)
		}
		sections = append(sections, consumerConfigSection{
			Header:  fmt.Sprintf("=============================%s:%s=============================", clusterName, brokerName),
			Entries: entries,
		})
	}
	return sections, nil
}

func (c *Client) clusterListBrokerRuntimeStats(ctx context.Context, nameServer string, clusterName string) ([]clusterListBrokerRuntimeStats, error) {
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}

	clusterNames := clusterInfo.clusterNames(clusterName)
	rows := make([]clusterListBrokerRuntimeStats, 0)
	for _, currentClusterName := range clusterNames {
		brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[currentClusterName]...)
		sort.Strings(brokerNames)
		for _, brokerName := range brokerNames {
			brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
			if !ok {
				continue
			}
			for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
				addr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
				if addr == "" {
					continue
				}
				stats, err := c.fetchBrokerRuntimeStats(ctx, addr)
				if err != nil {
					return nil, err
				}
				rows = append(rows, clusterListBrokerRuntimeStats{
					ClusterName: currentClusterName,
					BrokerName:  brokerName,
					BrokerID:    brokerID,
					Addr:        addr,
					Stats:       stats,
				})
			}
		}
	}
	return rows, nil
}

func (c *Client) fetchBrokerClusterInfo(ctx context.Context, nameServer string) (brokerClusterInfo, error) {
	clusterResponse, err := c.invokeNameServer(ctx, nameServer, remotingCommand{
		Code:     requestCodeGetBrokerClusterInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return brokerClusterInfo{}, err
	}
	if clusterResponse.Code != responseCodeSuccess {
		return brokerClusterInfo{}, fmt.Errorf("NameServer clusterInfo failed: code=%d remark=%s", clusterResponse.Code, clusterResponse.Remark)
	}
	return decodeBrokerClusterInfo(clusterResponse.Body)
}

func (c *Client) fetchBrokerRuntimeStats(ctx context.Context, brokerAddr string) (map[string]string, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetBrokerRuntimeInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker runtimeInfo failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return decodeBrokerRuntimeStatsBody(response.Body)
}

func (c *Client) fetchBrokerClusterAclConfigVersion(ctx context.Context, brokerAddr string) ([]clusterAclConfigVersionRow, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return nil, errors.New("BrokerAddr 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeGetBrokerClusterAclInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("cluster acl config version failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeClusterAclConfigVersionResponse(response.ExtFields)
}

func (c *Client) writeAuthUser(ctx context.Context, nameServer string, options authUserOptions, requestCode int, bodyMode authUserBodyMode, commandName string) ([]string, error) {
	targets, err := c.authUserTargetAddrs(ctx, nameServer, options)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.writeAuthUserAtBroker(ctx, target, options, requestCode, bodyMode, commandName); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func (c *Client) authUserTargetAddrs(ctx context.Context, nameServer string, options authUserOptions) ([]string, error) {
	brokerAddr := strings.TrimSpace(options.BrokerAddr)
	clusterName := strings.TrimSpace(options.ClusterName)
	if brokerAddr != "" {
		return []string{brokerAddr}, nil
	}
	if clusterName == "" {
		return nil, errors.New("BrokerAddr or ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New(authClusterNotFoundOutput)
	}
	sort.Strings(brokerNames)
	seen := make(map[string]struct{})
	targets := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
			addr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
			if addr == "" {
				continue
			}
			if _, exists := seen[addr]; exists {
				continue
			}
			seen[addr] = struct{}{}
			targets = append(targets, addr)
		}
	}
	return targets, nil
}

func (c *Client) writeAuthUserAtBroker(ctx context.Context, brokerAddr string, options authUserOptions, requestCode int, bodyMode authUserBodyMode, commandName string) error {
	fields := map[string]string{
		"username": strings.TrimSpace(options.Username),
	}
	var body []byte
	var err error
	if bodyMode != authUserBodyNone {
		body, err = encodeAuthUserBody(options, bodyMode)
		if err != nil {
			return err
		}
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCode,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
		Body:      body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("%s failed: broker=%s code=%d remark=%s", commandName, brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) writeAcl(ctx context.Context, nameServer string, options aclOptions, requestCode int, includeBody bool, commandName string) ([]string, error) {
	targets, err := c.aclTargetAddrs(ctx, nameServer, options)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.writeAclAtBroker(ctx, target, options, requestCode, includeBody, commandName); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func (c *Client) aclTargetAddrs(ctx context.Context, nameServer string, options aclOptions) ([]string, error) {
	return c.authUserTargetAddrs(ctx, nameServer, authUserOptions{
		BrokerAddr:  options.BrokerAddr,
		ClusterName: options.ClusterName,
	})
}

func (c *Client) aclConfigTargetAddrs(ctx context.Context, nameServer string, options aclConfigOptions) ([]string, error) {
	return c.authUserTargetAddrs(ctx, nameServer, authUserOptions{
		BrokerAddr:  options.BrokerAddr,
		ClusterName: options.ClusterName,
	})
}

func (c *Client) globalWhiteAddrTargetAddrs(ctx context.Context, nameServer string, options globalWhiteAddrOptions) ([]string, error) {
	return c.authUserTargetAddrs(ctx, nameServer, authUserOptions{
		BrokerAddr:  options.BrokerAddr,
		ClusterName: options.ClusterName,
	})
}

func (c *Client) updateAclConfigAtBroker(ctx context.Context, brokerAddr string, options aclConfigOptions) error {
	fields := aclConfigExtFields(options)
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCodeUpdateAndCreateAclConfig,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("updateAclConfig failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) deleteAclConfigAtBroker(ctx context.Context, brokerAddr string, accessKey string) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeDeleteAclConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"accessKey": strings.TrimSpace(accessKey),
		},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("deleteAclConfig failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) updateGlobalWhiteAddrAtBroker(ctx context.Context, brokerAddr string, options globalWhiteAddrOptions) error {
	fields := map[string]string{
		"globalWhiteAddrs": strings.TrimSpace(options.GlobalWhiteRemoteAddresses),
	}
	if strings.TrimSpace(options.AclFileFullPath) != "" {
		fields["aclFileFullPath"] = strings.TrimSpace(options.AclFileFullPath)
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCodeUpdateGlobalWhiteAddrsConfig,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("updateGlobalWhiteAddr failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) writeAclAtBroker(ctx context.Context, brokerAddr string, options aclOptions, requestCode int, includeBody bool, commandName string) error {
	fields := map[string]string{
		"subject": strings.TrimSpace(options.Subject),
	}
	if !includeBody && strings.TrimSpace(options.Resource) != "" {
		fields["resource"] = strings.TrimSpace(options.Resource)
	}
	var body []byte
	var err error
	if includeBody {
		body, err = encodeAclBody(options)
		if err != nil {
			return err
		}
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCode,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
		Body:      body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("%s failed: broker=%s code=%d remark=%s", commandName, brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) writeAclInfoAtBroker(ctx context.Context, brokerAddr string, acl aclInfo, requestCode int, commandName string) error {
	fields := map[string]string{
		"subject": strings.TrimSpace(acl.Subject),
	}
	body, err := encodeCopiedAclBody(acl)
	if err != nil {
		return err
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCode,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
		Body:      body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("%s failed: broker=%s code=%d remark=%s", commandName, brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) writeAuthUserInfoAtBroker(ctx context.Context, brokerAddr string, userInfo listUserRow, requestCode int, commandName string) error {
	fields := map[string]string{
		"username": userInfo.Username,
	}
	body, err := encodeCopiedAuthUserBody(userInfo)
	if err != nil {
		return err
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCode,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
		Body:      body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("%s failed: broker=%s code=%d remark=%s", commandName, brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) setCommitLogReadAheadModeAtBroker(ctx context.Context, brokerAddr string, mode string) (string, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return "", errors.New("BrokerAddr 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeSetCommitLogReadMode,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"READ_AHEAD_MODE": strings.TrimSpace(mode),
		},
	})
	if err != nil {
		return "", err
	}
	if response.Code != responseCodeSuccess {
		return "", fmt.Errorf("setCommitLogReadAheadMode failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	if response.Remark != "" {
		return response.Remark, nil
	}
	return "null", nil
}

func encodeAuthUserBody(options authUserOptions, mode authUserBodyMode) ([]byte, error) {
	payload := struct {
		Username   string  `json:"username,omitempty"`
		Password   *string `json:"password,omitempty"`
		UserType   *string `json:"userType,omitempty"`
		UserStatus *string `json:"userStatus,omitempty"`
	}{
		Username: strings.TrimSpace(options.Username),
	}
	switch mode {
	case authUserBodyCreate:
		if options.PasswordSet || strings.TrimSpace(options.Password) != "" {
			payload.Password = trimmedStringPointer(options.Password)
		}
		if options.UserTypeSet || strings.TrimSpace(options.UserType) != "" {
			payload.UserType = trimmedStringPointer(options.UserType)
		}
	case authUserBodyUpdate:
		if authUserFieldSelected(options.PasswordSet, options.Password, options) {
			payload.Password = trimmedStringPointer(options.Password)
		}
		if authUserFieldSelected(options.UserTypeSet, options.UserType, options) {
			payload.UserType = trimmedStringPointer(options.UserType)
		}
		if authUserFieldSelected(options.UserStatusSet, options.UserStatus, options) {
			payload.UserStatus = trimmedStringPointer(options.UserStatus)
		}
	default:
		return nil, fmt.Errorf("unsupported auth user body mode %d", mode)
	}
	return json.Marshal(payload)
}

func encodeAclBody(options aclOptions) ([]byte, error) {
	type aclEntry struct {
		Resource  string   `json:"resource,omitempty"`
		Actions   []string `json:"actions,omitempty"`
		SourceIps []string `json:"sourceIps,omitempty"`
		Decision  string   `json:"decision,omitempty"`
	}
	type aclPolicy struct {
		Entries []aclEntry `json:"entries,omitempty"`
	}
	payload := struct {
		Subject  string      `json:"subject,omitempty"`
		Policies []aclPolicy `json:"policies,omitempty"`
	}{
		Subject: strings.TrimSpace(options.Subject),
		Policies: []aclPolicy{{
			Entries: make([]aclEntry, 0, len(options.Resources)),
		}},
	}
	for _, resource := range options.Resources {
		payload.Policies[0].Entries = append(payload.Policies[0].Entries, aclEntry{
			Resource:  strings.TrimSpace(resource),
			Actions:   append([]string(nil), options.Actions...),
			SourceIps: append([]string(nil), options.SourceIps...),
			Decision:  strings.TrimSpace(options.Decision),
		})
	}
	return json.Marshal(payload)
}

func encodeCopiedAuthUserBody(userInfo listUserRow) ([]byte, error) {
	payload := struct {
		Username   *string `json:"username,omitempty"`
		Password   *string `json:"password,omitempty"`
		UserType   *string `json:"userType,omitempty"`
		UserStatus *string `json:"userStatus,omitempty"`
	}{
		Username:   stringValuePointer(userInfo.Username),
		Password:   stringValuePointer(userInfo.Password),
		UserType:   stringValuePointer(userInfo.UserType),
		UserStatus: stringValuePointer(userInfo.UserStatus),
	}
	return json.Marshal(payload)
}

func encodeCopiedAclBody(acl aclInfo) ([]byte, error) {
	acl.SourceAddress = ""
	return json.Marshal(acl)
}

// aclConfigExtFields 复刻官方 UpdateAccessConfigRequestHeader 的 header-only 请求字段。
func aclConfigExtFields(options aclConfigOptions) map[string]string {
	fields := map[string]string{
		"accessKey": strings.TrimSpace(options.AccessKey),
		"secretKey": strings.TrimSpace(options.SecretKey),
		"admin":     strconv.FormatBool(options.Admin),
	}
	if strings.TrimSpace(options.WhiteRemoteAddress) != "" {
		fields["whiteRemoteAddress"] = strings.TrimSpace(options.WhiteRemoteAddress)
	}
	if strings.TrimSpace(options.DefaultTopicPerm) != "" {
		fields["defaultTopicPerm"] = strings.TrimSpace(options.DefaultTopicPerm)
	}
	if strings.TrimSpace(options.DefaultGroupPerm) != "" {
		fields["defaultGroupPerm"] = strings.TrimSpace(options.DefaultGroupPerm)
	}
	if options.TopicPermsSet {
		fields["topicPerms"] = strings.Join(options.TopicPerms, ",")
	}
	if options.GroupPermsSet {
		fields["groupPerms"] = strings.Join(options.GroupPerms, ",")
	}
	return fields
}

// trimmedStringPointer 保留空字符串字段，用于复刻官方 UserInfo 在显式选项下的 JSON 语义。
func trimmedStringPointer(value string) *string {
	trimmed := strings.TrimSpace(value)
	return &trimmed
}

func stringValuePointer(value string) *string {
	return &value
}

func authUserFieldSelected(selected bool, value string, options authUserOptions) bool {
	if selected {
		return true
	}
	if options.PasswordSet || options.UserTypeSet || options.UserStatusSet {
		return false
	}
	return strings.TrimSpace(value) != ""
}

func (c *Client) fetchListUser(ctx context.Context, brokerAddr string, filter string) ([]listUserRow, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return nil, errors.New("BrokerAddr 必填")
	}
	fields := map[string]string{}
	if strings.TrimSpace(filter) != "" {
		fields["filter"] = strings.TrimSpace(filter)
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeAuthListUser,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("listUser failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeListUserBody(response.Body)
}

func (c *Client) fetchGetUser(ctx context.Context, brokerAddr string, username string) (*listUserRow, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return nil, errors.New("BrokerAddr 必填")
	}
	fields := map[string]string{}
	if strings.TrimSpace(username) != "" {
		fields["username"] = strings.TrimSpace(username)
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeAuthGetUser,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("getUser failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeGetUserBody(response.Body)
}

func (c *Client) fetchListAcl(ctx context.Context, brokerAddr string, subjectFilter string, resourceFilter string) ([]aclInfo, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return nil, errors.New("BrokerAddr 必填")
	}
	fields := map[string]string{}
	if strings.TrimSpace(subjectFilter) != "" {
		fields["subjectFilter"] = strings.TrimSpace(subjectFilter)
	}
	if strings.TrimSpace(resourceFilter) != "" {
		fields["resourceFilter"] = strings.TrimSpace(resourceFilter)
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeAuthListAcl,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("listAcl failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeListAclBody(response.Body)
}

func (c *Client) fetchGetAcl(ctx context.Context, brokerAddr string, subject string) (*aclInfo, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return nil, errors.New("BrokerAddr 必填")
	}
	fields := map[string]string{}
	if strings.TrimSpace(subject) != "" {
		fields["subject"] = strings.TrimSpace(subject)
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeAuthGetAcl,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("getAcl failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeGetAclBody(response.Body)
}

func (c *Client) fetchBrokerConfig(ctx context.Context, brokerAddr string) ([]brokerConfigEntry, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetBrokerConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker config failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return decodeBrokerConfigBody(response.Body)
}

func (c *Client) fetchUserTopicConfigValue(ctx context.Context, brokerAddr string, includeSpecialTopic bool) (orderedJSONValue, error) {
	body, err := c.fetchAllTopicConfigBody(ctx, brokerAddr)
	if err != nil {
		return orderedJSONValue{}, err
	}
	systemTopics, err := c.fetchSystemTopicSet(ctx, brokerAddr)
	if err != nil {
		return orderedJSONValue{}, err
	}
	value, err := decodeOrderedJSONValue(string(body))
	if err != nil {
		return orderedJSONValue{}, err
	}
	filterTopicConfigWrapper(&value, systemTopics, includeSpecialTopic)
	return value, nil
}

func (c *Client) fetchUserTopicConfigPairs(ctx context.Context, brokerAddr string, includeSpecialTopic bool) ([]orderedJSONPair, error) {
	value, err := c.fetchUserTopicConfigValue(ctx, brokerAddr, includeSpecialTopic)
	if err != nil {
		return nil, err
	}
	table, ok := value.objectField("topicConfigTable")
	if !ok || table.Kind != orderedJSONObject {
		return nil, nil
	}
	return append([]orderedJSONPair(nil), table.Pairs...), nil
}

func (c *Client) fetchAllTopicConfigBody(ctx context.Context, brokerAddr string) ([]byte, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetAllTopicConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker topic config failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return response.Body, nil
}

func (c *Client) fetchSystemTopicSet(ctx context.Context, brokerAddr string) (map[string]bool, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetSystemTopicListFromBroker,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker system topic list failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	topics, err := decodeTopicListBody(response.Body)
	if err != nil {
		return nil, err
	}
	values := make(map[string]bool, len(topics))
	for _, topic := range topics {
		values[topic] = true
	}
	return values, nil
}

func (c *Client) fetchSubscriptionGroupValue(ctx context.Context, brokerAddr string) (orderedJSONValue, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetAllSubscriptionGroupConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return orderedJSONValue{}, err
	}
	if response.Code != responseCodeSuccess {
		return orderedJSONValue{}, fmt.Errorf("Broker subscription group config failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	value, err := decodeOrderedJSONValue(string(response.Body))
	if err != nil {
		return orderedJSONValue{}, err
	}
	filterSubscriptionGroupWrapper(&value)
	return value, nil
}

func (c *Client) fetchSubscriptionGroupPairs(ctx context.Context, brokerAddr string) ([]orderedJSONPair, error) {
	value, err := c.fetchSubscriptionGroupValue(ctx, brokerAddr)
	if err != nil {
		return nil, err
	}
	table, ok := value.objectField("subscriptionGroupTable")
	if !ok || table.Kind != orderedJSONObject {
		return nil, nil
	}
	return append([]orderedJSONPair(nil), table.Pairs...), nil
}

func (c *Client) fetchConsumerConfig(ctx context.Context, brokerAddr string, groupName string) ([]consumerConfigEntry, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetSubscriptionGroupConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"group": strings.TrimSpace(groupName),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker consumer config failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	if len(response.Body) == 0 {
		return nil, nil
	}
	return decodeConsumerConfigBody(response.Body)
}

// TopicListCluster 查询 Topic、所属集群和消费组，行为对应官方 topicList -c 模式。
func (c *Client) TopicListCluster(ctx context.Context, nameServer string) ([]topicClusterRow, error) {
	clusterResponse, err := c.invokeNameServer(ctx, nameServer, remotingCommand{
		Code:     requestCodeGetBrokerClusterInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if clusterResponse.Code != responseCodeSuccess {
		return nil, fmt.Errorf("NameServer clusterInfo failed: code=%d remark=%s", clusterResponse.Code, clusterResponse.Remark)
	}
	brokerClusters, err := decodeBrokerClusterMap(clusterResponse.Body)
	if err != nil {
		return nil, err
	}
	topics, err := c.TopicList(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	rowsByTopic := make([][]topicClusterRow, len(topics))
	sem := make(chan struct{}, topicListClusterConcurrency)
	var wg sync.WaitGroup
	for index, topic := range topics {
		if strings.HasPrefix(topic, retryGroupTopicPrefix) || strings.HasPrefix(topic, dlqGroupTopicPrefix) {
			continue
		}
		wg.Add(1)
		go func(index int, topic string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rowsByTopic[index] = c.topicListClusterRows(ctx, nameServer, brokerClusters, topic)
		}(index, topic)
	}
	wg.Wait()

	rows := make([]topicClusterRow, 0, len(topics))
	for _, topicRows := range rowsByTopic {
		rows = append(rows, topicRows...)
	}
	return rows, nil
}

func (c *Client) topicListClusterRows(ctx context.Context, nameServer string, brokerClusters map[string]string, topic string) []topicClusterRow {
	clusterName := ""
	groups := []string{""}
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err == nil {
		brokers, decodeErr := decodeTopicRouteBrokers(routeBody)
		if decodeErr == nil && len(brokers) > 0 {
			clusterName = brokerClusters[brokers[0].BrokerName]
			groupList, groupErr := c.queryTopicConsumeByWho(ctx, topic, brokers)
			if groupErr == nil && len(groupList) > 0 {
				groups = groupList
			}
		}
	}
	rows := make([]topicClusterRow, 0, len(groups))
	for _, group := range groups {
		rows = append(rows, topicClusterRow{
			ClusterName:   clusterName,
			Topic:         topic,
			ConsumerGroup: group,
		})
	}
	return rows
}

func (c *Client) queryTopicConsumeByWho(ctx context.Context, topic string, brokers []topicRouteBroker) ([]string, error) {
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:     requestCodeQueryTopicConsumeByWho,
			Language: "JAVA",
			Version:  0,
			Opaque:   nextOpaque.Add(1),
			Flag:     0,
			ExtFields: map[string]string{
				"topic": topic,
			},
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("Broker queryTopicConsumeByWho failed: code=%d remark=%s", response.Code, response.Remark)
		}
		var payload struct {
			GroupList []string `json:"groupList"`
		}
		if len(response.Body) == 0 {
			return nil, nil
		}
		if err := json.Unmarshal(response.Body, &payload); err != nil {
			return nil, fmt.Errorf("解析 groupList 失败: %w", err)
		}
		return payload.GroupList, nil
	}
	return nil, nil
}

// TopicRoute 查询 Topic 路由，行为对应官方 topicRoute 默认 JSON 模式。
func (c *Client) TopicRoute(ctx context.Context, nameServer string, topic string) ([]byte, error) {
	response, err := c.invokeNameServer(ctx, nameServer, remotingCommand{
		Code:     requestCodeGetRouteInfoByTopic,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"topic": strings.TrimSpace(topic),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, &topicRouteError{Code: response.Code, Remark: response.Remark}
	}
	if len(response.Body) == 0 {
		return nil, errors.New("NameServer topicRoute response body empty")
	}
	return response.Body, nil
}

// TopicStatus 查询 Topic 各队列水位，行为对应官方 topicStatus 表格模式。
func (c *Client) TopicStatus(ctx context.Context, nameServer string, topic string) ([]topicStatusEntry, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	return c.topicStatusFromBrokers(ctx, topic, brokers)
}

// TopicStatusByCluster 复刻官方 topicStatus -c：先按 cluster 查询路由，再逐个 Broker 查询真实 topic 状态。
func (c *Client) TopicStatusByCluster(ctx context.Context, nameServer string, topic string, cluster string) ([]topicStatusEntry, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, cluster)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	return c.topicStatusFromBrokers(ctx, topic, brokers)
}

// topicStatusFromBrokers 对传入的 Broker 路由逐个发送 GET_TOPIC_STATS_INFO，并合并官方 TopicStatsTable。
func (c *Client) topicStatusFromBrokers(ctx context.Context, topic string, brokers []topicRouteBroker) ([]topicStatusEntry, error) {
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	var entries []topicStatusEntry
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:     requestCodeGetTopicStatsInfo,
			Language: "JAVA",
			Version:  0,
			Opaque:   nextOpaque.Add(1),
			Flag:     0,
			ExtFields: map[string]string{
				"topic": strings.TrimSpace(topic),
			},
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("Broker topicStatus failed: broker=%s code=%d remark=%s", broker.BrokerName, response.Code, response.Remark)
		}
		brokerEntries, err := decodeTopicStatsBody(response.Body)
		if err != nil {
			return nil, err
		}
		entries = append(entries, brokerEntries...)
	}
	if len(entries) == 0 {
		return nil, errors.New("topicStatus 未解析到队列状态")
	}
	return entries, nil
}

// TopicClusterList 查询 Topic 所在集群，行为对应官方 topicClusterList。
func (c *Client) TopicClusterList(ctx context.Context, nameServer string, topic string) ([]string, error) {
	clusterResponse, err := c.invokeNameServer(ctx, nameServer, remotingCommand{
		Code:     requestCodeGetBrokerClusterInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if clusterResponse.Code != responseCodeSuccess {
		return nil, fmt.Errorf("NameServer clusterInfo failed: code=%d remark=%s", clusterResponse.Code, clusterResponse.Remark)
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	return decodeTopicClusters(clusterResponse.Body, routeBody)
}

// ConsumerProgress 查询消费者组位点明细，行为对应官方 consumerProgress -g 明细模式。
func (c *Client) ConsumerProgress(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
	routeTopic := strings.TrimSpace(topic)
	if routeTopic == "" {
		routeTopic = retryGroupTopicPrefix + strings.TrimSpace(consumerGroup)
	} else if cluster := strings.TrimSpace(clusterName); cluster != "" {
		// 官方 examineConsumeStats(cluster, group, topic) 用 cluster/LMQ 父 Topic 查路由，Broker 请求仍携带真实 topic。
		routeTopic = cluster
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	result := &consumerProgress{}
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		fields := map[string]string{
			"consumerGroup": strings.TrimSpace(consumerGroup),
		}
		if strings.TrimSpace(topic) != "" {
			fields["topic"] = strings.TrimSpace(topic)
		}
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:      requestCodeGetConsumeStats,
			Language:  "JAVA",
			Version:   0,
			Opaque:    nextOpaque.Add(1),
			Flag:      0,
			ExtFields: fields,
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("Broker consumerProgress failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
		}
		progress, err := decodeConsumeStatsBody(response.Body)
		if err != nil {
			return nil, err
		}
		result.Entries = append(result.Entries, progress.Entries...)
		result.ConsumeTPS += progress.ConsumeTPS
	}
	return result, nil
}

// CloneGroupOffset 复刻官方 cloneGroupOffset：读取源组所有位点，再写入目标组对应队列位点。
func (c *Client) CloneGroupOffset(ctx context.Context, nameServer string, srcGroup string, destGroup string, topic string) error {
	nameServer = strings.TrimSpace(nameServer)
	srcGroup = strings.TrimSpace(srcGroup)
	destGroup = strings.TrimSpace(destGroup)
	topic = strings.TrimSpace(topic)
	if nameServer == "" {
		return errors.New("NameServer 必填")
	}
	if srcGroup == "" || destGroup == "" || topic == "" {
		return errors.New("SrcGroup、DestGroup、Topic 必填")
	}
	progress, err := c.ConsumerProgress(ctx, nameServer, srcGroup, "", "")
	if err != nil {
		return err
	}
	if progress == nil || len(progress.Entries) == 0 {
		return nil
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return err
	}
	addrByBroker := make(map[string]string, len(brokers))
	for _, broker := range brokers {
		if addr := broker.selectAddr(); addr != "" {
			addrByBroker[broker.BrokerName] = addr
		}
	}
	for _, entry := range progress.Entries {
		if entry.ConsumerOffset < 0 {
			continue
		}
		brokerAddr := strings.TrimSpace(addrByBroker[entry.BrokerName])
		if brokerAddr == "" {
			return fmt.Errorf("cloneGroupOffset 未找到 Broker %s 的地址", entry.BrokerName)
		}
		if err := c.updateConsumerOffsetAtBroker(ctx, brokerAddr, destGroup, entry); err != nil {
			return err
		}
	}
	return nil
}

// SendMessage 复刻官方 mqadmin sendMessage：先按 TopicRoute 选择发送队列，再向 Broker 发送 SEND_MESSAGE_V2。
func (c *Client) SendMessage(ctx context.Context, nameServer string, options sendMessageOptions) (*sendMessageResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	options.Topic = strings.TrimSpace(options.Topic)
	options.Body = strings.TrimSpace(options.Body)
	options.Keys = strings.TrimSpace(options.Keys)
	options.Tags = strings.TrimSpace(options.Tags)
	options.BrokerName = strings.TrimSpace(options.BrokerName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.Topic == "" || options.Body == "" {
		return nil, errors.New("Topic、Body 必填")
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, options.Topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	addrByBroker := make(map[string]string, len(brokers))
	for _, broker := range brokers {
		if addr := strings.TrimSpace(broker.BrokerAddrs["0"]); addr != "" {
			addrByBroker[broker.BrokerName] = addr
		}
	}
	queue, err := selectSendMessageQueue(options, routeBody)
	if err != nil {
		return nil, err
	}
	brokerAddr := strings.TrimSpace(addrByBroker[queue.BrokerName])
	if brokerAddr == "" {
		return nil, fmt.Errorf("sendMessage 未找到 Broker %s 的 master 地址", queue.BrokerName)
	}
	result, traceContext, err := c.sendMessageToBrokerWithTraceContext(ctx, brokerAddr, queue, options)
	if err != nil {
		return nil, err
	}
	if options.MsgTraceEnable && traceContext.TraceOn && strings.TrimSpace(traceContext.RegionID) != "" {
		if err := c.sendMessagePubTrace(ctx, nameServer, options, result, traceContext); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// SendMsgStatus 复刻官方 mqadmin sendMsgStatus：先发送一次 warmup，再打印 count 次发送耗时和 SendResult。
func (c *Client) SendMsgStatus(ctx context.Context, nameServer string, options sendMsgStatusOptions) ([]sendMsgStatusResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	options.BrokerName = strings.TrimSpace(options.BrokerName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.BrokerName == "" {
		return nil, errors.New("BrokerName 必填")
	}
	if options.MessageSize <= 0 {
		options.MessageSize = 128
	}
	if options.Count <= 0 {
		options.Count = 50
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, options.BrokerName)
	if err != nil {
		return nil, err
	}
	queue, err := selectSendMessageQueue(sendMessageOptions{Topic: options.BrokerName}, routeBody)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	broker, ok := findTopicRouteBroker(brokers, queue.BrokerName)
	if !ok {
		return nil, fmt.Errorf("sendMsgStatus 未找到 Broker %s", queue.BrokerName)
	}
	brokerAddr := broker.selectAddr()
	if brokerAddr == "" {
		return nil, fmt.Errorf("sendMsgStatus 未找到 Broker %s 的 master 地址", queue.BrokerName)
	}
	baseOptions := sendMessageOptions{
		Topic:                 options.BrokerName,
		BrokerName:            queue.BrokerName,
		QueueID:               queue.QueueID,
		HasQueueID:            true,
		ProducerGroup:         "PID_SMSC",
		OmitWaitStoreProperty: true,
	}
	warmupOptions := baseOptions
	warmupOptions.Body = buildSendMsgStatusBody(16)
	if _, err := c.sendMessageToBroker(ctx, brokerAddr, queue, warmupOptions); err != nil {
		return nil, err
	}
	results := make([]sendMsgStatusResult, 0, options.Count)
	messageBody := buildSendMsgStatusBody(options.MessageSize)
	for index := 0; index < options.Count; index++ {
		sendOptions := baseOptions
		sendOptions.Body = messageBody
		begin := time.Now()
		result, err := c.sendMessageToBroker(ctx, brokerAddr, queue, sendOptions)
		if err != nil {
			return nil, err
		}
		results = append(results, sendMsgStatusResult{
			RTMillis:   time.Since(begin).Milliseconds(),
			SendResult: *result,
		})
	}
	return results, nil
}

func (c *Client) CheckMsgSendRT(ctx context.Context, nameServer string, options checkMsgSendRTOptions) (*checkMsgSendRTResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	options.Topic = strings.TrimSpace(options.Topic)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.Topic == "" {
		return nil, errors.New("Topic 必填")
	}
	if options.Amount <= 0 {
		options.Amount = 100
	}
	if options.Size <= 0 {
		options.Size = 128
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, options.Topic)
	if err != nil {
		return nil, err
	}
	queues, err := decodeTopicRoutePublishQueues(options.Topic, routeBody)
	if err != nil {
		return nil, err
	}
	if len(queues) == 0 {
		return nil, fmt.Errorf("checkMsgSendRT topic %s 无可写队列", options.Topic)
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	brokerAddrs := make(map[string]string, len(brokers))
	for _, broker := range brokers {
		if addr := broker.selectAddr(); addr != "" {
			brokerAddrs[broker.BrokerName] = addr
		}
	}
	messageBody := strings.Repeat("a", options.Size)
	result := &checkMsgSendRTResult{Rows: make([]checkMsgSendRTRow, 0, options.Amount)}
	var elapsedTotal int64
	for index := 0; index < options.Amount; index++ {
		queue := queues[index%len(queues)]
		brokerAddr := strings.TrimSpace(brokerAddrs[queue.BrokerName])
		if brokerAddr == "" {
			return nil, fmt.Errorf("checkMsgSendRT 未找到 Broker %s 的 master 地址", queue.BrokerName)
		}
		sendOptions := sendMessageOptions{
			Topic:      options.Topic,
			BrokerName: queue.BrokerName,
			QueueID:    queue.QueueID,
			HasQueueID: true,
			Body:       messageBody,
		}
		begin := time.Now()
		_, sendErr := c.sendMessageToBroker(ctx, brokerAddr, queue, sendOptions)
		rtMillis := time.Since(begin).Milliseconds()
		if index != 0 {
			elapsedTotal += rtMillis
		}
		result.Rows = append(result.Rows, checkMsgSendRTRow{
			BrokerName:  queue.BrokerName,
			QueueID:     queue.QueueID,
			SendSuccess: sendErr == nil,
			RTMillis:    rtMillis,
		})
	}
	result.AvgRT = float64(elapsedTotal) / float64(options.Amount-1)
	return result, nil
}

// ClusterRT 复刻官方 mqadmin clusterRT 的单轮采样；连续循环由 CLI 层按 -i/default interval 控制。
func (c *Client) ClusterRT(ctx context.Context, nameServer string, options clusterRTOptions) (*clusterRTResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.Amount <= 0 {
		options.Amount = 100
	}
	if options.Size <= 0 {
		options.Size = 128
	}
	if strings.TrimSpace(options.MachineRoom) == "" {
		options.MachineRoom = "noname"
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	result := &clusterRTResult{}
	clusterNames := clusterInfo.clusterNames(options.ClusterName)
	for _, clusterName := range clusterNames {
		brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
		if len(brokerNames) == 0 {
			result.Raw = fmt.Sprintf("cluster [%s] not exist", clusterName)
			break
		}
		sort.Strings(brokerNames)
		for _, brokerName := range brokerNames {
			row, err := c.clusterRTBroker(ctx, nameServer, clusterName, brokerName, options)
			if err != nil {
				return nil, err
			}
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

func (c *Client) clusterRTBroker(ctx context.Context, nameServer string, clusterName string, brokerName string, options clusterRTOptions) (clusterRTRow, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, brokerName)
	if err != nil {
		return clusterRTRow{}, err
	}
	queues, err := decodeTopicRoutePublishQueues(brokerName, routeBody)
	if err != nil {
		return clusterRTRow{}, err
	}
	if len(queues) == 0 {
		return clusterRTRow{}, fmt.Errorf("clusterRT brokerName topic %s 无可写队列", brokerName)
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return clusterRTRow{}, err
	}
	brokerAddrs := make(map[string]string, len(brokers))
	for _, broker := range brokers {
		if addr := broker.selectAddr(); addr != "" {
			brokerAddrs[broker.BrokerName] = addr
		}
	}
	messageBody := strings.Repeat("a", options.Size)
	producerGroup := strconv.FormatInt(time.Now().UnixMilli(), 10)
	row := clusterRTRow{ClusterName: clusterName, BrokerName: brokerName, Timestamp: time.Now()}
	var elapsedTotal int64
	for index := 0; index < options.Amount; index++ {
		queue := queues[index%len(queues)]
		brokerAddr := strings.TrimSpace(brokerAddrs[queue.BrokerName])
		if brokerAddr == "" {
			return clusterRTRow{}, fmt.Errorf("clusterRT 未找到 Broker %s 的 master 地址", queue.BrokerName)
		}
		sendOptions := sendMessageOptions{
			Topic:                 brokerName,
			BrokerName:            queue.BrokerName,
			QueueID:               queue.QueueID,
			HasQueueID:            true,
			Body:                  messageBody,
			ProducerGroup:         producerGroup,
			OmitWaitStoreProperty: true,
		}
		begin := time.Now()
		_, sendErr := c.sendMessageToBroker(ctx, brokerAddr, queue, sendOptions)
		rtMillis := time.Since(begin).Milliseconds()
		if sendErr != nil {
			row.FailCount++
		} else {
			row.SuccessCount++
		}
		if index != 0 {
			elapsedTotal += rtMillis
		}
	}
	row.RT = float64(elapsedTotal) / float64(options.Amount-1)
	return row, nil
}

func selectSendMessageQueue(options sendMessageOptions, routeBody []byte) (messageQueueIdentity, error) {
	if options.HasQueueID && strings.TrimSpace(options.BrokerName) != "" {
		return messageQueueIdentity{Topic: strings.TrimSpace(options.Topic), BrokerName: strings.TrimSpace(options.BrokerName), QueueID: options.QueueID}, nil
	}
	queues, err := decodeTopicRoutePublishQueues(options.Topic, routeBody)
	if err != nil {
		return messageQueueIdentity{}, err
	}
	if len(queues) == 0 {
		return messageQueueIdentity{}, fmt.Errorf("sendMessage topic %s 无可写队列", options.Topic)
	}
	return queues[0], nil
}

func (c *Client) sendMessageToBroker(ctx context.Context, brokerAddr string, queue messageQueueIdentity, options sendMessageOptions) (*sendMessageResult, error) {
	result, _, err := c.sendMessageToBrokerWithTraceContext(ctx, brokerAddr, queue, options)
	return result, err
}

func (c *Client) sendMessageToBrokerWithTraceContext(ctx context.Context, brokerAddr string, queue messageQueueIdentity, options sendMessageOptions) (*sendMessageResult, sendMessageTraceContext, error) {
	uniqID := newSendMessageUniqueID()
	properties, messageID := encodeSendMessageProperties(options, uniqID)
	producerGroup := strings.TrimSpace(options.ProducerGroup)
	if producerGroup == "" {
		producerGroup = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	bornTimestamp := time.Now().UnixMilli()
	body := options.BodyBytes
	if body == nil {
		body = []byte(options.Body)
	}
	fields := map[string]string{
		"a": producerGroup,
		"b": queue.Topic,
		"c": defaultCreateTopicKey,
		"d": strconv.Itoa(defaultProducerTopicQueueNums),
		"e": strconv.Itoa(queue.QueueID),
		"f": "0",
		"g": strconv.FormatInt(bornTimestamp, 10),
		"h": "0",
		"i": properties,
		"j": strconv.FormatInt(int64(options.Flag), 10),
		"k": strconv.FormatBool(options.UnitMode),
		"m": "false",
		"n": queue.BrokerName,
	}
	begin := time.Now()
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCodeSendMessageV2,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
		Body:      body,
	})
	if err != nil {
		return nil, sendMessageTraceContext{}, err
	}
	costTimeMillis := int(time.Since(begin).Milliseconds())
	sendStatus, err := sendMessageStatusName(response.Code)
	if err != nil {
		return nil, sendMessageTraceContext{}, fmt.Errorf("Broker sendMessage failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	queueID := queue.QueueID
	if raw := strings.TrimSpace(response.ExtFields["queueId"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return nil, sendMessageTraceContext{}, fmt.Errorf("解析 sendMessage queueId 失败: %w", err)
		}
		queueID = parsed
	}
	queueOffset := int64(0)
	if raw := strings.TrimSpace(response.ExtFields["queueOffset"]); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, sendMessageTraceContext{}, fmt.Errorf("解析 sendMessage queueOffset 失败: %w", err)
		}
		queueOffset = parsed
	}
	regionID := strings.TrimSpace(response.ExtFields["MSG_REGION"])
	if regionID == "" {
		regionID = defaultTraceRegionID
	}
	traceOn := response.ExtFields["TRACE_ON"] != "false"
	result := &sendMessageResult{
		Topic:           queue.Topic,
		BrokerName:      queue.BrokerName,
		QueueID:         queueID,
		SendStatus:      sendStatus,
		MessageID:       messageID,
		OffsetMessageID: strings.TrimSpace(response.ExtFields["msgId"]),
		QueueOffset:     queueOffset,
	}
	traceContext := sendMessageTraceContext{
		ProducerGroup:  producerGroup,
		BrokerAddr:     strings.TrimSpace(brokerAddr),
		BornTimestamp:  bornTimestamp,
		CostTimeMillis: costTimeMillis,
		BodyLength:     len(body),
		RegionID:       regionID,
		TraceOn:        traceOn,
	}
	return result, traceContext, nil
}

func (c *Client) sendMessagePubTrace(ctx context.Context, nameServer string, options sendMessageOptions, result *sendMessageResult, traceContext sendMessageTraceContext) error {
	traceBody := encodeSendMessagePubTrace(options, result, traceContext)
	traceRouteBody, err := c.TopicRoute(ctx, nameServer, defaultTraceTopic)
	if err != nil {
		return err
	}
	traceQueue, err := selectSendMessageQueue(sendMessageOptions{Topic: defaultTraceTopic}, traceRouteBody)
	if err != nil {
		return err
	}
	brokers, err := decodeTopicRouteBrokers(traceRouteBody)
	if err != nil {
		return err
	}
	traceBroker, ok := findTopicRouteBroker(brokers, traceQueue.BrokerName)
	if !ok {
		return fmt.Errorf("sendMessage trace 未找到 Broker %s", traceQueue.BrokerName)
	}
	traceBrokerAddr := traceBroker.selectAddr()
	if traceBrokerAddr == "" {
		return fmt.Errorf("sendMessage trace 未找到 Broker %s 的 master 地址", traceQueue.BrokerName)
	}
	_, err = c.sendMessageToBroker(ctx, traceBrokerAddr, traceQueue, sendMessageOptions{
		Topic:                 defaultTraceTopic,
		Body:                  traceBody,
		Keys:                  sendMessageTraceKeys(result.MessageID, options.Keys),
		ProducerGroup:         sendMessageTraceProducerGroup(traceContext.ProducerGroup),
		OmitWaitStoreProperty: false,
	})
	return err
}

func encodeSendMessagePubTrace(options sendMessageOptions, result *sendMessageResult, traceContext sendMessageTraceContext) string {
	success := result != nil && result.SendStatus == "SEND_OK"
	msgID := ""
	offsetMsgID := ""
	if result != nil {
		msgID = result.MessageID
		offsetMsgID = result.OffsetMessageID
	}
	fields := []string{
		"Pub",
		strconv.FormatInt(traceContext.BornTimestamp, 10),
		traceContext.RegionID,
		traceContext.ProducerGroup,
		strings.TrimSpace(options.Topic),
		msgID,
		strings.TrimSpace(options.Tags),
		strings.TrimSpace(options.Keys),
		traceContext.BrokerAddr,
		strconv.Itoa(traceContext.BodyLength),
		strconv.Itoa(traceContext.CostTimeMillis),
		"0",
		offsetMsgID,
		strconv.FormatBool(success),
	}
	return strings.Join(fields, string(traceContentSplitter)) + string(traceFieldSplitter)
}

func sendMessageTraceKeys(msgID string, businessKeys string) string {
	keys := make([]string, 0, 1+len(splitMessageKeys(businessKeys)))
	msgID = strings.TrimSpace(msgID)
	if msgID != "" {
		keys = append(keys, msgID)
	}
	keys = append(keys, splitMessageKeys(businessKeys)...)
	return strings.Join(keys, " ")
}

func sendMessageTraceProducerGroup(producerGroup string) string {
	producerGroup = strings.TrimSpace(producerGroup)
	if producerGroup == "" {
		producerGroup = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	return "_INNER_TRACE_PRODUCER-" + producerGroup + "-Pub-1"
}

func encodeSendMessageProperties(options sendMessageOptions, uniqID string) (string, string) {
	properties := messageProperties{}
	if options.PreserveProperties {
		properties = append(properties, options.Properties...)
	} else {
		if options.Tags != "" {
			properties.Set("TAGS", options.Tags)
		}
		if options.Keys != "" {
			properties.Set("KEYS", options.Keys)
		}
		if !options.OmitWaitStoreProperty {
			properties.Set("WAIT", "true")
		}
	}
	messageID := strings.TrimSpace(properties.Get("UNIQ_KEY"))
	if messageID == "" {
		messageID = uniqID
		properties.Set("UNIQ_KEY", messageID)
	}
	return encodeMessageProperties(properties), messageID
}

func buildSendMsgStatusBody(messageSize int) string {
	var builder strings.Builder
	for index := 0; index < messageSize; index += 11 {
		builder.WriteString("hello jodie")
	}
	return builder.String()
}

func encodeMessageProperties(properties messageProperties) string {
	var builder strings.Builder
	for _, property := range properties {
		if property.Key == "" || property.Value == "" {
			continue
		}
		builder.WriteString(property.Key)
		builder.WriteRune(rune(1))
		builder.WriteString(property.Value)
		builder.WriteRune(rune(2))
	}
	return builder.String()
}

func newSendMessageUniqueID() string {
	raw := make([]byte, 16)
	binary.BigEndian.PutUint64(raw[0:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(raw[8:16], uint64(nextOpaque.Add(1)))
	return strings.ToUpper(hex.EncodeToString(raw))
}

func sendMessageStatusName(responseCode int) (string, error) {
	switch responseCode {
	case responseCodeSuccess:
		return "SEND_OK", nil
	case responseCodeFlushDiskTimeout:
		return "FLUSH_DISK_TIMEOUT", nil
	case responseCodeFlushSlaveTimeout:
		return "FLUSH_SLAVE_TIMEOUT", nil
	case responseCodeSlaveNotAvailable:
		return "SLAVE_NOT_AVAILABLE", nil
	default:
		return "", fmt.Errorf("unsupported send response code %d", responseCode)
	}
}

// updateConsumerOffsetAtBroker 向 Broker 写入目标消费组的单个 MessageQueue 位点。
func (c *Client) updateConsumerOffsetAtBroker(ctx context.Context, brokerAddr string, consumerGroup string, entry consumerProgressEntry) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateConsumerOffset,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"consumerGroup": strings.TrimSpace(consumerGroup),
			"topic":         strings.TrimSpace(entry.Topic),
			"queueId":       strconv.Itoa(entry.QueueID),
			"commitOffset":  strconv.FormatInt(entry.ConsumerOffset, 10),
			"brokerName":    strings.TrimSpace(entry.BrokerName),
		},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateConsumerOffset failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

// BrokerConsumeStats 查询指定 Broker 上所有消费组位点统计，行为对应官方 brokerConsumeStats。
func (c *Client) BrokerConsumeStats(ctx context.Context, brokerAddr string, isOrder bool, timeout time.Duration) (*brokerConsumeStats, error) {
	if timeout > 0 {
		// timeoutMillis 是官方客户端本地请求超时，不属于 Broker Remoting 自定义字段。
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	response, err := c.invoke(ctx, strings.TrimSpace(brokerAddr), remotingCommand{
		Code:     requestCodeGetBrokerConsumeStats,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"isOrder": strconv.FormatBool(isOrder),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker brokerConsumeStats failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return decodeBrokerConsumeStatsBody(response.Body)
}

// StatsAll 查询全部或指定 Topic 的生产/消费统计，行为对应官方 statsAll。
func (c *Client) StatsAll(ctx context.Context, nameServer string, selectedTopic string, activeOnly bool) ([]statsAllRow, error) {
	topics, err := c.TopicList(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	selectedTopic = strings.TrimSpace(selectedTopic)
	rowsByTopic := make([][]statsAllRow, len(topics))
	sem := make(chan struct{}, statsAllTopicConcurrency)
	var wg sync.WaitGroup
	for index, rawTopic := range topics {
		topic := strings.TrimSpace(rawTopic)
		if topic == "" || strings.HasPrefix(topic, retryGroupTopicPrefix) || strings.HasPrefix(topic, dlqGroupTopicPrefix) {
			continue
		}
		if selectedTopic != "" && topic != selectedTopic {
			continue
		}
		wg.Add(1)
		go func(index int, topic string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rows, err := c.statsAllTopicRows(ctx, nameServer, topic, activeOnly)
			if err == nil {
				rowsByTopic[index] = rows
			}
		}(index, topic)
	}
	wg.Wait()

	rows := make([]statsAllRow, 0)
	for _, topicRows := range rowsByTopic {
		rows = append(rows, topicRows...)
	}
	return rows, nil
}

func (c *Client) statsAllTopicRows(ctx context.Context, nameServer string, topic string, activeOnly bool) ([]statsAllRow, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	masterAddrs := masterBrokerAddrs(brokers)
	inTPS, inMsg24Hour, err := c.sumBrokerStats(ctx, masterAddrs, statsNameTopicPutNums, topic)
	if err != nil {
		return nil, err
	}
	groups, err := c.queryTopicConsumeByWho(ctx, topic, brokers)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		if activeOnly && inMsg24Hour <= 0 {
			return nil, nil
		}
		return []statsAllRow{{
			Topic:       topic,
			InTPS:       inTPS,
			InMsg24Hour: inMsg24Hour,
			NoConsumer:  true,
		}}, nil
	}

	rows := make([]statsAllRow, 0, len(groups))
	for _, group := range groups {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		outTPS, outMsg24Hour, err := c.sumBrokerStats(ctx, masterAddrs, statsNameGroupGetNums, topic+"@"+group)
		progress, err := c.ConsumerProgress(ctx, nameServer, group, topic, "")
		if err != nil {
			progress = nil
		}
		accumulation := consumerProgressDiffTotal(progress)
		if accumulation < 0 {
			accumulation = 0
		}
		if activeOnly && inMsg24Hour <= 0 && outMsg24Hour <= 0 {
			continue
		}
		rows = append(rows, statsAllRow{
			Topic:         topic,
			ConsumerGroup: group,
			Accumulation:  accumulation,
			InTPS:         inTPS,
			OutTPS:        outTPS,
			InMsg24Hour:   inMsg24Hour,
			OutMsg24Hour:  outMsg24Hour,
		})
	}
	return rows, nil
}

func (c *Client) sumBrokerStats(ctx context.Context, brokerAddrs []string, statsName string, statsKey string) (float64, int64, error) {
	var totalTPS float64
	var total24Hour int64
	var seen bool
	for _, brokerAddr := range brokerAddrs {
		stats, err := c.viewBrokerStatsData(ctx, brokerAddr, statsName, statsKey)
		if err != nil {
			continue
		}
		seen = true
		totalTPS += stats.StatsMinute.TPS
		total24Hour += brokerStats24HourSum(stats)
	}
	if !seen {
		return 0, 0, nil
	}
	return totalTPS, total24Hour, nil
}

func (c *Client) viewBrokerStatsData(ctx context.Context, brokerAddr string, statsName string, statsKey string) (*brokerStatsData, error) {
	response, err := c.invoke(ctx, strings.TrimSpace(brokerAddr), remotingCommand{
		Code:     requestCodeViewBrokerStatsData,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"statsName": strings.TrimSpace(statsName),
			"statsKey":  strings.TrimSpace(statsKey),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker viewBrokerStatsData failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return decodeBrokerStatsDataBody(response.Body)
}

// BrokerHAStatus 查询单个 Broker 的 HA 运行时状态，行为对应官方 haStatus -b。
func (c *Client) BrokerHAStatus(ctx context.Context, brokerAddr string) (*haStatusResult, error) {
	response, err := c.invoke(ctx, strings.TrimSpace(brokerAddr), remotingCommand{
		Code:     requestCodeGetBrokerHAStatus,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker haStatus failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return decodeHAStatusBody(response.Body)
}

// AddBroker 复现官方 addBroker，直连 BrokerContainer 加载本机 broker 配置文件。
func (c *Client) AddBroker(ctx context.Context, brokerContainerAddr string, options addBrokerOptions) error {
	addr := strings.TrimSpace(brokerContainerAddr)
	if addr == "" {
		return errors.New("BrokerContainerAddr 必填")
	}
	configPath := strings.TrimSpace(options.BrokerConfigPath)
	if configPath == "" {
		return errors.New("BrokerConfigPath 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeAddBroker,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"configPath": configPath,
		},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("BrokerContainer addBroker failed: brokerContainer=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return nil
}

// RemoveBroker 复现官方 removeBroker，直连 BrokerContainer 移除指定 Broker identity。
func (c *Client) RemoveBroker(ctx context.Context, brokerContainerAddr string, options removeBrokerOptions) error {
	addr := strings.TrimSpace(brokerContainerAddr)
	if addr == "" {
		return errors.New("BrokerContainerAddr 必填")
	}
	clusterName := strings.TrimSpace(options.ClusterName)
	brokerName := strings.TrimSpace(options.BrokerName)
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeRemoveBroker,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"brokerClusterName": clusterName,
			"brokerName":        brokerName,
			"brokerId":          strconv.FormatInt(options.BrokerID, 10),
		},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("BrokerContainer removeBroker failed: brokerContainer=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return nil
}

// GetBrokerEpoch 复现官方 getBrokerEpoch -b，先通过 NameServer 定位 brokerName 的地址，再逐个读取 epoch 缓存。
func (c *Client) GetBrokerEpoch(ctx context.Context, nameServer string, brokerName string) ([]brokerEpochResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	brokerName = strings.TrimSpace(brokerName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if brokerName == "" {
		return nil, errors.New("BrokerName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
	if !ok {
		// 官方 mqadmin 在 brokerName 未命中 NameServer 路由时静默输出空结果。
		return nil, nil
	}
	return c.getBrokerEpochFromBrokerData(ctx, brokerData)
}

// GetBrokerEpochByCluster 复现官方 getBrokerEpoch -c，按集群路由收集 Broker 地址后逐个读取 epoch 缓存。
func (c *Client) GetBrokerEpochByCluster(ctx context.Context, nameServer string, clusterName string) ([]brokerEpochResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	clusterName = strings.TrimSpace(clusterName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if clusterName == "" {
		return nil, errors.New("ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回 Broker", clusterName)
	}
	sort.Strings(brokerNames)
	results := make([]brokerEpochResult, 0)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		current, err := c.getBrokerEpochFromBrokerData(ctx, brokerData)
		if err != nil {
			var responseErr *rocketMQResponseError
			if errors.As(err, &responseErr) && isGetBrokerEpochControllerModeError(responseErr) {
				return nil, &OfficialCommandResult{
					ExitCode: 0,
					Stderr:   officialGetBrokerEpochControllerModeStderr(responseErr.Remark),
				}
			}
			return nil, err
		}
		results = append(results, current...)
	}
	return results, nil
}

func (c *Client) getBrokerEpochFromBrokerData(ctx context.Context, brokerData brokerClusterData) ([]brokerEpochResult, error) {
	results := make([]brokerEpochResult, 0, len(brokerData.BrokerAddrs))
	for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
		addr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
		if addr == "" {
			continue
		}
		result, err := c.fetchBrokerEpoch(ctx, addr)
		if err != nil {
			return nil, err
		}
		result.BrokerAddr = addr
		results = append(results, result)
	}
	return results, nil
}

func (c *Client) fetchBrokerEpoch(ctx context.Context, brokerAddr string) (brokerEpochResult, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetBrokerEpochCache,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return brokerEpochResult{}, err
	}
	if response.Code != responseCodeSuccess {
		return brokerEpochResult{}, &rocketMQResponseError{Code: response.Code, Remark: response.Remark}
	}
	return decodeBrokerEpochCache(response.Body)
}

func isGetBrokerEpochControllerModeError(err *rocketMQResponseError) bool {
	if err == nil {
		return false
	}
	return err.Code == 1 && strings.TrimSpace(err.Remark) == "this request only for controllerMode"
}

// ResetMasterFlushOffset 复现官方 resetMasterFlushOffset，直连指定 Broker 重置 master flush offset。
func (c *Client) ResetMasterFlushOffset(ctx context.Context, brokerAddr string, offset int64) error {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return errors.New("BrokerAddr 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeResetMasterFlushOffset,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"masterFlushOffset": strconv.FormatInt(offset, 10),
		},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker resetMasterFlushOffset failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return nil
}

// GetControllerMetaData 复现官方 getControllerMetaData，直连 controller 地址读取 leader 和 peer 信息。
func (c *Client) GetControllerMetaData(ctx context.Context, controllerAddr string) (*controllerMetaData, error) {
	addr := strings.TrimSpace(controllerAddr)
	if addr == "" {
		return nil, errors.New("ControllerAddress 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeControllerGetMetadataInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Controller getControllerMetaData failed: controller=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeControllerMetaDataHeader(response.ExtFields), nil
}

// GetSyncStateSet 复现官方 getSyncStateSet，先获取 controller leader，再读取指定 broker 的同步副本集合。
func (c *Client) GetSyncStateSet(ctx context.Context, controllerAddr string, brokerNames []string) (*syncStateSetResult, error) {
	addr := strings.TrimSpace(controllerAddr)
	if addr == "" {
		return nil, errors.New("ControllerAddress 必填")
	}
	names := normalizeBrokerNameList(brokerNames)
	if len(names) == 0 {
		return &syncStateSetResult{}, nil
	}
	meta, err := c.GetControllerMetaData(ctx, addr)
	if err != nil {
		return nil, err
	}
	leaderAddr := strings.TrimSpace(meta.ControllerLeaderAddress)
	if leaderAddr == "" {
		return nil, fmt.Errorf("Controller getSyncStateSet failed: controller=%s empty leader address", addr)
	}
	body, err := json.Marshal(names)
	if err != nil {
		return nil, err
	}
	response, err := c.invoke(ctx, leaderAddr, remotingCommand{
		Code:     requestCodeControllerGetSyncStateData,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Body:     body,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Controller getSyncStateSet failed: controller=%s code=%d remark=%s", leaderAddr, response.Code, response.Remark)
	}
	return decodeSyncStateSetBody(response.Body, names)
}

// GetSyncStateSetByCluster 复现官方 getSyncStateSet -c，先从 NameServer 取集群 brokerName，再查询 Controller 同步副本集合。
func (c *Client) GetSyncStateSetByCluster(ctx context.Context, nameServer string, controllerAddr string, clusterName string) (*syncStateSetResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	clusterName = strings.TrimSpace(clusterName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if clusterName == "" {
		return nil, errors.New("ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	return c.GetSyncStateSet(ctx, controllerAddr, brokerNames)
}

// GetControllerConfig 复现官方 getControllerConfig，按 -a 分号列表逐个直连 controller 读取配置。
func (c *Client) GetControllerConfig(ctx context.Context, controllerAddrs string) ([]namesrvConfigSection, error) {
	addrs := splitControllerAddresses(controllerAddrs)
	if len(addrs) == 0 {
		return nil, errors.New("ControllerAddress 必填")
	}
	sections := make([]namesrvConfigSection, 0, len(addrs))
	for _, addr := range addrs {
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:     requestCodeGetControllerConfig,
			Language: "JAVA",
			Version:  0,
			Opaque:   nextOpaque.Add(1),
			Flag:     0,
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("Controller getControllerConfig failed: controller=%s code=%d remark=%s", addr, response.Code, response.Remark)
		}
		entries, err := decodeBrokerConfigBody(response.Body)
		if err != nil {
			return nil, fmt.Errorf("解析 Controller 配置失败: %w", err)
		}
		sections = append(sections, namesrvConfigSection{NameServer: addr, Entries: entries})
	}
	return sections, nil
}

// decodeControllerMetaDataHeader 将 Remoting 自定义 header 转成命令输出所需的 controller 元数据。
func decodeControllerMetaDataHeader(fields map[string]string) *controllerMetaData {
	if fields == nil {
		fields = map[string]string{}
	}
	isLeader, _ := strconv.ParseBool(strings.TrimSpace(fields["isLeader"]))
	return &controllerMetaData{
		Group:                   strings.TrimSpace(fields["group"]),
		ControllerLeaderID:      strings.TrimSpace(fields["controllerLeaderId"]),
		ControllerLeaderAddress: strings.TrimSpace(fields["controllerLeaderAddress"]),
		IsLeader:                isLeader,
		Peers:                   strings.TrimSpace(fields["peers"]),
	}
}

func normalizeBrokerNameList(brokerNames []string) []string {
	seen := make(map[string]bool, len(brokerNames))
	names := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		name := strings.TrimSpace(brokerName)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names
}

func decodeSyncStateSetBody(body []byte, brokerNames []string) (*syncStateSetResult, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return &syncStateSetResult{}, nil
	}
	var payload struct {
		ReplicasInfoTable map[string]struct {
			MasterBrokerID    int64                         `json:"masterBrokerId"`
			MasterAddress     string                        `json:"masterAddress"`
			MasterEpoch       int                           `json:"masterEpoch"`
			SyncStateSetEpoch int                           `json:"syncStateSetEpoch"`
			InSyncReplicas    []syncStateSetReplicaIdentity `json:"inSyncReplicas"`
			NotInSyncReplicas []syncStateSetReplicaIdentity `json:"notInSyncReplicas"`
		} `json:"replicasInfoTable"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	result := &syncStateSetResult{Brokers: make([]syncStateSetBrokerInfo, 0, len(payload.ReplicasInfoTable))}
	used := make(map[string]bool, len(payload.ReplicasInfoTable))
	appendBroker := func(name string) {
		replicas, ok := payload.ReplicasInfoTable[name]
		if !ok || used[name] {
			return
		}
		used[name] = true
		result.Brokers = append(result.Brokers, syncStateSetBrokerInfo{
			BrokerName:        name,
			MasterBrokerID:    replicas.MasterBrokerID,
			MasterAddress:     replicas.MasterAddress,
			MasterEpoch:       replicas.MasterEpoch,
			SyncStateSetEpoch: replicas.SyncStateSetEpoch,
			InSyncReplicas:    replicas.InSyncReplicas,
			NotInSyncReplicas: replicas.NotInSyncReplicas,
		})
	}
	for _, brokerName := range brokerNames {
		appendBroker(brokerName)
	}
	remaining := make([]string, 0, len(payload.ReplicasInfoTable))
	for brokerName := range payload.ReplicasInfoTable {
		if !used[brokerName] {
			remaining = append(remaining, brokerName)
		}
	}
	sort.Strings(remaining)
	for _, brokerName := range remaining {
		appendBroker(brokerName)
	}
	return result, nil
}

func decodeElectMasterBrokerMembers(body []byte) ([]electMasterBrokerMember, bool, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, true, nil
	}
	var payload struct {
		BrokerAddrs map[string]string `json:"brokerAddrs"`
	}
	// 官方 BrokerMemberGroup 使用 fastjson 序列化 Map<Long,String>，数字 key 需要先补成标准 JSON 字符串。
	normalized := normalizeFastJSONNumericKeys(string(body))
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, false, err
	}
	if payload.BrokerAddrs == nil {
		return nil, false, nil
	}
	keys := make([]string, 0, len(payload.BrokerAddrs))
	for key := range payload.BrokerAddrs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i int, j int) bool {
		left, leftErr := strconv.ParseInt(keys[i], 10, 64)
		right, rightErr := strconv.ParseInt(keys[j], 10, 64)
		if leftErr == nil && rightErr == nil {
			return left < right
		}
		return keys[i] < keys[j]
	})
	members := make([]electMasterBrokerMember, 0, len(keys))
	for _, key := range keys {
		brokerID, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, false, err
		}
		members = append(members, electMasterBrokerMember{
			BrokerID:      brokerID,
			BrokerAddress: payload.BrokerAddrs[key],
		})
	}
	return members, true, nil
}

// ResetOffsetByTime 复现官方 resetOffsetByTime：指定队列分支可直写或按 timestamp 搜索 offset，timestamp 分支请求 Broker 计算并写回位点。
func (c *Client) ResetOffsetByTime(ctx context.Context, nameServer string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
	if options.SpecifiedQueue {
		resetOffset := options.ExpectOffset
		if !options.HasExpectOffset {
			var err error
			resetOffset, err = c.searchResetOffsetByTimeAtBroker(ctx, options.BrokerAddr, options.Topic, options.QueueID, options.TimestampMillis)
			if err != nil {
				return nil, err
			}
		}
		entry := consumerProgressEntry{
			Topic:          strings.TrimSpace(options.Topic),
			BrokerName:     strings.TrimSpace(options.BrokerAddr),
			QueueID:        options.QueueID,
			ConsumerOffset: resetOffset,
		}
		if resetOffset > 0 {
			if err := c.updateConsumerOffsetAtBroker(ctx, options.BrokerAddr, options.Group, entry); err != nil {
				return nil, err
			}
		}
		return []skipAccumulatedMessageRow{{
			Queue: messageQueueIdentity{
				Topic:      strings.TrimSpace(options.Topic),
				BrokerName: strings.TrimSpace(options.BrokerAddr),
				QueueID:    options.QueueID,
			},
			Offset: resetOffset,
		}}, nil
	}
	routeTopic := resetOffsetRouteTopic(options.ClusterName, options.Topic)
	routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	rows := make([]skipAccumulatedMessageRow, 0)
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		brokerRows, err := c.resetOffsetByTimeAtBroker(ctx, addr, options)
		if err != nil {
			return nil, err
		}
		rows = append(rows, brokerRows...)
	}
	return rows, nil
}

// SkipAccumulatedMessage 复刻官方 skipAccumulatedMessage：请求 Broker 将消费组位点推进到当前堆积末尾。
func (c *Client) SkipAccumulatedMessage(ctx context.Context, nameServer string, options skipAccumulatedMessageOptions) ([]skipAccumulatedMessageRow, error) {
	routeTopic := resetOffsetRouteTopic(options.ClusterName, options.Topic)
	routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	rows := make([]skipAccumulatedMessageRow, 0)
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		brokerRows, err := c.skipAccumulatedMessageAtBroker(ctx, addr, options)
		if err != nil {
			return nil, err
		}
		rows = append(rows, brokerRows...)
	}
	return rows, nil
}

// resetOffsetRouteTopic 复刻官方 resetOffsetByTimestamp 的路由选择：普通 Topic 查自身，LMQ/timer Topic 使用 -c 指定父 Topic。
func resetOffsetRouteTopic(clusterName string, topic string) string {
	topic = strings.TrimSpace(topic)
	clusterName = strings.TrimSpace(clusterName)
	if clusterName != "" && (isLMQTopic(topic) || topic == "rmq_sys_wheel_timer") {
		return clusterName
	}
	return topic
}

// isLMQTopic 判断 Topic 是否属于 RocketMQ LMQ 命名空间，用于决定 skipAccumulatedMessage 的路由 Topic。
func isLMQTopic(topic string) bool {
	return strings.HasPrefix(strings.TrimSpace(topic), "%LMQ%")
}

// skipAccumulatedMessageAtBroker 向单个 Broker 发送官方 222 请求，并解码 Broker 返回的重置后位点表。
func (c *Client) skipAccumulatedMessageAtBroker(ctx context.Context, brokerAddr string, options skipAccumulatedMessageOptions) ([]skipAccumulatedMessageRow, error) {
	response, err := c.invoke(ctx, strings.TrimSpace(brokerAddr), remotingCommand{
		Code:     requestCodeInvokeBrokerToResetOffset,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"group":     strings.TrimSpace(options.Group),
			"topic":     strings.TrimSpace(options.Topic),
			"timestamp": "-1",
			"isForce":   strconv.FormatBool(options.Force),
			"offset":    "-1",
			"queueId":   "-1",
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker skipAccumulatedMessage failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	if len(response.Body) == 0 {
		return nil, fmt.Errorf("Broker skipAccumulatedMessage response body empty: broker=%s", brokerAddr)
	}
	return decodeResetOffsetBody(response.Body)
}

// resetOffsetByTimeAtBroker 向单个 Broker 发送官方 timestamp reset 请求，并解码 Broker 返回的重置后位点表。
func (c *Client) resetOffsetByTimeAtBroker(ctx context.Context, brokerAddr string, options resetOffsetByTimeOptions) ([]skipAccumulatedMessageRow, error) {
	response, err := c.invoke(ctx, strings.TrimSpace(brokerAddr), remotingCommand{
		Code:     requestCodeInvokeBrokerToResetOffset,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"group":     strings.TrimSpace(options.Group),
			"topic":     strings.TrimSpace(options.Topic),
			"timestamp": strconv.FormatInt(options.TimestampMillis, 10),
			"isForce":   strconv.FormatBool(options.Force),
			"offset":    "-1",
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker resetOffsetByTime failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	if len(response.Body) == 0 {
		return nil, fmt.Errorf("Broker resetOffsetByTime response body empty: broker=%s", brokerAddr)
	}
	return decodeResetOffsetBody(response.Body)
}

// searchResetOffsetByTimeAtBroker 使用官方 resetOffsetByTime 指定队列分支的 deprecated searchOffset overload，只发送 topic、queueId 和 timestamp。
func (c *Client) searchResetOffsetByTimeAtBroker(ctx context.Context, brokerAddr string, topic string, queueID int, timestamp int64) (int64, error) {
	return c.offsetAtBroker(ctx, brokerAddr, requestCodeSearchOffsetByTimestamp, map[string]string{
		"topic":     strings.TrimSpace(topic),
		"queueId":   strconv.Itoa(queueID),
		"timestamp": strconv.FormatInt(timestamp, 10),
	})
}

// BrokerHAStatusByCluster 查询集群内每个 master 的 HA 运行时状态，行为对应官方 haStatus -c。
func (c *Client) BrokerHAStatusByCluster(ctx context.Context, nameServer string, clusterName string) ([]haStatusBrokerResult, error) {
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	clusterNames := clusterInfo.clusterNames(clusterName)
	if len(clusterNames) == 0 {
		return nil, errors.New("clusterName 未返回 Broker")
	}
	rows := make([]haStatusBrokerResult, 0)
	for _, currentClusterName := range clusterNames {
		brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[currentClusterName]...)
		sort.Strings(brokerNames)
		for _, brokerName := range brokerNames {
			brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
			if !ok {
				continue
			}
			addr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
			if addr == "" {
				continue
			}
			status, err := c.BrokerHAStatus(ctx, addr)
			if err != nil {
				return nil, err
			}
			rows = append(rows, haStatusBrokerResult{Addr: addr, Result: status})
		}
	}
	return rows, nil
}

// CheckRocksdbCqWriteProgress 查询集群内 master Broker 的 RocksDB ConsumeQueue 写入检查进度，行为对应官方 checkRocksdbCqWriteProgress。
func (c *Client) CheckRocksdbCqWriteProgress(ctx context.Context, nameServer string, clusterName string, topic string, checkStoreTime int64) ([]checkRocksdbCqWriteProgressRow, error) {
	cluster := strings.TrimSpace(clusterName)
	if cluster == "" {
		return nil, errors.New("ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[cluster]...)
	if len(brokerNames) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回 Broker", cluster)
	}
	sort.Strings(brokerNames)
	rows := make([]checkRocksdbCqWriteProgressRow, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		fields := map[string]string{
			"checkStoreTime": strconv.FormatInt(checkStoreTime, 10),
		}
		if currentTopic := strings.TrimSpace(topic); currentTopic != "" {
			fields["topic"] = currentTopic
		}
		response, err := c.invoke(ctx, masterAddr, remotingCommand{
			Code:      requestCodeCheckRocksdbCqWriteProgress,
			Language:  "JAVA",
			Version:   0,
			Opaque:    nextOpaque.Add(1),
			Flag:      0,
			ExtFields: fields,
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, fmt.Errorf("Broker checkRocksdbCqWriteProgress failed: broker=%s code=%d remark=%s", masterAddr, response.Code, response.Remark)
		}
		var payload struct {
			CheckStatus int    `json:"checkStatus"`
			CheckResult string `json:"checkResult"`
		}
		if err := json.Unmarshal(response.Body, &payload); err != nil {
			return nil, fmt.Errorf("解析 checkRocksdbCqWriteProgress 失败: %w", err)
		}
		outputBrokerName := strings.TrimSpace(brokerData.BrokerName)
		if outputBrokerName == "" {
			outputBrokerName = brokerName
		}
		rows = append(rows, checkRocksdbCqWriteProgressRow{
			BrokerName: outputBrokerName,
			CheckError: payload.CheckStatus == checkRocksdbCqWriteStatusError,
			ErrorInfo:  payload.CheckResult,
		})
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回可查询 Broker", cluster)
	}
	return rows, nil
}

// RocksDBConfigToJson 触发 Broker 将 RocksDB 元数据导出为 JSON，行为对应官方 rocksDBConfigToJson 的 RPC 模式。
func (c *Client) RocksDBConfigToJson(ctx context.Context, nameServer string, brokerAddr string, clusterName string, configTypes []string) error {
	normalizedTypes, err := normalizeRocksDBConfigTypes(configTypes)
	if err != nil {
		return err
	}
	configTypeHeader := formatRocksDBConfigTypes(normalizedTypes)
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	if brokerAddr != "" {
		return c.exportRocksDBConfigToJsonAtBroker(ctx, brokerAddr, configTypeHeader)
	}
	if clusterName == "" {
		return errors.New("ClusterName 或 BrokerAddr 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return errors.New("clusterAddrTable is empty")
	}
	sort.Strings(brokerNames)
	errs := make([]error, len(brokerNames))
	var waitGroup sync.WaitGroup
	for index, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		waitGroup.Add(1)
		go func(index int, addr string) {
			defer waitGroup.Done()
			errs[index] = c.exportRocksDBConfigToJsonAtBroker(ctx, addr, configTypeHeader)
		}(index, masterAddr)
	}
	waitGroup.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// exportRocksDBConfigToJsonAtBroker 发送官方 EXPORT_ROCKSDB_CONFIG_TO_JSON 请求，成功响应不解析 body。
func (c *Client) exportRocksDBConfigToJsonAtBroker(ctx context.Context, brokerAddr string, configTypeHeader string) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCodeExportRocksDBConfigToJson,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: map[string]string{"configType": configTypeHeader},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker rocksDBConfigToJson failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

// ExportPopRecord 导出 Broker POP 消费记录，行为对应官方 exportPopRecord。
func (c *Client) ExportPopRecord(ctx context.Context, nameServer string, brokerAddr string, clusterName string, dryRun bool) ([]exportPopRecordRow, error) {
	brokerAddr = strings.TrimSpace(brokerAddr)
	clusterName = strings.TrimSpace(clusterName)
	if brokerAddr != "" {
		entries, err := c.fetchBrokerConfig(ctx, brokerAddr)
		if err != nil {
			return nil, err
		}
		brokerName := brokerConfigEntryValue(entries, "brokerName")
		if brokerName == "" {
			brokerName = "null"
		}
		return []exportPopRecordRow{c.exportPopRecordAtBroker(ctx, brokerAddr, brokerName, dryRun)}, nil
	}
	if clusterName == "" {
		return nil, nil
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, nil
	}
	sort.Strings(brokerNames)
	rows := make([]exportPopRecordRow, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
			addr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
			if addr == "" {
				continue
			}
			rows = append(rows, c.exportPopRecordAtBroker(ctx, addr, brokerName, dryRun))
		}
	}
	return rows, nil
}

// exportPopRecordAtBroker 复刻官方 export 方法；dryRun=true 时只打印结果，不向 Broker 发请求。
func (c *Client) exportPopRecordAtBroker(ctx context.Context, brokerAddr string, brokerName string, dryRun bool) exportPopRecordRow {
	row := exportPopRecordRow{BrokerName: brokerName, BrokerAddr: brokerAddr, DryRun: dryRun}
	if dryRun {
		return row
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeExportPopRecord,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		row.Err = err
		return row
	}
	if response.Code != responseCodeSuccess {
		row.Err = fmt.Errorf("CODE: %d DESC: %s", response.Code, response.Remark)
	}
	return row
}

// UpdateKvConfig 在全部 NameServer 上创建或更新 KV 配置，行为对应官方 updateKvConfig。
func (c *Client) UpdateKvConfig(ctx context.Context, nameServers string, namespace string, key string, value string) error {
	fields := map[string]string{
		"namespace": strings.TrimSpace(namespace),
		"key":       strings.TrimSpace(key),
		"value":     strings.TrimSpace(value),
	}
	return c.invokeAllNameServers(ctx, nameServers, requestCodePutKVConfig, fields)
}

// GetKvConfig 从 NameServer 读取 KV 配置，行为对应官方 getKVConfigValue。
func (c *Client) GetKvConfig(ctx context.Context, nameServers string, namespace string, key string) (string, error) {
	fields := map[string]string{
		"namespace": strings.TrimSpace(namespace),
		"key":       strings.TrimSpace(key),
	}
	response, err := c.invokeNameServer(ctx, nameServers, remotingCommand{
		Code:      requestCodeGetKVConfig,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return "", err
	}
	if response.Code != responseCodeSuccess {
		return "", fmt.Errorf("NameServer getKvConfig failed: code=%d remark=%s", response.Code, response.Remark)
	}
	return response.ExtFields["value"], nil
}

// DeleteKvConfig 在全部 NameServer 上删除 KV 配置，行为对应官方 deleteKvConfig。
func (c *Client) DeleteKvConfig(ctx context.Context, nameServers string, namespace string, key string) error {
	fields := map[string]string{
		"namespace": strings.TrimSpace(namespace),
		"key":       strings.TrimSpace(key),
	}
	return c.invokeAllNameServers(ctx, nameServers, requestCodeDeleteKVConfig, fields)
}

// UpdateTopicList 批量创建或更新 Topic 配置，行为对应官方 updateTopicList。
func (c *Client) UpdateTopicList(ctx context.Context, nameServer string, options updateTopicListOptions) ([]string, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	if len(options.TopicConfigs) == 0 && strings.TrimSpace(options.FileName) != "" {
		configs, err := readUpdateTopicConfigsFile(options.FileName)
		if err != nil {
			return nil, err
		}
		options.TopicConfigs = configs
	}
	options.TopicConfigs = normalizeUpdateTopicConfigs(options.TopicConfigs)
	if len(options.TopicConfigs) == 0 {
		return nil, nil
	}
	if options.BrokerAddr != "" {
		if err := c.updateTopicListAtBroker(ctx, options.BrokerAddr, options.TopicConfigs); err != nil {
			return nil, err
		}
		return []string{options.BrokerAddr}, nil
	}
	if options.NameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	targets := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.updateTopicListAtBroker(ctx, masterAddr, options.TopicConfigs); err != nil {
			return nil, err
		}
		targets = append(targets, masterAddr)
	}
	return targets, nil
}

// UpdateTopic 创建或更新 Topic 配置，行为对应官方 updateTopic。
func (c *Client) UpdateTopic(ctx context.Context, nameServer string, options updateTopicOptions) (*updateTopicResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	config := options.TopicConfig()
	result := &updateTopicResult{Config: config}
	if options.BrokerAddr != "" {
		if err := c.updateTopicAtBroker(ctx, options.BrokerAddr, config); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, options.BrokerAddr)
		if options.Order {
			brokerName, err := c.brokerNameForOrderConf(ctx, options.NameServer, options.BrokerAddr)
			if err != nil {
				return nil, err
			}
			if err := c.updateOrderConf(ctx, options.NameServer, config.TopicName, fmt.Sprintf("%s:%d", brokerName, config.WriteQueueNums)); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	if options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	orderParts := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.updateTopicAtBroker(ctx, masterAddr, config); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, masterAddr)
		if options.Order {
			orderParts = append(orderParts, fmt.Sprintf("%s:%d", brokerName, config.WriteQueueNums))
		}
	}
	if options.Order {
		if err := c.updateOrderConf(ctx, options.NameServer, config.TopicName, strings.Join(orderParts, ";")); err != nil {
			return nil, err
		}
		result.OrderConf = strings.Join(orderParts, ";")
	}
	return result, nil
}

// UpdateStaticTopic 创建或扩容静态 Topic，流程对齐官方 updateStaticTopic 的非 mapFile 分支。
func (c *Client) UpdateStaticTopic(ctx context.Context, nameServer string, options updateStaticTopicOptions) (*updateStaticTopicResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.Topic = strings.TrimSpace(options.Topic)
	options.BrokerNames = trimStringSlice(options.BrokerNames)
	options.ClusterNames = trimStringSlice(options.ClusterNames)
	if options.NameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.Topic == "" || options.TotalQueueNums <= 0 {
		return nil, errors.New("Topic、TotalQueueNum 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	targetBrokers := resolveStaticTopicTargetBrokers(options, clusterInfo)
	if len(targetBrokers) == 0 {
		return nil, errors.New("Find none brokers, do nothing")
	}
	brokerConfigMap, err := c.fetchStaticTopicConfigAll(ctx, options.Topic, clusterInfo)
	if err != nil {
		return nil, err
	}
	oldEpoch, _, err := staticTopicMaxEpochAndQueueNum(options.Topic, brokerConfigMap, time.Now().UnixMilli(), options.TotalQueueNums)
	if err != nil {
		return nil, err
	}
	oldWrapper := staticTopicRemappingDetailWrapper{
		Topic:           options.Topic,
		Type:            "CREATE_OR_UPDATE",
		Epoch:           oldEpoch,
		BrokerConfigMap: brokerConfigMap,
		BrokerToMapIn:   []string{},
		BrokerToMapOut:  []string{},
	}
	beforeFile, err := writeStaticTopicWrapperTemp(oldWrapper, false)
	if err != nil {
		return nil, err
	}
	for brokerName := range brokerConfigMap {
		targetBrokers[brokerName] = true
	}
	newWrapper, err := buildStaticTopicCreateOrUpdateWrapper(options.Topic, options.TotalQueueNums, targetBrokers, brokerConfigMap)
	if err != nil {
		return nil, err
	}
	afterFile, err := writeStaticTopicWrapperTemp(newWrapper, true)
	if err != nil {
		return nil, err
	}
	completeStaticTopicNoTargetBrokers(newWrapper.BrokerConfigMap, clusterInfo)
	if err := c.updateStaticTopicConfigMappingAll(ctx, newWrapper.BrokerConfigMap, clusterInfo, options.ForceReplace); err != nil {
		return nil, err
	}
	return &updateStaticTopicResult{BeforeFile: beforeFile, AfterFile: afterFile}, nil
}

// RemappingStaticTopic 重新分配已有静态 Topic 的逻辑队列归属，流程对齐官方 remappingStaticTopic 的非 mapFile 分支。
func (c *Client) RemappingStaticTopic(ctx context.Context, nameServer string, options remappingStaticTopicOptions) (*remappingStaticTopicResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.Topic = strings.TrimSpace(options.Topic)
	options.BrokerNames = trimStringSlice(options.BrokerNames)
	options.ClusterNames = trimStringSlice(options.ClusterNames)
	if options.NameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.Topic == "" {
		return nil, errors.New("Topic 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	targetBrokers := resolveStaticTopicTargetBrokers(updateStaticTopicOptions{
		BrokerNames:  options.BrokerNames,
		ClusterNames: options.ClusterNames,
	}, clusterInfo)
	if len(targetBrokers) == 0 {
		return nil, errors.New("Find none brokers, do nothing")
	}
	for brokerName := range targetBrokers {
		if clusterInfo.BrokerAddrTable[brokerName].selectAddr() == "" {
			return nil, fmt.Errorf("Can't find addr for broker %s", brokerName)
		}
	}
	brokerConfigMap, err := c.fetchStaticTopicConfigAll(ctx, options.Topic, clusterInfo)
	if err != nil {
		return nil, err
	}
	if len(brokerConfigMap) == 0 {
		return nil, errors.New("No topic route to do the remapping")
	}
	oldEpoch, _, err := staticTopicMaxEpochAndQueueNum(options.Topic, brokerConfigMap, 0, 0)
	if err != nil {
		return nil, err
	}
	oldWrapper := staticTopicRemappingDetailWrapper{
		Topic:           options.Topic,
		Type:            "CREATE_OR_UPDATE",
		Epoch:           oldEpoch,
		BrokerConfigMap: brokerConfigMap,
		BrokerToMapIn:   []string{},
		BrokerToMapOut:  []string{},
	}
	beforeFile, err := writeStaticTopicWrapperTemp(oldWrapper, false)
	if err != nil {
		return nil, err
	}
	newWrapper, err := buildStaticTopicRemappingWrapper(options.Topic, targetBrokers, brokerConfigMap)
	if err != nil {
		return nil, err
	}
	afterFile, err := writeStaticTopicWrapperTemp(newWrapper, true)
	if err != nil {
		return nil, err
	}
	completeStaticTopicNoTargetBrokers(newWrapper.BrokerConfigMap, clusterInfo)
	if err := c.updateStaticTopicConfigMappingAll(ctx, newWrapper.BrokerConfigMap, clusterInfo, false); err != nil {
		return nil, err
	}
	return &remappingStaticTopicResult{BeforeFile: beforeFile, AfterFile: afterFile}, nil
}

func resolveStaticTopicTargetBrokers(options updateStaticTopicOptions, clusterInfo brokerClusterInfo) map[string]bool {
	targetBrokers := make(map[string]bool)
	for _, brokerName := range options.BrokerNames {
		if brokerName != "" {
			targetBrokers[brokerName] = true
		}
	}
	for _, clusterName := range options.ClusterNames {
		for _, brokerName := range clusterInfo.ClusterAddrTable[clusterName] {
			if brokerName != "" {
				targetBrokers[brokerName] = true
			}
		}
	}
	return targetBrokers
}

func (c *Client) fetchStaticTopicConfigAll(ctx context.Context, topic string, clusterInfo brokerClusterInfo) (map[string]staticTopicConfigAndQueueMapping, error) {
	configs := make(map[string]staticTopicConfigAndQueueMapping)
	for _, brokerName := range sortedBrokerNames(clusterInfo.BrokerAddrTable) {
		brokerData := clusterInfo.BrokerAddrTable[brokerName]
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		config, ok, err := c.fetchStaticTopicConfig(ctx, masterAddr, topic)
		if err != nil {
			return nil, err
		}
		if ok {
			normalizeStaticTopicConfig(&config, topic, brokerName)
			configs[brokerName] = config
		}
	}
	return configs, nil
}

func (c *Client) fetchStaticTopicConfig(ctx context.Context, brokerAddr string, topic string) (staticTopicConfigAndQueueMapping, bool, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeGetTopicConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"topic": strings.TrimSpace(topic),
			"lo":    "true",
		},
	})
	if err != nil {
		return staticTopicConfigAndQueueMapping{}, false, err
	}
	if response.Code == responseCodeTopicNotExist {
		return staticTopicConfigAndQueueMapping{}, false, nil
	}
	if response.Code != responseCodeSuccess {
		return staticTopicConfigAndQueueMapping{}, false, fmt.Errorf("Broker getTopicConfig failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	if len(response.Body) == 0 {
		return staticTopicConfigAndQueueMapping{}, false, nil
	}
	config, err := decodeStaticTopicConfigAndQueueMapping(response.Body)
	if err != nil {
		return staticTopicConfigAndQueueMapping{}, false, err
	}
	return config, true, nil
}

func decodeStaticTopicConfigAndQueueMapping(body []byte) (staticTopicConfigAndQueueMapping, error) {
	var config staticTopicConfigAndQueueMapping
	normalized := normalizeFastJSONNumericKeys(string(body))
	if err := json.Unmarshal([]byte(normalized), &config); err != nil {
		return staticTopicConfigAndQueueMapping{}, fmt.Errorf("解析静态 Topic 配置失败: %w", err)
	}
	return config, nil
}

func normalizeStaticTopicConfig(config *staticTopicConfigAndQueueMapping, topic string, brokerName string) {
	config.TopicName = strings.TrimSpace(config.TopicName)
	if config.TopicName == "" {
		config.TopicName = topic
	}
	if config.Perm == 0 {
		config.Perm = 6
	}
	config.TopicFilterType = strings.TrimSpace(config.TopicFilterType)
	if config.TopicFilterType == "" {
		config.TopicFilterType = "SINGLE_TAG"
	}
	if config.Attributes == nil {
		config.Attributes = map[string]string{}
	}
	if config.MappingDetail != nil {
		normalizeStaticTopicMappingDetail(config.MappingDetail, config.TopicName, brokerName)
	}
}

func normalizeStaticTopicMappingDetail(detail *staticTopicQueueMappingDetail, topic string, brokerName string) {
	if detail.HostedQueues == nil {
		detail.HostedQueues = map[string][]staticLogicQueueMappingItem{}
	}
	detail.Scope = strings.TrimSpace(detail.Scope)
	if detail.Scope == "" {
		detail.Scope = "__global__"
	}
	detail.Topic = strings.TrimSpace(detail.Topic)
	if detail.Topic == "" {
		detail.Topic = topic
	}
	detail.BName = strings.TrimSpace(detail.BName)
	if detail.BName == "" {
		detail.BName = brokerName
	}
	if detail.CurrIDMap == nil {
		detail.CurrIDMap = map[string]int{}
	}
}

func staticTopicMaxEpochAndQueueNum(topic string, brokerConfigMap map[string]staticTopicConfigAndQueueMapping, fallbackEpoch int64, fallbackQueueNum int) (int64, int, error) {
	if len(brokerConfigMap) == 0 {
		return fallbackEpoch, fallbackQueueNum, nil
	}
	maxEpoch := int64(-1)
	maxQueueNum := -1
	scope := ""
	for brokerName, config := range brokerConfigMap {
		if config.MappingDetail == nil {
			return 0, 0, fmt.Errorf("Mapping info should not be null in broker %s", brokerName)
		}
		detail := config.MappingDetail
		if brokerName != detail.BName {
			return 0, 0, fmt.Errorf("The broker name is not equal %s != %s ", brokerName, detail.BName)
		}
		if detail.Dirty {
			return 0, 0, fmt.Errorf("The mapping info is dirty in broker  %s", brokerName)
		}
		if config.TopicName != detail.Topic {
			return 0, 0, fmt.Errorf("The topic name is inconsistent in broker  %s", brokerName)
		}
		if topic != "" && topic != detail.Topic {
			return 0, 0, fmt.Errorf("The topic name is not match for broker  %s", brokerName)
		}
		if scope != "" && scope != detail.Scope {
			return 0, 0, fmt.Errorf("scope does not match %s != %s in %s", detail.Scope, scope, brokerName)
		}
		if scope == "" {
			scope = detail.Scope
		}
		if maxEpoch != -1 && maxEpoch != detail.Epoch {
			return 0, 0, fmt.Errorf("epoch does not match %d != %d in %s", maxEpoch, detail.Epoch, detail.BName)
		}
		maxEpoch = detail.Epoch
		if maxQueueNum != -1 && maxQueueNum != detail.TotalQueues {
			return 0, 0, fmt.Errorf("total queue number does not match %d != %d in %s", maxQueueNum, detail.TotalQueues, detail.BName)
		}
		maxQueueNum = detail.TotalQueues
	}
	return maxEpoch, maxQueueNum, nil
}

func buildStaticTopicCreateOrUpdateWrapper(topic string, queueNum int, targetBrokers map[string]bool, brokerConfigMap map[string]staticTopicConfigAndQueueMapping) (staticTopicRemappingDetailWrapper, error) {
	if err := checkStaticTopicTargetBrokersComplete(targetBrokers, brokerConfigMap); err != nil {
		return staticTopicRemappingDetailWrapper{}, err
	}
	maxEpoch, _, err := staticTopicMaxEpochAndQueueNum(topic, brokerConfigMap, time.Now().UnixMilli(), queueNum)
	if err != nil {
		return staticTopicRemappingDetailWrapper{}, err
	}
	globalIdMap, err := buildStaticTopicGlobalIDMap(brokerConfigMap)
	if err != nil {
		return staticTopicRemappingDetailWrapper{}, err
	}
	if queueNum < len(globalIdMap) {
		return staticTopicRemappingDetailWrapper{}, fmt.Errorf("Cannot decrease the queue num for static topic %d < %d", queueNum, len(globalIdMap))
	}
	if queueNum == len(globalIdMap) {
		return staticTopicRemappingDetailWrapper{}, errors.New("The topic queue num is equal the existed queue num, do nothing")
	}
	brokerNumMap := make(map[string]int, len(targetBrokers)+len(globalIdMap))
	for brokerName := range targetBrokers {
		brokerNumMap[brokerName] = 0
	}
	oldIDToBroker := make(map[int]string, len(globalIdMap))
	for queueID, brokerName := range globalIdMap {
		oldIDToBroker[queueID] = brokerName
		brokerNumMap[brokerName]++
	}
	allocator := newStaticTopicMappingAllocator(oldIDToBroker, brokerNumMap)
	allocator.upToNum(queueNum)
	newEpoch := maxInt64(maxEpoch+1000, time.Now().UnixMilli())
	for _, queueID := range sortedIntMapKeys(allocator.idToBroker) {
		if _, exists := globalIdMap[queueID]; exists {
			continue
		}
		brokerName := allocator.idToBroker[queueID]
		config, exists := brokerConfigMap[brokerName]
		if !exists {
			config = newStaticTopicConfig(topic, brokerName, 0, newEpoch)
			config.ReadQueueNums = 1
			config.WriteQueueNums = 1
		} else {
			config.ReadQueueNums++
			config.WriteQueueNums++
			if config.MappingDetail == nil {
				detail := newStaticTopicMappingDetail(topic, brokerName, 0, newEpoch)
				config.MappingDetail = &detail
			}
		}
		normalizeStaticTopicConfig(&config, topic, brokerName)
		config.MappingDetail.HostedQueues[strconv.Itoa(queueID)] = []staticLogicQueueMappingItem{{
			Gen:         0,
			QueueID:     config.WriteQueueNums - 1,
			BName:       brokerName,
			LogicOffset: 0,
			StartOffset: 0,
			EndOffset:   -1,
			TimeOfStart: -1,
			TimeOfEnd:   -1,
		}}
		brokerConfigMap[brokerName] = config
	}
	for brokerName, config := range brokerConfigMap {
		normalizeStaticTopicConfig(&config, topic, brokerName)
		if config.MappingDetail == nil {
			detail := newStaticTopicMappingDetail(topic, brokerName, queueNum, newEpoch)
			config.MappingDetail = &detail
		}
		config.MappingDetail.Epoch = newEpoch
		config.MappingDetail.TotalQueues = queueNum
		brokerConfigMap[brokerName] = config
	}
	return staticTopicRemappingDetailWrapper{
		Topic:           topic,
		Type:            "CREATE_OR_UPDATE",
		Epoch:           newEpoch,
		BrokerConfigMap: brokerConfigMap,
		BrokerToMapIn:   []string{},
		BrokerToMapOut:  []string{},
	}, nil
}

type staticTopicGlobalQueue struct {
	// BrokerName 是当前逻辑队列 leader 所在 Broker。
	BrokerName string
	// Items 是该逻辑队列的完整代际映射，remap 时会追加新一代。
	Items []staticLogicQueueMappingItem
}

func buildStaticTopicRemappingWrapper(topic string, targetBrokers map[string]bool, brokerConfigMap map[string]staticTopicConfigAndQueueMapping) (staticTopicRemappingDetailWrapper, error) {
	maxEpoch, totalQueues, err := staticTopicMaxEpochAndQueueNum(topic, brokerConfigMap, 0, 0)
	if err != nil {
		return staticTopicRemappingDetailWrapper{}, err
	}
	globalQueues, err := buildStaticTopicGlobalQueues(brokerConfigMap)
	if err != nil {
		return staticTopicRemappingDetailWrapper{}, err
	}
	brokerNumMap := make(map[string]int, len(targetBrokers))
	for brokerName := range targetBrokers {
		brokerNumMap[brokerName] = 0
	}
	beforeCounts := make(map[string]int)
	for _, queue := range globalQueues {
		beforeCounts[queue.BrokerName]++
	}
	allocator := newStaticTopicRemappingAllocator(brokerNumMap, beforeCounts)
	allocator.upToNum(totalQueues)
	expectedBrokerNumMap := allocator.brokerNumMap
	waitAssignQueues := make([]int, 0, len(globalQueues))
	expectedIDToBroker := make(map[int]string, len(globalQueues))
	for _, queueID := range sortedStaticTopicGlobalQueueKeys(globalQueues) {
		leaderBroker := globalQueues[queueID].BrokerName
		if expectedCount, exists := expectedBrokerNumMap[leaderBroker]; exists {
			if expectedCount > 0 {
				expectedIDToBroker[queueID] = leaderBroker
				expectedBrokerNumMap[leaderBroker] = expectedCount - 1
				continue
			}
			waitAssignQueues = append(waitAssignQueues, queueID)
			delete(expectedBrokerNumMap, leaderBroker)
			continue
		}
		waitAssignQueues = append(waitAssignQueues, queueID)
	}
	waitIndex := 0
	for _, brokerName := range sortedStringIntMapKeys(expectedBrokerNumMap) {
		queueCount := expectedBrokerNumMap[brokerName]
		for index := 0; index < queueCount; index++ {
			if waitIndex >= len(waitAssignQueues) {
				return staticTopicRemappingDetailWrapper{}, errors.New("remappingStaticTopic queue assignment is incomplete")
			}
			expectedIDToBroker[waitAssignQueues[waitIndex]] = brokerName
			waitIndex++
		}
	}
	newEpoch := maxInt64(maxEpoch+1000, time.Now().UnixMilli())
	brokersToMapIn := map[string]bool{}
	brokersToMapOut := map[string]bool{}
	for _, queueID := range sortedIntMapKeys(expectedIDToBroker) {
		targetBroker := expectedIDToBroker[queueID]
		currentQueue := globalQueues[queueID]
		if currentQueue.BrokerName == targetBroker {
			continue
		}
		brokersToMapIn[targetBroker] = true
		brokersToMapOut[currentQueue.BrokerName] = true
		mapInConfig := brokerConfigMap[targetBroker]
		if mapInConfig.TopicName == "" {
			mapInConfig = newStaticTopicConfig(topic, targetBroker, totalQueues, newEpoch)
		}
		normalizeStaticTopicConfig(&mapInConfig, topic, targetBroker)
		mapInConfig.ReadQueueNums++
		mapInConfig.WriteQueueNums++
		items := append([]staticLogicQueueMappingItem(nil), currentQueue.Items...)
		last := items[len(items)-1]
		items = append(items, staticLogicQueueMappingItem{
			Gen:         last.Gen + 1,
			QueueID:     mapInConfig.WriteQueueNums - 1,
			BName:       targetBroker,
			LogicOffset: -1,
			StartOffset: 0,
			EndOffset:   -1,
			TimeOfStart: -1,
			TimeOfEnd:   -1,
		})
		mapInConfig.MappingDetail.HostedQueues[strconv.Itoa(queueID)] = items
		brokerConfigMap[targetBroker] = mapInConfig

		mapOutConfig := brokerConfigMap[currentQueue.BrokerName]
		normalizeStaticTopicConfig(&mapOutConfig, topic, currentQueue.BrokerName)
		mapOutConfig.MappingDetail.HostedQueues[strconv.Itoa(queueID)] = items
		brokerConfigMap[currentQueue.BrokerName] = mapOutConfig
	}
	for brokerName, config := range brokerConfigMap {
		normalizeStaticTopicConfig(&config, topic, brokerName)
		config.MappingDetail.Epoch = newEpoch
		config.MappingDetail.TotalQueues = totalQueues
		brokerConfigMap[brokerName] = config
	}
	return staticTopicRemappingDetailWrapper{
		Topic:           topic,
		Type:            "REMAPPING",
		Epoch:           newEpoch,
		BrokerConfigMap: brokerConfigMap,
		BrokerToMapIn:   sortedBoolKeys(brokersToMapIn),
		BrokerToMapOut:  sortedBoolKeys(brokersToMapOut),
	}, nil
}

func buildStaticTopicGlobalQueues(brokerConfigMap map[string]staticTopicConfigAndQueueMapping) (map[int]staticTopicGlobalQueue, error) {
	globalQueues := make(map[int]staticTopicGlobalQueue)
	physicalQueueMap := make(map[string]int)
	for brokerName, config := range brokerConfigMap {
		if config.MappingDetail == nil {
			return nil, fmt.Errorf("Mapping info should not be null in broker %s", brokerName)
		}
		for rawQueueID, items := range config.MappingDetail.HostedQueues {
			queueID, err := strconv.Atoi(rawQueueID)
			if err != nil {
				return nil, fmt.Errorf("解析静态 Topic 逻辑队列 %q 失败: %w", rawQueueID, err)
			}
			if len(items) == 0 {
				continue
			}
			leader := items[len(items)-1]
			if leader.BName != config.MappingDetail.BName {
				continue
			}
			if _, exists := globalQueues[queueID]; exists {
				return nil, fmt.Errorf("The queue id is duplicated in broker %s %s", leader.BName, config.MappingDetail.BName)
			}
			physicalKey := fmt.Sprintf("%s-%d", leader.BName, leader.QueueID)
			if reusedQueueID, exists := physicalQueueMap[physicalKey]; exists {
				return nil, fmt.Errorf("Topic global queue id %d and %d shared the same physical queue %s", queueID, reusedQueueID, physicalKey)
			}
			physicalQueueMap[physicalKey] = queueID
			globalQueues[queueID] = staticTopicGlobalQueue{
				BrokerName: leader.BName,
				Items:      append([]staticLogicQueueMappingItem(nil), items...),
			}
		}
	}
	return globalQueues, nil
}

type staticTopicRemappingAllocator struct {
	// brokerNumMap 保存 remap 后每个目标 Broker 期望承载的逻辑队列数量。
	brokerNumMap map[string]int
	// brokerNumMapBeforeRemapping 用于对齐官方“优先减少迁移”的排序依据。
	brokerNumMapBeforeRemapping map[string]int
	leastBrokers                []string
	currentIndex                int
}

func newStaticTopicRemappingAllocator(brokerNumMap map[string]int, brokerNumMapBeforeRemapping map[string]int) *staticTopicRemappingAllocator {
	allocator := &staticTopicRemappingAllocator{
		brokerNumMap:                make(map[string]int, len(brokerNumMap)),
		brokerNumMapBeforeRemapping: make(map[string]int, len(brokerNumMapBeforeRemapping)),
		leastBrokers:                []string{},
		currentIndex:                0,
	}
	for brokerName, queueCount := range brokerNumMap {
		allocator.brokerNumMap[brokerName] = queueCount
	}
	for brokerName, queueCount := range brokerNumMapBeforeRemapping {
		allocator.brokerNumMapBeforeRemapping[brokerName] = queueCount
	}
	return allocator
}

func (allocator *staticTopicRemappingAllocator) freshState() {
	minNum := int(^uint(0) >> 1)
	allocator.leastBrokers = allocator.leastBrokers[:0]
	for brokerName, queueCount := range allocator.brokerNumMap {
		if queueCount < minNum {
			allocator.leastBrokers = allocator.leastBrokers[:0]
			allocator.leastBrokers = append(allocator.leastBrokers, brokerName)
			minNum = queueCount
			continue
		}
		if queueCount == minNum {
			allocator.leastBrokers = append(allocator.leastBrokers, brokerName)
		}
	}
	sort.SliceStable(allocator.leastBrokers, func(left, right int) bool {
		leftCount := allocator.brokerNumMapBeforeRemapping[allocator.leastBrokers[left]]
		rightCount := allocator.brokerNumMapBeforeRemapping[allocator.leastBrokers[right]]
		if leftCount == rightCount {
			return allocator.leastBrokers[left] < allocator.leastBrokers[right]
		}
		return leftCount < rightCount
	})
	allocator.currentIndex = len(allocator.leastBrokers) - 1
}

func (allocator *staticTopicRemappingAllocator) nextBroker() string {
	if len(allocator.leastBrokers) == 0 {
		allocator.freshState()
	}
	if len(allocator.leastBrokers) == 0 {
		return ""
	}
	index := allocator.currentIndex % len(allocator.leastBrokers)
	brokerName := allocator.leastBrokers[index]
	allocator.leastBrokers = append(allocator.leastBrokers[:index], allocator.leastBrokers[index+1:]...)
	return brokerName
}

func (allocator *staticTopicRemappingAllocator) upToNum(maxQueueNum int) {
	for index := 0; index < maxQueueNum; index++ {
		brokerName := allocator.nextBroker()
		if brokerName == "" {
			return
		}
		allocator.brokerNumMap[brokerName]++
	}
}

func checkStaticTopicTargetBrokersComplete(targetBrokers map[string]bool, brokerConfigMap map[string]staticTopicConfigAndQueueMapping) error {
	for brokerName, config := range brokerConfigMap {
		if config.MappingDetail == nil || len(config.MappingDetail.HostedQueues) == 0 {
			continue
		}
		if !targetBrokers[brokerName] {
			return fmt.Errorf("The existed broker %s does not in target brokers ", brokerName)
		}
	}
	return nil
}

func buildStaticTopicGlobalIDMap(brokerConfigMap map[string]staticTopicConfigAndQueueMapping) (map[int]string, error) {
	globalIDMap := make(map[int]string)
	physicalQueueMap := make(map[string]int)
	for brokerName, config := range brokerConfigMap {
		if config.MappingDetail == nil {
			return nil, fmt.Errorf("Mapping info should not be null in broker %s", brokerName)
		}
		for rawQueueID, items := range config.MappingDetail.HostedQueues {
			queueID, err := strconv.Atoi(rawQueueID)
			if err != nil {
				return nil, fmt.Errorf("解析静态 Topic 逻辑队列 %q 失败: %w", rawQueueID, err)
			}
			if len(items) == 0 {
				continue
			}
			leader := items[len(items)-1]
			if leader.BName != config.MappingDetail.BName {
				continue
			}
			if _, exists := globalIDMap[queueID]; exists {
				return nil, fmt.Errorf("The queue id is duplicated in broker %s %s", leader.BName, config.MappingDetail.BName)
			}
			physicalKey := fmt.Sprintf("%s-%d", leader.BName, leader.QueueID)
			if reusedQueueID, exists := physicalQueueMap[physicalKey]; exists {
				return nil, fmt.Errorf("Topic global queue id %d and %d shared the same physical queue %s", queueID, reusedQueueID, physicalKey)
			}
			physicalQueueMap[physicalKey] = queueID
			globalIDMap[queueID] = leader.BName
		}
	}
	return globalIDMap, nil
}

type staticTopicMappingAllocator struct {
	idToBroker   map[int]string
	brokerNumMap map[string]int
	leastBrokers []string
	currentIndex int
}

func newStaticTopicMappingAllocator(idToBroker map[int]string, brokerNumMap map[string]int) *staticTopicMappingAllocator {
	allocator := &staticTopicMappingAllocator{
		idToBroker:   make(map[int]string, len(idToBroker)),
		brokerNumMap: make(map[string]int, len(brokerNumMap)),
	}
	for key, value := range idToBroker {
		allocator.idToBroker[key] = value
	}
	for key, value := range brokerNumMap {
		allocator.brokerNumMap[key] = value
	}
	return allocator
}

func (allocator *staticTopicMappingAllocator) freshState() {
	minNum := int(^uint(0) >> 1)
	allocator.leastBrokers = allocator.leastBrokers[:0]
	for brokerName, queueCount := range allocator.brokerNumMap {
		if queueCount < minNum {
			allocator.leastBrokers = allocator.leastBrokers[:0]
			allocator.leastBrokers = append(allocator.leastBrokers, brokerName)
			minNum = queueCount
			continue
		}
		if queueCount == minNum {
			allocator.leastBrokers = append(allocator.leastBrokers, brokerName)
		}
	}
	sort.Strings(allocator.leastBrokers)
	allocator.currentIndex = len(allocator.leastBrokers) - 1
}

func (allocator *staticTopicMappingAllocator) nextBroker() string {
	if len(allocator.leastBrokers) == 0 {
		allocator.freshState()
	}
	if len(allocator.leastBrokers) == 0 {
		return ""
	}
	index := allocator.currentIndex % len(allocator.leastBrokers)
	brokerName := allocator.leastBrokers[index]
	allocator.leastBrokers = append(allocator.leastBrokers[:index], allocator.leastBrokers[index+1:]...)
	return brokerName
}

func (allocator *staticTopicMappingAllocator) upToNum(maxQueueNum int) {
	currentSize := len(allocator.idToBroker)
	for queueID := currentSize; queueID < maxQueueNum; queueID++ {
		brokerName := allocator.nextBroker()
		if brokerName == "" {
			return
		}
		allocator.brokerNumMap[brokerName]++
		allocator.idToBroker[queueID] = brokerName
	}
}

func completeStaticTopicNoTargetBrokers(brokerConfigMap map[string]staticTopicConfigAndQueueMapping, clusterInfo brokerClusterInfo) {
	if len(brokerConfigMap) == 0 {
		return
	}
	topic := ""
	queueNum := 0
	epoch := int64(0)
	for brokerName, config := range brokerConfigMap {
		normalizeStaticTopicConfig(&config, "", brokerName)
		if config.MappingDetail == nil {
			continue
		}
		topic = config.TopicName
		queueNum = config.MappingDetail.TotalQueues
		epoch = config.MappingDetail.Epoch
		break
	}
	if topic == "" {
		return
	}
	for _, brokerName := range staticTopicSameClusterBrokers(clusterInfo, brokerConfigMap) {
		if _, exists := brokerConfigMap[brokerName]; exists {
			continue
		}
		brokerConfigMap[brokerName] = newStaticTopicConfig(topic, brokerName, queueNum, epoch)
	}
}

func staticTopicSameClusterBrokers(clusterInfo brokerClusterInfo, brokerConfigMap map[string]staticTopicConfigAndQueueMapping) []string {
	brokers := make(map[string]bool)
	for brokerName := range brokerConfigMap {
		clusterName := clusterInfo.clusterNameForBroker(brokerName)
		for _, clusterBroker := range clusterInfo.ClusterAddrTable[clusterName] {
			if clusterBroker != "" {
				brokers[clusterBroker] = true
			}
		}
	}
	return sortedBoolKeys(brokers)
}

func (c *Client) updateStaticTopicConfigMappingAll(ctx context.Context, brokerConfigMap map[string]staticTopicConfigAndQueueMapping, clusterInfo brokerClusterInfo, force bool) error {
	for _, brokerName := range sortedStaticTopicBrokerConfigKeys(brokerConfigMap) {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			return fmt.Errorf("Can't find addr for broker %s", brokerName)
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			return fmt.Errorf("Can't find addr for broker %s", brokerName)
		}
		config := brokerConfigMap[brokerName]
		if err := c.updateStaticTopicAtBroker(ctx, masterAddr, config, force); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) updateStaticTopicAtBroker(ctx context.Context, brokerAddr string, config staticTopicConfigAndQueueMapping, force bool) error {
	if config.MappingDetail == nil {
		return fmt.Errorf("静态 Topic %s 缺少 mappingDetail", config.TopicName)
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateAndCreateStaticTopic,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"topic":           strings.TrimSpace(config.TopicName),
			"defaultTopic":    defaultCreateTopicKey,
			"readQueueNums":   strconv.Itoa(config.ReadQueueNums),
			"writeQueueNums":  strconv.Itoa(config.WriteQueueNums),
			"perm":            strconv.Itoa(config.Perm),
			"topicFilterType": config.TopicFilterType,
			"topicSysFlag":    strconv.Itoa(config.TopicSysFlag),
			"order":           strconv.FormatBool(config.Order),
			"force":           strconv.FormatBool(force),
		},
		Body: []byte(formatStaticTopicMappingDetailJSON(*config.MappingDetail)),
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateStaticTopic failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func writeStaticTopicWrapperTemp(wrapper staticTopicRemappingDetailWrapper, after bool) (string, error) {
	suffix := ".before"
	if after {
		suffix = ".after"
	}
	fileName := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d%s", wrapper.Topic, wrapper.Epoch, suffix))
	if err := os.WriteFile(fileName, []byte(formatStaticTopicWrapperJSON(wrapper)), 0o644); err != nil {
		return "", err
	}
	return fileName, nil
}

func newStaticTopicConfig(topic string, brokerName string, totalQueues int, epoch int64) staticTopicConfigAndQueueMapping {
	detail := newStaticTopicMappingDetail(topic, brokerName, totalQueues, epoch)
	return staticTopicConfigAndQueueMapping{
		TopicName:       topic,
		ReadQueueNums:   0,
		WriteQueueNums:  0,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    0,
		Order:           false,
		Attributes:      map[string]string{},
		MappingDetail:   &detail,
	}
}

func newStaticTopicMappingDetail(topic string, brokerName string, totalQueues int, epoch int64) staticTopicQueueMappingDetail {
	return staticTopicQueueMappingDetail{
		HostedQueues: map[string][]staticLogicQueueMappingItem{},
		Scope:        "__global__",
		Topic:        topic,
		TotalQueues:  totalQueues,
		BName:        brokerName,
		Epoch:        epoch,
		Dirty:        false,
		CurrIDMap:    map[string]int{},
	}
}

func formatStaticTopicWrapperJSON(wrapper staticTopicRemappingDetailWrapper) string {
	var builder strings.Builder
	builder.WriteByte('{')
	builder.WriteString(`"brokerConfigMap":`)
	builder.WriteString(formatStaticTopicBrokerConfigMapJSON(wrapper.BrokerConfigMap))
	builder.WriteString(`,"brokerToMapIn":`)
	builder.WriteString(formatStringArrayJSON(wrapper.BrokerToMapIn))
	builder.WriteString(`,"brokerToMapOut":`)
	builder.WriteString(formatStringArrayJSON(wrapper.BrokerToMapOut))
	builder.WriteString(`,"epoch":`)
	builder.WriteString(strconv.FormatInt(wrapper.Epoch, 10))
	builder.WriteString(`,"topic":`)
	builder.WriteString(jsonString(wrapper.Topic))
	builder.WriteString(`,"type":`)
	builder.WriteString(jsonString(wrapper.Type))
	builder.WriteByte('}')
	return builder.String()
}

func formatStaticTopicBrokerConfigMapJSON(configs map[string]staticTopicConfigAndQueueMapping) string {
	if len(configs) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteByte('{')
	keys := sortedStaticTopicBrokerConfigKeys(configs)
	for index, brokerName := range keys {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(jsonString(brokerName))
		builder.WriteByte(':')
		builder.WriteString(formatStaticTopicConfigJSON(configs[brokerName]))
	}
	builder.WriteByte('}')
	return builder.String()
}

func formatStaticTopicConfigJSON(config staticTopicConfigAndQueueMapping) string {
	var builder strings.Builder
	builder.WriteByte('{')
	builder.WriteString(`"attributes":`)
	builder.WriteString(formatStringMapJSON(config.Attributes))
	builder.WriteString(`,"mappingDetail":`)
	if config.MappingDetail == nil {
		builder.WriteString("null")
	} else {
		builder.WriteString(formatStaticTopicMappingDetailJSON(*config.MappingDetail))
	}
	builder.WriteString(`,"order":`)
	builder.WriteString(strconv.FormatBool(config.Order))
	builder.WriteString(`,"perm":`)
	builder.WriteString(strconv.Itoa(config.Perm))
	builder.WriteString(`,"readQueueNums":`)
	builder.WriteString(strconv.Itoa(config.ReadQueueNums))
	builder.WriteString(`,"topicFilterType":`)
	builder.WriteString(jsonString(config.TopicFilterType))
	builder.WriteString(`,"topicName":`)
	builder.WriteString(jsonString(config.TopicName))
	builder.WriteString(`,"topicSysFlag":`)
	builder.WriteString(strconv.Itoa(config.TopicSysFlag))
	builder.WriteString(`,"writeQueueNums":`)
	builder.WriteString(strconv.Itoa(config.WriteQueueNums))
	builder.WriteByte('}')
	return builder.String()
}

func formatStaticTopicMappingDetailJSON(detail staticTopicQueueMappingDetail) string {
	var builder strings.Builder
	builder.WriteByte('{')
	builder.WriteString(`"bname":`)
	builder.WriteString(jsonString(detail.BName))
	builder.WriteString(`,"currIdMap":`)
	builder.WriteString(formatNumericIntMapJSON(detail.CurrIDMap))
	builder.WriteString(`,"dirty":`)
	builder.WriteString(strconv.FormatBool(detail.Dirty))
	builder.WriteString(`,"epoch":`)
	builder.WriteString(strconv.FormatInt(detail.Epoch, 10))
	builder.WriteString(`,"hostedQueues":`)
	builder.WriteString(formatStaticTopicHostedQueuesJSON(detail.HostedQueues))
	builder.WriteString(`,"scope":`)
	builder.WriteString(jsonString(detail.Scope))
	builder.WriteString(`,"topic":`)
	builder.WriteString(jsonString(detail.Topic))
	builder.WriteString(`,"totalQueues":`)
	builder.WriteString(strconv.Itoa(detail.TotalQueues))
	builder.WriteByte('}')
	return builder.String()
}

func formatStaticTopicHostedQueuesJSON(queues map[string][]staticLogicQueueMappingItem) string {
	if len(queues) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteByte('{')
	keys := sortedNumericStringKeys(queues)
	for index, key := range keys {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key)
		builder.WriteByte(':')
		builder.WriteString(formatStaticTopicMappingItemsJSON(queues[key]))
	}
	builder.WriteByte('}')
	return builder.String()
}

func formatStaticTopicMappingItemsJSON(items []staticLogicQueueMappingItem) string {
	if len(items) == 0 {
		return "[]"
	}
	var builder strings.Builder
	builder.WriteByte('[')
	for index, item := range items {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('{')
		builder.WriteString(`"bname":`)
		builder.WriteString(jsonString(item.BName))
		builder.WriteString(`,"endOffset":`)
		builder.WriteString(strconv.FormatInt(item.EndOffset, 10))
		builder.WriteString(`,"gen":`)
		builder.WriteString(strconv.Itoa(item.Gen))
		builder.WriteString(`,"logicOffset":`)
		builder.WriteString(strconv.FormatInt(item.LogicOffset, 10))
		builder.WriteString(`,"queueId":`)
		builder.WriteString(strconv.Itoa(item.QueueID))
		builder.WriteString(`,"startOffset":`)
		builder.WriteString(strconv.FormatInt(item.StartOffset, 10))
		builder.WriteString(`,"timeOfEnd":`)
		builder.WriteString(strconv.FormatInt(item.TimeOfEnd, 10))
		builder.WriteString(`,"timeOfStart":`)
		builder.WriteString(strconv.FormatInt(item.TimeOfStart, 10))
		builder.WriteByte('}')
	}
	builder.WriteByte(']')
	return builder.String()
}

func formatStringMapJSON(values map[string]string) string {
	if len(values) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteByte('{')
	keys := sortedKeysAnyString(values)
	for index, key := range keys {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(jsonString(key))
		builder.WriteByte(':')
		builder.WriteString(jsonString(values[key]))
	}
	builder.WriteByte('}')
	return builder.String()
}

func formatNumericIntMapJSON(values map[string]int) string {
	if len(values) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteByte('{')
	keys := sortedNumericStringKeys(values)
	for index, key := range keys {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key)
		builder.WriteByte(':')
		builder.WriteString(strconv.Itoa(values[key]))
	}
	builder.WriteByte('}')
	return builder.String()
}

func formatStringArrayJSON(values []string) string {
	if len(values) == 0 {
		return "[]"
	}
	ordered := append([]string(nil), values...)
	sort.Strings(ordered)
	var builder strings.Builder
	builder.WriteByte('[')
	for index, value := range ordered {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(jsonString(value))
	}
	builder.WriteByte(']')
	return builder.String()
}

func sortedNumericStringKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		left, leftErr := strconv.Atoi(keys[i])
		right, rightErr := strconv.Atoi(keys[j])
		if leftErr == nil && rightErr == nil {
			return left < right
		}
		return keys[i] < keys[j]
	})
	return keys
}

func trimStringSlice(values []string) []string {
	trimmed := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			trimmed = append(trimmed, value)
		}
	}
	return trimmed
}

func sortedStaticTopicBrokerConfigKeys(value map[string]staticTopicConfigAndQueueMapping) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedBoolKeys(value map[string]bool) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		if value[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedIntMapKeys(value map[int]string) []int {
	keys := make([]int, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func sortedStringIntMapKeys(value map[string]int) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStaticTopicGlobalQueueKeys(value map[int]staticTopicGlobalQueue) []int {
	keys := make([]int, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func maxInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

// UpdateTopicPerm 按官方 updateTopicPerm 语义读取现有 TopicRoute，只改 Topic 权限后写回 Broker。
func (c *Client) UpdateTopicPerm(ctx context.Context, nameServer string, options updateTopicPermOptions) (*updateTopicPermResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.Topic = strings.TrimSpace(options.Topic)
	if options.NameServer == "" || options.Topic == "" {
		return nil, errors.New("NameServer、Topic 必填")
	}
	if options.Perm == 0 {
		return nil, errors.New("Perm 必填")
	}
	if options.BrokerAddr == "" && options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if options.BrokerAddr != "" && options.ClusterName != "" {
		return nil, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	routeBody, err := c.TopicRoute(ctx, options.NameServer, options.Topic)
	if err != nil {
		return nil, err
	}
	route, err := decodeTopicRouteForPerm(routeBody)
	if err != nil {
		return nil, err
	}
	if len(route.QueueDatas) == 0 {
		return nil, errors.New("topicRoute 未返回 QueueData")
	}
	baseQueue := route.QueueDatas[0]
	config := updateTopicConfig{
		TopicName:       options.Topic,
		ReadQueueNums:   baseQueue.ReadQueueNums,
		WriteQueueNums:  baseQueue.WriteQueueNums,
		Perm:            options.Perm,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    baseQueue.TopicSysFlag,
		Order:           false,
		Attributes:      map[string]string{},
	}
	if options.BrokerAddr != "" {
		brokerName := route.masterBrokerNameByAddr(options.BrokerAddr)
		if brokerName == "" {
			return &updateTopicPermResult{BrokerNotMaster: true}, nil
		}
		queue, ok := route.queueDataByBrokerName(brokerName)
		if !ok {
			return nil, fmt.Errorf("topicRoute 缺少 Broker %s 的 QueueData", brokerName)
		}
		if queue.Perm == options.Perm {
			return &updateTopicPermResult{SamePerm: true}, nil
		}
		if err := c.updateTopicAtBroker(ctx, options.BrokerAddr, config); err != nil {
			return nil, err
		}
		return &updateTopicPermResult{
			Rows: []updateTopicPermRow{{
				OldPerm:    queue.Perm,
				NewPerm:    options.Perm,
				BrokerAddr: options.BrokerAddr,
			}},
			Config:      config,
			PrintConfig: true,
		}, nil
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	result := &updateTopicPermResult{Rows: make([]updateTopicPermRow, 0, len(brokerNames))}
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.updateTopicAtBroker(ctx, masterAddr, config); err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, updateTopicPermRow{
			OldPerm:    baseQueue.Perm,
			NewPerm:    options.Perm,
			BrokerAddr: masterAddr,
		})
	}
	return result, nil
}

// SetConsumeMode 设置 Topic 与消费者组的消息请求模式，行为对应官方 setConsumeMode。
func (c *Client) SetConsumeMode(ctx context.Context, nameServer string, options setConsumeModeOptions) (*setConsumeModeResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.Topic = strings.TrimSpace(options.Topic)
	options.GroupName = strings.TrimSpace(options.GroupName)
	options.Mode = strings.TrimSpace(options.Mode)
	if options.Topic == "" || options.GroupName == "" || options.Mode == "" {
		return nil, errors.New("Topic、GroupName、Mode 必填")
	}
	if options.BrokerAddr == "" && options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	result := &setConsumeModeResult{
		Topic:            options.Topic,
		GroupName:        options.GroupName,
		Mode:             options.Mode,
		PopShareQueueNum: options.PopShareQueueNum,
	}
	if options.BrokerAddr != "" {
		if err := c.setConsumeModeAtBroker(ctx, options.BrokerAddr, options); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, options.BrokerAddr)
		return result, nil
	}
	if options.NameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.setConsumeModeAtBroker(ctx, masterAddr, options); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, masterAddr)
	}
	return result, nil
}

// UpdateColdDataFlowCtrGroupConfig 写入消费组冷读流控阈值，行为对应官方 updateColdDataFlowCtrGroupConfig。
func (c *Client) UpdateColdDataFlowCtrGroupConfig(ctx context.Context, nameServer string, options coldDataFlowCtrGroupConfigOptions) ([]string, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.ConsumerGroup = strings.TrimSpace(options.ConsumerGroup)
	options.Threshold = strings.TrimSpace(options.Threshold)
	if options.ConsumerGroup == "" || options.Threshold == "" {
		return nil, errors.New("ConsumerGroup、Threshold 必填")
	}
	if options.BrokerAddr != "" {
		if err := c.updateColdDataFlowCtrGroupConfigAtBroker(ctx, options.BrokerAddr, options.ConsumerGroup, options.Threshold); err != nil {
			return nil, err
		}
		return []string{options.BrokerAddr}, nil
	}
	targets, err := c.coldDataFlowCtrGroupConfigClusterTargets(ctx, options.NameServer, options.ClusterName)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.updateColdDataFlowCtrGroupConfigAtBroker(ctx, target, options.ConsumerGroup, options.Threshold); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// RemoveColdDataFlowCtrGroupConfig 删除消费组冷读流控阈值，行为对应官方 removeColdDataFlowCtrGroupConfig。
func (c *Client) RemoveColdDataFlowCtrGroupConfig(ctx context.Context, nameServer string, options removeColdDataFlowCtrGroupConfigOptions) ([]string, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.ConsumerGroup = strings.TrimSpace(options.ConsumerGroup)
	if options.ConsumerGroup == "" {
		return nil, errors.New("ConsumerGroup 必填")
	}
	if options.BrokerAddr != "" {
		if err := c.removeColdDataFlowCtrGroupConfigAtBroker(ctx, options.BrokerAddr, options.ConsumerGroup); err != nil {
			return nil, err
		}
		return []string{options.BrokerAddr}, nil
	}
	targets, err := c.coldDataFlowCtrGroupConfigClusterTargets(ctx, options.NameServer, options.ClusterName)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if err := c.removeColdDataFlowCtrGroupConfigAtBroker(ctx, target, options.ConsumerGroup); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// CleanExpiredCQ 清理 Broker 过期 ConsumeQueue，行为对应官方 cleanExpiredCQ。
func (c *Client) CleanExpiredCQ(ctx context.Context, nameServer string, options cleanExpiredCQOptions) (bool, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	if options.BrokerAddr != "" {
		return c.cleanExpiredCQAtBroker(ctx, options.BrokerAddr)
	}
	if options.NameServer == "" {
		return false, errors.New("NameServer 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return false, err
	}
	targets := brokerAddrsForClusters(clusterInfo, options.ClusterName)
	ok := false
	for _, target := range targets {
		ok, err = c.cleanExpiredCQAtBroker(ctx, target)
		if err != nil {
			return false, err
		}
	}
	return ok, nil
}

// CleanUnusedTopic 清理 Broker 未使用 Topic，行为对应官方 cleanUnusedTopic。
func (c *Client) CleanUnusedTopic(ctx context.Context, nameServer string, options cleanUnusedTopicOptions) (bool, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	if options.BrokerAddr != "" {
		return c.cleanUnusedTopicAtBroker(ctx, options.BrokerAddr)
	}
	if options.NameServer == "" {
		return false, errors.New("NameServer 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return false, err
	}
	targets := brokerAddrsForClusters(clusterInfo, options.ClusterName)
	ok := false
	for _, target := range targets {
		ok, err = c.cleanUnusedTopicAtBroker(ctx, target)
		if err != nil {
			return false, err
		}
	}
	return ok, nil
}

// DeleteExpiredCommitLog 删除 Broker 过期 CommitLog 文件，行为对应官方 deleteExpiredCommitLog。
func (c *Client) DeleteExpiredCommitLog(ctx context.Context, nameServer string, options deleteExpiredCommitLogOptions) (bool, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	if options.BrokerAddr != "" {
		return c.deleteExpiredCommitLogAtBroker(ctx, options.BrokerAddr)
	}
	if options.NameServer == "" {
		return false, errors.New("NameServer 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return false, err
	}
	targets := brokerAddrsForClusters(clusterInfo, options.ClusterName)
	ok := false
	for _, target := range targets {
		ok, err = c.deleteExpiredCommitLogAtBroker(ctx, target)
		if err != nil {
			return false, err
		}
	}
	return ok, nil
}

func (c *Client) coldDataFlowCtrGroupConfigClusterTargets(ctx context.Context, nameServer string, clusterName string) ([]string, error) {
	nameServer = strings.TrimSpace(nameServer)
	clusterName = strings.TrimSpace(clusterName)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if clusterName == "" {
		return nil, errors.New("ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	targets := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		targets = append(targets, masterAddr)
	}
	return targets, nil
}

func brokerAddrsForClusters(clusterInfo brokerClusterInfo, clusterName string) []string {
	clusterNames := clusterInfo.clusterNames(clusterName)
	targets := make([]string, 0)
	for _, name := range clusterNames {
		brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[name]...)
		sort.Strings(brokerNames)
		for _, brokerName := range brokerNames {
			brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
			if !ok {
				continue
			}
			for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
				addr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
				if addr != "" {
					targets = append(targets, addr)
				}
			}
		}
	}
	return targets
}

// DeleteTopic 从 Broker 与 NameServer 删除 Topic，行为对应官方 deleteTopic。
func (c *Client) DeleteTopic(ctx context.Context, nameServer string, clusterName string, topic string) error {
	nameServer = strings.TrimSpace(nameServer)
	clusterName = strings.TrimSpace(clusterName)
	topic = strings.TrimSpace(topic)
	if nameServer == "" || clusterName == "" || topic == "" {
		return errors.New("NameServer、ClusterName、Topic 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.deleteTopicAtBroker(ctx, masterAddr, topic); err != nil {
			return err
		}
	}
	fields := map[string]string{
		"topic":       topic,
		"clusterName": clusterName,
	}
	return c.invokeAllNameServers(ctx, nameServer, requestCodeDeleteTopicInNameServer, fields)
}

// UpdateSubGroup 创建或更新订阅组配置，行为对应官方 updateSubGroup。
func (c *Client) UpdateSubGroup(ctx context.Context, nameServer string, options updateSubGroupOptions) (*updateSubGroupResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	config := options.SubscriptionGroupConfig()
	result := &updateSubGroupResult{Config: config}
	if options.BrokerAddr != "" {
		if err := c.updateSubGroupAtBroker(ctx, options.BrokerAddr, config); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, options.BrokerAddr)
		return result, nil
	}
	if options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.updateSubGroupAtBroker(ctx, masterAddr, config); err != nil {
			return nil, err
		}
		result.Targets = append(result.Targets, masterAddr)
	}
	return result, nil
}

// UpdateSubGroupList 批量创建或更新订阅组配置，行为对应官方 updateSubGroupList。
func (c *Client) UpdateSubGroupList(ctx context.Context, nameServer string, options updateSubGroupListOptions) ([]string, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	if len(options.GroupConfigs) == 0 && strings.TrimSpace(options.FileName) != "" {
		configs, err := readSubscriptionGroupConfigsFile(options.FileName)
		if err != nil {
			return nil, err
		}
		options.GroupConfigs = configs
	}
	options.GroupConfigs = normalizeSubscriptionGroupConfigs(options.GroupConfigs)
	if len(options.GroupConfigs) == 0 {
		return nil, nil
	}
	if options.BrokerAddr != "" {
		if err := c.updateSubGroupListAtBroker(ctx, options.BrokerAddr, options.GroupConfigs); err != nil {
			return nil, err
		}
		return []string{options.BrokerAddr}, nil
	}
	if options.NameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if options.ClusterName == "" {
		return nil, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	targets := make([]string, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.updateSubGroupListAtBroker(ctx, masterAddr, options.GroupConfigs); err != nil {
			return nil, err
		}
		targets = append(targets, masterAddr)
	}
	return targets, nil
}

// DeleteSubGroup 删除订阅组配置，cluster 模式会复刻官方后续清理 retry/dlq topic 的行为。
func (c *Client) DeleteSubGroup(ctx context.Context, nameServer string, options deleteSubGroupOptions) ([]deleteSubGroupResult, error) {
	options.NameServer = strings.TrimSpace(nameServer)
	options.BrokerAddr = strings.TrimSpace(options.BrokerAddr)
	options.ClusterName = strings.TrimSpace(options.ClusterName)
	options.GroupName = strings.TrimSpace(options.GroupName)
	if options.GroupName == "" {
		return nil, errors.New("GroupName 必填")
	}
	if options.BrokerAddr != "" {
		if err := c.deleteSubGroupAtBroker(ctx, options.BrokerAddr, options.GroupName, options.RemoveOffset); err != nil {
			return nil, err
		}
		return []deleteSubGroupResult{{BrokerAddr: options.BrokerAddr}}, nil
	}
	if options.NameServer == "" || options.ClusterName == "" {
		return nil, errors.New("NameServer、ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, options.NameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[options.ClusterName]...)
	if len(brokerNames) == 0 {
		return nil, errors.New("[error] Make sure the specified clusterName exists or the name server connected to is correct.")
	}
	sort.Strings(brokerNames)
	rows := make([]deleteSubGroupResult, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.deleteSubGroupAtBroker(ctx, masterAddr, options.GroupName, options.RemoveOffset); err != nil {
			return nil, err
		}
		rows = append(rows, deleteSubGroupResult{BrokerAddr: masterAddr, ClusterName: options.ClusterName})
	}
	for _, topic := range []string{retryGroupTopicPrefix + options.GroupName, dlqGroupTopicPrefix + options.GroupName} {
		_ = c.deleteTopicWithClusterInfo(ctx, options.NameServer, options.ClusterName, topic, clusterInfo)
	}
	return rows, nil
}

func (c *Client) updateTopicAtBroker(ctx context.Context, brokerAddr string, config updateTopicConfig) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCodeUpdateAndCreateTopic,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: config.requestFields(),
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateTopic failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) updateTopicListAtBroker(ctx context.Context, brokerAddr string, configs []updateTopicConfig) error {
	body, err := json.Marshal(createTopicListRequestBody{TopicConfigList: normalizeUpdateTopicConfigs(configs)})
	if err != nil {
		return err
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateAndCreateTopicList,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateTopicList failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) setConsumeModeAtBroker(ctx context.Context, brokerAddr string, options setConsumeModeOptions) error {
	body, err := json.Marshal(setMessageRequestModeBody{
		Topic:            strings.TrimSpace(options.Topic),
		ConsumerGroup:    strings.TrimSpace(options.GroupName),
		Mode:             strings.TrimSpace(options.Mode),
		PopShareQueueNum: options.PopShareQueueNum,
	})
	if err != nil {
		return err
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeSetMessageRequestMode,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker setConsumeMode failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) updateColdDataFlowCtrGroupConfigAtBroker(ctx context.Context, brokerAddr string, consumerGroup string, threshold string) error {
	body := []byte(fmt.Sprintf("%s=%s\n", strings.TrimSpace(consumerGroup), strings.TrimSpace(threshold)))
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateColdDataFlowCtrConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateColdDataFlowCtrGroupConfig failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) removeColdDataFlowCtrGroupConfigAtBroker(ctx context.Context, brokerAddr string, consumerGroup string) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeRemoveColdDataFlowCtrConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     []byte(strings.TrimSpace(consumerGroup)),
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker removeColdDataFlowCtrGroupConfig failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) cleanExpiredCQAtBroker(ctx context.Context, brokerAddr string) (bool, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeCleanExpiredConsumeQueue,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return false, err
	}
	if response.Code != responseCodeSuccess {
		return false, fmt.Errorf("Broker cleanExpiredCQ failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return true, nil
}

// cleanUnusedTopicAtBroker 向指定 Broker 发送 CLEAN_UNUSED_TOPIC，请求体与扩展字段均为空。
func (c *Client) cleanUnusedTopicAtBroker(ctx context.Context, brokerAddr string) (bool, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeCleanUnusedTopic,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return false, err
	}
	if response.Code != responseCodeSuccess {
		return false, fmt.Errorf("Broker cleanUnusedTopic failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return true, nil
}

// deleteExpiredCommitLogAtBroker 向指定 Broker 发送 DELETE_EXPIRED_COMMITLOG，请求体与扩展字段均为空。
func (c *Client) deleteExpiredCommitLogAtBroker(ctx context.Context, brokerAddr string) (bool, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeDeleteExpiredCommitLog,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return false, err
	}
	if response.Code != responseCodeSuccess {
		return false, fmt.Errorf("Broker deleteExpiredCommitLog failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return true, nil
}

func (c *Client) updateBrokerConfigAtBroker(ctx context.Context, brokerAddr string, key string, value string) error {
	body := []byte(fmt.Sprintf("%s=%s\n", strings.TrimSpace(key), strings.TrimSpace(value)))
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateBrokerConfig,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateBrokerConfig failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) deleteTopicAtBroker(ctx context.Context, brokerAddr string, topic string) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCodeDeleteTopicInBroker,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: map[string]string{"topic": strings.TrimSpace(topic)},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker deleteTopic failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) updateSubGroupAtBroker(ctx context.Context, brokerAddr string, config subscriptionGroupConfig) error {
	body, err := config.requestBody()
	if err != nil {
		return err
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateAndCreateSubscriptionGroup,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateSubGroup failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func subscriptionGroupListRequestBodyFor(configs []subscriptionGroupConfig) ([]byte, error) {
	normalized := normalizeSubscriptionGroupConfigs(configs)
	rawConfigs := make([]json.RawMessage, 0, len(normalized))
	for _, config := range normalized {
		body, err := config.requestBody()
		if err != nil {
			return nil, err
		}
		rawConfigs = append(rawConfigs, json.RawMessage(body))
	}
	return json.Marshal(subscriptionGroupListRequestBody{GroupConfigList: rawConfigs})
}

func (c *Client) updateSubGroupListAtBroker(ctx context.Context, brokerAddr string, configs []subscriptionGroupConfig) error {
	body, err := subscriptionGroupListRequestBodyFor(configs)
	if err != nil {
		return err
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeUpdateAndCreateSubscriptionGroupList,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		Body:     body,
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker updateSubGroupList failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) deleteSubGroupAtBroker(ctx context.Context, brokerAddr string, groupName string, removeOffset bool) error {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodeDeleteSubscriptionGroup,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"groupName":   strings.TrimSpace(groupName),
			"cleanOffset": strconv.FormatBool(removeOffset),
		},
	})
	if err != nil {
		return err
	}
	if response.Code != responseCodeSuccess {
		return fmt.Errorf("Broker deleteSubGroup failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	return nil
}

func (c *Client) deleteTopicWithClusterInfo(ctx context.Context, nameServer string, clusterName string, topic string, clusterInfo brokerClusterInfo) error {
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	sort.Strings(brokerNames)
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		if err := c.deleteTopicAtBroker(ctx, masterAddr, topic); err != nil {
			return err
		}
	}
	fields := map[string]string{
		"topic":       topic,
		"clusterName": clusterName,
	}
	return c.invokeAllNameServers(ctx, nameServer, requestCodeDeleteTopicInNameServer, fields)
}

func (c *Client) brokerNameForOrderConf(ctx context.Context, nameServer string, brokerAddr string) (string, error) {
	entries, err := c.fetchBrokerConfig(ctx, brokerAddr)
	if err != nil {
		return "", err
	}
	brokerName := brokerConfigEntryValue(entries, "brokerName")
	if brokerName == "" {
		return "", fmt.Errorf("Broker %s 未返回 brokerName", brokerAddr)
	}
	return brokerName, nil
}

func (c *Client) updateOrderConf(ctx context.Context, nameServer string, topic string, orderConf string) error {
	if strings.TrimSpace(nameServer) == "" {
		return errors.New("NameServer 必填")
	}
	return c.UpdateKvConfig(ctx, nameServer, "ORDER_TOPIC_CONFIG", topic, orderConf)
}

func (c *Client) invokeAllNameServers(ctx context.Context, nameServers string, requestCode int, fields map[string]string) error {
	addrs := splitNameServers(nameServers)
	if len(addrs) == 0 {
		return errors.New("NameServer 必填")
	}
	var lastErr error
	for _, addr := range addrs {
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:      requestCode,
			Language:  "JAVA",
			Version:   0,
			Opaque:    nextOpaque.Add(1),
			Flag:      0,
			ExtFields: fields,
		})
		if err != nil {
			return err
		}
		if response.Code != responseCodeSuccess {
			lastErr = fmt.Errorf("NameServer request failed: addr=%s code=%d remark=%s", addr, response.Code, response.Remark)
		}
	}
	return lastErr
}

// QueryConsumeQueue 查询 ConsumeQueue 明细，行为对应官方 queryCq。
func (c *Client) QueryConsumeQueue(ctx context.Context, nameServer string, brokerAddr string, topic string, queueID int, index int64, count int, consumerGroup string) (*queryConsumeQueueResult, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		routeBody, err := c.TopicRoute(ctx, nameServer, topic)
		if err != nil {
			return nil, err
		}
		brokers, err := decodeTopicRouteBrokers(routeBody)
		if err != nil {
			return nil, err
		}
		if len(brokers) == 0 {
			return nil, errors.New("No topic route data!")
		}
		addr = strings.TrimSpace(brokers[0].BrokerAddrs["0"])
		if addr == "" {
			return nil, errors.New("topicRoute 未返回 Master Broker 地址")
		}
	}

	fields := map[string]string{
		"topic":   strings.TrimSpace(topic),
		"queueId": strconv.Itoa(queueID),
		"index":   strconv.FormatInt(index, 10),
		"count":   strconv.Itoa(count),
	}
	if strings.TrimSpace(consumerGroup) != "" {
		fields["consumerGroup"] = strings.TrimSpace(consumerGroup)
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeQueryConsumeQueue,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker queryCq failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeQueryConsumeQueueBody(response.Body)
}

// ConsumerProgressWithClientIP 查询消费者组位点，并按官方 showClientIP 模式补充队列分配客户端 IP。
func (c *Client) ConsumerProgressWithClientIP(ctx context.Context, nameServer string, consumerGroup string, topic string, clusterName string) (*consumerProgress, error) {
	progress, err := c.ConsumerProgress(ctx, nameServer, consumerGroup, topic, clusterName)
	if err != nil {
		return nil, err
	}
	allocation, err := c.consumerQueueAllocation(ctx, nameServer, consumerGroup)
	if err != nil {
		return progress, nil
	}
	for index := range progress.Entries {
		key := progress.Entries[index].identity()
		if clientIP, ok := allocation[key]; ok {
			progress.Entries[index].ClientIP = clientIP
		}
	}
	return progress, nil
}

// ConsumerProgressSummary 查询所有 %RETRY% Topic 对应的消费者组汇总，行为对应官方 consumerProgress 无 -g 模式。
func (c *Client) ConsumerProgressSummary(ctx context.Context, nameServer string) ([]consumerProgressSummaryRow, error) {
	topics, err := c.TopicList(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	rows := make([]consumerProgressSummaryRow, 0)
	for _, topic := range topics {
		if !strings.HasPrefix(topic, retryGroupTopicPrefix) {
			continue
		}
		group := strings.TrimPrefix(topic, retryGroupTopicPrefix)
		row := consumerProgressSummaryRow{
			Group:   group,
			Version: "OFFLINE",
		}
		progress, err := c.ConsumerProgress(ctx, nameServer, group, "", "")
		if err == nil && progress != nil {
			row.ConsumeTPS = int(progress.ConsumeTPS)
			row.DiffTotal = consumerProgressDiffTotal(progress)
		}
		connection, err := c.ConsumerConnectionSummary(ctx, nameServer, group)
		if err == nil && connection != nil {
			row.Count = connection.Count
			row.Version = connection.Version
			row.ConsumeType = connection.ConsumeType
			row.MessageModel = connection.MessageModel
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ConsumerConnectionSummary 查询消费者组连接摘要，行为对应官方 consumerProgress 无 -g 模式里的连接列。
func (c *Client) ConsumerConnectionSummary(ctx context.Context, nameServer string, consumerGroup string) (*consumerConnectionSummary, error) {
	routeTopic := retryGroupTopicPrefix + strings.TrimSpace(consumerGroup)
	routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	var lastErr error
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:      requestCodeGetConsumerConnectionList,
			Language:  "JAVA",
			Version:   0,
			Opaque:    nextOpaque.Add(1),
			Flag:      0,
			ExtFields: map[string]string{"consumerGroup": strings.TrimSpace(consumerGroup)},
		})
		if err != nil {
			lastErr = err
			continue
		}
		if response.Code != responseCodeSuccess {
			lastErr = fmt.Errorf("Broker consumerConnection failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
			continue
		}
		summary, err := decodeConsumerConnectionBody(response.Body)
		if err != nil {
			lastErr = err
			continue
		}
		if summary.Count == 0 {
			lastErr = errors.New("consumer connection 为空")
			continue
		}
		return summary, nil
	}
	if lastErr == nil {
		lastErr = errors.New("consumer connection 查询失败")
	}
	return nil, lastErr
}

// ConsumerConnection 查询消费者组连接、订阅和基础消费信息，行为对应官方 consumerConnection。
func (c *Client) ConsumerConnection(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string) (*consumerConnectionDetail, error) {
	var addr string
	if strings.TrimSpace(brokerAddr) != "" {
		addr = strings.TrimSpace(brokerAddr)
	} else {
		routeTopic := retryGroupTopicPrefix + strings.TrimSpace(consumerGroup)
		routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
		if err != nil {
			return nil, err
		}
		brokers, err := decodeTopicRouteBrokers(routeBody)
		if err != nil {
			return nil, err
		}
		if len(brokers) == 0 {
			return nil, errors.New("topicRoute 未返回 Broker")
		}
		for _, broker := range brokers {
			addr = broker.selectAddr()
			if addr != "" {
				break
			}
		}
		if addr == "" {
			return nil, errors.New("topicRoute 未返回 Broker 地址")
		}
	}

	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeGetConsumerConnectionList,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: map[string]string{"consumerGroup": strings.TrimSpace(consumerGroup)},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker consumerConnection failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	detail, err := decodeConsumerConnectionDetailBody(response.Body)
	if err != nil {
		return nil, err
	}
	if len(detail.Connections) == 0 {
		return nil, errors.New("consumer connection 为空")
	}
	return detail, nil
}

// ConsumerStatus 查询单个消费者客户端运行信息，行为对应官方 consumerStatus -i 模式。
func (c *Client) ConsumerStatus(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (string, error) {
	info, err := c.consumerRunningInfo(ctx, nameServer, consumerGroup, clientID, brokerAddr, jstack)
	if err != nil {
		return "", err
	}
	return formatConsumerRunningInfo(info), nil
}

// ConsumerStatusList 查询消费组所有在线客户端运行信息，写出官方 consumerStatus 文件并返回索引表。
func (c *Client) ConsumerStatusList(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string, jstack bool) (string, error) {
	entries, infos, now, err := c.consumerStatusEntries(ctx, nameServer, consumerGroup, brokerAddr, jstack)
	if err != nil {
		return "", err
	}
	return formatConsumerStatusList(entries, infos, now), nil
}

func (c *Client) consumerStatusEntries(ctx context.Context, nameServer string, consumerGroup string, brokerAddr string, jstack bool) ([]consumerStatusListEntry, map[string]*consumerRunningInfo, int64, error) {
	connection, err := c.ConsumerConnection(ctx, nameServer, consumerGroup, brokerAddr)
	if err != nil {
		return nil, nil, 0, err
	}
	now := time.Now().UnixMilli()
	dir := strconv.FormatInt(now, 10)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, 0, err
	}
	infos := make(map[string]*consumerRunningInfo)
	entries := make([]consumerStatusListEntry, 0, len(connection.Connections))
	for _, connection := range connection.Connections {
		clientID := strings.TrimSpace(connection.ClientID)
		if clientID == "" {
			continue
		}
		info, err := c.consumerRunningInfo(ctx, nameServer, consumerGroup, clientID, brokerAddr, jstack)
		if err != nil || info == nil {
			continue
		}
		filePath := dir + "/" + clientID
		if err := os.WriteFile(dir+string(os.PathSeparator)+clientID, []byte(formatConsumerRunningInfo(info)), 0644); err != nil {
			return nil, nil, 0, err
		}
		infos[clientID] = info
		entries = append(entries, consumerStatusListEntry{
			ClientID: clientID,
			Version:  rocketMQVersionDesc(connection.Version),
			FilePath: filePath,
		})
	}
	return entries, infos, now, nil
}

// StartMonitoring 启动与官方 startMonitoring 等价的静默监控循环，直到调用方取消上下文。
func (c *Client) StartMonitoring(ctx context.Context, nameServer string) error {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		return errors.New("NameServer 必填")
	}
	if ctx.Err() != nil {
		return nil
	}
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			c.runMonitoringRound(ctx, nameServer)
			timer.Reset(time.Minute)
		}
	}
}

func (c *Client) runMonitoringRound(ctx context.Context, nameServer string) {
	rows, err := c.ConsumerProgressSummary(ctx, nameServer)
	if err != nil {
		return
	}
	for _, row := range rows {
		group := strings.TrimSpace(row.Group)
		if group == "" {
			continue
		}
		_, _ = c.ConsumerProgress(ctx, nameServer, group, "", "")
	}
}

func (c *Client) consumerRunningInfo(ctx context.Context, nameServer string, consumerGroup string, clientID string, brokerAddr string, jstack bool) (*consumerRunningInfo, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		routeTopic := retryGroupTopicPrefix + strings.TrimSpace(consumerGroup)
		routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
		if err != nil {
			return nil, err
		}
		brokers, err := decodeTopicRouteBrokers(routeBody)
		if err != nil {
			return nil, err
		}
		if len(brokers) == 0 {
			return nil, errors.New("topicRoute 未返回 Broker")
		}
		for _, broker := range brokers {
			addr = broker.selectAddr()
			if addr != "" {
				break
			}
		}
		if addr == "" {
			return nil, errors.New("topicRoute 未返回 Broker 地址")
		}
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeGetConsumerRunningInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"consumerGroup": strings.TrimSpace(consumerGroup),
			"clientId":      strings.TrimSpace(clientID),
			"jstackEnable":  strconv.FormatBool(jstack),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker consumerRunningInfo failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	info, err := decodeConsumerRunningInfoDetailBody(response.Body)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// ProducerConnection 查询生产者组连接，行为对应官方 producerConnection。
func (c *Client) ProducerConnection(ctx context.Context, nameServer string, producerGroup string, topic string) (*producerConnectionDetail, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, strings.TrimSpace(topic))
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	var addr string
	for _, broker := range brokers {
		addr = broker.selectAddr()
		if addr != "" {
			break
		}
	}
	if addr == "" {
		return nil, errors.New("topicRoute 未返回 Broker 地址")
	}

	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:      requestCodeGetProducerConnectionList,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: map[string]string{"producerGroup": strings.TrimSpace(producerGroup)},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker producerConnection failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	detail, err := decodeProducerConnectionBody(response.Body)
	if err != nil {
		return nil, err
	}
	if len(detail.Connections) == 0 {
		return nil, errors.New("Not found the producer group connection")
	}
	return detail, nil
}

// Producer 查询 Broker 上全部在线生产者实例，行为对应官方 producer 子命令。
func (c *Client) Producer(ctx context.Context, brokerAddr string) (*producerTableInfo, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return nil, errors.New("BrokerAddr 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeGetAllProducerInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker producer failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeProducerTableInfoBody(response.Body)
}

// GetColdDataFlowCtrInfo 查询单个 Broker 的冷数据流控表，行为对应官方 getColdDataFlowCtrInfo -b。
func (c *Client) GetColdDataFlowCtrInfo(ctx context.Context, brokerAddr string) (string, error) {
	addr := strings.TrimSpace(brokerAddr)
	if addr == "" {
		return "", errors.New("BrokerAddr 必填")
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeGetColdDataFlowCtrInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
	})
	if err != nil {
		return "", err
	}
	if response.Code != responseCodeSuccess {
		return "", fmt.Errorf("get cold data flow ctr info failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	if len(response.Body) == 0 {
		return "", nil
	}
	return string(response.Body), nil
}

// GetColdDataFlowCtrInfoByCluster 查询集群内 master 与 slave 的冷数据流控表，行为对应官方 getColdDataFlowCtrInfo -c。
func (c *Client) GetColdDataFlowCtrInfoByCluster(ctx context.Context, nameServer string, clusterName string) ([]coldDataFlowCtrInfoSection, error) {
	cluster := strings.TrimSpace(clusterName)
	if cluster == "" {
		return nil, errors.New("ClusterName 必填")
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[cluster]...)
	if len(brokerNames) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回 Broker", cluster)
	}
	sort.Strings(brokerNames)
	sections := make([]coldDataFlowCtrInfoSection, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		masterAddr := strings.TrimSpace(brokerData.BrokerAddrs["0"])
		if masterAddr == "" {
			continue
		}
		sections = append(sections, coldDataFlowCtrInfoSection{
			Header:     fmt.Sprintf("============Master: %s============", masterAddr),
			BrokerAddr: masterAddr,
		})
		for _, brokerID := range sortedBrokerIDKeys(brokerData.BrokerAddrs) {
			if brokerID == "0" {
				continue
			}
			slaveAddr := strings.TrimSpace(brokerData.BrokerAddrs[brokerID])
			if slaveAddr == "" {
				continue
			}
			sections = append(sections, coldDataFlowCtrInfoSection{
				Header:     fmt.Sprintf("============My Master: %s=====Slave: %s============", masterAddr, slaveAddr),
				BrokerAddr: slaveAddr,
			})
		}
	}
	if len(sections) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回可查询 Broker", cluster)
	}
	var waitGroup sync.WaitGroup
	errorsByIndex := make([]error, len(sections))
	for index := range sections {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			body, err := c.GetColdDataFlowCtrInfo(ctx, sections[index].BrokerAddr)
			if err != nil {
				errorsByIndex[index] = err
				return
			}
			sections[index].Body = body
		}(index)
	}
	waitGroup.Wait()
	for _, err := range errorsByIndex {
		if err != nil {
			return nil, err
		}
	}
	return sections, nil
}

// consumerQueueAllocation 复现官方 showClientIP 的 MessageQueue 到 clientId 前缀映射。
func (c *Client) consumerQueueAllocation(ctx context.Context, nameServer string, consumerGroup string) (map[messageQueueIdentity]string, error) {
	connection, err := c.ConsumerConnectionSummary(ctx, nameServer, consumerGroup)
	if err != nil {
		return nil, err
	}
	allocation := make(map[messageQueueIdentity]string)
	if connection == nil || len(connection.ClientIDs) == 0 {
		return allocation, nil
	}
	routeTopic := retryGroupTopicPrefix + strings.TrimSpace(consumerGroup)
	routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
	if err != nil {
		return allocation, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return allocation, err
	}
	if len(brokers) == 0 {
		return allocation, errors.New("topicRoute 未返回 Broker")
	}
	addr := ""
	for _, broker := range brokers {
		addr = broker.selectAddr()
		if addr != "" {
			break
		}
	}
	if addr == "" {
		return allocation, errors.New("topicRoute 未返回 Broker 地址")
	}
	for _, clientID := range connection.ClientIDs {
		queues, err := c.consumerRunningInfoQueues(ctx, addr, consumerGroup, clientID)
		if err != nil {
			continue
		}
		clientIP := consumerClientIP(clientID)
		for _, queue := range queues {
			allocation[queue] = clientIP
		}
	}
	return allocation, nil
}

// consumerRunningInfoQueues 查询单个在线客户端的运行信息，只保留官方 showClientIP 需要的队列键。
func (c *Client) consumerRunningInfoQueues(ctx context.Context, addr string, consumerGroup string, clientID string) ([]messageQueueIdentity, error) {
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeGetConsumerRunningInfo,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"consumerGroup": strings.TrimSpace(consumerGroup),
			"clientId":      strings.TrimSpace(clientID),
			"jstackEnable":  "false",
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker consumerRunningInfo failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeConsumerRunningInfoBody(response.Body)
}

func consumerProgressDiffTotal(progress *consumerProgress) int64 {
	if progress == nil {
		return 0
	}
	var total int64
	for _, entry := range progress.Entries {
		total += entry.BrokerOffset - entry.ConsumerOffset
	}
	return total
}

// QueryMessagesByKey 查询 Topic 的消息索引，行为对应官方 queryMsgByKey。
func (c *Client) QueryMessagesByKey(ctx context.Context, nameServer string, topic string, key string, clusterName string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageSearchResult, error) {
	brokers, err := c.queryMessageRouteBrokers(ctx, nameServer, topic, clusterName)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	type brokerResult struct {
		results []messageSearchResult
	}
	resultCh := make(chan brokerResult, len(brokers))
	var wg sync.WaitGroup
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			entries, err := c.queryMessagesFromBroker(ctx, addr, topic, key, beginTimestamp, endTimestamp, maxNum, false)
			if err != nil || len(entries) == 0 {
				return
			}
			resultCh <- brokerResult{results: entries}
		}(addr)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]messageSearchResult, 0)
	for item := range resultCh {
		results = append(results, item.results...)
	}

	filtered := make([]messageSearchResult, 0, len(results))
	for _, item := range results {
		if item.Topic != strings.TrimSpace(topic) {
			continue
		}
		if containsMessageKey(item.Keys, key) {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) == 0 {
		return nil, errors.New("query message by key finished, but no message.")
	}
	return filtered, nil
}

// queryMessageRouteBrokers 按官方 queryMessage 的 cluster 参数缩小目标 Broker；未传 cluster 时沿用 TopicRoute。
func (c *Client) queryMessageRouteBrokers(ctx context.Context, nameServer string, topic string, clusterName string) ([]topicRouteBroker, error) {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		routeBody, err := c.TopicRoute(ctx, nameServer, topic)
		if err != nil {
			return nil, err
		}
		return decodeTopicRouteBrokers(routeBody)
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return nil, err
	}
	brokerNames := append([]string(nil), clusterInfo.ClusterAddrTable[clusterName]...)
	if len(brokerNames) == 0 {
		return nil, fmt.Errorf("clusterName %s 未返回 Broker", clusterName)
	}
	sort.Strings(brokerNames)
	brokers := make([]topicRouteBroker, 0, len(brokerNames))
	for _, brokerName := range brokerNames {
		brokerData, ok := clusterInfo.BrokerAddrTable[brokerName]
		if !ok {
			continue
		}
		brokers = append(brokers, topicRouteBroker{
			BrokerName:  brokerData.BrokerName,
			BrokerAddrs: brokerData.BrokerAddrs,
			Cluster:     brokerData.Cluster,
		})
	}
	return brokers, nil
}

func (c *Client) queryMessagesFromBroker(ctx context.Context, addr string, topic string, key string, beginTimestamp int64, endTimestamp int64, maxNum int, uniqueKey bool) ([]messageSearchResult, error) {
	details, err := c.queryMessageDetailsFromBroker(ctx, addr, topic, key, beginTimestamp, endTimestamp, maxNum, uniqueKey)
	if err != nil {
		return nil, err
	}
	results := make([]messageSearchResult, 0, len(details))
	for _, detail := range details {
		results = append(results, detail.toSearchResult())
	}
	return results, nil
}

func (c *Client) queryMessageDetailsFromBroker(ctx context.Context, addr string, topic string, key string, beginTimestamp int64, endTimestamp int64, maxNum int, uniqueKey bool) ([]messageDetail, error) {
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeQueryMessage,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"topic":            strings.TrimSpace(topic),
			"key":              strings.TrimSpace(key),
			"maxNum":           strconv.Itoa(maxNum),
			"beginTimestamp":   strconv.FormatInt(beginTimestamp, 10),
			"endTimestamp":     strconv.FormatInt(endTimestamp, 10),
			uniqueMsgQueryFlag: strconv.FormatBool(uniqueKey),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker queryMessage failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	return decodeMessageDetailsBody(response.Body)
}

// QueryMessageByID 查询消息详情，优先按 Broker OffsetID 直查，无法解析 OffsetID 时按官方逻辑回退 UNIQ_KEY 查询。
func (c *Client) QueryMessageByID(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
	addr, offset, ok := decodeOffsetMessageID(msgID)
	if ok {
		return c.viewMessageByOffsetID(ctx, addr, offset)
	}
	return c.queryMessageByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
}

// MessageTrackDetail 查询消息对应的消费轨迹，行为对应官方 DefaultMQAdminExt.messageTrackDetail。
func (c *Client) MessageTrackDetail(ctx context.Context, nameServer string, detail *messageDetail) ([]messageTrack, error) {
	if detail == nil {
		return nil, nil
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, detail.Topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	groups, err := c.queryTopicConsumeByWho(ctx, detail.Topic, brokers)
	if err != nil {
		return nil, err
	}
	tracks := make([]messageTrack, 0, len(groups))
	for _, group := range groups {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		tracks = append(tracks, c.messageTrackForGroup(ctx, nameServer, detail, group))
	}
	return tracks, nil
}

func (c *Client) messageTrackForGroup(ctx context.Context, nameServer string, detail *messageDetail, consumerGroup string) messageTrack {
	track := messageTrack{
		ConsumerGroup: consumerGroup,
		TrackType:     "UNKNOWN",
	}
	connection, err := c.messageTrackConsumerConnection(ctx, nameServer, consumerGroup)
	if err != nil {
		return messageTrackWithError(track, err)
	}
	switch strings.TrimSpace(connection.ConsumeType) {
	case "CONSUME_ACTIVELY":
		track.TrackType = "PULL"
	case "CONSUME_PASSIVELY":
		consumed, err := c.messageConsumedByGroup(ctx, nameServer, detail, consumerGroup, connection)
		if err != nil {
			return messageTrackWithError(track, err)
		}
		if consumed {
			track.TrackType = "CONSUMED"
			if !messageTrackSubscriptionMatches(connection.Subscriptions, detail.Topic, detail.Tags) {
				track.TrackType = "CONSUMED_BUT_FILTERED"
			}
		} else {
			track.TrackType = "NOT_CONSUME_YET"
		}
	}
	return track
}

func (c *Client) messageTrackConsumerConnection(ctx context.Context, nameServer string, consumerGroup string) (*consumerConnectionDetail, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, retryGroupTopicPrefix+strings.TrimSpace(consumerGroup))
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		response, err := c.invoke(ctx, addr, remotingCommand{
			Code:      requestCodeGetConsumerConnectionList,
			Language:  "JAVA",
			Version:   0,
			Opaque:    nextOpaque.Add(1),
			Flag:      0,
			ExtFields: map[string]string{"consumerGroup": strings.TrimSpace(consumerGroup)},
		})
		if err != nil {
			return nil, err
		}
		if response.Code != responseCodeSuccess {
			return nil, &rocketMQResponseError{Code: response.Code, Remark: response.Remark}
		}
		detail, err := decodeConsumerConnectionDetailBody(response.Body)
		if err != nil {
			return nil, err
		}
		if len(detail.Connections) == 0 {
			return nil, &rocketMQResponseError{Code: responseCodeConsumerNotOnline, Remark: "Not found the consumer group connection"}
		}
		return detail, nil
	}
	return nil, errors.New("topicRoute 未返回 Broker 地址")
}

func (c *Client) messageConsumedByGroup(ctx context.Context, nameServer string, detail *messageDetail, consumerGroup string, connection *consumerConnectionDetail) (bool, error) {
	progress, err := c.ConsumerProgress(ctx, nameServer, consumerGroup, "", "")
	if err != nil {
		return false, err
	}
	if progress == nil || len(progress.Entries) == 0 {
		if connection != nil && strings.TrimSpace(connection.MessageModel) == "BROADCASTING" {
			return false, &rocketMQResponseError{
				Code:   responseCodeBroadcastConsumption,
				Remark: "Not found the consumer group consume stats, because return offset table is empty, the consumer is under the broadcast mode",
			}
		}
		return false, &rocketMQResponseError{
			Code:   responseCodeConsumerNotOnline,
			Remark: "Not found the consumer group consume stats, because return offset table is empty, maybe the consumer not online",
		}
	}
	clusterInfo, err := c.fetchBrokerClusterInfo(ctx, nameServer)
	if err != nil {
		return false, err
	}
	for _, entry := range progress.Entries {
		if entry.Topic != detail.Topic || entry.QueueID != detail.QueueID {
			continue
		}
		brokerData, ok := clusterInfo.BrokerAddrTable[entry.BrokerName]
		if !ok {
			continue
		}
		if strings.TrimSpace(brokerData.BrokerAddrs["0"]) != strings.TrimSpace(detail.StoreHost) {
			continue
		}
		if entry.ConsumerOffset > detail.QueueOffset {
			return true, nil
		}
	}
	return false, nil
}

func messageTrackWithError(track messageTrack, err error) messageTrack {
	if err == nil {
		return track
	}
	var routeErr *topicRouteError
	if errors.As(err, &routeErr) {
		track.ExceptionDesc = routeErr.messageTrackExceptionDesc()
		return track
	}
	var responseErr *rocketMQResponseError
	if errors.As(err, &responseErr) {
		if responseErr.Code == responseCodeConsumerNotOnline {
			track.TrackType = "NOT_ONLINE"
		}
		if responseErr.Code == responseCodeBroadcastConsumption {
			track.TrackType = "CONSUME_BROADCASTING"
		}
		track.ExceptionDesc = responseErr.Error()
		return track
	}
	track.ExceptionDesc = err.Error()
	return track
}

func messageTrackSubscriptionMatches(subscriptions []consumerSubscriptionEntry, topic string, tag string) bool {
	for _, subscription := range subscriptions {
		if strings.TrimSpace(subscription.Topic) != strings.TrimSpace(topic) {
			continue
		}
		expression := strings.TrimSpace(subscription.Expression)
		if expression == "" || expression == "*" {
			return true
		}
		tags := strings.Split(expression, "||")
		for _, candidate := range tags {
			if strings.TrimSpace(candidate) == strings.TrimSpace(tag) {
				return true
			}
		}
		return false
	}
	return true
}

// QueryMessageByUniqueKey 按官方 queryMsgByUniqueKey 语义强制使用 UNIQ_KEY 索引查询，不把 ID 解释为 OffsetID。
func (c *Client) QueryMessageByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
	details, err := c.QueryMessagesByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
	if err != nil || len(details) == 0 {
		return nil, err
	}
	return &details[0], nil
}

// QueryMessagesByUniqueKey 查询所有匹配 UNIQ_KEY 的消息，并按官方命令要求用 store timestamp 升序排列。
func (c *Client) QueryMessagesByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) ([]messageDetail, error) {
	return c.queryMessagesByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
}

// ConsumeMessageDirectly 复刻官方 queryMsgByUniqueKey -g/-d 路径，向指定 push consumer 直接投递一次消息。
func (c *Client) ConsumeMessageDirectly(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error) {
	info, err := c.consumerRunningInfo(ctx, nameServer, consumerGroup, clientID, "", false)
	if err != nil {
		return nil, &consumeMessageDirectlyUnavailableError{
			ClientID:          strings.TrimSpace(clientID),
			RunningInfoFailed: true,
			Cause:             err,
		}
	}
	if !consumerRunningInfoIsPush(info) {
		return nil, &consumeMessageDirectlyUnavailableError{ClientID: strings.TrimSpace(clientID)}
	}
	detail, err := c.QueryMessageByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
	if err != nil {
		return nil, err
	}
	if detail == nil {
		return nil, errors.New("query message by unique key finished, but no message.")
	}
	brokerAddr := strings.TrimSpace(detail.StoreHost)
	if brokerAddr == "" {
		return nil, errors.New("message store host 为空")
	}
	consumeMsgID := strings.TrimSpace(msgID)
	if detail.Properties.Get("UNIQ_KEY") != "" && strings.TrimSpace(detail.OffsetMessageID) != "" {
		consumeMsgID = strings.TrimSpace(detail.OffsetMessageID)
	}
	return c.consumeMessageDirectlyAtBroker(ctx, brokerAddr, consumerGroup, clientID, topic, consumeMsgID)
}

// ConsumeMessageDirectlyByID 复刻官方 queryMsgById -g/-d 路径，先按 ID 查询消息，再将 offsetMsgId 发给目标 push consumer。
func (c *Client) ConsumeMessageDirectlyByID(ctx context.Context, nameServer string, consumerGroup string, clientID string, topic string, clusterName string, msgID string) (*consumeMessageDirectlyResult, error) {
	info, err := c.consumerRunningInfo(ctx, nameServer, consumerGroup, clientID, "", false)
	if err != nil {
		return nil, &consumeMessageDirectlyUnavailableError{
			ClientID:          strings.TrimSpace(clientID),
			RunningInfoFailed: true,
			Cause:             err,
		}
	}
	if !consumerRunningInfoIsPush(info) {
		return nil, &consumeMessageDirectlyUnavailableError{ClientID: strings.TrimSpace(clientID)}
	}
	detail, err := c.QueryMessageByID(ctx, nameServer, topic, clusterName, msgID)
	if err != nil {
		return nil, err
	}
	if detail == nil {
		return nil, errors.New("query message by id finished, but no message.")
	}
	brokerAddr := strings.TrimSpace(detail.StoreHost)
	if brokerAddr == "" {
		return nil, errors.New("message store host 为空")
	}
	consumeMsgID := strings.TrimSpace(msgID)
	if detail.Properties.Get("UNIQ_KEY") != "" && strings.TrimSpace(detail.OffsetMessageID) != "" {
		consumeMsgID = strings.TrimSpace(detail.OffsetMessageID)
	}
	return c.consumeMessageDirectlyAtBroker(ctx, brokerAddr, consumerGroup, clientID, topic, consumeMsgID)
}

// ResendMessageByID 复刻官方 queryMsgById -s true：先按 ID 查原消息，再用 ReSendMsgById 生产者重发原消息体和属性。
func (c *Client) ResendMessageByID(ctx context.Context, nameServer string, topic string, clusterName string, msgID string, unitName string) (*queryMsgByIDResendResult, error) {
	nameServer = strings.TrimSpace(nameServer)
	topic = strings.TrimSpace(topic)
	msgID = strings.TrimSpace(msgID)
	if nameServer == "" {
		return nil, errors.New("NameServer 必填")
	}
	if topic == "" {
		return nil, errors.New("Topic 必填")
	}
	if msgID == "" {
		return nil, errors.New("Message Id 必填")
	}
	detail, err := c.QueryMessageByID(ctx, nameServer, topic, clusterName, msgID)
	if err != nil {
		return nil, err
	}
	result := &queryMsgByIDResendResult{OriginalMsgID: msgID}
	if detail == nil {
		return result, nil
	}
	resendTopic := strings.TrimSpace(detail.Topic)
	if resendTopic == "" {
		resendTopic = topic
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, resendTopic)
	if err != nil {
		return nil, err
	}
	queue, err := selectSendMessageQueue(sendMessageOptions{Topic: resendTopic}, routeBody)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	broker, ok := findTopicRouteBroker(brokers, queue.BrokerName)
	if !ok {
		return nil, fmt.Errorf("queryMsgById resend 未找到 Broker %s", queue.BrokerName)
	}
	brokerAddr := broker.selectAddr()
	if brokerAddr == "" {
		return nil, fmt.Errorf("queryMsgById resend 未找到 Broker %s 的 master 地址", queue.BrokerName)
	}
	sendResult, err := c.sendMessageToBroker(ctx, brokerAddr, queue, sendMessageOptions{
		Topic:              resendTopic,
		BodyBytes:          append([]byte(nil), detail.Body...),
		Keys:               detail.Keys,
		Tags:               detail.Tags,
		ProducerGroup:      "ReSendMsgById",
		PreserveProperties: true,
		Properties:         append(messageProperties(nil), detail.Properties...),
		Flag:               detail.Flag,
		UnitMode:           strings.TrimSpace(unitName) != "",
	})
	if err != nil {
		return nil, err
	}
	result.SendResult = sendResult
	return result, nil
}

func (c *Client) consumeMessageDirectlyAtBroker(ctx context.Context, brokerAddr string, consumerGroup string, clientID string, topic string, msgID string) (*consumeMessageDirectlyResult, error) {
	response, err := c.invoke(ctx, strings.TrimSpace(brokerAddr), remotingCommand{
		Code:     requestCodeConsumeMessageDirectly,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"consumerGroup": strings.TrimSpace(consumerGroup),
			"clientId":      strings.TrimSpace(clientID),
			"topic":         strings.TrimSpace(topic),
			"msgId":         strings.TrimSpace(msgID),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker consumeMessageDirectly failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	var result consumeMessageDirectlyResult
	if err := json.Unmarshal(response.Body, &result); err != nil {
		return nil, fmt.Errorf("解析 consumeMessageDirectly 结果失败: %w", err)
	}
	return &result, nil
}

func (c *Client) viewMessageByOffsetID(ctx context.Context, addr string, offset int64) (*messageDetail, error) {
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodeViewMessageByID,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"offset": strconv.FormatInt(offset, 10),
		},
	})
	if err != nil {
		return nil, err
	}
	if response.Code != responseCodeSuccess {
		return nil, fmt.Errorf("Broker queryMsgById failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
	details, err := decodeMessageDetailsBody(response.Body)
	if err != nil {
		return nil, err
	}
	if len(details) == 0 {
		return nil, nil
	}
	return &details[0], nil
}

func (c *Client) queryMessageByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) (*messageDetail, error) {
	details, err := c.queryMessagesByUniqueKey(ctx, nameServer, topic, clusterName, msgID)
	if err != nil || len(details) == 0 {
		return nil, err
	}
	return &details[0], nil
}

func (c *Client) queryMessagesByUniqueKey(ctx context.Context, nameServer string, topic string, clusterName string, msgID string) ([]messageDetail, error) {
	if strings.TrimSpace(nameServer) == "" {
		return nil, errors.New("NameServer 必填")
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	beginTimestamp := time.Now().Add(-72 * time.Hour).UnixMilli()
	matched := make([]messageDetail, 0)
	for _, broker := range brokers {
		if clusterName != "" && broker.Cluster != clusterName {
			continue
		}
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		details, err := c.queryMessageDetailsFromBroker(ctx, addr, topic, msgID, beginTimestamp, defaultQueryMessageEndTimestamp, 32, true)
		if err != nil {
			continue
		}
		for _, detail := range details {
			if detail.DisplayMessageID == msgID {
				matched = append(matched, detail)
			}
		}
	}
	sort.SliceStable(matched, func(i, j int) bool {
		return matched[i].StoreTimestamp < matched[j].StoreTimestamp
	})
	return matched, nil
}

// QueryMessageTraceByID 查询并解码消息轨迹，行为对应官方 queryMsgTraceById。
func (c *Client) QueryMessageTraceByID(ctx context.Context, nameServer string, traceTopic string, msgID string, beginTimestamp int64, endTimestamp int64, maxNum int) ([]messageTraceView, error) {
	if strings.TrimSpace(nameServer) == "" {
		return nil, errors.New("NameServer 必填")
	}
	traceTopic = strings.TrimSpace(traceTopic)
	if traceTopic == "" {
		traceTopic = defaultTraceTopic
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, traceTopic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}

	type brokerResult struct {
		details []messageDetail
	}
	resultCh := make(chan brokerResult, len(brokers))
	var wg sync.WaitGroup
	for _, broker := range brokers {
		addr := broker.selectAddr()
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			details, err := c.queryMessageDetailsFromBroker(ctx, addr, traceTopic, msgID, beginTimestamp, endTimestamp, maxNum, false)
			if err != nil || len(details) == 0 {
				return
			}
			resultCh <- brokerResult{details: details}
		}(addr)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var matched []messageDetail
	for item := range resultCh {
		for _, detail := range item.details {
			if detail.Topic != traceTopic {
				continue
			}
			if containsMessageKey(splitMessageKeys(detail.Keys), msgID) {
				matched = append(matched, detail)
			}
		}
	}
	if len(matched) == 0 {
		return nil, nil
	}
	return decodeTraceViewsFromMessages(msgID, matched)
}

// QueryMessageByOffset 按 Broker、队列和 offset 精确拉取一条消息，行为对应官方 queryMsgByOffset。
func (c *Client) QueryMessageByOffset(ctx context.Context, nameServer string, topic string, brokerName string, queueID int, offset int64) (*messageDetail, error) {
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	broker, ok := findTopicRouteBroker(brokers, brokerName)
	if !ok {
		return nil, fmt.Errorf("topicRoute 未返回 Broker %s", brokerName)
	}
	addr := broker.selectAddr()
	if addr == "" {
		return nil, fmt.Errorf("Broker %s 未返回可用地址", brokerName)
	}
	response, err := c.invoke(ctx, addr, remotingCommand{
		Code:     requestCodePullMessage,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"consumerGroup":        toolsConsumerGroup,
			"topic":                strings.TrimSpace(topic),
			"queueId":              strconv.Itoa(queueID),
			"queueOffset":          strconv.FormatInt(offset, 10),
			"maxMsgNums":           "1",
			"sysFlag":              "4",
			"commitOffset":         "0",
			"suspendTimeoutMillis": strconv.Itoa(defaultBrokerSuspendMaxTimeMillis),
			"subscription":         "*",
			"subVersion":           "0",
			"expressionType":       "TAG",
			"maxMsgBytes":          strconv.Itoa(defaultPullMaxMsgBytes),
			"bname":                strings.TrimSpace(brokerName),
		},
	})
	if err != nil {
		return nil, err
	}
	switch response.Code {
	case responseCodeSuccess:
		details, err := decodeMessageDetailsBody(response.Body)
		if err != nil {
			return nil, err
		}
		if len(details) == 0 {
			return nil, nil
		}
		detail := details[0]
		detail.QueueID = queueID
		if deltaRaw := strings.TrimSpace(response.ExtFields["offsetDelta"]); deltaRaw != "" {
			delta, err := strconv.ParseInt(deltaRaw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("解析 pull offsetDelta 失败: %w", err)
			}
			detail.QueueOffset += delta
		}
		if minOffset := strings.TrimSpace(response.ExtFields["minOffset"]); minOffset != "" {
			detail.Properties.Set("MIN_OFFSET", minOffset)
		}
		if maxOffset := strings.TrimSpace(response.ExtFields["maxOffset"]); maxOffset != "" {
			detail.Properties.Set("MAX_OFFSET", maxOffset)
		}
		return &detail, nil
	case responseCodePullNotFound, responseCodePullRetryImmediately, responseCodePullOffsetMoved:
		return nil, nil
	default:
		return nil, fmt.Errorf("Broker queryMsgByOffset failed: broker=%s code=%d remark=%s", addr, response.Code, response.Remark)
	}
}

// PrintMessages 复刻官方 printMsg，按 Topic 的订阅队列顺序逐队列拉取并打印消息。
func (c *Client) PrintMessages(ctx context.Context, nameServer string, options printMsgOptions) (*printMsgResult, error) {
	topic := strings.TrimSpace(options.Topic)
	if topic == "" {
		return nil, errors.New("Topic 必填")
	}
	routeTopic := topic
	if parent := strings.TrimSpace(options.LMQParentTopic); parent != "" {
		routeTopic = parent
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, routeTopic)
	if err != nil {
		return nil, err
	}
	queues, err := decodeTopicRouteSubscribeQueues(topic, routeBody)
	if err != nil {
		return nil, err
	}
	if len(queues) == 0 {
		return nil, errors.New("topicRoute 未返回可读队列")
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}

	result := &printMsgResult{Queues: make([]printMsgQueueResult, 0, len(queues))}
	for _, queue := range queues {
		broker, ok := findTopicRouteBroker(brokers, queue.BrokerName)
		if !ok {
			return nil, fmt.Errorf("topicRoute 未返回 Broker %s", queue.BrokerName)
		}
		addr := broker.selectAddr()
		if addr == "" {
			return nil, fmt.Errorf("Broker %s 未返回可用地址", queue.BrokerName)
		}
		queueOptions := printMsgByQueueOptions{
			Topic:             topic,
			BrokerName:        queue.BrokerName,
			QueueID:           queue.QueueID,
			HasBeginTimestamp: options.HasBeginTimestamp,
			BeginTimestamp:    options.BeginTimestamp,
			HasEndTimestamp:   options.HasEndTimestamp,
			EndTimestamp:      options.EndTimestamp,
			SubExpression:     options.SubExpression,
		}
		beginOffset, err := c.getMinOffsetAtBroker(ctx, addr, topic, queue.BrokerName, queue.QueueID)
		if err != nil {
			return nil, err
		}
		endOffset, err := c.getMaxOffsetAtBroker(ctx, addr, topic, queue.BrokerName, queue.QueueID)
		if err != nil {
			return nil, err
		}
		if options.HasBeginTimestamp {
			beginOffset, err = c.searchOffsetAtBroker(ctx, addr, topic, queue.BrokerName, queue.QueueID, options.BeginTimestamp)
			if err != nil {
				return nil, err
			}
		}
		if options.HasEndTimestamp {
			endOffset, err = c.searchOffsetAtBroker(ctx, addr, topic, queue.BrokerName, queue.QueueID, options.EndTimestamp)
			if err != nil {
				return nil, err
			}
		}

		queueResult := printMsgQueueResult{
			Queue:     queue,
			MinOffset: beginOffset,
			MaxOffset: endOffset,
			Messages:  make([]messageDetail, 0),
		}
		offset := beginOffset
		for offset < endOffset {
			batch, err := c.pullMessagesAtBroker(ctx, addr, queueOptions, offset, 32)
			if err != nil {
				return nil, err
			}
			if !batch.Found {
				break
			}
			queueResult.Messages = append(queueResult.Messages, batch.Messages...)
			if batch.NextBeginOffset <= offset {
				return nil, fmt.Errorf("Broker pull nextBeginOffset 未前进: current=%d next=%d", offset, batch.NextBeginOffset)
			}
			offset = batch.NextBeginOffset
		}
		result.Queues = append(result.Queues, queueResult)
	}
	return result, nil
}

// ConsumeMessages 复刻官方 consumeMessage，通过 PullConsumer 语义消费并打印消息内容。
func (c *Client) ConsumeMessages(ctx context.Context, nameServer string, options consumeMessageOptions) (*consumeMessageResult, error) {
	topic := strings.TrimSpace(options.Topic)
	if topic == "" {
		return nil, errors.New("Topic 必填")
	}
	messageCount := options.MessageCount
	if messageCount <= 0 {
		messageCount = 128
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if options.HasQueueID {
		options.Topic = topic
		options.MessageCount = messageCount
		return c.consumeMessagesByCondition(ctx, brokers, options)
	}

	queues, err := decodeTopicRouteSubscribeQueues(topic, routeBody)
	if err != nil {
		return nil, err
	}
	if len(queues) == 0 {
		return nil, errors.New("topicRoute 未返回可读队列")
	}
	result := &consumeMessageResult{}
	countLeft := messageCount
	for _, queue := range queues {
		if countLeft == 0 {
			return result, nil
		}
		broker, ok := findTopicRouteBroker(brokers, queue.BrokerName)
		if !ok {
			return nil, fmt.Errorf("topicRoute 未返回 Broker %s", queue.BrokerName)
		}
		addr := broker.selectAddr()
		if addr == "" {
			return nil, fmt.Errorf("Broker %s 未返回可用地址", queue.BrokerName)
		}
		minOffset, maxOffset, err := c.consumeMessageOffsetRange(ctx, addr, queue, options)
		if err != nil {
			return nil, err
		}
		notice := ""
		if maxOffset-minOffset > countLeft {
			notice = fmt.Sprintf("The older %d message of the %d queue will be provided", countLeft, queue.QueueID)
			maxOffset = minOffset + countLeft - 1
			countLeft = 0
		} else {
			countLeft = countLeft - (maxOffset - minOffset) - 1
		}
		if err := c.appendConsumeMessagePullEntries(ctx, result, addr, queue, options, minOffset, maxOffset, notice); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (c *Client) consumeMessagesByCondition(ctx context.Context, brokers []topicRouteBroker, options consumeMessageOptions) (*consumeMessageResult, error) {
	brokerName := strings.TrimSpace(options.BrokerName)
	if brokerName == "" {
		return nil, errors.New("Broker Name 必填")
	}
	broker, ok := findTopicRouteBroker(brokers, brokerName)
	if !ok {
		return nil, fmt.Errorf("topicRoute 未返回 Broker %s", brokerName)
	}
	addr := broker.selectAddr()
	if addr == "" {
		return nil, fmt.Errorf("Broker %s 未返回可用地址", brokerName)
	}
	queue := messageQueueIdentity{Topic: strings.TrimSpace(options.Topic), BrokerName: brokerName, QueueID: options.QueueID}
	minOffset, maxOffset, err := c.consumeMessageOffsetRange(ctx, addr, queue, options)
	if err != nil {
		return nil, err
	}
	result := &consumeMessageResult{}
	offset := int64(0)
	if options.HasOffset {
		offset = options.Offset
	}
	if offset > maxOffset {
		result.Entries = append(result.Entries, consumeMessageOutputEntry{
			StatusLine: fmt.Sprintf("%s no matched msg, offset=%d", formatMessageQueueString(queue), offset),
		})
		return result, nil
	}
	if minOffset < offset {
		minOffset = offset
	}
	notice := ""
	if maxOffset-minOffset > options.MessageCount {
		notice = fmt.Sprintf("The oldler %d message will be provided", options.MessageCount)
		maxOffset = minOffset + options.MessageCount - 1
	}
	if err := c.appendConsumeMessagePullEntries(ctx, result, addr, queue, options, minOffset, maxOffset, notice); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) consumeMessageOffsetRange(ctx context.Context, brokerAddr string, queue messageQueueIdentity, options consumeMessageOptions) (int64, int64, error) {
	minOffset, err := c.getMinOffsetAtBroker(ctx, brokerAddr, queue.Topic, queue.BrokerName, queue.QueueID)
	if err != nil {
		return 0, 0, err
	}
	maxOffset, err := c.getMaxOffsetAtBroker(ctx, brokerAddr, queue.Topic, queue.BrokerName, queue.QueueID)
	if err != nil {
		return 0, 0, err
	}
	if options.HasBeginTimestamp {
		minOffset, err = c.searchOffsetAtBroker(ctx, brokerAddr, queue.Topic, queue.BrokerName, queue.QueueID, options.BeginTimestamp)
		if err != nil {
			return 0, 0, err
		}
	}
	if options.HasEndTimestamp {
		maxOffset, err = c.searchOffsetAtBroker(ctx, brokerAddr, queue.Topic, queue.BrokerName, queue.QueueID, options.EndTimestamp)
		if err != nil {
			return 0, 0, err
		}
	}
	return minOffset, maxOffset, nil
}

func (c *Client) appendConsumeMessagePullEntries(ctx context.Context, result *consumeMessageResult, brokerAddr string, queue messageQueueIdentity, options consumeMessageOptions, minOffset int64, maxOffset int64, notice string) error {
	if maxOffset < minOffset {
		return nil
	}
	queueOptions := printMsgByQueueOptions{
		Topic:         queue.Topic,
		BrokerName:    queue.BrokerName,
		QueueID:       queue.QueueID,
		ConsumerGroup: options.ConsumerGroup,
	}
	for offset := minOffset; offset <= maxOffset; {
		maxMsgNums := int(maxOffset - offset + 1)
		if maxMsgNums <= 0 {
			return nil
		}
		batch, err := c.pullMessagesAtBroker(ctx, brokerAddr, queueOptions, offset, maxMsgNums)
		if err != nil {
			return err
		}
		entry := consumeMessageOutputEntry{Notice: notice}
		notice = ""
		if batch.Found {
			entry.Messages = batch.Messages
			result.Entries = append(result.Entries, entry)
			if batch.NextBeginOffset <= offset {
				return fmt.Errorf("Broker pull nextBeginOffset 未前进: current=%d next=%d", offset, batch.NextBeginOffset)
			}
			offset = batch.NextBeginOffset
			continue
		}
		if batch.Status == "NO_MATCHED_MSG" {
			entry.StatusLine = fmt.Sprintf("%s no matched msg. status=%s, offset=%d", formatMessageQueueString(queue), batch.Status, batch.NextBeginOffset)
			result.Entries = append(result.Entries, entry)
			if batch.NextBeginOffset <= offset {
				return nil
			}
			offset = batch.NextBeginOffset
			continue
		}
		entry.StatusLine = fmt.Sprintf("%s print msg finished. status=%s, offset=%d", formatMessageQueueString(queue), batch.Status, batch.NextBeginOffset)
		result.Entries = append(result.Entries, entry)
		return nil
	}
	return nil
}

func formatMessageQueueString(queue messageQueueIdentity) string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", queue.Topic, queue.BrokerName, queue.QueueID)
}

// PrintMessagesByQueue 复刻官方 printMsgByQueue，通过队列 offset 范围批量拉取消息。
func (c *Client) PrintMessagesByQueue(ctx context.Context, nameServer string, options printMsgByQueueOptions) (*printMsgByQueueResult, error) {
	topic := strings.TrimSpace(options.Topic)
	brokerName := strings.TrimSpace(options.BrokerName)
	if topic == "" {
		return nil, errors.New("Topic 必填")
	}
	if brokerName == "" {
		return nil, errors.New("Broker Name 必填")
	}
	routeBody, err := c.TopicRoute(ctx, nameServer, topic)
	if err != nil {
		return nil, err
	}
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	broker, ok := findTopicRouteBroker(brokers, brokerName)
	if !ok {
		return nil, fmt.Errorf("topicRoute 未返回 Broker %s", brokerName)
	}
	addr := broker.selectAddr()
	if addr == "" {
		return nil, fmt.Errorf("Broker %s 未返回可用地址", brokerName)
	}
	beginOffset, err := c.getMinOffsetAtBroker(ctx, addr, topic, brokerName, options.QueueID)
	if err != nil {
		return nil, err
	}
	endOffset, err := c.getMaxOffsetAtBroker(ctx, addr, topic, brokerName, options.QueueID)
	if err != nil {
		return nil, err
	}
	if options.HasBeginTimestamp {
		beginOffset, err = c.searchOffsetAtBroker(ctx, addr, topic, brokerName, options.QueueID, options.BeginTimestamp)
		if err != nil {
			return nil, err
		}
	}
	if options.HasEndTimestamp {
		endOffset, err = c.searchOffsetAtBroker(ctx, addr, topic, brokerName, options.QueueID, options.EndTimestamp)
		if err != nil {
			return nil, err
		}
	}

	result := &printMsgByQueueResult{Messages: make([]messageDetail, 0)}
	offset := beginOffset
	for offset < endOffset {
		batch, err := c.pullMessagesAtBroker(ctx, addr, options, offset, 32)
		if err != nil {
			return nil, err
		}
		if !batch.Found {
			break
		}
		result.Messages = append(result.Messages, batch.Messages...)
		if batch.NextBeginOffset <= offset {
			return nil, fmt.Errorf("Broker pull nextBeginOffset 未前进: current=%d next=%d", offset, batch.NextBeginOffset)
		}
		offset = batch.NextBeginOffset
	}
	return result, nil
}

type pullMessagesBatch struct {
	// Messages 是当前 pull 批次解码后的消息。
	Messages []messageDetail
	// NextBeginOffset 是 Broker 返回的下一次拉取起点。
	NextBeginOffset int64
	// Found 表示当前响应是否为 FOUND；非 FOUND 时官方结束循环。
	Found bool
	// Status 是官方 PullStatus 名称，用于 consumeMessage 输出空结果状态。
	Status string
}

func (c *Client) getMinOffsetAtBroker(ctx context.Context, brokerAddr string, topic string, brokerName string, queueID int) (int64, error) {
	return c.offsetAtBroker(ctx, brokerAddr, requestCodeGetMinOffset, map[string]string{
		"topic":      strings.TrimSpace(topic),
		"queueId":    strconv.Itoa(queueID),
		"brokerName": strings.TrimSpace(brokerName),
	})
}

func (c *Client) getMaxOffsetAtBroker(ctx context.Context, brokerAddr string, topic string, brokerName string, queueID int) (int64, error) {
	return c.offsetAtBroker(ctx, brokerAddr, requestCodeGetMaxOffset, map[string]string{
		"topic":      strings.TrimSpace(topic),
		"queueId":    strconv.Itoa(queueID),
		"brokerName": strings.TrimSpace(brokerName),
	})
}

func (c *Client) searchOffsetAtBroker(ctx context.Context, brokerAddr string, topic string, brokerName string, queueID int, timestamp int64) (int64, error) {
	return c.offsetAtBroker(ctx, brokerAddr, requestCodeSearchOffsetByTimestamp, map[string]string{
		"topic":      strings.TrimSpace(topic),
		"queueId":    strconv.Itoa(queueID),
		"brokerName": strings.TrimSpace(brokerName),
		"timestamp":  strconv.FormatInt(timestamp, 10),
	})
}

func (c *Client) offsetAtBroker(ctx context.Context, brokerAddr string, requestCode int, fields map[string]string) (int64, error) {
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:      requestCode,
		Language:  "JAVA",
		Version:   0,
		Opaque:    nextOpaque.Add(1),
		Flag:      0,
		ExtFields: fields,
	})
	if err != nil {
		return 0, err
	}
	if response.Code != responseCodeSuccess {
		return 0, fmt.Errorf("Broker offset request failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
	offsetRaw := strings.TrimSpace(response.ExtFields["offset"])
	if offsetRaw == "" {
		return 0, errors.New("Broker offset response 缺少 offset")
	}
	offset, err := strconv.ParseInt(offsetRaw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析 Broker offset 失败: %w", err)
	}
	return offset, nil
}

func (c *Client) pullMessagesAtBroker(ctx context.Context, brokerAddr string, options printMsgByQueueOptions, offset int64, maxMsgNums int) (*pullMessagesBatch, error) {
	subExpression := strings.TrimSpace(options.SubExpression)
	if subExpression == "" {
		subExpression = "*"
	}
	consumerGroup := strings.TrimSpace(options.ConsumerGroup)
	if consumerGroup == "" {
		consumerGroup = toolsConsumerGroup
	}
	response, err := c.invoke(ctx, brokerAddr, remotingCommand{
		Code:     requestCodePullMessage,
		Language: "JAVA",
		Version:  0,
		Opaque:   nextOpaque.Add(1),
		Flag:     0,
		ExtFields: map[string]string{
			"consumerGroup":        consumerGroup,
			"topic":                strings.TrimSpace(options.Topic),
			"queueId":              strconv.Itoa(options.QueueID),
			"queueOffset":          strconv.FormatInt(offset, 10),
			"maxMsgNums":           strconv.Itoa(maxMsgNums),
			"sysFlag":              "4",
			"commitOffset":         "0",
			"suspendTimeoutMillis": strconv.Itoa(defaultBrokerSuspendMaxTimeMillis),
			"subscription":         subExpression,
			"subVersion":           "0",
			"expressionType":       "TAG",
			"maxMsgBytes":          strconv.Itoa(defaultPullMaxMsgBytes),
			"bname":                strings.TrimSpace(options.BrokerName),
		},
	})
	if err != nil {
		return nil, err
	}
	nextBeginOffset, err := parsePullNextBeginOffset(response)
	if err != nil {
		return nil, err
	}
	switch response.Code {
	case responseCodeSuccess:
		details, err := decodeMessageDetailsBody(response.Body)
		if err != nil {
			return nil, err
		}
		for index := range details {
			details[index].BrokerName = strings.TrimSpace(options.BrokerName)
			details[index].QueueID = options.QueueID
			if minOffset := strings.TrimSpace(response.ExtFields["minOffset"]); minOffset != "" {
				details[index].Properties.Set("MIN_OFFSET", minOffset)
			}
			if maxOffset := strings.TrimSpace(response.ExtFields["maxOffset"]); maxOffset != "" {
				details[index].Properties.Set("MAX_OFFSET", maxOffset)
			}
		}
		return &pullMessagesBatch{Messages: details, NextBeginOffset: nextBeginOffset, Found: true}, nil
	case responseCodePullNotFound, responseCodePullRetryImmediately, responseCodePullOffsetMoved:
		return &pullMessagesBatch{NextBeginOffset: nextBeginOffset, Found: false, Status: pullStatusName(response.Code)}, nil
	default:
		return nil, fmt.Errorf("Broker printMsgByQueue pull failed: broker=%s code=%d remark=%s", brokerAddr, response.Code, response.Remark)
	}
}

func pullStatusName(responseCode int) string {
	switch responseCode {
	case responseCodePullNotFound:
		return "NO_NEW_MSG"
	case responseCodePullRetryImmediately:
		return "NO_MATCHED_MSG"
	case responseCodePullOffsetMoved:
		return "OFFSET_ILLEGAL"
	default:
		return ""
	}
}

func parsePullNextBeginOffset(response remotingCommand) (int64, error) {
	nextRaw := strings.TrimSpace(response.ExtFields["nextBeginOffset"])
	if nextRaw == "" {
		return 0, errors.New("Broker pull response 缺少 nextBeginOffset")
	}
	nextBeginOffset, err := strconv.ParseInt(nextRaw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析 pull nextBeginOffset 失败: %w", err)
	}
	return nextBeginOffset, nil
}

func findTopicRouteBroker(brokers []topicRouteBroker, brokerName string) (topicRouteBroker, bool) {
	target := strings.TrimSpace(brokerName)
	for _, broker := range brokers {
		if broker.BrokerName == target {
			return broker, true
		}
	}
	return topicRouteBroker{}, false
}

func decodeQueryMessageBody(body []byte) ([]messageSearchResult, error) {
	details, err := decodeMessageDetailsBody(body)
	if err != nil {
		return nil, err
	}
	results := make([]messageSearchResult, 0, len(details))
	for _, detail := range details {
		results = append(results, detail.toSearchResult())
	}
	return results, nil
}

func decodeMessageDetailsBody(body []byte) ([]messageDetail, error) {
	results := make([]messageDetail, 0)
	cursor := 0
	for cursor < len(body) {
		if len(body)-cursor < 4 {
			return nil, io.ErrUnexpectedEOF
		}
		totalSize := int(binary.BigEndian.Uint32(body[cursor : cursor+4]))
		if totalSize <= 0 || cursor+totalSize > len(body) {
			return nil, fmt.Errorf("message body size invalid: %d", totalSize)
		}
		record, err := decodeQueryMessageRecord(body[cursor : cursor+totalSize])
		if err != nil {
			return nil, err
		}
		if record != nil {
			results = append(results, *record)
		}
		cursor += totalSize
	}
	return results, nil
}

// dumpCompactionLogFile 复刻官方 dumpCompactionLog 的本地文件解析路径，每条记录按 MessageExt.toString 单独输出。
func dumpCompactionLogFile(fileName string) (string, error) {
	info, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("file %s not exist.", fileName)
		}
		return "", err
	}
	if info.IsDir() {
		return "", fmt.Errorf("file %s is a directory.", fileName)
	}
	file, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()

	fileSize := info.Size()
	var builder strings.Builder
	header := make([]byte, 4)
	for current := int64(0); current < fileSize; {
		if current+4 > fileSize {
			break
		}
		if _, err := file.ReadAt(header, current); err != nil {
			return "", err
		}
		recordSize := int64(int32(binary.BigEndian.Uint32(header)))
		if recordSize <= 0 || recordSize > fileSize {
			break
		}
		if current+recordSize > fileSize {
			break
		}
		record := make([]byte, recordSize)
		if _, err := file.ReadAt(record, current); err != nil {
			return "", err
		}
		message, err := decodeQueryMessageRecord(record)
		if err != nil {
			return "", err
		}
		if message == nil {
			break
		}
		builder.WriteString(formatDumpCompactionLogMessageExtString(*message))
		builder.WriteByte('\n')
		current += recordSize
	}
	return builder.String(), nil
}

func (detail messageDetail) toSearchResult() messageSearchResult {
	keys := splitMessageKeys(detail.Keys)
	messageID := detail.DisplayMessageID
	if strings.TrimSpace(messageID) == "" {
		messageID = detail.OffsetMessageID
	}
	return messageSearchResult{
		MessageID:   messageID,
		QueueID:     detail.QueueID,
		QueueOffset: detail.QueueOffset,
		Topic:       detail.Topic,
		Keys:        keys,
		UniqKey:     detail.Properties.Get("UNIQ_KEY"),
	}
}

func decodeQueryMessageRecord(raw []byte) (*messageDetail, error) {
	pos := 0
	readInt32 := func() (int32, error) {
		if pos+4 > len(raw) {
			return 0, io.ErrUnexpectedEOF
		}
		value := int32(binary.BigEndian.Uint32(raw[pos : pos+4]))
		pos += 4
		return value, nil
	}
	readInt64 := func() (int64, error) {
		if pos+8 > len(raw) {
			return 0, io.ErrUnexpectedEOF
		}
		value := int64(binary.BigEndian.Uint64(raw[pos : pos+8]))
		pos += 8
		return value, nil
	}
	readBytes := func(length int) ([]byte, error) {
		if length < 0 || pos+length > len(raw) {
			return nil, io.ErrUnexpectedEOF
		}
		value := append([]byte(nil), raw[pos:pos+length]...)
		pos += length
		return value, nil
	}
	readPort := func() (int32, error) {
		return readInt32()
	}

	storeSize32, err := readInt32()
	if err != nil {
		return nil, err
	}
	magicCode, err := readInt32()
	if err != nil {
		return nil, err
	}
	topicLenSize, err := topicLengthSizeFromMagicCode(magicCode)
	if err != nil {
		return nil, err
	}
	bodyCRC, err := readInt32()
	if err != nil {
		return nil, err
	}
	queueID32, err := readInt32()
	if err != nil {
		return nil, err
	}
	flag, err := readInt32()
	if err != nil {
		return nil, err
	}
	queueOffset, err := readInt64()
	if err != nil {
		return nil, err
	}
	commitLogOffset, err := readInt64()
	if err != nil {
		return nil, err
	}
	sysFlag, err := readInt32()
	if err != nil {
		return nil, err
	}
	bornTimestamp, err := readInt64()
	if err != nil {
		return nil, err
	}
	bornHostIPv6 := (sysFlag & 0x10) != 0
	bornHostIPLength := 4
	if bornHostIPv6 {
		bornHostIPLength = 16
	}
	bornHostIP, err := readBytes(bornHostIPLength)
	if err != nil {
		return nil, err
	}
	bornHostPort, err := readPort()
	if err != nil {
		return nil, err
	}
	storeTimestamp, err := readInt64()
	if err != nil {
		return nil, err
	}
	storeHostIPv6 := (sysFlag & 0x20) != 0
	storeHostIPLength := 4
	if storeHostIPv6 {
		storeHostIPLength = 16
	}
	storeHostIP, err := readBytes(storeHostIPLength)
	if err != nil {
		return nil, err
	}
	storeHostPort, err := readPort()
	if err != nil {
		return nil, err
	}
	reconsumeTimes, err := readInt32()
	if err != nil {
		return nil, err
	}
	preparedTransactionOffset, err := readInt64()
	if err != nil {
		return nil, err
	}
	bodyLen32, err := readInt32()
	if err != nil {
		return nil, err
	}
	if bodyLen32 < 0 {
		return nil, errors.New("message body length invalid")
	}
	bodyBytes, err := readBytes(int(bodyLen32))
	if err != nil {
		return nil, err
	}
	topicLen := 0
	switch topicLenSize {
	case 1:
		topicLenByte, err := readBytes(1)
		if err != nil {
			return nil, err
		}
		topicLen = int(topicLenByte[0])
	case 2:
		var value uint16
		if pos+2 > len(raw) {
			return nil, io.ErrUnexpectedEOF
		}
		value = binary.BigEndian.Uint16(raw[pos : pos+2])
		pos += 2
		topicLen = int(value)
	default:
		return nil, fmt.Errorf("unsupported topic length size %d", topicLenSize)
	}
	topicBytes, err := readBytes(topicLen)
	if err != nil {
		return nil, err
	}
	propertiesLen, err := func() (int, error) {
		if pos+2 > len(raw) {
			return 0, io.ErrUnexpectedEOF
		}
		value := int(binary.BigEndian.Uint16(raw[pos : pos+2]))
		pos += 2
		return value, nil
	}()
	if err != nil {
		return nil, err
	}
	propertiesBytes, err := readBytes(propertiesLen)
	if err != nil {
		return nil, err
	}
	properties := decodeMessageProperties(string(propertiesBytes))
	offsetMessageID := createMessageID(storeHostIP, storeHostPort, commitLogOffset)
	uniqKey := strings.TrimSpace(properties.Get("UNIQ_KEY"))
	displayMessageID := offsetMessageID
	if uniqKey != "" {
		displayMessageID = uniqKey
	}
	result := &messageDetail{
		OffsetMessageID:           offsetMessageID,
		DisplayMessageID:          displayMessageID,
		Topic:                     string(topicBytes),
		Tags:                      properties.Get("TAGS"),
		Keys:                      properties.Get("KEYS"),
		QueueID:                   int(queueID32),
		QueueOffset:               queueOffset,
		CommitLogOffset:           commitLogOffset,
		StoreSize:                 int(storeSize32),
		BodyCRC:                   bodyCRC,
		Flag:                      flag,
		ReconsumeTimes:            int(reconsumeTimes),
		PreparedTransactionOffset: preparedTransactionOffset,
		BornTimestamp:             bornTimestamp,
		StoreTimestamp:            storeTimestamp,
		BornHost:                  formatHost(bornHostIP, bornHostPort),
		StoreHost:                 formatHost(storeHostIP, storeHostPort),
		SysFlag:                   int(sysFlag),
		Properties:                properties,
		Body:                      bodyBytes,
	}
	return result, nil
}

func formatMessageSearchResults(results []messageSearchResult) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-50s %4s %40s\n", "#Message ID", "#QID", "#Offset"))
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("%-50s %4d %40d\n", result.MessageID, result.QueueID, result.QueueOffset))
	}
	return builder.String()
}

func formatPrintMsg(result *printMsgResult, options printMsgOptions) (string, error) {
	if result == nil {
		return "", nil
	}
	charsetName := strings.TrimSpace(options.CharsetName)
	if charsetName == "" {
		charsetName = "UTF-8"
	}
	printBody := true
	if options.HasPrintBody {
		printBody = options.PrintBody
	}
	var builder strings.Builder
	for _, queue := range result.Queues {
		builder.WriteString(fmt.Sprintf("minOffset=%d, maxOffset=%d, %s\n", queue.MinOffset, queue.MaxOffset, formatMessageQueueIdentity(queue.Queue)))
		for _, message := range queue.Messages {
			bodyText := "NOT PRINT BODY"
			if printBody {
				decoded, err := decodeBodyByCharset(message.Body, charsetName)
				if err != nil {
					return "", err
				}
				bodyText = decoded
			}
			builder.WriteString(fmt.Sprintf("MSGID: %s %s BODY: %s\n", message.DisplayMessageID, formatMessageExtString(message), bodyText))
		}
		builder.WriteString("--------------------------------------------------------\n")
	}
	return builder.String(), nil
}

func formatConsumeMessage(result *consumeMessageResult) (string, error) {
	if result == nil {
		return "", nil
	}
	var builder strings.Builder
	for _, notice := range result.Notices {
		if strings.TrimSpace(notice) != "" {
			builder.WriteString(notice)
			builder.WriteByte('\n')
		}
	}
	if len(result.Messages) > 0 {
		builder.WriteString("Consume ok\n")
		for index := range result.Messages {
			message := result.Messages[index]
			decoded, err := decodeBodyByCharset(message.Body, "UTF-8")
			if err != nil {
				return "", err
			}
			builder.WriteString(fmt.Sprintf("MSGID: %s %s BODY: %s\n", message.DisplayMessageID, formatMessageExtString(message), decoded))
		}
	}
	for _, entry := range result.Entries {
		if strings.TrimSpace(entry.Notice) != "" {
			builder.WriteString(entry.Notice)
			builder.WriteByte('\n')
		}
		if len(entry.Messages) > 0 {
			builder.WriteString("Consume ok\n")
			for index := range entry.Messages {
				message := entry.Messages[index]
				decoded, err := decodeBodyByCharset(message.Body, "UTF-8")
				if err != nil {
					return "", err
				}
				builder.WriteString(fmt.Sprintf("MSGID: %s %s BODY: %s\n", message.DisplayMessageID, formatMessageExtString(message), decoded))
			}
		}
		if strings.TrimSpace(entry.StatusLine) != "" {
			builder.WriteString(entry.StatusLine)
			builder.WriteByte('\n')
		}
	}
	return builder.String(), nil
}

func formatPrintMsgByQueue(result *printMsgByQueueResult, options printMsgByQueueOptions) (string, error) {
	if result == nil {
		return "", nil
	}
	charsetName := strings.TrimSpace(options.CharsetName)
	if charsetName == "" {
		charsetName = "UTF-8"
	}
	var builder strings.Builder
	if options.PrintMessage {
		for index := range result.Messages {
			message := result.Messages[index]
			bodyText := "NOT PRINT BODY"
			if options.PrintBody {
				decoded, err := decodeBodyByCharset(message.Body, charsetName)
				if err != nil {
					return "", err
				}
				bodyText = decoded
			}
			builder.WriteString(fmt.Sprintf("MSGID: %s %s BODY: %s\n", message.DisplayMessageID, formatMessageExtString(message), bodyText))
		}
	}
	if options.CalculateByTag {
		for _, item := range calculatePrintMsgByQueueTagCounts(result.Messages) {
			builder.WriteString(fmt.Sprintf("Tag: %-30s Count: %d\n", item.Tag, item.Count))
		}
	}
	return builder.String(), nil
}

func formatMessageExtString(detail messageDetail) string {
	return formatMessageExtStringWithBody(detail, formatJavaByteArray(detail.Body))
}

// formatDumpCompactionLogMessageExtString 复刻 MessageDecoder.decode(readBody=false) 后的 MessageExt.toString，body 固定为 null。
func formatDumpCompactionLogMessageExtString(detail messageDetail) string {
	if strings.TrimSpace(detail.BrokerName) == "" {
		detail.BrokerName = "null"
	}
	return formatMessageExtStringWithBody(detail, "null")
}

func formatMessageExtStringWithBody(detail messageDetail, bodyText string) string {
	return fmt.Sprintf(
		"MessageExt [brokerName=%s, queueId=%d, storeSize=%d, queueOffset=%d, sysFlag=%d, bornTimestamp=%d, bornHost=/%s, storeTimestamp=%d, storeHost=/%s, msgId=%s, commitLogOffset=%d, bodyCRC=%d, reconsumeTimes=%d, preparedTransactionOffset=%d, toString()=%s]",
		detail.BrokerName,
		detail.QueueID,
		detail.StoreSize,
		detail.QueueOffset,
		detail.SysFlag,
		detail.BornTimestamp,
		detail.BornHost,
		detail.StoreTimestamp,
		detail.StoreHost,
		detail.OffsetMessageID,
		detail.CommitLogOffset,
		detail.BodyCRC,
		detail.ReconsumeTimes,
		detail.PreparedTransactionOffset,
		formatRocketMQMessageStringWithBody(detail, bodyText),
	)
}

func formatRocketMQMessageString(detail messageDetail) string {
	return formatRocketMQMessageStringWithBody(detail, formatJavaByteArray(detail.Body))
}

func formatRocketMQMessageStringWithBody(detail messageDetail, bodyText string) string {
	return fmt.Sprintf("Message{topic='%s', flag=%d, properties=%s, body=%s, transactionId='null'}",
		detail.Topic,
		detail.Flag,
		formatJavaHashMapProperties(detail.Properties),
		bodyText,
	)
}

func formatJavaByteArray(body []byte) string {
	var builder strings.Builder
	builder.WriteByte('[')
	for index, item := range body {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(strconv.Itoa(int(int8(item))))
	}
	builder.WriteByte(']')
	return builder.String()
}

func calculatePrintMsgByQueueTagCounts(messages []messageDetail) []printMsgByQueueTagCount {
	counts := make(map[string]int64)
	for _, message := range messages {
		tag := message.Tags
		// 官方只用 trim 后的结果判空，计数 key 仍保留 MessageExt.getTags() 原始文本。
		if strings.TrimSpace(tag) == "" {
			continue
		}
		counts[tag]++
	}
	tags := make([]string, 0, len(counts))
	for tag := range counts {
		tags = append(tags, tag)
	}
	capacity := javaHashMapCapacity(len(tags))
	sort.SliceStable(tags, func(i, j int) bool {
		return javaHashMapBucketWithCapacity(tags[i], capacity) < javaHashMapBucketWithCapacity(tags[j], capacity)
	})
	results := make([]printMsgByQueueTagCount, 0, len(tags))
	for _, tag := range tags {
		results = append(results, printMsgByQueueTagCount{Tag: tag, Count: counts[tag]})
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].Count > results[j].Count
	})
	return results
}

func formatMessageDetail(detail *messageDetail, bodyFormat string) (string, error) {
	return formatMessageDetailWithTracks(detail, bodyFormat, true, nil)
}

// formatMessageDetailForUniqueKey 复刻官方 queryMsgByUniqueKey.showMessage 输出，该命令不打印 OffsetID。
func formatMessageDetailForUniqueKey(detail *messageDetail) (string, error) {
	return formatMessageDetailWithTracks(detail, "", false, nil)
}

// formatMessageDetailsForUniqueKey 按官方 -a/showAll 语义逐条打印匹配消息。
func formatMessageDetailsForUniqueKey(details []messageDetail) (string, error) {
	var builder strings.Builder
	for index := range details {
		if index > 0 {
			builder.WriteString("\n")
		}
		output, err := formatMessageDetailForUniqueKey(&details[index])
		if err != nil {
			return "", err
		}
		builder.WriteString(output)
	}
	return builder.String(), nil
}

func formatConsumeMessageDirectlyResult(result *consumeMessageDirectlyResult) string {
	if result == nil {
		result = &consumeMessageDirectlyResult{}
	}
	remark := "null"
	if result.Remark != nil {
		remark = *result.Remark
	}
	return fmt.Sprintf("ConsumeMessageDirectlyResult [order=%s, autoCommit=%s, consumeResult=%s, remark=%s, spentTimeMills=%d]",
		strconv.FormatBool(result.Order),
		strconv.FormatBool(result.AutoCommit),
		result.ConsumeResult,
		remark,
		result.SpentTimeMills)
}

func formatConsumeMessageDirectlyUnavailable(err *consumeMessageDirectlyUnavailableError) string {
	if err == nil {
		return ""
	}
	clientID := strings.TrimSpace(err.ClientID)
	var builder strings.Builder
	if err.RunningInfoFailed {
		builder.WriteString(fmt.Sprintf("get consumer runtime info for %s client failed \n", clientID))
	}
	builder.WriteString(fmt.Sprintf("get consumer info failed or this %s client is not push consumer ,not support direct push \n", clientID))
	return builder.String()
}

// formatMessageDetailWithTracks 统一消息详情格式化逻辑，并按官方子命令差异控制 OffsetID 和 MessageTrack 后缀。
func formatMessageDetailWithTracks(detail *messageDetail, bodyFormat string, includeOffsetID bool, tracks []messageTrack) (string, error) {
	if detail == nil {
		return "", nil
	}
	bodyPath, err := writeMessageBody(detail)
	if err != nil {
		return "", err
	}
	tags := nullText(detail.Tags)
	keys := nullText(detail.Keys)

	var builder strings.Builder
	if includeOffsetID {
		builder.WriteString(fmt.Sprintf("%-20s %s\n", "OffsetID:", detail.OffsetMessageID))
	}
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Topic:", detail.Topic))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Tags:", "["+tags+"]"))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Keys:", "["+keys+"]"))
	builder.WriteString(fmt.Sprintf("%-20s %d\n", "Queue ID:", detail.QueueID))
	builder.WriteString(fmt.Sprintf("%-20s %d\n", "Queue Offset:", detail.QueueOffset))
	builder.WriteString(fmt.Sprintf("%-20s %d\n", "CommitLog Offset:", detail.CommitLogOffset))
	builder.WriteString(fmt.Sprintf("%-20s %d\n", "Reconsume Times:", detail.ReconsumeTimes))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Born Timestamp:", formatRocketMQMillis(detail.BornTimestamp)))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Store Timestamp:", formatRocketMQMillis(detail.StoreTimestamp)))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Born Host:", detail.BornHost))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Store Host:", detail.StoreHost))
	builder.WriteString(fmt.Sprintf("%-20s %d\n", "System Flag:", detail.SysFlag))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Properties:", formatJavaHashMapProperties(detail.Properties)))
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "Message Body Path:", bodyPath))
	if strings.TrimSpace(bodyFormat) != "" {
		body, err := decodeBodyByCharset(detail.Body, bodyFormat)
		if err != nil {
			return "", err
		}
		builder.WriteString(fmt.Sprintf("%-20s %s\n", "Message Body:", body))
	}
	builder.WriteString(formatMessageTracks(tracks))
	return builder.String(), nil
}

func formatMessageTracks(tracks []messageTrack) string {
	if len(tracks) == 0 {
		return "\n\nWARN: No Consumer"
	}
	var builder strings.Builder
	builder.WriteString("\n\n")
	for _, track := range tracks {
		exceptionDesc := strings.TrimSpace(track.ExceptionDesc)
		if exceptionDesc == "" {
			exceptionDesc = "null"
		}
		trackType := strings.TrimSpace(track.TrackType)
		if trackType == "" {
			trackType = "UNKNOWN"
		}
		builder.WriteString(fmt.Sprintf("MessageTrack [consumerGroup=%s, trackType=%s, exceptionDesc=%s]\n",
			strings.TrimSpace(track.ConsumerGroup),
			trackType,
			exceptionDesc))
	}
	return builder.String()
}

func decodeTraceViewsFromMessages(key string, messages []messageDetail) ([]messageTraceView, error) {
	views := make([]messageTraceView, 0)
	for _, message := range messages {
		body := string(message.Body)
		if body == "" {
			continue
		}
		contexts := javaStyleSplit(body, string(traceFieldSplitter))
		for _, contextLine := range contexts {
			if contextLine == "" {
				continue
			}
			fields := javaStyleSplit(contextLine, string(traceContentSplitter))
			if len(fields) == 0 {
				continue
			}
			switch fields[0] {
			case "Pub":
				view, ok, err := decodePubTraceView(key, traceClientHost(message.BornHost), fields)
				if err != nil {
					return nil, err
				}
				if ok {
					views = append(views, view)
				}
			case "SubAfter":
				view, ok, err := decodeSubAfterTraceView(key, traceClientHost(message.BornHost), fields)
				if err != nil {
					return nil, err
				}
				if ok {
					views = append(views, view)
				}
			}
		}
	}
	return views, nil
}

func decodePubTraceView(key string, clientHost string, fields []string) (messageTraceView, bool, error) {
	if len(fields) < 13 {
		return messageTraceView{}, false, fmt.Errorf("Pub trace fields invalid: %d", len(fields))
	}
	if fields[5] != key {
		return messageTraceView{}, false, nil
	}
	timestamp, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return messageTraceView{}, false, fmt.Errorf("解析 Pub trace timestamp 失败: %w", err)
	}
	costTime, err := strconv.Atoi(fields[10])
	if err != nil {
		return messageTraceView{}, false, fmt.Errorf("解析 Pub trace costTime 失败: %w", err)
	}
	offsetMessageID := ""
	successRaw := fields[12]
	if len(fields) >= 14 {
		offsetMessageID = fields[12]
		successRaw = fields[13]
	}
	return messageTraceView{
		MsgID:           fields[5],
		Tags:            fields[6],
		Keys:            fields[7],
		StoreHost:       fields[8],
		ClientHost:      clientHost,
		CostTime:        costTime,
		MsgType:         "Pub",
		OffsetMessageID: offsetMessageID,
		TimeStamp:       timestamp,
		Topic:           fields[4],
		GroupName:       fields[3],
		Status:          traceStatus(successRaw),
	}, true, nil
}

func decodeSubAfterTraceView(key string, clientHost string, fields []string) (messageTraceView, bool, error) {
	if len(fields) < 6 {
		return messageTraceView{}, false, fmt.Errorf("SubAfter trace fields invalid: %d", len(fields))
	}
	if fields[2] != key {
		return messageTraceView{}, false, nil
	}
	costTime, err := strconv.Atoi(fields[3])
	if err != nil {
		return messageTraceView{}, false, fmt.Errorf("解析 SubAfter trace costTime 失败: %w", err)
	}
	timestamp := time.Now().UnixMilli()
	groupName := ""
	if len(fields) >= 9 {
		timestamp, err = strconv.ParseInt(fields[7], 10, 64)
		if err != nil {
			return messageTraceView{}, false, fmt.Errorf("解析 SubAfter trace timestamp 失败: %w", err)
		}
		groupName = fields[8]
	}
	return messageTraceView{
		MsgID:      fields[2],
		Keys:       fields[5],
		ClientHost: clientHost,
		CostTime:   costTime,
		MsgType:    "SubAfter",
		TimeStamp:  timestamp,
		GroupName:  groupName,
		Status:     traceStatus(fields[4]),
	}, true, nil
}

func traceStatus(success string) string {
	if strings.EqualFold(strings.TrimSpace(success), "true") {
		return "success"
	}
	return "failed"
}

func traceClientHost(bornHost string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(bornHost))
	if err == nil {
		return host
	}
	return strings.TrimSpace(bornHost)
}

func javaStyleSplit(value string, separator string) []string {
	parts := strings.Split(value, separator)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func formatMessageTrace(views []messageTraceView) string {
	var builder strings.Builder
	consumerTraceMap := make(map[string][]messageTraceView)
	consumerGroups := make([]string, 0)
	seenGroups := make(map[string]bool)
	for _, view := range views {
		switch view.MsgType {
		case "Pub":
			builder.WriteString(fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "#Type", "#ProducerGroup", "#ClientHost", "#SendTime", "#CostTimes", "#Status"))
			builder.WriteString(fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "Pub", view.GroupName, view.ClientHost, formatTraceTime(view.TimeStamp), strconv.Itoa(view.CostTime)+"ms", view.Status))
			builder.WriteString("\n")
		case "SubAfter":
			if !seenGroups[view.GroupName] {
				seenGroups[view.GroupName] = true
				consumerGroups = append(consumerGroups, view.GroupName)
			}
			consumerTraceMap[view.GroupName] = append(consumerTraceMap[view.GroupName], view)
		}
	}
	sort.SliceStable(consumerGroups, func(i, j int) bool {
		return javaHashMapBucketWithCapacity(consumerGroups[i], 16) < javaHashMapBucketWithCapacity(consumerGroups[j], 16)
	})
	for _, groupName := range consumerGroups {
		builder.WriteString(fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "#Type", "#ConsumerGroup", "#ClientHost", "#ConsumerTime", "#CostTimes", "#Status"))
		for _, view := range consumerTraceMap[groupName] {
			builder.WriteString(fmt.Sprintf("%-10s %-20s %-20s %-20s %-10s %-10s\n", "Sub", view.GroupName, view.ClientHost, formatTraceTime(view.TimeStamp), strconv.Itoa(view.CostTime)+"ms", view.Status))
		}
		builder.WriteString("\n")
	}
	return builder.String()
}

func formatConsumerProgress(progress *consumerProgress) string {
	if progress == nil {
		progress = &consumerProgress{}
	}
	entries := append([]consumerProgressEntry(nil), progress.Entries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Topic != entries[j].Topic {
			return entries[i].Topic < entries[j].Topic
		}
		if entries[i].BrokerName != entries[j].BrokerName {
			return entries[i].BrokerName < entries[j].BrokerName
		}
		return entries[i].QueueID < entries[j].QueueID
	})

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s%s\n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#Inflight", "#LastTime"))
	var diffTotal int64
	var inflightTotal int64
	for _, entry := range entries {
		diff := entry.BrokerOffset - entry.ConsumerOffset
		inflight := entry.PullOffset - entry.ConsumerOffset
		diffTotal += diff
		inflightTotal += inflight
		lastTime := "N/A"
		if entry.LastTimestamp != 0 {
			lastTime = formatTraceTime(entry.LastTimestamp)
		}
		builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20d %-20d %s\n",
			frontStringAtLeast(entry.Topic, 64),
			frontStringAtLeast(entry.BrokerName, 32),
			entry.QueueID,
			entry.BrokerOffset,
			entry.ConsumerOffset,
			diff,
			inflight,
			lastTime,
		))
	}
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("Consume TPS: %.2f\n", progress.ConsumeTPS))
	builder.WriteString(fmt.Sprintf("Consume Diff Total: %d\n", diffTotal))
	builder.WriteString(fmt.Sprintf("Consume Inflight Total: %d\n", inflightTotal))
	return builder.String()
}

func formatBrokerConsumeStats(stats *brokerConsumeStats, level int64) string {
	if stats == nil {
		stats = &brokerConsumeStats{}
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-64s  %-64s  %-32s  %-4s  %-20s  %-20s  %-20s  %s\n", "#Topic", "#Group", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Diff", "#LastTime"))
	for _, group := range stats.Groups {
		for _, consumeStats := range group.Stats {
			entries := append([]consumerProgressEntry(nil), consumeStats.Entries...)
			sort.Slice(entries, func(i, j int) bool {
				if entries[i].Topic != entries[j].Topic {
					return entries[i].Topic < entries[j].Topic
				}
				if entries[i].BrokerName != entries[j].BrokerName {
					return entries[i].BrokerName < entries[j].BrokerName
				}
				return entries[i].QueueID < entries[j].QueueID
			})
			for _, entry := range entries {
				diff := entry.BrokerOffset - entry.ConsumerOffset
				if diff < level || entry.LastTimestamp <= 0 {
					continue
				}
				builder.WriteString(fmt.Sprintf("%-64s  %-64s  %-32s  %-4d  %-20d  %-20d  %-20d  %s\n",
					frontStringAtLeast(entry.Topic, 64),
					group.Group,
					frontStringAtLeast(entry.BrokerName, 32),
					entry.QueueID,
					entry.BrokerOffset,
					entry.ConsumerOffset,
					diff,
					formatTraceTime(entry.LastTimestamp),
				))
			}
		}
	}
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("Diff Total: %d\n", stats.TotalDiff))
	return builder.String()
}

func formatStatsAll(rows []statsAllRow) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-64s  %-64s %12s %11s %11s %14s %14s\n", "#Topic", "#Consumer Group", "#Accumulation", "#InTPS", "#OutTPS", "#InMsg24Hour", "#OutMsg24Hour"))
	for _, row := range rows {
		if row.NoConsumer {
			builder.WriteString(fmt.Sprintf("%-64s  %-64s %12d %11.2f %11s %14d %14s\n",
				frontStringAtLeast(row.Topic, 64),
				"",
				row.Accumulation,
				row.InTPS,
				"",
				row.InMsg24Hour,
				"NO_CONSUMER",
			))
			continue
		}
		builder.WriteString(fmt.Sprintf("%-64s  %-64s %12d %11.2f %11.2f %14d %14d\n",
			frontStringAtLeast(row.Topic, 64),
			frontStringAtLeast(row.ConsumerGroup, 64),
			row.Accumulation,
			row.InTPS,
			row.OutTPS,
			row.InMsg24Hour,
			row.OutMsg24Hour,
		))
	}
	return builder.String()
}

func formatHAStatus(addr string, status *haStatusResult) string {
	if status == nil {
		status = &haStatusResult{}
	}
	var builder strings.Builder
	if status.Master {
		builder.WriteString(fmt.Sprintf("\n#MasterAddr\t%s\n#MasterCommitLogMaxOffset\t%d\n#SlaveNum\t%d\n#InSyncSlaveNum\t%d\n",
			addr,
			status.MasterCommitLogMaxOffset,
			len(status.HAConnectionInfo),
			status.InSyncSlaveNums,
		))
		builder.WriteString(fmt.Sprintf("%-32s  %-16s %16s %16s %16s %16s\n", "#SlaveAddr", "#SlaveAckOffset", "#Diff", "#TransferSpeed(KB/s)", "#Status", "#TransferFromWhere"))
		for _, connection := range status.HAConnectionInfo {
			statusText := "Fall Behind"
			if connection.InSync {
				statusText = "OK"
			}
			builder.WriteString(fmt.Sprintf("%-32s  %-16d %16d %16.2f %16s %16d\n",
				frontStringAtLeast(connection.Addr, 32),
				connection.SlaveAckOffset,
				connection.Diff,
				float64(connection.TransferredByteInSecond)/1024.0,
				statusText,
				connection.TransferFromWhere,
			))
		}
		return builder.String()
	}
	client := status.HAClientRuntimeInfo
	masterAddr := strings.TrimSpace(client.MasterAddr)
	if masterAddr == "" {
		masterAddr = strings.TrimSpace(addr)
	}
	builder.WriteString(fmt.Sprintf("\n#MasterAddr\t%s\n", masterAddr))
	builder.WriteString(fmt.Sprintf("#CommitLogMaxOffset\t%d\n", client.MaxOffset))
	builder.WriteString(fmt.Sprintf("#TransferSpeed(KB/s)\t%.2f\n", float64(client.TransferredByteInSecond)/1024.0))
	builder.WriteString(fmt.Sprintf("#LastReadTime\t%s\n", formatTraceTime(client.LastReadTimestamp)))
	builder.WriteString(fmt.Sprintf("#LastWriteTime\t%s\n", formatTraceTime(client.LastWriteTimestamp)))
	builder.WriteString(fmt.Sprintf("#MasterFlushOffset\t%d\n", client.MasterFlushOffset))
	return builder.String()
}

// formatControllerMetaData 复刻官方 getControllerMetaData 的逐行 stdout，包括开头空行和 peer 行。
func formatControllerMetaData(meta *controllerMetaData) string {
	if meta == nil {
		meta = &controllerMetaData{}
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("\n#ControllerGroup\t%s", meta.Group))
	builder.WriteString(fmt.Sprintf("\n#ControllerLeaderId\t%s", meta.ControllerLeaderID))
	builder.WriteString(fmt.Sprintf("\n#ControllerLeaderAddress\t%s", meta.ControllerLeaderAddress))
	if strings.TrimSpace(meta.Peers) != "" {
		for _, peer := range strings.Split(meta.Peers, ";") {
			if peer == "" {
				continue
			}
			builder.WriteString(fmt.Sprintf("\n#Peer:\t%s", peer))
		}
	}
	builder.WriteString("\n")
	return builder.String()
}

func formatCheckRocksdbCqWriteProgress(rows []checkRocksdbCqWriteProgressRow) string {
	var builder strings.Builder
	for _, row := range rows {
		if row.CheckError {
			builder.WriteString(row.BrokerName)
			builder.WriteString(" check error, please check log... errInfo:")
			builder.WriteString(row.ErrorInfo)
			continue
		}
		builder.WriteString(row.BrokerName)
		builder.WriteString(" check doing, please wait and get the result from log... \n")
	}
	return builder.String()
}

func parseExportPopRecordDryRun(args []string) bool {
	return strings.EqualFold(stringArg(args, "-d", "--dryRun"), "false")
}

func formatExportPopRecord(rows []exportPopRecordRow) string {
	var builder strings.Builder
	for _, row := range rows {
		if row.Err != nil {
			builder.WriteString(fmt.Sprintf("Export broker records error, brokerName=%s, brokerAddr=%s, dryRun=%t\n%s",
				row.BrokerName,
				row.BrokerAddr,
				row.DryRun,
				row.Err,
			))
			continue
		}
		builder.WriteString(fmt.Sprintf("Export broker records, brokerName=%s, brokerAddr=%s, dryRun=%t\n",
			row.BrokerName,
			row.BrokerAddr,
			row.DryRun,
		))
	}
	return builder.String()
}

func parseUpdateBrokerConfigOptions(args []string) updateBrokerConfigOptions {
	return updateBrokerConfigOptions{
		NameServer:      stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:      stringArg(args, "-b", "--brokerAddr"),
		ClusterName:     stringArg(args, "-c", "--clusterName"),
		Key:             strings.TrimSpace(stringArg(args, "-k", "--key")),
		Value:           strings.TrimSpace(stringArg(args, "-v", "--value")),
		UpdateAllBroker: hasFlag(args, "-a", "--updateAllBroker"),
	}
}

func parseUpdateNamesrvConfigOptions(args []string) updateNamesrvConfigOptions {
	return updateNamesrvConfigOptions{
		NameServers: stringArg(args, "-n", "--namesrvAddr"),
		Key:         strings.TrimSpace(stringArg(args, "-k", "--key")),
		Value:       strings.TrimSpace(stringArg(args, "-v", "--value")),
	}
}

func parseUpdateControllerConfigOptions(args []string) updateControllerConfigOptions {
	return updateControllerConfigOptions{
		ControllerAddrs: stringArg(args, "-a", "--controllerAddress"),
		Key:             strings.TrimSpace(stringArg(args, "-k", "--key")),
		Value:           strings.TrimSpace(stringArg(args, "-v", "--value")),
	}
}

func parseCleanBrokerMetadataOptions(args []string) (cleanBrokerMetadataOptions, error) {
	options := cleanBrokerMetadataOptions{
		ControllerAddr:             strings.TrimSpace(stringArg(args, "-a", "--controllerAddress")),
		ClusterName:                strings.TrimSpace(stringArg(args, "-c", "--clusterName")),
		BrokerName:                 strings.TrimSpace(stringArg(args, "-bn", "--brokerName")),
		BrokerControllerIDsToClean: strings.TrimSpace(stringArg(args, "-b", "--brokerControllerIdsToClean")),
		CleanLivingBroker:          hasFlag(args, "-l", "--cleanLivingBroker"),
	}
	if options.ControllerAddr == "" {
		return options, errors.New("ControllerAddress 必填")
	}
	if options.BrokerName == "" {
		return options, errors.New("BrokerName 必填")
	}
	if !options.CleanLivingBroker && options.ClusterName == "" {
		return options, errors.New("cleanLivingBroker option is false, clusterName option can not be empty.")
	}
	return options, nil
}

func parseElectMasterOptions(args []string) (electMasterOptions, error) {
	options := electMasterOptions{
		ControllerAddr: strings.TrimSpace(stringArg(args, "-a", "--controllerAddress")),
		ClusterName:    strings.TrimSpace(stringArg(args, "-c", "--clusterName")),
		BrokerName:     strings.TrimSpace(stringArg(args, "-bn", "--brokerName")),
	}
	brokerIDText := strings.TrimSpace(stringArg(args, "-b", "--brokerId"))
	if options.ControllerAddr == "" {
		return options, errors.New("ControllerAddress 必填")
	}
	if options.ClusterName == "" {
		return options, errors.New("ClusterName 必填")
	}
	if options.BrokerName == "" {
		return options, errors.New("BrokerName 必填")
	}
	if brokerIDText == "" {
		return options, errors.New("BrokerId 必填")
	}
	brokerID, err := strconv.ParseInt(brokerIDText, 10, 64)
	if err != nil {
		return options, err
	}
	options.BrokerID = brokerID
	return options, nil
}

func parseAddBrokerOptions(args []string) (addBrokerOptions, error) {
	options := addBrokerOptions{
		BrokerContainerAddr: strings.TrimSpace(stringArg(args, "-c", "--brokerContainerAddr")),
		BrokerConfigPath:    strings.TrimSpace(stringArg(args, "-b", "--brokerConfigPath")),
	}
	if options.BrokerContainerAddr == "" {
		return options, errors.New("BrokerContainerAddr 必填")
	}
	if options.BrokerConfigPath == "" {
		return options, errors.New("BrokerConfigPath 必填")
	}
	return options, nil
}

func parseRemoveBrokerOptions(args []string) (removeBrokerOptions, error) {
	options := removeBrokerOptions{
		BrokerContainerAddr: strings.TrimSpace(stringArg(args, "-c", "--brokerContainerAddr")),
		BrokerIdentity:      strings.TrimSpace(stringArg(args, "-b", "--brokerIdentity")),
	}
	if options.BrokerContainerAddr == "" {
		return options, errors.New("BrokerContainerAddr 必填")
	}
	if options.BrokerIdentity == "" {
		return options, errors.New("BrokerIdentity 必填")
	}
	parts := strings.Split(options.BrokerIdentity, ":")
	if len(parts) < 3 {
		return options, errors.New("BrokerIdentity 必须为 clusterName:brokerName:brokerId")
	}
	options.ClusterName = strings.TrimSpace(parts[0])
	options.BrokerName = strings.TrimSpace(parts[1])
	brokerID, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
	if err != nil {
		return options, err
	}
	options.BrokerID = brokerID
	return options, nil
}

func parseUpdateTopicListOptions(args []string) (updateTopicListOptions, error) {
	fileName := stringArg(args, "-f", "--filename")
	if fileName == "" {
		return updateTopicListOptions{}, errors.New("Filename 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return updateTopicListOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return updateTopicListOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	configs, err := readUpdateTopicConfigsFile(fileName)
	if err != nil {
		return updateTopicListOptions{}, err
	}
	return updateTopicListOptions{
		NameServer:   stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:   brokerAddr,
		ClusterName:  clusterName,
		FileName:     fileName,
		TopicConfigs: normalizeUpdateTopicConfigs(configs),
	}, nil
}

func parseUpdateTopicOptions(args []string) (updateTopicOptions, error) {
	readQueueNums, err := intArg(args, 8, "-r", "--readQueueNums")
	if err != nil {
		return updateTopicOptions{}, err
	}
	writeQueueNums, err := intArg(args, 8, "-w", "--writeQueueNums")
	if err != nil {
		return updateTopicOptions{}, err
	}
	perm, err := intArg(args, 6, "-p", "--perm")
	if err != nil {
		return updateTopicOptions{}, err
	}
	attributes := stringArg(args, "-a", "--attributes")
	if _, err := parseTopicAttributesStrict(attributes); err != nil {
		return updateTopicOptions{}, err
	}
	topic := stringArg(args, "-t", "--topic")
	if topic == "" {
		return updateTopicOptions{}, errors.New("Topic 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return updateTopicOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return updateTopicOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	return updateTopicOptions{
		NameServer:      stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:      brokerAddr,
		ClusterName:     clusterName,
		Topic:           topic,
		ReadQueueNums:   readQueueNums,
		WriteQueueNums:  writeQueueNums,
		Perm:            perm,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag: buildTopicSysFlag(
			strings.EqualFold(stringArg(args, "-u", "--unit"), "true"),
			strings.EqualFold(stringArg(args, "-s", "--hasUnitSub"), "true"),
		),
		Order:      strings.EqualFold(stringArg(args, "-o", "--order"), "true"),
		Attributes: attributes,
	}, nil
}

func parseUpdateStaticTopicOptions(args []string) (updateStaticTopicOptions, error) {
	totalQueueNums, err := intArg(args, 0, "-qn", "--totalQueueNum")
	if err != nil {
		return updateStaticTopicOptions{}, err
	}
	topic := strings.TrimSpace(stringArg(args, "-t", "--topic"))
	if topic == "" {
		return updateStaticTopicOptions{}, errors.New("Topic 必填")
	}
	if totalQueueNums <= 0 {
		return updateStaticTopicOptions{}, errors.New("TotalQueueNum 必填")
	}
	brokerNames := splitCommaList(stringArg(args, "-b", "--brokers"))
	clusterNames := splitCommaList(stringArg(args, "-c", "--clusters"))
	if len(brokerNames) == 0 && len(clusterNames) == 0 {
		return updateStaticTopicOptions{}, errors.New("Brokers 或 Clusters 必填")
	}
	if len(brokerNames) > 0 && len(clusterNames) > 0 {
		return updateStaticTopicOptions{}, errors.New("Brokers 与 Clusters 只能二选一")
	}
	return updateStaticTopicOptions{
		NameServer:     strings.TrimSpace(stringArg(args, "-n", "--namesrvAddr")),
		BrokerNames:    brokerNames,
		ClusterNames:   clusterNames,
		Topic:          topic,
		TotalQueueNums: totalQueueNums,
		MapFile:        strings.TrimSpace(stringArg(args, "-mf", "--mapFile")),
		ForceReplace:   strings.EqualFold(strings.TrimSpace(stringArg(args, "-fr", "--forceReplace")), "true"),
	}, nil
}

func parseRemappingStaticTopicOptions(args []string) (remappingStaticTopicOptions, error) {
	topic := strings.TrimSpace(stringArg(args, "-t", "--topic"))
	if topic == "" {
		return remappingStaticTopicOptions{}, errors.New("Topic 必填")
	}
	brokerNames := splitCommaList(stringArg(args, "-b", "--brokers"))
	clusterNames := splitCommaList(stringArg(args, "-c", "--clusters"))
	if len(brokerNames) == 0 && len(clusterNames) == 0 {
		return remappingStaticTopicOptions{}, errors.New("Brokers 或 Clusters 必填")
	}
	if len(brokerNames) > 0 && len(clusterNames) > 0 {
		return remappingStaticTopicOptions{}, errors.New("Brokers 与 Clusters 只能二选一")
	}
	return remappingStaticTopicOptions{
		NameServer:   strings.TrimSpace(stringArg(args, "-n", "--namesrvAddr")),
		BrokerNames:  brokerNames,
		ClusterNames: clusterNames,
		Topic:        topic,
		MapFile:      strings.TrimSpace(stringArg(args, "-mf", "--mapFile")),
	}, nil
}

func parseUpdateTopicPermOptions(args []string) (updateTopicPermOptions, error) {
	perm, err := intArg(args, 0, "-p", "--perm")
	if err != nil {
		return updateTopicPermOptions{}, err
	}
	topic := stringArg(args, "-t", "--topic")
	if topic == "" {
		return updateTopicPermOptions{}, errors.New("Topic 必填")
	}
	if perm == 0 {
		return updateTopicPermOptions{}, errors.New("Perm 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return updateTopicPermOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return updateTopicPermOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	return updateTopicPermOptions{
		NameServer:  stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:  brokerAddr,
		ClusterName: clusterName,
		Topic:       topic,
		Perm:        perm,
	}, nil
}

func parseSetConsumeModeOptions(args []string) (setConsumeModeOptions, error) {
	popShareQueueNum, err := intArg(args, 0, "-q", "--popShareQueueNum")
	if err != nil {
		return setConsumeModeOptions{}, err
	}
	topic := stringArg(args, "-t", "--topicName")
	groupName := stringArg(args, "-g", "--groupName")
	mode := strings.TrimSpace(stringArg(args, "-m", "--mode"))
	if topic == "" || groupName == "" || mode == "" {
		return setConsumeModeOptions{}, errors.New("Topic、GroupName、Mode 必填")
	}
	return setConsumeModeOptions{
		NameServer:       stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:       stringArg(args, "-b", "--brokerAddr"),
		ClusterName:      stringArg(args, "-c", "--clusterName"),
		Topic:            topic,
		GroupName:        groupName,
		Mode:             mode,
		PopShareQueueNum: popShareQueueNum,
	}, nil
}

func parseSendMessageOptions(args []string) (sendMessageOptions, error) {
	options := sendMessageOptions{
		Topic:          strings.TrimSpace(stringArg(args, "-t", "--topic")),
		Body:           strings.TrimSpace(stringArg(args, "-p", "--body")),
		Keys:           strings.TrimSpace(stringArg(args, "-k", "--key")),
		Tags:           strings.TrimSpace(stringArg(args, "-c", "--tags")),
		BrokerName:     strings.TrimSpace(stringArg(args, "-b", "--broker")),
		MsgTraceEnable: boolArg(args, false, "-m", "--msgTraceEnable"),
	}
	queueIDRaw := strings.TrimSpace(stringArg(args, "-i", "--qid"))
	if queueIDRaw != "" {
		queueID, err := strconv.Atoi(queueIDRaw)
		if err != nil {
			return sendMessageOptions{}, fmt.Errorf("解析 QueueID 失败: %w", err)
		}
		options.QueueID = queueID
		options.HasQueueID = true
	}
	return options, nil
}

func parseSendMsgStatusOptions(args []string) (sendMsgStatusOptions, error) {
	messageSize, err := intArg(args, 128, "-s", "--messageSize")
	if err != nil {
		return sendMsgStatusOptions{}, fmt.Errorf("解析 MessageSize 失败: %w", err)
	}
	count, err := intArg(args, 50, "-c", "--count")
	if err != nil {
		return sendMsgStatusOptions{}, fmt.Errorf("解析 Count 失败: %w", err)
	}
	return sendMsgStatusOptions{
		BrokerName:  strings.TrimSpace(stringArg(args, "-b", "--brokerName")),
		MessageSize: messageSize,
		Count:       count,
	}, nil
}

func parseCheckMsgSendRTOptions(args []string) (checkMsgSendRTOptions, error) {
	amount, err := intArg(args, 100, "-a", "--amount")
	if err != nil {
		return checkMsgSendRTOptions{}, fmt.Errorf("解析 Amount 失败: %w", err)
	}
	size, err := intArg(args, 128, "-s", "--size")
	if err != nil {
		return checkMsgSendRTOptions{}, fmt.Errorf("解析 Size 失败: %w", err)
	}
	return checkMsgSendRTOptions{
		Topic:  strings.TrimSpace(stringArg(args, "-t", "--topic")),
		Amount: amount,
		Size:   size,
	}, nil
}

func parseClusterRTOptions(args []string) (clusterRTOptions, error) {
	amount, err := intArg(args, 100, "-a", "--amount")
	if err != nil {
		return clusterRTOptions{}, fmt.Errorf("解析 Amount 失败: %w", err)
	}
	size, err := intArg(args, 128, "-s", "--size")
	if err != nil {
		return clusterRTOptions{}, fmt.Errorf("解析 Size 失败: %w", err)
	}
	machineRoom := strings.TrimSpace(stringArg(args, "-m", "--machine", "--machine room"))
	if machineRoom == "" {
		machineRoom = "noname"
	}
	return clusterRTOptions{
		ClusterName: strings.TrimSpace(stringArg(args, "-c", "--cluster")),
		Amount:      amount,
		Size:        size,
		PrintAsTlog: boolArg(args, false, "-p", "--print", "--print log"),
		MachineRoom: machineRoom,
	}, nil
}

func parseResetOffsetByTimeOptions(args []string) (resetOffsetByTimeOptions, error) {
	timestampText := strings.TrimSpace(stringArg(args, "-s", "--timestamp"))
	if timestampText == "" {
		return resetOffsetByTimeOptions{}, errors.New("Timestamp 必填")
	}
	timestampMillis, err := parseResetOffsetTimestamp(timestampText)
	if err != nil {
		return resetOffsetByTimeOptions{}, err
	}
	queueID, queueErr := intArg(args, -1, "-q", "--queue")
	if queueErr != nil {
		return resetOffsetByTimeOptions{}, fmt.Errorf("解析 QueueID 失败: %w", queueErr)
	}
	expectOffset, offsetErr := int64Arg(args, 0, "-o", "--offset")
	if offsetErr != nil {
		return resetOffsetByTimeOptions{}, fmt.Errorf("解析 Offset 失败: %w", offsetErr)
	}
	options := resetOffsetByTimeOptions{
		NameServer:      strings.TrimSpace(stringArg(args, "-n", "--namesrvAddr")),
		Group:           strings.TrimSpace(stringArg(args, "-g", "--group")),
		Topic:           strings.TrimSpace(stringArg(args, "-t", "--topic")),
		TimestampText:   timestampText,
		TimestampMillis: timestampMillis,
		Force:           true,
		ClusterName:     strings.TrimSpace(stringArg(args, "-c", "--cluster")),
		BrokerAddr:      strings.TrimSpace(stringArg(args, "-b", "--broker")),
		QueueID:         queueID,
		ExpectOffset:    expectOffset,
		HasQueueID:      hasFlag(args, "-q", "--queue"),
		HasExpectOffset: hasFlag(args, "-o", "--offset"),
	}
	if hasFlag(args, "-f", "--force") {
		options.Force = strings.EqualFold(strings.TrimSpace(stringArg(args, "-f", "--force")), "true")
	}
	options.SpecifiedQueue = options.BrokerAddr != "" && options.HasQueueID
	if options.Group == "" || options.Topic == "" {
		return resetOffsetByTimeOptions{}, errors.New("Group、Topic 必填")
	}
	return options, nil
}

func parseResetOffsetTimestamp(value string) (int64, error) {
	if strings.EqualFold(value, "now") {
		return time.Now().UnixMilli(), nil
	}
	if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
		return timestamp, nil
	}
	parsed, err := time.ParseInLocation("2006-01-02#15:04:05:000", value, time.Local)
	if err != nil {
		return 0, fmt.Errorf("解析 Timestamp 失败: %w", err)
	}
	return parsed.UnixMilli(), nil
}

func parseSkipAccumulatedMessageOptions(args []string) (skipAccumulatedMessageOptions, error) {
	options := skipAccumulatedMessageOptions{
		NameServer:  strings.TrimSpace(stringArg(args, "-n", "--namesrvAddr")),
		Group:       strings.TrimSpace(stringArg(args, "-g", "--group")),
		Topic:       strings.TrimSpace(stringArg(args, "-t", "--topic")),
		ClusterName: strings.TrimSpace(stringArg(args, "-c", "--cluster")),
		Force:       true,
	}
	if hasFlag(args, "-f", "--force") {
		options.Force = strings.EqualFold(strings.TrimSpace(stringArg(args, "-f", "--force")), "true")
	}
	if options.Group == "" || options.Topic == "" {
		return skipAccumulatedMessageOptions{}, errors.New("Group、Topic 必填")
	}
	return options, nil
}

func parseColdDataFlowCtrGroupConfigOptions(args []string) (coldDataFlowCtrGroupConfigOptions, error) {
	consumerGroup := stringArg(args, "-g", "--consumerGroup")
	threshold := strings.TrimSpace(stringArg(args, "-v", "--threshold"))
	if consumerGroup == "" || threshold == "" {
		return coldDataFlowCtrGroupConfigOptions{}, errors.New("ConsumerGroup、Threshold 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return coldDataFlowCtrGroupConfigOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return coldDataFlowCtrGroupConfigOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	return coldDataFlowCtrGroupConfigOptions{
		NameServer:    stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:    brokerAddr,
		ClusterName:   clusterName,
		ConsumerGroup: consumerGroup,
		Threshold:     threshold,
	}, nil
}

func parseAuthUserOptions(args []string) authUserOptions {
	return authUserOptions{
		BrokerAddr:    strings.TrimSpace(stringArg(args, "-b", "--brokerAddr")),
		ClusterName:   strings.TrimSpace(stringArg(args, "-c", "--clusterName")),
		Username:      strings.TrimSpace(stringArg(args, "-u", "--username")),
		Password:      strings.TrimSpace(stringArg(args, "-p", "--password")),
		UserType:      strings.TrimSpace(stringArg(args, "-t", "--userType")),
		UserStatus:    strings.TrimSpace(stringArg(args, "-s", "--userStatus")),
		PasswordSet:   hasStringArg(args, "-p", "--password"),
		UserTypeSet:   hasStringArg(args, "-t", "--userType"),
		UserStatusSet: hasStringArg(args, "-s", "--userStatus"),
	}
}

func parseAclOptions(args []string) aclOptions {
	sourceIpsRaw := strings.TrimSpace(stringArg(args, "-i", "--sourceIp"))
	var sourceIps []string
	if sourceIpsRaw != "" {
		sourceIps = splitCommaList(sourceIpsRaw)
	}
	return aclOptions{
		BrokerAddr:  strings.TrimSpace(stringArg(args, "-b", "--brokerAddr")),
		ClusterName: strings.TrimSpace(stringArg(args, "-c", "--clusterName")),
		Subject:     strings.TrimSpace(stringArg(args, "-s", "--subject")),
		Resources:   splitCommaList(stringArg(args, "-r", "--resources")),
		Actions:     splitCommaList(stringArg(args, "-a", "--actions")),
		SourceIps:   sourceIps,
		Decision:    strings.TrimSpace(stringArg(args, "-d", "--decision")),
		Resource:    strings.TrimSpace(stringArg(args, "-r", "--resources")),
	}
}

func parseAclConfigOptions(args []string) aclConfigOptions {
	return aclConfigOptions{
		BrokerAddr:         strings.TrimSpace(stringArg(args, "-b", "--brokerAddr")),
		ClusterName:        strings.TrimSpace(stringArg(args, "-c", "--clusterName")),
		AccessKey:          strings.TrimSpace(stringArg(args, "-a", "--accessKey")),
		SecretKey:          strings.TrimSpace(stringArg(args, "-s", "--secretKey")),
		WhiteRemoteAddress: strings.TrimSpace(stringArg(args, "-w", "--whiteRemoteAddress")),
		DefaultTopicPerm:   strings.TrimSpace(stringArg(args, "-i", "--defaultTopicPerm")),
		DefaultGroupPerm:   strings.TrimSpace(stringArg(args, "-u", "--defaultGroupPerm")),
		TopicPerms:         splitCommaList(stringArg(args, "-t", "--topicPerms")),
		TopicPermsSet:      hasStringArg(args, "-t", "--topicPerms"),
		GroupPerms:         splitCommaList(stringArg(args, "-g", "--groupPerms")),
		GroupPermsSet:      hasStringArg(args, "-g", "--groupPerms"),
		Admin:              boolArg(args, false, "-m", "--admin"),
		AdminSet:           hasStringArg(args, "-m", "--admin"),
	}
}

func parseGlobalWhiteAddrOptions(args []string) globalWhiteAddrOptions {
	return globalWhiteAddrOptions{
		BrokerAddr:                 strings.TrimSpace(stringArg(args, "-b", "--brokerAddr")),
		ClusterName:                strings.TrimSpace(stringArg(args, "-c", "--clusterName")),
		GlobalWhiteRemoteAddresses: strings.TrimSpace(stringArg(args, "-g", "--globalWhiteRemoteAddresses")),
		AclFileFullPath:            strings.TrimSpace(stringArg(args, "-p", "--aclFileFullPath")),
	}
}

func aclTargetSelected(options aclOptions) bool {
	return (options.BrokerAddr != "" || options.ClusterName != "") && !(options.BrokerAddr != "" && options.ClusterName != "")
}

func aclConfigTargetSelected(options aclConfigOptions) bool {
	return (options.BrokerAddr != "" || options.ClusterName != "") && !(options.BrokerAddr != "" && options.ClusterName != "")
}

func globalWhiteAddrTargetSelected(options globalWhiteAddrOptions) bool {
	return (options.BrokerAddr != "" || options.ClusterName != "") && !(options.BrokerAddr != "" && options.ClusterName != "")
}

func authUserUpdateFieldCount(options authUserOptions) int {
	count := 0
	if authUserFieldSelected(options.PasswordSet, options.Password, options) {
		count++
	}
	if authUserFieldSelected(options.UserTypeSet, options.UserType, options) {
		count++
	}
	if authUserFieldSelected(options.UserStatusSet, options.UserStatus, options) {
		count++
	}
	return count
}

func parseRemoveColdDataFlowCtrGroupConfigOptions(args []string) (removeColdDataFlowCtrGroupConfigOptions, error) {
	consumerGroup := stringArg(args, "-g", "--consumerGroup")
	if consumerGroup == "" {
		return removeColdDataFlowCtrGroupConfigOptions{}, errors.New("ConsumerGroup 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return removeColdDataFlowCtrGroupConfigOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return removeColdDataFlowCtrGroupConfigOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	return removeColdDataFlowCtrGroupConfigOptions{
		NameServer:    stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:    brokerAddr,
		ClusterName:   clusterName,
		ConsumerGroup: consumerGroup,
	}, nil
}

func parseCleanExpiredCQOptions(args []string) cleanExpiredCQOptions {
	return cleanExpiredCQOptions{
		NameServer:  stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:  stringArg(args, "-b", "--brokerAddr"),
		ClusterName: stringArg(args, "-c", "--cluster"),
	}
}

func parseCleanUnusedTopicOptions(args []string) cleanUnusedTopicOptions {
	return cleanUnusedTopicOptions{
		NameServer:  stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:  stringArg(args, "-b", "--brokerAddr"),
		ClusterName: stringArg(args, "-c", "--cluster"),
	}
}

func parseDeleteExpiredCommitLogOptions(args []string) deleteExpiredCommitLogOptions {
	return deleteExpiredCommitLogOptions{
		NameServer:  stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:  stringArg(args, "-b", "--brokerAddr"),
		ClusterName: stringArg(args, "-c", "--cluster"),
	}
}

func parseUpdateSubGroupOptions(args []string) (updateSubGroupOptions, error) {
	retryQueueNums, err := intArg(args, 1, "-q", "--retryQueueNums")
	if err != nil {
		return updateSubGroupOptions{}, err
	}
	retryMaxTimes, err := intArg(args, 16, "-r", "--retryMaxTimes")
	if err != nil {
		return updateSubGroupOptions{}, err
	}
	brokerID, err := int64Arg(args, 0, "-i", "--brokerId")
	if err != nil {
		return updateSubGroupOptions{}, err
	}
	whichBroker, err := int64Arg(args, 1, "-w", "--whichBrokerWhenConsumeSlowly")
	if err != nil {
		return updateSubGroupOptions{}, err
	}
	attributes := stringArg(args, "--attributes")
	if _, err := parseTopicAttributesStrict(attributes); err != nil {
		return updateSubGroupOptions{}, err
	}
	groupRetryPolicy, err := normalizeGroupRetryPolicyJSON(stringArg(args, "-p", "--groupRetryPolicy"))
	if err != nil {
		return updateSubGroupOptions{}, err
	}
	groupName := stringArg(args, "-g", "--groupName")
	if groupName == "" {
		return updateSubGroupOptions{}, errors.New("GroupName 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return updateSubGroupOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return updateSubGroupOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	return updateSubGroupOptions{
		NameServer:                   stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:                   brokerAddr,
		ClusterName:                  clusterName,
		GroupName:                    groupName,
		ConsumeEnable:                boolArg(args, true, "-s", "--consumeEnable"),
		ConsumeFromMinEnable:         boolArg(args, false, "-m", "--consumeFromMinEnable"),
		ConsumeBroadcastEnable:       boolArg(args, false, "-d", "--consumeBroadcastEnable"),
		ConsumeMessageOrderly:        boolArg(args, false, "-o", "--consumeMessageOrderly"),
		RetryQueueNums:               retryQueueNums,
		RetryMaxTimes:                retryMaxTimes,
		GroupRetryPolicy:             groupRetryPolicy,
		BrokerID:                     brokerID,
		WhichBrokerWhenConsumeSlowly: whichBroker,
		NotifyConsumerIdsChanged:     boolArg(args, true, "-a", "--notifyConsumerIdsChanged"),
		Attributes:                   attributes,
	}, nil
}

func parseUpdateSubGroupListOptions(args []string) (updateSubGroupListOptions, error) {
	fileName := stringArg(args, "-f", "--filename")
	if fileName == "" {
		return updateSubGroupListOptions{}, errors.New("Filename 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return updateSubGroupListOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return updateSubGroupListOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	configs, err := readSubscriptionGroupConfigsFile(fileName)
	if err != nil {
		return updateSubGroupListOptions{}, err
	}
	return updateSubGroupListOptions{
		NameServer:   stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:   brokerAddr,
		ClusterName:  clusterName,
		FileName:     fileName,
		GroupConfigs: configs,
	}, nil
}

func parseDeleteSubGroupOptions(args []string) (deleteSubGroupOptions, error) {
	groupName := stringArg(args, "-g", "--groupName")
	if groupName == "" {
		return deleteSubGroupOptions{}, errors.New("GroupName 必填")
	}
	brokerAddr := stringArg(args, "-b", "--brokerAddr")
	clusterName := stringArg(args, "-c", "--clusterName")
	if brokerAddr == "" && clusterName == "" {
		return deleteSubGroupOptions{}, errors.New("BrokerAddr 或 ClusterName 必填")
	}
	if brokerAddr != "" && clusterName != "" {
		return deleteSubGroupOptions{}, errors.New("BrokerAddr 与 ClusterName 只能二选一")
	}
	return deleteSubGroupOptions{
		NameServer:   stringArg(args, "-n", "--namesrvAddr"),
		BrokerAddr:   brokerAddr,
		ClusterName:  clusterName,
		GroupName:    groupName,
		RemoveOffset: boolArg(args, false, "-r", "--removeOffset"),
	}, nil
}

func buildTopicSysFlag(unit bool, hasUnitSub bool) int {
	flag := 0
	if unit {
		flag |= 1
	}
	if hasUnitSub {
		flag |= 2
	}
	return flag
}

func formatUpdateSubGroup(result *updateSubGroupResult) string {
	if result == nil {
		return ""
	}
	var builder strings.Builder
	for _, target := range result.Targets {
		builder.WriteString(fmt.Sprintf("create subscription group to %s success.\n", target))
	}
	builder.WriteString(formatSubscriptionGroupConfig(result.Config))
	return builder.String()
}

func formatUpdateSubGroupList(targets []string, brokerMode bool) string {
	var builder strings.Builder
	for _, target := range targets {
		if brokerMode {
			builder.WriteString(fmt.Sprintf("submit batch of group config to %s success, please check the result later.\n", target))
			continue
		}
		builder.WriteString(fmt.Sprintf("submit batch of subscription group config to %s success, please check the result later.\n", target))
	}
	if !brokerMode {
		builder.WriteString(updateSubGroupListUsage)
	}
	return builder.String()
}

func formatDeleteSubGroup(groupName string, rows []deleteSubGroupResult) string {
	var builder strings.Builder
	for _, row := range rows {
		if row.ClusterName != "" {
			builder.WriteString(fmt.Sprintf("delete subscription group [%s] from broker [%s] in cluster [%s] success.\n", groupName, row.BrokerAddr, row.ClusterName))
			continue
		}
		builder.WriteString(fmt.Sprintf("delete subscription group [%s] from broker [%s] success.\n", groupName, row.BrokerAddr))
	}
	return builder.String()
}

func formatUpdateTopicList(targets []string, includeHelp bool) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("submit batch of topic config to %s success, please check the result later.\n", target))
	}
	if includeHelp {
		builder.WriteString(updateTopicListUsage)
	}
	return builder.String()
}

func formatUpdateTopic(result *updateTopicResult) string {
	if result == nil {
		return ""
	}
	var builder strings.Builder
	if result.Config.Order && result.OrderConf != "" {
		if len(result.Targets) == 1 {
			builder.WriteString(fmt.Sprintf("set broker orderConf. isOrder=%t, orderConf=[%s]\n", result.Config.Order, result.OrderConf))
		} else {
			builder.WriteString(fmt.Sprintf("set cluster orderConf. isOrder=%t, orderConf=[%s]\n", result.Config.Order, result.OrderConf))
		}
	}
	for _, target := range result.Targets {
		builder.WriteString(fmt.Sprintf("create topic to %s success.\n", target))
	}
	builder.WriteString(formatTopicConfig(result.Config))
	builder.WriteByte('\n')
	return builder.String()
}

func formatUpdateStaticTopic(result *updateStaticTopicResult) string {
	if result == nil {
		return ""
	}
	return fmt.Sprintf("The old mapping data is written to file %s\nThe new mapping data is written to file %s\n", result.BeforeFile, result.AfterFile)
}

func formatRemappingStaticTopic(result *remappingStaticTopicResult) string {
	if result == nil {
		return ""
	}
	return fmt.Sprintf("The old mapping data is written to file %s\nThe old mapping data is written to file %s\n", result.BeforeFile, result.AfterFile)
}

func formatUpdateTopicPerm(result *updateTopicPermResult) string {
	if result == nil {
		return ""
	}
	if result.BrokerNotMaster {
		return "updateTopicPerm error broker not exit or broker is not master!.\n"
	}
	if result.SamePerm {
		return "new perm equals to the old one!\n"
	}
	var builder strings.Builder
	for _, row := range result.Rows {
		builder.WriteString(fmt.Sprintf("update topic perm from %d to %d in %s success.\n", row.OldPerm, row.NewPerm, row.BrokerAddr))
	}
	if result.PrintConfig {
		builder.WriteString(formatTopicConfig(result.Config))
		builder.WriteString(".\n")
	}
	return builder.String()
}

func formatSetConsumeMode(result *setConsumeModeResult) string {
	if result == nil {
		return ""
	}
	var builder strings.Builder
	for _, target := range result.Targets {
		builder.WriteString(fmt.Sprintf("set consume mode to %s success.\n", target))
	}
	builder.WriteString(fmt.Sprintf("topic[%s] group[%s] consume mode[%s] popShareQueueNum[%d]",
		result.Topic,
		result.GroupName,
		result.Mode,
		result.PopShareQueueNum,
	))
	return builder.String()
}

func formatUpdateColdDataFlowCtrGroupConfig(targets []string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("updateColdDataFlowCtrGroupConfig success, %s\n", target))
	}
	return builder.String()
}

func formatRemoveColdDataFlowCtrGroupConfig(targets []string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("remove broker cold read threshold success, %s\n", target))
	}
	return builder.String()
}

func formatBrokerBooleanResult(ok bool) string {
	if ok {
		return "success"
	}
	return "false"
}

func formatWipeWritePerm(brokerName string, results []writePermResult) string {
	var builder strings.Builder
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("wipe write perm of broker[%s] in name server[%s] OK, %d\n", brokerName, result.NameServer, result.Count))
	}
	return builder.String()
}

func formatAddWritePerm(brokerName string, results []writePermResult) string {
	var builder strings.Builder
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("add write perm of broker[%s] in name server[%s] OK, %d\n", brokerName, result.NameServer, result.Count))
	}
	return builder.String()
}

func formatCloneGroupOffset(srcGroup string, destGroup string, topic string) string {
	return fmt.Sprintf("clone group offset success. srcGroup[%s], destGroup=[%s], topic[%s]", srcGroup, destGroup, topic)
}

func cloneGroupOffsetUsage() string {
	return "usage: mqadmin cloneGroupOffset -d <arg> [-h] [-n <arg>] [-o <arg>] -s <arg> -t <arg>\n" +
		" -d,--destGroup <arg>     set destination consumer group\n" +
		" -h,--help                Print help\n" +
		" -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'\n" +
		" -o,--offline <arg>       the group or the topic is offline\n" +
		" -s,--srcGroup <arg>      set source consumer group\n" +
		" -t,--topic <arg>         set the topic\n"
}

func formatSendMessage(result *sendMessageResult) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-32s  %-4s  %-20s    %s\n", "#Broker Name", "#QID", "#Send Result", "#MsgId"))
	if result == nil {
		builder.WriteString(fmt.Sprintf("%-32s  %-4s  %-20s    %s\n", "Unknown", "Unknown", "Failed", "None"))
		return builder.String()
	}
	builder.WriteString(fmt.Sprintf("%-32s  %-4d  %-20s    %s\n", result.BrokerName, result.QueueID, result.SendStatus, result.MessageID))
	return builder.String()
}

func formatQueryMsgByIDResend(result *queryMsgByIDResendResult, fallbackMsgID string) string {
	msgID := strings.TrimSpace(fallbackMsgID)
	if result != nil && strings.TrimSpace(result.OriginalMsgID) != "" {
		msgID = strings.TrimSpace(result.OriginalMsgID)
	}
	if result == nil || result.SendResult == nil {
		return fmt.Sprintf("no message. msgId=%s", msgID)
	}
	return fmt.Sprintf("prepare resend msg. originalMsgId=%s%s", msgID, formatSendResultString(result.SendResult))
}

func formatSendResultString(result *sendMessageResult) string {
	if result == nil {
		return "SendResult [sendStatus=null, msgId=null, offsetMsgId=null, messageQueue=null, queueOffset=0, recallHandle=null]"
	}
	return fmt.Sprintf(
		"SendResult [sendStatus=%s, msgId=%s, offsetMsgId=%s, messageQueue=MessageQueue [topic=%s, brokerName=%s, queueId=%d], queueOffset=%d, recallHandle=null]",
		result.SendStatus,
		result.MessageID,
		result.OffsetMessageID,
		result.Topic,
		result.BrokerName,
		result.QueueID,
		result.QueueOffset,
	)
}

func formatSendMsgStatus(results []sendMsgStatusResult) string {
	var builder strings.Builder
	for _, result := range results {
		sendResult := result.SendResult
		builder.WriteString(fmt.Sprintf("rt=%dms, SendResult=%s\n", result.RTMillis, formatSendResultString(&sendResult)))
	}
	return builder.String()
}

func formatCheckMsgSendRT(result *checkMsgSendRTResult) string {
	if result == nil {
		return ""
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-32s  %-4s  %-20s    %s\n", "#Broker Name", "#QID", "#Send Result", "#RT"))
	for _, row := range result.Rows {
		builder.WriteString(fmt.Sprintf("%-32s  %-4d  %-20t    %d\n", row.BrokerName, row.QueueID, row.SendSuccess, row.RTMillis))
	}
	builder.WriteString(fmt.Sprintf("Avg RT: %.2f\n", result.AvgRT))
	return builder.String()
}

func formatClusterRT(result *clusterRTResult, options clusterRTOptions) string {
	if result == nil {
		return ""
	}
	if result.Raw != "" {
		return result.Raw
	}
	if strings.TrimSpace(options.MachineRoom) == "" {
		options.MachineRoom = "noname"
	}
	if options.PrintAsTlog {
		return formatClusterRTTlog(result.Rows, options.MachineRoom)
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-24s  %-24s  %-4s  %-8s  %-8s\n",
		"#Cluster Name",
		"#Broker Name",
		"#RT",
		"#successCount",
		"#failCount",
	))
	for _, row := range result.Rows {
		builder.WriteString(fmt.Sprintf("%-24s  %-24s  %-8s  %-16d  %-16d\n",
			row.ClusterName,
			row.BrokerName,
			fmt.Sprintf("%.2f", row.RT),
			row.SuccessCount,
			row.FailCount,
		))
	}
	return builder.String()
}

func formatClusterRTTlog(rows []clusterRTRow, machineRoom string) string {
	var builder strings.Builder
	location := time.FixedZone("GMT+8", 8*60*60)
	for _, row := range rows {
		timestamp := row.Timestamp
		if timestamp.IsZero() {
			timestamp = time.Now()
		}
		builder.WriteString(fmt.Sprintf("%s|%s|%s|%s|%.0f\n",
			timestamp.In(location).Format("2006-01-02 15:04:05"),
			machineRoom,
			row.ClusterName,
			row.BrokerName,
			clusterRTHalfUp(row.RT),
		))
	}
	return builder.String()
}

func clusterRTHalfUp(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value
	}
	return float64(int64(value + 0.5))
}

func formatResetOffsetByTimeSpecifiedQueue(options resetOffsetByTimeOptions, resetOffset int64) string {
	return fmt.Sprintf(
		"start reset consumer offset by specified, group[%s], topic[%s], queueId[%d], broker[%s], timestamp(string)[%s], timestamp(long)[%d]\n",
		options.Group,
		options.Topic,
		options.QueueID,
		options.BrokerAddr,
		options.TimestampText,
		options.TimestampMillis,
	) + fmt.Sprintf("reset consumer offset to %d\n", resetOffset)
}

func formatResetOffsetByTimeTimestamp(options resetOffsetByTimeOptions, rows []skipAccumulatedMessageRow) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(
		"start reset consumer offset by specified, group[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%d]\n",
		options.Group,
		options.Topic,
		strconv.FormatBool(options.Force),
		options.TimestampText,
		options.TimestampMillis,
	))
	builder.WriteString(formatResetOffsetRows(rows))
	return builder.String()
}

func formatSkipAccumulatedMessage(rows []skipAccumulatedMessageRow) string {
	return formatResetOffsetRows(rows)
}

func formatResetOffsetRows(rows []skipAccumulatedMessageRow) string {
	ordered := append([]skipAccumulatedMessageRow(nil), rows...)
	// 官方 CLI 打印的是 fastjson 解码后的 HashMap.entrySet 顺序，初始容量按 8 复刻才能匹配真实 mqadmin。
	capacity := javaDecodedHashMapCapacity(len(ordered))
	sort.SliceStable(ordered, func(i, j int) bool {
		return javaMessageQueueHashMapBucket(ordered[i].Queue, capacity) < javaMessageQueueHashMapBucket(ordered[j].Queue, capacity)
	})
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-40s  %-40s  %-40s\n", "#brokerName", "#queueId", "#offset"))
	for _, row := range ordered {
		builder.WriteString(fmt.Sprintf("%-40s  %-40d  %-40d\n",
			frontStringAtLeast(row.Queue.BrokerName, 32),
			row.Queue.QueueID,
			row.Offset,
		))
	}
	return builder.String()
}

func formatUpdateBrokerConfig(targets []string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("update broker config success, %s\n", target))
	}
	return builder.String()
}

func formatUpdateNamesrvConfig(targets []string, key string, value string) string {
	targetList := ""
	if len(targets) > 0 {
		targetList = "[" + strings.Join(targets, ", ") + "]"
	}
	return fmt.Sprintf("update name server config success!%s\n%s : %s\n", targetList, key, value)
}

func formatUpdateControllerConfig(targets []string, key string, value string) string {
	targetList := ""
	if len(targets) > 0 {
		targetList = "[" + strings.Join(targets, ", ") + "]"
	}
	return fmt.Sprintf("update controller config success!%s\n%s : %s\n", targetList, key, value)
}

func formatCleanBrokerMetadata(brokerName string) string {
	return fmt.Sprintf("clear broker %s metadata from controller success! \n", brokerName)
}

func formatElectMaster(result *electMasterResult) string {
	if result == nil {
		result = &electMasterResult{}
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("\n#ClusterName\t%s", result.ClusterName))
	builder.WriteString(fmt.Sprintf("\n#BrokerName\t%s", result.BrokerName))
	builder.WriteString(fmt.Sprintf("\n#BrokerMasterAddr\t%s", result.BrokerMasterAddr))
	builder.WriteString(fmt.Sprintf("\n#MasterEpoch\t%d", result.MasterEpoch))
	builder.WriteString(fmt.Sprintf("\n#SyncStateSetEpoch\t%d\n", result.SyncStateSetEpoch))
	for _, member := range result.BrokerMemberAddrs {
		builder.WriteString(fmt.Sprintf("\t#Broker\t%d\t%s\n", member.BrokerID, member.BrokerAddress))
	}
	return builder.String()
}

func formatAddBroker(brokerContainerAddr string) string {
	return fmt.Sprintf("add broker to %s success\n", brokerContainerAddr)
}

func formatRemoveBroker(brokerContainerAddr string) string {
	return fmt.Sprintf("remove broker from %s success\n", brokerContainerAddr)
}

func formatBrokerEpoch(results []brokerEpochResult) string {
	var builder strings.Builder
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("\n#clusterName\t%s\n#brokerName\t%s\n#brokerAddr\t%s\n#brokerId\t%d",
			result.ClusterName, result.BrokerName, result.BrokerAddr, result.BrokerID))
		for index, entry := range result.EpochList {
			if index == len(result.EpochList)-1 {
				entry.EndOffset = result.MaxOffset
			}
			builder.WriteString(fmt.Sprintf("\n#Epoch: EpochEntry{epoch=%d, startOffset=%d, endOffset=%d}",
				entry.Epoch, entry.StartOffset, entry.EndOffset))
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

func formatDeleteTopic(clusterName string, topic string) string {
	return fmt.Sprintf("delete topic [%s] from cluster [%s] success.\n", topic, clusterName) +
		fmt.Sprintf("delete topic [%s] from NameServer success.\n", topic)
}

func formatSubscriptionGroupConfig(config subscriptionGroupConfig) string {
	return fmt.Sprintf("SubscriptionGroupConfig{groupName=%s, consumeEnable=%t, consumeFromMinEnable=%t, consumeBroadcastEnable=%t, consumeMessageOrderly=%t, retryQueueNums=%d, retryMaxTimes=%d, groupRetryPolicy=%s, brokerId=%d, whichBrokerWhenConsumeSlowly=%d, notifyConsumerIdsChangedEnable=%t, groupSysFlag=%d, consumeTimeoutMinute=%d, subscriptionDataSet=null, attributes=%s}",
		config.GroupName,
		config.ConsumeEnable,
		config.ConsumeFromMinEnable,
		config.ConsumeBroadcastEnable,
		config.ConsumeMessageOrderly,
		config.RetryQueueNums,
		config.RetryMaxTimes,
		formatGroupRetryPolicy(config.GroupRetryPolicy),
		config.BrokerID,
		config.WhichBrokerWhenConsumeSlowly,
		config.NotifyConsumerIdsChanged,
		config.GroupSysFlag,
		config.ConsumeTimeoutMinute,
		formatJavaMapStringString(config.Attributes),
	)
}

func formatTopicConfig(config updateTopicConfig) string {
	return fmt.Sprintf("TopicConfig [topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%s, topicFilterType=%s, topicSysFlag=%d, order=%t, attributes=%s]",
		config.TopicName,
		config.ReadQueueNums,
		config.WriteQueueNums,
		formatTopicPerm(config.Perm),
		config.TopicFilterType,
		config.TopicSysFlag,
		config.Order,
		formatJavaMapStringString(config.Attributes),
	)
}

func normalizeGroupRetryPolicyJSON(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultGroupRetryPolicyJSON, nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(raw)); err != nil {
		return "", err
	}
	return compact.String(), nil
}

func formatGroupRetryPolicy(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = defaultGroupRetryPolicyJSON
	}
	var payload struct {
		Type                   string                  `json:"type"`
		ExponentialRetryPolicy *exponentialRetryPolicy `json:"exponentialRetryPolicy"`
		CustomizedRetryPolicy  *customizedRetryPolicy  `json:"customizedRetryPolicy"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "GroupRetryPolicy{type=CUSTOMIZED, exponentialRetryPolicy=null, customizedRetryPolicy=null}"
	}
	if payload.Type == "" {
		payload.Type = "CUSTOMIZED"
	}
	return fmt.Sprintf("GroupRetryPolicy{type=%s, exponentialRetryPolicy=%s, customizedRetryPolicy=%s}",
		payload.Type,
		formatExponentialRetryPolicy(payload.ExponentialRetryPolicy),
		formatCustomizedRetryPolicy(payload.CustomizedRetryPolicy),
	)
}

type exponentialRetryPolicy struct {
	Initial    int64 `json:"initial"`
	Max        int64 `json:"max"`
	Multiplier int64 `json:"multiplier"`
}

type customizedRetryPolicy struct {
	Next []int64 `json:"next"`
}

func formatExponentialRetryPolicy(policy *exponentialRetryPolicy) string {
	if policy == nil {
		return "null"
	}
	initial := policy.Initial
	if initial == 0 {
		initial = 5000
	}
	maxValue := policy.Max
	if maxValue == 0 {
		maxValue = 7200000
	}
	multiplier := policy.Multiplier
	if multiplier == 0 {
		multiplier = 2
	}
	return fmt.Sprintf("ExponentialRetryPolicy{initial=%d, max=%d, multiplier=%d}", initial, maxValue, multiplier)
}

func formatCustomizedRetryPolicy(policy *customizedRetryPolicy) string {
	if policy == nil {
		return "null"
	}
	parts := make([]string, 0, len(policy.Next))
	for _, value := range policy.Next {
		parts = append(parts, strconv.FormatInt(value, 10))
	}
	return fmt.Sprintf("CustomizedRetryPolicy{next=[%s]}", strings.Join(parts, ", "))
}

func formatTopicPerm(perm int) string {
	var builder strings.Builder
	if perm&4 == 4 {
		builder.WriteByte('R')
	} else {
		builder.WriteByte('-')
	}
	if perm&2 == 2 {
		builder.WriteByte('W')
	} else {
		builder.WriteByte('-')
	}
	if perm&1 == 1 {
		builder.WriteByte('X')
	} else {
		builder.WriteByte('-')
	}
	return builder.String()
}

func parseTopicAttributes(raw string) map[string]string {
	attributes, err := parseTopicAttributesStrict(raw)
	if err != nil {
		return map[string]string{}
	}
	return attributes
}

func parseTopicAttributesStrict(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}, nil
	}
	attributes := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key := part
		value := ""
		if strings.Contains(part, "=") {
			pieces := strings.SplitN(part, "=", 2)
			key = strings.TrimSpace(pieces[0])
			value = strings.TrimSpace(pieces[1])
			if !strings.Contains(key, "+") {
				return nil, fmt.Errorf("add/alter attribute format is wrong:%s", key)
			}
		} else if !strings.Contains(key, "-") {
			return nil, fmt.Errorf("delete attribute format is wrong:%s", key)
		}
		if _, exists := attributes[key]; exists {
			return nil, fmt.Errorf("key duplication:%s", key)
		}
		attributes[key] = value
	}
	return attributes, nil
}

func formatTopicAttributesHeader(attributes map[string]string) string {
	if len(attributes) == 0 {
		return ""
	}
	keys := sortedKeysAnyString(attributes)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if attributes[key] == "" {
			parts = append(parts, key)
			continue
		}
		parts = append(parts, key+"="+attributes[key])
	}
	return strings.Join(parts, ",")
}

func parseRocksDBConfigTypes(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return []string{"topics", "subscriptionGroups", "consumerOffsets"}, nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ';' || r == ','
	})
	return normalizeRocksDBConfigTypes(parts)
}

func normalizeRocksDBConfigTypes(values []string) ([]string, error) {
	if len(values) == 0 {
		return []string{"topics", "subscriptionGroups", "consumerOffsets"}, nil
	}
	types := make([]string, 0, len(values))
	for _, part := range values {
		current := strings.TrimSpace(part)
		if current == "" {
			continue
		}
		switch strings.ToLower(current) {
		case "topics":
			types = append(types, "topics")
		case "subscriptiongroups":
			types = append(types, "subscriptionGroups")
		case "consumeroffsets":
			types = append(types, "consumerOffsets")
		default:
			return nil, fmt.Errorf("未知 RocksDB configType: %s", current)
		}
	}
	if len(types) == 0 {
		return []string{"topics", "subscriptionGroups", "consumerOffsets"}, nil
	}
	return types, nil
}

func formatRocksDBConfigTypes(configTypes []string) string {
	normalized, _ := normalizeRocksDBConfigTypes(configTypes)
	var builder strings.Builder
	for _, configType := range normalized {
		builder.WriteString(configType)
		builder.WriteByte(';')
	}
	return builder.String()
}

func exportMetadataInRocksDBLocal(args []string) (string, error) {
	path := strings.TrimSpace(stringArg(args, "-p", "--path"))
	if path == "" || !pathExists(path) {
		return "RocksDB path is invalid.\n", nil
	}
	configType := strings.TrimSpace(stringArg(args, "-t", "--configType"))
	targetPath := joinRocketMQPath(path, configType)
	if !pathExists(targetPath) {
		return fmt.Sprintf("RocksDB load error, path=%s\n", targetPath), nil
	}
	if !strings.EqualFold(configType, "topics") && !strings.EqualFold(configType, "subscriptionGroups") {
		return fmt.Sprintf("Invalid config type=%s, Options: topics,subscriptionGroups\n", configType), nil
	}
	rows, err := readExportMetadataInRocksDB(targetPath)
	if err != nil {
		return fmt.Sprintf("RocksDB load error, path=%s\n", targetPath), nil
	}
	jsonEnable := false
	if rawJSONEnable := strings.TrimSpace(stringArg(args, "-j", "--jsonEnable")); rawJSONEnable != "" {
		jsonEnable, _ = strconv.ParseBool(rawJSONEnable)
	}
	if jsonEnable {
		output, err := formatExportMetadataInRocksDBJSON(configType, rows)
		if err != nil {
			return "", err
		}
		return output, nil
	}
	return formatExportMetadataRows(rows), nil
}

func rocksDBConfigToJsonLocal(args []string) (string, error) {
	path := strings.TrimSpace(stringArg(args, "-p", "--configPath"))
	if path == "" {
		return "Convert RocksDB kv config (topics/subscriptionGroups/consumerOffsets) to json. [rpc mode] Use [-n, -c, -b, -t] to send Request to broker ( version >= 5.3.2 ) or [local mode] use [-p, -t, -j, -e] to load RocksDB. If -e is provided, tools will export json file instead of std print\n", nil
	}
	outputPrefix := "Use [local mode] load rocksdb to print or export file \n"
	rawType := strings.TrimSpace(stringArg(args, "-t", "--configType"))
	configTypes, err := parseRocksDBConfigTypes(rawType)
	if err != nil {
		return outputPrefix + fmt.Sprintf("Invalid configType: %s please input topics/subscriptionGroups/consumerOffsets \n", rawType), nil
	}
	if len(configTypes) == 0 {
		return outputPrefix + fmt.Sprintf("Invalid configType: %s please input topics/subscriptionGroups/consumerOffsets \n", rawType), nil
	}
	configType := configTypes[0]
	if !pathExists(path) {
		return outputPrefix + "Rocksdb path is invalid.\n", nil
	}
	configPath := joinRocketMQPath(path, rocksDBConfigLocalDir(configType))
	rows, err := readExportMetadataInRocksDB(configPath)
	if err != nil {
		return outputPrefix + fmt.Sprintf("Error occurred while converting RocksDB kv config to json, configType=%s, %s\n", rocksDBConfigLocalName(configType), err.Error()), nil
	}
	root, err := rocksDBConfigToJsonValue(configType, rows)
	if err != nil {
		return "", err
	}
	jsonOutput := formatRocksDBConfigPretty(configType, root)
	exportFile := strings.TrimSpace(stringArg(args, "-e", "--exportFile"))
	if exportFile != "" {
		if err := writeRocketMQStringFile(exportFile, jsonOutput); err != nil {
			return outputPrefix + fmt.Sprintf("persist file %s exception%v", exportFile, err), nil
		}
		return outputPrefix, nil
	}
	if rawJSONEnable := strings.TrimSpace(stringArg(args, "-j", "--jsonEnable")); strings.EqualFold(rawJSONEnable, "false") {
		return outputPrefix + formatRocksDBConfigRaw(configType, root), nil
	}
	return outputPrefix + jsonOutput + "\n", nil
}

type exportMetadataRow struct {
	// Key 是 RocksDB metadata 条目的原始 key，官方按迭代顺序直接打印。
	Key string
	// Value 是 RocksDB metadata 条目的 UTF-8 JSON 原文，raw 模式不重新格式化。
	Value string
}

// readExportMetadataInRocksDB 只读打开官方 ConfigRocksDBStorage 目录并按迭代器顺序返回 kv。
func readExportMetadataInRocksDB(path string) ([]exportMetadataRow, error) {
	options := rockyardkv.DefaultOptions()
	database, err := rockyardkv.OpenForReadOnly(path, options, false)
	if err != nil {
		return nil, err
	}
	defer database.Close()
	iterator := database.NewIterator(rockyardkv.DefaultReadOptions())
	defer iterator.Close()
	rows := make([]exportMetadataRow, 0)
	for iterator.SeekToFirst(); iterator.Valid(); iterator.Next() {
		rows = append(rows, exportMetadataRow{
			Key:   string(append([]byte(nil), iterator.Key()...)),
			Value: string(append([]byte(nil), iterator.Value()...)),
		})
	}
	if err := iterator.Error(); err != nil {
		return nil, err
	}
	return rows, nil
}

// formatExportMetadataRows 复刻官方非 JSON 模式的逐行 kv 输出。
func formatExportMetadataRows(rows []exportMetadataRow) string {
	var builder strings.Builder
	for index, row := range rows {
		builder.WriteString(fmt.Sprintf("%d, Key: %s, Value: %s\n", index+1, row.Key, row.Value))
	}
	return builder.String()
}

// formatExportMetadataInRocksDBJSON 复刻官方 fastjson pretty 输出并保留 value 内字段顺序。
func formatExportMetadataInRocksDBJSON(configType string, rows []exportMetadataRow) (string, error) {
	tablePairs := make([]orderedJSONPair, 0, len(rows))
	for _, row := range rows {
		// 官方 fastjson 会按解析出的对象顺序输出字段，不能经由 map 丢失顺序。
		value, err := decodeOrderedJSONValue(row.Value)
		if err != nil {
			return "", fmt.Errorf("解析 exportMetadataInRocksDB JSON 失败: %w", err)
		}
		tablePairs = append(tablePairs, orderedJSONPair{Key: row.Key, Value: value})
	}
	tableName := "topicConfigTable"
	if strings.EqualFold(configType, "subscriptionGroups") {
		tableName = "subscriptionGroupTable"
	}
	root := orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{
				Key:   tableName,
				Value: orderedJSONValue{Kind: orderedJSONObject, Pairs: tablePairs},
			},
		},
	}
	return formatOrderedFastJSONValue(root, 0) + "\n", nil
}

func rocksDBConfigToJsonValue(configType string, rows []exportMetadataRow) (orderedJSONValue, error) {
	tablePairs := make([]orderedJSONPair, 0, len(rows))
	for _, row := range rows {
		value, err := decodeOrderedJSONValue(row.Value)
		if err != nil {
			return orderedJSONValue{}, fmt.Errorf("解析 rocksDBConfigToJson JSON 失败: %w", err)
		}
		if strings.EqualFold(configType, "consumerOffsets") {
			offsetTable, ok := value.objectField("offsetTable")
			if ok {
				value = *offsetTable
			}
		}
		tablePairs = append(tablePairs, orderedJSONPair{Key: row.Key, Value: value})
	}
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{{
			Key:   rocksDBConfigTableName(configType),
			Value: orderedJSONValue{Kind: orderedJSONObject, Pairs: tablePairs},
		}},
	}, nil
}

func formatRocksDBConfigRaw(configType string, root orderedJSONValue) string {
	tableName := rocksDBConfigTableName(configType)
	table, ok := root.objectField(tableName)
	if !ok || table.Kind != orderedJSONObject {
		return "type: " + tableName
	}
	var builder strings.Builder
	builder.WriteString("type: ")
	builder.WriteString(tableName)
	for index, pair := range javaHashMapOrderedJSONPairs(table.Pairs) {
		builder.WriteString(fmt.Sprintf("%d, Key: %s, Value: %s\n", index+1, pair.Key, formatRocksDBConfigRawValue(configType, pair.Value)))
	}
	return builder.String()
}

// formatRocksDBConfigPretty 统一处理 rocksDBConfigToJson 的 pretty 输出，consumerOffsets 使用官方专用格式。
func formatRocksDBConfigPretty(configType string, root orderedJSONValue) string {
	if strings.EqualFold(configType, "consumerOffsets") {
		return formatConsumerOffsetsPretty(root)
	}
	return formatOrderedFastJSONValue(root, 0)
}

// formatRocksDBConfigRawValue 统一处理 rocksDBConfigToJson 的 raw 值格式，consumerOffsets 需要保持 Java Map 风格。
func formatRocksDBConfigRawValue(configType string, value orderedJSONValue) string {
	if strings.EqualFold(configType, "consumerOffsets") {
		return formatConsumerOffsetsRawValue(value)
	}
	return formatOrderedFastJSONCompactValue(value)
}

// formatConsumerOffsetsPretty 复刻官方 consumerOffsets 的 pretty 输出，内部 offsetTable 不加引号并保持 Java Map 行为。
func formatConsumerOffsetsPretty(root orderedJSONValue) string {
	table, ok := root.objectField("offsetTable")
	if !ok || table.Kind != orderedJSONObject {
		return formatOrderedFastJSONValue(root, 0)
	}
	var builder strings.Builder
	builder.WriteString("{\n")
	builder.WriteString("\t\"offsetTable\":{\n")
	ordered := javaHashMapOrderedJSONPairs(table.Pairs)
	for index, pair := range ordered {
		builder.WriteString("\t\t")
		encodedKey, _ := json.Marshal(pair.Key)
		builder.WriteString(string(encodedKey))
		builder.WriteByte(':')
		builder.WriteString(formatConsumerOffsetsOffsetValue(pair.Value, 2))
		if index < len(ordered)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString("\t}\n")
	builder.WriteByte('}')
	return builder.String()
}

// formatConsumerOffsetsRawValue 复刻官方 consumerOffsets 的 raw 值输出，offset 键和值保持 Map.toString 风格。
func formatConsumerOffsetsRawValue(value orderedJSONValue) string {
	if value.Kind != orderedJSONObject {
		return formatOrderedFastJSONCompactValue(value)
	}
	ordered := javaHashMapOrderedJSONPairs(value.Pairs)
	if len(ordered) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteByte('{')
	for index, pair := range ordered {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(pair.Key)
		builder.WriteByte('=')
		builder.WriteString(formatOrderedFastJSONCompactValue(pair.Value))
	}
	builder.WriteByte('}')
	return builder.String()
}

// formatConsumerOffsetsOffsetValue 复刻官方 consumerOffsets 内层 offsetTable 的 pretty 输出，整数 key 不加引号。
func formatConsumerOffsetsOffsetValue(value orderedJSONValue, indent int) string {
	if value.Kind != orderedJSONObject {
		return formatOrderedFastJSONValue(value, indent)
	}
	ordered := javaHashMapOrderedJSONPairs(value.Pairs)
	if len(ordered) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteByte('{')
	for index, pair := range ordered {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(pair.Key)
		builder.WriteByte(':')
		builder.WriteString(formatOrderedFastJSONValue(pair.Value, indent+1))
	}
	builder.WriteByte('\n')
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte('}')
	return builder.String()
}

func rocksDBConfigTableName(configType string) string {
	switch {
	case strings.EqualFold(configType, "topics"):
		return "topicConfigTable"
	case strings.EqualFold(configType, "subscriptionGroups"):
		return "subscriptionGroupTable"
	case strings.EqualFold(configType, "consumerOffsets"):
		return "offsetTable"
	default:
		return configType
	}
}

func rocksDBConfigLocalDir(configType string) string {
	switch {
	case strings.EqualFold(configType, "topics"):
		return "TOPICS"
	case strings.EqualFold(configType, "subscriptionGroups"):
		return "SUBSCRIPTION_GROUPS"
	case strings.EqualFold(configType, "consumerOffsets"):
		return "CONSUMER_OFFSETS"
	default:
		return strings.ToUpper(configType)
	}
}

func rocksDBConfigLocalName(configType string) string {
	switch {
	case strings.EqualFold(configType, "topics"):
		return "TOPICS"
	case strings.EqualFold(configType, "subscriptionGroups"):
		return "SUBSCRIPTION_GROUPS"
	case strings.EqualFold(configType, "consumerOffsets"):
		return "CONSUMER_OFFSETS"
	default:
		return strings.ToUpper(configType)
	}
}

func writeRocketMQStringFile(fileName string, content string) error {
	if previous, err := os.ReadFile(fileName); err == nil {
		if err := os.WriteFile(fileName+".bak", previous, 0o644); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if parent := filepath.Dir(fileName); parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(fileName, []byte(content), 0o644)
}

func joinRocketMQPath(base string, elem string) string {
	normalized := strings.TrimSpace(base)
	if !strings.HasSuffix(normalized, "/") {
		normalized += "/"
	}
	return normalized + elem
}

func pathExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

func formatQueryConsumeQueue(result *queryConsumeQueueResult, startIndex int64) string {
	if result == nil {
		result = &queryConsumeQueueResult{}
	}
	var builder strings.Builder
	if strings.TrimSpace(result.SubscriptionData) != "" && strings.TrimSpace(result.SubscriptionData) != "null" {
		builder.WriteString("Subscription data: \n")
		builder.WriteString(formatQueryCqJSON(result.SubscriptionData))
		builder.WriteByte('\n')
		builder.WriteString("======================================\n")
	}
	if result.FilterData != "" {
		builder.WriteString("Filter data: \n")
		builder.WriteString(result.FilterData)
		builder.WriteByte('\n')
		builder.WriteString("======================================\n")
	}
	builder.WriteString(fmt.Sprintf("Queue data: \nmax: %d, min: %d\n", result.MaxQueueIndex, result.MinQueueIndex))
	builder.WriteString("======================================\n")
	currentIndex := startIndex
	for _, data := range result.QueueData {
		builder.WriteString(fmt.Sprintf("idx: %d\n", currentIndex))
		builder.WriteString(formatConsumeQueueData(data))
		builder.WriteByte('\n')
		builder.WriteString("======================================\n")
		currentIndex++
	}
	return builder.String()
}

func formatConsumeQueueData(data consumeQueueData) string {
	return fmt.Sprintf("ConsumeQueueData{physicOffset=%d, physicSize=%d, tagsCode=%d, extendDataJson='%s', bitMap='%s', eval=%s, msg='%s'}",
		data.PhysicOffset,
		data.PhysicSize,
		data.TagsCode,
		data.ExtendDataJSON,
		data.BitMap,
		strconv.FormatBool(data.Eval),
		data.Msg,
	)
}

func formatQueryCqJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	var buffer bytes.Buffer
	if err := json.Indent(&buffer, []byte(raw), "", "\t"); err != nil {
		return raw
	}
	return buffer.String()
}

func formatConsumerProgressWithClientIP(progress *consumerProgress) string {
	if progress == nil {
		progress = &consumerProgress{}
	}
	entries := append([]consumerProgressEntry(nil), progress.Entries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Topic != entries[j].Topic {
			return entries[i].Topic < entries[j].Topic
		}
		if entries[i].BrokerName != entries[j].BrokerName {
			return entries[i].BrokerName < entries[j].BrokerName
		}
		return entries[i].QueueID < entries[j].QueueID
	})

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4s  %-20s  %-20s  %-20s %-20s %-20s%s\n", "#Topic", "#Broker Name", "#QID", "#Broker Offset", "#Consumer Offset", "#Client IP", "#Diff", "#Inflight", "#LastTime"))
	var diffTotal int64
	var inflightTotal int64
	for _, entry := range entries {
		diff := entry.BrokerOffset - entry.ConsumerOffset
		inflight := entry.PullOffset - entry.ConsumerOffset
		diffTotal += diff
		inflightTotal += inflight
		lastTime := "N/A"
		if entry.LastTimestamp != 0 {
			lastTime = formatTraceTime(entry.LastTimestamp)
		}
		clientIP := strings.TrimSpace(entry.ClientIP)
		if clientIP == "" {
			clientIP = "N/A"
		}
		builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4d  %-20d  %-20d  %-20s %-20d %-20d %s\n",
			frontStringAtLeast(entry.Topic, 64),
			frontStringAtLeast(entry.BrokerName, 32),
			entry.QueueID,
			entry.BrokerOffset,
			entry.ConsumerOffset,
			clientIP,
			diff,
			inflight,
			lastTime,
		))
	}
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("Consume TPS: %.2f\n", progress.ConsumeTPS))
	builder.WriteString(fmt.Sprintf("Consume Diff Total: %d\n", diffTotal))
	builder.WriteString(fmt.Sprintf("Consume Inflight Total: %d\n", inflightTotal))
	return builder.String()
}

func formatConsumerProgressSummary(rows []consumerProgressSummaryRow) string {
	const headerFormat = "%-64s  %-6s  %-24s %-5s  %-14s  %-7s  %s\n"
	const rowFormat = "%-64s  %-6d  %-24s %-5s  %-14s  %-7d  %d\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(headerFormat, "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS", "#Diff Total"))
	for _, row := range rows {
		version := row.Version
		if version == "" {
			version = "OFFLINE"
		}
		builder.WriteString(fmt.Sprintf(rowFormat,
			frontStringAtLeast(row.Group, 64),
			row.Count,
			version,
			row.ConsumeType,
			row.MessageModel,
			row.ConsumeTPS,
			row.DiffTotal,
		))
	}
	return builder.String()
}

func formatTraceTime(timestamp int64) string {
	return time.UnixMilli(timestamp).In(time.Local).Format("2006-01-02 15:04:05")
}

func writeMessageBody(detail *messageDetail) (string, error) {
	messageID := strings.TrimSpace(detail.DisplayMessageID)
	if messageID == "" {
		messageID = strings.TrimSpace(detail.OffsetMessageID)
	}
	bodyPath := messageBodyDirectory + "/" + messageID
	if err := os.MkdirAll(messageBodyDirectory, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(bodyPath, detail.Body, 0o644); err != nil {
		return "", err
	}
	return bodyPath, nil
}

func decodeBodyByCharset(body []byte, charset string) (string, error) {
	label := strings.TrimSpace(charset)
	normalized := strings.ToUpper(strings.ReplaceAll(label, "_", "-"))
	switch normalized {
	case "UTF-8", "UTF8":
		if utf8.Valid(body) {
			return string(body), nil
		}
		var builder strings.Builder
		builder.Grow(len(body))
		for len(body) > 0 {
			r, width := utf8.DecodeRune(body)
			builder.WriteRune(r)
			body = body[width:]
		}
		return builder.String(), nil
	case "US-ASCII", "ASCII":
		for _, item := range body {
			if item > 127 {
				return "", fmt.Errorf("消息体不是 ASCII: byte=%d", item)
			}
		}
		return string(body), nil
	default:
		decoder, err := htmlindex.Get(label)
		if err != nil || decoder == nil {
			return "", fmt.Errorf("暂不支持 bodyFormat %s", charset)
		}
		decoded, _, err := transform.Bytes(decoder.NewDecoder(), body)
		if err != nil {
			return "", fmt.Errorf("按 bodyFormat %s 解码消息体失败: %w", charset, err)
		}
		return string(decoded), nil
	}
}

func nullText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "null"
	}
	return value
}

func formatJavaHashMapProperties(properties messageProperties) string {
	if len(properties) == 0 {
		return "{}"
	}
	ordered := javaHashMapOrderedProperties(properties)
	var builder strings.Builder
	builder.WriteByte('{')
	for index, property := range ordered {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(property.Key)
		builder.WriteByte('=')
		builder.WriteString(property.Value)
	}
	builder.WriteByte('}')
	return builder.String()
}

func javaHashMapOrderedProperties(properties messageProperties) messageProperties {
	compacted := make(messageProperties, 0, len(properties))
	indices := make(map[string]int, len(properties))
	for _, property := range properties {
		if index, ok := indices[property.Key]; ok {
			compacted[index].Value = property.Value
			continue
		}
		indices[property.Key] = len(compacted)
		compacted = append(compacted, property)
	}
	sort.SliceStable(compacted, func(i, j int) bool {
		return javaHashMapBucket(compacted[i].Key) < javaHashMapBucket(compacted[j].Key)
	})
	return compacted
}

func javaHashMapBucket(key string) int {
	return javaHashMapBucketWithCapacity(key, 128)
}

func javaHashMapBucketWithCapacity(key string, capacity int) int {
	hash := uint32(javaStringHash(key))
	spread := hash ^ (hash >> 16)
	return int(spread & uint32(capacity-1))
}

func javaStringHash(value string) int32 {
	var hash int32
	for _, unit := range utf16.Encode([]rune(value)) {
		hash = 31*hash + int32(unit)
	}
	return hash
}

func topicLengthSizeFromMagicCode(magicCode int32) (int, error) {
	switch magicCode {
	case messageMagicCodeV1:
		return 1, nil
	case messageMagicCodeV2:
		return 2, nil
	default:
		return 0, fmt.Errorf("unsupported message magic code %d", magicCode)
	}
}

func createMessageID(storeHostIP []byte, storeHostPort int32, commitLogOffset int64) string {
	if len(storeHostIP) != 4 && len(storeHostIP) != 16 {
		return ""
	}
	raw := make([]byte, len(storeHostIP)+4+8)
	copy(raw, storeHostIP)
	binary.BigEndian.PutUint32(raw[len(storeHostIP):len(storeHostIP)+4], uint32(storeHostPort))
	binary.BigEndian.PutUint64(raw[len(storeHostIP)+4:], uint64(commitLogOffset))
	return strings.ToUpper(hex.EncodeToString(raw))
}

func decodeOffsetMessageID(msgID string) (string, int64, bool) {
	raw, err := hex.DecodeString(strings.TrimSpace(msgID))
	if err != nil {
		return "", 0, false
	}
	if len(raw) != 16 && len(raw) != 28 {
		return "", 0, false
	}
	ipLength := len(raw) - 12
	port := int(binary.BigEndian.Uint32(raw[ipLength : ipLength+4]))
	if port < 0 || port > 65535 {
		return "", 0, false
	}
	offset := int64(binary.BigEndian.Uint64(raw[ipLength+4:]))
	addr := net.JoinHostPort(net.IP(raw[:ipLength]).String(), strconv.Itoa(port))
	return addr, offset, true
}

func formatHost(ip []byte, port int32) string {
	return fmt.Sprintf("%s:%d", net.IP(ip).String(), port)
}

func (entry consumerProgressEntry) identity() messageQueueIdentity {
	return messageQueueIdentity{Topic: entry.Topic, BrokerName: entry.BrokerName, QueueID: entry.QueueID}
}

func consumerClientIP(clientID string) string {
	clientID = strings.TrimSpace(clientID)
	if index := strings.Index(clientID, "@"); index >= 0 {
		return clientID[:index]
	}
	return clientID
}

func decodeMessageProperties(raw string) messageProperties {
	properties := make(messageProperties, 0)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return properties
	}
	parts := strings.Split(raw, string(rune(2)))
	for _, part := range parts {
		if part == "" {
			continue
		}
		pair := strings.SplitN(part, string(rune(1)), 2)
		if len(pair) != 2 {
			continue
		}
		properties.Set(pair[0], pair[1])
	}
	return properties
}

func splitMessageKeys(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	values := strings.Fields(raw)
	if len(values) == 0 {
		return nil
	}
	return values
}

func containsMessageKey(keys []string, target string) bool {
	for _, key := range keys {
		if key == target {
			return true
		}
	}
	return false
}

func (c *Client) invokeNameServer(ctx context.Context, nameServers string, command remotingCommand) (remotingCommand, error) {
	var lastErr error
	for _, addr := range splitNameServers(nameServers) {
		response, err := c.invoke(ctx, addr, command)
		if err == nil {
			return response, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("NameServer 必填")
	}
	return remotingCommand{}, lastErr
}

func (c *Client) invoke(ctx context.Context, addr string, command remotingCommand) (remotingCommand, error) {
	ctx, cancel := contextWithTimeoutIfMissing(ctx, c.timeout)
	defer cancel()
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return remotingCommand{}, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	frame, err := encodeCommand(command)
	if err != nil {
		return remotingCommand{}, err
	}
	if _, err := conn.Write(frame); err != nil {
		return remotingCommand{}, err
	}
	response, err := decodeCommand(conn)
	if err != nil {
		return remotingCommand{}, err
	}
	if response.Opaque != command.Opaque {
		return remotingCommand{}, fmt.Errorf("opaque mismatch: request=%d response=%d", command.Opaque, response.Opaque)
	}
	return response, nil
}

func contextWithTimeoutIfMissing(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func encodeCommand(command remotingCommand) ([]byte, error) {
	header, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	totalLength := 4 + len(header) + len(command.Body)
	frame := make([]byte, 8, 4+totalLength)
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(header)))
	frame = append(frame, header...)
	frame = append(frame, command.Body...)
	return frame, nil
}

func decodeCommand(reader io.Reader) (remotingCommand, error) {
	lengthHeader := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthHeader); err != nil {
		return remotingCommand{}, err
	}
	length := int(binary.BigEndian.Uint32(lengthHeader))
	if length < 4 {
		return remotingCommand{}, fmt.Errorf("bad remoting frame length %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return remotingCommand{}, err
	}
	headerMark := binary.BigEndian.Uint32(payload[0:4])
	headerLength := int(headerMark & 0x00ffffff)
	if headerLength < 0 || headerLength > len(payload)-4 {
		return remotingCommand{}, fmt.Errorf("bad remoting header length %d", headerLength)
	}
	protocol := byte(headerMark >> 24)
	header := payload[4 : 4+headerLength]
	var command remotingCommand
	var err error
	switch protocol {
	case serializeTypeJSON:
		err = json.Unmarshal(header, &command)
	case serializeTypeRocketMQ:
		command, err = decodeRocketMQRemotingHeader(header)
	default:
		err = fmt.Errorf("unsupported remoting serialize type %d", protocol)
	}
	if err != nil {
		return remotingCommand{}, err
	}
	bodyLength := len(payload) - 4 - headerLength
	if bodyLength > 0 {
		command.Body = append([]byte(nil), payload[4+headerLength:]...)
	}
	return command, nil
}

func decodeRocketMQRemotingHeader(header []byte) (remotingCommand, error) {
	reader := &rocketMQHeaderReader{data: header}
	code, err := reader.readUint16()
	if err != nil {
		return remotingCommand{}, err
	}
	languageCode, err := reader.readByte()
	if err != nil {
		return remotingCommand{}, err
	}
	version, err := reader.readUint16()
	if err != nil {
		return remotingCommand{}, err
	}
	opaque, err := reader.readUint32()
	if err != nil {
		return remotingCommand{}, err
	}
	flag, err := reader.readUint32()
	if err != nil {
		return remotingCommand{}, err
	}
	remark, err := reader.readRocketMQString(false)
	if err != nil {
		return remotingCommand{}, err
	}
	extFieldsLength, err := reader.readUint32()
	if err != nil {
		return remotingCommand{}, err
	}
	if extFieldsLength > uint32(len(header)) {
		return remotingCommand{}, fmt.Errorf("bad rocketmq extFields length %d", extFieldsLength)
	}
	extFields, err := reader.readRocketMQExtFields(int(extFieldsLength))
	if err != nil {
		return remotingCommand{}, err
	}
	if reader.cursor != len(header) {
		return remotingCommand{}, fmt.Errorf("bad rocketmq header boundary %d/%d", reader.cursor, len(header))
	}
	return remotingCommand{
		Code:      int(code),
		Language:  rocketMQLanguageName(languageCode),
		Version:   int(version),
		Opaque:    int32(opaque),
		Flag:      int(flag),
		Remark:    remark,
		ExtFields: extFields,
	}, nil
}

type rocketMQHeaderReader struct {
	data   []byte
	cursor int
}

func (reader *rocketMQHeaderReader) readByte() (byte, error) {
	if reader.cursor+1 > len(reader.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value := reader.data[reader.cursor]
	reader.cursor++
	return value, nil
}

func (reader *rocketMQHeaderReader) readUint16() (uint16, error) {
	if reader.cursor+2 > len(reader.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value := binary.BigEndian.Uint16(reader.data[reader.cursor : reader.cursor+2])
	reader.cursor += 2
	return value, nil
}

func (reader *rocketMQHeaderReader) readUint32() (uint32, error) {
	if reader.cursor+4 > len(reader.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value := binary.BigEndian.Uint32(reader.data[reader.cursor : reader.cursor+4])
	reader.cursor += 4
	return value, nil
}

func (reader *rocketMQHeaderReader) readRocketMQString(useShortLength bool) (string, error) {
	var length uint32
	if useShortLength {
		rawLength, err := reader.readUint16()
		if err != nil {
			return "", err
		}
		length = uint32(rawLength)
	} else {
		rawLength, err := reader.readUint32()
		if err != nil {
			return "", err
		}
		length = rawLength
	}
	if length == 0 {
		return "", nil
	}
	if length > uint32(len(reader.data)-reader.cursor) {
		return "", fmt.Errorf("bad rocketmq string length %d", length)
	}
	value := string(reader.data[reader.cursor : reader.cursor+int(length)])
	reader.cursor += int(length)
	return value, nil
}

func (reader *rocketMQHeaderReader) readRocketMQExtFields(length int) (map[string]string, error) {
	if length == 0 {
		return nil, nil
	}
	if reader.cursor+length > len(reader.data) {
		return nil, io.ErrUnexpectedEOF
	}
	end := reader.cursor + length
	fields := make(map[string]string)
	for reader.cursor < end {
		key, err := reader.readRocketMQString(true)
		if err != nil {
			return nil, err
		}
		value, err := reader.readRocketMQString(false)
		if err != nil {
			return nil, err
		}
		fields[key] = value
	}
	if reader.cursor != end {
		return nil, fmt.Errorf("bad rocketmq extFields boundary %d/%d", reader.cursor, end)
	}
	return fields, nil
}

func rocketMQLanguageName(code byte) string {
	switch code {
	case 0:
		return "JAVA"
	case 1:
		return "CPP"
	case 2:
		return "DOTNET"
	case 3:
		return "PYTHON"
	case 4:
		return "DELPHI"
	case 5:
		return "ERLANG"
	case 6:
		return "RUBY"
	case 7:
		return "OTHER"
	case 8:
		return "HTTP"
	case 9:
		return "GO"
	case 10:
		return "PHP"
	case 11:
		return "OMS"
	case 12:
		return "RUST"
	case 13:
		return "NODE_JS"
	default:
		return ""
	}
}

func decodeTopicListBody(body []byte) ([]string, error) {
	var payload struct {
		TopicList []string `json:"topicList"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	// 官方 TopicList.topicList 是 Set<String>，fastjson 反序列化后按 Java HashSet 迭代顺序打印。
	return javaHashSetOrderedStrings(payload.TopicList), nil
}

func formatTopicList(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	return strings.Join(topics, "\n") + "\n"
}

func formatTopicListCluster(rows []topicClusterRow) string {
	const headerFormat = "%-20s  %-48s  %-48s\n"
	const rowFormat = "%-20s  %-64s  %-64s\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(headerFormat, "#Cluster Name", "#Topic", "#Consumer Group"))
	for _, row := range rows {
		builder.WriteString(fmt.Sprintf(rowFormat,
			frontStringAtLeast(row.ClusterName, 20),
			frontStringAtLeast(row.Topic, 64),
			frontStringAtLeast(row.ConsumerGroup, 64),
		))
	}
	return builder.String()
}

func allocateMQAssignmentsFromRoute(topic string, ipList string, routeBody []byte) ([]allocateMQAssignment, error) {
	queues, err := decodeTopicRouteSubscribeQueues(topic, routeBody)
	if err != nil {
		return nil, err
	}
	ips := strings.Split(strings.TrimSpace(ipList), ",")
	assignments := make([]allocateMQAssignment, 0, len(ips))
	for _, ip := range ips {
		assignments = append(assignments, allocateMQAssignment{
			IP:     ip,
			Queues: allocateMessageQueueAveragely(queues, ips, ip),
		})
	}
	return assignments, nil
}

func decodeTopicRouteSubscribeQueues(topic string, body []byte) ([]messageQueueIdentity, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	var payload struct {
		QueueDatas []struct {
			BrokerName    string `json:"brokerName"`
			Perm          int    `json:"perm"`
			ReadQueueNums int    `json:"readQueueNums"`
		} `json:"queueDatas"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, fmt.Errorf("解析 allocateMQ TopicRoute 失败: %w", err)
	}
	queues := make([]messageQueueIdentity, 0)
	for _, queueData := range payload.QueueDatas {
		if queueData.Perm&4 != 4 {
			continue
		}
		for queueID := 0; queueID < queueData.ReadQueueNums; queueID++ {
			queues = append(queues, messageQueueIdentity{
				Topic:      strings.TrimSpace(topic),
				BrokerName: queueData.BrokerName,
				QueueID:    queueID,
			})
		}
	}
	return javaHashSetOrderedMessageQueues(queues), nil
}

// decodeTopicRoutePublishQueues 按官方 TopicPublishInfo 语义从 TopicRoute 中提取可写 master 队列。
func decodeTopicRoutePublishQueues(topic string, body []byte) ([]messageQueueIdentity, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	var payload struct {
		BrokerDatas []struct {
			BrokerName  string            `json:"brokerName"`
			BrokerAddrs map[string]string `json:"brokerAddrs"`
		} `json:"brokerDatas"`
		QueueDatas []struct {
			BrokerName     string `json:"brokerName"`
			Perm           int    `json:"perm"`
			WriteQueueNums int    `json:"writeQueueNums"`
		} `json:"queueDatas"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, fmt.Errorf("解析 sendMessage topicRoute 失败: %w", err)
	}
	hasMaster := make(map[string]bool, len(payload.BrokerDatas))
	for _, broker := range payload.BrokerDatas {
		if strings.TrimSpace(broker.BrokerAddrs["0"]) != "" {
			hasMaster[broker.BrokerName] = true
		}
	}
	sort.SliceStable(payload.QueueDatas, func(left, right int) bool {
		return payload.QueueDatas[left].BrokerName < payload.QueueDatas[right].BrokerName
	})
	queues := make([]messageQueueIdentity, 0)
	for _, queueData := range payload.QueueDatas {
		if queueData.Perm&2 != 2 || !hasMaster[queueData.BrokerName] {
			continue
		}
		for queueID := 0; queueID < queueData.WriteQueueNums; queueID++ {
			queues = append(queues, messageQueueIdentity{
				Topic:      strings.TrimSpace(topic),
				BrokerName: queueData.BrokerName,
				QueueID:    queueID,
			})
		}
	}
	return queues, nil
}

func allocateMessageQueueAveragely(mqAll []messageQueueIdentity, cidAll []string, currentCID string) []messageQueueIdentity {
	index := -1
	for currentIndex, cid := range cidAll {
		if cid == currentCID {
			index = currentIndex
			break
		}
	}
	if index < 0 || len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}
	mod := len(mqAll) % len(cidAll)
	averageSize := 1
	if len(mqAll) > len(cidAll) {
		averageSize = len(mqAll) / len(cidAll)
		if mod > 0 && index < mod {
			averageSize++
		}
	}
	startIndex := index * averageSize
	if mod > 0 && index >= mod {
		startIndex += mod
	}
	count := averageSize
	if remaining := len(mqAll) - startIndex; remaining < count {
		count = remaining
	}
	result := make([]messageQueueIdentity, 0, count)
	for offset := 0; offset < count; offset++ {
		result = append(result, mqAll[(startIndex+offset)%len(mqAll)])
	}
	return result
}

func formatAllocateMQ(assignments []allocateMQAssignment) string {
	ordered := append([]allocateMQAssignment(nil), assignments...)
	capacity := javaHashMapCapacity(len(ordered))
	sort.SliceStable(ordered, func(i, j int) bool {
		return javaHashMapBucketWithCapacity(ordered[i].IP, capacity) < javaHashMapBucketWithCapacity(ordered[j].IP, capacity)
	})
	var builder strings.Builder
	builder.WriteString(`{"result":{`)
	for index, assignment := range ordered {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(jsonString(assignment.IP))
		builder.WriteString(`:[`)
		for queueIndex, queue := range assignment.Queues {
			if queueIndex > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(`{"brokerName":`)
			builder.WriteString(jsonString(queue.BrokerName))
			builder.WriteString(`,"queueId":`)
			builder.WriteString(strconv.Itoa(queue.QueueID))
			builder.WriteString(`,"topic":`)
			builder.WriteString(jsonString(queue.Topic))
			builder.WriteByte('}')
		}
		builder.WriteByte(']')
	}
	builder.WriteString("}}\n")
	return builder.String()
}

func jsonString(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(encoded)
}

func javaHashSetOrderedMessageQueues(queues []messageQueueIdentity) []messageQueueIdentity {
	compacted := make([]messageQueueIdentity, 0, len(queues))
	seen := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		key := fmt.Sprintf("%s\x00%s\x00%d", queue.Topic, queue.BrokerName, queue.QueueID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		compacted = append(compacted, queue)
	}
	capacity := javaHashMapCapacity(len(compacted))
	sort.SliceStable(compacted, func(i, j int) bool {
		return javaMessageQueueHashMapBucket(compacted[i], capacity) < javaMessageQueueHashMapBucket(compacted[j], capacity)
	})
	return compacted
}

func javaHashSetOrderedStrings(values []string) []string {
	compacted := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		compacted = append(compacted, value)
	}
	capacity := javaHashMapCapacity(len(compacted))
	sort.SliceStable(compacted, func(i, j int) bool {
		return javaHashMapBucketWithCapacity(compacted[i], capacity) < javaHashMapBucketWithCapacity(compacted[j], capacity)
	})
	return compacted
}

func javaMessageQueueHashMapBucket(queue messageQueueIdentity, capacity int) int {
	hash := uint32(javaMessageQueueHash(queue))
	spread := hash ^ (hash >> 16)
	return int(spread & uint32(capacity-1))
}

func javaMessageQueueHash(queue messageQueueIdentity) int32 {
	result := int32(1)
	result = 31*result + javaStringHash(queue.BrokerName)
	result = 31*result + int32(queue.QueueID)
	result = 31*result + javaStringHash(queue.Topic)
	return result
}

func javaHashMapCapacity(size int) int {
	capacity := 16
	threshold := capacity * 3 / 4
	for size > threshold {
		capacity *= 2
		threshold = capacity * 3 / 4
	}
	return capacity
}

// javaDecodedHashMapCapacity 复现 fastjson 反序列化普通对象 Map 时的初始 HashMap 容量。
func javaDecodedHashMapCapacity(size int) int {
	capacity := 8
	threshold := capacity * 3 / 4
	for size > threshold {
		capacity *= 2
		threshold = capacity * 3 / 4
	}
	return capacity
}

func formatClusterList(rows []clusterListRow) string {
	const headerFormat = "%-22s  %-22s  %-4s  %-22s %-16s  %16s  %30s  %-22s  %-11s  %-12s  %-8s  %-10s\n"
	const rowFormat = "%-22s  %-22s  %-4s  %-22s %-16s  %16s  %30s  %-22s  %11s  %-12s  %-8s  %10s\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(headerFormat,
		"#Cluster Name",
		"#Broker Name",
		"#BID",
		"#Addr",
		"#Version",
		"#InTPS(LOAD)",
		"#OutTPS(LOAD)",
		"#Timer(Progress)",
		"#PCWait(ms)",
		"#Hour",
		"#SPACE",
		"#ACTIVATED",
	))
	for _, row := range rows {
		builder.WriteString(fmt.Sprintf(rowFormat,
			row.ClusterName,
			row.BrokerName,
			row.BrokerID,
			row.Addr,
			row.Version,
			fmt.Sprintf("%9.2f(%s,%sms)", row.InTPS, row.SendThreadPoolQueueSize, row.SendThreadPoolQueueHeadWaitMS),
			fmt.Sprintf("%9.2f(%s,%sms|%s,%sms)", row.OutTPS, row.PullThreadPoolQueueSize, row.PullThreadPoolQueueHeadWaitMS, row.AckThreadPoolQueueSize, row.AckThreadPoolQueueHeadWaitMS),
			fmt.Sprintf("%d-%d(%.1fw, %.1f, %.1f)", row.TimerReadBehind, row.TimerOffsetBehind, float64(row.TimerCongestNum)/10000.0, row.TimerEnqueueTPS, row.TimerDequeueTPS),
			row.PageCacheLockTimeMS,
			fmt.Sprintf("%2.2f", row.Hour),
			fmt.Sprintf("%.4f", row.CommitLogDiskRatio),
			strconv.FormatBool(row.BrokerActive),
		))
	}
	return builder.String()
}

func formatClusterAclConfigVersion(rows []clusterAclConfigVersionRow) string {
	const rowFormat = "%-16s  %-22s  %-22s  %-20s  %-22s  %-22s\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(rowFormat,
		"#Cluster Name",
		"#Broker Name",
		"#Broker Addr",
		"#AclFilePath",
		"#AclConfigVersionNum",
		"#AclLastUpdateTime"))
	for _, row := range rows {
		builder.WriteString(fmt.Sprintf(rowFormat,
			row.ClusterName,
			row.BrokerName,
			row.BrokerAddr,
			row.AclFilePath,
			strconv.FormatInt(row.VersionCounter, 10),
			row.LastUpdateTime.Local().Format("2006-01-02 15:04:05")))
	}
	builder.WriteString("get cluster's plain access config version success.\n")
	return builder.String()
}

func formatListUser(rows []listUserRow, clusterMode bool) string {
	if len(rows) == 0 {
		return ""
	}
	const rowFormat = "%-16s  %-22s  %-22s  %-22s\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(rowFormat, "#UserName", "#Password", "#UserType", "#UserStatus"))
	for _, row := range rows {
		builder.WriteString(fmt.Sprintf(rowFormat, row.Username, row.Password, row.UserType, row.UserStatus))
	}
	if clusterMode {
		sourceAddress := strings.TrimSpace(rows[0].SourceAddress)
		if sourceAddress != "" {
			builder.WriteString(fmt.Sprintf("get user from %s success.\n", sourceAddress))
		}
	}
	return builder.String()
}

func formatAuthUserTargets(action string, targets []string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("%s user to %s success.\n", action, target))
	}
	return builder.String()
}

func formatAclTargets(action string, targets []string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("%s acl to %s success.\n", action, target))
	}
	return builder.String()
}

func formatUpdateAclConfig(targets []string, options aclConfigOptions) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("create or update plain access config to %s success.\n", target))
	}
	builder.WriteString(formatPlainAccessConfig(options))
	return builder.String()
}

func formatDeleteAclConfig(targets []string, accessKey string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("delete plain access config account from %s success.\n", target))
	}
	builder.WriteString(fmt.Sprintf("account's accessKey is:%s", strings.TrimSpace(accessKey)))
	return builder.String()
}

func formatGlobalWhiteAddrTargets(targets []string) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(fmt.Sprintf("update global white remote addresses to %s success.\n", target))
	}
	return builder.String()
}

func formatPlainAccessConfig(options aclConfigOptions) string {
	return fmt.Sprintf(
		"PlainAccessConfig{accessKey='%s', whiteRemoteAddress='%s', admin=%t, defaultTopicPerm='%s', defaultGroupPerm='%s', topicPerms=%s, groupPerms=%s}",
		strings.TrimSpace(options.AccessKey),
		javaNullableString(options.WhiteRemoteAddress),
		options.Admin,
		javaNullableString(options.DefaultTopicPerm),
		javaNullableString(options.DefaultGroupPerm),
		formatAclConfigList(options.TopicPerms, options.TopicPermsSet),
		formatAclConfigList(options.GroupPerms, options.GroupPermsSet),
	)
}

func javaNullableString(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "null"
	}
	return trimmed
}

func formatAclConfigList(values []string, selected bool) string {
	if !selected {
		return "null"
	}
	return formatJavaStringList(values)
}

func formatAclInfoTable(rows []aclInfo) string {
	if len(rows) == 0 {
		return ""
	}
	const rowFormat = "%-16s  %-10s  %-22s  %-20s  %-24s  %-10s\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(rowFormat, "#Subject", "#PolicyType", "#Resource", "#Actions", "#SourceIp", "#Decision"))
	for _, row := range rows {
		if len(row.Policies) == 0 {
			builder.WriteString(fmt.Sprintf(rowFormat, row.Subject, "", "", "", "", ""))
			continue
		}
		for _, policy := range row.Policies {
			if len(policy.Entries) == 0 {
				continue
			}
			for _, entry := range policy.Entries {
				builder.WriteString(fmt.Sprintf(
					rowFormat,
					row.Subject,
					policy.PolicyType,
					entry.Resource,
					formatJavaStringList(entry.Actions),
					formatJavaStringList(entry.SourceIps),
					entry.Decision,
				))
			}
		}
	}
	return builder.String()
}

func formatJavaStringList(values []string) string {
	if values == nil {
		return "null"
	}
	if len(values) == 0 {
		return "[]"
	}
	return "[" + strings.Join(values, ", ") + "]"
}

func formatCopyUserResults(results []copyUserResult) string {
	var builder strings.Builder
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("copy user of %s from %s to %s success.\n", result.Username, result.SourceBroker, result.TargetBroker))
	}
	return builder.String()
}

func formatCopyAclResults(results []copyAclResult) string {
	var builder strings.Builder
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("copy acl of %s from %s to %s success.\n", result.Subject, result.SourceBroker, result.TargetBroker))
	}
	return builder.String()
}

func formatCommitLogReadAheadMode(sections []commitLogReadAheadModeSection) string {
	var builder strings.Builder
	for _, section := range sections {
		if section.Raw != "" {
			builder.WriteString(section.Raw)
			continue
		}
		builder.WriteString(" ")
		builder.WriteString(section.Header)
		builder.WriteByte('\n')
		builder.WriteString("commitLog set readAhead mode rstStr")
		builder.WriteString(section.Result)
		builder.WriteByte('\n')
	}
	return builder.String()
}

func formatClusterListMoreStats(rows []clusterListMoreStatsRow) string {
	const headerFormat = "%-16s  %-32s %14s %14s %14s %14s\n"
	const rowFormat = "%-16s  %-32s %14d %14d %14d %14d\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(headerFormat,
		"#Cluster Name",
		"#Broker Name",
		"#InTotalYest",
		"#OutTotalYest",
		"#InTotalToday",
		"#OutTotalToday",
	))
	for _, row := range rows {
		builder.WriteString(fmt.Sprintf(rowFormat,
			row.ClusterName,
			row.BrokerName,
			row.InTotalYest,
			row.OutTotalYest,
			row.InTotalToday,
			row.OutTotalToday,
		))
	}
	return builder.String()
}

func formatBrokerStatus(tables []brokerStatusTable) string {
	var builder strings.Builder
	for _, table := range tables {
		keys := make([]string, 0, len(table.Stats))
		for key := range table.Stats {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if table.BrokerAddr != "" {
				builder.WriteString(fmt.Sprintf("%-24s %-32s: %s\n", table.BrokerAddr, key, table.Stats[key]))
			} else {
				builder.WriteString(fmt.Sprintf("%-32s: %s\n", key, table.Stats[key]))
			}
		}
	}
	return builder.String()
}

func formatBrokerConfig(sections []brokerConfigSection) string {
	var builder strings.Builder
	for _, section := range sections {
		if section.Raw != "" {
			builder.WriteString(section.Raw)
			continue
		}
		builder.WriteString(section.Header)
		if !strings.HasSuffix(section.Header, "\n") {
			builder.WriteByte('\n')
		}
		for _, entry := range section.Entries {
			builder.WriteString(fmt.Sprintf("%-50s=  %s\n", entry.Key, entry.Value))
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

// formatSyncStateSet 复刻官方 getSyncStateSet 的 stdout；controller 返回空表时官方不打印任何内容。
func formatSyncStateSet(result *syncStateSetResult) string {
	if result == nil || len(result.Brokers) == 0 {
		return ""
	}
	var builder strings.Builder
	for _, broker := range result.Brokers {
		builder.WriteString(fmt.Sprintf("\n#brokerName\t%s\n#MasterBrokerId\t%d\n#MasterAddr\t%s\n#MasterEpoch\t%d\n#SyncStateSetEpoch\t%d\n#SyncStateSetNums\t%d\n",
			broker.BrokerName,
			broker.MasterBrokerID,
			broker.MasterAddress,
			broker.MasterEpoch,
			broker.SyncStateSetEpoch,
			len(broker.InSyncReplicas),
		))
		for _, member := range broker.InSyncReplicas {
			builder.WriteString(fmt.Sprintf("\nInSyncReplica:\t%s\n", formatSyncStateSetReplicaIdentity(member)))
		}
		for _, member := range broker.NotInSyncReplicas {
			builder.WriteString(fmt.Sprintf("\nNotInSyncReplica:\t%s\n", formatSyncStateSetReplicaIdentity(member)))
		}
	}
	return builder.String()
}

func formatSyncStateSetReplicaIdentity(member syncStateSetReplicaIdentity) string {
	alive := "null"
	if member.Alive != nil {
		alive = strconv.FormatBool(*member.Alive)
	}
	return fmt.Sprintf("ReplicaIdentity{brokerName='%s', brokerId=%d, brokerAddress='%s', alive=%s}",
		member.BrokerName,
		member.BrokerID,
		member.BrokerAddress,
		alive,
	)
}

func formatNamesrvConfig(sections []namesrvConfigSection) string {
	var builder strings.Builder
	for _, section := range sections {
		builder.WriteString(fmt.Sprintf("============%s============\n", section.NameServer))
		for _, entry := range section.Entries {
			builder.WriteString(fmt.Sprintf("%-50s=  %s\n", entry.Key, entry.Value))
		}
	}
	return builder.String()
}

var exportConfigsBrokerPropertyNames = []string{
	"brokerId",
	"brokerName",
	"brokerRole",
	"fileReservedTime",
	"brokerClusterName",
	"filterServerNums",
	"flushDiskType",
	"maxMessageSize",
	"messageDelayLevel",
	"msgTraceTopicName",
	"slaveReadEnable",
	"traceOn",
	"traceTopicEnable",
	"useTLS",
	"autoCreateTopicEnable",
	"autoCreateSubscriptionGroup",
}

func formatExportConfigsJSON(data exportConfigsData) string {
	brokerPairs := make([]orderedJSONPair, 0, len(data.BrokerConfigs))
	for _, brokerConfig := range data.BrokerConfigs {
		brokerName := strings.TrimSpace(brokerConfig.BrokerName)
		if brokerName == "" {
			continue
		}
		brokerPairs = append(brokerPairs, orderedJSONPair{
			Key: brokerName,
			Value: orderedJSONValue{
				Kind:  orderedJSONObject,
				Pairs: exportBrokerConfigJSONPairs(brokerConfig.Entries),
			},
		})
	}
	root := orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{
				Key: "clusterScale",
				Value: orderedJSONValue{
					Kind: orderedJSONObject,
					Pairs: []orderedJSONPair{
						{Key: "namesrvSize", Value: orderedJSONNumberValue(data.NameServerSize)},
						{Key: "masterBrokerSize", Value: orderedJSONNumberValue(data.MasterBrokerSize)},
						{Key: "slaveBrokerSize", Value: orderedJSONNumberValue(data.SlaveBrokerSize)},
					},
				},
			},
			{
				Key: "brokerConfigs",
				Value: orderedJSONValue{
					Kind:  orderedJSONObject,
					Pairs: brokerPairs,
				},
			},
		},
	}
	return formatOrderedFastJSONValue(root, 0)
}

func formatExportMetricsJSON(data exportMetricsData) string {
	reportPairs := make([]orderedJSONPair, 0, len(data.Reports))
	for _, report := range data.Reports {
		brokerName := strings.TrimSpace(report.BrokerName)
		if brokerName == "" {
			continue
		}
		reportPairs = append(reportPairs, orderedJSONPair{
			Key:   brokerName,
			Value: exportMetricsBrokerReportJSONValue(report),
		})
	}
	root := orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{
				Key: "evaluateReport",
				Value: orderedJSONValue{
					Kind:  orderedJSONObject,
					Pairs: reportPairs,
				},
			},
			{
				Key:   "totalData",
				Value: exportMetricsTotalJSONValue(data.Total),
			},
		},
	}
	return formatOrderedFastJSONValue(root, 0)
}

func exportMetricsTotalJSONValue(total exportMetricsTotal) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{
				Key:   "totalTps",
				Value: exportMetricsTotalTPSJSONValue(total),
			},
			{
				Key:   "totalOneDayNum",
				Value: exportMetricsTotalOneDayJSONValue(total),
			},
		},
	}
}

func exportMetricsTotalTPSJSONValue(total exportMetricsTotal) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "totalNormalInTps", Value: orderedJSONFloatValue(total.NormalInTps)},
			{Key: "totalNormalOutTps", Value: orderedJSONFloatValue(total.NormalOutTps)},
			{Key: "totalTransInTps", Value: orderedJSONFloatValue(total.TransInTps)},
			{Key: "totalScheduleInTps", Value: orderedJSONFloatValue(total.ScheduleInTps)},
		},
	}
}

func exportMetricsTotalOneDayJSONValue(total exportMetricsTotal) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "normalOneDayInNum", Value: orderedJSONInt64Value(total.NormalOneDayInNum)},
			{Key: "normalOneDayOutNum", Value: orderedJSONInt64Value(total.NormalOneDayOutNum)},
			{Key: "transOneDayInNum", Value: orderedJSONInt64Value(total.TransOneDayInNum)},
			{Key: "scheduleOneDayInNum", Value: orderedJSONInt64Value(total.ScheduleOneDayInNum)},
		},
	}
}

func exportMetricsBrokerReportJSONValue(report exportMetricsBrokerReport) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "runtimeEnv", Value: exportMetricsRuntimeEnvJSONValue(report.RuntimeEnv)},
			{Key: "runtimeQuota", Value: exportMetricsRuntimeQuotaJSONValue(report.RuntimeQuota)},
			{Key: "runtimeVersion", Value: exportMetricsRuntimeVersionJSONValue(report.RuntimeVersion)},
		},
	}
}

func exportMetricsRuntimeEnvJSONValue(env exportMetricsRuntimeEnv) orderedJSONValue {
	pairs := make([]orderedJSONPair, 0, 2)
	if strings.TrimSpace(env.CPUNum) != "" {
		pairs = append(pairs, orderedJSONPair{Key: "cpuNum", Value: orderedJSONStringValue(env.CPUNum)})
	}
	if strings.TrimSpace(env.TotalMemKBytes) != "" {
		pairs = append(pairs, orderedJSONPair{Key: "totalMemKBytes", Value: orderedJSONStringValue(env.TotalMemKBytes)})
	}
	return orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}
}

func exportMetricsRuntimeQuotaJSONValue(quota exportMetricsRuntimeQuota) orderedJSONValue {
	pairs := []orderedJSONPair{
		{Key: "diskRatio", Value: exportMetricsDiskRatioJSONValue(quota)},
		{Key: "tps", Value: exportMetricsTPSJSONValue(quota)},
		{Key: "oneDayNum", Value: exportMetricsOneDayJSONValue(quota)},
	}
	if strings.TrimSpace(quota.MessageAverageSize) != "" {
		pairs = append(pairs, orderedJSONPair{Key: "messageAverageSize", Value: orderedJSONStringValue(quota.MessageAverageSize)})
	}
	pairs = append(pairs,
		orderedJSONPair{Key: "topicSize", Value: orderedJSONNumberValue(quota.TopicSize)},
		orderedJSONPair{Key: "groupSize", Value: orderedJSONNumberValue(quota.GroupSize)},
	)
	return orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}
}

func exportMetricsDiskRatioJSONValue(quota exportMetricsRuntimeQuota) orderedJSONValue {
	pairs := make([]orderedJSONPair, 0, 2)
	if strings.TrimSpace(quota.CommitLogDiskRatio) != "" {
		pairs = append(pairs, orderedJSONPair{Key: "commitLogDiskRatio", Value: orderedJSONStringValue(quota.CommitLogDiskRatio)})
	}
	if strings.TrimSpace(quota.ConsumeQueueDiskRatio) != "" {
		pairs = append(pairs, orderedJSONPair{Key: "consumeQueueDiskRatio", Value: orderedJSONStringValue(quota.ConsumeQueueDiskRatio)})
	}
	return orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}
}

func exportMetricsTPSJSONValue(quota exportMetricsRuntimeQuota) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "normalInTps", Value: orderedJSONFloatValue(quota.NormalInTps)},
			{Key: "normalOutTps", Value: orderedJSONFloatValue(quota.NormalOutTps)},
			{Key: "transInTps", Value: orderedJSONFloatValue(quota.TransInTps)},
			{Key: "scheduleInTps", Value: orderedJSONFloatValue(quota.ScheduleInTps)},
		},
	}
}

func exportMetricsOneDayJSONValue(quota exportMetricsRuntimeQuota) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "normalOneDayInNum", Value: orderedJSONInt64Value(quota.NormalOneDayInNum)},
			{Key: "normalOneDayOutNum", Value: orderedJSONInt64Value(quota.NormalOneDayOutNum)},
			{Key: "transOneDayInNum", Value: orderedJSONInt64Value(quota.TransOneDayInNum)},
			{Key: "scheduleOneDayInNum", Value: orderedJSONInt64Value(quota.ScheduleOneDayInNum)},
		},
	}
}

func exportMetricsRuntimeVersionJSONValue(version exportMetricsRuntimeVersion) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "rocketmqVersion", Value: orderedJSONStringValue(version.RocketMQVersion)},
			{Key: "clientInfo", Value: exportMetricsClientInfoJSONValue(version.ClientInfo)},
		},
	}
}

func exportMetricsClientInfoJSONValue(values []string) orderedJSONValue {
	items := make([]orderedJSONValue, 0, len(values))
	for _, value := range values {
		items = append(items, orderedJSONStringValue(value))
	}
	return orderedJSONValue{Kind: orderedJSONArray, Items: items}
}

func exportBrokerConfigJSONPairs(entries []brokerConfigEntry) []orderedJSONPair {
	values := brokerConfigEntryMap(entries)
	pairs := make([]orderedJSONPair, 0, len(exportConfigsBrokerPropertyNames))
	for _, name := range exportConfigsBrokerPropertyNames {
		value, ok := values[name]
		if !ok {
			continue
		}
		pairs = append(pairs, orderedJSONPair{
			Key: name,
			Value: orderedJSONValue{
				Kind: orderedJSONString,
				Text: value,
			},
		})
	}
	return pairs
}

func orderedJSONNumberValue(value int) orderedJSONValue {
	return orderedJSONValue{Kind: orderedJSONNumber, Text: strconv.Itoa(value)}
}

func orderedJSONInt64Value(value int64) orderedJSONValue {
	return orderedJSONValue{Kind: orderedJSONNumber, Text: strconv.FormatInt(value, 10)}
}

func orderedJSONFloatValue(value float64) orderedJSONValue {
	return orderedJSONValue{Kind: orderedJSONNumber, Text: exportMetricsFloatText(value)}
}

func orderedJSONStringValue(value string) orderedJSONValue {
	return orderedJSONValue{Kind: orderedJSONString, Text: value}
}

func brokerConfigEntryMap(entries []brokerConfigEntry) map[string]string {
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		values[entry.Key] = entry.Value
	}
	return values
}

func brokerConfigEntryValue(entries []brokerConfigEntry, key string) string {
	return brokerConfigEntryMap(entries)[key]
}

func exportConfigsOutputPath(filePath string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(filePath), `/\`)
	if trimmed == "" {
		trimmed = "/tmp/rocketmq/export"
	}
	return trimmed + "/configs.json"
}

func exportMetricsOutputPath(filePath string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(filePath), `/\`)
	if trimmed == "" {
		trimmed = "/tmp/rocketmq/export"
	}
	return trimmed + "/metrics.json"
}

func formatExportMetadataJSON(data exportMetadataData) string {
	pairs := make([]orderedJSONPair, 0, 3)
	pairs = append(pairs, orderedJSONPair{
		Key:   "exportTime",
		Value: orderedJSONValue{Kind: orderedJSONNumber, Text: strconv.FormatInt(data.ExportTime, 10)},
	})
	if data.IncludeTopics {
		pairs = append(pairs, orderedJSONPair{
			Key: "topicConfigTable",
			Value: orderedJSONValue{
				Kind:  orderedJSONObject,
				Pairs: exportMetadataTopicConfigPairs(data.TopicConfigs),
			},
		})
	}
	if data.IncludeGroups {
		pairs = append(pairs, orderedJSONPair{
			Key: "subscriptionGroupTable",
			Value: orderedJSONValue{
				Kind:  orderedJSONObject,
				Pairs: data.SubscriptionGroups,
			},
		})
	}
	return formatExportMetadataValue(orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}, 0)
}

func formatExportMetadataValue(value orderedJSONValue, indent int) string {
	switch value.Kind {
	case orderedJSONObject:
		return formatExportMetadataObject(value.Pairs, indent)
	case orderedJSONArray:
		return formatExportMetadataArray(value.Items, indent)
	case orderedJSONString:
		encoded, _ := json.Marshal(value.Text)
		return string(encoded)
	case orderedJSONNumber:
		return value.Text
	case orderedJSONBool:
		if value.Bool {
			return "true"
		}
		return "false"
	case orderedJSONNull:
		return "null"
	default:
		return "null"
	}
}

func formatExportMetadataObject(pairs []orderedJSONPair, indent int) string {
	if len(pairs) == 0 {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteString("{\n")
	for index, pair := range pairs {
		encodedKey, _ := json.Marshal(pair.Key)
		builder.WriteString(strings.Repeat("\t", indent+1))
		builder.WriteString(string(encodedKey))
		builder.WriteByte(':')
		builder.WriteString(formatExportMetadataValue(pair.Value, indent+1))
		if index < len(pairs)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte('}')
	return builder.String()
}

func formatExportMetadataArray(items []orderedJSONValue, indent int) string {
	if len(items) == 0 {
		return "[]"
	}
	var builder strings.Builder
	builder.WriteString("[\n")
	for index, item := range items {
		builder.WriteString(strings.Repeat("\t", indent+1))
		builder.WriteString(formatExportMetadataValue(item, indent+1))
		if index < len(items)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte(']')
	return builder.String()
}

func exportMetadataTopicConfigPairs(configs []exportMetadataTopicConfig) []orderedJSONPair {
	pairs := make([]orderedJSONPair, 0, len(configs))
	for _, config := range configs {
		if strings.TrimSpace(config.Name) == "" {
			continue
		}
		pairs = append(pairs, orderedJSONPair{Key: config.Name, Value: normalizeExportMetadataTopicConfig(config.Value)})
	}
	return pairs
}

func exportMetadataTopicConfigsFromMap(values map[string]orderedJSONValue) []exportMetadataTopicConfig {
	pairs := make([]orderedJSONPair, 0, len(values))
	for name, value := range values {
		pairs = append(pairs, orderedJSONPair{Key: name, Value: value})
	}
	return exportMetadataTopicConfigsFromOrderedPairs(pairs)
}

func exportMetadataTopicConfigsFromOrderedKeys(values map[string]orderedJSONValue, keys []string) []exportMetadataTopicConfig {
	pairs := orderedJSONPairsFromKeys(values, keys)
	return exportMetadataTopicConfigsFromOrderedPairs(pairs)
}

// exportMetadataTopicConfigsFromOrderedPairs 复现官方先 put HashMap、再由 fastjson 遍历 HashMap 的顺序。
// 同一个 bucket 内必须保留首次 put 的链表顺序；Go map 迭代顺序不能参与该语义。
func exportMetadataTopicConfigsFromOrderedPairs(pairs []orderedJSONPair) []exportMetadataTopicConfig {
	configs := make([]exportMetadataTopicConfig, 0, len(pairs))
	indexByName := make(map[string]int, len(pairs))
	for _, pair := range pairs {
		if strings.TrimSpace(pair.Key) == "" {
			continue
		}
		if index, ok := indexByName[pair.Key]; ok {
			configs[index].Value = pair.Value
			continue
		}
		indexByName[pair.Key] = len(configs)
		configs = append(configs, exportMetadataTopicConfig{Name: pair.Key, Value: pair.Value})
	}
	sort.SliceStable(configs, func(left int, right int) bool {
		capacity := javaHashMapCapacity(len(configs))
		return javaHashMapBucketWithCapacity(configs[left].Name, capacity) < javaHashMapBucketWithCapacity(configs[right].Name, capacity)
	})
	return configs
}

func exportMetadataPairsFromMap(values map[string]orderedJSONValue) []orderedJSONPair {
	pairs := make([]orderedJSONPair, 0, len(values))
	for key, value := range values {
		pairs = append(pairs, orderedJSONPair{Key: key, Value: value})
	}
	return javaHashMapOrderedJSONPairs(pairs)
}

func exportMetadataPairsFromOrderedKeys(values map[string]orderedJSONValue, keys []string) []orderedJSONPair {
	return javaHashMapOrderedJSONPairs(orderedJSONPairsFromKeys(values, keys))
}

func orderedJSONPairsFromKeys(values map[string]orderedJSONValue, keys []string) []orderedJSONPair {
	pairs := make([]orderedJSONPair, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		value, ok := values[key]
		if !ok {
			continue
		}
		seen[key] = struct{}{}
		pairs = append(pairs, orderedJSONPair{Key: key, Value: value})
	}
	return pairs
}

func exportMetadataTopicConfigValue(topicName string, queueNums int) orderedJSONValue {
	return orderedJSONValue{
		Kind: orderedJSONObject,
		Pairs: []orderedJSONPair{
			{Key: "attributes", Value: orderedJSONValue{Kind: orderedJSONObject}},
			{Key: "order", Value: orderedJSONValue{Kind: orderedJSONBool, Bool: false}},
			{Key: "perm", Value: orderedJSONValue{Kind: orderedJSONNumber, Text: "6"}},
			{Key: "readQueueNums", Value: orderedJSONValue{Kind: orderedJSONNumber, Text: strconv.Itoa(queueNums)}},
			{Key: "topicFilterType", Value: orderedJSONValue{Kind: orderedJSONString, Text: "SINGLE_TAG"}},
			{Key: "topicName", Value: orderedJSONValue{Kind: orderedJSONString, Text: topicName}},
			{Key: "topicSysFlag", Value: orderedJSONValue{Kind: orderedJSONNumber, Text: "0"}},
			{Key: "writeQueueNums", Value: orderedJSONValue{Kind: orderedJSONNumber, Text: strconv.Itoa(queueNums)}},
		},
	}
}

func normalizeExportMetadataTopicConfig(value orderedJSONValue) orderedJSONValue {
	if value.Kind != orderedJSONObject {
		return value
	}
	fieldOrder := []string{
		"attributes",
		"order",
		"perm",
		"readQueueNums",
		"topicFilterType",
		"topicName",
		"topicSysFlag",
		"writeQueueNums",
	}
	values := make(map[string]orderedJSONValue, len(value.Pairs))
	for _, pair := range value.Pairs {
		values[pair.Key] = pair.Value
	}
	pairs := make([]orderedJSONPair, 0, len(value.Pairs))
	for _, field := range fieldOrder {
		if fieldValue, ok := values[field]; ok {
			pairs = append(pairs, orderedJSONPair{Key: field, Value: fieldValue})
			delete(values, field)
		}
	}
	for _, pair := range javaHashMapOrderedJSONPairs(exportMetadataPairsFromMap(values)) {
		pairs = append(pairs, pair)
	}
	return orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}
}

func filterTopicConfigWrapper(value *orderedJSONValue, systemTopics map[string]bool, includeSpecialTopic bool) {
	keepOrderedJSONObjectFields(value, map[string]bool{
		"dataVersion":      true,
		"topicConfigTable": true,
	})
	table, ok := value.objectField("topicConfigTable")
	if !ok || table.Kind != orderedJSONObject {
		return
	}
	table.Pairs = javaConcurrentHashMapOrderedJSONPairs(table.Pairs)
	filtered := make([]orderedJSONPair, 0, len(table.Pairs))
	for _, pair := range table.Pairs {
		topicName := exportMetadataTopicName(pair)
		if topicName == "" {
			topicName = pair.Key
		}
		if systemTopics[topicName] || isRocketMQSystemTopic(topicName) {
			continue
		}
		if !includeSpecialTopic && (strings.HasPrefix(topicName, "%RETRY%") || strings.HasPrefix(topicName, "%DLQ%")) {
			continue
		}
		if perm, ok := exportMetadataIntField(pair.Value, "perm"); ok && perm <= 0 {
			continue
		}
		filtered = append(filtered, orderedJSONPair{Key: pair.Key, Value: normalizeExportMetadataTopicConfig(pair.Value)})
	}
	table.Pairs = filtered
}

func filterSubscriptionGroupWrapper(value *orderedJSONValue) {
	keepOrderedJSONObjectFields(value, map[string]bool{
		"dataVersion":            true,
		"subscriptionGroupTable": true,
	})
	table, ok := value.objectField("subscriptionGroupTable")
	if !ok || table.Kind != orderedJSONObject {
		return
	}
	table.Pairs = javaConcurrentHashMapOrderedJSONPairs(table.Pairs)
	filtered := make([]orderedJSONPair, 0, len(table.Pairs))
	for _, pair := range table.Pairs {
		if isExportMetadataSystemConsumerGroup(pair.Key) {
			continue
		}
		filtered = append(filtered, pair)
	}
	table.Pairs = filtered
}

func keepOrderedJSONObjectFields(value *orderedJSONValue, allowed map[string]bool) {
	if value == nil || value.Kind != orderedJSONObject {
		return
	}
	filtered := value.Pairs[:0]
	for _, pair := range value.Pairs {
		if allowed[pair.Key] {
			filtered = append(filtered, pair)
		}
	}
	value.Pairs = filtered
}

func isExportMetadataSystemConsumerGroup(groupName string) bool {
	if strings.HasPrefix(groupName, "CID_RMQ_SYS_") {
		return true
	}
	_, ok := map[string]struct{}{
		"DEFAULT_CONSUMER":      {},
		"DEFAULT_PRODUCER":      {},
		"TOOLS_CONSUMER":        {},
		"SCHEDULE_CONSUMER":     {},
		"FILTERSRV_CONSUMER":    {},
		"__MONITOR_CONSUMER":    {},
		"CLIENT_INNER_PRODUCER": {},
		"SELF_TEST_P_GROUP":     {},
		"SELF_TEST_C_GROUP":     {},
		"CID_ONS-HTTP-PROXY":    {},
		"CID_ONSAPI_PERMISSION": {},
		"CID_ONSAPI_OWNER":      {},
		"CID_ONSAPI_PULL":       {},
		"CID_RMQ_SYS_TRANS":     {},
	}[groupName]
	return ok
}

func exportMetadataTopicName(pair orderedJSONPair) string {
	if pair.Value.Kind != orderedJSONObject {
		return pair.Key
	}
	if field, ok := pair.Value.objectField("topicName"); ok {
		if value, ok := field.primitiveString(); ok {
			return value
		}
	}
	return pair.Key
}

func mergeExportMetadataTopicConfig(left orderedJSONValue, right orderedJSONValue) orderedJSONValue {
	merged := normalizeExportMetadataTopicConfig(right)
	for _, field := range []string{"readQueueNums", "writeQueueNums"} {
		leftValue, leftOK := exportMetadataIntField(left, field)
		rightValue, rightOK := exportMetadataIntField(right, field)
		if leftOK && rightOK {
			setOrderedJSONNumberField(&merged, field, leftValue+rightValue)
		}
	}
	return merged
}

func mergeExportMetadataSubscriptionGroup(left orderedJSONValue, right orderedJSONValue) orderedJSONValue {
	merged := right
	leftValue, leftOK := exportMetadataIntField(left, "retryQueueNums")
	rightValue, rightOK := exportMetadataIntField(right, "retryQueueNums")
	if leftOK && rightOK {
		setOrderedJSONNumberField(&merged, "retryQueueNums", leftValue+rightValue)
	}
	return merged
}

func exportMetadataIntField(value orderedJSONValue, key string) (int, bool) {
	if value.Kind != orderedJSONObject {
		return 0, false
	}
	field, ok := value.objectField(key)
	if !ok {
		return 0, false
	}
	text, ok := field.primitiveString()
	if !ok {
		return 0, false
	}
	number, err := strconv.Atoi(text)
	return number, err == nil
}

func setOrderedJSONNumberField(value *orderedJSONValue, key string, number int) {
	if value == nil || value.Kind != orderedJSONObject {
		return
	}
	replacement := orderedJSONValue{Kind: orderedJSONNumber, Text: strconv.Itoa(number)}
	for index := range value.Pairs {
		if value.Pairs[index].Key == key {
			value.Pairs[index].Value = replacement
			return
		}
	}
	value.Pairs = append(value.Pairs, orderedJSONPair{Key: key, Value: replacement})
}

func isRocketMQSystemTopic(topicName string) bool {
	if topicName == "" {
		return false
	}
	switch topicName {
	case "TBW102", "SELF_TEST_TOPIC", "BenchmarkTest", "OFFSET_MOVED_EVENT", "SCHEDULE_TOPIC_XXXX", "RMQ_SYS_TRANS_HALF_TOPIC", "RMQ_SYS_TRACE_TOPIC":
		return true
	}
	return strings.HasPrefix(topicName, "rmq_sys_") || strings.HasPrefix(topicName, "CID_RMQ_SYS_")
}

func exportMetadataOutputPath(filePath string, fileName string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(filePath), `/\`)
	if trimmed == "" {
		trimmed = "/tmp/rocketmq/export"
	}
	return trimmed + "/" + fileName
}

func writeFastJSONFile(path string, content string) error {
	dir := path
	if index := strings.LastIndexAny(path, `/\`); index >= 0 {
		dir = path[:index]
	}
	if strings.TrimSpace(dir) != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(content), 0644)
}

func formatConsumerConfig(sections []consumerConfigSection) string {
	var builder strings.Builder
	for _, section := range sections {
		builder.WriteString(section.Header)
		if !strings.HasSuffix(section.Header, "\n") {
			builder.WriteByte('\n')
		}
		for _, entry := range section.Entries {
			builder.WriteString(fmt.Sprintf("%-40s=  %s\n", entry.Name, entry.Value))
		}
	}
	return builder.String()
}

func formatTopicClusterList(clusters []string) string {
	if len(clusters) == 0 {
		return ""
	}
	return strings.Join(clusters, "\n") + "\n"
}

func formatTopicStatus(entries []topicStatusEntry) string {
	sorted := append([]topicStatusEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].BrokerName != sorted[j].BrokerName {
			return sorted[i].BrokerName < sorted[j].BrokerName
		}
		return sorted[i].QueueID < sorted[j].QueueID
	})
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-32s  %-4s  %-20s  %-20s    %s\n", "#Broker Name", "#QID", "#Min Offset", "#Max Offset", "#Last Updated"))
	for _, entry := range sorted {
		lastUpdated := ""
		if entry.LastUpdateTimestamp > 0 {
			lastUpdated = formatRocketMQMillis(entry.LastUpdateTimestamp)
		}
		builder.WriteString(fmt.Sprintf("%-32s  %-4d  %-20d  %-20d    %s\n",
			frontStringAtLeast(entry.BrokerName, 32),
			entry.QueueID,
			entry.MinOffset,
			entry.MaxOffset,
			lastUpdated,
		))
	}
	return builder.String()
}

func formatRocketMQMillis(timestamp int64) string {
	return time.UnixMilli(timestamp).In(time.Local).Format("2006-01-02 15:04:05,000")
}

func frontStringAtLeast(value string, length int) string {
	if len(value) <= length {
		return value
	}
	return value[:length]
}

func formatTopicRoute(body []byte) (string, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	decoder := json.NewDecoder(bytes.NewReader([]byte(normalized)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("解析 topicRoute JSON 失败: %w", err)
	}
	return formatFastJSONValue(value, 0) + "\n", nil
}

func formatTopicRouteList(body []byte) (string, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	var payload struct {
		BrokerDatas []struct {
			BrokerName  string            `json:"brokerName"`
			BrokerAddrs map[string]string `json:"brokerAddrs"`
			Cluster     string            `json:"cluster"`
		} `json:"brokerDatas"`
		QueueDatas []struct {
			BrokerName     string `json:"brokerName"`
			Perm           int    `json:"perm"`
			ReadQueueNums  int    `json:"readQueueNums"`
			WriteQueueNums int    `json:"writeQueueNums"`
			TopicSysFlag   int    `json:"topicSysFlag"`
		} `json:"queueDatas"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return "", fmt.Errorf("解析 topicRoute -l JSON 失败: %w", err)
	}
	queueByBroker := make(map[string]struct {
		Perm           int
		ReadQueueNums  int
		WriteQueueNums int
	}, len(payload.QueueDatas))
	for _, queue := range payload.QueueDatas {
		queueByBroker[queue.BrokerName] = struct {
			Perm           int
			ReadQueueNums  int
			WriteQueueNums int
		}{Perm: queue.Perm, ReadQueueNums: queue.ReadQueueNums, WriteQueueNums: queue.WriteQueueNums}
	}
	brokers := append([]struct {
		BrokerName  string            `json:"brokerName"`
		BrokerAddrs map[string]string `json:"brokerAddrs"`
		Cluster     string            `json:"cluster"`
	}{}, payload.BrokerDatas...)
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].BrokerName < brokers[j].BrokerName
	})

	const topicRouteListFormat = "%-45s %-32s %-50s %-10s %-11s %-5s\n"
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(topicRouteListFormat, "#ClusterName", "#BrokerName", "#BrokerAddrs", "#ReadQueue", "#WriteQueue", "#Perm"))
	totalReadQueue := 0
	totalWriteQueue := 0
	for _, broker := range brokers {
		queue, ok := queueByBroker[broker.BrokerName]
		if !ok {
			return "", fmt.Errorf("topicRoute -l 缺少 Broker %s 的 QueueData", broker.BrokerName)
		}
		totalReadQueue += queue.ReadQueueNums
		totalWriteQueue += queue.WriteQueueNums
		builder.WriteString(fmt.Sprintf(topicRouteListFormat,
			broker.Cluster,
			broker.BrokerName,
			formatJavaMapStringString(broker.BrokerAddrs),
			strconv.Itoa(queue.ReadQueueNums),
			strconv.Itoa(queue.WriteQueueNums),
			strconv.Itoa(queue.Perm),
		))
	}
	builder.WriteString(strings.Repeat("-", 158))
	builder.WriteByte('\n')
	builder.WriteString(fmt.Sprintf(topicRouteListFormat, "Total:", strconv.Itoa(len(queueByBroker)), "", strconv.Itoa(totalReadQueue), strconv.Itoa(totalWriteQueue), ""))
	return builder.String(), nil
}

func formatJavaMapStringString(values map[string]string) string {
	if len(values) == 0 {
		return "{}"
	}
	keys := sortedKeysAnyString(values)
	var builder strings.Builder
	builder.WriteByte('{')
	for index, key := range keys {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(values[key])
	}
	builder.WriteByte('}')
	return builder.String()
}

type topicRouteBroker struct {
	BrokerName  string
	BrokerAddrs map[string]string
	Cluster     string
}

func (broker topicRouteBroker) selectAddr() string {
	if addr := strings.TrimSpace(broker.BrokerAddrs["0"]); addr != "" {
		return addr
	}
	keys := sortedKeysAnyString(broker.BrokerAddrs)
	for _, key := range keys {
		if addr := strings.TrimSpace(broker.BrokerAddrs[key]); addr != "" {
			return addr
		}
	}
	return ""
}

func masterBrokerAddrs(brokers []topicRouteBroker) []string {
	addrs := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		addr := strings.TrimSpace(broker.BrokerAddrs["0"])
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func decodeTopicRouteBrokers(body []byte) ([]topicRouteBroker, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	var payload struct {
		BrokerDatas []struct {
			BrokerName  string            `json:"brokerName"`
			BrokerAddrs map[string]string `json:"brokerAddrs"`
			Cluster     string            `json:"cluster"`
		} `json:"brokerDatas"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, fmt.Errorf("解析 topicRoute Broker 失败: %w", err)
	}
	brokers := make([]topicRouteBroker, 0, len(payload.BrokerDatas))
	for _, broker := range payload.BrokerDatas {
		brokers = append(brokers, topicRouteBroker{BrokerName: broker.BrokerName, BrokerAddrs: broker.BrokerAddrs, Cluster: broker.Cluster})
	}
	return brokers, nil
}

type topicRoutePermQueueData struct {
	// BrokerName 是 QueueData 归属的 Broker 名称，用于 broker 模式找旧权限。
	BrokerName string
	// Perm 是 TopicRoute 中现有权限，官方输出的 oldPerm 来自这里。
	Perm int
	// ReadQueueNums 是写回 TopicConfig 时保留的读队列数。
	ReadQueueNums int
	// WriteQueueNums 是写回 TopicConfig 时保留的写队列数。
	WriteQueueNums int
	// TopicSysFlag 是写回 TopicConfig 时保留的系统标志。
	TopicSysFlag int
}

type topicRoutePermData struct {
	// Brokers 是 TopicRoute 中的 BrokerData 列表，用于匹配 master broker 地址。
	Brokers []topicRouteBroker
	// QueueDatas 是 TopicRoute 中的 QueueData 列表，用于继承队列数和旧权限。
	QueueDatas []topicRoutePermQueueData
}

func (route topicRoutePermData) masterBrokerNameByAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	for _, broker := range route.Brokers {
		if strings.TrimSpace(broker.BrokerAddrs["0"]) == addr {
			return broker.BrokerName
		}
	}
	return ""
}

func (route topicRoutePermData) queueDataByBrokerName(brokerName string) (topicRoutePermQueueData, bool) {
	for _, queue := range route.QueueDatas {
		if queue.BrokerName == brokerName {
			return queue, true
		}
	}
	return topicRoutePermQueueData{}, false
}

func decodeTopicRouteForPerm(body []byte) (topicRoutePermData, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	var payload struct {
		BrokerDatas []struct {
			BrokerName  string            `json:"brokerName"`
			BrokerAddrs map[string]string `json:"brokerAddrs"`
			Cluster     string            `json:"cluster"`
		} `json:"brokerDatas"`
		QueueDatas []struct {
			BrokerName     string `json:"brokerName"`
			Perm           int    `json:"perm"`
			ReadQueueNums  int    `json:"readQueueNums"`
			WriteQueueNums int    `json:"writeQueueNums"`
			TopicSysFlag   int    `json:"topicSysFlag"`
		} `json:"queueDatas"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return topicRoutePermData{}, fmt.Errorf("解析 updateTopicPerm topicRoute 失败: %w", err)
	}
	route := topicRoutePermData{
		Brokers:    make([]topicRouteBroker, 0, len(payload.BrokerDatas)),
		QueueDatas: make([]topicRoutePermQueueData, 0, len(payload.QueueDatas)),
	}
	for _, broker := range payload.BrokerDatas {
		route.Brokers = append(route.Brokers, topicRouteBroker{
			BrokerName:  broker.BrokerName,
			BrokerAddrs: broker.BrokerAddrs,
			Cluster:     broker.Cluster,
		})
	}
	for _, queue := range payload.QueueDatas {
		route.QueueDatas = append(route.QueueDatas, topicRoutePermQueueData{
			BrokerName:     queue.BrokerName,
			Perm:           queue.Perm,
			ReadQueueNums:  queue.ReadQueueNums,
			WriteQueueNums: queue.WriteQueueNums,
			TopicSysFlag:   queue.TopicSysFlag,
		})
	}
	return route, nil
}

func decodeTopicStatsBody(body []byte) ([]topicStatusEntry, error) {
	raw := string(body)
	offsetIndex := strings.Index(raw, `"offsetTable"`)
	if offsetIndex < 0 {
		return nil, errors.New("topicStatus body 缺少 offsetTable")
	}
	colonIndex := strings.Index(raw[offsetIndex:], ":")
	if colonIndex < 0 {
		return nil, errors.New("topicStatus body offsetTable 缺少冒号")
	}
	cursor := offsetIndex + colonIndex + 1
	cursor = skipJSONSpaces(raw, cursor)
	if cursor >= len(raw) || raw[cursor] != '{' {
		return nil, errors.New("topicStatus body offsetTable 不是对象")
	}
	cursor++
	entries := make([]topicStatusEntry, 0)
	for {
		cursor = skipJSONDelimiters(raw, cursor)
		if cursor >= len(raw) {
			return nil, errors.New("topicStatus body offsetTable 未闭合")
		}
		if raw[cursor] == '}' {
			break
		}
		keyStart := cursor
		keyEnd, err := findJSONObjectEnd(raw, keyStart)
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(raw, keyEnd+1)
		if cursor >= len(raw) || raw[cursor] != ':' {
			return nil, errors.New("topicStatus body offsetTable key 后缺少冒号")
		}
		cursor = skipJSONSpaces(raw, cursor+1)
		valueStart := cursor
		valueEnd, err := findJSONObjectEnd(raw, valueStart)
		if err != nil {
			return nil, err
		}
		entry, err := decodeTopicStatsEntry(raw[keyStart:keyEnd+1], raw[valueStart:valueEnd+1])
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		cursor = valueEnd + 1
	}
	return entries, nil
}

func decodeTopicStatsEntry(keyJSON string, valueJSON string) (topicStatusEntry, error) {
	var key struct {
		BrokerName string `json:"brokerName"`
		QueueID    int    `json:"queueId"`
	}
	if err := json.Unmarshal([]byte(keyJSON), &key); err != nil {
		return topicStatusEntry{}, fmt.Errorf("解析 topicStatus 队列 key 失败: %w", err)
	}
	var value struct {
		LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
		MaxOffset           int64 `json:"maxOffset"`
		MinOffset           int64 `json:"minOffset"`
	}
	if err := json.Unmarshal([]byte(valueJSON), &value); err != nil {
		return topicStatusEntry{}, fmt.Errorf("解析 topicStatus offset value 失败: %w", err)
	}
	return topicStatusEntry{
		BrokerName:          key.BrokerName,
		QueueID:             key.QueueID,
		MinOffset:           value.MinOffset,
		MaxOffset:           value.MaxOffset,
		LastUpdateTimestamp: value.LastUpdateTimestamp,
	}, nil
}

func decodeBrokerStatsDataBody(body []byte) (*brokerStatsData, error) {
	var stats brokerStatsData
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, fmt.Errorf("解析 broker stats data 失败: %w", err)
	}
	return &stats, nil
}

func brokerStats24HourSum(stats *brokerStatsData) int64 {
	if stats == nil {
		return 0
	}
	if stats.StatsDay.Sum != 0 {
		return stats.StatsDay.Sum
	}
	if stats.StatsHour.Sum != 0 {
		return stats.StatsHour.Sum
	}
	if stats.StatsMinute.Sum != 0 {
		return stats.StatsMinute.Sum
	}
	return 0
}

func exportMetricsStatsMinuteTPS(stats *brokerStatsData) float64 {
	if stats == nil {
		return 0
	}
	return stats.StatsMinute.TPS
}

func exportMetricsFloatText(value float64) string {
	if value == 0 {
		return "0.0"
	}
	text := strconv.FormatFloat(value, 'f', -1, 64)
	if !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

func exportMetricsJavaHashSetStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	ordered := append([]string(nil), values...)
	sort.Strings(ordered)
	capacity := javaHashMapCapacity(len(ordered))
	sort.SliceStable(ordered, func(i, j int) bool {
		return javaHashMapBucketWithCapacity(ordered[i], capacity) < javaHashMapBucketWithCapacity(ordered[j], capacity)
	})
	return ordered
}

func decodeBrokerConsumeStatsBody(body []byte) (*brokerConsumeStats, error) {
	raw := string(body)
	totalDiff, err := jsonInt64Field(raw, "totalDiff")
	if err != nil {
		return nil, err
	}
	totalInflightDiff, err := jsonInt64Field(raw, "totalInflightDiff")
	if err != nil {
		return nil, err
	}
	brokerAddr, _, err := jsonStringField(raw, "brokerAddr")
	if err != nil {
		return nil, err
	}
	consumeStatsList, err := jsonArrayField(raw, "consumeStatsList")
	if err != nil {
		return nil, err
	}
	groups, err := decodeBrokerConsumeStatsGroups(consumeStatsList)
	if err != nil {
		return nil, err
	}
	return &brokerConsumeStats{
		BrokerAddr:        brokerAddr,
		TotalDiff:         totalDiff,
		TotalInflightDiff: totalInflightDiff,
		Groups:            groups,
	}, nil
}

func decodeBrokerConsumeStatsGroups(rawArray string) ([]brokerConsumeStatsGroup, error) {
	cursor := skipJSONSpaces(rawArray, 0)
	if cursor >= len(rawArray) || rawArray[cursor] != '[' {
		return nil, errors.New("brokerConsumeStats consumeStatsList 不是数组")
	}
	cursor++
	groups := make([]brokerConsumeStatsGroup, 0)
	for {
		cursor = skipJSONDelimiters(rawArray, cursor)
		if cursor >= len(rawArray) {
			return nil, errors.New("brokerConsumeStats consumeStatsList 未闭合")
		}
		if rawArray[cursor] == ']' {
			return groups, nil
		}
		objectEnd, err := findJSONObjectEnd(rawArray, cursor)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeBrokerConsumeStatsGroupMap(rawArray[cursor : objectEnd+1])
		if err != nil {
			return nil, err
		}
		groups = append(groups, decoded...)
		cursor = objectEnd + 1
	}
}

func decodeBrokerConsumeStatsGroupMap(rawObject string) ([]brokerConsumeStatsGroup, error) {
	cursor := skipJSONSpaces(rawObject, 0)
	if cursor >= len(rawObject) || rawObject[cursor] != '{' {
		return nil, errors.New("brokerConsumeStats group map 不是对象")
	}
	cursor++
	groups := make([]brokerConsumeStatsGroup, 0)
	for {
		cursor = skipJSONDelimiters(rawObject, cursor)
		if cursor >= len(rawObject) {
			return nil, errors.New("brokerConsumeStats group map 未闭合")
		}
		if rawObject[cursor] == '}' {
			return groups, nil
		}
		groupName, next, err := decodeJSONStringAt(rawObject, cursor)
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(rawObject, next)
		if cursor >= len(rawObject) || rawObject[cursor] != ':' {
			return nil, errors.New("brokerConsumeStats group map key 后缺少冒号")
		}
		cursor = skipJSONSpaces(rawObject, cursor+1)
		arrayEnd, err := findJSONArrayEnd(rawObject, cursor)
		if err != nil {
			return nil, err
		}
		stats, err := decodeBrokerConsumeStatsArray(rawObject[cursor : arrayEnd+1])
		if err != nil {
			return nil, err
		}
		groups = append(groups, brokerConsumeStatsGroup{Group: groupName, Stats: stats})
		cursor = arrayEnd + 1
	}
}

func decodeBrokerConsumeStatsArray(rawArray string) ([]consumerProgress, error) {
	cursor := skipJSONSpaces(rawArray, 0)
	if cursor >= len(rawArray) || rawArray[cursor] != '[' {
		return nil, errors.New("brokerConsumeStats group value 不是数组")
	}
	cursor++
	stats := make([]consumerProgress, 0)
	for {
		cursor = skipJSONDelimiters(rawArray, cursor)
		if cursor >= len(rawArray) {
			return nil, errors.New("brokerConsumeStats group value 未闭合")
		}
		if rawArray[cursor] == ']' {
			return stats, nil
		}
		objectEnd, err := findJSONObjectEnd(rawArray, cursor)
		if err != nil {
			return nil, err
		}
		progress, err := decodeConsumeStatsBody([]byte(rawArray[cursor : objectEnd+1]))
		if err != nil {
			return nil, err
		}
		stats = append(stats, *progress)
		cursor = objectEnd + 1
	}
}

func decodeConsumeStatsBody(body []byte) (*consumerProgress, error) {
	raw := string(body)
	consumeTPS, err := jsonNumberField(raw, "consumeTps")
	if err != nil {
		return nil, err
	}
	offsetIndex := strings.Index(raw, `"offsetTable"`)
	if offsetIndex < 0 {
		return nil, errors.New("consumerProgress body 缺少 offsetTable")
	}
	colonIndex := strings.Index(raw[offsetIndex:], ":")
	if colonIndex < 0 {
		return nil, errors.New("consumerProgress body offsetTable 缺少冒号")
	}
	cursor := offsetIndex + colonIndex + 1
	cursor = skipJSONSpaces(raw, cursor)
	if cursor >= len(raw) || raw[cursor] != '{' {
		return nil, errors.New("consumerProgress body offsetTable 不是对象")
	}
	cursor++
	progress := &consumerProgress{ConsumeTPS: consumeTPS}
	for {
		cursor = skipJSONDelimiters(raw, cursor)
		if cursor >= len(raw) {
			return nil, errors.New("consumerProgress body offsetTable 未闭合")
		}
		if raw[cursor] == '}' {
			break
		}
		keyStart := cursor
		keyEnd, err := findJSONObjectEnd(raw, keyStart)
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(raw, keyEnd+1)
		if cursor >= len(raw) || raw[cursor] != ':' {
			return nil, errors.New("consumerProgress body offsetTable key 后缺少冒号")
		}
		cursor = skipJSONSpaces(raw, cursor+1)
		valueStart := cursor
		valueEnd, err := findJSONObjectEnd(raw, valueStart)
		if err != nil {
			return nil, err
		}
		entry, err := decodeConsumeStatsEntry(raw[keyStart:keyEnd+1], raw[valueStart:valueEnd+1])
		if err != nil {
			return nil, err
		}
		progress.Entries = append(progress.Entries, entry)
		cursor = valueEnd + 1
	}
	return progress, nil
}

func decodeConsumeStatsEntry(keyJSON string, valueJSON string) (consumerProgressEntry, error) {
	var key struct {
		Topic      string `json:"topic"`
		BrokerName string `json:"brokerName"`
		QueueID    int    `json:"queueId"`
	}
	if err := json.Unmarshal([]byte(keyJSON), &key); err != nil {
		return consumerProgressEntry{}, fmt.Errorf("解析 consumerProgress 队列 key 失败: %w", err)
	}
	var value struct {
		BrokerOffset   int64 `json:"brokerOffset"`
		ConsumerOffset int64 `json:"consumerOffset"`
		PullOffset     int64 `json:"pullOffset"`
		LastTimestamp  int64 `json:"lastTimestamp"`
	}
	if err := json.Unmarshal([]byte(valueJSON), &value); err != nil {
		return consumerProgressEntry{}, fmt.Errorf("解析 consumerProgress offset value 失败: %w", err)
	}
	return consumerProgressEntry{
		Topic:          key.Topic,
		BrokerName:     key.BrokerName,
		QueueID:        key.QueueID,
		BrokerOffset:   value.BrokerOffset,
		ConsumerOffset: value.ConsumerOffset,
		PullOffset:     value.PullOffset,
		LastTimestamp:  value.LastTimestamp,
	}, nil
}

// decodeResetOffsetBody 解码 ResetOffsetBody.offsetTable，保留 Broker fastjson body 中的队列键和值。
func decodeResetOffsetBody(body []byte) ([]skipAccumulatedMessageRow, error) {
	raw := string(body)
	offsetIndex := strings.Index(raw, `"offsetTable"`)
	if offsetIndex < 0 {
		return nil, errors.New("resetOffset body 缺少 offsetTable")
	}
	colonIndex := strings.Index(raw[offsetIndex:], ":")
	if colonIndex < 0 {
		return nil, errors.New("resetOffset body offsetTable 缺少冒号")
	}
	cursor := offsetIndex + colonIndex + 1
	cursor = skipJSONSpaces(raw, cursor)
	if cursor >= len(raw) || raw[cursor] != '{' {
		return nil, errors.New("resetOffset body offsetTable 不是对象")
	}
	cursor++
	rows := make([]skipAccumulatedMessageRow, 0)
	for {
		cursor = skipJSONDelimiters(raw, cursor)
		if cursor >= len(raw) {
			return nil, errors.New("resetOffset body offsetTable 未闭合")
		}
		if raw[cursor] == '}' {
			return rows, nil
		}
		keyStart := cursor
		keyEnd, err := findJSONObjectEnd(raw, keyStart)
		if err != nil {
			return nil, err
		}
		queue, err := decodeMessageQueueIdentity(raw[keyStart : keyEnd+1])
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(raw, keyEnd+1)
		if cursor >= len(raw) || raw[cursor] != ':' {
			return nil, errors.New("resetOffset body offsetTable key 后缺少冒号")
		}
		cursor = skipJSONSpaces(raw, cursor+1)
		valueEnd := cursor
		for valueEnd < len(raw) && raw[valueEnd] != ',' && raw[valueEnd] != '}' {
			valueEnd++
		}
		valueText := strings.TrimSpace(raw[cursor:valueEnd])
		offset, err := strconv.ParseInt(valueText, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("解析 resetOffset offset 失败: %w", err)
		}
		rows = append(rows, skipAccumulatedMessageRow{Queue: queue, Offset: offset})
		cursor = valueEnd
	}
}

// decodeConsumerConnectionBody 解码 ConsumerConnection body，并转成官方 consumerProgress summary 展示字段。
func decodeConsumerConnectionBody(body []byte) (*consumerConnectionSummary, error) {
	detail, err := decodeConsumerConnectionDetailBody(body)
	if err != nil {
		return nil, err
	}
	summary := &consumerConnectionSummary{
		Count:        len(detail.Connections),
		ConsumeType:  consumerConnectionConsumeTypeDesc(detail.ConsumeType),
		MessageModel: consumerConnectionMessageModelDesc(detail.MessageModel, detail.ConsumeType),
		ClientIDs:    make([]string, 0, len(detail.Connections)),
	}
	if summary.Count == 0 {
		summary.Version = "OFFLINE"
		return summary, nil
	}
	minVersion := detail.Connections[0].Version
	for _, connection := range detail.Connections {
		if strings.TrimSpace(connection.ClientID) != "" {
			summary.ClientIDs = append(summary.ClientIDs, strings.TrimSpace(connection.ClientID))
		}
		if connection.Version < minVersion {
			minVersion = connection.Version
		}
	}
	summary.Version = rocketMQVersionDesc(minVersion)
	return summary, nil
}

// decodeConsumerConnectionDetailBody 解码 ConsumerConnection body，并保留官方 connection 与 subscription 的原始顺序。
func decodeConsumerConnectionDetailBody(body []byte) (*consumerConnectionDetail, error) {
	var payload struct {
		ConnectionSet []struct {
			ClientID   string `json:"clientId"`
			ClientAddr string `json:"clientAddr"`
			Language   string `json:"language"`
			Version    int    `json:"version"`
		} `json:"connectionSet"`
		SubscriptionTable json.RawMessage `json:"subscriptionTable"`
		ConsumeType       string          `json:"consumeType"`
		MessageModel      string          `json:"messageModel"`
		ConsumeFromWhere  string          `json:"consumeFromWhere"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 consumerConnection 失败: %w", err)
	}
	detail := &consumerConnectionDetail{
		Connections:      make([]consumerConnectionEntry, 0, len(payload.ConnectionSet)),
		ConsumeType:      strings.TrimSpace(payload.ConsumeType),
		MessageModel:     strings.TrimSpace(payload.MessageModel),
		ConsumeFromWhere: strings.TrimSpace(payload.ConsumeFromWhere),
	}
	for _, connection := range payload.ConnectionSet {
		detail.Connections = append(detail.Connections, consumerConnectionEntry{
			ClientID:   strings.TrimSpace(connection.ClientID),
			ClientAddr: strings.TrimSpace(connection.ClientAddr),
			Language:   strings.TrimSpace(connection.Language),
			Version:    connection.Version,
		})
	}
	subscriptions, err := decodeConsumerSubscriptionsBody(payload.SubscriptionTable)
	if err != nil {
		return nil, err
	}
	detail.Subscriptions = subscriptions
	return detail, nil
}

// decodeProducerConnectionBody 解码 ProducerConnection body，并保留官方 connectionSet 的原始顺序。
func decodeProducerConnectionBody(body []byte) (*producerConnectionDetail, error) {
	var payload struct {
		ConnectionSet []struct {
			ClientID   string `json:"clientId"`
			ClientAddr string `json:"clientAddr"`
			Language   string `json:"language"`
			Version    int    `json:"version"`
		} `json:"connectionSet"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 producerConnection 失败: %w", err)
	}
	detail := &producerConnectionDetail{
		Connections: make([]producerConnectionEntry, 0, len(payload.ConnectionSet)),
	}
	for _, connection := range payload.ConnectionSet {
		detail.Connections = append(detail.Connections, producerConnectionEntry{
			ClientID:   strings.TrimSpace(connection.ClientID),
			ClientAddr: strings.TrimSpace(connection.ClientAddr),
			Language:   strings.TrimSpace(connection.Language),
			Version:    connection.Version,
		})
	}
	return detail, nil
}

// decodeProducerTableInfoBody 解码官方 ProducerTableInfo，保留 data map 的 JSON 读入顺序供 HashMap 排序稳定使用。
func decodeProducerTableInfoBody(body []byte) (*producerTableInfo, error) {
	var payload struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 producer 失败: %w", err)
	}
	table := &producerTableInfo{}
	raw := bytes.TrimSpace(payload.Data)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return table, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("解析 producer data 失败: %w", err)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("解析 producer data 失败: data 不是对象")
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("解析 producer group 失败: %w", err)
		}
		group, ok := token.(string)
		if !ok {
			return nil, errors.New("解析 producer group 失败: group key 不是字符串")
		}
		var producers []struct {
			ClientID            string `json:"clientId"`
			RemoteIP            string `json:"remoteIP"`
			Language            string `json:"language"`
			Version             int    `json:"version"`
			LastUpdateTimestamp int64  `json:"lastUpdateTimestamp"`
		}
		if err := decoder.Decode(&producers); err != nil {
			return nil, fmt.Errorf("解析 producer group %s 失败: %w", group, err)
		}
		info := producerGroupInfo{
			Group:     strings.TrimSpace(group),
			Producers: make([]producerInfo, 0, len(producers)),
		}
		for _, producer := range producers {
			info.Producers = append(info.Producers, producerInfo{
				ClientID:            strings.TrimSpace(producer.ClientID),
				RemoteIP:            strings.TrimSpace(producer.RemoteIP),
				Language:            strings.TrimSpace(producer.Language),
				Version:             producer.Version,
				LastUpdateTimestamp: producer.LastUpdateTimestamp,
			})
		}
		table.Groups = append(table.Groups, info)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("解析 producer data 结束失败: %w", err)
	}
	return table, nil
}

// decodeHAStatusBody 解码 HARuntimeInfo body，覆盖 master 与 slave 两种官方输出分支。
func decodeHAStatusBody(body []byte) (*haStatusResult, error) {
	var payload struct {
		Master                   bool  `json:"master"`
		MasterCommitLogMaxOffset int64 `json:"masterCommitLogMaxOffset"`
		InSyncSlaveNums          int   `json:"inSyncSlaveNums"`
		HAConnectionInfo         []struct {
			Addr                    string `json:"addr"`
			SlaveAckOffset          int64  `json:"slaveAckOffset"`
			Diff                    int64  `json:"diff"`
			InSync                  bool   `json:"inSync"`
			TransferredByteInSecond int64  `json:"transferredByteInSecond"`
			TransferFromWhere       int64  `json:"transferFromWhere"`
		} `json:"haConnectionInfo"`
		HAClientRuntimeInfo struct {
			MasterAddr              string `json:"masterAddr"`
			TransferredByteInSecond int64  `json:"transferredByteInSecond"`
			MaxOffset               int64  `json:"maxOffset"`
			LastReadTimestamp       int64  `json:"lastReadTimestamp"`
			LastWriteTimestamp      int64  `json:"lastWriteTimestamp"`
			MasterFlushOffset       int64  `json:"masterFlushOffset"`
		} `json:"haClientRuntimeInfo"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 haStatus 失败: %w", err)
	}
	result := &haStatusResult{
		Master:                   payload.Master,
		MasterCommitLogMaxOffset: payload.MasterCommitLogMaxOffset,
		InSyncSlaveNums:          payload.InSyncSlaveNums,
		HAConnectionInfo:         make([]haConnectionRuntimeInfo, 0, len(payload.HAConnectionInfo)),
		HAClientRuntimeInfo: haClientRuntimeInfo{
			MasterAddr:              strings.TrimSpace(payload.HAClientRuntimeInfo.MasterAddr),
			TransferredByteInSecond: payload.HAClientRuntimeInfo.TransferredByteInSecond,
			MaxOffset:               payload.HAClientRuntimeInfo.MaxOffset,
			LastReadTimestamp:       payload.HAClientRuntimeInfo.LastReadTimestamp,
			LastWriteTimestamp:      payload.HAClientRuntimeInfo.LastWriteTimestamp,
			MasterFlushOffset:       payload.HAClientRuntimeInfo.MasterFlushOffset,
		},
	}
	for _, connection := range payload.HAConnectionInfo {
		result.HAConnectionInfo = append(result.HAConnectionInfo, haConnectionRuntimeInfo{
			Addr:                    strings.TrimSpace(connection.Addr),
			SlaveAckOffset:          connection.SlaveAckOffset,
			Diff:                    connection.Diff,
			InSync:                  connection.InSync,
			TransferredByteInSecond: connection.TransferredByteInSecond,
			TransferFromWhere:       connection.TransferFromWhere,
		})
	}
	return result, nil
}

// decodeQueryConsumeQueueBody 解码 QueryConsumeQueueResponseBody，并把 Java toString 会显示为 null 的字段转为字面量。
func decodeQueryConsumeQueueBody(body []byte) (*queryConsumeQueueResult, error) {
	var payload struct {
		SubscriptionData json.RawMessage `json:"subscriptionData"`
		FilterData       string          `json:"filterData"`
		QueueData        []struct {
			PhysicOffset   int64   `json:"physicOffset"`
			PhysicSize     int     `json:"physicSize"`
			TagsCode       int64   `json:"tagsCode"`
			ExtendDataJSON *string `json:"extendDataJson"`
			BitMap         *string `json:"bitMap"`
			Eval           bool    `json:"eval"`
			Msg            *string `json:"msg"`
		} `json:"queueData"`
		MaxQueueIndex int64 `json:"maxQueueIndex"`
		MinQueueIndex int64 `json:"minQueueIndex"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 queryCq 失败: %w", err)
	}
	result := &queryConsumeQueueResult{
		SubscriptionData: strings.TrimSpace(string(payload.SubscriptionData)),
		FilterData:       payload.FilterData,
		QueueData:        make([]consumeQueueData, 0, len(payload.QueueData)),
		MaxQueueIndex:    payload.MaxQueueIndex,
		MinQueueIndex:    payload.MinQueueIndex,
	}
	for _, data := range payload.QueueData {
		result.QueueData = append(result.QueueData, consumeQueueData{
			PhysicOffset:   data.PhysicOffset,
			PhysicSize:     data.PhysicSize,
			TagsCode:       data.TagsCode,
			ExtendDataJSON: stringOrNull(data.ExtendDataJSON),
			BitMap:         stringOrNull(data.BitMap),
			Eval:           data.Eval,
			Msg:            stringOrNull(data.Msg),
		})
	}
	return result, nil
}

func stringOrNull(value *string) string {
	if value == nil {
		return "null"
	}
	return *value
}

// decodeConsumerSubscriptionsBody 按官方 subscriptionTable 的 JSON 对象顺序解码订阅信息。
func decodeConsumerSubscriptionsBody(body []byte) ([]consumerSubscriptionEntry, error) {
	raw := strings.TrimSpace(string(body))
	if raw == "" || raw == "null" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	tok, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("解析 consumerConnection subscriptionTable 失败: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("consumerConnection subscriptionTable 不是对象")
	}
	subscriptions := make([]consumerSubscriptionEntry, 0)
	for decoder.More() {
		keyTok, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("解析 consumerConnection subscriptionTable key 失败: %w", err)
		}
		topic, ok := keyTok.(string)
		if !ok {
			return nil, errors.New("consumerConnection subscriptionTable key 不是字符串")
		}
		var payload struct {
			Topic      string `json:"topic"`
			SubString  string `json:"subString"`
			Expression string `json:"expression"`
		}
		if err := decoder.Decode(&payload); err != nil {
			return nil, fmt.Errorf("解析 consumerConnection subscriptionTable value 失败: %w", err)
		}
		expression := strings.TrimSpace(payload.SubString)
		if expression == "" {
			expression = strings.TrimSpace(payload.Expression)
		}
		subTopic := strings.TrimSpace(payload.Topic)
		if subTopic == "" {
			subTopic = strings.TrimSpace(topic)
		}
		subscriptions = append(subscriptions, consumerSubscriptionEntry{
			Topic:      subTopic,
			Expression: expression,
		})
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("解析 consumerConnection subscriptionTable 结束符失败: %w", err)
	}
	return subscriptions, nil
}

func formatConsumerConnection(detail *consumerConnectionDetail) string {
	if detail == nil {
		detail = &consumerConnectionDetail{}
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-36s %-22s %-10s %s\n", "#ClientId", "#ClientAddr", "#Language", "#Version"))
	for _, connection := range detail.Connections {
		builder.WriteString(fmt.Sprintf("%-36s %-22s %-10s %s\n",
			frontStringAtLeast(connection.ClientID, 36),
			frontStringAtLeast(connection.ClientAddr, 22),
			frontStringAtLeast(connection.Language, 10),
			rocketMQVersionDesc(connection.Version),
		))
	}
	builder.WriteString("\nBelow is subscription:\n")
	builder.WriteString(fmt.Sprintf("%-20s %s\n", "#Topic", "#SubExpression"))
	for _, subscription := range detail.Subscriptions {
		builder.WriteString(fmt.Sprintf("%-20s %s\n", subscription.Topic, subscription.Expression))
	}
	builder.WriteString("\n")
	builder.WriteString(fmt.Sprintf("ConsumeType: %s\n", detail.ConsumeType))
	builder.WriteString(fmt.Sprintf("MessageModel: %s\n", detail.MessageModel))
	builder.WriteString(fmt.Sprintf("ConsumeFromWhere: %s\n", detail.ConsumeFromWhere))
	return builder.String()
}

func formatConsumerRunningInfo(info *consumerRunningInfo) string {
	if info == nil {
		info = &consumerRunningInfo{}
	}
	var builder strings.Builder
	builder.WriteString("#Consumer Properties#\n")
	for _, property := range orderedConsumerRunningProperties(info.Properties) {
		builder.WriteString(fmt.Sprintf("%-40s: %s\n", property.Key, property.Value))
	}
	builder.WriteString("\n\n#Consumer Subscription#\n")
	subscriptions := append([]consumerRunningSubscription(nil), info.Subscriptions...)
	sort.SliceStable(subscriptions, func(left int, right int) bool {
		return subscriptions[left].Topic+"@"+subscriptions[left].SubString < subscriptions[right].Topic+"@"+subscriptions[right].SubString
	})
	for index, subscription := range subscriptions {
		builder.WriteString(fmt.Sprintf("%03d Topic: %-40s ClassFilter: %-8s SubExpression: %s\n",
			index+1,
			subscription.Topic,
			strconv.FormatBool(subscription.ClassFilterMode),
			subscription.SubString,
		))
	}
	mqTable := append([]consumerRunningProcessQueue(nil), info.MQTable...)
	sort.SliceStable(mqTable, func(left int, right int) bool {
		return compareMessageQueueIdentity(mqTable[left].Queue, mqTable[right].Queue) < 0
	})
	builder.WriteString("\n\n#Consumer Offset#\n")
	builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4s  %-20s\n", "#Topic", "#Broker Name", "#QID", "#Consumer Offset"))
	for _, entry := range mqTable {
		builder.WriteString(fmt.Sprintf("%-32s  %-32s  %-4d  %-20d\n",
			entry.Queue.Topic,
			entry.Queue.BrokerName,
			entry.Queue.QueueID,
			entry.Info.CommitOffset,
		))
	}
	builder.WriteString("\n\n#Consumer MQ Detail#\n")
	builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4s  %-20s\n", "#Topic", "#Broker Name", "#QID", "#ProcessQueueInfo"))
	for _, entry := range mqTable {
		builder.WriteString(fmt.Sprintf("%-64s  %-32s  %-4d  %s\n",
			entry.Queue.Topic,
			entry.Queue.BrokerName,
			entry.Queue.QueueID,
			formatProcessQueueInfo(entry.Info),
		))
	}
	popTable := append([]consumerRunningPopQueue(nil), info.MQPopTable...)
	sort.SliceStable(popTable, func(left int, right int) bool {
		return compareMessageQueueIdentity(popTable[left].Queue, popTable[right].Queue) < 0
	})
	builder.WriteString("\n\n#Consumer Pop Detail#\n")
	builder.WriteString(fmt.Sprintf("%-32s  %-32s  %-4s  %-20s\n", "#Topic", "#Broker Name", "#QID", "#ProcessQueueInfo"))
	for _, entry := range popTable {
		builder.WriteString(fmt.Sprintf("%-32s  %-32s  %-4d  %s\n",
			entry.Queue.Topic,
			entry.Queue.BrokerName,
			entry.Queue.QueueID,
			formatPopProcessQueueInfo(entry.Info),
		))
	}
	statusTable := append([]consumerRunningStatus(nil), info.StatusTable...)
	sort.SliceStable(statusTable, func(left int, right int) bool {
		return statusTable[left].Topic < statusTable[right].Topic
	})
	builder.WriteString("\n\n#Consumer RT&TPS#\n")
	builder.WriteString(fmt.Sprintf("%-64s  %14s %14s %14s %14s %18s %25s\n",
		"#Topic",
		"#Pull RT",
		"#Pull TPS",
		"#Consume RT",
		"#ConsumeOK TPS",
		"#ConsumeFailed TPS",
		"#ConsumeFailedMsgsInHour",
	))
	for _, entry := range statusTable {
		builder.WriteString(fmt.Sprintf("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25d\n",
			entry.Topic,
			entry.Status.PullRT,
			entry.Status.PullTPS,
			entry.Status.ConsumeRT,
			entry.Status.ConsumeOKTPS,
			entry.Status.ConsumeFailedTPS,
			entry.Status.ConsumeFailedMsgs,
		))
	}
	if info.UserConsumerInfo != nil {
		builder.WriteString("\n\n#User Consume Info#\n")
		userInfo := append([]orderedStringValue(nil), info.UserConsumerInfo...)
		sort.SliceStable(userInfo, func(left int, right int) bool {
			return userInfo[left].Key < userInfo[right].Key
		})
		for _, entry := range userInfo {
			builder.WriteString(fmt.Sprintf("%-40s: %s\n", entry.Key, entry.Value))
		}
	}
	if info.Jstack != "" {
		builder.WriteString("\n\n#Consumer jstack#\n")
		builder.WriteString(info.Jstack)
	}
	return builder.String()
}

func formatProcessQueueInfo(info processQueueInfo) string {
	return fmt.Sprintf("ProcessQueueInfo [commitOffset=%d, cachedMsgMinOffset=%d, cachedMsgMaxOffset=%d, cachedMsgCount=%d, cachedMsgSizeInMiB=%d, transactionMsgMinOffset=%d, transactionMsgMaxOffset=%d, transactionMsgCount=%d, locked=%t, tryUnlockTimes=%d, lastLockTimestamp=%s, droped=%t, lastPullTimestamp=%s, lastConsumeTimestamp=%s]",
		info.CommitOffset,
		info.CachedMsgMinOffset,
		info.CachedMsgMaxOffset,
		info.CachedMsgCount,
		info.CachedMsgSizeInMiB,
		info.TransactionMsgMinOffset,
		info.TransactionMsgMaxOffset,
		info.TransactionMsgCount,
		info.Locked,
		info.TryUnlockTimes,
		formatRocketMQHumanMillis(info.LastLockTimestamp),
		info.Droped,
		formatRocketMQHumanMillis(info.LastPullTimestamp),
		formatRocketMQHumanMillis(info.LastConsumeTimestamp),
	)
}

func formatPopProcessQueueInfo(info popProcessQueueInfo) string {
	return fmt.Sprintf("PopProcessQueueInfo [waitAckCount:%d, droped:%t, lastPopTimestamp:%d]",
		info.WaitAckCount,
		info.Droped,
		info.LastPopTimestamp,
	)
}

func formatRocketMQHumanMillis(timestamp int64) string {
	return time.UnixMilli(timestamp).In(time.Local).Format("20060102150405") + fmt.Sprintf("%03d", timestamp%1000)
}

func formatProducerConnection(detail *producerConnectionDetail) string {
	if detail == nil {
		detail = &producerConnectionDetail{}
	}
	var builder strings.Builder
	for index, connection := range detail.Connections {
		builder.WriteString(fmt.Sprintf("%04d  %-32s %-22s %-8s %s\n",
			index+1,
			connection.ClientID,
			connection.ClientAddr,
			connection.Language,
			rocketMQVersionDesc(connection.Version),
		))
	}
	return builder.String()
}

func formatProducerTableInfo(table *producerTableInfo) string {
	if table == nil {
		table = &producerTableInfo{}
	}
	var builder strings.Builder
	for _, group := range orderedProducerGroups(table.Groups) {
		if len(group.Producers) == 0 {
			builder.WriteString(fmt.Sprintf("producer group (%s) instances are empty\n", group.Group))
			continue
		}
		for _, producer := range group.Producers {
			builder.WriteString(fmt.Sprintf(
				"producer group (%s) instance : clientId=%s,remoteIP=%s, language=%s, version=%d, lastUpdateTimestamp=%d\n",
				group.Group,
				producer.ClientID,
				producer.RemoteIP,
				producer.Language,
				producer.Version,
				producer.LastUpdateTimestamp,
			))
		}
	}
	return builder.String()
}

func orderedProducerGroups(groups []producerGroupInfo) []producerGroupInfo {
	ordered := append([]producerGroupInfo(nil), groups...)
	capacity := javaHashMapCapacity(len(ordered))
	sort.SliceStable(ordered, func(i, j int) bool {
		return javaHashMapBucketWithCapacity(ordered[i].Group, capacity) < javaHashMapBucketWithCapacity(ordered[j].Group, capacity)
	})
	return ordered
}

func formatColdDataFlowCtrInfo(sections []coldDataFlowCtrInfoSection) (string, error) {
	var builder strings.Builder
	for _, section := range sections {
		builder.WriteByte(' ')
		builder.WriteString(strings.TrimRight(section.Header, "\r\n"))
		builder.WriteByte('\n')
		if strings.TrimSpace(section.Body) == "" {
			builder.WriteString(fmt.Sprintf("Broker[%s] has no cold ctr table !\n", section.BrokerAddr))
			continue
		}
		formatted, err := formatColdDataFlowCtrInfoBody(section.Body)
		if err != nil {
			return "", err
		}
		builder.WriteString(formatted)
	}
	return builder.String(), nil
}

func formatColdDataFlowCtrInfoBody(body string) (string, error) {
	value, err := decodeOrderedJSONValue(strings.TrimSpace(body))
	if err != nil {
		return "", err
	}
	if err := applyColdDataFlowCtrRuntimeFormats(&value); err != nil {
		return "", err
	}
	return formatOrderedFastJSONValue(value, 0) + "\n", nil
}

func applyColdDataFlowCtrRuntimeFormats(root *orderedJSONValue) error {
	runtimeTable, ok := root.objectField("runtimeTable")
	if !ok || runtimeTable.Kind != orderedJSONObject {
		return nil
	}
	for index := range runtimeTable.Pairs {
		entry := &runtimeTable.Pairs[index].Value
		if entry.Kind != orderedJSONObject {
			return fmt.Errorf("runtimeTable[%s] 不是 JSON 对象", runtimeTable.Pairs[index].Key)
		}
		lastColdValue, ok := entry.objectField("lastColdReadTimeMills")
		if !ok {
			return fmt.Errorf("runtimeTable[%s] 缺少 lastColdReadTimeMills", runtimeTable.Pairs[index].Key)
		}
		lastColdFormat, err := formatColdDataFlowCtrMillis(lastColdValue)
		if err != nil {
			return err
		}
		entry.putObjectString("lastColdReadTimeFormat", lastColdFormat)
		entry.removeObjectField("lastColdReadTimeMills")

		createValue, ok := entry.objectField("createTimeMills")
		if !ok {
			return fmt.Errorf("runtimeTable[%s] 缺少 createTimeMills", runtimeTable.Pairs[index].Key)
		}
		createFormat, err := formatColdDataFlowCtrMillis(createValue)
		if err != nil {
			return err
		}
		entry.putObjectString("createTimeFormat", createFormat)
		entry.removeObjectField("createTimeMills")
	}
	return nil
}

func formatColdDataFlowCtrMillis(value *orderedJSONValue) (string, error) {
	raw, ok := value.primitiveString()
	if !ok {
		return "", errors.New("冷数据流控时间戳不是标量")
	}
	millis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return "", err
	}
	return time.UnixMilli(millis).In(time.Local).Format("2006-01-02 15:04:05"), nil
}

type orderedJSONKind int

const (
	orderedJSONNull orderedJSONKind = iota
	orderedJSONObject
	orderedJSONArray
	orderedJSONString
	orderedJSONNumber
	orderedJSONBool
)

type orderedJSONPair struct {
	// Key 是 JSON 对象中的字段名，保留解析时的原始出现顺序用于 HashMap 同桶排序。
	Key string
	// Value 是字段对应的 JSON 值。
	Value orderedJSONValue
}

type orderedJSONValue struct {
	// Kind 标识当前 JSON 值的类型。
	Kind orderedJSONKind
	// Pairs 保存对象字段，只有 Kind 为 orderedJSONObject 时使用。
	Pairs []orderedJSONPair
	// Items 保存数组元素，只有 Kind 为 orderedJSONArray 时使用。
	Items []orderedJSONValue
	// Text 保存字符串或数字文本，只有 Kind 为 orderedJSONString 或 orderedJSONNumber 时使用。
	Text string
	// Bool 保存布尔值，只有 Kind 为 orderedJSONBool 时使用。
	Bool bool
}

func decodeOrderedJSONValue(raw string) (orderedJSONValue, error) {
	decoder := json.NewDecoder(strings.NewReader(normalizeFastJSONNumericKeys(raw)))
	decoder.UseNumber()
	value, err := decodeOrderedJSONToken(decoder)
	if err != nil {
		return orderedJSONValue{}, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return orderedJSONValue{}, err
		}
		return orderedJSONValue{}, fmt.Errorf("JSON 结尾存在多余 token %v", token)
	}
	return value, nil
}

func decodeOrderedJSONToken(decoder *json.Decoder) (orderedJSONValue, error) {
	token, err := decoder.Token()
	if err != nil {
		return orderedJSONValue{}, err
	}
	if token == nil {
		return orderedJSONValue{Kind: orderedJSONNull}, nil
	}
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			pairs := make([]orderedJSONPair, 0)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return orderedJSONValue{}, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return orderedJSONValue{}, fmt.Errorf("JSON 对象字段不是字符串: %v", keyToken)
				}
				item, err := decodeOrderedJSONToken(decoder)
				if err != nil {
					return orderedJSONValue{}, err
				}
				pairs = append(pairs, orderedJSONPair{Key: key, Value: item})
			}
			end, err := decoder.Token()
			if err != nil {
				return orderedJSONValue{}, err
			}
			if end != json.Delim('}') {
				return orderedJSONValue{}, fmt.Errorf("JSON 对象结束符非法: %v", end)
			}
			return orderedJSONValue{Kind: orderedJSONObject, Pairs: pairs}, nil
		case '[':
			items := make([]orderedJSONValue, 0)
			for decoder.More() {
				item, err := decodeOrderedJSONToken(decoder)
				if err != nil {
					return orderedJSONValue{}, err
				}
				items = append(items, item)
			}
			end, err := decoder.Token()
			if err != nil {
				return orderedJSONValue{}, err
			}
			if end != json.Delim(']') {
				return orderedJSONValue{}, fmt.Errorf("JSON 数组结束符非法: %v", end)
			}
			return orderedJSONValue{Kind: orderedJSONArray, Items: items}, nil
		default:
			return orderedJSONValue{}, fmt.Errorf("JSON 分隔符非法: %v", typed)
		}
	case string:
		return orderedJSONValue{Kind: orderedJSONString, Text: typed}, nil
	case json.Number:
		return orderedJSONValue{Kind: orderedJSONNumber, Text: typed.String()}, nil
	case bool:
		return orderedJSONValue{Kind: orderedJSONBool, Bool: typed}, nil
	default:
		return orderedJSONValue{}, fmt.Errorf("JSON token 类型不支持: %T", token)
	}
}

func (value *orderedJSONValue) objectField(key string) (*orderedJSONValue, bool) {
	if value == nil || value.Kind != orderedJSONObject {
		return nil, false
	}
	for index := range value.Pairs {
		if value.Pairs[index].Key == key {
			return &value.Pairs[index].Value, true
		}
	}
	return nil, false
}

func (value *orderedJSONValue) removeObjectField(key string) {
	if value == nil || value.Kind != orderedJSONObject {
		return
	}
	next := value.Pairs[:0]
	for _, pair := range value.Pairs {
		if pair.Key != key {
			next = append(next, pair)
		}
	}
	value.Pairs = next
}

func (value *orderedJSONValue) putObjectString(key string, text string) {
	if value == nil || value.Kind != orderedJSONObject {
		return
	}
	replacement := orderedJSONValue{Kind: orderedJSONString, Text: text}
	for index := range value.Pairs {
		if value.Pairs[index].Key == key {
			value.Pairs[index].Value = replacement
			return
		}
	}
	value.Pairs = append(value.Pairs, orderedJSONPair{Key: key, Value: replacement})
}

func (value *orderedJSONValue) primitiveString() (string, bool) {
	if value == nil {
		return "", false
	}
	switch value.Kind {
	case orderedJSONString, orderedJSONNumber:
		return value.Text, true
	case orderedJSONBool:
		return strconv.FormatBool(value.Bool), true
	case orderedJSONNull:
		return "null", true
	default:
		return "", false
	}
}

func formatOrderedFastJSONValue(value orderedJSONValue, indent int) string {
	switch value.Kind {
	case orderedJSONObject:
		return formatOrderedFastJSONObject(value.Pairs, indent)
	case orderedJSONArray:
		return formatOrderedFastJSONArray(value.Items, indent)
	case orderedJSONString:
		encoded, _ := json.Marshal(value.Text)
		return string(encoded)
	case orderedJSONNumber:
		return value.Text
	case orderedJSONBool:
		return strconv.FormatBool(value.Bool)
	default:
		return "null"
	}
}

// formatOrderedFastJSONCompactValue 复刻官方 fastjson.toJSONString 的单行输出，用于 rocksDBConfigToJson -j false raw 行。
func formatOrderedFastJSONCompactValue(value orderedJSONValue) string {
	switch value.Kind {
	case orderedJSONObject:
		if len(value.Pairs) == 0 {
			return "{}"
		}
		ordered := javaHashMapOrderedJSONPairs(value.Pairs)
		var builder strings.Builder
		builder.WriteByte('{')
		for index, pair := range ordered {
			if index > 0 {
				builder.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(pair.Key)
			builder.WriteString(string(encodedKey))
			builder.WriteByte(':')
			builder.WriteString(formatOrderedFastJSONCompactValue(pair.Value))
		}
		builder.WriteByte('}')
		return builder.String()
	case orderedJSONArray:
		if len(value.Items) == 0 {
			return "[]"
		}
		var builder strings.Builder
		builder.WriteByte('[')
		for index, item := range value.Items {
			if index > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(formatOrderedFastJSONCompactValue(item))
		}
		builder.WriteByte(']')
		return builder.String()
	case orderedJSONString:
		encoded, _ := json.Marshal(value.Text)
		return string(encoded)
	case orderedJSONNumber:
		return value.Text
	case orderedJSONBool:
		return strconv.FormatBool(value.Bool)
	default:
		return "null"
	}
}

func formatOrderedFastJSONObject(pairs []orderedJSONPair, indent int) string {
	if len(pairs) == 0 {
		return "{}"
	}
	ordered := javaHashMapOrderedJSONPairs(pairs)
	var builder strings.Builder
	builder.WriteString("{\n")
	for index, pair := range ordered {
		encodedKey, _ := json.Marshal(pair.Key)
		builder.WriteString(strings.Repeat("\t", indent+1))
		builder.WriteString(string(encodedKey))
		builder.WriteByte(':')
		builder.WriteString(formatOrderedFastJSONValue(pair.Value, indent+1))
		if index < len(ordered)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte('}')
	return builder.String()
}

func formatOrderedFastJSONArray(items []orderedJSONValue, indent int) string {
	if len(items) == 0 {
		return "[]"
	}
	var builder strings.Builder
	builder.WriteString("[\n")
	for index, item := range items {
		builder.WriteString(strings.Repeat("\t", indent+1))
		builder.WriteString(formatOrderedFastJSONValue(item, indent+1))
		if index < len(items)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte(']')
	return builder.String()
}

func javaHashMapOrderedJSONPairs(pairs []orderedJSONPair) []orderedJSONPair {
	ordered := append([]orderedJSONPair(nil), pairs...)
	capacity := javaHashMapCapacity(len(ordered))
	sort.SliceStable(ordered, func(left int, right int) bool {
		return javaHashMapBucketWithCapacity(ordered[left].Key, capacity) < javaHashMapBucketWithCapacity(ordered[right].Key, capacity)
	})
	return ordered
}

// javaConcurrentHashMapOrderedJSONPairs 复现 RocketMQ wrapper 内 ConcurrentHashMap.entrySet 的遍历顺序。
func javaConcurrentHashMapOrderedJSONPairs(pairs []orderedJSONPair) []orderedJSONPair {
	entries := make([]brokerConfigEntry, 0, len(pairs))
	latestByKey := make(map[string]orderedJSONValue, len(pairs))
	for _, pair := range pairs {
		entries = append(entries, brokerConfigEntry{Key: pair.Key, Value: ""})
		latestByKey[pair.Key] = pair.Value
	}
	orderedEntries := javaPropertiesEntrySetOrder(entries)
	ordered := make([]orderedJSONPair, 0, len(orderedEntries))
	for _, entry := range orderedEntries {
		ordered = append(ordered, orderedJSONPair{Key: entry.Key, Value: latestByKey[entry.Key]})
	}
	return ordered
}

type consumerStatusListEntry struct {
	// ClientID 是官方 consumerStatus 列表里的消费者客户端 ID。
	ClientID string
	// Version 是 MQVersion.getVersionDesc 后的客户端版本。
	Version string
	// FilePath 是写出的 ConsumerRunningInfo 文件相对路径，官方固定为 now/clientId。
	FilePath string
}

func formatConsumerStatusList(entries []consumerStatusListEntry, infos map[string]*consumerRunningInfo, now int64) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%-10s %-40s %-20s %s\n", "#Index", "#ClientId", "#Version", "#ConsumerRunningInfoFile"))
	for index, entry := range entries {
		filePath := strings.TrimSpace(entry.FilePath)
		if filePath == "" {
			filePath = strconv.FormatInt(now, 10) + "/" + entry.ClientID
		}
		builder.WriteString(fmt.Sprintf("%-10d %-40s %-20s %s\n", index+1, entry.ClientID, entry.Version, filePath))
	}
	if len(infos) == 0 {
		return builder.String()
	}
	if consumerRunningInfoSubscriptionsSame(infos) {
		builder.WriteString("\n\nSame subscription in the same group of consumer")
		builder.WriteString("\n\nRebalance OK\n")
		for _, clientID := range sortedConsumerRunningInfoClientIDs(infos) {
			builder.WriteString(consumerRunningInfoProcessQueueWarnings(clientID, infos[clientID], now))
		}
	} else {
		builder.WriteString("\n\nWARN: Different subscription in the same group of consumer!!!")
	}
	return builder.String()
}

func sortedConsumerRunningInfoClientIDs(infos map[string]*consumerRunningInfo) []string {
	clientIDs := make([]string, 0, len(infos))
	for clientID := range infos {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Strings(clientIDs)
	return clientIDs
}

func consumerRunningInfoSubscriptionsSame(infos map[string]*consumerRunningInfo) bool {
	clientIDs := sortedConsumerRunningInfoClientIDs(infos)
	if len(clientIDs) == 0 {
		return true
	}
	first := infos[clientIDs[0]]
	if first == nil {
		return true
	}
	if !consumerRunningInfoIsPush(first) {
		return true
	}
	startTimestamp := consumerRunningInfoProperty(first, "PROP_CONSUMER_START_TIMESTAMP")
	if startTimestamp != "" {
		value, err := strconv.ParseInt(startTimestamp, 10, 64)
		if err == nil && time.Now().UnixMilli()-value <= 1000*60*2 {
			return true
		}
	}
	expected := consumerRunningSubscriptionSignature(first.Subscriptions)
	for _, clientID := range clientIDs[1:] {
		current := infos[clientID]
		if current == nil {
			continue
		}
		if consumerRunningSubscriptionSignature(current.Subscriptions) != expected {
			return false
		}
	}
	return true
}

func consumerRunningSubscriptionSignature(subscriptions []consumerRunningSubscription) string {
	items := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		items = append(items, fmt.Sprintf("%s\x00%s\x00%t", subscription.Topic, subscription.SubString, subscription.ClassFilterMode))
	}
	sort.Strings(items)
	return strings.Join(items, "\x01")
}

func consumerRunningInfoProperty(info *consumerRunningInfo, key string) string {
	if info == nil {
		return ""
	}
	for _, property := range info.Properties {
		if property.Key == key {
			return property.Value
		}
	}
	return ""
}

func consumerRunningInfoProcessQueueWarnings(clientID string, info *consumerRunningInfo, now int64) string {
	if info == nil || !consumerRunningInfoIsPush(info) {
		return ""
	}
	orderMsg := strings.EqualFold(consumerRunningInfoProperty(info, "PROP_CONSUMEORDERLY"), "true")
	var builder strings.Builder
	mqTable := append([]consumerRunningProcessQueue(nil), info.MQTable...)
	sort.SliceStable(mqTable, func(left int, right int) bool {
		return compareMessageQueueIdentity(mqTable[left].Queue, mqTable[right].Queue) < 0
	})
	for _, entry := range mqTable {
		if orderMsg {
			if !entry.Info.Locked {
				builder.WriteString(fmt.Sprintf("%s %s can't lock for a while, %dms\n", clientID, formatMessageQueueIdentity(entry.Queue), now-entry.Info.LastLockTimestamp))
			} else if entry.Info.Droped && entry.Info.TryUnlockTimes > 0 {
				builder.WriteString(fmt.Sprintf("%s %s unlock %d times, still failed\n", clientID, formatMessageQueueIdentity(entry.Queue), entry.Info.TryUnlockTimes))
			}
			continue
		}
		diff := now - entry.Info.LastConsumeTimestamp
		if diff > 1000*60 && entry.Info.CachedMsgCount > 0 {
			builder.WriteString(fmt.Sprintf("%s %s can't consume for a while, maybe blocked, %dms\n", clientID, formatMessageQueueIdentity(entry.Queue), diff))
		}
	}
	return builder.String()
}

func formatMessageQueueIdentity(queue messageQueueIdentity) string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", queue.Topic, queue.BrokerName, queue.QueueID)
}

func consumerRunningInfoIsPush(info *consumerRunningInfo) bool {
	if info == nil {
		return false
	}
	for _, property := range info.Properties {
		if property.Key == "PROP_CONSUME_TYPE" {
			return property.Value == "CONSUME_PASSIVELY"
		}
	}
	return false
}

func decodeConsumerRunningInfoDetailBody(body []byte) (*consumerRunningInfo, error) {
	raw := string(body)
	info := &consumerRunningInfo{}
	if propertiesRaw, ok, err := jsonObjectFieldRaw(raw, "properties"); err != nil {
		return nil, err
	} else if ok {
		properties, err := decodeOrderedStringObject(propertiesRaw)
		if err != nil {
			return nil, fmt.Errorf("解析 consumerRunningInfo properties 失败: %w", err)
		}
		info.Properties = properties
	}
	if subscriptionsRaw, ok, err := jsonArrayFieldRaw(raw, "subscriptionSet"); err != nil {
		return nil, err
	} else if ok {
		var subscriptions []struct {
			Topic           string `json:"topic"`
			SubString       string `json:"subString"`
			ClassFilterMode bool   `json:"classFilterMode"`
		}
		if err := json.Unmarshal([]byte(subscriptionsRaw), &subscriptions); err != nil {
			return nil, fmt.Errorf("解析 consumerRunningInfo subscriptionSet 失败: %w", err)
		}
		info.Subscriptions = make([]consumerRunningSubscription, 0, len(subscriptions))
		for _, subscription := range subscriptions {
			info.Subscriptions = append(info.Subscriptions, consumerRunningSubscription{
				Topic:           subscription.Topic,
				SubString:       subscription.SubString,
				ClassFilterMode: subscription.ClassFilterMode,
			})
		}
	}
	mqTable, err := decodeConsumerRunningProcessQueueTable(raw, "mqTable")
	if err != nil {
		return nil, err
	}
	info.MQTable = mqTable
	mqPopTable, err := decodeConsumerRunningPopQueueTable(raw, "mqPopTable")
	if err != nil {
		return nil, err
	}
	info.MQPopTable = mqPopTable
	if statusRaw, ok, err := jsonObjectFieldRaw(raw, "statusTable"); err != nil {
		return nil, err
	} else if ok {
		var statusMap map[string]consumeStatusInfo
		if err := json.Unmarshal([]byte(statusRaw), &statusMap); err != nil {
			return nil, fmt.Errorf("解析 consumerRunningInfo statusTable 失败: %w", err)
		}
		info.StatusTable = make([]consumerRunningStatus, 0, len(statusMap))
		for topic, status := range statusMap {
			info.StatusTable = append(info.StatusTable, consumerRunningStatus{Topic: topic, Status: status})
		}
	}
	if userInfoRaw, ok, err := jsonObjectFieldRaw(raw, "userConsumerInfo"); err != nil {
		return nil, err
	} else if ok {
		userInfo, err := decodeOrderedStringObject(userInfoRaw)
		if err != nil {
			return nil, fmt.Errorf("解析 consumerRunningInfo userConsumerInfo 失败: %w", err)
		}
		info.UserConsumerInfo = userInfo
	}
	if jstack, ok, err := jsonStringField(raw, "jstack"); err != nil {
		return nil, err
	} else if ok {
		info.Jstack = jstack
	}
	return info, nil
}

func decodeConsumerRunningProcessQueueTable(raw string, field string) ([]consumerRunningProcessQueue, error) {
	tableRaw, ok, err := jsonObjectFieldRaw(raw, field)
	if err != nil || !ok {
		return nil, err
	}
	cursor := skipJSONSpaces(tableRaw, 0)
	if cursor >= len(tableRaw) || tableRaw[cursor] != '{' {
		return nil, fmt.Errorf("consumerRunningInfo %s 不是对象", field)
	}
	cursor++
	entries := make([]consumerRunningProcessQueue, 0)
	for {
		cursor = skipJSONDelimiters(tableRaw, cursor)
		if cursor >= len(tableRaw) {
			return nil, fmt.Errorf("consumerRunningInfo %s 未闭合", field)
		}
		if tableRaw[cursor] == '}' {
			return entries, nil
		}
		keyStart := cursor
		keyEnd, err := findJSONObjectEnd(tableRaw, keyStart)
		if err != nil {
			return nil, err
		}
		queue, err := decodeMessageQueueIdentity(tableRaw[keyStart : keyEnd+1])
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(tableRaw, keyEnd+1)
		if cursor >= len(tableRaw) || tableRaw[cursor] != ':' {
			return nil, fmt.Errorf("consumerRunningInfo %s key 后缺少冒号", field)
		}
		cursor = skipJSONSpaces(tableRaw, cursor+1)
		valueEnd, err := findJSONObjectEnd(tableRaw, cursor)
		if err != nil {
			return nil, err
		}
		var info processQueueInfo
		if err := json.Unmarshal([]byte(tableRaw[cursor:valueEnd+1]), &info); err != nil {
			return nil, fmt.Errorf("解析 ProcessQueueInfo 失败: %w", err)
		}
		entries = append(entries, consumerRunningProcessQueue{Queue: queue, Info: info})
		cursor = valueEnd + 1
	}
}

func decodeConsumerRunningPopQueueTable(raw string, field string) ([]consumerRunningPopQueue, error) {
	tableRaw, ok, err := jsonObjectFieldRaw(raw, field)
	if err != nil || !ok {
		return nil, err
	}
	cursor := skipJSONSpaces(tableRaw, 0)
	if cursor >= len(tableRaw) || tableRaw[cursor] != '{' {
		return nil, fmt.Errorf("consumerRunningInfo %s 不是对象", field)
	}
	cursor++
	entries := make([]consumerRunningPopQueue, 0)
	for {
		cursor = skipJSONDelimiters(tableRaw, cursor)
		if cursor >= len(tableRaw) {
			return nil, fmt.Errorf("consumerRunningInfo %s 未闭合", field)
		}
		if tableRaw[cursor] == '}' {
			return entries, nil
		}
		keyStart := cursor
		keyEnd, err := findJSONObjectEnd(tableRaw, keyStart)
		if err != nil {
			return nil, err
		}
		queue, err := decodeMessageQueueIdentity(tableRaw[keyStart : keyEnd+1])
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(tableRaw, keyEnd+1)
		if cursor >= len(tableRaw) || tableRaw[cursor] != ':' {
			return nil, fmt.Errorf("consumerRunningInfo %s key 后缺少冒号", field)
		}
		cursor = skipJSONSpaces(tableRaw, cursor+1)
		valueEnd, err := findJSONObjectEnd(tableRaw, cursor)
		if err != nil {
			return nil, err
		}
		var info popProcessQueueInfo
		if err := json.Unmarshal([]byte(tableRaw[cursor:valueEnd+1]), &info); err != nil {
			return nil, fmt.Errorf("解析 PopProcessQueueInfo 失败: %w", err)
		}
		entries = append(entries, consumerRunningPopQueue{Queue: queue, Info: info})
		cursor = valueEnd + 1
	}
}

func decodeOrderedStringObject(rawObject string) ([]orderedStringValue, error) {
	cursor := skipJSONSpaces(rawObject, 0)
	if cursor >= len(rawObject) || rawObject[cursor] != '{' {
		return nil, errors.New("JSON 对象起始位置非法")
	}
	cursor++
	values := make([]orderedStringValue, 0)
	for {
		cursor = skipJSONDelimiters(rawObject, cursor)
		if cursor >= len(rawObject) {
			return nil, errors.New("JSON 对象未闭合")
		}
		if rawObject[cursor] == '}' {
			return values, nil
		}
		key, next, err := decodeJSONStringAt(rawObject, cursor)
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(rawObject, next)
		if cursor >= len(rawObject) || rawObject[cursor] != ':' {
			return nil, errors.New("JSON 对象 key 后缺少冒号")
		}
		cursor = skipJSONSpaces(rawObject, cursor+1)
		value, next, err := decodeJSONDisplayValue(rawObject, cursor)
		if err != nil {
			return nil, err
		}
		values = append(values, orderedStringValue{Key: key, Value: value})
		cursor = next
	}
}

func decodeJSONDisplayValue(raw string, cursor int) (string, int, error) {
	cursor = skipJSONSpaces(raw, cursor)
	if cursor >= len(raw) {
		return "", 0, errors.New("JSON value 缺失")
	}
	switch raw[cursor] {
	case '"':
		return decodeJSONStringAt(raw, cursor)
	case '{':
		end, err := findJSONObjectEnd(raw, cursor)
		if err != nil {
			return "", 0, err
		}
		return raw[cursor : end+1], end + 1, nil
	case '[':
		end, err := findJSONArrayEnd(raw, cursor)
		if err != nil {
			return "", 0, err
		}
		return raw[cursor : end+1], end + 1, nil
	default:
		end := cursor
		for end < len(raw) && raw[end] != ',' && raw[end] != '}' && raw[end] != ']' {
			end++
		}
		value := strings.TrimSpace(raw[cursor:end])
		if value == "null" {
			return "", end, nil
		}
		return value, end, nil
	}
}

func jsonObjectFieldRaw(raw string, field string) (string, bool, error) {
	fieldIndex := strings.Index(raw, `"`+field+`"`)
	if fieldIndex < 0 {
		return "", false, nil
	}
	colonIndex := strings.Index(raw[fieldIndex:], ":")
	if colonIndex < 0 {
		return "", true, fmt.Errorf("JSON 字段 %s 缺少冒号", field)
	}
	cursor := skipJSONSpaces(raw, fieldIndex+colonIndex+1)
	if strings.HasPrefix(raw[cursor:], "null") {
		return "", true, nil
	}
	end, err := findJSONObjectEnd(raw, cursor)
	if err != nil {
		return "", true, err
	}
	return raw[cursor : end+1], true, nil
}

func jsonArrayFieldRaw(raw string, field string) (string, bool, error) {
	fieldIndex := strings.Index(raw, `"`+field+`"`)
	if fieldIndex < 0 {
		return "", false, nil
	}
	colonIndex := strings.Index(raw[fieldIndex:], ":")
	if colonIndex < 0 {
		return "", true, fmt.Errorf("JSON 字段 %s 缺少冒号", field)
	}
	cursor := skipJSONSpaces(raw, fieldIndex+colonIndex+1)
	if strings.HasPrefix(raw[cursor:], "null") {
		return "", true, nil
	}
	end, err := findJSONArrayEnd(raw, cursor)
	if err != nil {
		return "", true, err
	}
	return raw[cursor : end+1], true, nil
}

// decodeConsumerRunningInfoBody 解码 ConsumerRunningInfo.mqTable，仅提取 showClientIP 分配所需的队列键。
func decodeConsumerRunningInfoBody(body []byte) ([]messageQueueIdentity, error) {
	raw := string(body)
	mqTableIndex := strings.Index(raw, `"mqTable"`)
	if mqTableIndex < 0 {
		return nil, errors.New("consumerRunningInfo body 缺少 mqTable")
	}
	colonIndex := strings.Index(raw[mqTableIndex:], ":")
	if colonIndex < 0 {
		return nil, errors.New("consumerRunningInfo body mqTable 缺少冒号")
	}
	cursor := mqTableIndex + colonIndex + 1
	cursor = skipJSONSpaces(raw, cursor)
	if cursor >= len(raw) || raw[cursor] != '{' {
		return nil, errors.New("consumerRunningInfo body mqTable 不是对象")
	}
	cursor++
	queues := make([]messageQueueIdentity, 0)
	for {
		cursor = skipJSONDelimiters(raw, cursor)
		if cursor >= len(raw) {
			return nil, errors.New("consumerRunningInfo body mqTable 未闭合")
		}
		if raw[cursor] == '}' {
			break
		}
		keyStart := cursor
		keyEnd, err := findJSONObjectEnd(raw, keyStart)
		if err != nil {
			return nil, err
		}
		cursor = skipJSONSpaces(raw, keyEnd+1)
		if cursor >= len(raw) || raw[cursor] != ':' {
			return nil, errors.New("consumerRunningInfo body mqTable key 后缺少冒号")
		}
		cursor = skipJSONSpaces(raw, cursor+1)
		valueEnd, err := findJSONObjectEnd(raw, cursor)
		if err != nil {
			return nil, err
		}
		queue, err := decodeMessageQueueIdentity(raw[keyStart : keyEnd+1])
		if err != nil {
			return nil, err
		}
		queues = append(queues, queue)
		cursor = valueEnd + 1
	}
	return queues, nil
}

func decodeMessageQueueIdentity(keyJSON string) (messageQueueIdentity, error) {
	var key struct {
		Topic      string `json:"topic"`
		BrokerName string `json:"brokerName"`
		QueueID    int    `json:"queueId"`
	}
	if err := json.Unmarshal([]byte(keyJSON), &key); err != nil {
		return messageQueueIdentity{}, fmt.Errorf("解析 consumerRunningInfo 队列 key 失败: %w", err)
	}
	return messageQueueIdentity{Topic: key.Topic, BrokerName: key.BrokerName, QueueID: key.QueueID}, nil
}

func compareMessageQueueIdentity(left messageQueueIdentity, right messageQueueIdentity) int {
	if left.Topic != right.Topic {
		return strings.Compare(left.Topic, right.Topic)
	}
	if left.BrokerName != right.BrokerName {
		return strings.Compare(left.BrokerName, right.BrokerName)
	}
	return left.QueueID - right.QueueID
}

var consumerRunningPropertyOrder = []string{
	"consumeTimestamp",
	"pullThresholdForTopic",
	"awaitTerminationMillisWhenShutdown",
	"pullBatchSize",
	"enableStreamRequestType",
	"pullTimeDelayMillsWhenException",
	"consumeMessageBatchMaxSize",
	"pullThresholdForQueue",
	"language",
	"PROP_NAMESERVER_ADDR",
	"useHeartbeatV2",
	"maxReconsumeTimes",
	"accessChannel",
	"PROP_CONSUMEORDERLY",
	"clientIP",
	"mqClientApiTimeout",
	"decodeReadBody",
	"suspendCurrentQueueTimeMillis",
	"PROP_CONSUMER_START_TIMESTAMP",
	"PROP_CONSUME_TYPE",
	"adjustThreadPoolNumsThreshold",
	"sendLatencyEnable",
	"startDetectorEnable",
	"consumeConcurrentlyMaxSpan",
	"popBatchNums",
	"pullThresholdSizeForQueue",
	"decodeDecompressBody",
	"messageModel",
	"pullBatchSizeInBytes",
	"namesrvAddr",
	"allocateMessageQueueStrategy",
	"messageListener",
	"popThresholdForQueue",
	"enableTrace",
	"postSubscriptionWhenPull",
	"consumerGroup",
	"popInvisibleTime",
	"traceMsgBatchNum",
	"log",
	"instanceName",
	"consumeFromWhere",
	"persistConsumerOffsetInterval",
	"subscription",
	"consumeThreadMax",
	"unitMode",
	"pollNameServerInterval",
	"pullInterval",
	"detectInterval",
	"clientCallbackExecutorThreads",
	"consumeTimeout",
	"consumeThreadMin",
	"useTLS",
	"enableHeartbeatChannelEventListener",
	"pullThresholdSizeForTopic",
	"offsetStore",
	"PROP_CLIENT_VERSION",
	"clientRebalance",
	"PROP_THREADPOOL_CORE_SIZE",
	"vipChannelEnabled",
	"detectTimeout",
	"heartbeatBrokerInterval",
	"socksProxyConfig",
	"defaultMQPushConsumerImpl",
	"namespaceInitialized",
}

func orderedConsumerRunningProperties(values []orderedStringValue) []orderedStringValue {
	if len(values) == 0 {
		return nil
	}
	byKey := make(map[string]orderedStringValue, len(values))
	for _, value := range values {
		byKey[value.Key] = value
	}
	ordered := make([]orderedStringValue, 0, len(values))
	used := make(map[string]struct{}, len(values))
	for _, key := range consumerRunningPropertyOrder {
		value, ok := byKey[key]
		if !ok {
			continue
		}
		ordered = append(ordered, value)
		used[key] = struct{}{}
	}
	for _, value := range values {
		if _, ok := used[value.Key]; ok {
			continue
		}
		ordered = append(ordered, value)
	}
	return ordered
}

// rocketMQVersionDesc 按官方 MQVersion.getVersionDesc 的 ordinal 规则返回版本名。
func rocketMQVersionDesc(value int) string {
	if len(rocketMQVersionDescTable) == 0 {
		return ""
	}
	if value < 0 {
		return rocketMQVersionDescTable[0]
	}
	if value >= len(rocketMQVersionDescTable) {
		return rocketMQVersionDescTable[len(rocketMQVersionDescTable)-1]
	}
	return rocketMQVersionDescTable[value]
}

// buildRocketMQVersionDescTable 构造 RocketMQ 5.3.2 版本枚举表，避免维护 612 个硬编码条目。
func buildRocketMQVersionDescTable() []string {
	versions := []string{
		"V3_0_0_SNAPSHOT",
		"V3_0_0_ALPHA1",
		"V3_0_0_BETA1",
		"V3_0_0_BETA2",
		"V3_0_0_BETA3",
		"V3_0_0_BETA4",
		"V3_0_0_BETA5",
		"V3_0_0_BETA6_SNAPSHOT",
		"V3_0_0_BETA6",
		"V3_0_0_BETA7_SNAPSHOT",
		"V3_0_0_BETA7",
		"V3_0_0_BETA8_SNAPSHOT",
		"V3_0_0_BETA8",
		"V3_0_0_BETA9_SNAPSHOT",
		"V3_0_0_BETA9",
		"V3_0_0_FINAL",
	}
	appendVersionRange := func(major, minor, startPatch, endPatch int) {
		for patch := startPatch; patch <= endPatch; patch++ {
			versions = append(versions, fmt.Sprintf("V%d_%d_%d_SNAPSHOT", major, minor, patch))
			versions = append(versions, fmt.Sprintf("V%d_%d_%d", major, minor, patch))
		}
	}
	appendVersionRange(3, 0, 1, 9)
	appendVersionRange(3, 0, 10, 15)
	appendVersionRange(3, 1, 0, 9)
	appendVersionRange(3, 2, 0, 9)
	for minor := 3; minor <= 9; minor++ {
		appendVersionRange(3, minor, 1, 9)
	}
	for major := 4; major <= 5; major++ {
		for minor := 0; minor <= 9; minor++ {
			appendVersionRange(major, minor, 0, 9)
		}
	}
	return versions
}

// consumerConnectionConsumeTypeDesc 复现 GroupConsumeInfo.consumeTypeDesc 的 PULL/PUSH 展示逻辑。
func consumerConnectionConsumeTypeDesc(raw string) string {
	switch strings.TrimSpace(raw) {
	case "CONSUME_ACTIVELY":
		return "PULL"
	case "CONSUME_PASSIVELY":
		return "PUSH"
	default:
		return ""
	}
}

// consumerConnectionMessageModelDesc 复现官方仅在 PUSH 消费时展示 MessageModel 的逻辑。
func consumerConnectionMessageModelDesc(messageModel string, consumeType string) string {
	if strings.TrimSpace(consumeType) != "CONSUME_PASSIVELY" {
		return ""
	}
	return strings.TrimSpace(messageModel)
}

func jsonNumberField(raw string, field string) (float64, error) {
	fieldIndex := strings.Index(raw, `"`+field+`"`)
	if fieldIndex < 0 {
		return 0, nil
	}
	colonIndex := strings.Index(raw[fieldIndex:], ":")
	if colonIndex < 0 {
		return 0, fmt.Errorf("JSON 字段 %s 缺少冒号", field)
	}
	cursor := skipJSONSpaces(raw, fieldIndex+colonIndex+1)
	start := cursor
	for cursor < len(raw) {
		ch := raw[cursor]
		if (ch >= '0' && ch <= '9') || ch == '.' || ch == '-' || ch == '+' || ch == 'e' || ch == 'E' {
			cursor++
			continue
		}
		break
	}
	if start == cursor {
		return 0, fmt.Errorf("JSON 字段 %s 不是数字", field)
	}
	value, err := strconv.ParseFloat(raw[start:cursor], 64)
	if err != nil {
		return 0, fmt.Errorf("解析 JSON 字段 %s 失败: %w", field, err)
	}
	return value, nil
}

func jsonInt64Field(raw string, field string) (int64, error) {
	fieldIndex := strings.Index(raw, `"`+field+`"`)
	if fieldIndex < 0 {
		return 0, nil
	}
	colonIndex := strings.Index(raw[fieldIndex:], ":")
	if colonIndex < 0 {
		return 0, fmt.Errorf("JSON 字段 %s 缺少冒号", field)
	}
	cursor := skipJSONSpaces(raw, fieldIndex+colonIndex+1)
	start := cursor
	if cursor < len(raw) && raw[cursor] == '-' {
		cursor++
	}
	for cursor < len(raw) && raw[cursor] >= '0' && raw[cursor] <= '9' {
		cursor++
	}
	if start == cursor || (raw[start] == '-' && start+1 == cursor) {
		return 0, fmt.Errorf("JSON 字段 %s 不是整数", field)
	}
	value, err := strconv.ParseInt(raw[start:cursor], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析 JSON 字段 %s 失败: %w", field, err)
	}
	return value, nil
}

func jsonArrayField(raw string, field string) (string, error) {
	fieldIndex := strings.Index(raw, `"`+field+`"`)
	if fieldIndex < 0 {
		return "", fmt.Errorf("JSON 字段 %s 不存在", field)
	}
	colonIndex := strings.Index(raw[fieldIndex:], ":")
	if colonIndex < 0 {
		return "", fmt.Errorf("JSON 字段 %s 缺少冒号", field)
	}
	cursor := skipJSONSpaces(raw, fieldIndex+colonIndex+1)
	end, err := findJSONArrayEnd(raw, cursor)
	if err != nil {
		return "", err
	}
	return raw[cursor : end+1], nil
}

func jsonStringField(raw string, field string) (string, bool, error) {
	fieldIndex := strings.Index(raw, `"`+field+`"`)
	if fieldIndex < 0 {
		return "", false, nil
	}
	colonIndex := strings.Index(raw[fieldIndex:], ":")
	if colonIndex < 0 {
		return "", true, fmt.Errorf("JSON 字段 %s 缺少冒号", field)
	}
	cursor := skipJSONSpaces(raw, fieldIndex+colonIndex+1)
	if strings.HasPrefix(raw[cursor:], "null") {
		return "", true, nil
	}
	value, _, err := decodeJSONStringAt(raw, cursor)
	if err != nil {
		return "", true, err
	}
	return value, true, nil
}

func decodeTopicClusters(clusterBody []byte, routeBody []byte) ([]string, error) {
	brokers, err := decodeTopicRouteBrokers(routeBody)
	if err != nil {
		return nil, err
	}
	if len(brokers) == 0 {
		return nil, errors.New("topicRoute 未返回 Broker")
	}
	brokerName := brokers[0].BrokerName
	normalized := normalizeFastJSONNumericKeys(string(clusterBody))
	var payload struct {
		ClusterAddrTable map[string][]string `json:"clusterAddrTable"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, fmt.Errorf("解析 clusterInfo 失败: %w", err)
	}
	clusters := make([]string, 0, len(payload.ClusterAddrTable))
	for clusterName, brokerNames := range payload.ClusterAddrTable {
		for _, candidate := range brokerNames {
			if candidate == brokerName {
				clusters = append(clusters, clusterName)
				break
			}
		}
	}
	if len(clusters) == 0 {
		return nil, errors.New("topicClusterList 未解析到集群")
	}
	sort.Strings(clusters)
	return clusters, nil
}

type brokerClusterInfo struct {
	ClusterAddrTable map[string][]string
	BrokerAddrTable  map[string]brokerClusterData
}

type brokerClusterData struct {
	BrokerName  string
	BrokerAddrs map[string]string
	Cluster     string
}

func (broker brokerClusterData) selectAddr() string {
	if addr := strings.TrimSpace(broker.BrokerAddrs["0"]); addr != "" {
		return addr
	}
	for _, brokerID := range sortedBrokerIDKeys(broker.BrokerAddrs) {
		if addr := strings.TrimSpace(broker.BrokerAddrs[brokerID]); addr != "" {
			return addr
		}
	}
	return ""
}

func (info brokerClusterInfo) clusterNames(clusterName string) []string {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName != "" {
		return []string{clusterName}
	}
	names := make([]string, 0, len(info.ClusterAddrTable))
	for name := range info.ClusterAddrTable {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (info brokerClusterInfo) clusterNameForBroker(brokerName string) string {
	clusterNames := make([]string, 0, len(info.ClusterAddrTable))
	for clusterName := range info.ClusterAddrTable {
		clusterNames = append(clusterNames, clusterName)
	}
	sort.Strings(clusterNames)
	for _, clusterName := range clusterNames {
		for _, candidate := range info.ClusterAddrTable[clusterName] {
			if candidate == brokerName {
				return clusterName
			}
		}
	}
	return ""
}

func decodeBrokerClusterInfo(clusterBody []byte) (brokerClusterInfo, error) {
	normalized := normalizeFastJSONNumericKeys(string(clusterBody))
	var payload struct {
		ClusterAddrTable map[string][]string `json:"clusterAddrTable"`
		BrokerAddrTable  map[string]struct {
			BrokerName  string            `json:"brokerName"`
			BrokerAddrs map[string]string `json:"brokerAddrs"`
			Cluster     string            `json:"cluster"`
		} `json:"brokerAddrTable"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return brokerClusterInfo{}, fmt.Errorf("解析 clusterInfo 失败: %w", err)
	}
	info := brokerClusterInfo{
		ClusterAddrTable: payload.ClusterAddrTable,
		BrokerAddrTable:  make(map[string]brokerClusterData, len(payload.BrokerAddrTable)),
	}
	for brokerName, brokerData := range payload.BrokerAddrTable {
		info.BrokerAddrTable[brokerName] = brokerClusterData{
			BrokerName:  brokerData.BrokerName,
			BrokerAddrs: brokerData.BrokerAddrs,
			Cluster:     brokerData.Cluster,
		}
	}
	return info, nil
}

func decodeBrokerEpochCache(body []byte) (brokerEpochResult, error) {
	var payload struct {
		BrokerID    int64        `json:"brokerId"`
		BrokerName  string       `json:"brokerName"`
		ClusterName string       `json:"clusterName"`
		EpochList   []epochEntry `json:"epochList"`
		MaxOffset   int64        `json:"maxOffset"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return brokerEpochResult{}, fmt.Errorf("解析 broker epoch cache 失败: %w", err)
	}
	return brokerEpochResult{
		ClusterName: payload.ClusterName,
		BrokerName:  payload.BrokerName,
		BrokerID:    payload.BrokerID,
		MaxOffset:   payload.MaxOffset,
		EpochList:   payload.EpochList,
	}, nil
}

func decodeBrokerClusterMap(clusterBody []byte) (map[string]string, error) {
	normalized := normalizeFastJSONNumericKeys(string(clusterBody))
	var payload struct {
		ClusterAddrTable map[string][]string `json:"clusterAddrTable"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, fmt.Errorf("解析 clusterInfo 失败: %w", err)
	}
	brokerClusters := make(map[string]string)
	for clusterName, brokerNames := range payload.ClusterAddrTable {
		for _, brokerName := range brokerNames {
			brokerClusters[brokerName] = clusterName
		}
	}
	return brokerClusters, nil
}

func decodeBrokerRuntimeStatsBody(body []byte) (map[string]string, error) {
	normalized := normalizeFastJSONNumericKeys(string(body))
	var payload struct {
		Table map[string]string `json:"table"`
	}
	if err := json.Unmarshal([]byte(normalized), &payload); err != nil {
		return nil, fmt.Errorf("解析 broker runtime stats 失败: %w", err)
	}
	if payload.Table == nil {
		payload.Table = map[string]string{}
	}
	return payload.Table, nil
}

func decodeClusterAclConfigVersionResponse(fields map[string]string) ([]clusterAclConfigVersionRow, error) {
	rawVersions := strings.TrimSpace(fields["allAclFileVersion"])
	if rawVersions == "" {
		return nil, errors.New("Broker 未返回 allAclFileVersion")
	}
	versions := make(map[string]struct {
		Timestamp int64 `json:"timestamp"`
		Counter   int64 `json:"counter"`
	})
	if err := json.Unmarshal([]byte(rawVersions), &versions); err != nil {
		return nil, fmt.Errorf("解析 ACL 文件版本失败: %w", err)
	}
	paths := make([]string, 0, len(versions))
	for path := range versions {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	rows := make([]clusterAclConfigVersionRow, 0, len(paths))
	for _, path := range paths {
		version := versions[path]
		rows = append(rows, clusterAclConfigVersionRow{
			ClusterName:    strings.TrimSpace(fields["clusterName"]),
			BrokerName:     strings.TrimSpace(fields["brokerName"]),
			BrokerAddr:     strings.TrimSpace(fields["brokerAddr"]),
			AclFilePath:    path,
			VersionCounter: version.Counter,
			LastUpdateTime: time.UnixMilli(version.Timestamp),
		})
	}
	return rows, nil
}

func decodeListUserBody(body []byte) ([]listUserRow, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var payload []struct {
		Username   string `json:"username"`
		Password   string `json:"password"`
		UserType   string `json:"userType"`
		UserStatus string `json:"userStatus"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 listUser 响应失败: %w", err)
	}
	rows := make([]listUserRow, 0, len(payload))
	for _, user := range payload {
		rows = append(rows, listUserRow{
			Username:   user.Username,
			Password:   user.Password,
			UserType:   user.UserType,
			UserStatus: user.UserStatus,
		})
	}
	return rows, nil
}

func decodeGetUserBody(body []byte) (*listUserRow, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var payload struct {
		Username   string `json:"username"`
		Password   string `json:"password"`
		UserType   string `json:"userType"`
		UserStatus string `json:"userStatus"`
	}
	if err := json.Unmarshal(trimmed, &payload); err != nil {
		return nil, fmt.Errorf("解析 getUser 响应失败: %w", err)
	}
	return &listUserRow{
		Username:   payload.Username,
		Password:   payload.Password,
		UserType:   payload.UserType,
		UserStatus: payload.UserStatus,
	}, nil
}

func decodeListAclBody(body []byte) ([]aclInfo, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var rows []aclInfo
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("解析 listAcl 响应失败: %w", err)
	}
	return rows, nil
}

func decodeGetAclBody(body []byte) (*aclInfo, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var row aclInfo
	if err := json.Unmarshal(trimmed, &row); err != nil {
		return nil, fmt.Errorf("解析 getAcl 响应失败: %w", err)
	}
	if strings.TrimSpace(row.Subject) == "" && len(row.Policies) == 0 {
		return nil, nil
	}
	return &row, nil
}

func decodeBrokerConfigBody(body []byte) ([]brokerConfigEntry, error) {
	lines := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n")
	entries := make([]brokerConfigEntry, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		key, value, ok := splitBrokerConfigLine(line)
		if !ok {
			continue
		}
		entries = append(entries, brokerConfigEntry{Key: key, Value: value})
	}
	return javaPropertiesEntrySetOrder(entries), nil
}

var consumerConfigFieldOrder = []string{
	"groupName",
	"consumeEnable",
	"consumeFromMinEnable",
	"consumeBroadcastEnable",
	"consumeMessageOrderly",
	"retryQueueNums",
	"retryMaxTimes",
	"groupRetryPolicy",
	"brokerId",
	"whichBrokerWhenConsumeSlowly",
	"notifyConsumerIdsChangedEnable",
	"groupSysFlag",
	"consumeTimeoutMinute",
	"subscriptionDataSet",
	"attributes",
}

func decodeConsumerConfigBody(body []byte) ([]consumerConfigEntry, error) {
	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 consumer config 失败: %w", err)
	}
	entries := make([]consumerConfigEntry, 0, len(consumerConfigFieldOrder))
	for _, field := range consumerConfigFieldOrder {
		value := ""
		raw, ok := payload[field]
		if ok && !isJSONNull(raw) {
			parsedValue, err := consumerConfigValueString(field, raw)
			if err != nil {
				return nil, err
			}
			value = parsedValue
		}
		entries = append(entries, consumerConfigEntry{Name: field, Value: value})
	}
	return entries, nil
}

func consumerConfigValueString(field string, raw json.RawMessage) (string, error) {
	if field == "groupRetryPolicy" {
		return consumerGroupRetryPolicyString(raw)
	}
	value, err := decodeJSONValue(raw)
	if err != nil {
		return "", fmt.Errorf("解析 consumer config 字段 %s 失败: %w", field, err)
	}
	return javaObjectString(value), nil
}

func consumerGroupRetryPolicyString(raw json.RawMessage) (string, error) {
	value, err := decodeJSONValue(raw)
	if err != nil {
		return "", fmt.Errorf("解析 groupRetryPolicy 失败: %w", err)
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return javaObjectString(value), nil
	}
	policyType := "CUSTOMIZED"
	if rawType, ok := fields["type"]; ok && rawType != nil {
		policyType = javaObjectString(rawType)
	}
	exponential := "null"
	if rawExponential, ok := fields["exponentialRetryPolicy"]; ok && rawExponential != nil {
		exponential = javaNamedObjectString("ExponentialRetryPolicy", []string{"initial", "max", "multiplier"}, rawExponential)
	}
	customized := "null"
	if rawCustomized, ok := fields["customizedRetryPolicy"]; ok && rawCustomized != nil {
		customized = javaNamedObjectString("CustomizedRetryPolicy", []string{"next"}, rawCustomized)
	}
	return fmt.Sprintf("GroupRetryPolicy{type=%s, exponentialRetryPolicy=%s, customizedRetryPolicy=%s}", policyType, exponential, customized), nil
}

func javaNamedObjectString(name string, fieldOrder []string, value any) string {
	fields, ok := value.(map[string]any)
	if !ok {
		return javaObjectString(value)
	}
	parts := make([]string, 0, len(fieldOrder))
	for _, field := range fieldOrder {
		parts = append(parts, fmt.Sprintf("%s=%s", field, javaObjectString(fields[field])))
	}
	return fmt.Sprintf("%s{%s}", name, strings.Join(parts, ", "))
}

func decodeJSONValue(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.EqualFold(strings.TrimSpace(string(raw)), "null")
}

func javaObjectString(value any) string {
	switch current := value.(type) {
	case nil:
		return "null"
	case string:
		return current
	case bool:
		return strconv.FormatBool(current)
	case json.Number:
		return current.String()
	case []any:
		parts := make([]string, 0, len(current))
		for _, item := range current {
			parts = append(parts, javaObjectString(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		if len(current) == 0 {
			return "{}"
		}
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, fmt.Sprintf("%s=%s", key, javaObjectString(current[key])))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprint(current)
	}
}

func splitBrokerConfigLine(line string) (string, string, bool) {
	if index := strings.IndexAny(line, "=:"); index >= 0 {
		return strings.TrimSpace(line[:index]), strings.TrimSpace(line[index+1:]), true
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", "", false
	}
	return fields[0], strings.Join(fields[1:], " "), true
}

type javaPropertiesNode struct {
	hash  int
	entry brokerConfigEntry
	next  *javaPropertiesNode
}

func javaPropertiesEntrySetOrder(entries []brokerConfigEntry) []brokerConfigEntry {
	table := make([]*javaPropertiesNode, 16)
	threshold := 12
	count := 0
	for _, entry := range entries {
		hash := javaConcurrentHashMapSpread(javaStringHashCode(entry.Key))
		index := hash & (len(table) - 1)
		if table[index] == nil {
			table[index] = &javaPropertiesNode{hash: hash, entry: entry}
			count++
		} else {
			inserted := false
			for node := table[index]; node != nil; node = node.next {
				if node.entry.Key == entry.Key {
					node.entry.Value = entry.Value
					inserted = true
					break
				}
				if node.next == nil {
					node.next = &javaPropertiesNode{hash: hash, entry: entry}
					count++
					inserted = true
					break
				}
			}
			if !inserted {
				table[index] = &javaPropertiesNode{hash: hash, entry: entry, next: table[index]}
				count++
			}
		}
		if count >= threshold {
			table = resizeJavaPropertiesTable(table)
			threshold = len(table) - len(table)/4
		}
	}
	ordered := make([]brokerConfigEntry, 0, count)
	for _, node := range table {
		for current := node; current != nil; current = current.next {
			ordered = append(ordered, current.entry)
		}
	}
	return ordered
}

func resizeJavaPropertiesTable(table []*javaPropertiesNode) []*javaPropertiesNode {
	oldCapacity := len(table)
	newTable := make([]*javaPropertiesNode, oldCapacity*2)
	for index, first := range table {
		if first == nil {
			continue
		}
		if first.next == nil {
			newIndex := first.hash & (len(newTable) - 1)
			newTable[newIndex] = cloneJavaPropertiesChain(first)
			continue
		}
		runBit := first.hash & oldCapacity
		lastRun := first
		for node := first.next; node != nil; node = node.next {
			bit := node.hash & oldCapacity
			if bit != runBit {
				runBit = bit
				lastRun = node
			}
		}
		var lowHead *javaPropertiesNode
		var highHead *javaPropertiesNode
		if runBit == 0 {
			lowHead = cloneJavaPropertiesChain(lastRun)
		} else {
			highHead = cloneJavaPropertiesChain(lastRun)
		}
		for node := first; node != lastRun; node = node.next {
			cloned := &javaPropertiesNode{hash: node.hash, entry: node.entry}
			if node.hash&oldCapacity == 0 {
				cloned.next = lowHead
				lowHead = cloned
			} else {
				cloned.next = highHead
				highHead = cloned
			}
		}
		newTable[index] = lowHead
		newTable[index+oldCapacity] = highHead
	}
	return newTable
}

func cloneJavaPropertiesChain(node *javaPropertiesNode) *javaPropertiesNode {
	if node == nil {
		return nil
	}
	head := &javaPropertiesNode{hash: node.hash, entry: node.entry}
	tail := head
	for current := node.next; current != nil; current = current.next {
		tail.next = &javaPropertiesNode{hash: current.hash, entry: current.entry}
		tail = tail.next
	}
	return head
}

func javaConcurrentHashMapSpread(hash int32) int {
	unsigned := uint32(hash)
	return int((unsigned ^ (unsigned >> 16)) & 0x7fffffff)
}

func javaStringHashCode(value string) int32 {
	var hash int32
	for _, codeUnit := range utf16.Encode([]rune(value)) {
		hash = hash*31 + int32(codeUnit)
	}
	return hash
}

func buildClusterListRow(clusterName string, brokerName string, brokerID string, addr string, stats map[string]string, now time.Time) clusterListRow {
	row := clusterListRow{
		ClusterName:                   clusterName,
		BrokerName:                    brokerName,
		BrokerID:                      brokerID,
		Addr:                          addr,
		Version:                       stats["brokerVersionDesc"],
		InTPS:                         parseFirstFloat(stats["putTps"]),
		SendThreadPoolQueueSize:       stats["sendThreadPoolQueueSize"],
		SendThreadPoolQueueHeadWaitMS: stats["sendThreadPoolQueueHeadWaitTimeMills"],
		OutTPS:                        parseFirstFloat(stats["getTransferredTps"]),
		PullThreadPoolQueueSize:       stats["pullThreadPoolQueueSize"],
		PullThreadPoolQueueHeadWaitMS: stats["pullThreadPoolQueueHeadWaitTimeMills"],
		AckThreadPoolQueueSize:        statsStringDefault(stats, "ackThreadPoolQueueSize", "N"),
		AckThreadPoolQueueHeadWaitMS:  statsStringDefault(stats, "ackThreadPoolQueueHeadWaitTimeMills", "N"),
		TimerReadBehind:               parseInt64Default(stats["timerReadBehind"], 0),
		TimerOffsetBehind:             parseInt64Default(stats["timerOffsetBehind"], 0),
		TimerCongestNum:               parseInt64Default(stats["timerCongestNum"], 0),
		TimerEnqueueTPS:               parseFloatDefault(stats["timerEnqueueTps"], 0),
		TimerDequeueTPS:               parseFloatDefault(stats["timerDequeueTps"], 0),
		PageCacheLockTimeMS:           stats["pageCacheLockTimeMills"],
		CommitLogDiskRatio:            parseFloatDefault(stats["commitLogDiskRatio"], 0),
		BrokerActive:                  strings.EqualFold(stats["brokerActive"], "true"),
	}
	if timestamp := parseInt64Default(stats["earliestMessageTimeStamp"], 0); timestamp > 0 {
		row.Hour = now.Sub(time.UnixMilli(timestamp)).Hours()
	}
	return row
}

func buildClusterListMoreStatsRow(clusterName string, brokerName string, stats map[string]string) clusterListMoreStatsRow {
	msgPutTotalYesterdayMorning := parseInt64Default(stats["msgPutTotalYesterdayMorning"], 0)
	msgPutTotalTodayMorning := parseInt64Default(stats["msgPutTotalTodayMorning"], 0)
	msgPutTotalTodayNow := parseInt64Default(stats["msgPutTotalTodayNow"], 0)
	msgGetTotalYesterdayMorning := parseInt64Default(stats["msgGetTotalYesterdayMorning"], 0)
	msgGetTotalTodayMorning := parseInt64Default(stats["msgGetTotalTodayMorning"], 0)
	msgGetTotalTodayNow := parseInt64Default(stats["msgGetTotalTodayNow"], 0)
	return clusterListMoreStatsRow{
		ClusterName:   clusterName,
		BrokerName:    brokerName,
		InTotalYest:   msgPutTotalTodayMorning - msgPutTotalYesterdayMorning,
		OutTotalYest:  msgGetTotalTodayMorning - msgGetTotalYesterdayMorning,
		InTotalToday:  msgPutTotalTodayNow - msgPutTotalTodayMorning,
		OutTotalToday: msgGetTotalTodayNow - msgGetTotalTodayMorning,
	}
}

func statsStringDefault(stats map[string]string, key string, fallback string) string {
	if value, ok := stats[key]; ok {
		return value
	}
	return fallback
}

func parseFirstFloat(value string) float64 {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return 0
	}
	return parseFloatDefault(fields[0], 0)
}

func parseFloatDefault(value string, fallback float64) float64 {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64Default(value string, fallback int64) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func findJSONObjectEnd(raw string, start int) (int, error) {
	if start >= len(raw) || raw[start] != '{' {
		return 0, errors.New("JSON 对象起始位置非法")
	}
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(raw); index++ {
		ch := raw[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return index, nil
			}
		}
	}
	return 0, errors.New("JSON 对象未闭合")
}

func findJSONArrayEnd(raw string, start int) (int, error) {
	if start >= len(raw) || raw[start] != '[' {
		return 0, errors.New("JSON 数组起始位置非法")
	}
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(raw); index++ {
		ch := raw[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return index, nil
			}
		}
	}
	return 0, errors.New("JSON 数组未闭合")
}

func decodeJSONStringAt(raw string, start int) (string, int, error) {
	if start >= len(raw) || raw[start] != '"' {
		return "", 0, errors.New("JSON 字符串起始位置非法")
	}
	escaped := false
	for index := start + 1; index < len(raw); index++ {
		ch := raw[index]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '"' {
			value, err := strconv.Unquote(raw[start : index+1])
			if err != nil {
				return "", 0, fmt.Errorf("解析 JSON 字符串失败: %w", err)
			}
			return value, index + 1, nil
		}
	}
	return "", 0, errors.New("JSON 字符串未闭合")
}

func skipJSONSpaces(raw string, cursor int) int {
	for cursor < len(raw) {
		switch raw[cursor] {
		case ' ', '\t', '\r', '\n':
			cursor++
		default:
			return cursor
		}
	}
	return cursor
}

func skipJSONDelimiters(raw string, cursor int) int {
	for cursor < len(raw) {
		switch raw[cursor] {
		case ' ', '\t', '\r', '\n', ',':
			cursor++
		default:
			return cursor
		}
	}
	return cursor
}

func normalizeFastJSONNumericKeys(raw string) string {
	var builder strings.Builder
	builder.Grow(len(raw) + 8)
	inString := false
	escaped := false
	for index := 0; index < len(raw); index++ {
		ch := raw[index]
		if inString {
			builder.WriteByte(ch)
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			builder.WriteByte(ch)
			continue
		}
		if isObjectKeyStart(raw, index) {
			end := index
			for end < len(raw) && raw[end] >= '0' && raw[end] <= '9' {
				end++
			}
			cursor := end
			for cursor < len(raw) && (raw[cursor] == ' ' || raw[cursor] == '\t' || raw[cursor] == '\r' || raw[cursor] == '\n') {
				cursor++
			}
			if cursor < len(raw) && raw[cursor] == ':' {
				builder.WriteByte('"')
				builder.WriteString(raw[index:end])
				builder.WriteByte('"')
				index = end - 1
				continue
			}
		}
		builder.WriteByte(ch)
	}
	return builder.String()
}

func isObjectKeyStart(raw string, index int) bool {
	if index >= len(raw) || raw[index] < '0' || raw[index] > '9' {
		return false
	}
	for cursor := index - 1; cursor >= 0; cursor-- {
		switch raw[cursor] {
		case ' ', '\t', '\r', '\n':
			continue
		case '{', ',':
			return true
		default:
			return false
		}
	}
	return false
}

func formatFastJSONValue(value any, indent int) string {
	switch typed := value.(type) {
	case map[string]any:
		return formatFastJSONObject(typed, indent)
	case []any:
		return formatFastJSONArray(typed, indent)
	case string:
		encoded, _ := json.Marshal(typed)
		return string(encoded)
	case json.Number:
		return typed.String()
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case nil:
		return "null"
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		encoded, _ := json.Marshal(typed)
		return string(encoded)
	}
}

func formatFastJSONObject(value map[string]any, indent int) string {
	if len(value) == 0 {
		return "{}"
	}
	if isNumericScalarMap(value) {
		return formatFastJSONNumericMap(value, indent)
	}
	keys := sortedKeys(value)
	var builder strings.Builder
	builder.WriteString("{\n")
	for index, key := range keys {
		encodedKey, _ := json.Marshal(key)
		builder.WriteString(strings.Repeat("\t", indent+1))
		builder.WriteString(string(encodedKey))
		builder.WriteByte(':')
		builder.WriteString(formatFastJSONValue(value[key], indent+1))
		if index < len(keys)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte('}')
	return builder.String()
}

func formatFastJSONArray(value []any, indent int) string {
	if len(value) == 0 {
		return "[]"
	}
	var builder strings.Builder
	builder.WriteString("[\n")
	for index, item := range value {
		builder.WriteString(strings.Repeat("\t", indent+1))
		builder.WriteString(formatFastJSONValue(item, indent+1))
		if index < len(value)-1 {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte(']')
	return builder.String()
}

func formatFastJSONNumericMap(value map[string]any, indent int) string {
	keys := sortedKeys(value)
	var builder strings.Builder
	builder.WriteByte('{')
	for index, key := range keys {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key)
		builder.WriteByte(':')
		builder.WriteString(formatFastJSONValue(value[key], indent))
	}
	builder.WriteByte('\n')
	builder.WriteString(strings.Repeat("\t", indent))
	builder.WriteByte('}')
	return builder.String()
}

func sortedKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysAnyString(value map[string]string) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedBrokerNames(value map[string]brokerClusterData) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedBrokerIDKeys(value map[string]string) []string {
	keys := sortedKeysAnyString(value)
	sort.SliceStable(keys, func(i, j int) bool {
		left, leftErr := strconv.ParseInt(keys[i], 10, 64)
		right, rightErr := strconv.ParseInt(keys[j], 10, 64)
		if leftErr == nil && rightErr == nil {
			return left < right
		}
		return keys[i] < keys[j]
	})
	return keys
}

func isNumericScalarMap(value map[string]any) bool {
	for key, item := range value {
		if !isDigits(key) || !isFastJSONScalar(item) {
			return false
		}
	}
	return len(value) > 0
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func isFastJSONScalar(value any) bool {
	switch value.(type) {
	case string, json.Number, bool, nil, float64:
		return true
	default:
		return false
	}
}

func splitNameServers(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ';' || r == ','
	})
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func splitControllerAddresses(raw string) []string {
	parts := strings.Split(raw, ";")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func hasFlag(args []string, names ...string) bool {
	for _, arg := range args {
		for _, name := range names {
			if arg == name || strings.HasPrefix(arg, name+"=") {
				return true
			}
		}
	}
	return false
}

func stringArg(args []string, names ...string) string {
	for index, arg := range args {
		for _, name := range names {
			if arg == name && index+1 < len(args) {
				return strings.TrimSpace(args[index+1])
			}
			if strings.HasPrefix(arg, name+"=") {
				return strings.TrimSpace(strings.TrimPrefix(arg, name+"="))
			}
		}
	}
	return ""
}

func hasStringArg(args []string, names ...string) bool {
	for index, arg := range args {
		for _, name := range names {
			if arg == name && index+1 < len(args) {
				return true
			}
			if strings.HasPrefix(arg, name+"=") {
				return true
			}
		}
	}
	return false
}

func cloneGroupOffsetOfflineMissingValue(args []string) bool {
	knownOptions := map[string]struct{}{
		"-d":            {},
		"--destGroup":   {},
		"-h":            {},
		"--help":        {},
		"-n":            {},
		"--namesrvAddr": {},
		"-o":            {},
		"--offline":     {},
		"-s":            {},
		"--srcGroup":    {},
		"-t":            {},
		"--topic":       {},
	}
	for index, arg := range args {
		if arg != "-o" && arg != "--offline" {
			continue
		}
		if index+1 >= len(args) {
			return true
		}
		// Commons CLI 遇到下一个已声明 option 时判定 -o/--offline 缺少参数；未知 -x 会被当作参数值继续执行。
		if _, ok := knownOptions[args[index+1]]; ok {
			return true
		}
	}
	return false
}

func parsePrintMsgByQueueTimestamp(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, errors.New("时间戳不能为空")
	}
	if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
		return timestamp, nil
	}
	parsed, err := time.ParseInLocation("2006-01-02#15:04:05:000", value, time.Local)
	if err != nil {
		return 0, fmt.Errorf("解析 printMsgByQueue 时间戳 %q 失败: %w", raw, err)
	}
	return parsed.UnixMilli(), nil
}

func splitMessageIDs(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func splitCommaList(raw string) []string {
	return splitMessageIDs(raw)
}

func intArg(args []string, fallback int, names ...string) (int, error) {
	raw := stringArg(args, names...)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("解析整数参数 %q 失败: %w", raw, err)
	}
	return value, nil
}

func int64Arg(args []string, fallback int64, names ...string) (int64, error) {
	raw := stringArg(args, names...)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析长整数参数 %q 失败: %w", raw, err)
	}
	return value, nil
}

func boolArg(args []string, fallback bool, names ...string) bool {
	raw := stringArg(args, names...)
	if raw == "" {
		return fallback
	}
	return strings.EqualFold(strings.TrimSpace(raw), "true")
}
