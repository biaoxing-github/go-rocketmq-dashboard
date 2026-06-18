# RocketMQ Go Dashboard 使用说明

## 使用流程

### 1. 切换集群

1. 打开 Dashboard 地址，例如 `http://127.0.0.1:18090`。
2. 左侧查看当前 NameServer。
3. 点击“添加 / 切换集群”。
4. 输入目标 NameServer，例如 `127.0.0.1:9876`。
5. 点击“切换”。

切换后，后端会重建 RocketMQ Provider 和所有快照缓存，页面会重新读取集群、Topic、Consumer 和消息链路数据。

### 2. 总览页

总览页用于快速判断当前连接是否正常。

- Broker 版本：来自 `/api/clusters` 的 broker 自报版本。
- Topic 数：来自 `/api/topics`。
- Consumer 数和最大堆积：来自 `/api/consumers`。
- 快照状态：显示缓存命中、后台刷新和上次失败信息。

### 3. 集群页

集群页展示 Broker、地址、版本和运行状态。

常用流程：

1. 进入“集群”。
2. 找到目标 Cluster。
3. 点击“选择”。
4. 页面跳到 Topic 页，后续 Topic 写入目标会优先使用这个 Cluster。

### 4. Topic 页

Topic 页是排查消息的主入口。

左侧列表：

- 搜索框支持按 Topic 名称和类型过滤。
- 点击“选择”后，右侧会加载当前 Topic 的配置、消息、路由和水位。
- 长 Topic 名会自动换行，不需要横向拖动。

右侧子功能：

- 配置：创建 / 更新 / 删除 Topic，目标可以是 Cluster 或 Broker。
- 发送：向当前 Topic 发送一条消息，成功后回显 messageId，并可直接跳转链路。
- 消息：按 Topic 浏览 Broker 保留窗口内最近可回查消息。
- 路由：查看 Topic 分布在哪些 Broker、读写队列数和权限。
- 水位：查看每个队列的最小位点、最大位点、消息数和最后写入时间。

推荐排查流程：

1. 搜索 Topic。
2. 点击目标 Topic 的“选择”。
3. 进入“消息”子 tab。
4. 设置 Limit，默认 12，最大 24。
5. 等后台刷新完成。
6. 点击某条消息的“链路”。
7. 消息链路页会自动填入 topic 和 messageId。

### 5. Consumer 页

Consumer 页用于查看消费组状态和执行消费点重置。

左侧列表：

- 展示消费者组、在线数、版本、消费模式和堆积。
- 点击“详情”进入右侧子 tab。

右侧子功能：

- 概览：展示 group、订阅 Topic、堆积和 TPS。
- 重置：执行 `resetOffsetByTime`，支持按 group/topic 或单队列重置。
- 连接：展示 Consumer 客户端连接。
- 订阅：展示订阅 Topic 和过滤表达式。
- 进度：展示 Broker 位点、消费位点和堆积差。

重置消费点会影响真实消费进度，使用前需要确认 group、topic、timestamp 和 force。

### 6. 消息链路页

消息链路页用于查看单条消息的状态。

推荐入口是从 Topic 消息列表点击“链路”，这样系统会自动填入 messageId。

高级查询可手动填：

- Topic：必填。
- Message ID：优先使用，最准确。
- Key：没有 messageId 时可用 key 查询候选消息。
- Consumer Group：可选，用于叠加消费进度。
- Trace Topic：可选，默认交给 RocketMQ tools。
- Max Num：限制 key 查询候选数量。

链路数据由三部分组合：

- Broker 存储详情：`queryMsgById` 或 `queryMsgByOffset`。
- Trace 事件：`queryMsgTraceById`。
- Consumer 消费进度：`consumerProgress`。

Trace 是否完整取决于 RocketMQ trace 是否开启，以及 trace 数据是否仍在保留窗口内。

#### 为什么有时看不到具体消费链路

RocketMQ 的 Trace 不是 Broker 主存储里天然存在的消费流水，而是 Producer / Consumer 客户端在开启 trace 后额外写入 trace topic 的链路消息。Dashboard 调用 `queryMsgTraceById` 只能读取这份额外 trace 数据，所以以下情况都会导致具体消费链路缺失：

- 生产端或消费端没有开启 trace。
- trace topic 没有创建、没有被保留，或保留时间短于业务消息保留时间。
- 历史消息还在 Broker 里，但对应 trace 消息已经过期。
- 消费者没有成功上报消费 trace，或客户端版本/配置不支持消费 trace。
- 只填写 messageId 不填写 Consumer Group 时，页面只能展示消息存储和 trace；无法叠加某个消费组的位点判断。

Consumer 进度来自 `consumerProgress`，它展示的是消费组在每个队列上的当前 offset、Broker 最大 offset 和堆积差。它可以辅助判断“这个 group 的位点是否已经越过某条消息的 queueOffset”，但不能精确还原“哪台客户端、哪次调用、哪个时间点消费了这条消息”。如果需要精确到业务处理链路，需要业务消费端记录 messageId、topic、queueId、queueOffset、消费时间和处理结果，或确保 RocketMQ trace 对生产端和消费端都开启并且保留足够久。

## 实现原理

### 前端

- `index.html` 定义页面结构。
- `app.js` 保存页面状态、调用 API、渲染表格和时间线。
- `styles.css` 控制运维台布局、表格密度、子 tab 和移动端适配。

### 后端

- 所有 API 在 `internal/server/server.go` 注册。
- 统一响应包含 `cacheHit`、`hasData`、`stale`、`refreshing`、`lastError` 和 `latencyMillis`。
- 冷查询不阻塞首包，接口会先返回缓存或占位结果，再后台刷新。
- NameServer 切换后会重建 Provider 和所有快照仓库，避免跨集群数据混用。

### RocketMQ 数据来源

当前 Provider 复用 RocketMQ 官方 tools：

- `clusterList`：集群和 Broker 版本。
- `topicList`：Topic 列表。
- `topicRoute`：Topic 路由。
- `topicStatus`：Topic 队列位点和水位。
- `queryMsgByOffset`：按队列位点回查历史消息。
- `queryMsgById`：按 messageId 回查单条消息。
- `queryMsgTraceById`：查 Trace。
- `consumerProgress`：消费组进度。
- `consumerConnection`：消费组连接和订阅。
- `sendMessage`：发送消息。
- `resetOffsetByTime`：重置消费点。

### RocketMQ 核心原理

RocketMQ 的核心对象可以按这个顺序理解：

1. NameServer 只保存路由元数据，告诉客户端某个 Topic 在哪些 Broker 上有队列；它不保存消息正文。
2. Topic 是业务消息分类，一个 Topic 会被拆成多个 MessageQueue，分布在一个或多个 Broker 上。
3. Producer 发送消息时，最终会落到某个 Broker 的某个队列。
4. Broker 真实存储消息。消息主体写入 CommitLog，队列维度通过 ConsumeQueue 记录 `topic + queueId + queueOffset` 到 CommitLog 位置的索引。
5. Consumer Group 独立维护消费进度，同一条消息是否被某个 group 消费，取决于这个 group 在对应队列上的消费位点。
6. Trace 是单独的链路数据，只有开启 trace 且 trace 消息仍在保留窗口内，Dashboard 才能补齐发送、投递、消费事件。

所以 Dashboard 不能只问 NameServer “给我这个 Topic 最近 12 条消息”。NameServer 没有正文，Topic 又被拆在多个队列里，必须先查队列水位，再按 `brokerName + queueId + queueOffset` 去 Broker 回查具体消息。

## 历史消息为什么慢

历史消息浏览不是 RocketMQ 原生的全局分页列表接口。当前流程是：

1. 先调用 `topicStatus` 获取每个队列的最小位点和最大位点。
2. 从每个队列最大位点往回推。
3. 对每个 offset 调一次 `queryMsgByOffset`。
4. 每次调用都会启动一次 RocketMQ tools Java 命令。
5. 后端聚合结果后按存储时间排序。

所以慢点主要来自：

- 每条消息一次 mqadmin 命令。
- 跨多个队列时会扫描多个 queue。
- Java 进程启动成本高。
- NameServer/Broker 网络超时会放大延迟。
- 部分位点可能不可回查，仍然消耗一次尝试。

## 可优化点

当前已经有的优化：

- 首包走缓存/占位，不同步等待冷查询。
- 后台刷新完成后短轮询更新。
- `limit` 限制最多返回 24 条。
- 失败后不会无限自动重试，需要用户点击刷新。
- 同一个 Topic/Limit 刷新时会复用上一轮已经查过的 `topic + brokerName + queueId + queueOffset`，只对缺失 offset 继续调用 `queryMsgByOffset`。
- Topic 消息摘要展示“新拉 / 复用”，分别对应本轮真实 mqadmin 回源位点数和缓存复用位点数。

为什么这样优化：

- RocketMQ 队列 offset 是追加式增长，已经存在的旧 offset 通常不会改变。
- 用户在 Dashboard 里反复刷新同一个 Topic 时，大部分旧消息其实已经查过。
- 复用旧 offset 可以直接减少 Java mqadmin 进程启动次数，也减少 Broker 网络请求次数。
- 新消息通常只出现在更高的 queue offset，因此刷新时把缺失 offset 补上即可。

建议继续做的优化：

1. 在消息浏览 UI 增加 Broker 和 Queue 过滤，只查某个队列，减少扫描面。
2. 后端对 `queryMsgByOffset` 做有界并发，例如每个 Topic 最多 3-4 个并发 mqadmin。
3. 增加按业务 key 查询入口，能拿到 key 时避免扫描历史位点。
4. 做一个 JVM sidecar 或直接使用 RocketMQ Java client，避免每条消息启动一次 Java 进程。
5. 如果业务方可以写入 messageId/key 索引，Dashboard 优先走索引查询，而不是 offset 扫描。

## 历史消息乱码说明

本轮已修复两类 Dashboard 侧乱码来源：

1. 启动 RocketMQ tools Java 进程时强制 `file.encoding/stdout/stderr=UTF-8`，避免 Windows 默认编码把中文输出成非 UTF-8 字节。
2. 消息体预览截断改为按 Unicode rune 截断，避免中文字符被截半后出现 `�`。

如果消息生产端已经把 `qaName` 写成了 `��������`，Dashboard 无法从 Broker 恢复原文；如果 Broker 中保存的是正确 UTF-8 中文，本轮修复后应能正常显示。
