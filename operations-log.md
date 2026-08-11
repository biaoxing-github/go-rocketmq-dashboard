# 操作日志

## 2026-07-26T13:42:58+08:00 缺少访问令牌密码弹框（执行者：Devil）

- 前馈读取：项目 `AGENTS.md`、Obsidian 项目索引、历史 process/feature 记录和相关 rollout 摘要。
- 代码分析：CodeGraph 定位 `fetchJSON` 为全部受审计写操作的统一令牌入口；PowerShell 定向读取现有 Dialog、写操作处理函数和静态契约测试。
- 规划说明：当前环境未提供 sequential-thinking 与 shrimp-task-manager 工具，改用内置计划工具维护红灯测试、实现、记录和验证状态。
- TDD：先新增 `TestPublicMutationPromptsForMissingCredential` 并确认因缺少 `authTokenDialog` 失败，再实现 HTML、JavaScript 和 CSS 后确认转绿。
- 自动验证：运行全量 Go 测试、两个 Go 命令构建、JavaScript 语法、Compose 配置、diff 检查和 Browser 桌面/移动交互验证。
- 隔离边界：本地实例使用不可达的 `127.0.0.1:19876` NameServer 和未配置写凭据服务；未访问或修改 172.168.1.93 RocketMQ 数据。

## 2026-07-28T12:50:27+08:00 密码弹框生产发布（执行者：Devil）

- Git：提交并推送 `a25a5e5` 到 `origin/codex/goadmin-rocksdb-local`。
- 镜像：从 `git archive HEAD` 构建并推送 `20260728-1241-auth-prompt-a25a5e56`，digest 为 `sha256:20ef8840ea872de4bb05666dfa5c768d667cc9279896b9e6a573fffd6130ce79`。
- 首次部署：远端候选 Compose 校验和镜像拉取成功，但脚本使用本地服务名导致 `no such service`；重建前即失败，生产 Compose 已恢复且旧容器始终 healthy。
- 校正部署：读取远端 `docker-compose config --services` 得到真实服务名 `rocketmq-go-dashboard`，重新执行候选校验、备份、替换和健康等待；备份为 `docker-compose.yml.bak.20260728124429-auth-prompt`。
- 生产验证：新容器 running/healthy、restart=0、北京时间正确；凭据只读挂载和运行卷保持不变，18080/18085/18090 均可达，健康接口返回 code=0 和 3 个固定集群。
- 浏览器验证：桌面与 390x844 移动端均显示密码弹框和 WIFI 提示；取消不发送，错误令牌确认后返回“身份凭据无效”，控制台 0 error/0 warn，测试 Topic 未创建。

## 2026-07-29T10:21:33+08:00 页面添加集群与基础镜像推送（执行者：Devil）

- 前馈读取：项目 AGENTS、Obsidian 项目索引、历史 process/feature 记录和 RocketMQ Dashboard rollout 摘要。
- 代码分析：CodeGraph 定位固定 clusterId 请求隔离、ProviderFactory、clusterMu、配置 API 和前端集群切换链路。
- 功能实现：新增服务端集群注册表、受令牌和操作理由保护的注册接口、独立运行时创建、持久卷恢复，以及页面添加集群弹框和自动切换。
- 配置文档：新增 RMQD_CLUSTER_REGISTRY_PATH，Compose 与镜像默认保存到 /app/runtime/clusters.json，并修正多集群使用说明。
- 用户边界：按明确要求不运行测试、构建、语法检查、Compose 检查或浏览器验证；完成后直接提交推送，验证由用户执行。
- 发布构建排障：默认 proxy.golang.org 下载多个模块时返回 unexpected EOF；使用同一 Harbor Go 基础镜像确认 goproxy.cn 可下载后，为 Dockerfile 和 Compose 增加可覆盖 GOPROXY 构建参数。

## 2026-07-29T11:06:03+08:00 快照错误原因展示修复（执行者：Devil）

- 前馈读取：项目 AGENTS、Obsidian 项目索引、历史 process/feature 记录和 RocketMQ Dashboard 发布摘要。
- 根因取证：`172.168.1.160:9876` NameServer 可达，但 `clusterList` 返回 Broker 连接异常；Broker 地址为 `rmqbroker-a-master:10911` 与 `rmqbroker-b-slave:10921`，Dashboard 原先把异常行误报为“行字段不足”，总览只显示“有错误”。
- 范围边界：按用户要求只修改 Dashboard，不改 160 的 NameServer、Broker 或容器配置。
- TDD：解析器和页面展示契约测试在实现前均按预期失败；实现后 focused tests 转绿。
- 实现：解析器聚合全部 `RemotingConnectException`，总览按错误文本聚合受影响快照并展示完整 `lastError`。
- 本地验证：全量 Go 测试、两个 Go 命令构建、JavaScript 语法、Compose 配置和 diff 检查全部通过。

## 2026-07-29T11:22:24+08:00 集群修改与删除（执行者：Devil）

- 前馈读取：项目 AGENTS、Obsidian 项目索引、历史 process/feature 记录和 RocketMQ Dashboard 发布摘要。
- 代码分析：CodeGraph 仍返回 `Transport closed`，按既有故障记录降级为 PowerShell 定向读取集群注册表、运行时、审计和前端集群切换路径。
- 后端实现：新增 `PUT /api/config/clusters/{id}` 和 `DELETE /api/config/clusters/{id}`，修改时替换独立 Provider 与快照运行时，删除时同步移除运行时和持久化定义。
- 边界控制：启动配置集群保持只读；重复 ID、不存在目标、未知字段和多余 JSON 均返回稳定错误，所有写操作继续要求令牌、操作理由和审计记录。
- 前端实现：集群管理弹框展示全部集群，对页面注册集群提供编辑/删除图标；修改或删除当前集群后同步刷新选择器、会话选择和当前快照。
- 自动验证：聚焦 API/持久化/前端契约测试、全量 Go 测试、两个 Go 命令构建、JavaScript 语法、Compose 配置和 diff 检查全部通过。
- 浏览器冒烟：本地 18190 页面可打开集群管理弹框，启动配置集群标记正确且编辑/删除按钮禁用；共享 Browser MCP 被占用，使用独立 Playwright CLI 会话完成验证并清理临时进程与产物。

## 2026-07-29T11:15:00+08:00 快照错误原因展示生产发布（执行者：Devil）

- Git：提交 `09b19c9` 已推送到 `origin/codex/goadmin-rocksdb-local`。
- 镜像：从 `git archive HEAD` 构建并推送 `20260729-1108-snapshot-errors-09b19c95`，digest 为 `sha256:97b0077cc00ae2e259914fa3d5e1a25edba49c64503d240bf7411faa9f2a3d43`。
- 93 部署：使用服务器实际的 `docker-compose` 工具，备份为 `docker-compose.yml.bak.20260729111225.snapshot-errors`；仅替换 Dashboard 镜像引用，容器 running/healthy、restart=0、北京时间正确。
- 线上 API：选择 `test-160` 后，`/api/clusters` 和 `/api/features` 的 `lastError` 均显示两个 Broker 连接失败；`/api/topics` 返回 15 条、`/api/consumers` 返回 1 条。
- 浏览器：生产桌面与 390x844 移动端均显示“快照错误原因 / 2 项失败”及完整 Broker 地址；移动端 `scrollWidth=clientWidth=375`，控制台 0 error/0 warning。
- 边界：未修改 `172.168.1.160` 的 NameServer、Broker 或任何 RocketMQ 数据；页面现已把底层配置问题明确展示出来。

## 2026-07-29T11:33:27+08:00 test-160 Broker 主机名连接修复（执行者：Devil）

- 根因复核：93 Dashboard 容器无法解析 `rmqbroker-a-master` 和 `rmqbroker-b-slave`，但直连 `172.168.1.160:10911`、`:10921` 均开放。
- 范围边界：仅修改 93 Dashboard 容器 Compose 的 `extra_hosts`，未修改 160 的 NameServer、Broker 配置或 RocketMQ 数据，也未更换 Dashboard 镜像。
- 配置变更：将两个 Broker 主机名都映射到 `172.168.1.160`；候选 Compose 在本地及远端均通过配置校验。
- 部署：远端备份为 `docker-compose.yml.bak.20260729112932.broker-hosts`，只重建 `rocketmq-go-dashboard` 服务；容器 running/healthy、restart=0、北京时间正确。
- 网络验证：容器内 `getent hosts` 均返回 `172.168.1.160`，10911 与 10921 TCP 连接成功。
- 功能验证：`test-160` 集群/能力快照 code=0、stale=false、refreshing=false、lastError 为空；返回主 Broker 与从 Broker，Topic 15 个、Consumer 1 个。
- 浏览器验证：生产页面选择 `test-160` 后状态“已就绪”，Broker 版本 `V5_2_0`，错误面板隐藏，控制台 0 error/0 warning。

## 2026-07-29T11:45:00+08:00 集群管理功能部署到 93（执行者：Devil）

- Git：提交 `254987b` 已推送到 `origin/codex/goadmin-rocksdb-local`。
- 镜像：已构建并推送 `is01.jx907.cn/platform/rocketmq-go-dashboard:20260729-1127-cluster-manage-254987b9`，digest 为 `sha256:4ba6bbf1ac7540f7ae03f3140a76a7ad6b2a47acbf39cfd1bf7647fa0115aafd`。
- 部署：93 Compose 备份为 `docker-compose.yml.bak.20260729113714-cluster-manage`；容器 running/healthy、restart=0、北京时间正确，端口映射和只读凭据、运行卷保持不变。
- API：`/api/health` 返回 code=0、configuredClusterCount=4；`/api/config` 返回 4 个集群，`clusterManagementEnabled=true`，页面注册集群为 `test-160`。
- 页面：启动配置集群的编辑/删除按钮禁用，`test-160` 的编辑/删除可用；编辑弹框预填 `172.168.1.160:9876`，取消后未产生写请求。
- 移动端：390x844 下文档与弹框无横向溢出；生产 API 请求全部 200，控制台 0 error/0 warning。

## 2026-07-29T12:06:04+08:00 main 分支与 v1.0.0 Release 发布（执行者：Devil）

- Git：远端 `main` 已快进到 `254987b954c63c0275c0c165bd993b22415928ad`，本地 `main` 与 `origin/main` 同步。
- 分支：本地及远端 `codex/goadmin-cli-parallel`、`codex/goadmin-rocksdb-local` 已删除，未发现残留 `codex/*` 分支。
- Tag：annotated tag `v1.0.0` 已推送，解引用后指向提交 `254987b954c63c0275c0c165bd993b22415928ad`。
- Release：GitHub Release `v1.0.0` 已发布并标记为 `Latest`，页面为 `https://github.com/biaoxing-github/go-rocketmq-dashboard/releases/tag/v1.0.0`。
- 边界：未改业务代码；保留工作区原有未提交 `operations-log.md` 修改，不清理来源不明的临时目录。

## 2026-08-11T15:01:20+08:00 Topic 创建串集群排查与本地修复（执行者：Devil）

- CodeGraph：追踪 `POST /api/topics -> handleTopicUpsert -> MQAdminProvider.UpsertTopic -> goadmin/mqadmin`，确认创建命令是 `updateTopic`，报错类来自创建后的 `topicStatus` 查询。
- 生产审计/API：读取五个集群的 Topic 写审计；`test-49/test` 创建记录成功，但 49 route/status 失败，160 route 成功。
- 93 只读检查：容器持续健康；`getent hosts` 显示 `rmqbroker-a-master`、`rmqbroker-b-slave` 均被 Compose 全局映射到 `172.168.1.160`。
- 实现：增加集群级 Broker 地址映射持久化、校验、页面编辑、完整 Provider 工厂传参和独立 mqadmin JVM hosts 注入；映射集群禁用共享 sidecar/native 路径。
- 验证：TDD 红灯、focused tests、全量 Go 测试、双命令构建、JS 语法、Compose、diff 检查及桌面/移动浏览器检查通过。
- 边界：未提交、未推送、未部署，未删除 160 上已有的 `test` Topic，也未执行新的 RocketMQ 写操作。
