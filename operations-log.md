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
