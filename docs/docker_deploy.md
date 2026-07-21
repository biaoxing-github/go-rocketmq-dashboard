# Docker 部署说明

## 镜像内容

`rocketmq-go-dashboard` 运行时需要两部分能力：

- Go 编译出的 Dashboard 服务，入口为 `cmd/rmqdash`。
- Go 编译出的 `goadmin` CLI，入口为 `cmd/goadmin`，用于在容器内直接执行与官方 `mqadmin` 兼容的管理命令。
- Java 和 RocketMQ tools classpath，用于执行 `org.apache.rocketmq.tools.command.MQAdminStartup` 查询集群、Topic、Consumer、消息和链路。
- Java Admin sidecar，入口为 `dev.codex.rocketmq.AdminSidecar`，在常驻 JVM 内复用 RocketMQ 官方 tools 命令，避免消息链路每次查询都重新拉起 Java 进程。

因此 `Dockerfile` 采用多阶段构建：

- `golang:1.25-bookworm` 编译 `/usr/local/bin/rmqdash` 和 `/usr/local/bin/goadmin`。
- `eclipse-temurin:17-jdk-jammy` 作为运行时和 sidecar 编译环境，并从国内 Apache 镜像下载 RocketMQ `rocketmq-all-5.3.2-bin-release.zip`。
- 默认设置 `RMQD_MQADMIN_CLASSPATH=/opt/rocketmq/lib/*`，容器内不依赖宿主机 Maven 仓库或 `.tmp/rocketmq-runtime-classpath.txt`。
- 默认启用 `RMQD_ADMIN_SIDECAR_ENABLED=true`；如果 sidecar 启动失败，Go 服务会继续使用原 mqadmin 进程路径。

RocketMQ 发布包默认来源：

- https://mirrors.huaweicloud.com/apache/rocketmq/
- https://mirrors.huaweicloud.com/apache/rocketmq/5.3.2/rocketmq-all-5.3.2-bin-release.zip

## 构建镜像

```powershell
docker build -t rocketmq-go-dashboard:local .
```

如需指定 RocketMQ tools 版本：

```powershell
docker build --build-arg ROCKETMQ_VERSION=5.3.2 -t rocketmq-go-dashboard:5.3.2 .
```

如果 Apache 下载源不稳定，可以切换 RocketMQ 发布包镜像源：

```powershell
docker build `
  --build-arg ROCKETMQ_DOWNLOAD_BASE=https://mirrors.aliyun.com/apache/rocketmq `
  -t rocketmq-go-dashboard:local .
```

默认会从 Apache 官方下载很小的 `.sha512` 文件校验发布包；如果部署环境完全不能访问 Apache 官方，可以显式关闭校验：

```powershell
docker build `
  --build-arg ROCKETMQ_CHECKSUM_BASE= `
  -t rocketmq-go-dashboard:local .
```

如果服务器的 apt 源较慢，也可以切换 Ubuntu 源：

```powershell
docker build `
  --build-arg APT_MIRROR=https://mirrors.aliyun.com/ubuntu/ `
  -t rocketmq-go-dashboard:local .
```

如果服务器不能直接访问 Docker Hub，可以把基础镜像切换到内网镜像仓库：

```powershell
docker build `
  --build-arg GO_IMAGE=registry.example.com/library/golang:1.25-bookworm `
  --build-arg JAVA_IMAGE=registry.example.com/library/eclipse-temurin:17-jre-jammy `
  -t rocketmq-go-dashboard:local .
```

## 使用 docker compose 启动

```powershell
$env:RMQD_NAMESRV = "host.docker.internal:9876"
$env:RMQD_NAMESRV_OPTIONS = "host.docker.internal:9876"
$env:RMQD_HTTP_PORT = "18090"
docker compose up -d --build
```

启动后访问：

```text
http://localhost:18090
```

健康检查：

```powershell
docker compose ps
docker compose logs -f rocketmq-dashboard
```

## 常用环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `RMQD_HTTP_PORT` | `18090` | 宿主机暴露端口，仅用于 `docker-compose.yml` 端口映射 |
| `RMQD_ADDR` | `:18090` | 容器内 HTTP 监听地址 |
| `RMQD_NAMESRV` | `host.docker.internal:9876` | 首次启动连接的 RocketMQ NameServer |
| `RMQD_NAMESRV_OPTIONS` | `host.docker.internal:9876` | 启动时的候选 NameServer 列表，多个地址用逗号分隔 |
| `RMQD_ROCKETMQ_VERSION` | `5.3.2` | RocketMQ tools 版本 |
| `RMQD_REQUEST_TIMEOUT_MS` | `60000` | mqadmin 单次命令超时时间 |
| `RMQD_CLUSTER_CACHE_TTL_MS` | `30000` | 集群快照缓存时间 |
| `RMQD_MESSAGE_CHAIN_CACHE_TTL_MS` | `1800000` | 历史消息链路快照、detail/trace/progress 子缓存时间 |
| `RMQD_COMMAND_MAX_LATENCY_MS` | `1000` | 页面上标记慢命令的阈值 |
| `RMQD_ADMIN_SIDECAR_ENABLED` | `true` | 是否启用常驻 Java Admin sidecar |
| `RMQD_ADMIN_SIDECAR_ADDR` | `127.0.0.1:18091` | Go 服务访问 sidecar 的本机地址 |
| `RMQD_ADMIN_SIDECAR_CLASSPATH` | `/app/rocketmq-admin-sidecar:/opt/rocketmq/lib/*` | sidecar classpath |
| `RMQD_ADMIN_SIDECAR_MAIN_CLASS` | `dev.codex.rocketmq.AdminSidecar` | sidecar Java 主类 |
| `RMQD_ADMIN_SIDECAR_TIMEOUT_MS` | `3000` | Go 调用 sidecar 的 HTTP 超时时间 |
| `RMQD_PROXY_EXTERNAL_HOST` | `127.0.0.1` | 页面展示的 Proxy 对外宿主机或域名，不包含协议和端口 |
| `RMQD_PROXY_GRPC_HOST_PORT` | `18081` | Proxy gRPC 的宿主机映射端口，同时用于页面展示 |
| `RMQD_PROXY_REMOTING_HOST_PORT` | `18080` | Proxy Remoting 的宿主机映射端口，同时用于页面展示 |

| 构建变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GO_IMAGE` | `golang:1.25-bookworm` | Go 编译阶段基础镜像，可替换为内网镜像 |
| `JAVA_IMAGE` | `eclipse-temurin:17-jdk-jammy` | Java 运行时和 sidecar 编译基础镜像，可替换为内网镜像 |
| `ROCKETMQ_DOWNLOAD_BASE` | `https://mirrors.huaweicloud.com/apache/rocketmq` | RocketMQ 发布包下载源 |
| `ROCKETMQ_CHECKSUM_BASE` | `https://downloads.apache.org/rocketmq` | RocketMQ `.sha512` 校验文件下载源，设为空可跳过 |
| `APT_MIRROR` | `https://repo.huaweicloud.com/ubuntu/` | 运行时镜像安装 `curl/unzip` 时使用的 Ubuntu apt 源 |

如果 NameServer 在宿主机本机运行，容器内不能使用 `127.0.0.1:9876` 访问宿主机；Docker Desktop 下应改成：

```powershell
$env:RMQD_NAMESRV = "host.docker.internal:9876"
```

如果 NameServer 是局域网或服务器地址，直接使用实际 IP 和端口即可。

## 多集群使用

容器启动后仍然支持页面里的“添加 / 切换集群”弹窗。`RMQD_NAMESRV` 只决定默认连接，`RMQD_NAMESRV_OPTIONS` 只决定启动候选；用户在页面里添加的 NameServer 会保存在浏览器本地。

## goadmin CLI

容器内置 `goadmin`，子命令参数与官方 `mqadmin` 保持一致。默认会从 `RMQD_NAMESRV` 自动补齐 `-n`，显式传入 `-n` 时不重复插入。

```powershell
docker exec -it rocketmq-go-dashboard goadmin topicList
docker exec -it rocketmq-go-dashboard goadmin --transport native topicRoute -t GoadminTopicRouteTest
docker exec -it rocketmq-go-dashboard goadmin --transport native topicStatus -t GoadminOffsetTest
docker exec -it rocketmq-go-dashboard goadmin --transport native topicClusterList -t GoadminClusterTest
docker exec -it rocketmq-go-dashboard goadmin queryMsgById -t sample_order_events_topic -i 0AE97A6A00017F3CA64A23D49A900003 -f UTF-8
docker exec -it rocketmq-go-dashboard goadmin --transport sidecar topicList
```

`RMQD_GOADMIN_TRANSPORT` 支持 `auto`、`native`、`sidecar`、`process`。`native` 使用 Go 原生 Remoting，目前已支持非 `-c` 模式的 `topicList`、默认 JSON 模式的 `topicRoute`、表格模式的 `topicStatus` 和 `topicClusterList`；`topicList -c`、`topicRoute -l` 以及其它尚未原生化命令会在 `auto` 下继续走 sidecar/process 官方兼容层；`sidecar` 复用常驻 Java Admin sidecar，适合连续执行只读查询；`process` 与官方 `MQAdminStartup` 同源，适合排查 sidecar 行为差异。
