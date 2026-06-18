# RocketMQ Go Dashboard

一个用 Go 写的 RocketMQ Dashboard 和 `goadmin` CLI。

## 能做什么

- 浏览集群、Topic、Consumer 和消息链路
- 通过 `goadmin` 执行和官方 `mqadmin` 兼容的只读命令
- 支持 Docker 部署

## 快速开始

```powershell
go run ./cmd/rmqdash
```

默认连接本机 RocketMQ NameServer：

```text
127.0.0.1:9876
```

如果要启动 Docker：

```powershell
docker compose up -d --build
```

Docker Compose 默认使用 `host.docker.internal:9876` 连接宿主机 NameServer。部署到服务器时可以通过 `RMQD_NAMESRV` 覆盖。

## 环境变量

- `RMQD_ADDR`
- `RMQD_NAMESRV`
- `RMQD_NAMESRV_OPTIONS`
- `RMQD_ADMIN_PROVIDER`
- `RMQD_ADMIN_SIDECAR_ENABLED`

更多参数见 [docs/docker_deploy.md](docs/docker_deploy.md)。

## 目录

- `cmd/goadmin`：CLI 入口
- `cmd/rmqdash`：Dashboard 入口
- `internal/rocketmq`：RocketMQ 适配层
- `internal/server`：HTTP 服务
- `org/apache/rocketmq`：保留的 Apache RocketMQ 依赖源码

## 许可证

Apache-2.0
