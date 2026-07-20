package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 保存 Dashboard 运行参数，所有字段均可通过环境变量覆盖，便于本机和服务器直接运行。
type Config struct {
	Addr                  string
	NameServer            string
	NameServerOptions     []string
	JavaPath              string
	MavenRepository       string
	MQAdminClasspath      string
	MQAdminClasspathFile  string
	RocketMQVersion       string
	RequestTimeout        time.Duration
	ClusterCacheTTL       time.Duration
	MessageChainCacheTTL  time.Duration
	CommandMaxLatency     time.Duration
	AdminProvider         string
	GoAdminShadow         bool
	GoAdminTimeout        time.Duration
	AdminSidecarEnabled   bool
	AdminSidecarAddr      string
	AdminSidecarClasspath string
	AdminSidecarMainClass string
	AdminSidecarTimeout   time.Duration
	// RuntimeConfigEnabled 控制在线修改 NameServer、Broker 和 Proxy 配置的写入口。
	RuntimeConfigEnabled bool
	// ProxyRuntimeDir 保存 Proxy 持久化状态、生成配置和运行日志。
	ProxyRuntimeDir string
	// ProxyRocketMQHome 指向包含官方 Proxy 依赖的 RocketMQ 二进制目录。
	ProxyRocketMQHome string
	// ProxyHeapMB 控制 Proxy Java 进程堆大小。
	ProxyHeapMB int
	// ProxyStartTimeout 是等待 gRPC 端口启动的最长时间。
	ProxyStartTimeout time.Duration
	// ProxyStopTimeout 是等待 Proxy Java 进程停止的最长时间。
	ProxyStopTimeout time.Duration
}

// Load 从环境变量读取配置，并填充本地开发可直接运行的默认值。
func Load() Config {
	return Config{
		Addr:                 getenv("RMQD_ADDR", ":18090"),
		NameServer:           getenv("RMQD_NAMESRV", "127.0.0.1:9876"),
		NameServerOptions:    splitCSV(getenv("RMQD_NAMESRV_OPTIONS", getenv("RMQD_NAMESRV", "127.0.0.1:9876"))),
		JavaPath:             getenv("RMQD_JAVA", "java"),
		MavenRepository:      getenv("RMQD_M2_REPO", defaultMavenRepository()),
		MQAdminClasspath:     os.Getenv("RMQD_MQADMIN_CLASSPATH"),
		MQAdminClasspathFile: getenv("RMQD_MQADMIN_CLASSPATH_FILE", defaultMQAdminClasspathFile()),
		RocketMQVersion:      getenv("RMQD_ROCKETMQ_VERSION", "5.3.2"),
		// 线上 consumerProgress 冷查询会逐组拉位点，默认给后台采集留足窗口；HTTP 热路径仍只读内存快照。
		RequestTimeout:        durationFromMillis("RMQD_REQUEST_TIMEOUT_MS", 60000),
		ClusterCacheTTL:       durationFromMillis("RMQD_CLUSTER_CACHE_TTL_MS", 30000),
		MessageChainCacheTTL:  durationFromMillis("RMQD_MESSAGE_CHAIN_CACHE_TTL_MS", 1800000),
		CommandMaxLatency:     durationFromMillis("RMQD_COMMAND_MAX_LATENCY_MS", 1000),
		AdminProvider:         strings.ToLower(strings.TrimSpace(getenv("RMQD_ADMIN_PROVIDER", "auto"))),
		GoAdminShadow:         boolFromEnv("RMQD_GO_ADMIN_SHADOW", false),
		GoAdminTimeout:        durationFromMillis("RMQD_GO_ADMIN_TIMEOUT_MS", 3000),
		AdminSidecarEnabled:   boolFromEnv("RMQD_ADMIN_SIDECAR_ENABLED", false),
		AdminSidecarAddr:      getenv("RMQD_ADMIN_SIDECAR_ADDR", "127.0.0.1:18091"),
		AdminSidecarClasspath: getenv("RMQD_ADMIN_SIDECAR_CLASSPATH", ""),
		AdminSidecarMainClass: getenv("RMQD_ADMIN_SIDECAR_MAIN_CLASS", "dev.codex.rocketmq.AdminSidecar"),
		AdminSidecarTimeout:   durationFromMillis("RMQD_ADMIN_SIDECAR_TIMEOUT_MS", 3000),
		RuntimeConfigEnabled:  boolFromEnv("RMQD_RUNTIME_CONFIG_ENABLED", false),
		ProxyRuntimeDir:       getenv("RMQD_PROXY_RUNTIME_DIR", defaultProxyRuntimeDir()),
		ProxyRocketMQHome:     getenv("RMQD_PROXY_ROCKETMQ_HOME", getenv("ROCKETMQ_HOME", "/opt/rocketmq")),
		ProxyHeapMB:           positiveIntFromEnv("RMQD_PROXY_HEAP_MB", 512),
		ProxyStartTimeout:     durationFromMillis("RMQD_PROXY_START_TIMEOUT_MS", 30000),
		ProxyStopTimeout:      durationFromMillis("RMQD_PROXY_STOP_TIMEOUT_MS", 30000),
	}
}

func splitCSV(raw string) []string {
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

func getenv(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func durationFromMillis(name string, fallback int) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return time.Duration(fallback) * time.Millisecond
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return time.Duration(fallback) * time.Millisecond
	}
	return time.Duration(value) * time.Millisecond
}

func boolFromEnv(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

// positiveIntFromEnv 读取正整数环境变量，非法值回到部署默认值。
func positiveIntFromEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func defaultMavenRepository() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".m2/repository"
	}
	return home + string(os.PathSeparator) + ".m2" + string(os.PathSeparator) + "repository"
}

func defaultMQAdminClasspathFile() string {
	path := ".tmp" + string(os.PathSeparator) + "rocketmq-runtime-classpath.txt"
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return ""
}

// defaultProxyRuntimeDir 返回本地开发可写的 Proxy 状态目录；容器部署会显式覆盖到持久卷。
func defaultProxyRuntimeDir() string {
	return ".tmp" + string(os.PathSeparator) + "runtime"
}
