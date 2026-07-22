package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const proxyRuntimeStateVersion = 1

// ProxyRuntimeOptions 定义官方 RocketMQ Proxy 进程的本地运行环境。
type ProxyRuntimeOptions struct {
	// RuntimeDir 保存持久化状态、生成的 rmq-proxy.json 和进程日志。
	RuntimeDir string
	// JavaPath 是启动 Proxy 使用的 Java 可执行文件。
	JavaPath string
	// RocketMQHome 指向包含 conf 和 lib 的 RocketMQ 二进制目录。
	RocketMQHome string
	// NameServer 用于生成首次打开面板时的默认 Proxy 配置。
	NameServer string
	// ExternalHost 是客户端访问 Proxy 时使用的宿主机或域名。
	ExternalHost string
	// ExternalGRPCPort 是 gRPC 监听端口映射到宿主机后的对外端口。
	ExternalGRPCPort int
	// ExternalRemotingPort 是 Remoting 监听端口映射到宿主机后的对外端口。
	ExternalRemotingPort int
	// HeapMB 是 Proxy Java 进程的最小和最大堆大小。
	HeapMB int
	// StartTimeout 是启动后等待 gRPC 端口就绪的最长时间。
	StartTimeout time.Duration
	// StopTimeout 是发送中断信号后等待进程退出的最长时间。
	StopTimeout time.Duration
	// ProbeTimeout 是一次 gRPC Reflection 服务发现允许占用的最长时间。
	ProbeTimeout time.Duration
}

// proxyRuntimeState 是持久化到磁盘的唯一配置真源。
type proxyRuntimeState struct {
	// Version 用于后续配置结构升级。
	Version int `json:"version"`
	// Enabled 表示 Dashboard 启动时需要恢复 Proxy 进程。
	Enabled bool `json:"enabled"`
	// Settings 是直接写入官方 rmq-proxy.json 的配置字段。
	Settings map[string]any `json:"settings"`
}

// proxyRuntimeFieldDefinition 为常用 Proxy 字段提供中文标签和输入约束。
type proxyRuntimeFieldDefinition struct {
	key         string
	label       string
	group       string
	description string
	fieldType   string
	options     []string
	min         *float64
	max         *float64
}

// ProxyRuntimeManager 管理 Proxy 配置原子写入、Java 进程和 gRPC 端口健康检查。
type ProxyRuntimeManager struct {
	options ProxyRuntimeOptions

	operationMu    sync.Mutex
	mu             sync.RWMutex
	state          proxyRuntimeState
	cmd            *exec.Cmd
	done           chan error
	stopping       bool
	running        bool
	healthy        bool
	status         string
	lastError      string
	startedAt      int64
	restarts       int
	probeSuccessAt int64
	probeEndpoint  string
	probeError     string
	probeServices  []string
}

var proxyRuntimeFieldDefinitions = []proxyRuntimeFieldDefinition{
	{key: "rocketMQClusterName", label: "RocketMQ 集群", group: "连接", description: "Proxy 访问的 Broker 集群名称。", fieldType: "text"},
	{key: "namesrvAddr", label: "NameServer 地址", group: "连接", description: "Proxy 用于发现 Broker 路由的 NameServer 地址。", fieldType: "text"},
	{key: "proxyMode", label: "运行模式", group: "连接", description: "cluster 只启动 Proxy；local 会在同一进程启动 Broker。", fieldType: "select", options: []string{"cluster", "local"}},
	{key: "grpcServerPort", label: "gRPC 端口", group: "监听", description: "RocketMQ 5.x gRPC SDK 连接的监听端口。", fieldType: "integer", min: floatPointer(1), max: floatPointer(65535)},
	{key: "remotingListenPort", label: "Remoting 端口", group: "监听", description: "兼容 Remoting 客户端的 Proxy 监听端口。", fieldType: "integer", min: floatPointer(1), max: floatPointer(65535)},
	{key: "enableGrpcEpoll", label: "gRPC Epoll", group: "gRPC", description: "在 Linux 环境启用 gRPC Epoll 事件循环。", fieldType: "boolean"},
	{key: "grpcThreadPoolNums", label: "gRPC 工作线程", group: "gRPC", description: "处理 gRPC 请求的通用线程数量。", fieldType: "integer", min: floatPointer(1), max: floatPointer(4096)},
	{key: "grpcThreadPoolQueueCapacity", label: "gRPC 队列容量", group: "gRPC", description: "通用 gRPC 线程池等待队列容量。", fieldType: "integer", min: floatPointer(1), max: floatPointer(10000000)},
	{key: "grpcMaxInboundMessageSize", label: "最大入站消息字节", group: "gRPC", description: "单个 gRPC 入站请求允许的最大字节数。", fieldType: "integer", min: floatPointer(1024), max: floatPointer(1073741824)},
	{key: "enableMessageBodyEmptyCheck", label: "空消息体校验", group: "消息校验", description: "拒绝消息体为空的发送请求。", fieldType: "boolean"},
	{key: "enableTopicMessageTypeCheck", label: "Topic 消息类型校验", group: "消息校验", description: "发送前校验 Topic 声明的消息类型。", fieldType: "boolean"},
	{key: "grpcClientProducerMaxAttempts", label: "生产重试次数", group: "客户端策略", description: "gRPC 生产请求的最大尝试次数。", fieldType: "integer", min: floatPointer(1), max: floatPointer(100)},
	{key: "grpcClientConsumerMinLongPollingTimeoutMillis", label: "最小长轮询毫秒", group: "客户端策略", description: "消费端长轮询允许的最短等待时间。", fieldType: "integer", min: floatPointer(100), max: floatPointer(300000)},
	{key: "grpcClientConsumerMaxLongPollingTimeoutMillis", label: "最大长轮询毫秒", group: "客户端策略", description: "消费端长轮询允许的最长等待时间。", fieldType: "integer", min: floatPointer(100), max: floatPointer(300000)},
	{key: "grpcClientIdleTimeMills", label: "客户端空闲毫秒", group: "客户端策略", description: "客户端连接进入空闲清理前的等待时间。", fieldType: "integer", min: floatPointer(1000), max: floatPointer(86400000)},
	{key: "enableProxyAutoRenew", label: "自动续期", group: "运行策略", description: "允许 Proxy 自动续期客户端活动状态。", fieldType: "boolean"},
	{key: "enableACL", label: "Proxy ACL", group: "访问控制", description: "启用 Proxy 侧 ACL 检查。", fieldType: "boolean"},
	{key: "enableBatchAck", label: "批量确认", group: "消费", description: "启用消费消息的批量 ACK 能力。", fieldType: "boolean"},
}

// NewProxyRuntimeManager 创建运行器并加载已有持久化配置，但不会自动启动进程。
func NewProxyRuntimeManager(options ProxyRuntimeOptions) (*ProxyRuntimeManager, error) {
	options = normalizeProxyRuntimeOptions(options)
	if err := os.MkdirAll(options.RuntimeDir, 0o750); err != nil {
		return nil, fmt.Errorf("创建 Proxy 运行目录失败: %w", err)
	}
	manager := &ProxyRuntimeManager{options: options, status: "stopped"}
	state, err := manager.loadState()
	if err != nil {
		return nil, err
	}
	manager.state = state
	if !state.Enabled {
		manager.status = "disabled"
	}
	return manager, nil
}

// Restore 根据持久化 enabled 状态恢复 Proxy，Dashboard 启动时调用一次。
func (m *ProxyRuntimeManager) Restore(ctx context.Context) error {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	state := m.currentState()
	if !state.Enabled {
		return nil
	}
	if err := m.writeProxyConfig(state.Settings); err != nil {
		return err
	}
	return m.startProxy(ctx, state.Settings)
}

// Snapshot 返回配置和进程状态副本，并即时完成只读 gRPC Reflection 服务发现。
func (m *ProxyRuntimeManager) Snapshot() ProxyRuntimeSnapshot {
	m.mu.RLock()
	state := cloneProxyRuntimeState(m.state)
	running := m.running
	healthy := m.healthy
	status := m.status
	lastError := m.lastError
	startedAt := m.startedAt
	restarts := m.restarts
	probeSuccessAt := m.probeSuccessAt
	probeEndpoint := m.probeEndpoint
	probeError := m.probeError
	probeServices := append([]string(nil), m.probeServices...)
	process := m.cmd
	pid := 0
	if m.cmd != nil && m.cmd.Process != nil {
		pid = m.cmd.Process.Pid
	}
	m.mu.RUnlock()
	grpcPort := proxySettingInt(state.Settings, "grpcServerPort")
	if running {
		probeContext, cancel := context.WithTimeout(context.Background(), m.options.ProbeTimeout)
		probe, probeErr := probeProxyGRPC(probeContext, grpcPort)
		cancel()
		probeEndpoint = probe.Endpoint
		if probeErr != nil {
			healthy = false
			probeError = probeErr.Error()
			probeServices = nil
			if status == "running" {
				status = "error"
			}
			lastError = "gRPC Reflection 探测失败: " + probeError
		} else {
			healthy = true
			probeSuccessAt = time.Now().UnixMilli()
			probeError = ""
			probeServices = probe.Services
		}
		m.recordProxyGRPCProbe(process, healthy, probeEndpoint, probeSuccessAt, probeError, probeServices)
	}
	return ProxyRuntimeSnapshot{
		Available:                   m.available() == nil,
		Enabled:                     state.Enabled,
		Running:                     running,
		Healthy:                     healthy,
		Status:                      status,
		PID:                         pid,
		GrpcEndpoint:                fmt.Sprintf("0.0.0.0:%d", grpcPort),
		RemotingEndpoint:            fmt.Sprintf("0.0.0.0:%d", proxySettingInt(state.Settings, "remotingListenPort")),
		GrpcExternalEndpoint:        proxyExternalEndpoint(m.options.ExternalHost, m.options.ExternalGRPCPort),
		RemotingExternalEndpoint:    proxyExternalEndpoint(m.options.ExternalHost, m.options.ExternalRemotingPort),
		GrpcProbeEndpoint:           probeEndpoint,
		GrpcProbeSuccessAtUnixMilli: probeSuccessAt,
		GrpcProbeError:              probeError,
		GrpcServices:                probeServices,
		StartedAtUnixMilli:          startedAt,
		RestartCount:                restarts,
		LastError:                   lastError,
		Fields:                      proxyRuntimeFields(state.Settings),
	}
}

// recordProxyGRPCProbe 仅在被探测的 Java 进程仍是当前进程时保存服务发现结果。
func (m *ProxyRuntimeManager) recordProxyGRPCProbe(process *exec.Cmd, healthy bool, endpoint string, successAt int64, probeError string, services []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || m.cmd != process {
		return
	}
	m.healthy = healthy
	m.probeEndpoint = endpoint
	if healthy {
		m.probeSuccessAt = successAt
		m.probeError = ""
		m.probeServices = append([]string(nil), services...)
		return
	}
	m.probeError = probeError
	m.probeServices = nil
}

// Apply 原子保存配置；启用状态会立即启动或重启，失败时恢复旧配置和旧运行态。
func (m *ProxyRuntimeManager) Apply(ctx context.Context, request ProxyRuntimeApplyRequest) (ProxyRuntimeSnapshot, error) {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()

	settings, err := normalizeProxyRuntimeSettings(request.Settings, m.options.NameServer)
	if err != nil {
		return m.Snapshot(), err
	}
	previous := m.currentState()
	next := proxyRuntimeState{Version: proxyRuntimeStateVersion, Enabled: request.Enabled, Settings: settings}
	if err := m.stopProxy(); err != nil {
		return m.Snapshot(), fmt.Errorf("停止旧 Proxy 失败: %w", err)
	}
	if err := m.persistState(next); err != nil {
		_ = m.restorePreviousState(ctx, previous)
		return m.Snapshot(), err
	}
	if err := m.writeProxyConfig(next.Settings); err != nil {
		_ = m.restorePreviousState(ctx, previous)
		return m.Snapshot(), err
	}
	m.setState(next)
	if !next.Enabled {
		m.setStoppedStatus("disabled", "")
		return m.Snapshot(), nil
	}
	if err := m.startProxy(ctx, next.Settings); err != nil {
		rollbackErr := m.restorePreviousState(ctx, previous)
		if rollbackErr != nil {
			return m.Snapshot(), fmt.Errorf("Proxy 新配置启动失败且旧配置恢复失败: %v; rollback=%v", err, rollbackErr)
		}
		return m.Snapshot(), fmt.Errorf("Proxy 新配置启动失败，已恢复旧配置: %w", err)
	}
	return m.Snapshot(), nil
}

// Restart 使用当前持久化配置重启 Proxy；配置未启用时不启动进程。
func (m *ProxyRuntimeManager) Restart(ctx context.Context) (ProxyRuntimeSnapshot, error) {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	state := m.currentState()
	if !state.Enabled {
		return m.Snapshot(), errors.New("Proxy 当前为关闭状态，请先开启并保存")
	}
	if err := m.stopProxy(); err != nil {
		return m.Snapshot(), fmt.Errorf("停止 Proxy 失败: %w", err)
	}
	if err := m.writeProxyConfig(state.Settings); err != nil {
		return m.Snapshot(), err
	}
	if err := m.startProxy(ctx, state.Settings); err != nil {
		return m.Snapshot(), fmt.Errorf("重启 Proxy 失败: %w", err)
	}
	return m.Snapshot(), nil
}

// Close 停止 Proxy 进程，供测试或显式关闭 Dashboard 时使用。
func (m *ProxyRuntimeManager) Close() error {
	m.operationMu.Lock()
	defer m.operationMu.Unlock()
	return m.stopProxy()
}

// restorePreviousState 在新配置失败后恢复磁盘、内存和旧运行态。
func (m *ProxyRuntimeManager) restorePreviousState(ctx context.Context, previous proxyRuntimeState) error {
	_ = m.stopProxy()
	if err := m.persistState(previous); err != nil {
		return err
	}
	if err := m.writeProxyConfig(previous.Settings); err != nil {
		return err
	}
	m.setState(previous)
	if !previous.Enabled {
		m.setStoppedStatus("disabled", "")
		return nil
	}
	return m.startProxy(ctx, previous.Settings)
}

// startProxy 直接启动官方 ProxyStartup Java 主类，并等待 gRPC Reflection 服务发现就绪。
func (m *ProxyRuntimeManager) startProxy(ctx context.Context, settings map[string]any) error {
	if err := m.available(); err != nil {
		m.setStoppedStatus("unavailable", err.Error())
		return err
	}
	logFile, err := os.OpenFile(m.logPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("打开 Proxy 日志失败: %w", err)
	}
	args := m.javaArgs(settings)
	cmd := exec.Command(m.options.JavaPath, args...)
	cmd.Dir = m.options.RuntimeDir
	cmd.Env = append(os.Environ(), "ROCKETMQ_HOME="+m.options.RocketMQHome)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		m.setStoppedStatus("error", err.Error())
		return fmt.Errorf("启动 Proxy Java 进程失败: %w", err)
	}
	done := make(chan error, 1)
	m.mu.Lock()
	m.cmd = cmd
	m.done = done
	m.stopping = false
	m.running = true
	m.healthy = false
	m.status = "starting"
	m.lastError = ""
	m.mu.Unlock()
	go m.waitProxy(cmd, done, logFile)

	timeout := m.options.StartTimeout
	startCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < timeout {
			cancel()
			startCtx, cancel = context.WithTimeout(context.Background(), remaining)
			defer cancel()
		}
	}
	port := proxySettingInt(settings, "grpcServerPort")
	probeEndpoint := proxyGRPCProbeEndpoint(port)
	var lastProbeError error
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeContext, probeCancel := context.WithTimeout(startCtx, m.options.ProbeTimeout)
		probe, probeErr := probeProxyGRPC(probeContext, port)
		probeCancel()
		if probeErr == nil {
			probeSuccessAt := time.Now().UnixMilli()
			m.recordProxyGRPCProbe(cmd, true, probe.Endpoint, probeSuccessAt, "", probe.Services)
			m.mu.Lock()
			if m.cmd == cmd {
				m.healthy = true
				m.status = "running"
				m.startedAt = time.Now().UnixMilli()
				m.restarts++
			}
			m.mu.Unlock()
			return nil
		}
		lastProbeError = probeErr
		m.recordProxyGRPCProbe(cmd, false, probe.Endpoint, 0, probeErr.Error(), nil)
		select {
		case processErr := <-done:
			if processErr == nil {
				processErr = errors.New("Proxy 进程在端口就绪前退出")
			}
			return processErr
		case <-startCtx.Done():
			_ = m.stopProxy()
			return fmt.Errorf("等待 gRPC Reflection 服务发现 %s 就绪超时: %w; 最近失败: %v", probeEndpoint, startCtx.Err(), lastProbeError)
		case <-ticker.C:
		}
	}
}

// waitProxy 回收 Java 进程并更新运行状态，避免产生僵尸进程。
func (m *ProxyRuntimeManager) waitProxy(cmd *exec.Cmd, done chan error, logFile *os.File) {
	err := cmd.Wait()
	_ = logFile.Close()
	m.mu.Lock()
	stopping := m.stopping
	if m.cmd == cmd {
		m.cmd = nil
		m.done = nil
		m.running = false
		m.healthy = false
		if stopping {
			m.status = "stopped"
			m.lastError = ""
		} else {
			m.status = "error"
			if err != nil {
				m.lastError = err.Error()
			} else {
				m.lastError = "Proxy 进程已退出"
			}
		}
	}
	m.mu.Unlock()
	done <- err
	close(done)
}

// stopProxy 发送中断信号并等待退出，超时后终止进程。
func (m *ProxyRuntimeManager) stopProxy() error {
	m.mu.Lock()
	cmd := m.cmd
	done := m.done
	if cmd == nil || cmd.Process == nil {
		m.running = false
		m.healthy = false
		if m.state.Enabled {
			m.status = "stopped"
		} else {
			m.status = "disabled"
		}
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	m.status = "stopping"
	m.mu.Unlock()

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		if killErr := cmd.Process.Kill(); killErr != nil {
			return fmt.Errorf("发送停止信号失败: %v; kill=%v", err, killErr)
		}
	}
	select {
	case <-done:
		return nil
	case <-time.After(m.options.StopTimeout):
		if err := cmd.Process.Kill(); err != nil {
			return fmt.Errorf("Proxy 停止超时且终止失败: %w", err)
		}
		select {
		case <-done:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("Proxy 进程终止后仍未退出")
		}
	}
}

// currentState 返回深拷贝，避免 HTTP 请求修改运行器内部 map。
func (m *ProxyRuntimeManager) currentState() proxyRuntimeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneProxyRuntimeState(m.state)
}

// setState 替换当前持久化配置的内存副本。
func (m *ProxyRuntimeManager) setState(state proxyRuntimeState) {
	m.mu.Lock()
	m.state = cloneProxyRuntimeState(state)
	m.mu.Unlock()
}

// setStoppedStatus 更新没有活动进程时的状态和错误。
func (m *ProxyRuntimeManager) setStoppedStatus(status string, lastError string) {
	m.mu.Lock()
	m.running = false
	m.healthy = false
	m.status = status
	m.lastError = lastError
	m.mu.Unlock()
}

// available 核验 Java、RocketMQ lib 和 conf 是否存在。
func (m *ProxyRuntimeManager) available() error {
	if _, err := exec.LookPath(m.options.JavaPath); err != nil {
		return fmt.Errorf("Java 不可用: %w", err)
	}
	for _, path := range []string{filepath.Join(m.options.RocketMQHome, "lib"), filepath.Join(m.options.RocketMQHome, "conf")} {
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("RocketMQ 目录不可用: %s", path)
		}
	}
	return nil
}

// javaArgs 构造直接运行 ProxyStartup 的 JVM 参数，避免 shell 包装进程影响停止和回收。
func (m *ProxyRuntimeManager) javaArgs(settings map[string]any) []string {
	heap := m.options.HeapMB
	classpath := strings.Join([]string{
		filepath.Join(m.options.RocketMQHome, "conf"),
		filepath.Join(m.options.RocketMQHome, "lib", "*"),
	}, string(os.PathListSeparator))
	return []string{
		fmt.Sprintf("-Xms%dm", heap),
		fmt.Sprintf("-Xmx%dm", heap),
		"-XX:+UseG1GC",
		"-XX:-OmitStackTraceInFastThrow",
		"-Drocketmq.home.dir=" + m.options.RocketMQHome,
		"-Drmq.logback.configurationFile=" + filepath.Join(m.options.RocketMQHome, "conf", "rmq.proxy.logback.xml"),
		"-cp", classpath,
		"org.apache.rocketmq.proxy.ProxyStartup",
		"-pc", m.proxyConfigPath(),
		"-pm", proxySettingString(settings, "proxyMode"),
		"-n", proxySettingString(settings, "namesrvAddr"),
	}
}

// loadState 从磁盘读取状态；首次运行时返回关闭状态和官方常用默认值。
func (m *ProxyRuntimeManager) loadState() (proxyRuntimeState, error) {
	data, err := os.ReadFile(m.statePath())
	if errors.Is(err, os.ErrNotExist) {
		settings, normalizeErr := normalizeProxyRuntimeSettings(nil, m.options.NameServer)
		if normalizeErr != nil {
			return proxyRuntimeState{}, normalizeErr
		}
		return proxyRuntimeState{Version: proxyRuntimeStateVersion, Enabled: false, Settings: settings}, nil
	}
	if err != nil {
		return proxyRuntimeState{}, fmt.Errorf("读取 Proxy 状态失败: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var state proxyRuntimeState
	if err := decoder.Decode(&state); err != nil {
		return proxyRuntimeState{}, fmt.Errorf("解析 Proxy 状态失败: %w", err)
	}
	settings, err := normalizeProxyRuntimeSettings(state.Settings, m.options.NameServer)
	if err != nil {
		return proxyRuntimeState{}, fmt.Errorf("Proxy 持久化配置无效: %w", err)
	}
	state.Version = proxyRuntimeStateVersion
	state.Settings = settings
	return state, nil
}

// persistState 使用同目录临时文件和 rename 原子替换持久化状态。
func (m *ProxyRuntimeManager) persistState(state proxyRuntimeState) error {
	return writeJSONAtomic(m.statePath(), state)
}

// writeProxyConfig 从规范化状态生成官方 Proxy 可直接读取的 JSON 文件。
func (m *ProxyRuntimeManager) writeProxyConfig(settings map[string]any) error {
	return writeJSONAtomic(m.proxyConfigPath(), settings)
}

// writeJSONAtomic 将结构化 JSON 写到临时文件后原子替换目标文件。
func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化 JSON 失败: %w", err)
	}
	data = append(data, '\n')
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o640); err != nil {
		return fmt.Errorf("写入临时配置失败: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("替换配置文件失败: %w", err)
	}
	return nil
}

// normalizeProxyRuntimeOptions 填充运行目录、堆大小和超时默认值。
func normalizeProxyRuntimeOptions(options ProxyRuntimeOptions) ProxyRuntimeOptions {
	if strings.TrimSpace(options.RuntimeDir) == "" {
		options.RuntimeDir = filepath.Join(".tmp", "runtime")
	}
	if strings.TrimSpace(options.JavaPath) == "" {
		options.JavaPath = "java"
	}
	if strings.TrimSpace(options.RocketMQHome) == "" {
		options.RocketMQHome = "/opt/rocketmq"
	}
	options.ExternalHost = strings.TrimSpace(options.ExternalHost)
	if options.ExternalHost == "" {
		options.ExternalHost = "127.0.0.1"
	}
	if options.ExternalGRPCPort <= 0 {
		options.ExternalGRPCPort = 8081
	}
	if options.ExternalRemotingPort <= 0 {
		options.ExternalRemotingPort = 8080
	}
	if options.HeapMB <= 0 {
		options.HeapMB = 512
	}
	if options.StartTimeout <= 0 {
		options.StartTimeout = 30 * time.Second
	}
	if options.StopTimeout <= 0 {
		options.StopTimeout = 30 * time.Second
	}
	if options.ProbeTimeout <= 0 {
		options.ProbeTimeout = 3 * time.Second
	}
	return options
}

// normalizeProxyRuntimeSettings 合并默认配置、校验字段类型和跨字段约束。
func normalizeProxyRuntimeSettings(raw map[string]any, nameServer string) (map[string]any, error) {
	settings := defaultProxyRuntimeSettings(nameServer)
	for key, value := range raw {
		if !runtimeConfigKeyPattern.MatchString(strings.TrimSpace(key)) {
			return nil, fmt.Errorf("Proxy 配置 key %q 格式不正确", key)
		}
		settings[key] = value
	}
	definitions := proxyRuntimeDefinitionMap()
	for key, value := range settings {
		definition, known := definitions[key]
		if !known {
			normalized, err := normalizeUnknownProxyValue(key, value)
			if err != nil {
				return nil, err
			}
			settings[key] = normalized
			continue
		}
		normalized, err := normalizeProxyFieldValue(definition, value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", definition.label, err)
		}
		settings[key] = normalized
	}
	if strings.TrimSpace(proxySettingString(settings, "rocketMQClusterName")) == "" {
		return nil, errors.New("RocketMQ 集群必填")
	}
	if strings.TrimSpace(proxySettingString(settings, "namesrvAddr")) == "" {
		return nil, errors.New("NameServer 地址必填")
	}
	grpcPort := proxySettingInt(settings, "grpcServerPort")
	remotingPort := proxySettingInt(settings, "remotingListenPort")
	if grpcPort == remotingPort {
		return nil, errors.New("gRPC 端口与 Remoting 端口不能相同")
	}
	minPolling := proxySettingInt(settings, "grpcClientConsumerMinLongPollingTimeoutMillis")
	maxPolling := proxySettingInt(settings, "grpcClientConsumerMaxLongPollingTimeoutMillis")
	if minPolling > maxPolling {
		return nil, errors.New("最小长轮询时间不能大于最大长轮询时间")
	}
	return settings, nil
}

// defaultProxyRuntimeSettings 返回官方 5.3.2 常用 ProxyConfig 字段的可运行默认值。
func defaultProxyRuntimeSettings(nameServer string) map[string]any {
	nameServer = strings.TrimSpace(nameServer)
	if nameServer == "" {
		nameServer = "127.0.0.1:9876"
	}
	return map[string]any{
		"rocketMQClusterName":                           "DefaultCluster",
		"namesrvAddr":                                   nameServer,
		"proxyMode":                                     "cluster",
		"grpcServerPort":                                8081,
		"remotingListenPort":                            8080,
		"enableGrpcEpoll":                               false,
		"grpcThreadPoolNums":                            32,
		"grpcThreadPoolQueueCapacity":                   100000,
		"grpcMaxInboundMessageSize":                     136314880,
		"enableMessageBodyEmptyCheck":                   true,
		"enableTopicMessageTypeCheck":                   true,
		"grpcClientProducerMaxAttempts":                 3,
		"grpcClientConsumerMinLongPollingTimeoutMillis": 5000,
		"grpcClientConsumerMaxLongPollingTimeoutMillis": 20000,
		"grpcClientIdleTimeMills":                       120000,
		"enableProxyAutoRenew":                          true,
		"enableACL":                                     false,
		"enableBatchAck":                                false,
	}
}

// normalizeProxyFieldValue 按字段定义把 JSON 值规范化为 bool、int、float64 或 string。
func normalizeProxyFieldValue(definition proxyRuntimeFieldDefinition, value any) (any, error) {
	switch definition.fieldType {
	case "boolean":
		return proxyBoolValue(value)
	case "integer":
		integer, err := proxyIntegerValue(value)
		if err != nil {
			return nil, err
		}
		if definition.min != nil && float64(integer) < *definition.min {
			return nil, fmt.Errorf("不能小于 %v", *definition.min)
		}
		if definition.max != nil && float64(integer) > *definition.max {
			return nil, fmt.Errorf("不能大于 %v", *definition.max)
		}
		return integer, nil
	case "number":
		number, err := proxyNumberValue(value)
		if err != nil {
			return nil, err
		}
		return number, nil
	case "select":
		text := strings.TrimSpace(fmt.Sprint(value))
		for _, option := range definition.options {
			if strings.EqualFold(text, option) {
				return option, nil
			}
		}
		return nil, fmt.Errorf("只能是 %s", strings.Join(definition.options, "、"))
	default:
		text := strings.TrimSpace(fmt.Sprint(value))
		if strings.ContainsAny(text, "\r\n") || len(text) > 8192 {
			return nil, errors.New("文本不能包含换行且长度不能超过 8192")
		}
		return text, nil
	}
}

// normalizeUnknownProxyValue 保留用户已有的扩展标量字段，并拒绝页面无法表达的嵌套结构。
func normalizeUnknownProxyValue(key string, value any) (any, error) {
	switch typed := value.(type) {
	case bool, string:
		return typed, nil
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return int(integer), nil
		}
		return typed.Float64()
	case float64:
		if typed == float64(int64(typed)) {
			return int(typed), nil
		}
		return typed, nil
	case int, int32, int64:
		return typed, nil
	default:
		return nil, fmt.Errorf("Proxy 配置 %s 只能使用布尔、数字或文本值", key)
	}
}

// proxyRuntimeFields 把全部已保存配置转换为页面控件，并为未知字段推断类型。
func proxyRuntimeFields(settings map[string]any) []ProxyRuntimeField {
	definitions := proxyRuntimeDefinitionMap()
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(left, right int) bool {
		leftDefinition, leftKnown := definitions[keys[left]]
		rightDefinition, rightKnown := definitions[keys[right]]
		if leftKnown != rightKnown {
			return leftKnown
		}
		if leftKnown && leftDefinition.group != rightDefinition.group {
			return leftDefinition.group < rightDefinition.group
		}
		return keys[left] < keys[right]
	})
	fields := make([]ProxyRuntimeField, 0, len(keys))
	for _, key := range keys {
		value := settings[key]
		definition, ok := definitions[key]
		if !ok {
			definition = proxyRuntimeFieldDefinition{
				key:         key,
				label:       key,
				group:       "高级配置",
				description: "当前 rmq-proxy.json 中的扩展配置。",
				fieldType:   inferProxyFieldType(value),
			}
		}
		fields = append(fields, ProxyRuntimeField{
			Key:             key,
			Label:           definition.label,
			Group:           definition.group,
			Description:     definition.description,
			Type:            definition.fieldType,
			Value:           value,
			Options:         append([]string(nil), definition.options...),
			Min:             definition.min,
			Max:             definition.max,
			RestartRequired: true,
		})
	}
	return fields
}

// proxyRuntimeDefinitionMap 按 JSON key 索引字段定义。
func proxyRuntimeDefinitionMap() map[string]proxyRuntimeFieldDefinition {
	result := make(map[string]proxyRuntimeFieldDefinition, len(proxyRuntimeFieldDefinitions))
	for _, definition := range proxyRuntimeFieldDefinitions {
		result[definition.key] = definition
	}
	return result
}

// inferProxyFieldType 根据扩展字段当前值选择页面输入类型。
func inferProxyFieldType(value any) string {
	switch value.(type) {
	case bool:
		return "boolean"
	case int, int32, int64, json.Number:
		return "integer"
	case float32, float64:
		return "number"
	default:
		return "text"
	}
}

// proxyBoolValue 解析 JSON 布尔值或常见布尔文本。
func proxyBoolValue(value any) (bool, error) {
	if typed, ok := value.(bool); ok {
		return typed, nil
	}
	parsed, err := strconv.ParseBool(strings.ToLower(strings.TrimSpace(fmt.Sprint(value))))
	if err != nil {
		return false, errors.New("必须是 true 或 false")
	}
	return parsed, nil
}

// proxyIntegerValue 将 JSON number 和整数文本转换为平台 int。
func proxyIntegerValue(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case float64:
		if typed != float64(int64(typed)) {
			return 0, errors.New("必须是整数")
		}
		return int(typed), nil
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, errors.New("必须是整数")
		}
		return int(parsed), nil
	default:
		parsed, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
		if err != nil {
			return 0, errors.New("必须是整数")
		}
		return int(parsed), nil
	}
}

// proxyNumberValue 将 JSON number 或数字文本转换为 float64。
func proxyNumberValue(value any) (float64, error) {
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case json.Number:
		return typed.Float64()
	default:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(value)), 64)
		if err != nil {
			return 0, errors.New("必须是数字")
		}
		return parsed, nil
	}
}

// proxySettingInt 从已规范化设置读取整数。
func proxySettingInt(settings map[string]any, key string) int {
	value, _ := proxyIntegerValue(settings[key])
	return value
}

// proxySettingString 从设置读取文本。
func proxySettingString(settings map[string]any, key string) string {
	return strings.TrimSpace(fmt.Sprint(settings[key]))
}

// cloneProxyRuntimeState 通过结构化复制隔离 map 引用。
func cloneProxyRuntimeState(state proxyRuntimeState) proxyRuntimeState {
	settings := make(map[string]any, len(state.Settings))
	for key, value := range state.Settings {
		settings[key] = value
	}
	return proxyRuntimeState{Version: state.Version, Enabled: state.Enabled, Settings: settings}
}

// proxyExternalEndpoint 组合部署宿主机和映射端口，供页面直接展示客户端连接地址。
func proxyExternalEndpoint(host string, port int) string {
	host = strings.TrimSpace(host)
	if host == "" || port <= 0 {
		return ""
	}
	return net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(port))
}

// statePath 返回持久化状态文件路径。
func (m *ProxyRuntimeManager) statePath() string {
	return filepath.Join(m.options.RuntimeDir, "proxy-state.json")
}

// proxyConfigPath 返回官方 Proxy 读取的 JSON 配置路径。
func (m *ProxyRuntimeManager) proxyConfigPath() string {
	return filepath.Join(m.options.RuntimeDir, "rmq-proxy.json")
}

// logPath 返回 Proxy 标准输出和错误日志路径。
func (m *ProxyRuntimeManager) logPath() string {
	return filepath.Join(m.options.RuntimeDir, "mqproxy.log")
}

// floatPointer 为字段范围定义创建稳定指针。
func floatPointer(value float64) *float64 {
	return &value
}
