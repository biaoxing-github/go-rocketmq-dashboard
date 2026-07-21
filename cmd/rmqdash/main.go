package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"rocketmq-go-dashboard/internal/config"
	goadmincli "rocketmq-go-dashboard/internal/goadmin"
	"rocketmq-go-dashboard/internal/rocketmq"
	goadminshadow "rocketmq-go-dashboard/internal/rocketmq/goadmin"
	"rocketmq-go-dashboard/internal/server"
)

func main() {
	checkCluster := flag.Bool("check-cluster", false, "只读检查 RocketMQ 集群并输出 JSON")
	flag.Parse()

	cfg := config.Load()
	stopSidecar := startAdminSidecar(cfg)
	if stopSidecar != nil {
		defer stopSidecar()
	}
	providerFactory := func(nameServer string) rocketmq.Provider {
		return mqAdminProviderForMode(cfg, nameServer)
	}
	provider := providerFactory(cfg.NameServer)

	if *checkCluster {
		clusters, err := provider.ClusterList(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(clusters); err != nil {
			log.Fatal(err)
		}
		return
	}

	proxyRuntimeManager, proxyRuntimeErr := server.NewProxyRuntimeManager(server.ProxyRuntimeOptions{
		RuntimeDir:           cfg.ProxyRuntimeDir,
		JavaPath:             cfg.JavaPath,
		RocketMQHome:         cfg.ProxyRocketMQHome,
		NameServer:           cfg.NameServer,
		ExternalHost:         cfg.ProxyExternalHost,
		ExternalGRPCPort:     cfg.ProxyGRPCHostPort,
		ExternalRemotingPort: cfg.ProxyRemotingHostPort,
		HeapMB:               cfg.ProxyHeapMB,
		StartTimeout:         cfg.ProxyStartTimeout,
		StopTimeout:          cfg.ProxyStopTimeout,
	})
	if proxyRuntimeErr != nil {
		log.Printf("RocketMQ Proxy runtime unavailable: %v", proxyRuntimeErr)
		proxyRuntimeManager = nil
	} else {
		defer proxyRuntimeManager.Close()
		if err := proxyRuntimeManager.Restore(context.Background()); err != nil {
			log.Printf("RocketMQ Proxy restore failed: %v", err)
		}
	}

	app := server.New(server.AppConfig{
		ProviderFactory:      providerFactory,
		ClusterCacheTTL:      cfg.ClusterCacheTTL,
		MessageChainCacheTTL: cfg.MessageChainCacheTTL,
		LatencyBudget:        cfg.CommandMaxLatency,
		NameServer:           cfg.NameServer,
		NameServerOptions:    cfg.NameServerOptions,
		RuntimeConfigEnabled: cfg.RuntimeConfigEnabled,
		ProxyRuntime:         proxyRuntimeManager,
	})

	log.Printf("RocketMQ Go Dashboard listening on %s, nameserver=%s", cfg.Addr, cfg.NameServer)
	if err := http.ListenAndServe(cfg.Addr, app); err != nil {
		log.Fatal(err)
	}
}

func mqAdminProviderForMode(cfg config.Config, nameServer string) *rocketmq.MQAdminProvider {
	provider := &rocketmq.MQAdminProvider{
		NameServer:       nameServer,
		JavaPath:         cfg.JavaPath,
		MavenRepository:  cfg.MavenRepository,
		Classpath:        cfg.MQAdminClasspath,
		ClasspathFile:    cfg.MQAdminClasspathFile,
		Version:          cfg.RocketMQVersion,
		Timeout:          cfg.RequestTimeout,
		SidecarEnabled:   cfg.AdminSidecarEnabled,
		SidecarAddr:      cfg.AdminSidecarAddr,
		SidecarClasspath: cfg.AdminSidecarClasspath,
		SidecarMainClass: cfg.AdminSidecarMainClass,
		SidecarTimeout:   cfg.AdminSidecarTimeout,
		MessageCacheTTL:  cfg.MessageChainCacheTTL,
	}
	switch strings.ToLower(strings.TrimSpace(cfg.AdminProvider)) {
	case "sidecar":
		provider.SidecarEnabled = true
	case "goadmin":
		provider.SidecarEnabled = false
		provider.CommandRunner = goAdminCommandRunnerWithShadow(cfg, nameServer, "native")
	case "", "auto":
		provider.SidecarEnabled = false
		provider.CommandRunner = goAdminCommandRunnerWithShadow(cfg, nameServer, "auto")
	case "mqadmin", "process":
		provider.SidecarEnabled = false
		if cfg.GoAdminShadow {
			provider.CommandRunner = processCommandRunnerWithGoAdminShadow(cfg, nameServer, *provider)
		}
	default:
		provider.SidecarEnabled = false
	}
	return provider
}

// processCommandRunnerWithGoAdminShadow 保持官方 mqadmin 进程为主路径，同时旁路执行 Go native shadow 对照。
func processCommandRunnerWithGoAdminShadow(cfg config.Config, nameServer string, provider rocketmq.MQAdminProvider) rocketmq.CommandRunner {
	provider.SidecarEnabled = false
	provider.CommandRunner = nil
	provider.SidecarTransport = nil
	return shadowingCommandRunner{
		primaryName: "process",
		primary: processPrimaryRunner{
			provider: &provider,
		},
		timeout: goAdminShadowTimeout(cfg),
		shadows: []goadminshadow.ShadowTarget{{
			Name:   "native",
			Runner: newGoAdminShadowRunner(cfg, nameServer, "native"),
		}},
		normalizer: goadminshadow.DefaultM6ShadowNormalizer(),
		report:     reportGoAdminShadowResult,
	}
}

// goAdminCommandRunnerWithShadow 根据 Dashboard 配置创建主 runner，并在 M6 shadow 开关打开时挂载旁路对比。
func goAdminCommandRunnerWithShadow(cfg config.Config, nameServer string, transport string) rocketmq.CommandRunner {
	primary := goAdminCommandRunner(cfg, nameServer, transport)
	if !cfg.GoAdminShadow {
		return primary
	}
	return shadowingCommandRunner{
		primaryName: transport,
		primary:     primary,
		timeout:     goAdminShadowTimeout(cfg),
		shadows:     goAdminShadowTargets(cfg, nameServer, transport),
		normalizer:  goadminshadow.DefaultM6ShadowNormalizer(),
		report:      reportGoAdminShadowResult,
	}
}

// goAdminCommandRunner 创建 Dashboard Provider 使用的 goadmin CLI runner，transport 决定 native/auto 等执行路径。
func goAdminCommandRunner(cfg config.Config, nameServer string, transport string) rocketmq.CommandRunner {
	timeout := cfg.GoAdminTimeout
	if timeout <= 0 {
		timeout = cfg.RequestTimeout
	}
	options := goadmincli.OptionsFromConfig(cfg)
	options.NameServer = nameServer
	options.Transport = transport
	options.Timeout = timeout
	options.SidecarTimeout = cfg.AdminSidecarTimeout
	options.Stdout = io.Discard
	options.Stderr = io.Discard
	return goadminRunner{options: options}
}

// processPrimaryRunner 调用 MQAdminProvider 的官方进程 runner，用于 M6 第一阶段 mqadmin 主路径 shadow。
type processPrimaryRunner struct {
	// provider 保存官方 mqadmin 进程模式所需的 classpath、java、timeout 和 NameServer 配置。
	provider *rocketmq.MQAdminProvider
}

// Run 执行官方 process runner；该类型只在 cmd/rmqdash 同包内使用，避免暴露新的 provider API。
func (runner processPrimaryRunner) Run(ctx context.Context, args ...string) (string, error) {
	if runner.provider == nil {
		return "", fmt.Errorf("mqadmin process primary provider is nil")
	}
	return runner.provider.RunCommand(ctx, args...)
}

// goAdminShadowTimeout 返回 shadow 对比的最大后台执行时间，避免旁路验证无限占用资源。
func goAdminShadowTimeout(cfg config.Config) time.Duration {
	if cfg.GoAdminTimeout > 0 {
		return cfg.GoAdminTimeout
	}
	if cfg.RequestTimeout > 0 {
		return cfg.RequestTimeout
	}
	return 3 * time.Second
}

// goAdminShadowTargets 构造除 primary transport 外的 M6 provider 对照目标。
func goAdminShadowTargets(cfg config.Config, nameServer string, primaryTransport string) []goadminshadow.ShadowTarget {
	transports := []string{"process", "sidecar", "native", "auto"}
	targets := make([]goadminshadow.ShadowTarget, 0, len(transports)-1)
	for _, transport := range transports {
		if transport == primaryTransport {
			continue
		}
		// sidecar shadow 只有在 sidecar 已明确启用时才加入，避免未启动 JVM 服务制造后台噪声。
		if transport == "sidecar" && !cfg.AdminSidecarEnabled {
			continue
		}
		targets = append(targets, goadminshadow.ShadowTarget{
			Name:   transport,
			Runner: newGoAdminShadowRunner(cfg, nameServer, transport),
		})
	}
	return targets
}

// goadminRunner 把 goadmin CLI 的一次执行适配成 MQAdminProvider 可使用的 CommandRunner。
type goadminRunner struct {
	// options 是本 runner 固化的全局 CLI 参数；每次调用会复制后再写入 stdout/stderr。
	options goadmincli.Options
}

// Run 执行一次 goadmin 命令，并把非零退出码转换为 provider 错误。
func (runner goadminRunner) Run(ctx context.Context, args ...string) (string, error) {
	var stdout strings.Builder
	var stderr strings.Builder
	options := runner.options
	options.Stdout = &stdout
	options.Stderr = &stderr
	code := goadmincli.Run(ctx, options, args)
	if code != 0 {
		return "", fmt.Errorf("goadmin command failed with exit %d: %s", code, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// goadminShadowRunner 保留 stdout 和 stderr，用于 shadow compare 观察旁路 provider 的完整输出。
type goadminShadowRunner struct {
	// options 是 shadow provider 固化的 transport/nameServer/timeout 等 CLI 参数。
	options goadmincli.Options
}

// RunShadow 执行一次旁路 goadmin 命令，返回 stdout/stderr 和退出码错误供比较器记录。
func (runner goadminShadowRunner) RunShadow(ctx context.Context, args []string) (goadminshadow.ShadowOutput, error) {
	var stdout strings.Builder
	var stderr strings.Builder
	options := runner.options
	options.Stdout = &stdout
	options.Stderr = &stderr
	code := goadmincli.Run(ctx, options, args)
	output := goadminshadow.ShadowOutput{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}
	if code != 0 {
		return output, fmt.Errorf("goadmin shadow command failed with exit %d: %s", code, strings.TrimSpace(stderr.String()))
	}
	return output, nil
}

// newGoAdminShadowRunner 创建指定 transport 的 shadow provider runner。
func newGoAdminShadowRunner(cfg config.Config, nameServer string, transport string) goadminShadowRunner {
	options := goadmincli.OptionsFromConfig(cfg)
	options.NameServer = nameServer
	options.Transport = transport
	options.Timeout = goAdminShadowTimeout(cfg)
	options.SidecarTimeout = cfg.AdminSidecarTimeout
	options.Stdout = io.Discard
	options.Stderr = io.Discard
	return goadminShadowRunner{options: options}
}

// shadowingCommandRunner 先返回主路径结果，再异步执行 M6 shadow compare，保证用户请求不被旁路验证阻塞。
type shadowingCommandRunner struct {
	// primaryName 是主路径 provider 名称，用于差异日志。
	primaryName string
	// primary 是实际服务用户请求的 provider runner。
	primary rocketmq.CommandRunner
	// timeout 是单次后台 shadow compare 的最大执行时间。
	timeout time.Duration
	// shadows 是后台对照的 provider 列表。
	shadows []goadminshadow.ShadowTarget
	// normalizer 用于屏蔽动态时间戳、耗时等字段。
	normalizer goadminshadow.ShadowNormalizer
	// report 接收完整 shadow compare 结果；生产环境默认写入日志。
	report func(goadminshadow.ShadowResult)
}

// Run 执行 primary runner 并异步触发 shadow compare，shadow 错误只进入报告，不改变主路径返回。
func (runner shadowingCommandRunner) Run(ctx context.Context, args ...string) (string, error) {
	if runner.primary == nil {
		return "", fmt.Errorf("goadmin shadow primary runner is nil")
	}
	output, err := runner.primary.Run(ctx, args...)
	if len(runner.shadows) == 0 || runner.report == nil {
		return output, err
	}
	argsCopy := append([]string(nil), args...)
	primary := goadminshadow.ShadowTarget{
		Name: runner.primaryName,
		Runner: capturedShadowRunner{
			stdout: output,
			err:    err,
		},
	}
	timeout := runner.timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	go func() {
		shadowCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		runner.report(goadminshadow.RunShadowCompare(shadowCtx, argsCopy, primary, runner.shadows, runner.normalizer))
	}()
	return output, err
}

// capturedShadowRunner 把 primary 已完成的输出适配成 ShadowRunner，避免为比较再执行一次主路径。
type capturedShadowRunner struct {
	// stdout 是 primary 已返回给用户的标准输出。
	stdout string
	// err 是 primary 已返回给用户的错误。
	err error
}

// RunShadow 返回 primary 的已捕获输出和错误，供 RunShadowCompare 统一比较。
func (runner capturedShadowRunner) RunShadow(ctx context.Context, args []string) (goadminshadow.ShadowOutput, error) {
	return goadminshadow.ShadowOutput{Stdout: runner.stdout}, runner.err
}

// reportGoAdminShadowResult 记录每个旁路 provider 的匹配状态和耗时，作为 M6 shadow 证据入口。
func reportGoAdminShadowResult(result goadminshadow.ShadowResult) {
	line, err := goadminshadow.MarshalShadowReportJSONLine(result)
	if err != nil {
		log.Printf("goadmin shadow report marshal failed command=%s error=%v", result.Command, err)
		return
	}
	log.Printf("goadmin shadow report %s", strings.TrimSpace(string(line)))
}

func startAdminSidecar(cfg config.Config) func() {
	if !cfg.AdminSidecarEnabled {
		return nil
	}
	classpath := strings.TrimSpace(cfg.AdminSidecarClasspath)
	if classpath == "" {
		log.Printf("RocketMQ admin sidecar enabled but RMQD_ADMIN_SIDECAR_CLASSPATH is empty; using mqadmin process fallback")
		return nil
	}
	javaPath := strings.TrimSpace(cfg.JavaPath)
	if javaPath == "" {
		javaPath = "java"
	}
	ctx, cancel := context.WithCancel(context.Background())
	args := adminSidecarJavaArgs(cfg, classpath)
	cmd := exec.CommandContext(ctx, javaPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		cancel()
		log.Printf("RocketMQ admin sidecar start failed, using mqadmin process fallback: %v", err)
		return nil
	}
	if err := waitForAdminSidecar(cfg.AdminSidecarAddr, 8*time.Second); err != nil {
		log.Printf("RocketMQ admin sidecar health check failed, using mqadmin process fallback: %v", err)
		cancel()
		_ = cmd.Wait()
		return nil
	}
	log.Printf("RocketMQ admin sidecar started on %s", cfg.AdminSidecarAddr)
	return func() {
		cancel()
		_ = cmd.Wait()
	}
}

// adminSidecarJavaArgs 生成常驻官方 tools JVM 的启动参数，显式使用 tools logback 配置，避免首次命令把 logback 初始化日志写进捕获 stdout。
func adminSidecarJavaArgs(cfg config.Config, classpath string) []string {
	return []string{
		"-Dfile.encoding=UTF-8",
		"-Dsun.stdout.encoding=UTF-8",
		"-Dsun.stderr.encoding=UTF-8",
		"-Drmq.logback.configurationFile=/opt/rocketmq/conf/rmq.tools.logback.xml",
		"-cp",
		classpath,
		cfg.AdminSidecarMainClass,
		"--addr",
		cfg.AdminSidecarAddr,
	}
}

func waitForAdminSidecar(addr string, timeout time.Duration) error {
	baseURL, err := sidecarBaseURL(addr)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: time.Second}
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Get(baseURL + "/health")
		if err == nil && response.StatusCode == http.StatusOK {
			_ = response.Body.Close()
			return nil
		}
		if response != nil {
			_ = response.Body.Close()
			lastErr = fmt.Errorf("http %d", response.StatusCode)
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return lastErr
}

func sidecarBaseURL(addr string) (string, error) {
	value := strings.TrimSpace(addr)
	if value == "" {
		return "", fmt.Errorf("sidecar addr is empty")
	}
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	return strings.TrimRight(value, "/"), nil
}
