package goadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"rocketmq-go-dashboard/internal/config"
	"rocketmq-go-dashboard/internal/rocketmq"
	nativeadmin "rocketmq-go-dashboard/internal/rocketmq/goadmin"
)

// nativeCommandRunner 执行一次 Go 原生 remoting 命令；测试会替换它来验证 CLI 控制流。
var nativeCommandRunner = nativeadmin.RunCommand

// Options 保存 goadmin CLI 的全局执行参数；子命令参数保持与官方 mqadmin 一致并原样透传。
type Options struct {
	// NameServer 是默认 RocketMQ NameServer；用户未在子命令里写 -n 时自动补齐。
	NameServer string
	// Transport 控制执行路径：auto 优先 sidecar 后回退进程，process 直接拉起官方 tools，sidecar 调用常驻 JVM。
	Transport string
	// JavaPath 是进程模式下的 Java 可执行文件路径。
	JavaPath string
	// MavenRepository 是按 Maven 本地仓库拼装 RocketMQ tools classpath 时的根目录。
	MavenRepository string
	// Classpath 是显式 RocketMQ tools classpath，优先级高于 ClasspathFile 和 MavenRepository。
	Classpath string
	// ClasspathFile 是保存 RocketMQ tools classpath 的文件路径。
	ClasspathFile string
	// Version 是 RocketMQ tools 版本，用于 MavenRepository fallback。
	Version string
	// Timeout 是单次官方命令的最大执行时间。
	Timeout time.Duration
	// SidecarAddr 是常驻 Java sidecar 的 HTTP 地址。
	SidecarAddr string
	// SidecarTimeout 是调用 sidecar 的 HTTP 超时时间。
	SidecarTimeout time.Duration
	// Runner 是测试或嵌入场景注入的官方命令执行器；非空时跳过真实 provider 构造。
	Runner rocketmq.CommandRunner
	// Stdout 接收官方命令 stdout 文本。
	Stdout io.Writer
	// Stderr 接收 CLI 错误和帮助提示。
	Stderr io.Writer
}

// OptionsFromConfig 将 Dashboard 环境变量配置映射为 goadmin CLI 默认参数。
func OptionsFromConfig(cfg config.Config) Options {
	sidecarTimeout := cfg.RequestTimeout
	if strings.TrimSpace(os.Getenv("RMQD_ADMIN_SIDECAR_TIMEOUT_MS")) != "" {
		sidecarTimeout = cfg.AdminSidecarTimeout
	}
	return Options{
		NameServer:      cfg.NameServer,
		Transport:       getenv("RMQD_GOADMIN_TRANSPORT", "auto"),
		JavaPath:        cfg.JavaPath,
		MavenRepository: cfg.MavenRepository,
		Classpath:       cfg.MQAdminClasspath,
		ClasspathFile:   cfg.MQAdminClasspathFile,
		Version:         cfg.RocketMQVersion,
		Timeout:         cfg.RequestTimeout,
		SidecarAddr:     cfg.AdminSidecarAddr,
		SidecarTimeout:  sidecarTimeout,
		Stdout:          os.Stdout,
		Stderr:          os.Stderr,
	}
}

// Run 执行 goadmin CLI，一次调用对应一次官方 mqadmin 子命令，返回进程退出码。
func Run(ctx context.Context, options Options, args []string) int {
	options = normalizeOptions(options)
	if hasM6ShadowRunFlag(args) {
		return runM6ShadowBatchCLI(ctx, options, args)
	}
	commandArgs, exitCode, handled := parseGlobalFlags(&options, args)
	if handled {
		return exitCode
	}
	if output, err := officialParserPreflight(commandArgs); err != nil {
		writeOfficialParserError(options, output, err)
		return 1
	}
	commandArgs = injectNameServer(commandArgs, options.NameServer)
	if output, err := officialParserPreflight(commandArgs); err != nil {
		writeOfficialParserError(options, output, err)
		return 1
	}
	interval, intervalArgs, intervalMode, intervalErr := intervalCommand(commandArgs)
	if intervalErr != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", intervalErr)
		return 1
	}
	if intervalMode {
		if err := runIntervalCommand(ctx, options, intervalArgs, interval); err != nil {
			_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
			return 1
		}
		return 0
	}
	output, err := runCommand(ctx, options, commandArgs)
	if err != nil {
		var parserErr *nativeadmin.OfficialParserError
		if errors.As(err, &parserErr) {
			_, _ = fmt.Fprint(options.Stdout, output)
			_, _ = fmt.Fprint(options.Stderr, parserErr.Stderr)
			return 1
		}
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 1
	}
	if isStartMonitoringCommand(commandArgs) && output == "" {
		waitStartMonitoring(ctx)
	}
	_, _ = fmt.Fprint(options.Stdout, output)
	return 0
}

func hasM6ShadowRunFlag(args []string) bool {
	for _, arg := range args {
		name, _, _ := strings.Cut(arg, "=")
		if name == "--m6-shadow-run" {
			return true
		}
	}
	return false
}

func officialParserPreflight(args []string) (string, error) {
	return nativeadmin.OfficialParserPreflight(args)
}

func writeOfficialParserError(options Options, output string, err error) {
	var parserErr *nativeadmin.OfficialParserError
	if errors.As(err, &parserErr) {
		_, _ = fmt.Fprint(options.Stdout, output)
		_, _ = fmt.Fprint(options.Stderr, parserErr.Stderr)
		return
	}
	_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
}

// isStartMonitoringCommand 判断当前子命令是否为官方连续监控命令 startMonitoring。
func isStartMonitoringCommand(args []string) bool {
	return len(args) > 0 && strings.EqualFold(args[0], "startMonitoring")
}

// waitStartMonitoring 让空输出的 startMonitoring 保持前台存活，直到调用方取消上下文。
func waitStartMonitoring(ctx context.Context) {
	<-ctx.Done()
}

func intervalCommand(args []string) (time.Duration, []string, bool, error) {
	if len(args) == 0 {
		return 0, nil, false, nil
	}
	command := args[0]
	defaultSeconds := int64(1)
	implicitInterval := false
	switch {
	case strings.EqualFold(command, "clusterList"):
	case strings.EqualFold(command, "clusterRT"):
		defaultSeconds = 10
		implicitInterval = true
	case strings.EqualFold(command, "getBrokerEpoch"):
		// 官方 getBrokerEpoch 只有显式 -i/--interval 才连续刷新，未写秒数时源码默认 3 秒。
		defaultSeconds = 3
	case strings.EqualFold(command, "getSyncStateSet"):
		// 官方 getSyncStateSet 只有显式 -i/--interval 才连续刷新，未写秒数时源码默认 3 秒。
		defaultSeconds = 3
	case strings.EqualFold(command, "haStatus"):
		// 官方 haStatus 只有显式 -i/--interval 才连续刷新，未写秒数时源码默认 3 秒。
		defaultSeconds = 3
	default:
		return 0, nil, false, nil
	}
	stripped := []string{args[0]}
	found := false
	seconds := defaultSeconds
	for index := 1; index < len(args); index++ {
		arg := args[index]
		var raw string
		switch {
		case arg == "-i" || arg == "--interval":
			found = true
			index++
			if index >= len(args) {
				return 0, nil, false, fmt.Errorf("%s -i/--interval requires a value", command)
			}
			raw = args[index]
		case strings.HasPrefix(arg, "-i="):
			found = true
			raw = strings.TrimPrefix(arg, "-i=")
		case strings.HasPrefix(arg, "--interval="):
			found = true
			raw = strings.TrimPrefix(arg, "--interval=")
		default:
			stripped = append(stripped, arg)
			continue
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			seconds = defaultSeconds
			continue
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, nil, false, fmt.Errorf("解析 %s interval %q 失败: %w", command, raw, err)
		}
		seconds = value
	}
	if !found && !implicitInterval {
		return 0, nil, false, nil
	}
	return time.Duration(seconds) * time.Second, stripped, true, nil
}

func runIntervalCommand(ctx context.Context, options Options, args []string, interval time.Duration) error {
	iteration := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		output, err := runIntervalIteration(ctx, options, args)
		if err != nil {
			return err
		}
		output = intervalOutput(args, iteration, output)
		if _, err := fmt.Fprint(options.Stdout, output); err != nil {
			return err
		}
		iteration++
		if !waitInterval(ctx, interval) {
			return nil
		}
	}
}

func runIntervalIteration(ctx context.Context, options Options, args []string) (string, error) {
	if len(args) > 0 && strings.EqualFold(args[0], "clusterRT") {
		// 官方 clusterRT 即使不带 -i 也会 while(true)，sidecar/process 的整段 stdout 接口会被无限命令卡住。
		// CLI 因此固定用可返回的原生单轮 snapshot 做每一轮输出，再在 Go 层负责 interval、取消和表头去重。
		output, supported, err := nativeCommandRunner(ctx, args, options.Timeout)
		if !supported {
			return "", fmt.Errorf("native transport does not support %q yet", commandName(args))
		}
		return output, err
	}
	// 其他连续命令每轮执行同一份单次查询；这里复用现有 transport，仅由 Go 层负责循环和等待。
	return runCommand(ctx, options, args)
}

func intervalOutput(args []string, iteration int, output string) string {
	if iteration == 0 || len(args) == 0 || !strings.EqualFold(args[0], "clusterRT") {
		return output
	}
	if clusterRTPrintAsTlogArgs(args[1:]) {
		return output
	}
	if !strings.HasPrefix(output, "#Cluster Name") {
		return output
	}
	newline := strings.IndexByte(output, '\n')
	if newline < 0 {
		return ""
	}
	return output[newline+1:]
}

func clusterRTPrintAsTlogArgs(args []string) bool {
	value := cliStringArg(args, "-p", "--print", "--print log")
	return strings.EqualFold(strings.TrimSpace(value), "true")
}

func cliStringArg(args []string, names ...string) string {
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

func waitInterval(ctx context.Context, interval time.Duration) bool {
	if interval <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func normalizeOptions(options Options) Options {
	if options.Stdout == nil {
		options.Stdout = os.Stdout
	}
	if options.Stderr == nil {
		options.Stderr = os.Stderr
	}
	options.NameServer = strings.TrimSpace(options.NameServer)
	options.Transport = strings.ToLower(strings.TrimSpace(options.Transport))
	if options.Transport == "" {
		options.Transport = "auto"
	}
	if options.Timeout <= 0 {
		options.Timeout = 60 * time.Second
	}
	if options.SidecarTimeout <= 0 {
		options.SidecarTimeout = options.Timeout
	}
	return options
}

func parseGlobalFlags(options *Options, args []string) ([]string, int, bool) {
	commandArgs := make([]string, 0, len(args))
	m6ShadowPlan := false
	m6ShadowFixtures := ""
	m6ShadowFixturesFile := ""
	printShadowPlan := func() ([]string, int, bool) {
		if err := printM6ShadowPlan(options.Stdout, m6ShadowFixtures, m6ShadowFixturesFile); err != nil {
			_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
			return nil, 1, true
		}
		return nil, 0, true
	}
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--" {
			if m6ShadowPlan {
				return printShadowPlan()
			}
			commandArgs = append(commandArgs, args[index+1:]...)
			return commandArgs, 0, false
		}
		if !strings.HasPrefix(arg, "--") {
			if m6ShadowPlan {
				return printShadowPlan()
			}
			commandArgs = append(commandArgs, args[index:]...)
			return commandArgs, 0, false
		}
		name, value, hasValue := strings.Cut(arg, "=")
		switch name {
		case "--help":
			printUsage(options.Stderr)
			return nil, 0, true
		case "--m6-shadow-plan":
			m6ShadowPlan = true
		case "--m6-shadow-fixtures":
			if !hasValue {
				index++
				if index >= len(args) {
					_, _ = fmt.Fprintln(options.Stderr, "goadmin: --m6-shadow-fixtures requires a value")
					return nil, 2, true
				}
				value = args[index]
			}
			m6ShadowFixtures = value
		case "--m6-shadow-fixtures-file":
			if !hasValue {
				index++
				if index >= len(args) {
					_, _ = fmt.Fprintln(options.Stderr, "goadmin: --m6-shadow-fixtures-file requires a value")
					return nil, 2, true
				}
				value = args[index]
			}
			m6ShadowFixturesFile = strings.TrimSpace(value)
		case "--transport":
			if !hasValue {
				index++
				if index >= len(args) {
					_, _ = fmt.Fprintln(options.Stderr, "goadmin: --transport requires a value")
					return nil, 2, true
				}
				value = args[index]
			}
			options.Transport = strings.ToLower(strings.TrimSpace(value))
		case "--namesrv":
			if !hasValue {
				index++
				if index >= len(args) {
					_, _ = fmt.Fprintln(options.Stderr, "goadmin: --namesrv requires a value")
					return nil, 2, true
				}
				value = args[index]
			}
			options.NameServer = strings.TrimSpace(value)
		case "--sidecar-addr":
			if !hasValue {
				index++
				if index >= len(args) {
					_, _ = fmt.Fprintln(options.Stderr, "goadmin: --sidecar-addr requires a value")
					return nil, 2, true
				}
				value = args[index]
			}
			options.SidecarAddr = strings.TrimSpace(value)
		case "--timeout-ms":
			if !hasValue {
				index++
				if index >= len(args) {
					_, _ = fmt.Fprintln(options.Stderr, "goadmin: --timeout-ms requires a value")
					return nil, 2, true
				}
				value = args[index]
			}
			millis, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || millis <= 0 {
				_, _ = fmt.Fprintf(options.Stderr, "goadmin: invalid --timeout-ms %q\n", value)
				return nil, 2, true
			}
			options.Timeout = time.Duration(millis) * time.Millisecond
			options.SidecarTimeout = options.Timeout
		default:
			if m6ShadowPlan {
				return printShadowPlan()
			}
			commandArgs = append(commandArgs, args[index:]...)
			return commandArgs, 0, false
		}
	}
	if m6ShadowPlan {
		return printShadowPlan()
	}
	if strings.TrimSpace(m6ShadowFixtures) != "" {
		_, _ = fmt.Fprintln(options.Stderr, "goadmin: --m6-shadow-fixtures requires --m6-shadow-plan")
		return nil, 2, true
	}
	if strings.TrimSpace(m6ShadowFixturesFile) != "" {
		_, _ = fmt.Fprintln(options.Stderr, "goadmin: --m6-shadow-fixtures-file requires --m6-shadow-plan")
		return nil, 2, true
	}
	return commandArgs, 0, false
}

func printM6ShadowPlan(stdout io.Writer, fixturesJSON string, fixturesFile string) error {
	samples := nativeadmin.DefaultM6ShadowPlan()
	resolvedFixturesJSON, err := readM6ShadowFixtures(fixturesJSON, fixturesFile)
	if err != nil {
		return err
	}
	if strings.TrimSpace(resolvedFixturesJSON) != "" {
		var fixtures nativeadmin.ShadowFixtureOverrides
		if err := json.Unmarshal([]byte(resolvedFixturesJSON), &fixtures); err != nil {
			return fmt.Errorf("parse --m6-shadow-fixtures: %w", err)
		}
		merged, err := nativeadmin.ApplyShadowFixtureOverrides(samples, fixtures)
		if err != nil {
			return err
		}
		samples = merged
	}
	line, err := nativeadmin.MarshalShadowBatchPlanJSONLine(nativeadmin.PlanShadowBatch(samples))
	if err != nil {
		return err
	}
	_, err = stdout.Write(line)
	return err
}

func readM6ShadowFixtures(fixturesJSON string, fixturesFile string) (string, error) {
	fixturesJSON = strings.TrimSpace(fixturesJSON)
	fixturesFile = strings.TrimSpace(fixturesFile)
	if fixturesJSON != "" && fixturesFile != "" {
		return "", errors.New("--m6-shadow-fixtures and --m6-shadow-fixtures-file cannot be used together")
	}
	if fixturesFile == "" {
		return fixturesJSON, nil
	}
	data, err := os.ReadFile(fixturesFile)
	if err != nil {
		return "", fmt.Errorf("read --m6-shadow-fixtures-file: %w", err)
	}
	return string(data), nil
}

func runM6ShadowBatchCLI(ctx context.Context, options Options, args []string) int {
	fixturesJSON, fixturesFile, concurrency, err := parseM6ShadowRunFlags(&options, args)
	if err != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 2
	}
	result, err := runM6ShadowBatch(ctx, options, fixturesJSON, fixturesFile, concurrency)
	if err != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 1
	}
	lines, err := nativeadmin.MarshalShadowBatchJSONLines(result)
	if err != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 1
	}
	summary, err := nativeadmin.MarshalShadowBatchSummaryJSONLine(result)
	if err != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 1
	}
	if _, err := options.Stdout.Write(lines); err != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 1
	}
	if _, err := options.Stdout.Write(summary); err != nil {
		_, _ = fmt.Fprintf(options.Stderr, "goadmin: %v\n", err)
		return 1
	}
	if len(result.Errors) > 0 || result.Summary.Mismatches > 0 {
		return 1
	}
	return 0
}

func parseM6ShadowRunFlags(options *Options, args []string) (string, string, int, error) {
	fixturesJSON := ""
	fixturesFile := ""
	concurrency := 1
	seenRun := false
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--" {
			if index+1 < len(args) {
				return "", "", 0, errors.New("--m6-shadow-run does not accept mqadmin command arguments")
			}
			break
		}
		if !strings.HasPrefix(arg, "--") {
			return "", "", 0, fmt.Errorf("--m6-shadow-run does not accept mqadmin command argument %q", arg)
		}
		name, value, hasValue := strings.Cut(arg, "=")
		switch name {
		case "--m6-shadow-run":
			seenRun = true
		case "--m6-shadow-plan":
			return "", "", 0, errors.New("--m6-shadow-run and --m6-shadow-plan cannot be used together")
		case "--m6-shadow-fixtures":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--m6-shadow-fixtures requires a value")
				}
				value = args[index]
			}
			fixturesJSON = value
		case "--m6-shadow-fixtures-file":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--m6-shadow-fixtures-file requires a value")
				}
				value = args[index]
			}
			fixturesFile = strings.TrimSpace(value)
		case "--m6-shadow-concurrency":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--m6-shadow-concurrency requires a value")
				}
				value = args[index]
			}
			parsedConcurrency, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || parsedConcurrency <= 0 {
				return "", "", 0, fmt.Errorf("invalid --m6-shadow-concurrency %q", value)
			}
			concurrency = parsedConcurrency
		case "--transport":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--transport requires a value")
				}
				value = args[index]
			}
			options.Transport = strings.ToLower(strings.TrimSpace(value))
		case "--namesrv":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--namesrv requires a value")
				}
				value = args[index]
			}
			options.NameServer = strings.TrimSpace(value)
		case "--sidecar-addr":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--sidecar-addr requires a value")
				}
				value = args[index]
			}
			options.SidecarAddr = strings.TrimSpace(value)
		case "--timeout-ms":
			if !hasValue {
				index++
				if index >= len(args) {
					return "", "", 0, errors.New("--timeout-ms requires a value")
				}
				value = args[index]
			}
			millis, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || millis <= 0 {
				return "", "", 0, fmt.Errorf("invalid --timeout-ms %q", value)
			}
			options.Timeout = time.Duration(millis) * time.Millisecond
			options.SidecarTimeout = options.Timeout
		default:
			return "", "", 0, fmt.Errorf("unknown --m6-shadow-run flag %q", name)
		}
	}
	if !seenRun {
		return "", "", 0, errors.New("--m6-shadow-run flag is required")
	}
	return fixturesJSON, fixturesFile, concurrency, nil
}

func runM6ShadowBatch(ctx context.Context, options Options, fixturesJSON string, fixturesFile string, concurrency int) (nativeadmin.ShadowBatchResult, error) {
	samples := nativeadmin.DefaultM6ShadowPlan()
	resolvedFixturesJSON, err := readM6ShadowFixtures(fixturesJSON, fixturesFile)
	if err != nil {
		return nativeadmin.ShadowBatchResult{}, err
	}
	if strings.TrimSpace(resolvedFixturesJSON) != "" {
		var fixtures nativeadmin.ShadowFixtureOverrides
		if err := json.Unmarshal([]byte(resolvedFixturesJSON), &fixtures); err != nil {
			return nativeadmin.ShadowBatchResult{}, fmt.Errorf("parse --m6-shadow-fixtures: %w", err)
		}
		merged, err := nativeadmin.ApplyShadowFixtureOverrides(samples, fixtures)
		if err != nil {
			return nativeadmin.ShadowBatchResult{}, err
		}
		samples = merged
	}
	plan := nativeadmin.PlanShadowBatch(samples)
	if len(plan.Errors) > 0 {
		return nativeadmin.ShadowBatchResult{}, plan.Errors[0]
	}
	if plan.ExecutableSamples == 0 {
		return nativeadmin.ShadowBatchResult{}, errors.New("no executable M6 shadow samples; provide --m6-shadow-fixtures or --m6-shadow-fixtures-file")
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	beforeRun := chainM6ShadowHooks(
		m6ShadowWritePermBeforeRun(options),
		m6ShadowNamesrvConfigBeforeRun(options),
		m6ShadowBrokerConfigBeforeRun(options),
		m6ShadowColdDataFlowCtrBeforeRun(options),
		m6ShadowUpdateTopicBeforeRun(options),
		m6ShadowDeleteTopicBeforeRun(options),
		m6ShadowUpdateSubGroupBeforeRun(options),
		m6ShadowDeleteSubGroupBeforeRun(options),
		m6ShadowKVBeforeRun(options),
		m6ShadowUpdateUserBeforeRun(options),
		m6ShadowCreateUserBeforeRun(options),
		m6ShadowCopyUserBeforeRun(options),
		m6ShadowCreateAclBeforeRun(options),
		m6ShadowUpdateAclBeforeRun(options),
		m6ShadowDeleteAclBeforeRun(options),
		m6ShadowListAclBeforeRun(options),
		m6ShadowGetAclBeforeRun(options),
	)
	afterRun := chainM6ShadowHooks(
		m6ShadowGetAclAfterRun(options),
		m6ShadowListAclAfterRun(options),
		m6ShadowDeleteAclAfterRun(options),
		m6ShadowUpdateAclAfterRun(options),
		m6ShadowCreateAclAfterRun(options),
		m6ShadowCopyUserAfterRun(options),
		m6ShadowCreateUserAfterRun(options),
		m6ShadowUpdateUserAfterRun(options),
		m6ShadowKVAfterRun(options),
		m6ShadowDeleteSubGroupAfterRun(options),
		m6ShadowUpdateSubGroupAfterRun(options),
		m6ShadowDeleteTopicAfterRun(options),
		m6ShadowUpdateTopicAfterRun(options),
		m6ShadowColdDataFlowCtrAfterRun(options),
		m6ShadowBrokerConfigAfterRun(options),
		m6ShadowNamesrvConfigAfterRun(options),
		m6ShadowWritePermAfterRun(options),
	)
	batch := nativeadmin.ShadowBatch{
		Primary: nativeadmin.ShadowTarget{
			Name:      string(nativeadmin.ShadowProviderOfficial),
			Runner:    m6ShadowTransportRunner{options: options, transport: "process"},
			BeforeRun: beforeRun,
			AfterRun:  afterRun,
		},
		Targets: []nativeadmin.ShadowTarget{
			{Name: string(nativeadmin.ShadowProviderSidecar), Runner: m6ShadowTransportRunner{options: options, transport: "sidecar"}, BeforeRun: beforeRun, AfterRun: afterRun},
			{Name: string(nativeadmin.ShadowProviderNative), Runner: m6ShadowTransportRunner{options: options, transport: "native"}, BeforeRun: beforeRun, AfterRun: afterRun},
			{Name: string(nativeadmin.ShadowProviderAuto), Runner: m6ShadowTransportRunner{options: options, transport: "auto"}, BeforeRun: beforeRun, AfterRun: afterRun},
		},
		Samples:        samples,
		Normalizer:     nativeadmin.DefaultM6ShadowNormalizer(),
		MaxConcurrency: concurrency,
	}
	return nativeadmin.RunShadowBatch(ctx, batch), nil
}

// chainM6ShadowHooks 将多个样本级 hook 串成一个 hook；每个 hook 自行判断当前命令是否需要动作。
func chainM6ShadowHooks(hooks ...func(context.Context, []string) error) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		for _, hook := range hooks {
			if hook == nil {
				continue
			}
			if err := hook(ctx, append([]string(nil), args...)); err != nil {
				return err
			}
		}
		return nil
	}
}

// m6ShadowWritePermBeforeRun 为写权限类 mutation 命令准备同一初始状态。
func m6ShadowWritePermBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		restoreArgs, ok := m6ShadowWritePermBeforeRunArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowWritePermRestore(ctx, options, args, restoreArgs, "before")
	}
}

// m6ShadowWritePermAfterRun 将写权限类 mutation 命令留下的状态恢复为 broker 可写。
func m6ShadowWritePermAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		restoreArgs, ok := m6ShadowWritePermAfterRunArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowWritePermRestore(ctx, options, args, restoreArgs, "after")
	}
}

// runM6ShadowWritePermRestore 通过注入 runner 或原生 remoting 执行恢复命令，恢复耗时不计入 provider duration。
func runM6ShadowWritePermRestore(ctx context.Context, options Options, originalArgs []string, restoreArgs []string, phase string) error {
	if options.Runner != nil {
		restoreOptions := options
		restoreOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, restoreOptions, restoreArgs)
		if err != nil {
			return fmt.Errorf("restore write permission with %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(restoreArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	restoreArgs = injectNameServer(restoreArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, restoreArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native write permission restore does not support %q", commandName(restoreArgs))
	}
	if err != nil {
		return fmt.Errorf("restore write permission with %s %s %s: %w",
			strings.Join(restoreArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

func m6ShadowWritePermBeforeRunArgs(args []string) ([]string, bool) {
	switch strings.ToLower(commandName(args)) {
	case "wipewriteperm":
		return m6ShadowWritePermArgs(args, "addWritePerm")
	case "addwriteperm":
		return m6ShadowWritePermArgs(args, "wipeWritePerm")
	default:
		return nil, false
	}
}

func m6ShadowWritePermAfterRunArgs(args []string) ([]string, bool) {
	switch strings.ToLower(commandName(args)) {
	case "wipewriteperm", "addwriteperm":
		return m6ShadowWritePermArgs(args, "addWritePerm")
	default:
		return nil, false
	}
}

// m6ShadowWritePermArgs 从原命令复制 namesrv/broker 参数，生成对应的写权限准备或恢复命令。
func m6ShadowWritePermArgs(args []string, command string) ([]string, bool) {
	brokerName := cliStringArg(args[1:], "-b", "--brokerName")
	if strings.TrimSpace(brokerName) == "" {
		return nil, false
	}
	restoreArgs := []string{command}
	if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
		restoreArgs = append(restoreArgs, "-n", nameServers)
	}
	restoreArgs = append(restoreArgs, "-b", brokerName)
	return restoreArgs, true
}

// m6ShadowNamesrvConfigBeforeRun 将 updateNamesrvConfig 样本恢复到固定 baseline，确保每路 provider 从同一动态配置值开始。
func m6ShadowNamesrvConfigBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		resetArgs, ok := m6ShadowNamesrvConfigResetArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowNamesrvConfigCommand(ctx, options, args, resetArgs, "before")
	}
}

// m6ShadowNamesrvConfigAfterRun 将 updateNamesrvConfig 样本写过的隔离 key 恢复到固定 baseline。
func m6ShadowNamesrvConfigAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		resetArgs, ok := m6ShadowNamesrvConfigResetArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowNamesrvConfigCommand(ctx, options, args, resetArgs, "after")
	}
}

// runM6ShadowNamesrvConfigCommand 执行 NameServer 配置恢复命令，恢复耗时不计入 provider duration。
func runM6ShadowNamesrvConfigCommand(ctx context.Context, options Options, originalArgs []string, resetArgs []string, phase string) error {
	if options.Runner != nil {
		resetOptions := options
		resetOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, resetOptions, resetArgs)
		if err != nil {
			return fmt.Errorf("run namesrv config fixture command %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(resetArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	resetArgs = injectNameServer(resetArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, resetArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native namesrv config fixture command does not support %q", commandName(resetArgs))
	}
	if err != nil {
		return fmt.Errorf("run namesrv config fixture command %s %s %s: %w",
			strings.Join(resetArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

func m6ShadowNamesrvConfigResetArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "updateNamesrvConfig") {
		return nil, false
	}
	key := cliStringArg(args[1:], "-k", "--key")
	if strings.TrimSpace(key) == "" {
		return nil, false
	}
	resetArgs := []string{"updateNamesrvConfig"}
	if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
		resetArgs = append(resetArgs, "-n", nameServers)
	}
	resetArgs = append(resetArgs, "-k", key, "-v", "m6-shadow-namesrv-baseline")
	return resetArgs, true
}

// m6ShadowBrokerConfigBeforeRun 将 updateBrokerConfig 样本恢复到固定 baseline，确保每路 provider 从同一 Broker 动态配置值开始。
func m6ShadowBrokerConfigBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		resetArgs, ok := m6ShadowBrokerConfigResetArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowBrokerConfigCommand(ctx, options, args, resetArgs, "before")
	}
}

// m6ShadowBrokerConfigAfterRun 将 updateBrokerConfig 样本写过的隔离 key 恢复到固定 baseline。
func m6ShadowBrokerConfigAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		resetArgs, ok := m6ShadowBrokerConfigResetArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowBrokerConfigCommand(ctx, options, args, resetArgs, "after")
	}
}

// runM6ShadowBrokerConfigCommand 执行 Broker 配置恢复命令，恢复耗时不计入 provider duration。
func runM6ShadowBrokerConfigCommand(ctx context.Context, options Options, originalArgs []string, resetArgs []string, phase string) error {
	if options.Runner != nil {
		resetOptions := options
		resetOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, resetOptions, resetArgs)
		if err != nil {
			return fmt.Errorf("run broker config fixture command %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(resetArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	resetArgs = injectNameServer(resetArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, resetArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native broker config fixture command does not support %q", commandName(resetArgs))
	}
	if err != nil {
		return fmt.Errorf("run broker config fixture command %s %s %s: %w",
			strings.Join(resetArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

func m6ShadowBrokerConfigResetArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "updateBrokerConfig") {
		return nil, false
	}
	brokerAddr := cliStringArg(args[1:], "-b", "--brokerAddr")
	key := cliStringArg(args[1:], "-k", "--key")
	if strings.TrimSpace(brokerAddr) == "" || strings.TrimSpace(key) == "" {
		return nil, false
	}
	resetArgs := []string{"updateBrokerConfig", "-b", brokerAddr, "-k", key, "-v", "m6-shadow-broker-baseline"}
	return resetArgs, true
}

// m6ShadowColdDataFlowCtrBeforeRun 为冷数据流控 mutation 样本准备同一初始状态。
func m6ShadowColdDataFlowCtrBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		fixtureArgs, ok := m6ShadowColdDataFlowCtrBeforeArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowColdDataFlowCtrCommand(ctx, options, args, fixtureArgs, "before")
	}
}

// m6ShadowColdDataFlowCtrAfterRun 清理冷数据流控 mutation 样本留下的目标 group，保持 fixture 可重复执行。
func m6ShadowColdDataFlowCtrAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowColdDataFlowCtrCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowColdDataFlowCtrCommand(ctx, options, args, cleanupArgs, "after")
	}
}

// runM6ShadowColdDataFlowCtrCommand 执行冷数据流控阈值清理命令，清理耗时不计入 provider duration。
func runM6ShadowColdDataFlowCtrCommand(ctx context.Context, options Options, originalArgs []string, cleanupArgs []string, phase string) error {
	if options.Runner != nil {
		cleanupOptions := options
		cleanupOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, cleanupOptions, cleanupArgs)
		if err != nil {
			return fmt.Errorf("run cold data flow ctr fixture command %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(cleanupArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	cleanupArgs = injectNameServer(cleanupArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, cleanupArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native cold data flow ctr fixture command does not support %q", commandName(cleanupArgs))
	}
	if err != nil {
		return fmt.Errorf("run cold data flow ctr fixture command %s %s %s: %w",
			strings.Join(cleanupArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

func m6ShadowColdDataFlowCtrBeforeArgs(args []string) ([]string, bool) {
	switch strings.ToLower(commandName(args)) {
	case "updatecolddataflowctrgroupconfig":
		return m6ShadowColdDataFlowCtrCleanupArgs(args)
	case "removecolddataflowctrgroupconfig":
		return m6ShadowColdDataFlowCtrPrepareArgs(args)
	default:
		return nil, false
	}
}

func m6ShadowColdDataFlowCtrCleanupArgs(args []string) ([]string, bool) {
	switch strings.ToLower(commandName(args)) {
	case "updatecolddataflowctrgroupconfig", "removecolddataflowctrgroupconfig":
	default:
		return nil, false
	}
	consumerGroup := cliStringArg(args[1:], "-g", "--consumerGroup")
	if strings.TrimSpace(consumerGroup) == "" {
		return nil, false
	}
	cleanupArgs := []string{"removeColdDataFlowCtrGroupConfig"}
	brokerAddr := cliStringArg(args[1:], "-b", "--brokerAddr")
	clusterName := cliStringArg(args[1:], "-c", "--clusterName")
	switch {
	case strings.TrimSpace(brokerAddr) != "":
		cleanupArgs = append(cleanupArgs, "-b", brokerAddr)
	case strings.TrimSpace(clusterName) != "":
		if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
			cleanupArgs = append(cleanupArgs, "-n", nameServers)
		}
		cleanupArgs = append(cleanupArgs, "-c", clusterName)
	default:
		return nil, false
	}
	cleanupArgs = append(cleanupArgs, "-g", consumerGroup)
	return cleanupArgs, true
}

func m6ShadowColdDataFlowCtrPrepareArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "removeColdDataFlowCtrGroupConfig") {
		return nil, false
	}
	consumerGroup := cliStringArg(args[1:], "-g", "--consumerGroup")
	if strings.TrimSpace(consumerGroup) == "" {
		return nil, false
	}
	prepareArgs := []string{"updateColdDataFlowCtrGroupConfig"}
	brokerAddr := cliStringArg(args[1:], "-b", "--brokerAddr")
	clusterName := cliStringArg(args[1:], "-c", "--clusterName")
	switch {
	case strings.TrimSpace(brokerAddr) != "":
		prepareArgs = append(prepareArgs, "-b", brokerAddr)
	case strings.TrimSpace(clusterName) != "":
		if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
			prepareArgs = append(prepareArgs, "-n", nameServers)
		}
		prepareArgs = append(prepareArgs, "-c", clusterName)
	default:
		return nil, false
	}
	prepareArgs = append(prepareArgs, "-g", consumerGroup, "-v", "m6-shadow-cold-flow-threshold")
	return prepareArgs, true
}

// m6ShadowDeleteTopicBeforeRun 为 deleteTopic 样本预置同一 Topic，确保每路 provider 都删除真实存在的 Topic。
func m6ShadowDeleteTopicBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		prepareArgs, ok := m6ShadowDeleteTopicPrepareArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowTopicFixtureCommand(ctx, options, args, prepareArgs, "before")
	}
}

// m6ShadowDeleteTopicAfterRun 清理 deleteTopic 样本可能留下的 Topic 元数据，保持 fixture 可重复执行。
func m6ShadowDeleteTopicAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowDeleteTopicCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowTopicFixtureCommand(ctx, options, args, cleanupArgs, "after")
	}
}

// m6ShadowUpdateTopicBeforeRun 清理 updateTopic 目标 Topic，确保每路 provider 都执行真实创建或更新路径。
func m6ShadowUpdateTopicBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowUpdateTopicCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowTopicFixtureCommand(ctx, options, args, cleanupArgs, "before")
	}
}

// m6ShadowUpdateTopicAfterRun 清理 updateTopic 写入的 Topic 元数据，保持 fixture 可重复执行。
func m6ShadowUpdateTopicAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowUpdateTopicCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowTopicFixtureCommand(ctx, options, args, cleanupArgs, "after")
	}
}

// m6ShadowUpdateSubGroupBeforeRun 清理 updateSubGroup 目标订阅组，确保每路 provider 都执行真实创建或更新路径。
func m6ShadowUpdateSubGroupBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowUpdateSubGroupCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowSubGroupFixtureCommand(ctx, options, args, cleanupArgs, "before")
	}
}

// m6ShadowUpdateSubGroupAfterRun 清理 updateSubGroup 写入的订阅组元数据，保持 fixture 可重复执行。
func m6ShadowUpdateSubGroupAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowUpdateSubGroupCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowSubGroupFixtureCommand(ctx, options, args, cleanupArgs, "after")
	}
}

// m6ShadowDeleteSubGroupBeforeRun 为 deleteSubGroup 样本预置订阅组，确保每路 provider 都删除真实存在的 group。
func m6ShadowDeleteSubGroupBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		prepareArgs, ok := m6ShadowDeleteSubGroupPrepareArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowSubGroupFixtureCommand(ctx, options, args, prepareArgs, "before")
	}
}

// m6ShadowDeleteSubGroupAfterRun 清理 deleteSubGroup 样本可能留下的订阅组元数据，保持 fixture 可重复执行。
func m6ShadowDeleteSubGroupAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowDeleteSubGroupCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowSubGroupFixtureCommand(ctx, options, args, cleanupArgs, "after")
	}
}

// runM6ShadowSubGroupFixtureCommand 执行订阅组 mutation 的准备或清理命令，耗时不计入 provider duration。
func runM6ShadowSubGroupFixtureCommand(ctx context.Context, options Options, originalArgs []string, fixtureArgs []string, phase string) error {
	if options.Runner != nil {
		fixtureOptions := options
		fixtureOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, fixtureOptions, fixtureArgs)
		if err != nil {
			return fmt.Errorf("run subscription group fixture command %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(fixtureArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	fixtureArgs = injectNameServer(fixtureArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, fixtureArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native subscription group fixture command does not support %q", commandName(fixtureArgs))
	}
	if err != nil {
		return fmt.Errorf("run subscription group fixture command %s %s %s: %w",
			strings.Join(fixtureArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

// runM6ShadowTopicFixtureCommand 执行 Topic mutation 的准备或清理命令，耗时不计入 provider duration。
func runM6ShadowTopicFixtureCommand(ctx context.Context, options Options, originalArgs []string, fixtureArgs []string, phase string) error {
	if options.Runner != nil {
		fixtureOptions := options
		fixtureOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, fixtureOptions, fixtureArgs)
		if err != nil {
			return fmt.Errorf("run topic fixture command %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(fixtureArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	fixtureArgs = injectNameServer(fixtureArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, fixtureArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native topic fixture command does not support %q", commandName(fixtureArgs))
	}
	if err != nil {
		return fmt.Errorf("run topic fixture command %s %s %s: %w",
			strings.Join(fixtureArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

func m6ShadowDeleteTopicPrepareArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteTopic") {
		return nil, false
	}
	return m6ShadowDeleteTopicArgs(args, "updateTopic")
}

func m6ShadowDeleteTopicCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteTopic") {
		return nil, false
	}
	return m6ShadowDeleteTopicArgs(args, "deleteTopic")
}

func m6ShadowUpdateTopicCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "updateTopic") {
		return nil, false
	}
	return m6ShadowDeleteTopicArgs(args, "deleteTopic")
}

func m6ShadowUpdateSubGroupCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "updateSubGroup") {
		return nil, false
	}
	return m6ShadowSubGroupArgs(args, "deleteSubGroup")
}

func m6ShadowDeleteSubGroupPrepareArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteSubGroup") {
		return nil, false
	}
	return m6ShadowSubGroupArgs(args, "updateSubGroup")
}

func m6ShadowDeleteSubGroupCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteSubGroup") {
		return nil, false
	}
	return m6ShadowSubGroupArgs(args, "deleteSubGroup")
}

// m6ShadowSubGroupArgs 从订阅组原命令复制 namesrv、broker/cluster 和 group，生成准备或清理命令。
func m6ShadowSubGroupArgs(args []string, command string) ([]string, bool) {
	groupName := cliStringArg(args[1:], "-g", "--groupName")
	if strings.TrimSpace(groupName) == "" {
		return nil, false
	}
	subGroupArgs := []string{command}
	brokerAddr := cliStringArg(args[1:], "-b", "--brokerAddr")
	clusterName := cliStringArg(args[1:], "-c", "--clusterName")
	switch {
	case strings.TrimSpace(brokerAddr) != "":
		subGroupArgs = append(subGroupArgs, "-b", brokerAddr)
	case strings.TrimSpace(clusterName) != "":
		if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
			subGroupArgs = append(subGroupArgs, "-n", nameServers)
		}
		subGroupArgs = append(subGroupArgs, "-c", clusterName)
	default:
		return nil, false
	}
	subGroupArgs = append(subGroupArgs, "-g", groupName)
	return subGroupArgs, true
}

// m6ShadowDeleteTopicArgs 从 deleteTopic 原命令复制 namesrv、cluster 和 topic，生成 topic 准备或清理命令。
func m6ShadowDeleteTopicArgs(args []string, command string) ([]string, bool) {
	clusterName := cliStringArg(args[1:], "-c", "--clusterName", "--cluster")
	topic := cliStringArg(args[1:], "-t", "--topic")
	if strings.TrimSpace(clusterName) == "" || strings.TrimSpace(topic) == "" {
		return nil, false
	}
	topicArgs := []string{command}
	if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
		topicArgs = append(topicArgs, "-n", nameServers)
	}
	topicArgs = append(topicArgs, "-c", clusterName, "-t", topic)
	return topicArgs, true
}

// m6ShadowKVBeforeRun 为 deleteKvConfig 样本预置同一 namespace/key，确保每路 provider 都删除真实存在的 KV。
func m6ShadowKVBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		prepareArgs, ok := m6ShadowDeleteKVPrepareArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowKVCommand(ctx, options, args, prepareArgs, "before")
	}
}

// m6ShadowKVAfterRun 清理 deleteKvConfig 样本留下的 namespace/key 状态，恢复隔离 fixture。
func m6ShadowKVAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowKVDeleteArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowKVCommand(ctx, options, args, cleanupArgs, "after")
	}
}

// runM6ShadowKVCommand 通过注入 runner 或原生 remoting 执行 KV 准备/清理命令，耗时不计入 provider duration。
func runM6ShadowKVCommand(ctx context.Context, options Options, originalArgs []string, kvArgs []string, phase string) error {
	if options.Runner != nil {
		kvOptions := options
		kvOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, kvOptions, kvArgs)
		if err != nil {
			return fmt.Errorf("run KV fixture command %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(kvArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	kvArgs = injectNameServer(kvArgs, options.NameServer)
	_, supported, err := nativeCommandRunner(ctx, kvArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native KV fixture command does not support %q", commandName(kvArgs))
	}
	if err != nil {
		return fmt.Errorf("run KV fixture command %s %s %s: %w",
			strings.Join(kvArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

func m6ShadowDeleteKVPrepareArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteKvConfig") {
		return nil, false
	}
	return m6ShadowKVArgs(args, "updateKvConfig", "m6-shadow-delete-kv-prepare")
}

func m6ShadowKVDeleteArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteKvConfig") {
		return nil, false
	}
	return m6ShadowKVArgs(args, "deleteKvConfig", "")
}

// m6ShadowKVArgs 从原 KV 命令复制 namesrv/namespace/key，并按需要追加 value 参数。
func m6ShadowKVArgs(args []string, command string, value string) ([]string, bool) {
	namespace := cliStringArg(args[1:], "-s", "--namespace")
	key := cliStringArg(args[1:], "-k", "--key")
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(key) == "" {
		return nil, false
	}
	kvArgs := []string{command}
	if nameServers := cliStringArg(args[1:], "-n", "--namesrvAddr"); strings.TrimSpace(nameServers) != "" {
		kvArgs = append(kvArgs, "-n", nameServers)
	}
	kvArgs = append(kvArgs, "-s", namespace, "-k", key)
	if value != "" {
		kvArgs = append(kvArgs, "-v", value)
	}
	return kvArgs, true
}

// m6ShadowUpdateUserBeforeRun 为 updateUser 样本恢复目标用户状态，确保每路 provider 都从 enable baseline 更新。
func m6ShadowUpdateUserBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		resetArgs, ok := m6ShadowUpdateUserResetArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAuthUserCleanup(ctx, options, args, resetArgs, "before")
	}
}

// m6ShadowUpdateUserAfterRun 将 updateUser 样本修改过的目标用户状态恢复为 enable。
func m6ShadowUpdateUserAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		resetArgs, ok := m6ShadowUpdateUserResetArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAuthUserCleanup(ctx, options, args, resetArgs, "after")
	}
}

// m6ShadowCreateUserBeforeRun 为 createUser 样本删除目标用户，确保每路 provider 都执行真实创建路径。
func m6ShadowCreateUserBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowCreateUserCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAuthUserCleanup(ctx, options, args, cleanupArgs, "before")
	}
}

// m6ShadowCreateUserAfterRun 清理 createUser 留在目标 broker 的用户元数据，保持 fixture 可重复执行。
func m6ShadowCreateUserAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowCreateUserCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAuthUserCleanup(ctx, options, args, cleanupArgs, "after")
	}
}

// m6ShadowCopyUserBeforeRun 为 copyUser 样本删除目标用户，确保每路 provider 都执行 create 路径。
func m6ShadowCopyUserBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowCopyUserCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAuthUserCleanup(ctx, options, args, cleanupArgs, "before")
	}
}

// m6ShadowCopyUserAfterRun 清理 copyUser 留在目标 broker 的用户元数据，保持 fixture 可重复执行。
func m6ShadowCopyUserAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		cleanupArgs, ok := m6ShadowCopyUserCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAuthUserCleanup(ctx, options, args, cleanupArgs, "after")
	}
}

// m6ShadowCreateAclBeforeRun 为 createAcl 样本创建目标 User subject，确保官方 createAcl 走真实写入路径。
func m6ShadowCreateAclBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteUserArgs, createUserArgs, ok := m6ShadowCreateAclPrepareArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "before"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, createUserArgs, "before")
	}
}

// m6ShadowCreateAclAfterRun 清理 createAcl 写入的 ACL 与 seed 用户，保持每路 provider 初始状态一致。
func m6ShadowCreateAclAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteAclArgs, deleteUserArgs, ok := m6ShadowCreateAclCleanupArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteAclArgs, "after"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "after")
	}
}

// m6ShadowUpdateAclBeforeRun 为 updateAcl 样本重建 User subject 与 Pub/Allow ACL baseline，确保 provider 执行真实更新。
func m6ShadowUpdateAclBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteUserArgs, createUserArgs, createAclArgs, ok := m6ShadowUpdateAclPrepareArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "before"); err != nil {
			return err
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, createUserArgs, "before"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, createAclArgs, "before")
	}
}

// m6ShadowUpdateAclAfterRun 清理 updateAcl 样本写入的 ACL 与 seed 用户，避免后续 provider 复用脏状态。
func m6ShadowUpdateAclAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteAclArgs, deleteUserArgs, ok := m6ShadowUpdateAclCleanupArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteAclArgs, "after"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "after")
	}
}

// m6ShadowDeleteAclBeforeRun 为 deleteAcl 样本重建 User subject 与 Pub/Allow ACL baseline，确保 provider 删除真实 ACL。
func m6ShadowDeleteAclBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteUserArgs, createUserArgs, createAclArgs, ok := m6ShadowDeleteAclPrepareArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "before"); err != nil {
			return err
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, createUserArgs, "before"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, createAclArgs, "before")
	}
}

// m6ShadowDeleteAclAfterRun 清理 deleteAcl 样本使用的 seed 用户；目标 ACL 已由 provider 删除。
func m6ShadowDeleteAclAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteUserArgs, ok := m6ShadowDeleteAclCleanupArgs(args)
		if !ok {
			return nil
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "after")
	}
}

// m6ShadowListAclBeforeRun 为 listAcl 样本重建 User subject 与 Pub/Allow ACL baseline，确保只读查询有稳定真实数据。
func m6ShadowListAclBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteUserArgs, createUserArgs, createAclArgs, ok := m6ShadowListAclPrepareArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "before"); err != nil {
			return err
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, createUserArgs, "before"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, createAclArgs, "before")
	}
}

// m6ShadowListAclAfterRun 清理 listAcl 样本读取过的 ACL baseline 与 seed 用户，保持 fixture 可重复执行。
func m6ShadowListAclAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteAclArgs, deleteUserArgs, ok := m6ShadowListAclCleanupArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteAclArgs, "after"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "after")
	}
}

// m6ShadowGetAclBeforeRun 为 getAcl 样本重建 User subject 与 Pub/Allow ACL baseline，确保详情查询读取真实 ACL。
func m6ShadowGetAclBeforeRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteUserArgs, createUserArgs, createAclArgs, ok := m6ShadowGetAclPrepareArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "before"); err != nil {
			return err
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, createUserArgs, "before"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, createAclArgs, "before")
	}
}

// m6ShadowGetAclAfterRun 清理 getAcl 样本读取过的 ACL baseline 与 seed 用户，保持 fixture 可重复执行。
func m6ShadowGetAclAfterRun(options Options) func(context.Context, []string) error {
	return func(ctx context.Context, args []string) error {
		deleteAclArgs, deleteUserArgs, ok := m6ShadowGetAclCleanupArgs(args)
		if !ok {
			return nil
		}
		if err := runM6ShadowAclFixtureCommand(ctx, options, args, deleteAclArgs, "after"); err != nil {
			return err
		}
		return runM6ShadowAclFixtureCommand(ctx, options, args, deleteUserArgs, "after")
	}
}

// runM6ShadowAuthUserCleanup 通过注入 runner 或原生 remoting 删除目标用户，耗时不计入 provider duration。
func runM6ShadowAuthUserCleanup(ctx context.Context, options Options, originalArgs []string, cleanupArgs []string, phase string) error {
	if options.Runner != nil {
		cleanupOptions := options
		cleanupOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, cleanupOptions, cleanupArgs)
		if err != nil {
			return fmt.Errorf("cleanup auth user fixture with %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(cleanupArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	_, supported, err := nativeCommandRunner(ctx, cleanupArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native auth user cleanup does not support %q", commandName(cleanupArgs))
	}
	if err != nil {
		return fmt.Errorf("cleanup auth user fixture with %s %s %s: %w",
			strings.Join(cleanupArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

// runM6ShadowAclFixtureCommand 执行 createAcl 样本的用户预置和 ACL 清理命令，耗时不计入 provider duration。
func runM6ShadowAclFixtureCommand(ctx context.Context, options Options, originalArgs []string, fixtureArgs []string, phase string) error {
	if options.Runner != nil {
		fixtureOptions := options
		fixtureOptions.Transport = "process"
		output, err := runM6ShadowCommand(ctx, fixtureOptions, fixtureArgs)
		if err != nil {
			return fmt.Errorf("run acl fixture with %s %s %s: stdout=%q stderr=%q: %w",
				strings.Join(fixtureArgs, " "), phase, commandName(originalArgs), output.Stdout, output.Stderr, err)
		}
		return nil
	}
	_, supported, err := nativeCommandRunner(ctx, fixtureArgs, options.Timeout)
	if !supported {
		return fmt.Errorf("native acl fixture command does not support %q", commandName(fixtureArgs))
	}
	if err != nil {
		return fmt.Errorf("run acl fixture with %s %s %s: %w",
			strings.Join(fixtureArgs, " "), phase, commandName(originalArgs), err)
	}
	return nil
}

// m6ShadowUpdateUserResetArgs 从 updateUser 原命令复制目标 broker 和用户名，生成 userStatus baseline 恢复命令。
func m6ShadowUpdateUserResetArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "updateUser") {
		return nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	username := cliStringArg(args[1:], "-u", "--username")
	if strings.TrimSpace(targetBroker) == "" || strings.TrimSpace(username) == "" {
		return nil, false
	}
	resetArgs := []string{"updateUser", "-b", targetBroker, "-u", username, "-s", "enable"}
	return resetArgs, true
}

// m6ShadowCreateUserCleanupArgs 从 createUser 原命令复制目标 broker 和用户名，生成 deleteUser 清理命令。
func m6ShadowCreateUserCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "createUser") {
		return nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	username := cliStringArg(args[1:], "-u", "--username")
	if strings.TrimSpace(targetBroker) == "" || strings.TrimSpace(username) == "" {
		return nil, false
	}
	cleanupArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	return cleanupArgs, true
}

// m6ShadowCopyUserCleanupArgs 从 copyUser 原命令复制目标 broker 和用户名，生成 deleteUser 清理命令。
func m6ShadowCopyUserCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "copyUser") {
		return nil, false
	}
	targetBroker := cliStringArg(args[1:], "-t", "--toBroker")
	usernames := cliStringArg(args[1:], "-u", "--usernames")
	if strings.TrimSpace(targetBroker) == "" || strings.TrimSpace(usernames) == "" {
		return nil, false
	}
	cleanupArgs := []string{"deleteUser", "-b", targetBroker, "-u", usernames}
	return cleanupArgs, true
}

// m6ShadowCreateAclPrepareArgs 从 createAcl 原命令派生 seed 用户重建命令。
func m6ShadowCreateAclPrepareArgs(args []string) ([]string, []string, bool) {
	if !strings.EqualFold(commandName(args), "createAcl") {
		return nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	username, ok := m6ShadowCreateAclUsername(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok {
		return nil, nil, false
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	createUserArgs := []string{"createUser", "-b", targetBroker, "-u", username, "-p", "m6-shadow-acl-pass", "-t", "Super"}
	return deleteUserArgs, createUserArgs, true
}

// m6ShadowCreateAclCleanupArgs 从 createAcl 原命令派生 ACL 与 seed 用户清理命令。
func m6ShadowCreateAclCleanupArgs(args []string) ([]string, []string, bool) {
	if !strings.EqualFold(commandName(args), "createAcl") {
		return nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	resource := cliStringArg(args[1:], "-r", "--resources")
	username, ok := m6ShadowCreateAclUsername(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok {
		return nil, nil, false
	}
	deleteAclArgs := []string{"deleteAcl", "-b", targetBroker, "-s", subject}
	if strings.TrimSpace(resource) != "" {
		deleteAclArgs = append(deleteAclArgs, "-r", resource)
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	return deleteAclArgs, deleteUserArgs, true
}

// m6ShadowUpdateAclPrepareArgs 从 updateAcl 原命令派生 seed 用户与 ACL baseline 命令。
func m6ShadowUpdateAclPrepareArgs(args []string) ([]string, []string, []string, bool) {
	if !strings.EqualFold(commandName(args), "updateAcl") {
		return nil, nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	resource := cliStringArg(args[1:], "-r", "--resources")
	sourceIP := cliStringArg(args[1:], "-i", "--sourceIp")
	username, ok := m6ShadowCreateAclUsername(subject)
	if strings.TrimSpace(targetBroker) == "" || strings.TrimSpace(resource) == "" || !ok {
		return nil, nil, nil, false
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	createUserArgs := []string{"createUser", "-b", targetBroker, "-u", username, "-p", "m6-shadow-acl-pass", "-t", "Super"}
	createAclArgs := []string{"createAcl", "-b", targetBroker, "-s", subject, "-r", resource, "-a", "Pub", "-d", "Allow"}
	if strings.TrimSpace(sourceIP) != "" {
		createAclArgs = append(createAclArgs, "-i", sourceIP)
	}
	return deleteUserArgs, createUserArgs, createAclArgs, true
}

// m6ShadowUpdateAclCleanupArgs 从 updateAcl 原命令派生 ACL 与 seed 用户清理命令。
func m6ShadowUpdateAclCleanupArgs(args []string) ([]string, []string, bool) {
	if !strings.EqualFold(commandName(args), "updateAcl") {
		return nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	resource := cliStringArg(args[1:], "-r", "--resources")
	username, ok := m6ShadowCreateAclUsername(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok {
		return nil, nil, false
	}
	deleteAclArgs := []string{"deleteAcl", "-b", targetBroker, "-s", subject}
	if strings.TrimSpace(resource) != "" {
		deleteAclArgs = append(deleteAclArgs, "-r", resource)
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	return deleteAclArgs, deleteUserArgs, true
}

// m6ShadowDeleteAclPrepareArgs 从 deleteAcl 原命令派生 seed 用户与 ACL baseline 命令。
func m6ShadowDeleteAclPrepareArgs(args []string) ([]string, []string, []string, bool) {
	if !strings.EqualFold(commandName(args), "deleteAcl") {
		return nil, nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	resource := cliStringArg(args[1:], "-r", "--resources")
	username, ok := m6ShadowCreateAclUsername(subject)
	if strings.TrimSpace(targetBroker) == "" || strings.TrimSpace(resource) == "" || !ok {
		return nil, nil, nil, false
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	createUserArgs := []string{"createUser", "-b", targetBroker, "-u", username, "-p", "m6-shadow-acl-pass", "-t", "Super"}
	createAclArgs := []string{"createAcl", "-b", targetBroker, "-s", subject, "-r", resource, "-a", "Pub", "-d", "Allow"}
	return deleteUserArgs, createUserArgs, createAclArgs, true
}

// m6ShadowDeleteAclCleanupArgs 从 deleteAcl 原命令派生 seed 用户清理命令。
func m6ShadowDeleteAclCleanupArgs(args []string) ([]string, bool) {
	if !strings.EqualFold(commandName(args), "deleteAcl") {
		return nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	username, ok := m6ShadowCreateAclUsername(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok {
		return nil, false
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	return deleteUserArgs, true
}

// m6ShadowListAclPrepareArgs 从 listAcl 原命令派生 seed 用户与可查询 ACL baseline 命令。
func m6ShadowListAclPrepareArgs(args []string) ([]string, []string, []string, bool) {
	if !strings.EqualFold(commandName(args), "listAcl") {
		return nil, nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	username, ok := m6ShadowCreateAclUsername(subject)
	resource, resourceOK := m6ShadowListAclResource(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok || !resourceOK {
		return nil, nil, nil, false
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	createUserArgs := []string{"createUser", "-b", targetBroker, "-u", username, "-p", "m6-shadow-acl-pass", "-t", "Super"}
	createAclArgs := []string{"createAcl", "-b", targetBroker, "-s", subject, "-r", resource, "-a", "Pub", "-d", "Allow"}
	return deleteUserArgs, createUserArgs, createAclArgs, true
}

// m6ShadowListAclCleanupArgs 从 listAcl 原命令派生 ACL 与 seed 用户清理命令。
func m6ShadowListAclCleanupArgs(args []string) ([]string, []string, bool) {
	if !strings.EqualFold(commandName(args), "listAcl") {
		return nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	username, ok := m6ShadowCreateAclUsername(subject)
	resource, resourceOK := m6ShadowListAclResource(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok || !resourceOK {
		return nil, nil, false
	}
	deleteAclArgs := []string{"deleteAcl", "-b", targetBroker, "-s", subject, "-r", resource}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	return deleteAclArgs, deleteUserArgs, true
}

// m6ShadowGetAclPrepareArgs 从 getAcl 原命令派生 seed 用户与可查询 ACL baseline 命令。
func m6ShadowGetAclPrepareArgs(args []string) ([]string, []string, []string, bool) {
	if !strings.EqualFold(commandName(args), "getAcl") {
		return nil, nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	username, ok := m6ShadowCreateAclUsername(subject)
	resource, resourceOK := m6ShadowListAclResource(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok || !resourceOK {
		return nil, nil, nil, false
	}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	createUserArgs := []string{"createUser", "-b", targetBroker, "-u", username, "-p", "m6-shadow-acl-pass", "-t", "Super"}
	createAclArgs := []string{"createAcl", "-b", targetBroker, "-s", subject, "-r", resource, "-a", "Pub", "-d", "Allow"}
	return deleteUserArgs, createUserArgs, createAclArgs, true
}

// m6ShadowGetAclCleanupArgs 从 getAcl 原命令派生 ACL 与 seed 用户清理命令。
func m6ShadowGetAclCleanupArgs(args []string) ([]string, []string, bool) {
	if !strings.EqualFold(commandName(args), "getAcl") {
		return nil, nil, false
	}
	targetBroker := cliStringArg(args[1:], "-b", "--brokerAddr")
	subject := cliStringArg(args[1:], "-s", "--subject")
	username, ok := m6ShadowCreateAclUsername(subject)
	resource, resourceOK := m6ShadowListAclResource(subject)
	if strings.TrimSpace(targetBroker) == "" || !ok || !resourceOK {
		return nil, nil, false
	}
	deleteAclArgs := []string{"deleteAcl", "-b", targetBroker, "-s", subject, "-r", resource}
	deleteUserArgs := []string{"deleteUser", "-b", targetBroker, "-u", username}
	return deleteAclArgs, deleteUserArgs, true
}

// m6ShadowListAclResource 为 listAcl 样本从 seed 用户名推导确定的 Topic resource，避免给官方命令增加非官方参数。
func m6ShadowListAclResource(subject string) (string, bool) {
	username, ok := m6ShadowCreateAclUsername(subject)
	if !ok {
		return "", false
	}
	resourceName := strings.TrimSpace(username)
	switch {
	case strings.HasSuffix(resourceName, "_user"):
		resourceName = strings.TrimSuffix(resourceName, "_user") + "_topic"
	case strings.HasSuffix(resourceName, "-user"):
		resourceName = strings.TrimSuffix(resourceName, "-user") + "-topic"
	default:
		resourceName += "-topic"
	}
	return "Topic:" + resourceName, true
}

// m6ShadowCreateAclUsername 只接受官方 ACL subject 的 User:<name> 形式，避免为非用户 subject 伪造 fixture。
func m6ShadowCreateAclUsername(subject string) (string, bool) {
	prefix, username, found := strings.Cut(strings.TrimSpace(subject), ":")
	if !found || !strings.EqualFold(strings.TrimSpace(prefix), "User") {
		return "", false
	}
	username = strings.TrimSpace(username)
	if username == "" {
		return "", false
	}
	return username, true
}

type m6ShadowTransportRunner struct {
	// options 是当前 CLI 配置副本，RunShadow 会按 transport 覆盖执行路径。
	options Options
	// transport 是本 runner 固定使用的 provider 路径名称。
	transport string
}

// RunShadow 复用普通 goadmin 命令执行链路，确保 shadow batch 和单命令 CLI 的解析、NameServer 注入一致。
func (runner m6ShadowTransportRunner) RunShadow(ctx context.Context, args []string) (nativeadmin.ShadowOutput, error) {
	options := runner.options
	options.Transport = runner.transport
	return runM6ShadowCommand(ctx, options, args)
}

func runM6ShadowCommand(ctx context.Context, options Options, args []string) (nativeadmin.ShadowOutput, error) {
	if isMessageChainPseudoCommand(args) {
		commandArgs := injectNameServer(args, options.NameServer)
		output, err := runMessageChainPseudoCommand(ctx, options, commandArgs)
		return nativeadmin.ShadowOutput{Stdout: output}, err
	}
	if output, err := officialParserPreflight(args); err != nil {
		return shadowOutputForParserError(output, err), err
	}
	commandArgs := injectNameServer(args, options.NameServer)
	if output, err := officialParserPreflight(commandArgs); err != nil {
		return shadowOutputForParserError(output, err), err
	}
	output, err := runCommand(ctx, options, commandArgs)
	shadowOutput := nativeadmin.ShadowOutput{Stdout: output}
	if err != nil {
		return shadowOutput, err
	}
	artifacts, artifactErr := m6ShadowArtifacts(commandArgs)
	shadowOutput.Artifacts = artifacts
	return shadowOutput, artifactErr
}

func shadowOutputForParserError(output string, err error) nativeadmin.ShadowOutput {
	var parserErr *nativeadmin.OfficialParserError
	if errors.As(err, &parserErr) {
		return nativeadmin.ShadowOutput{Stdout: output, Stderr: parserErr.Stderr}
	}
	return nativeadmin.ShadowOutput{Stdout: output}
}

func m6ShadowArtifacts(args []string) (map[string]string, error) {
	if len(args) == 0 {
		return nil, nil
	}
	command := strings.ToLower(strings.TrimSpace(args[0]))
	filePath := cliStringArg(args[1:], "-f", "--filePath")
	if strings.TrimSpace(filePath) == "" {
		filePath = "/tmp/rocketmq/export"
	}
	switch command {
	case "exportconfigs":
		return m6ShadowFileArtifact(filePath, "configs.json", "exportConfigs")
	case "exportmetadata":
		fileName := "metadata.json"
		if hasFlagValue(args[1:], "-t", "--topic") {
			fileName = "topic.json"
		} else if hasFlagValue(args[1:], "-g", "--subscriptionGroup") {
			fileName = "subscriptionGroup.json"
		}
		return m6ShadowFileArtifact(filePath, fileName, "exportMetadata")
	case "exportmetrics":
		return m6ShadowFileArtifact(filePath, "metrics.json", "exportMetrics")
	default:
		return nil, nil
	}
}

func m6ShadowFileArtifact(filePath string, fileName string, command string) (map[string]string, error) {
	content, err := os.ReadFile(filepath.Join(filePath, fileName))
	if err != nil {
		return nil, fmt.Errorf("read %s artifact %s: %w", command, fileName, err)
	}
	return map[string]string{fileName: string(content)}, nil
}

// isMessageChainPseudoCommand 判断 M6 shadow 内部组合命令；官方 mqadmin 没有该子命令，不能走官方 parser 预检。
func isMessageChainPseudoCommand(args []string) bool {
	return len(args) > 0 && strings.EqualFold(strings.TrimSpace(args[0]), "messageChain")
}

// runMessageChainPseudoCommand 将 M6 messageChain 样本展开为 typed provider 调用，并输出稳定 JSON 供四路 provider 严格对比。
func runMessageChainPseudoCommand(ctx context.Context, options Options, args []string) (string, error) {
	query, nameServer, err := parseMessageChainPseudoArgs(options, args)
	if err != nil {
		return "", err
	}
	provider := &rocketmq.MQAdminProvider{
		NameServer:      nameServer,
		CommandRunner:   messageChainTransportRunner{options: options},
		MessageCacheTTL: options.Timeout,
	}
	chain, err := provider.MessageChain(ctx, query)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(chain)
	if err != nil {
		return "", fmt.Errorf("序列化 messageChain 输出失败: %w", err)
	}
	return string(payload) + "\n", nil
}

// messageChainTransportRunner 把 MessageChain 编排产生的底层 mqadmin 子命令交回当前 shadow transport 执行。
type messageChainTransportRunner struct {
	// options 保存当前 provider path 的 transport、timeout、sidecar 和测试 runner 配置。
	options Options
}

func (runner messageChainTransportRunner) Run(ctx context.Context, args ...string) (string, error) {
	return runCommand(ctx, runner.options, append([]string(nil), args...))
}

func parseMessageChainPseudoArgs(options Options, args []string) (rocketmq.MessageQuery, string, error) {
	if !isMessageChainPseudoCommand(args) {
		return rocketmq.MessageQuery{}, "", fmt.Errorf("messageChain pseudo command expected, got %q", commandName(args))
	}
	commandArgs := args[1:]
	nameServer := cliStringArg(commandArgs, "-n", "--namesrvAddr")
	if nameServer == "" {
		nameServer = options.NameServer
	}
	query := rocketmq.MessageQuery{
		Topic:         cliStringArg(commandArgs, "-t", "--topic"),
		MessageID:     cliStringArg(commandArgs, "-i", "--msgId", "--messageId", "--messageID"),
		Key:           cliStringArg(commandArgs, "-k", "--key"),
		ConsumerGroup: cliStringArg(commandArgs, "-g", "--consumerGroup", "--groupName"),
		TraceTopic:    cliStringArg(commandArgs, "--traceTopic"),
		BrokerName:    cliStringArg(commandArgs, "--brokerName"),
	}
	if raw := cliStringArg(commandArgs, "-b", "--beginTimestamp"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return rocketmq.MessageQuery{}, "", fmt.Errorf("解析 messageChain beginTimestamp %q 失败: %w", raw, err)
		}
		query.BeginTimestamp = value
	}
	if raw := cliStringArg(commandArgs, "-e", "--endTimestamp"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return rocketmq.MessageQuery{}, "", fmt.Errorf("解析 messageChain endTimestamp %q 失败: %w", raw, err)
		}
		query.EndTimestamp = value
	}
	if raw := cliStringArg(commandArgs, "-m", "--maxNum"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return rocketmq.MessageQuery{}, "", fmt.Errorf("解析 messageChain maxNum %q 失败: %w", raw, err)
		}
		query.MaxNum = value
	}
	if raw := cliStringArg(commandArgs, "--queueId"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return rocketmq.MessageQuery{}, "", fmt.Errorf("解析 messageChain queueId %q 失败: %w", raw, err)
		}
		query.QueueID = value
	}
	if raw := cliStringArg(commandArgs, "--queueOffset"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return rocketmq.MessageQuery{}, "", fmt.Errorf("解析 messageChain queueOffset %q 失败: %w", raw, err)
		}
		query.QueueOffset = value
		query.HasQueueOffset = true
	}
	return query, nameServer, nil
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `goadmin passes RocketMQ mqadmin commands through the configured high-speed transport.

Usage:
  goadmin [--transport auto|native|sidecar|process] [--namesrv host:port] <mqadmin-command> [mqadmin-options]
  goadmin --m6-shadow-plan [--m6-shadow-fixtures '{"samples":[{"name":"known-message","args":["queryMsgById","-i","MESSAGE_ID"]}]}']
  goadmin --m6-shadow-plan --m6-shadow-fixtures-file fixtures.json
  goadmin --m6-shadow-run [--m6-shadow-concurrency 4] --m6-shadow-fixtures-file fixtures.json
  goadmin help <mqadmin-command>
  goadmin

Examples:
  goadmin clusterList
  goadmin queryMsgById -t TopicTest -i MESSAGE_ID
  goadmin --transport native topicList
  goadmin --transport sidecar topicList
  goadmin --m6-shadow-plan
  goadmin --m6-shadow-run --m6-shadow-concurrency 4 --m6-shadow-fixtures-file fixtures.json

`)
}

func injectNameServer(args []string, nameServer string) []string {
	if len(args) == 0 || strings.TrimSpace(nameServer) == "" || strings.EqualFold(args[0], "help") || hasNameServerArg(args) || skipsDefaultNameServer(args) {
		return append([]string(nil), args...)
	}
	injected := make([]string, 0, len(args)+2)
	injected = append(injected, args[0], "-n", nameServer)
	injected = append(injected, args[1:]...)
	return injected
}

func skipsDefaultNameServer(args []string) bool {
	if len(args) == 0 {
		return false
	}
	// BrokerContainer 写命令只接受 -c/-b；注入 -n 会让官方 sidecar/process 分支按未知参数失败。
	if strings.EqualFold(args[0], "addBroker") || strings.EqualFold(args[0], "removeBroker") {
		return true
	}
	// rocksDBConfigToJson 本地模式由官方按 -p 读取本机 RocksDB；注入 -n 会把官方切到 RPC 分支并触发 Invalid args。
	if strings.EqualFold(args[0], "rocksDBConfigToJson") && hasFlagValue(args[1:], "-p", "--configPath") {
		return true
	}
	if strings.EqualFold(args[0], "rocksDBConfigToJson") && hasFlagValue(args[1:], "-b", "--brokerAddr") && !hasFlagValue(args[1:], "-c", "--cluster") {
		return true
	}
	return false
}

func hasNameServerArg(args []string) bool {
	for _, arg := range args {
		if arg == "-n" || arg == "--namesrvAddr" || strings.HasPrefix(arg, "-n=") || strings.HasPrefix(arg, "--namesrvAddr=") {
			return true
		}
	}
	return false
}

func hasFlagValue(args []string, short string, long string) bool {
	for _, arg := range args {
		if arg == short || arg == long || strings.HasPrefix(arg, short+"=") || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
}

func runCommand(ctx context.Context, options Options, args []string) (string, error) {
	if options.Runner != nil {
		return options.Runner.Run(ctx, args...)
	}
	if options.Transport == "native" || options.Transport == "auto" {
		output, supported, err := nativeCommandRunner(ctx, args, options.Timeout)
		if supported {
			if err == nil {
				return output, nil
			}
			if options.Transport == "native" {
				return output, err
			}
		} else if options.Transport == "native" {
			return "", fmt.Errorf("native transport does not support %q yet", commandName(args))
		}
	}
	provider := &rocketmq.MQAdminProvider{
		NameServer:      options.NameServer,
		JavaPath:        options.JavaPath,
		MavenRepository: options.MavenRepository,
		Classpath:       options.Classpath,
		ClasspathFile:   options.ClasspathFile,
		Version:         options.Version,
		Timeout:         options.Timeout,
		SidecarAddr:     options.SidecarAddr,
		SidecarTimeout:  options.SidecarTimeout,
	}
	switch options.Transport {
	case "auto":
		provider.SidecarEnabled = strings.TrimSpace(options.SidecarAddr) != ""
	case "native":
		return "", fmt.Errorf("native transport fallback unexpectedly reached for %q", commandName(args))
	case "sidecar":
		provider.SidecarEnabled = true
	case "process":
		provider.SidecarEnabled = false
	default:
		return "", fmt.Errorf("unknown transport %q", options.Transport)
	}
	return provider.RunCommand(ctx, args...)
}

func commandName(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func getenv(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}
