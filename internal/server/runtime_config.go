package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

var runtimeConfigKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,160}$`)

var runtimeConfigEnumOptions = map[string][]string{
	"brokerrole":    {"ASYNC_MASTER", "SYNC_MASTER", "SLAVE"},
	"flushdisktype": {"ASYNC_FLUSH", "SYNC_FLUSH"},
	"proxymode":     {"cluster", "local"},
}

var runtimeConfigRestartKeys = map[string]bool{
	"brokerclustername": true,
	"brokerid":          true,
	"brokerip1":         true,
	"brokerip2":         true,
	"brokername":        true,
	"brokerrole":        true,
	"halistenport":      true,
	"listenport":        true,
	"usetls":            true,
}

// runtimeConfigCommandProvider 表示支持执行官方兼容配置命令的 Provider。
type runtimeConfigCommandProvider interface {
	RunCommand(ctx context.Context, args ...string) (string, error)
}

// runtimeConfigPayload 返回在线配置入口和 Proxy 当前运行状态。
type runtimeConfigPayload struct {
	// Enabled 表示当前部署是否允许执行配置写入。
	Enabled bool `json:"enabled"`
	// ClusterWritable 表示当前 Provider 是否支持 NameServer/Broker 配置命令。
	ClusterWritable bool `json:"clusterWritable"`
	// Proxy 是官方 RocketMQ Proxy 的配置和进程状态。
	Proxy ProxyRuntimeSnapshot `json:"proxy"`
}

// runtimeConfigChangeRequest 表示一次 NameServer 或 Broker 配置修改。
type runtimeConfigChangeRequest struct {
	// Scope 只能是 nameserver 或 broker。
	Scope string `json:"scope"`
	// Target 是具体地址，星号表示对当前快照中包含该 Key 的全部节点生效。
	Target string `json:"target"`
	// Key 是 RocketMQ 原始配置键。
	Key string `json:"key"`
	// Value 是要写入的配置值。
	Value string `json:"value"`
}

// runtimeConfigChangeResult 描述一个节点的配置写入和回读结果。
type runtimeConfigChangeResult struct {
	// Scope 是配置所属组件类型。
	Scope string `json:"scope"`
	// Target 是实际写入的节点地址。
	Target string `json:"target"`
	// Key 是写入的配置键。
	Key string `json:"key"`
	// PreviousValue 是写入前从节点读取的原值。
	PreviousValue string `json:"previousValue"`
	// Value 是写入并回读确认后的值。
	Value string `json:"value"`
	// Changed 表示本次是否实际执行了更新命令。
	Changed bool `json:"changed"`
	// RestartRequired 表示该键通常需要重启对应 RocketMQ 组件才能完整生效。
	RestartRequired bool `json:"restartRequired"`
}

// runtimeConfigApplyPayload 汇总一次单节点或批量配置写入结果。
type runtimeConfigApplyPayload struct {
	// Results 按实际节点列出原值、新值和生效方式。
	Results []runtimeConfigChangeResult `json:"results"`
	// RestartRequired 表示至少一个修改项需要组件重启。
	RestartRequired bool `json:"restartRequired"`
	// RolledBack 表示失败路径是否已经执行原值回滚。
	RolledBack bool `json:"rolledBack"`
}

// runtimePendingChange 保存批量写入前读取到的节点原值和规范化目标值。
type runtimePendingChange struct {
	// target 是实际节点地址。
	target string
	// previous 是写入前原值，用于失败回滚。
	previous string
	// value 是通过类型校验后的目标值。
	value string
}

// ProxyRuntimeField 描述 Proxy 面板中一个可编辑字段。
type ProxyRuntimeField struct {
	// Key 是 RocketMQ ProxyConfig 的 JSON 键。
	Key string `json:"key"`
	// Label 是页面展示的中文名称。
	Label string `json:"label"`
	// Group 是页面分组名称。
	Group string `json:"group"`
	// Description 说明该字段对 Proxy 的作用。
	Description string `json:"description"`
	// Type 是 boolean、integer、number、select 或 text。
	Type string `json:"type"`
	// Value 是当前持久化值。
	Value any `json:"value"`
	// Options 是 select 类型允许的枚举值。
	Options []string `json:"options,omitempty"`
	// Min 是数字输入的最小值。
	Min *float64 `json:"min,omitempty"`
	// Max 是数字输入的最大值。
	Max *float64 `json:"max,omitempty"`
	// RestartRequired 表示修改后需要重启 Proxy，本面板保存时会自动完成。
	RestartRequired bool `json:"restartRequired"`
}

// ProxyRuntimeSnapshot 是 Proxy 配置、进程和健康检查的统一快照。
type ProxyRuntimeSnapshot struct {
	// Available 表示当前镜像具备启动官方 Proxy 所需的 Java 和 RocketMQ 目录。
	Available bool `json:"available"`
	// Enabled 表示持久化配置要求 Proxy 随 Dashboard 启动。
	Enabled bool `json:"enabled"`
	// Running 表示 Proxy Java 进程仍在运行。
	Running bool `json:"running"`
	// Healthy 表示 gRPC 监听端口已经可以建立 TCP 连接。
	Healthy bool `json:"healthy"`
	// Status 是 disabled、stopped、starting、running、error 或 unavailable。
	Status string `json:"status"`
	// PID 是当前 Proxy Java 进程号，未运行时为 0。
	PID int `json:"pid"`
	// GrpcEndpoint 是容器内 gRPC 监听地址。
	GrpcEndpoint string `json:"grpcEndpoint"`
	// RemotingEndpoint 是容器内 Remoting 兼容监听地址。
	RemotingEndpoint string `json:"remotingEndpoint"`
	// StartedAtUnixMilli 是最近一次成功启动时间。
	StartedAtUnixMilli int64 `json:"startedAtUnixMilli"`
	// RestartCount 是当前 Dashboard 进程内成功启动 Proxy 的次数。
	RestartCount int `json:"restartCount"`
	// LastError 记录最近一次启动、停止或健康检查错误。
	LastError string `json:"lastError,omitempty"`
	// Fields 是所有当前 Proxy JSON 配置生成的类型化控件。
	Fields []ProxyRuntimeField `json:"fields"`
}

// ProxyRuntimeApplyRequest 表示保存 Proxy 配置并立即应用的请求。
type ProxyRuntimeApplyRequest struct {
	// Enabled 控制 Proxy 是否运行。
	Enabled bool `json:"enabled"`
	// Settings 保存 ProxyConfig 的全部 JSON 字段。
	Settings map[string]any `json:"settings"`
}

// ProxyRuntime 定义 Proxy 配置保存、进程重启和状态读取能力。
type ProxyRuntime interface {
	// Snapshot 返回当前内存和持久化状态的只读副本。
	Snapshot() ProxyRuntimeSnapshot
	// Apply 校验并保存新配置；启用状态下会重启并完成健康检查，失败时恢复旧配置。
	Apply(ctx context.Context, request ProxyRuntimeApplyRequest) (ProxyRuntimeSnapshot, error)
	// Restart 使用当前持久化配置重启 Proxy 并执行健康检查。
	Restart(ctx context.Context) (ProxyRuntimeSnapshot, error)
}

// handleRuntimeConfig 返回运行配置状态或执行一个 NameServer/Broker 动态配置修改。
func (a *App) handleRuntimeConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		_, clusterWritable := a.currentProvider().(runtimeConfigCommandProvider)
		writeJSON(w, http.StatusOK, responsePayload[runtimeConfigPayload]{
			Code:    0,
			Message: "ok",
			Data: runtimeConfigPayload{
				Enabled:         a.runtimeConfigEnabled,
				ClusterWritable: a.runtimeConfigEnabled && clusterWritable,
				Proxy:           a.proxyRuntimeSnapshot(),
			},
		})
	case http.MethodPut, http.MethodPost:
		if !a.runtimeConfigEnabled {
			writeError(w, http.StatusForbidden, errors.New("当前部署未开启在线配置写入"))
			return
		}
		var request runtimeConfigChangeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, errors.New("请求体必须是 JSON"))
			return
		}
		start := time.Now()
		result, err := a.applyRuntimeConfigChange(r.Context(), request)
		if err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeJSON(w, http.StatusOK, responsePayload[runtimeConfigApplyPayload]{
			Code:          0,
			Message:       "ok",
			Data:          result,
			LatencyMillis: time.Since(start).Milliseconds(),
		})
	default:
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET/POST/PUT"))
	}
}

// handleProxyRuntimeConfig 保存 Proxy 配置，并按启用状态自动启动、停止或重启进程。
func (a *App) handleProxyRuntimeConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST/PUT"))
		return
	}
	if !a.runtimeConfigEnabled {
		writeError(w, http.StatusForbidden, errors.New("当前部署未开启在线配置写入"))
		return
	}
	if a.proxyRuntime == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("当前部署未配置 RocketMQ Proxy 运行器"))
		return
	}
	var request ProxyRuntimeApplyRequest
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("请求体必须是 JSON"))
		return
	}
	start := time.Now()
	snapshot, err := a.proxyRuntime.Apply(r.Context(), request)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[ProxyRuntimeSnapshot]{
		Code:          0,
		Message:       "ok",
		Data:          snapshot,
		LatencyMillis: time.Since(start).Milliseconds(),
	})
}

// handleProxyRuntimeRestart 使用已保存配置执行一次显式 Proxy 重启和健康检查。
func (a *App) handleProxyRuntimeRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST"))
		return
	}
	if !a.runtimeConfigEnabled {
		writeError(w, http.StatusForbidden, errors.New("当前部署未开启在线配置写入"))
		return
	}
	if a.proxyRuntime == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("当前部署未配置 RocketMQ Proxy 运行器"))
		return
	}
	start := time.Now()
	snapshot, err := a.proxyRuntime.Restart(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[ProxyRuntimeSnapshot]{
		Code:          0,
		Message:       "ok",
		Data:          snapshot,
		LatencyMillis: time.Since(start).Milliseconds(),
	})
}

// proxyRuntimeSnapshot 在未配置运行器时返回明确的不可用状态。
func (a *App) proxyRuntimeSnapshot() ProxyRuntimeSnapshot {
	if a.proxyRuntime == nil {
		return ProxyRuntimeSnapshot{Status: "unavailable", LastError: "当前部署未配置 RocketMQ Proxy 运行器"}
	}
	return a.proxyRuntime.Snapshot()
}

// applyRuntimeConfigChange 串行执行读取、更新、回读和失败回滚，避免并发写入相互覆盖。
func (a *App) applyRuntimeConfigChange(ctx context.Context, request runtimeConfigChangeRequest) (runtimeConfigApplyPayload, error) {
	a.runtimeConfigMu.Lock()
	defer a.runtimeConfigMu.Unlock()

	scope := strings.ToLower(strings.TrimSpace(request.Scope))
	if scope != "broker" && scope != "nameserver" {
		return runtimeConfigApplyPayload{}, errors.New("scope 只能是 broker 或 nameserver")
	}
	target := strings.TrimSpace(request.Target)
	if target == "" {
		return runtimeConfigApplyPayload{}, errors.New("target 必填")
	}
	key := strings.TrimSpace(request.Key)
	if !runtimeConfigKeyPattern.MatchString(key) {
		return runtimeConfigApplyPayload{}, errors.New("配置 key 格式不正确")
	}
	if strings.ContainsAny(request.Value, "\r\n") || len(request.Value) > 8192 {
		return runtimeConfigApplyPayload{}, errors.New("配置 value 不能包含换行且长度不能超过 8192")
	}

	provider := a.currentProvider()
	commandProvider, ok := provider.(runtimeConfigCommandProvider)
	if !ok {
		return runtimeConfigApplyPayload{}, errors.New("当前 Provider 不支持在线配置命令")
	}
	targets, err := runtimeConfigTargets(ctx, provider, scope, target, key)
	if err != nil {
		return runtimeConfigApplyPayload{}, err
	}

	pending := make([]runtimePendingChange, 0, len(targets))
	for _, actualTarget := range targets {
		previous, found, err := readRuntimeConfigValue(ctx, commandProvider, scope, actualTarget, key)
		if err != nil {
			return runtimeConfigApplyPayload{}, fmt.Errorf("读取 %s %s 原配置失败: %w", scope, actualTarget, err)
		}
		if !found {
			return runtimeConfigApplyPayload{}, fmt.Errorf("%s %s 不存在配置 %s", scope, actualTarget, key)
		}
		value, err := normalizeRuntimeConfigValue(key, previous, request.Value)
		if err != nil {
			return runtimeConfigApplyPayload{}, err
		}
		pending = append(pending, runtimePendingChange{target: actualTarget, previous: previous, value: value})
	}

	results := make([]runtimeConfigChangeResult, 0, len(pending))
	applied := make([]runtimePendingChange, 0, len(pending))
	for _, change := range pending {
		changed := change.previous != change.value
		if changed {
			if err := writeRuntimeConfigValue(ctx, commandProvider, scope, change.target, key, change.value, a.currentNameServer()); err != nil {
				rollbackErr := rollbackRuntimeConfigChanges(ctx, commandProvider, scope, key, applied, a.currentNameServer())
				return runtimeConfigApplyPayload{Results: results, RolledBack: len(applied) > 0}, runtimeConfigApplyError(err, rollbackErr)
			}
			applied = append(applied, change)
			actual, found, readErr := readRuntimeConfigValue(ctx, commandProvider, scope, change.target, key)
			if readErr != nil || !found || actual != change.value {
				verifyErr := readErr
				if verifyErr == nil {
					verifyErr = fmt.Errorf("回读值为 %q，期望 %q", actual, change.value)
				}
				rollbackErr := rollbackRuntimeConfigChanges(ctx, commandProvider, scope, key, applied, a.currentNameServer())
				return runtimeConfigApplyPayload{Results: results, RolledBack: true}, runtimeConfigApplyError(fmt.Errorf("%s %s 配置回读校验失败: %w", scope, change.target, verifyErr), rollbackErr)
			}
		}
		results = append(results, runtimeConfigChangeResult{
			Scope:           scope,
			Target:          change.target,
			Key:             key,
			PreviousValue:   change.previous,
			Value:           change.value,
			Changed:         changed,
			RestartRequired: runtimeConfigRequiresRestart(key),
		})
	}

	if len(applied) > 0 {
		log.Printf("runtime config updated scope=%q targets=%d key=%q restartRequired=%t", scope, len(applied), key, runtimeConfigRequiresRestart(key))
		a.featureSnapshotStore().refreshAsync(context.Background())
	}
	return runtimeConfigApplyPayload{Results: results, RestartRequired: runtimeConfigRequiresRestart(key)}, nil
}

// currentNameServer 返回写 Broker 配置命令使用的当前 NameServer 地址。
func (a *App) currentNameServer() string {
	nameServer, _ := a.nameServerConfig()
	return nameServer
}

// runtimeConfigTargets 从最新能力画像中限定允许修改的节点，并展开星号批量目标。
func runtimeConfigTargets(ctx context.Context, provider rocketmq.Provider, scope string, requestedTarget string, key string) ([]string, error) {
	featureProvider, ok := provider.(clusterFeaturesProvider)
	if !ok {
		return nil, errors.New("当前 Provider 不支持读取配置画像")
	}
	report, err := featureProvider.ClusterFeatures(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取配置画像失败: %w", err)
	}
	targets := make([]string, 0)
	if scope == "broker" {
		for _, broker := range report.BrokerConfigs {
			if requestedTarget != "*" && !strings.EqualFold(strings.TrimSpace(broker.BrokerAddr), requestedTarget) {
				continue
			}
			if configEntryExists(broker.Entries, key) {
				targets = append(targets, strings.TrimSpace(broker.BrokerAddr))
			}
		}
	} else {
		for _, nameServer := range report.NameServerConfigs {
			if requestedTarget != "*" && !strings.EqualFold(strings.TrimSpace(nameServer.NameServer), requestedTarget) {
				continue
			}
			if configEntryExists(nameServer.Entries, key) {
				targets = append(targets, strings.TrimSpace(nameServer.NameServer))
			}
		}
	}
	targets = uniqueSortedRuntimeTargets(targets)
	if len(targets) == 0 {
		return nil, fmt.Errorf("当前配置画像中未找到 scope=%s target=%s key=%s", scope, requestedTarget, key)
	}
	return targets, nil
}

// configEntryExists 判断配置快照是否包含指定键。
func configEntryExists(entries []rocketmq.ConfigEntry, key string) bool {
	for _, entry := range entries {
		if strings.EqualFold(strings.TrimSpace(entry.Key), key) {
			return true
		}
	}
	return false
}

// uniqueSortedRuntimeTargets 去重并排序实际节点，保证批量修改顺序稳定。
func uniqueSortedRuntimeTargets(values []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// readRuntimeConfigValue 使用官方读取命令获取单个节点的当前配置值。
func readRuntimeConfigValue(ctx context.Context, provider runtimeConfigCommandProvider, scope string, target string, key string) (string, bool, error) {
	if scope == "broker" {
		output, err := provider.RunCommand(ctx, "getBrokerConfig", "-b", target)
		if err != nil {
			return "", false, err
		}
		sections, err := rocketmq.ParseConfigSections(output)
		if err != nil {
			return "", false, err
		}
		for _, section := range sections {
			if value, ok := runtimeConfigEntryValue(section.Entries, key); ok {
				return value, true, nil
			}
		}
		return "", false, nil
	}
	output, err := provider.RunCommand(ctx, "getNamesrvConfig", "-n", target)
	if err != nil {
		return "", false, err
	}
	configs, err := rocketmq.ParseNameServerConfigs(output)
	if err != nil {
		return "", false, err
	}
	for _, config := range configs {
		if strings.TrimSpace(config.NameServer) != "" && !strings.EqualFold(strings.TrimSpace(config.NameServer), target) {
			continue
		}
		if value, ok := runtimeConfigEntryValue(config.Entries, key); ok {
			return value, true, nil
		}
	}
	return "", false, nil
}

// runtimeConfigEntryValue 按不区分大小写方式读取配置键。
func runtimeConfigEntryValue(entries []rocketmq.ConfigEntry, key string) (string, bool) {
	for _, entry := range entries {
		if strings.EqualFold(strings.TrimSpace(entry.Key), key) {
			return strings.TrimSpace(entry.Value), true
		}
	}
	return "", false
}

// writeRuntimeConfigValue 调用官方兼容命令写入一个 NameServer 或 Broker 配置值。
func writeRuntimeConfigValue(ctx context.Context, provider runtimeConfigCommandProvider, scope string, target string, key string, value string, nameServer string) error {
	if scope == "broker" {
		args := []string{"updateBrokerConfig", "-b", target, "-k", key, "-v", value}
		if strings.TrimSpace(nameServer) != "" {
			args = append(args, "-n", nameServer)
		}
		_, err := provider.RunCommand(ctx, args...)
		return err
	}
	_, err := provider.RunCommand(ctx, "updateNamesrvConfig", "-n", target, "-k", key, "-v", value)
	return err
}

// rollbackRuntimeConfigChanges 按写入逆序恢复原值，并聚合所有回滚错误。
func rollbackRuntimeConfigChanges(ctx context.Context, provider runtimeConfigCommandProvider, scope string, key string, applied []runtimePendingChange, nameServer string) error {
	errorsFound := make([]string, 0)
	for index := len(applied) - 1; index >= 0; index-- {
		change := applied[index]
		if err := writeRuntimeConfigValue(ctx, provider, scope, change.target, key, change.previous, nameServer); err != nil {
			errorsFound = append(errorsFound, fmt.Sprintf("%s: %s", change.target, err.Error()))
		}
	}
	if len(errorsFound) > 0 {
		return errors.New(strings.Join(errorsFound, "; "))
	}
	return nil
}

// runtimeConfigApplyError 将原始失败和回滚结论合并成一个可操作的错误说明。
func runtimeConfigApplyError(applyErr error, rollbackErr error) error {
	if rollbackErr != nil {
		return fmt.Errorf("配置应用失败且回滚不完整: %v; rollback=%v", applyErr, rollbackErr)
	}
	return fmt.Errorf("配置应用失败，已恢复原值: %w", applyErr)
}

// normalizeRuntimeConfigValue 根据原值和已知枚举校验输入，并返回规范化字符串。
func normalizeRuntimeConfigValue(key string, previous string, next string) (string, error) {
	normalizedKey := strings.ToLower(strings.TrimSpace(key))
	value := strings.TrimSpace(next)
	if options := runtimeConfigEnumOptions[normalizedKey]; len(options) > 0 {
		for _, option := range options {
			if strings.EqualFold(value, option) {
				return option, nil
			}
		}
		return "", fmt.Errorf("%s 只能是 %s", key, strings.Join(options, "、"))
	}
	if _, err := strconv.ParseBool(strings.ToLower(strings.TrimSpace(previous))); err == nil {
		parsed, parseErr := strconv.ParseBool(strings.ToLower(value))
		if parseErr != nil {
			return "", fmt.Errorf("%s 必须是 true 或 false", key)
		}
		return strconv.FormatBool(parsed), nil
	}
	if _, err := strconv.ParseInt(strings.TrimSpace(previous), 10, 64); err == nil {
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil {
			return "", fmt.Errorf("%s 必须是整数", key)
		}
		return strconv.FormatInt(parsed, 10), nil
	}
	if _, err := strconv.ParseFloat(strings.TrimSpace(previous), 64); err == nil {
		parsed, parseErr := strconv.ParseFloat(value, 64)
		if parseErr != nil {
			return "", fmt.Errorf("%s 必须是数字", key)
		}
		return strconv.FormatFloat(parsed, 'f', -1, 64), nil
	}
	return value, nil
}

// runtimeConfigRequiresRestart 识别监听地址、身份和存储路径等需要重启才能完整生效的配置。
func runtimeConfigRequiresRestart(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if runtimeConfigRestartKeys[normalized] {
		return true
	}
	return strings.HasPrefix(normalized, "storepath") || strings.HasSuffix(normalized, "listenport")
}
