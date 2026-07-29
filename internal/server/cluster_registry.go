package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	// clusterIDPattern 约束可进入请求头、URL 和审计记录的动态 clusterId。
	clusterIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	// errClusterAlreadyExists 区分重复 ID 与持久化 I/O 错误的 HTTP 状态。
	errClusterAlreadyExists = errors.New("集群 ID 已存在")
)

// loadClusterDefinitions 读取页面动态添加的集群定义；显式文件损坏时立即终止启动。
func loadClusterDefinitions(path string) []ClusterDefinition {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		panic(fmt.Sprintf("读取集群注册表失败: %v", err))
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var definitions []ClusterDefinition
	if err := decoder.Decode(&definitions); err != nil {
		panic(fmt.Sprintf("解析集群注册表失败: %v", err))
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		panic("集群注册表只能包含一个 JSON 数组")
	}
	normalized, err := normalizeManagedClusterDefinitions(definitions)
	if err != nil {
		panic(fmt.Sprintf("集群注册表无效: %v", err))
	}
	return normalized
}

// normalizeManagedClusterDefinition 校验页面可写集群定义，避免不稳定 ID 或多行地址进入请求头和命令参数。
func normalizeManagedClusterDefinition(definition ClusterDefinition) (ClusterDefinition, error) {
	definition.ID = strings.TrimSpace(definition.ID)
	definition.Label = strings.TrimSpace(definition.Label)
	definition.NameServer = strings.TrimSpace(definition.NameServer)
	if !clusterIDPattern.MatchString(definition.ID) {
		return ClusterDefinition{}, errors.New("集群 ID 需为 1 到 64 位字母、数字、点、下划线或短横线，且必须以字母或数字开头")
	}
	if definition.NameServer == "" {
		return ClusterDefinition{}, errors.New("NameServer 为必填项")
	}
	if strings.ContainsAny(definition.NameServer, "\r\n\t") {
		return ClusterDefinition{}, errors.New("NameServer 不能包含换行或制表符")
	}
	if definition.Label == "" {
		definition.Label = definition.ID
	}
	return definition, nil
}

// normalizeManagedClusterDefinitions 校验持久化列表并拒绝重复 clusterId。
func normalizeManagedClusterDefinitions(definitions []ClusterDefinition) ([]ClusterDefinition, error) {
	seen := make(map[string]struct{}, len(definitions))
	normalized := make([]ClusterDefinition, 0, len(definitions))
	for _, definition := range definitions {
		item, err := normalizeManagedClusterDefinition(definition)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[item.ID]; exists {
			return nil, fmt.Errorf("集群 ID 不能重复: %s", item.ID)
		}
		seen[item.ID] = struct{}{}
		normalized = append(normalized, item)
	}
	sort.SliceStable(normalized, func(left, right int) bool {
		return normalized[left].ID < normalized[right].ID
	})
	return normalized, nil
}

// mergeClusterDefinitions 合并启动配置与页面注册表，同时保持稳定排序和 clusterId 唯一性。
func mergeClusterDefinitions(base []ClusterDefinition, persisted []ClusterDefinition) []ClusterDefinition {
	merged := append([]ClusterDefinition(nil), base...)
	seen := make(map[string]struct{}, len(base)+len(persisted))
	for _, definition := range base {
		seen[definition.ID] = struct{}{}
	}
	for _, definition := range persisted {
		if _, exists := seen[definition.ID]; exists {
			panic("集群 ID 同时存在于启动配置和页面注册表: " + definition.ID)
		}
		seen[definition.ID] = struct{}{}
		merged = append(merged, definition)
	}
	sort.SliceStable(merged, func(left, right int) bool {
		return merged[left].ID < merged[right].ID
	})
	return merged
}

// saveClusterDefinitions 将页面添加的集群定义写入持久化卷。
func saveClusterDefinitions(path string, definitions []ClusterDefinition) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("未配置集群注册表路径")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("创建集群注册表目录失败: %w", err)
	}
	content, err := json.MarshalIndent(definitions, "", "  ")
	if err != nil {
		return fmt.Errorf("编码集群注册表失败: %w", err)
	}
	content = append(content, '\n')
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("写入集群注册表失败: %w", err)
	}
	return nil
}

// registerCluster 持久化并注册新的独立 Provider 与快照运行时。
func (a *App) registerCluster(definition ClusterDefinition) (*clusterRuntime, error) {
	a.clusterMu.Lock()
	defer a.clusterMu.Unlock()
	if _, exists := a.clusters[definition.ID]; exists {
		return nil, fmt.Errorf("%w: %s", errClusterAlreadyExists, definition.ID)
	}
	nextPersisted := append(append([]ClusterDefinition(nil), a.persistedClusters...), definition)
	sort.SliceStable(nextPersisted, func(left, right int) bool {
		return nextPersisted[left].ID < nextPersisted[right].ID
	})
	if err := saveClusterDefinitions(a.clusterRegistryPath, nextPersisted); err != nil {
		return nil, err
	}
	runtime := newClusterRuntime(definition, a.providerFactory(definition.NameServer), a.clusterCacheTTL, a.messageChainCacheTTL)
	a.persistedClusters = nextPersisted
	a.clusters[definition.ID] = runtime
	a.clusterOrder = append(a.clusterOrder, definition.ID)
	sort.Strings(a.clusterOrder)
	return runtime, nil
}

// handleClusterRegistry 新增一个可跨容器重启恢复的集群运行时。
func (a *App) handleClusterRegistry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST"))
		return
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request ClusterDefinition
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("请求体必须是集群定义 JSON"))
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, errors.New("请求体只能包含一个集群定义"))
		return
	}
	definition, err := normalizeManagedClusterDefinition(request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	audit, err := a.beginMutationForCluster(r, PermissionRuntimeConfig, "cluster.register", definition.ID, definition.ID, nil)
	if err != nil {
		writeMutationAdmissionError(w, err)
		return
	}
	w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
	runtime, err := a.registerCluster(definition)
	if err != nil {
		if auditErr := audit.complete(r.Context(), nil, nil, err, false); auditErr != nil {
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("注册失败且审计完成记录失败: %v; %w", err, auditErr))
			return
		}
		status := http.StatusServiceUnavailable
		if errors.Is(err, errClusterAlreadyExists) {
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}
	runtime.refreshSnapshots(context.Background())
	verification := map[string]any{"persisted": true, "registered": true, "clusterId": definition.ID}
	if err := audit.complete(r.Context(), definition, verification, nil, false); err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("集群已注册，但审计完成记录失败: %w", err))
		return
	}
	writeJSON(w, http.StatusCreated, responsePayload[dashboardConfigPayload]{
		Code:    0,
		Message: "ok",
		Data:    a.configPayload(),
	})
}
