package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	// errClusterNotFound 表示请求的集群运行时不存在。
	errClusterNotFound = errors.New("集群不存在")
	// errClusterImmutable 表示启动环境变量定义的集群不能由页面覆盖。
	errClusterImmutable = errors.New("启动配置集群不可在页面修改或删除")
	// brokerHostPattern 约束可写入 JVM hosts 文件的 Broker 主机名。
	brokerHostPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$`)
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
	mappings, err := normalizeBrokerAddressMappings(definition.NameServer, definition.BrokerAddressMappings)
	if err != nil {
		return ClusterDefinition{}, err
	}
	definition.BrokerAddressMappings = mappings
	return definition, nil
}

// normalizeBrokerAddressMappings 校验、规范化并稳定排序当前集群的 Broker 主机名映射。
func normalizeBrokerAddressMappings(nameServer string, mappings []BrokerAddressMapping) ([]BrokerAddressMapping, error) {
	if len(mappings) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(mappings))
	normalized := make([]BrokerAddressMapping, 0, len(mappings))
	for _, mapping := range mappings {
		host := strings.ToLower(strings.TrimSpace(mapping.Host))
		ip := net.ParseIP(strings.TrimSpace(mapping.IP))
		if !brokerHostPattern.MatchString(host) || net.ParseIP(host) != nil {
			return nil, fmt.Errorf("Broker 映射主机名无效: %s", mapping.Host)
		}
		if ip == nil {
			return nil, fmt.Errorf("Broker 映射 IP 无效: %s", mapping.IP)
		}
		if _, exists := seen[host]; exists {
			return nil, fmt.Errorf("Broker 映射主机名不能重复: %s", host)
		}
		seen[host] = struct{}{}
		normalized = append(normalized, BrokerAddressMapping{Host: host, IP: ip.String()})
	}
	for _, endpoint := range strings.Split(nameServer, ";") {
		host := nameServerHost(endpoint)
		if host != "" && net.ParseIP(host) == nil {
			if _, exists := seen[strings.ToLower(host)]; !exists {
				return nil, fmt.Errorf("启用 Broker 地址映射时，NameServer 主机名也必须配置映射: %s", host)
			}
		}
	}
	sort.SliceStable(normalized, func(left, right int) bool {
		return normalized[left].Host < normalized[right].Host
	})
	return normalized, nil
}

// nameServerHost 提取单个 NameServer 端点的主机部分，用于校验自定义 hosts 文件是否完整。
func nameServerHost(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err == nil {
		return strings.Trim(host, "[]")
	}
	index := strings.LastIndex(endpoint, ":")
	if index <= 0 {
		return strings.Trim(endpoint, "[]")
	}
	return strings.Trim(endpoint[:index], "[]")
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
	runtime := newClusterRuntime(definition, a.providerFactory(definition), a.clusterCacheTTL, a.messageChainCacheTTL)
	a.persistedClusters = nextPersisted
	a.clusters[definition.ID] = runtime
	a.clusterOrder = append(a.clusterOrder, definition.ID)
	sort.Strings(a.clusterOrder)
	return runtime, nil
}

// clusterDefinitionForID 返回集群定义及其是否来自页面注册表。
func (a *App) clusterDefinitionForID(clusterID string) (ClusterDefinition, bool, bool) {
	a.clusterMu.RLock()
	defer a.clusterMu.RUnlock()
	runtime, exists := a.clusters[clusterID]
	if !exists {
		return ClusterDefinition{}, false, false
	}
	for _, definition := range a.persistedClusters {
		if definition.ID == clusterID {
			return runtime.definition, true, true
		}
	}
	return runtime.definition, true, false
}

// updateCluster 修改页面注册的集群定义，并替换对应的独立 Provider 和快照仓库。
func (a *App) updateCluster(previousID string, definition ClusterDefinition) (*clusterRuntime, error) {
	a.clusterMu.Lock()
	defer a.clusterMu.Unlock()
	if _, exists := a.clusters[previousID]; !exists {
		return nil, fmt.Errorf("%w: %s", errClusterNotFound, previousID)
	}
	managedIndex := -1
	for index, persisted := range a.persistedClusters {
		if persisted.ID == previousID {
			managedIndex = index
			break
		}
	}
	if managedIndex < 0 {
		return nil, errClusterImmutable
	}
	if previousID != definition.ID {
		if _, exists := a.clusters[definition.ID]; exists {
			return nil, fmt.Errorf("%w: %s", errClusterAlreadyExists, definition.ID)
		}
	}
	nextPersisted := append([]ClusterDefinition(nil), a.persistedClusters...)
	nextPersisted[managedIndex] = definition
	nextPersisted, err := normalizeManagedClusterDefinitions(nextPersisted)
	if err != nil {
		return nil, err
	}
	if err := saveClusterDefinitions(a.clusterRegistryPath, nextPersisted); err != nil {
		return nil, err
	}
	runtime := newClusterRuntime(definition, a.providerFactory(definition), a.clusterCacheTTL, a.messageChainCacheTTL)
	delete(a.clusters, previousID)
	a.clusters[definition.ID] = runtime
	for index, clusterID := range a.clusterOrder {
		if clusterID == previousID {
			a.clusterOrder[index] = definition.ID
			break
		}
	}
	sort.Strings(a.clusterOrder)
	a.persistedClusters = nextPersisted
	return runtime, nil
}

// deleteCluster 删除页面注册的集群及其独立运行时，并保留启动配置集群。
func (a *App) deleteCluster(clusterID string) error {
	a.clusterMu.Lock()
	defer a.clusterMu.Unlock()
	if _, exists := a.clusters[clusterID]; !exists {
		return fmt.Errorf("%w: %s", errClusterNotFound, clusterID)
	}
	managedIndex := -1
	for index, definition := range a.persistedClusters {
		if definition.ID == clusterID {
			managedIndex = index
			break
		}
	}
	if managedIndex < 0 {
		return errClusterImmutable
	}
	nextPersisted := append([]ClusterDefinition(nil), a.persistedClusters[:managedIndex]...)
	nextPersisted = append(nextPersisted, a.persistedClusters[managedIndex+1:]...)
	if err := saveClusterDefinitions(a.clusterRegistryPath, nextPersisted); err != nil {
		return err
	}
	delete(a.clusters, clusterID)
	for index, currentID := range a.clusterOrder {
		if currentID == clusterID {
			a.clusterOrder = append(a.clusterOrder[:index], a.clusterOrder[index+1:]...)
			break
		}
	}
	a.persistedClusters = nextPersisted
	return nil
}

// clusterIDFromPath 提取并校验 /api/config/clusters/{id} 路径中的动态集群 ID。
func clusterIDFromPath(r *http.Request) (string, error) {
	clusterID := strings.TrimPrefix(r.URL.Path, "/api/config/clusters/")
	clusterID = strings.TrimSuffix(clusterID, "/")
	if clusterID == "" || strings.Contains(clusterID, "/") {
		return "", errors.New("集群路径 ID 无效")
	}
	return clusterID, nil
}

// decodeClusterDefinition 解码修改集群所需的完整定义，并拒绝未知字段和多余 JSON。
func decodeClusterDefinition(r *http.Request) (ClusterDefinition, error) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request ClusterDefinition
	if err := decoder.Decode(&request); err != nil {
		return ClusterDefinition{}, errors.New("请求体必须是集群定义 JSON")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ClusterDefinition{}, errors.New("请求体只能包含一个集群定义")
	}
	return normalizeManagedClusterDefinition(request)
}

// clusterMutationStatus 将集群管理错误转换为稳定的 API 状态码。
func clusterMutationStatus(err error) int {
	switch {
	case errors.Is(err, errClusterNotFound):
		return http.StatusNotFound
	case errors.Is(err, errClusterAlreadyExists), errors.Is(err, errClusterImmutable):
		return http.StatusConflict
	default:
		return http.StatusServiceUnavailable
	}
}

// writeClusterConfigResponse 返回修改或删除后的全量集群配置，前端据此刷新选择器。
func (a *App) writeClusterConfigResponse(w http.ResponseWriter, status int) {
	writeJSON(w, status, responsePayload[dashboardConfigPayload]{
		Code:    0,
		Message: "ok",
		Data:    a.configPayload(),
	})
}

// handleClusterRegistry 新增一个可跨容器重启恢复的集群运行时。
func (a *App) handleClusterRegistry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST"))
		return
	}
	definition, err := decodeClusterDefinition(r)
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

// handleClusterRegistryItem 修改或删除一个页面注册的集群定义。
func (a *App) handleClusterRegistryItem(w http.ResponseWriter, r *http.Request) {
	clusterID, err := clusterIDFromPath(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	before, exists, _ := a.clusterDefinitionForID(clusterID)
	switch r.Method {
	case http.MethodPut:
		definition, decodeErr := decodeClusterDefinition(r)
		if decodeErr != nil {
			writeError(w, http.StatusBadRequest, decodeErr)
			return
		}
		audit, auditErr := a.beginMutationForCluster(r, PermissionRuntimeConfig, "cluster.update", clusterID, clusterID, before)
		if auditErr != nil {
			writeMutationAdmissionError(w, auditErr)
			return
		}
		w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
		runtime, updateErr := a.updateCluster(clusterID, definition)
		if updateErr != nil {
			if completeErr := audit.complete(r.Context(), nil, nil, updateErr, false); completeErr != nil {
				writeError(w, http.StatusServiceUnavailable, fmt.Errorf("修改失败且审计完成记录失败: %v; %w", updateErr, completeErr))
				return
			}
			writeError(w, clusterMutationStatus(updateErr), updateErr)
			return
		}
		runtime.refreshSnapshots(context.Background())
		if completeErr := audit.complete(r.Context(), definition, map[string]any{"updated": true, "clusterId": definition.ID}, nil, false); completeErr != nil {
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("集群已修改，但审计完成记录失败: %w", completeErr))
			return
		}
		a.writeClusterConfigResponse(w, http.StatusOK)
	case http.MethodDelete:
		if !exists {
			writeError(w, http.StatusNotFound, fmt.Errorf("%w: %s", errClusterNotFound, clusterID))
			return
		}
		audit, auditErr := a.beginMutationForCluster(r, PermissionRuntimeConfig, "cluster.delete", clusterID, clusterID, before)
		if auditErr != nil {
			writeMutationAdmissionError(w, auditErr)
			return
		}
		w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
		if deleteErr := a.deleteCluster(clusterID); deleteErr != nil {
			if completeErr := audit.complete(r.Context(), nil, nil, deleteErr, false); completeErr != nil {
				writeError(w, http.StatusServiceUnavailable, fmt.Errorf("删除失败且审计完成记录失败: %v; %w", deleteErr, completeErr))
				return
			}
			writeError(w, clusterMutationStatus(deleteErr), deleteErr)
			return
		}
		if completeErr := audit.complete(r.Context(), nil, map[string]any{"deleted": true, "clusterId": clusterID}, nil, false); completeErr != nil {
			writeError(w, http.StatusServiceUnavailable, fmt.Errorf("集群已删除，但审计完成记录失败: %w", completeErr))
			return
		}
		a.writeClusterConfigResponse(w, http.StatusOK)
	default:
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 PUT 或 DELETE"))
	}
}
