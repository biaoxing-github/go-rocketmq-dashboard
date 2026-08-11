package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

// newClusterRegistryTestApp 构造同时包含启动配置集群和页面注册集群的受认证测试应用。
func newClusterRegistryTestApp(t *testing.T, registryPath string) *App {
	t.Helper()
	config := mutationTestAppConfig(t, AppConfig{
		ProviderFactory: func(string) rocketmq.Provider { return &fakeProvider{} },
		Clusters: []ClusterDefinition{{
			ID:         "base",
			Label:      "启动集群",
			NameServer: "base-ns:9876",
		}},
		ClusterRegistryPath: registryPath,
		ClusterCacheTTL:     time.Hour,
	})
	return New(config)
}

// writeClusterRegistryTestFile 写入测试启动时需要恢复的页面注册集群定义。
func writeClusterRegistryTestFile(t *testing.T, path string, definitions []ClusterDefinition) {
	t.Helper()
	content, err := json.Marshal(definitions)
	if err != nil {
		t.Fatalf("encode cluster registry: %v", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write cluster registry: %v", err)
	}
}

// decodeClusterConfigResponse 解码集群管理接口返回的完整 Dashboard 配置。
func decodeClusterConfigResponse(t *testing.T, recorder *httptest.ResponseRecorder) dashboardConfigPayload {
	t.Helper()
	var payload responsePayload[dashboardConfigPayload]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode cluster config response: %v body=%s", err, recorder.Body.String())
	}
	if payload.Code != 0 {
		t.Fatalf("expected successful cluster config response, got %#v", payload)
	}
	return payload.Data
}

// TestClusterRegistryUpdateDeletePersistsAndRebuildsRuntime 验证修改和删除同时更新文件、配置响应与独立运行时。
func TestClusterRegistryUpdateDeletePersistsAndRebuildsRuntime(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "clusters.json")
	writeClusterRegistryTestFile(t, registryPath, []ClusterDefinition{{
		ID:         "managed-a",
		Label:      "页面集群 A",
		NameServer: "managed-a-ns:9876",
	}})
	app := newClusterRegistryTestApp(t, registryPath)

	update := authorizedMutationRequest(
		http.MethodPut,
		"/api/config/clusters/managed-a",
		bytes.NewBufferString(`{"id":"managed-b","label":"页面集群 B","nameServer":"managed-b-ns:9876","brokerAddressMappings":[{"host":"managed-b-ns","ip":"10.0.0.2"},{"host":"rmqbroker-a-master","ip":"10.0.0.2"}]}`),
	)
	updateRecorder := httptest.NewRecorder()
	app.ServeHTTP(updateRecorder, update)
	if updateRecorder.Code != http.StatusOK {
		t.Fatalf("expected update status 200, got %d body=%s", updateRecorder.Code, updateRecorder.Body.String())
	}
	if updateRecorder.Header().Get("X-RMQD-Operation-ID") == "" {
		t.Fatal("expected update operation identifier")
	}
	updatedConfig := decodeClusterConfigResponse(t, updateRecorder)
	if len(updatedConfig.Clusters) != 2 || updatedConfig.Clusters[1].ID != "managed-b" || updatedConfig.Clusters[1].NameServer != "managed-b-ns:9876" || len(updatedConfig.Clusters[1].BrokerAddressMappings) != 2 {
		t.Fatalf("unexpected updated clusters %#v", updatedConfig.Clusters)
	}
	if len(updatedConfig.ManagedClusterIDs) != 1 || updatedConfig.ManagedClusterIDs[0] != "managed-b" {
		t.Fatalf("unexpected managed cluster ids %#v", updatedConfig.ManagedClusterIDs)
	}
	app.clusterMu.RLock()
	_, oldRuntimeExists := app.clusters["managed-a"]
	newRuntime := app.clusters["managed-b"]
	app.clusterMu.RUnlock()
	if oldRuntimeExists || newRuntime == nil || newRuntime.definition.NameServer != "managed-b-ns:9876" {
		t.Fatalf("runtime replacement failed oldExists=%t new=%#v", oldRuntimeExists, newRuntime)
	}
	persistedAfterUpdate := loadClusterDefinitions(registryPath)
	if len(persistedAfterUpdate) != 1 || persistedAfterUpdate[0].ID != "managed-b" || persistedAfterUpdate[0].Label != "页面集群 B" || len(persistedAfterUpdate[0].BrokerAddressMappings) != 2 {
		t.Fatalf("unexpected persisted update %#v", persistedAfterUpdate)
	}

	restarted := newClusterRegistryTestApp(t, registryPath)
	restartedConfig := restarted.configPayload()
	if len(restartedConfig.ManagedClusterIDs) != 1 || restartedConfig.ManagedClusterIDs[0] != "managed-b" {
		t.Fatalf("updated cluster was not restored after restart %#v", restartedConfig)
	}
	deleteRecorder := httptest.NewRecorder()
	restarted.ServeHTTP(deleteRecorder, authorizedMutationRequest(http.MethodDelete, "/api/config/clusters/managed-b", nil))
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("expected delete status 200, got %d body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	deletedConfig := decodeClusterConfigResponse(t, deleteRecorder)
	if len(deletedConfig.Clusters) != 1 || deletedConfig.Clusters[0].ID != "base" || len(deletedConfig.ManagedClusterIDs) != 0 {
		t.Fatalf("unexpected config after delete %#v", deletedConfig)
	}
	restarted.clusterMu.RLock()
	_, deletedRuntimeExists := restarted.clusters["managed-b"]
	restarted.clusterMu.RUnlock()
	if deletedRuntimeExists {
		t.Fatal("deleted cluster runtime is still registered")
	}
	if persistedAfterDelete := loadClusterDefinitions(registryPath); len(persistedAfterDelete) != 0 {
		t.Fatalf("deleted cluster is still persisted %#v", persistedAfterDelete)
	}
}

// TestClusterRegistryRejectsImmutableCollisionAndMissingTargets 验证启动配置、重复 ID 和不存在目标均不会被页面写入覆盖。
func TestClusterRegistryRejectsImmutableCollisionAndMissingTargets(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "clusters.json")
	writeClusterRegistryTestFile(t, registryPath, []ClusterDefinition{{
		ID:         "managed-a",
		Label:      "页面集群 A",
		NameServer: "managed-a-ns:9876",
	}})
	app := newClusterRegistryTestApp(t, registryPath)

	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantText   string
	}{
		{name: "update immutable", method: http.MethodPut, path: "/api/config/clusters/base", body: `{"id":"base","label":"覆盖启动集群","nameServer":"other:9876"}`, wantStatus: http.StatusConflict, wantText: "启动配置集群不可在页面修改或删除"},
		{name: "delete immutable", method: http.MethodDelete, path: "/api/config/clusters/base", wantStatus: http.StatusConflict, wantText: "启动配置集群不可在页面修改或删除"},
		{name: "rename to existing id", method: http.MethodPut, path: "/api/config/clusters/managed-a", body: `{"id":"base","label":"重复 ID","nameServer":"other:9876"}`, wantStatus: http.StatusConflict, wantText: "集群 ID 已存在"},
		{name: "update missing", method: http.MethodPut, path: "/api/config/clusters/missing", body: `{"id":"missing","label":"不存在","nameServer":"other:9876"}`, wantStatus: http.StatusNotFound, wantText: "集群不存在"},
		{name: "delete missing", method: http.MethodDelete, path: "/api/config/clusters/missing", wantStatus: http.StatusNotFound, wantText: "集群不存在"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := authorizedMutationRequest(testCase.method, testCase.path, strings.NewReader(testCase.body))
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, request)
			if recorder.Code != testCase.wantStatus || !strings.Contains(recorder.Body.String(), testCase.wantText) {
				t.Fatalf("expected status=%d text=%q, got status=%d body=%s", testCase.wantStatus, testCase.wantText, recorder.Code, recorder.Body.String())
			}
		})
	}
	persisted := loadClusterDefinitions(registryPath)
	if len(persisted) != 1 || persisted[0].ID != "managed-a" || persisted[0].NameServer != "managed-a-ns:9876" {
		t.Fatalf("rejected mutations changed persistence %#v", persisted)
	}
	config := app.configPayload()
	if len(config.Clusters) != 2 || config.Clusters[0].ID != "base" || config.Clusters[1].ID != "managed-a" {
		t.Fatalf("rejected mutations changed runtimes %#v", config.Clusters)
	}
}

// TestNormalizeBrokerAddressMappingsRejectsAmbiguousDefinitions 验证重复主机、非法 IP 和缺失 NameServer 映射会立即失败。
func TestNormalizeBrokerAddressMappingsRejectsAmbiguousDefinitions(t *testing.T) {
	tests := []struct {
		name       string
		nameServer string
		mappings   []BrokerAddressMapping
		wantText   string
	}{
		{
			name:       "duplicate host",
			nameServer: "172.168.1.49:9876",
			mappings:   []BrokerAddressMapping{{Host: "broker-a", IP: "172.168.1.49"}, {Host: "BROKER-A", IP: "172.168.1.50"}},
			wantText:   "不能重复",
		},
		{
			name:       "invalid ip",
			nameServer: "172.168.1.49:9876",
			mappings:   []BrokerAddressMapping{{Host: "broker-a", IP: "not-an-ip"}},
			wantText:   "IP 无效",
		},
		{
			name:       "missing nameserver host",
			nameServer: "rocket-server:9876",
			mappings:   []BrokerAddressMapping{{Host: "broker-a", IP: "172.168.1.49"}},
			wantText:   "NameServer 主机名也必须配置映射",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := normalizeBrokerAddressMappings(testCase.nameServer, testCase.mappings)
			if err == nil || !strings.Contains(err.Error(), testCase.wantText) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantText, err)
			}
		})
	}
}

// TestPublicClusterManagementExposesEditDeleteContract 锁定页面修改、删除和只读启动集群的交互契约。
func TestPublicClusterManagementExposesEditDeleteContract(t *testing.T) {
	index, err := os.ReadFile("public/index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`id="clusterManagementList"`, `id="clusterManagementCount"`, `id="clusterAddDialogTitle"`, `id="clusterAddBrokerMappings"`} {
		if !strings.Contains(string(index), expected) {
			t.Fatalf("public/index.html should expose cluster management control %q", expected)
		}
	}

	script, err := os.ReadFile("public/app.js")
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	for _, expected := range []string{
		"managedClusterIds",
		`data-cluster-action="edit"`,
		`data-cluster-action="delete"`,
		`method: editing ? "PUT" : "POST"`,
		`method: "DELETE"`,
		"启动配置集群不可在页面修改",
		"state.selectedClusterId = definition.id",
		`state.selectedClusterId = ""`,
		"brokerAddressMappings",
		"parseBrokerAddressMappings",
	} {
		if !strings.Contains(source, expected) {
			t.Fatalf("public/app.js should keep cluster management contract %q", expected)
		}
	}
}
