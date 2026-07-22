package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

// TestNormalizeClusterDefinitionsKeepsDefaultSampleCluster 保证未配置 NameServer 时仍可启动样例或注入 Provider 的单集群应用。
func TestNormalizeClusterDefinitionsKeepsDefaultSampleCluster(t *testing.T) {
	definitions := normalizeClusterDefinitions(nil, "", nil)
	if len(definitions) != 1 {
		t.Fatalf("expected one default cluster, got %#v", definitions)
	}
	definition := definitions[0]
	if definition.ID != "default" || definition.Label != "默认集群" || definition.NameServer != "" {
		t.Fatalf("unexpected default sample cluster %#v", definition)
	}
}

// TestNormalizeClusterDefinitionsRejectsExplicitClusterWithoutNameServer 保证多集群部署不会因缺失连接目标而悄然启动。
func TestNormalizeClusterDefinitionsRejectsExplicitClusterWithoutNameServer(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected explicit cluster without NameServer to panic")
		}
	}()
	normalizeClusterDefinitions([]ClusterDefinition{{ID: "prod", Label: "生产集群"}}, "", nil)
}

// clusterScopedProvider 为隔离测试提供不同的 Topic 数据，同时复用其余 Provider 行为。
type clusterScopedProvider struct {
	*fakeProvider
	topicName string
}

// TopicList 返回所属集群独有的 Topic，用于验证缓存和请求没有串到其他 Provider。
func (p *clusterScopedProvider) TopicList(context.Context) ([]rocketmq.Topic, error) {
	p.topicCalls++
	return []rocketmq.Topic{{Name: p.topicName, Kind: "normal"}}, nil
}

// clusterRuntimeForTest 读取指定集群的运行时，保持测试对生产请求隔离机制的显式认知。
func clusterRuntimeForTest(t *testing.T, app *App, clusterID string) *clusterRuntime {
	t.Helper()
	app.clusterMu.RLock()
	defer app.clusterMu.RUnlock()
	runtime, ok := app.clusters[clusterID]
	if !ok {
		t.Fatalf("cluster runtime %q is missing", clusterID)
	}
	return runtime
}

// TestMultiClusterRequestsIsolateProviderCacheAndAudit 验证请求、快照、写入和审计均按 clusterId 隔离。
func TestMultiClusterRequestsIsolateProviderCacheAndAudit(t *testing.T) {
	providers := map[string]*clusterScopedProvider{}
	config := mutationTestAppConfig(t, AppConfig{
		ProviderFactory: func(nameServer string) rocketmq.Provider {
			provider := &clusterScopedProvider{fakeProvider: &fakeProvider{}, topicName: nameServer + "-topic"}
			providers[nameServer] = provider
			return provider
		},
		Clusters: []ClusterDefinition{
			{ID: "cluster-a", Label: "集群 A", NameServer: "ns-a:9876"},
			{ID: "cluster-b", Label: "集群 B", NameServer: "ns-b:9876"},
		},
		ClusterCacheTTL: time.Hour,
	})
	store := config.AuditStore
	app := New(config)
	for _, clusterID := range []string{"cluster-a", "cluster-b"} {
		waitForSnapshot(t, clusterRuntimeForTest(t, app, clusterID).topicSnapshot)
	}

	for _, testCase := range []struct {
		clusterID string
		expected  string
	}{
		{clusterID: "cluster-a", expected: "ns-a:9876-topic"},
		{clusterID: "cluster-b", expected: "ns-b:9876-topic"},
	} {
		recorder := httptest.NewRecorder()
		app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/topics?clusterId="+testCase.clusterID, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("cluster %s expected 200, got %d body=%s", testCase.clusterID, recorder.Code, recorder.Body.String())
		}
		var payload responsePayload[[]rocketmq.Topic]
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode cluster %s topics: %v", testCase.clusterID, err)
		}
		if len(payload.Data) != 1 || payload.Data[0].Name != testCase.expected {
			t.Fatalf("cluster %s received foreign topic data %#v", testCase.clusterID, payload.Data)
		}
	}

	write := httptest.NewRecorder()
	app.ServeHTTP(write, authorizedMutationRequest(http.MethodPost, "/api/topics?clusterId=cluster-b", bytes.NewBufferString(`{"topic":"isolated_topic","clusterName":"DefaultCluster","readQueueNums":4,"writeQueueNums":4,"perm":6}`)))
	if write.Code != http.StatusOK {
		t.Fatalf("expected cluster-b write success, got %d body=%s", write.Code, write.Body.String())
	}
	if providers["ns-a:9876"].upsertTopicCalls != 0 || providers["ns-b:9876"].upsertTopicCalls != 1 {
		t.Fatalf("write crossed provider boundary a=%d b=%d", providers["ns-a:9876"].upsertTopicCalls, providers["ns-b:9876"].upsertTopicCalls)
	}
	auditRecords, err := store.List(context.Background(), "cluster-b", 10)
	if err != nil {
		t.Fatalf("list cluster-b audit: %v", err)
	}
	if len(auditRecords) != 2 || auditRecords[0].ClusterID != "cluster-b" || auditRecords[1].ClusterID != "cluster-b" {
		t.Fatalf("unexpected cluster-b audit records %#v", auditRecords)
	}
	otherRecords, err := store.List(context.Background(), "cluster-a", 10)
	if err != nil {
		t.Fatalf("list cluster-a audit: %v", err)
	}
	if len(otherRecords) != 0 {
		t.Fatalf("cluster-a should not contain cluster-b records %#v", otherRecords)
	}

	missingCluster := httptest.NewRecorder()
	app.ServeHTTP(missingCluster, httptest.NewRequest(http.MethodGet, "/api/topics", nil))
	if missingCluster.Code != http.StatusBadRequest {
		t.Fatalf("expected missing clusterId rejection, got %d body=%s", missingCluster.Code, missingCluster.Body.String())
	}
	conflictingCluster := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/topics?clusterId=cluster-a", nil)
	request.Header.Set("X-RMQD-Cluster-ID", "cluster-b")
	app.ServeHTTP(conflictingCluster, request)
	if conflictingCluster.Code != http.StatusBadRequest {
		t.Fatalf("expected conflicting clusterId rejection, got %d body=%s", conflictingCluster.Code, conflictingCluster.Body.String())
	}
}
