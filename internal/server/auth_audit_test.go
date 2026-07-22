package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const mutationTestToken = "dashboard-test-admin"

// mutationTestAppConfig 为写操作测试注入独立身份和内存审计，不依赖部署文件或真实凭据。
func mutationTestAppConfig(t *testing.T, config AppConfig) AppConfig {
	t.Helper()
	authenticator, err := NewTokenAuthenticator([]TokenCredential{{
		Token:   mutationTestToken,
		Subject: "dashboard-test-admin",
		Roles:   []string{"admin"},
	}})
	if err != nil {
		t.Fatalf("create test authenticator: %v", err)
	}
	config.Authenticator = authenticator
	config.AuditStore = NewMemoryAuditStore()
	return config
}

// authorizedMutationRequest 构造携带测试身份、操作原因和固定请求标识的写操作请求。
func authorizedMutationRequest(method string, target string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, target, body)
	request.Header.Set("Authorization", "Bearer "+mutationTestToken)
	request.Header.Set("X-RMQD-Operation-Reason", "server test")
	request.Header.Set("X-Request-ID", "server-test-request")
	return request
}

// TestMutationRejectsUnauthenticatedRequestAndAuditsDenial 验证默认拒绝写入并保留最小拒绝记录。
func TestMutationRejectsUnauthenticatedRequestAndAuditsDenial(t *testing.T) {
	store := NewMemoryAuditStore()
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, AuditStore: store, ClusterCacheTTL: time.Hour})
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/topics", bytes.NewBufferString(`{"topic":"codex_topic","clusterName":"DefaultCluster","readQueueNums":4,"writeQueueNums":4,"perm":6}`)))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if provider.upsertTopicCalls != 0 {
		t.Fatalf("expected provider not to be called, got %d", provider.upsertTopicCalls)
	}
	records, err := store.List(context.Background(), "default", 10)
	if err != nil {
		t.Fatalf("list audit records: %v", err)
	}
	if len(records) != 1 || records[0].Result != "denied" || records[0].Actor.Subject != "anonymous" {
		t.Fatalf("unexpected denial audit records %#v", records)
	}
}

// TestMutationAuditsAuthenticatedPermissionDenial 验证越权时保留已认证操作者而不是降级为匿名。
func TestMutationAuditsAuthenticatedPermissionDenial(t *testing.T) {
	authenticator, err := NewTokenAuthenticator([]TokenCredential{{Token: "auditor-token", Subject: "dashboard-auditor", Roles: []string{"auditor"}}})
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryAuditStore()
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, Authenticator: authenticator, AuditStore: store, ClusterCacheTTL: time.Hour})
	request := httptest.NewRequest(http.MethodPost, "/api/topics", bytes.NewBufferString(`{"topic":"codex_topic","clusterName":"DefaultCluster","readQueueNums":4,"writeQueueNums":4,"perm":6}`))
	request.Header.Set("Authorization", "Bearer auditor-token")
	request.Header.Set("X-RMQD-Operation-Reason", "permission test")
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	records, err := store.List(context.Background(), "default", 10)
	if err != nil {
		t.Fatalf("list audit records: %v", err)
	}
	if len(records) != 1 || records[0].Result != "denied" || records[0].Actor.Subject != "dashboard-auditor" {
		t.Fatalf("unexpected permission denial audit records %#v", records)
	}
}

// TestMutationPersistsStartCompletionAndVerification 验证成功写入可按操作标识关联开始、完成和读回信息。
func TestMutationPersistsStartCompletionAndVerification(t *testing.T) {
	provider := &fakeProvider{}
	config := mutationTestAppConfig(t, AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})
	store := config.AuditStore
	app := New(config)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, authorizedMutationRequest(http.MethodPost, "/api/topics", bytes.NewBufferString(`{"topic":"codex_topic","clusterName":"DefaultCluster","readQueueNums":4,"writeQueueNums":4,"perm":6}`)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	operationID := recorder.Header().Get("X-RMQD-Operation-ID")
	if operationID == "" {
		t.Fatal("expected operation identifier response header")
	}
	records, err := store.List(context.Background(), "default", 10)
	if err != nil {
		t.Fatalf("list audit records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected started and completed audit records, got %#v", records)
	}
	if records[0].OperationID != operationID || records[1].OperationID != operationID || records[0].Result != "pending" || records[1].Result != "succeeded" {
		t.Fatalf("unexpected operation audit records %#v", records)
	}
	if records[1].Actor.Subject != "dashboard-test-admin" || len(records[1].Verification) == 0 || records[1].RequestID != "server-test-request" {
		t.Fatalf("expected actor, verification and request id in completion record %#v", records[1])
	}
}
