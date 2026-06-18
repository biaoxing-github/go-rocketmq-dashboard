package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

type fakeProvider struct {
	clusterCalls          int
	brokerStatusCalls     int
	topicCalls            int
	topicRouteCalls       int
	topicStatusCalls      int
	topicMessageCalls     int
	upsertTopicCalls      int
	deleteTopicCalls      int
	sendTopicMessageCalls int
	consumerCalls         int
	consumerDetailCalls   int
	resetOffsetCalls      int
	messageChainCalls     int
	lastTopicMutation     rocketmq.TopicConfigMutation
	lastTopicDelete       rocketmq.TopicDeleteRequest
	lastMessageSend       rocketmq.TopicMessageSendRequest
	lastOffsetReset       rocketmq.ConsumerOffsetResetRequest
	lastMessageChainQuery rocketmq.MessageQuery
}

func (p *fakeProvider) ClusterList(ctx context.Context) ([]rocketmq.Cluster, error) {
	p.clusterCalls++
	return []rocketmq.Cluster{{
		Name: "DefaultCluster",
		Brokers: []rocketmq.Broker{{
			Cluster:   "DefaultCluster",
			Name:      "broker-a",
			Address:   "127.0.0.1:10911",
			Version:   "V5_2_0",
			Activated: true,
		}},
	}}, nil
}

func (p *fakeProvider) BrokerStatus(ctx context.Context, brokerAddr string) (rocketmq.BrokerStatus, error) {
	p.brokerStatusCalls++
	return rocketmq.BrokerStatus{
		BrokerAddr:         brokerAddr,
		BrokerVersionDesc:  "V5_2_0",
		BrokerRole:         "ASYNC_MASTER",
		BootTimestamp:      "2026-06-07 09:28:00",
		PutTps:             "1.00 0.50 0.10",
		GetFoundTps:        "0.80 0.40 0.08",
		GetTotalTps:        "0.90 0.50 0.10",
		CommitLogCapacity:  "Total : 100 GiB, Free : 88 GiB.",
		DispatchBehind:     "0",
		RuntimeDescription: "ASYNC_MASTER · V5_2_0 · PUT 1.00 0.50 0.10 · GET 0.80 0.40 0.08",
		Metrics: []rocketmq.BrokerRuntimeMetric{
			{Key: "brokerRole", Value: "ASYNC_MASTER"},
			{Key: "brokerVersionDesc", Value: "V5_2_0"},
			{Key: "putTps", Value: "1.00 0.50 0.10"},
			{Key: "getFoundTps", Value: "0.80 0.40 0.08"},
			{Key: "getTotalTps", Value: "0.90 0.50 0.10"},
			{Key: "commitLogDirCapacity", Value: "Total : 100 GiB, Free : 88 GiB."},
			{Key: "dispatchBehindBytes", Value: "0"},
		},
	}, nil
}

func (p *fakeProvider) TopicList(ctx context.Context) ([]rocketmq.Topic, error) {
	p.topicCalls++
	return []rocketmq.Topic{
		{Name: "sample_notice_topic", Kind: "normal"},
		{Name: "%RETRY%sample-order-events-consumer", Kind: "retry"},
	}, nil
}

func (p *fakeProvider) TopicRoute(ctx context.Context, topic string) (rocketmq.TopicRoute, error) {
	p.topicRouteCalls++
	return rocketmq.TopicRoute{
		Topic:            topic,
		TotalReadQueues:  4,
		TotalWriteQueues: 4,
		Queues: []rocketmq.TopicQueueRoute{{
			BrokerName:      "broker-a",
			ReadQueueNums:   4,
			WriteQueueNums:  4,
			Perm:            6,
			PermissionLabel: "RW",
		}},
		Brokers: []rocketmq.TopicBrokerRoute{{
			Cluster:    "DefaultCluster",
			BrokerName: "broker-a",
			Addrs:      map[string]string{"0": "127.0.0.1:10911"},
		}},
	}, nil
}

func (p *fakeProvider) TopicStatus(ctx context.Context, topic string) (rocketmq.TopicStatus, error) {
	p.topicStatusCalls++
	return rocketmq.TopicStatus{
		Topic:             topic,
		TotalQueues:       4,
		TotalMessageCount: 5,
		MinOffsetTotal:    0,
		MaxOffsetTotal:    5,
		Rows: []rocketmq.TopicStatusRow{
			{BrokerName: "broker-a", QueueID: 0, MinOffset: 0, MaxOffset: 1, MessageCount: 1, LastUpdated: "2026-06-05 16:20:48,715"},
			{BrokerName: "broker-a", QueueID: 1, MinOffset: 0, MaxOffset: 2, MessageCount: 2, LastUpdated: "2026-06-06 23:45:34,278"},
			{BrokerName: "broker-a", QueueID: 2, MinOffset: 0, MaxOffset: 0, MessageCount: 0, LastUpdated: ""},
			{BrokerName: "broker-a", QueueID: 3, MinOffset: 0, MaxOffset: 2, MessageCount: 2, LastUpdated: "2026-06-05 22:04:56,759"},
		},
	}, nil
}

func (p *fakeProvider) TopicMessages(ctx context.Context, query rocketmq.MessageBrowseQuery) (rocketmq.TopicMessages, error) {
	p.topicMessageCalls++
	return rocketmq.TopicMessages{
		Topic:          query.Topic,
		BrokerName:     query.BrokerName,
		QueueID:        query.QueueID,
		Limit:          query.Limit,
		ScannedOffsets: 2,
		Rows: []rocketmq.MessageDetail{
			{
				MessageID:      "abc",
				Topic:          query.Topic,
				BrokerName:     "broker-a",
				Keys:           []string{"order_created"},
				StoreTimestamp: 1717651200000,
				QueueID:        0,
				QueueOffset:    1,
				StoreHost:      "127.0.0.1:10911",
				BodyPreview:    "{\"event\":\"created\"}",
			},
		},
	}, nil
}

type incrementalTopicMessagesProvider struct {
	fakeProvider

	incrementalCalls int
	previousRows     int
	previousMessage  string
}

func (p *incrementalTopicMessagesProvider) TopicMessagesIncremental(ctx context.Context, query rocketmq.MessageBrowseQuery, previous rocketmq.TopicMessages) (rocketmq.TopicMessages, error) {
	p.incrementalCalls++
	p.previousRows = len(previous.Rows)
	if len(previous.Rows) > 0 {
		p.previousMessage = previous.Rows[0].MessageID
	}
	rows := append([]rocketmq.MessageDetail{}, previous.Rows...)
	rows = append(rows, rocketmq.MessageDetail{
		MessageID:      "fresh",
		Topic:          query.Topic,
		BrokerName:     "broker-a",
		StoreTimestamp: 1717651300000,
		QueueID:        0,
		QueueOffset:    2,
		StoreHost:      "127.0.0.1:10911",
		BodyPreview:    "{\"event\":\"fresh\"}",
	})
	return rocketmq.TopicMessages{
		Topic:          query.Topic,
		BrokerName:     query.BrokerName,
		QueueID:        query.QueueID,
		Limit:          query.Limit,
		ScannedOffsets: len(rows),
		Rows:           rows,
	}, nil
}

func (p *fakeProvider) UpsertTopic(ctx context.Context, request rocketmq.TopicConfigMutation) (rocketmq.TopicMutationResult, error) {
	p.upsertTopicCalls++
	p.lastTopicMutation = request
	return rocketmq.TopicMutationResult{
		Topic:     request.Topic,
		Operation: "upsertTopic",
		Target:    request.TargetLabel(),
		Output:    "topic updated",
	}, nil
}

func (p *fakeProvider) DeleteTopic(ctx context.Context, request rocketmq.TopicDeleteRequest) (rocketmq.TopicMutationResult, error) {
	p.deleteTopicCalls++
	p.lastTopicDelete = request
	return rocketmq.TopicMutationResult{
		Topic:     request.Topic,
		Operation: "deleteTopic",
		Target:    request.ClusterName,
		Output:    "topic deleted",
	}, nil
}

func (p *fakeProvider) SendTopicMessage(ctx context.Context, request rocketmq.TopicMessageSendRequest) (rocketmq.TopicMessageSendResult, error) {
	p.sendTopicMessageCalls++
	p.lastMessageSend = request
	return rocketmq.TopicMessageSendResult{
		Topic:      request.Topic,
		Operation:  "sendMessage",
		BrokerName: "broker-a",
		QueueID:    1,
		SendStatus: "SEND_OK",
		MessageID:  "msg-001",
		Output:     "message sent",
	}, nil
}

func (p *fakeProvider) ConsumerGroups(ctx context.Context) ([]rocketmq.ConsumerGroup, error) {
	p.consumerCalls++
	return []rocketmq.ConsumerGroup{
		{Name: "sample-order-events-consumer", Count: 1, Version: "V5_3_2", Type: "PUSH", Model: "CLUSTERING", TPS: "0", DiffTotal: 0, Online: true},
		{Name: "sample-offline-consumer", Count: 0, Version: "OFFLINE", TPS: "0", DiffTotal: 22653, Online: false},
	}, nil
}

func (p *fakeProvider) ConsumerDetail(ctx context.Context, group string, topic string) (rocketmq.ConsumerDetail, error) {
	p.consumerDetailCalls++
	return rocketmq.ConsumerDetail{
		Group:            group,
		Topic:            topic,
		ConsumeType:      "CONSUME_PASSIVELY",
		MessageModel:     "CLUSTERING",
		ConsumeFromWhere: "CONSUME_FROM_LAST_OFFSET",
		Connections: []rocketmq.ConsumerConnection{{
			ClientID:   "127.0.0.1@1#9790766882943522",
			ClientAddr: "127.0.0.1:44832",
			Language:   "JAVA",
			Version:    "V5_3_2",
		}},
		Subscriptions: []rocketmq.ConsumerSubscription{{
			Topic:      "sample_order_events_topic",
			Expression: "order_created",
		}},
		ProgressRows: []rocketmq.ConsumerProgressRow{{
			Topic:          topic,
			BrokerName:     "broker-a",
			QueueID:        0,
			BrokerOffset:   1,
			ConsumerOffset: 1,
			ClientIP:       "127.0.0.1",
			Diff:           0,
			Inflight:       0,
			LastTime:       "2026-06-05 16:20:48",
		}},
	}, nil
}

func (p *fakeProvider) ResetConsumerOffset(ctx context.Context, request rocketmq.ConsumerOffsetResetRequest) (rocketmq.ConsumerOffsetResetResult, error) {
	p.resetOffsetCalls++
	p.lastOffsetReset = request
	return rocketmq.ConsumerOffsetResetResult{
		Group:     request.Group,
		Topic:     request.Topic,
		Operation: "resetOffsetByTime",
		Timestamp: request.Normalized().Timestamp,
		Target:    request.TargetLabel(),
		Output:    "consumer offset reset",
	}, nil
}

func (p *fakeProvider) MessageChain(ctx context.Context, query rocketmq.MessageQuery) (rocketmq.MessageStatusChain, error) {
	p.messageChainCalls++
	p.lastMessageChainQuery = query
	return rocketmq.BuildMessageStatusChain(
		rocketmq.MessageDetail{MessageID: query.MessageID, Topic: query.Topic, StoreTimestamp: 1717651200000},
		[]rocketmq.TraceEvent{{Stage: "SEND_SUCCESS", Group: "producer", Timestamp: 1717651200100, Detail: "发送成功"}},
		[]rocketmq.ConsumerState{{Group: "consumer", Topic: query.Topic, Status: "CONSUMED", Lag: 0}},
	), nil
}

func (p *fakeProvider) BrokerStatusCalls() int {
	return p.brokerStatusCalls
}

func TestClustersEndpointReturnsBrokerVersionAndUsesCache(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	waitForSnapshot(t, app.clusterSnapshot)

	first := httptest.NewRecorder()
	app.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/clusters", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	app.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/clusters", nil))
	if second.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", second.Code, second.Body.String())
	}
	if provider.clusterCalls != 1 {
		t.Fatalf("expected cache to avoid second provider call, got %d calls", provider.clusterCalls)
	}

	var payload responsePayload[[]rocketmq.Cluster]
	if err := json.Unmarshal(first.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Data[0].Brokers[0].Version != "V5_2_0" {
		t.Fatalf("version mismatch: %#v", payload.Data)
	}
	if payload.LatencyMillis < 0 {
		t.Fatalf("latency should be non-negative")
	}
	if !payload.CacheHit || payload.Refreshing || payload.Stale || !payload.HasData {
		t.Fatalf("expected hot snapshot metadata, got %#v", payload)
	}
}

func TestBrokerStatusEndpointReturnsRuntimeMetricsAndUsesCache(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	brokerAddr := "127.0.0.1:10911"

	first := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/broker-status?brokerAddr="+brokerAddr, nil)
	app.ServeHTTP(first, request)
	if first.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", first.Code, first.Body.String())
	}
	waitForSnapshot(t, app.brokerStatusSnapshots.snapshot(brokerAddr))

	cached := httptest.NewRecorder()
	app.ServeHTTP(cached, request)

	var payload responsePayload[rocketmq.BrokerStatus]
	if err := json.Unmarshal(cached.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.CacheHit || payload.Refreshing || payload.Stale {
		t.Fatalf("expected hot cached broker status metadata, got %#v", payload)
	}
	if payload.Data.BrokerAddr != brokerAddr || payload.Data.BrokerVersionDesc != "V5_2_0" || payload.Data.BrokerRole != "ASYNC_MASTER" {
		t.Fatalf("unexpected broker status payload: %#v", payload.Data)
	}
	if len(payload.Data.Metrics) < 7 || payload.Data.RuntimeDescription == "" {
		t.Fatalf("expected runtime metrics and description, got %#v", payload.Data)
	}
	if provider.BrokerStatusCalls() != 1 {
		t.Fatalf("expected cached broker status to avoid second provider call, got %d calls", provider.BrokerStatusCalls())
	}
}

func TestBrokerStatusEndpointReturnsFastBeforeSlowProviderFinishes(t *testing.T) {
	provider := &blockingBrokerStatusProvider{
		fakeProvider: fakeProvider{},
		started:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	brokerAddr := "127.0.0.1:10911"

	start := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/broker-status?brokerAddr="+brokerAddr, nil)
	app.ServeHTTP(recorder, request)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected broker status response without waiting for provider")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing response, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[rocketmq.BrokerStatus]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.HasData || !payload.Refreshing || !payload.Stale || payload.CacheHit {
		t.Fatalf("unexpected cold broker status metadata: %#v", payload)
	}
	if payload.Data.BrokerAddr != brokerAddr {
		t.Fatalf("expected placeholder to preserve broker address, got %#v", payload.Data)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatalf("broker status refresh did not start")
	}
	close(provider.release)
	waitForSnapshot(t, app.brokerStatusSnapshots.snapshot(brokerAddr))
}

func TestRefreshEndpointForcesSnapshotsBeforeTTLExpires(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})
	waitForSnapshot(t, app.clusterSnapshot)
	waitForSnapshot(t, app.topicSnapshot)
	waitForSnapshot(t, app.consumerSnapshot)

	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/refresh", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[refreshTriggerPayload]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.Data.Clusters || !payload.Data.Topics || !payload.Data.Consumers {
		t.Fatalf("expected all snapshot refreshes to be triggered, got %#v", payload.Data)
	}
	waitForProviderCalls(t, provider, 2, 2, 2)
}

func TestTopicRouteEndpointReturnsBrokerRouting(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/topic-route?topic=sample_order_events_topic", nil)
	app.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	waitForSnapshot(t, app.topicRouteSnapshots.snapshot("sample_order_events_topic"))

	cached := httptest.NewRecorder()
	app.ServeHTTP(cached, request)

	var payload responsePayload[rocketmq.TopicRoute]
	if err := json.Unmarshal(cached.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.CacheHit || payload.Refreshing || payload.Stale {
		t.Fatalf("expected hot cached topic route metadata, got %#v", payload)
	}
	if payload.Data.Topic != "sample_order_events_topic" || payload.Data.TotalReadQueues != 4 {
		t.Fatalf("unexpected topic route payload: %#v", payload.Data)
	}
	if payload.Data.Brokers[0].Addrs["0"] != "127.0.0.1:10911" {
		t.Fatalf("unexpected broker addrs: %#v", payload.Data.Brokers)
	}
	if provider.topicRouteCalls != 1 {
		t.Fatalf("expected cached topic route to avoid second provider call, got %d calls", provider.topicRouteCalls)
	}
}

func TestTopicStatusEndpointReturnsQueueWatermarks(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/topic-status?topic=sample_order_events_topic", nil)
	app.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	waitForSnapshot(t, app.topicStatusSnapshots.snapshot("sample_order_events_topic"))

	cached := httptest.NewRecorder()
	app.ServeHTTP(cached, request)

	var payload responsePayload[rocketmq.TopicStatus]
	if err := json.Unmarshal(cached.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.CacheHit || payload.Refreshing || payload.Stale {
		t.Fatalf("expected hot cached topic status metadata, got %#v", payload)
	}
	if payload.Data.Topic != "sample_order_events_topic" || payload.Data.TotalQueues != 4 || payload.Data.TotalMessageCount != 5 {
		t.Fatalf("unexpected topic status payload: %#v", payload.Data)
	}
	if payload.Data.Rows[1].QueueID != 1 || payload.Data.Rows[1].MessageCount != 2 || payload.Data.Rows[1].LastUpdated != "2026-06-06 23:45:34,278" {
		t.Fatalf("unexpected topic status row: %#v", payload.Data.Rows[1])
	}
	if provider.topicStatusCalls != 1 {
		t.Fatalf("expected cached topic status to avoid second provider call, got %d calls", provider.topicStatusCalls)
	}
}

func TestTopicMessagesEndpointReturnsRecentMessagesAndUsesCache(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	request := httptest.NewRequest(http.MethodGet, "/api/topic-messages?topic=sample_order_events_topic&limit=8", nil)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	key, err := messageBrowseCacheKey(rocketmq.MessageBrowseQuery{Topic: "sample_order_events_topic", QueueID: -1, Limit: 8})
	if err != nil {
		t.Fatalf("build message browse cache key: %v", err)
	}
	waitForSnapshot(t, app.topicMessageSnapshots.snapshot(key))

	cached := httptest.NewRecorder()
	app.ServeHTTP(cached, request)
	var payload responsePayload[rocketmq.TopicMessages]
	if err := json.Unmarshal(cached.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.CacheHit || payload.Refreshing || payload.Stale {
		t.Fatalf("expected hot cached topic messages metadata, got %#v", payload)
	}
	if len(payload.Data.Rows) != 1 || payload.Data.Rows[0].MessageID != "abc" || payload.Data.Rows[0].BrokerName != "broker-a" {
		t.Fatalf("unexpected topic messages payload: %#v", payload.Data)
	}
	if provider.topicMessageCalls != 1 {
		t.Fatalf("expected cached topic messages to avoid second provider call, got %d calls", provider.topicMessageCalls)
	}
}

func TestTopicMessagesRefreshPassesPreviousSnapshotToIncrementalProvider(t *testing.T) {
	provider := &incrementalTopicMessagesProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})

	request := httptest.NewRequest(http.MethodGet, "/api/topic-messages?topic=sample_order_events_topic&limit=8", nil)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	key, err := messageBrowseCacheKey(rocketmq.MessageBrowseQuery{Topic: "sample_order_events_topic", QueueID: -1, Limit: 8})
	if err != nil {
		t.Fatalf("build message browse cache key: %v", err)
	}
	store := app.topicMessageSnapshots.snapshot(key)
	waitForSnapshot(t, store)

	refresh := httptest.NewRecorder()
	app.ServeHTTP(refresh, httptest.NewRequest(http.MethodGet, "/api/topic-messages?topic=sample_order_events_topic&limit=8&refresh=true", nil))
	if refresh.Code != http.StatusOK {
		t.Fatalf("expected 200 on refresh, got %d body=%s", refresh.Code, refresh.Body.String())
	}
	waitForTopicMessageIncrementalCall(t, provider, store)

	if provider.incrementalCalls != 1 {
		t.Fatalf("expected one incremental refresh, got %d", provider.incrementalCalls)
	}
	if provider.previousRows != 1 || provider.previousMessage != "abc" {
		t.Fatalf("expected previous message snapshot, rows=%d message=%q", provider.previousRows, provider.previousMessage)
	}
	view := store.view(time.Now())
	if len(view.Data.Rows) != 2 || view.Data.Rows[1].MessageID != "fresh" {
		t.Fatalf("expected incremental snapshot rows, got %#v", view.Data.Rows)
	}
}

func TestMessageChainEndpointReturnsTimeline(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	request := httptest.NewRequest(http.MethodGet, "/api/message-chain?topic=sample_notice_topic&messageId=abc", nil)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	key, err := messageChainCacheKey(rocketmq.MessageQuery{Topic: "sample_notice_topic", MessageID: "abc"})
	if err != nil {
		t.Fatalf("build message chain cache key: %v", err)
	}
	waitForSnapshot(t, app.messageChainSnapshots.snapshot(key))

	cached := httptest.NewRecorder()
	app.ServeHTTP(cached, request)

	var payload responsePayload[rocketmq.MessageStatusChain]
	if err := json.Unmarshal(cached.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.CacheHit || payload.Refreshing || payload.Stale {
		t.Fatalf("expected hot cached message chain metadata, got %#v", payload)
	}
	if payload.Data.OverallStatus != "CONSUMED" {
		t.Fatalf("expected consumed chain, got %#v", payload.Data)
	}
	if provider.messageChainCalls != 1 {
		t.Fatalf("expected cached message chain to avoid second provider call, got %d calls", provider.messageChainCalls)
	}
}

func TestMessageChainEndpointUsesDedicatedLongCacheTTL(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{
		Provider:             provider,
		ClusterCacheTTL:      10 * time.Millisecond,
		MessageChainCacheTTL: time.Hour,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/message-chain?topic=sample_notice_topic&messageId=abc", nil)

	first := httptest.NewRecorder()
	app.ServeHTTP(first, request)
	key, err := messageChainCacheKey(rocketmq.MessageQuery{Topic: "sample_notice_topic", MessageID: "abc"})
	if err != nil {
		t.Fatalf("build message chain cache key: %v", err)
	}
	waitForSnapshot(t, app.messageChainSnapshots.snapshot(key))
	time.Sleep(30 * time.Millisecond)

	second := httptest.NewRecorder()
	app.ServeHTTP(second, request)
	var payload responsePayload[rocketmq.MessageStatusChain]
	if err := json.Unmarshal(second.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || payload.Stale || !payload.CacheHit {
		t.Fatalf("expected message chain cache to stay fresh beyond cluster ttl, got %#v", payload)
	}
	if provider.messageChainCalls != 1 {
		t.Fatalf("expected dedicated chain ttl to avoid refresh, got %d calls", provider.messageChainCalls)
	}
}

func TestMessageChainEndpointParsesQueueLocationAndTimeWindow(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	url := "/api/message-chain?topic=sample_notice_topic&messageId=abc&brokerName=broker-a&queueId=3&queueOffset=10240&beginTimestamp=1717650000000&endTimestamp=1717653600000"

	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, url, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	query := rocketmq.MessageQuery{
		Topic:          "sample_notice_topic",
		MessageID:      "abc",
		BrokerName:     "broker-a",
		QueueID:        3,
		QueueOffset:    10240,
		HasQueueOffset: true,
		BeginTimestamp: 1717650000000,
		EndTimestamp:   1717653600000,
	}
	key, err := messageChainCacheKey(query)
	if err != nil {
		t.Fatalf("build message chain cache key: %v", err)
	}
	waitForSnapshot(t, app.messageChainSnapshots.snapshot(key))

	if provider.lastMessageChainQuery != query {
		t.Fatalf("unexpected provider query\nexpected=%#v\nactual=%#v", query, provider.lastMessageChainQuery)
	}
}

func TestTopicsEndpointReturnsClassifiedTopics(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	waitForSnapshot(t, app.topicSnapshot)

	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/topics", nil))
	second := httptest.NewRecorder()
	app.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/topics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if second.Code != http.StatusOK {
		t.Fatalf("expected 200 on cached call, got %d body=%s", second.Code, second.Body.String())
	}
	if provider.topicCalls != 1 {
		t.Fatalf("expected topics cache to avoid second provider call, got %d calls", provider.topicCalls)
	}
	var payload responsePayload[[]rocketmq.Topic]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Data) != 2 || payload.Data[1].Kind != "retry" {
		t.Fatalf("unexpected topics payload: %#v", payload.Data)
	}
}

func TestTopicUpsertEndpointCallsProviderAndRefreshesTopics(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})
	waitForSnapshot(t, app.topicSnapshot)

	body := bytes.NewBufferString(`{"topic":"codex_topic","clusterName":"DefaultCluster","readQueueNums":4,"writeQueueNums":4,"perm":6,"order":true,"attributes":"+message.type=NORMAL"}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/topics", body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	if provider.upsertTopicCalls != 1 {
		t.Fatalf("expected provider upsert once, got %d", provider.upsertTopicCalls)
	}
	if provider.lastTopicMutation.Topic != "codex_topic" || provider.lastTopicMutation.ClusterName != "DefaultCluster" {
		t.Fatalf("unexpected mutation request: %#v", provider.lastTopicMutation)
	}
	if provider.lastTopicMutation.ReadQueueNums != 4 || !provider.lastTopicMutation.Order {
		t.Fatalf("expected queue config and order flag, got %#v", provider.lastTopicMutation)
	}
	waitForTopicCalls(t, provider, 2)

	var payload responsePayload[rocketmq.TopicMutationResult]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Data.Topic != "codex_topic" || payload.Data.Operation != "upsertTopic" {
		t.Fatalf("unexpected mutation result: %#v", payload.Data)
	}
}

func TestTopicDeleteEndpointCallsProviderAndRefreshesTopics(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})
	waitForSnapshot(t, app.topicSnapshot)

	body := bytes.NewBufferString(`{"topic":"codex_topic","clusterName":"DefaultCluster"}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/topics", body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	if provider.deleteTopicCalls != 1 {
		t.Fatalf("expected provider delete once, got %d", provider.deleteTopicCalls)
	}
	if provider.lastTopicDelete.Topic != "codex_topic" || provider.lastTopicDelete.ClusterName != "DefaultCluster" {
		t.Fatalf("unexpected delete request: %#v", provider.lastTopicDelete)
	}
	waitForTopicCalls(t, provider, 2)
}

func TestTopicMessageSendEndpointCallsProvider(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})
	queueID := 1

	body := bytes.NewBufferString(`{"topic":"codex_topic","body":"{\"hello\":\"rocketmq\"}","keys":"codex-key","tags":"qa","brokerName":"broker-a","queueId":1,"traceEnable":true}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/topic-messages/send", body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	if provider.sendTopicMessageCalls != 1 {
		t.Fatalf("expected provider send once, got %d", provider.sendTopicMessageCalls)
	}
	if provider.lastMessageSend.Topic != "codex_topic" || provider.lastMessageSend.Keys != "codex-key" {
		t.Fatalf("unexpected send request: %#v", provider.lastMessageSend)
	}
	if provider.lastMessageSend.QueueID == nil || *provider.lastMessageSend.QueueID != queueID || !provider.lastMessageSend.TraceEnable {
		t.Fatalf("expected queue and trace fields, got %#v", provider.lastMessageSend)
	}

	var payload responsePayload[rocketmq.TopicMessageSendResult]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Data.MessageID != "msg-001" || payload.Data.Operation != "sendMessage" {
		t.Fatalf("unexpected send result: %#v", payload.Data)
	}
}

func TestTopicMessageSendEndpointRejectsMissingBody(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})

	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"topic":"codex_topic"}`)
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/topic-messages/send", body))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if provider.sendTopicMessageCalls != 0 {
		t.Fatalf("expected provider not to be called, got %d", provider.sendTopicMessageCalls)
	}
}

func TestTopicUpsertEndpointRejectsMissingTarget(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})

	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"topic":"codex_topic","readQueueNums":4,"writeQueueNums":4}`)
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/topics", body))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if provider.upsertTopicCalls != 0 {
		t.Fatalf("expected provider not to be called, got %d", provider.upsertTopicCalls)
	}
}

func TestConsumerOffsetResetEndpointCallsProvider(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})

	body := bytes.NewBufferString(`{"group":"codex-group","topic":"codex_topic","timestamp":"now","force":true}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/consumer-offset/reset", body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	if provider.resetOffsetCalls != 1 {
		t.Fatalf("expected provider reset once, got %d", provider.resetOffsetCalls)
	}
	if provider.lastOffsetReset.Group != "codex-group" || provider.lastOffsetReset.Topic != "codex_topic" || !provider.lastOffsetReset.Force {
		t.Fatalf("unexpected reset request: %#v", provider.lastOffsetReset)
	}

	var payload responsePayload[rocketmq.ConsumerOffsetResetResult]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Data.Operation != "resetOffsetByTime" || payload.Data.Timestamp != "now" {
		t.Fatalf("unexpected reset result: %#v", payload.Data)
	}
}

func TestConsumerOffsetResetEndpointRejectsMissingGroup(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Hour})

	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"topic":"codex_topic","timestamp":"now"}`)
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/consumer-offset/reset", body))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if provider.resetOffsetCalls != 0 {
		t.Fatalf("expected provider not to be called, got %d", provider.resetOffsetCalls)
	}
}

func TestConsumersEndpointReturnsLagAndOnlineState(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	waitForSnapshot(t, app.consumerSnapshot)

	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/consumers", nil))
	second := httptest.NewRecorder()
	app.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/consumers", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if second.Code != http.StatusOK {
		t.Fatalf("expected 200 on cached call, got %d body=%s", second.Code, second.Body.String())
	}
	if provider.consumerCalls != 1 {
		t.Fatalf("expected consumers cache to avoid second provider call, got %d calls", provider.consumerCalls)
	}
	var payload responsePayload[[]rocketmq.ConsumerGroup]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Data) != 2 || payload.Data[0].Version != "V5_3_2" || payload.Data[1].DiffTotal != 22653 {
		t.Fatalf("unexpected consumers payload: %#v", payload.Data)
	}
}

func TestConsumerDetailEndpointReturnsConnectionsAndProgress(t *testing.T) {
	provider := &fakeProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/consumer-detail?group=sample-order-events-consumer&topic=sample_order_events_topic", nil)
	app.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	waitForSnapshot(t, app.consumerDetailSnapshots.snapshot(consumerDetailCacheKey("sample-order-events-consumer", "sample_order_events_topic")))

	cached := httptest.NewRecorder()
	app.ServeHTTP(cached, request)

	var payload responsePayload[rocketmq.ConsumerDetail]
	if err := json.Unmarshal(cached.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.CacheHit || payload.Refreshing || payload.Stale {
		t.Fatalf("expected hot cached consumer detail metadata, got %#v", payload)
	}
	if payload.Data.Group != "sample-order-events-consumer" || len(payload.Data.Connections) != 1 {
		t.Fatalf("unexpected consumer detail: %#v", payload.Data)
	}
	if payload.Data.ProgressRows[0].BrokerName != "broker-a" || payload.Data.Subscriptions[0].Expression != "order_created" {
		t.Fatalf("unexpected consumer detail rows: %#v", payload.Data)
	}
	if provider.consumerDetailCalls != 1 {
		t.Fatalf("expected cached detail to avoid second provider call, got %d calls", provider.consumerDetailCalls)
	}
}

func TestEndpointReturnsFastRefreshingSnapshotBeforeSlowProviderFinishes(t *testing.T) {
	provider := &blockingProvider{started: make(chan struct{}), release: make(chan struct{})}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	<-provider.started

	start := time.Now()
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/clusters", nil))
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected snapshot response without waiting for provider")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing response, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[[]rocketmq.Cluster]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.HasData || !payload.Refreshing || !payload.Stale || payload.CacheHit {
		t.Fatalf("unexpected cold snapshot metadata: %#v", payload)
	}
	close(provider.release)
}

func TestTopicRouteEndpointReturnsFastBeforeSlowProviderFinishes(t *testing.T) {
	provider := &blockingTopicRouteProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	start := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/topic-route?topic=sample_order_events_topic", nil)
	app.ServeHTTP(recorder, request)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected topic route response without waiting for provider")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing response, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[rocketmq.TopicRoute]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.HasData || !payload.Refreshing || !payload.Stale || payload.CacheHit {
		t.Fatalf("unexpected cold topic route metadata: %#v", payload)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatalf("topic route refresh did not start")
	}
	close(provider.release)
	waitForSnapshot(t, app.topicRouteSnapshots.snapshot("sample_order_events_topic"))
}

func TestTopicStatusEndpointReturnsFastBeforeSlowProviderFinishes(t *testing.T) {
	provider := &blockingTopicStatusProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	start := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/topic-status?topic=sample_order_events_topic", nil)
	app.ServeHTTP(recorder, request)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected topic status response without waiting for provider")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing response, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[rocketmq.TopicStatus]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.HasData || !payload.Refreshing || !payload.Stale || payload.CacheHit {
		t.Fatalf("unexpected cold topic status metadata: %#v", payload)
	}
	if payload.Data.Topic != "sample_order_events_topic" {
		t.Fatalf("expected placeholder to preserve topic, got %#v", payload.Data)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatalf("topic status refresh did not start")
	}
	close(provider.release)
	waitForSnapshot(t, app.topicStatusSnapshots.snapshot("sample_order_events_topic"))
}

func TestConsumerDetailEndpointReturnsFastBeforeSlowProviderFinishes(t *testing.T) {
	provider := &blockingConsumerDetailProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	start := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/consumer-detail?group=sample-order-events-consumer", nil)
	app.ServeHTTP(recorder, request)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected consumer detail response without waiting for provider")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing response, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[rocketmq.ConsumerDetail]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.HasData || !payload.Refreshing || !payload.Stale || payload.CacheHit {
		t.Fatalf("unexpected cold consumer detail metadata: %#v", payload)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatalf("consumer detail refresh did not start")
	}
	close(provider.release)
	waitForSnapshot(t, app.consumerDetailSnapshots.snapshot(consumerDetailCacheKey("sample-order-events-consumer", "")))
}

func TestMessageChainEndpointReturnsFastBeforeSlowProviderFinishes(t *testing.T) {
	provider := &blockingMessageChainProvider{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})

	start := time.Now()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/message-chain?topic=sample_notice_topic&messageId=abc", nil)
	app.ServeHTTP(recorder, request)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("expected message chain response without waiting for provider")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing response, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var payload responsePayload[rocketmq.MessageStatusChain]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.HasData || !payload.Refreshing || !payload.Stale || payload.CacheHit {
		t.Fatalf("unexpected cold message chain metadata: %#v", payload)
	}
	if payload.Data.Topic != "sample_notice_topic" || payload.Data.MessageID != "abc" {
		t.Fatalf("expected placeholder to preserve query target, got %#v", payload.Data)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatalf("message chain refresh did not start")
	}
	close(provider.release)
	key, err := messageChainCacheKey(rocketmq.MessageQuery{Topic: "sample_notice_topic", MessageID: "abc"})
	if err != nil {
		t.Fatalf("build message chain cache key: %v", err)
	}
	waitForSnapshot(t, app.messageChainSnapshots.snapshot(key))
}

func TestMessageChainEndpointDoesNotRetryFailedColdQueryWithoutRefresh(t *testing.T) {
	provider := &failingMessageChainProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	query := rocketmq.MessageQuery{Topic: "sample_notice_topic", MessageID: "missing"}
	key, err := messageChainCacheKey(query)
	if err != nil {
		t.Fatalf("build message chain cache key: %v", err)
	}

	first := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/message-chain?topic=sample_notice_topic&messageId=missing", nil)
	app.ServeHTTP(first, request)
	waitForLastError(t, app.messageChainSnapshots.snapshot(key))

	second := httptest.NewRecorder()
	app.ServeHTTP(second, request)
	var payload responsePayload[rocketmq.MessageStatusChain]
	if err := json.Unmarshal(second.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Refreshing || payload.LastError == "" || payload.HasData {
		t.Fatalf("expected failed cold query to stay idle until explicit refresh, got %#v", payload)
	}
	if provider.messageChainCalls != 1 {
		t.Fatalf("expected no implicit retry after failure, got %d calls", provider.messageChainCalls)
	}

	refresh := httptest.NewRecorder()
	app.ServeHTTP(refresh, httptest.NewRequest(http.MethodGet, "/api/message-chain?topic=sample_notice_topic&messageId=missing&refresh=true", nil))
	waitForMessageChainCalls(t, provider, 2)
	if provider.messageChainCalls != 2 {
		t.Fatalf("expected explicit refresh to retry failed query, got %d calls", provider.messageChainCalls)
	}
}

func TestTopicStatusEndpointDoesNotRetryFailedColdQueryWithoutRefresh(t *testing.T) {
	provider := &failingTopicStatusProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Second})
	topic := "sample_order_events_topic"

	first := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/topic-status?topic=sample_order_events_topic", nil)
	app.ServeHTTP(first, request)
	waitForLastError(t, app.topicStatusSnapshots.snapshot(topic))

	second := httptest.NewRecorder()
	app.ServeHTTP(second, request)
	var payload responsePayload[rocketmq.TopicStatus]
	if err := json.Unmarshal(second.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Refreshing || payload.LastError == "" || payload.HasData {
		t.Fatalf("expected failed topic status to stay idle until explicit refresh, got %#v", payload)
	}
	if provider.topicStatusCalls != 1 {
		t.Fatalf("expected no implicit retry after failure, got %d calls", provider.topicStatusCalls)
	}

	refresh := httptest.NewRecorder()
	app.ServeHTTP(refresh, httptest.NewRequest(http.MethodGet, "/api/topic-status?topic=sample_order_events_topic&refresh=true", nil))
	waitForTopicStatusCalls(t, provider, 2)
	if provider.topicStatusCalls != 2 {
		t.Fatalf("expected explicit refresh to retry failed topic status, got %d calls", provider.topicStatusCalls)
	}
}

func TestEndpointKeepsLastSnapshotWhenRefreshFails(t *testing.T) {
	provider := &flakyProvider{}
	app := New(AppConfig{Provider: provider, ClusterCacheTTL: time.Millisecond})
	waitForSnapshot(t, app.clusterSnapshot)
	time.Sleep(2 * time.Millisecond)

	first := httptest.NewRecorder()
	app.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/clusters", nil))
	waitForLastError(t, app.clusterSnapshot)

	second := httptest.NewRecorder()
	app.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/clusters", nil))

	var payload responsePayload[[]rocketmq.Cluster]
	if err := json.Unmarshal(second.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.HasData || !payload.Stale || payload.LastError == "" {
		t.Fatalf("expected stale data with refresh error, got %#v", payload)
	}
	if len(payload.Data) != 1 || payload.Data[0].Name != "DefaultCluster" {
		t.Fatalf("expected previous cluster snapshot, got %#v", payload.Data)
	}
}

func TestHealthEndpointReturnsTargetLatencyBudget(t *testing.T) {
	app := New(AppConfig{Provider: &fakeProvider{}, ClusterCacheTTL: time.Second})
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if recorder.Body.String() == "" {
		t.Fatalf("expected health payload")
	}
}

func TestConfigEndpointSwitchesNameServerAndClearsSnapshots(t *testing.T) {
	factories := make(map[string]*fakeProvider)
	app := New(AppConfig{
		ProviderFactory: func(nameServer string) rocketmq.Provider {
			provider := &fakeProvider{}
			factories[nameServer] = provider
			return provider
		},
		NameServer:        "ns-a:9876",
		NameServerOptions: []string{"ns-a:9876", "ns-b:9876"},
		ClusterCacheTTL:   time.Second,
	})

	body := bytes.NewBufferString(`{"nameServer":"ns-b:9876"}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/config", body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	health := httptest.NewRecorder()
	app.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	var payload responsePayload[map[string]any]
	if err := json.Unmarshal(health.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if payload.Data["nameServer"] != "ns-b:9876" {
		t.Fatalf("expected switched nameserver, got %#v", payload.Data)
	}
	if _, ok := factories["ns-b:9876"]; !ok {
		t.Fatalf("expected provider factory to be called for ns-b")
	}
}

type blockingProvider struct {
	started chan struct{}
	release chan struct{}
}

func (p *blockingProvider) ClusterList(ctx context.Context) ([]rocketmq.Cluster, error) {
	close(p.started)
	<-p.release
	return []rocketmq.Cluster{{Name: "DefaultCluster"}}, nil
}

func (p *blockingProvider) BrokerStatus(ctx context.Context, brokerAddr string) (rocketmq.BrokerStatus, error) {
	return rocketmq.BrokerStatus{BrokerAddr: brokerAddr}, nil
}

func (p *blockingProvider) TopicList(ctx context.Context) ([]rocketmq.Topic, error) {
	return nil, nil
}

func (p *blockingProvider) TopicRoute(ctx context.Context, topic string) (rocketmq.TopicRoute, error) {
	return rocketmq.TopicRoute{}, nil
}

func (p *blockingProvider) TopicStatus(ctx context.Context, topic string) (rocketmq.TopicStatus, error) {
	return rocketmq.TopicStatus{}, nil
}

func (p *blockingProvider) TopicMessages(ctx context.Context, query rocketmq.MessageBrowseQuery) (rocketmq.TopicMessages, error) {
	return rocketmq.TopicMessages{}, nil
}

func (p *blockingProvider) UpsertTopic(ctx context.Context, request rocketmq.TopicConfigMutation) (rocketmq.TopicMutationResult, error) {
	return rocketmq.TopicMutationResult{Topic: request.Topic, Operation: "upsertTopic"}, nil
}

func (p *blockingProvider) DeleteTopic(ctx context.Context, request rocketmq.TopicDeleteRequest) (rocketmq.TopicMutationResult, error) {
	return rocketmq.TopicMutationResult{Topic: request.Topic, Operation: "deleteTopic"}, nil
}

func (p *blockingProvider) SendTopicMessage(ctx context.Context, request rocketmq.TopicMessageSendRequest) (rocketmq.TopicMessageSendResult, error) {
	return rocketmq.TopicMessageSendResult{Topic: request.Topic, Operation: "sendMessage", MessageID: "msg-001"}, nil
}

func (p *blockingProvider) ConsumerGroups(ctx context.Context) ([]rocketmq.ConsumerGroup, error) {
	return nil, nil
}

func (p *blockingProvider) ConsumerDetail(ctx context.Context, group string, topic string) (rocketmq.ConsumerDetail, error) {
	return rocketmq.ConsumerDetail{}, nil
}

func (p *blockingProvider) ResetConsumerOffset(ctx context.Context, request rocketmq.ConsumerOffsetResetRequest) (rocketmq.ConsumerOffsetResetResult, error) {
	return rocketmq.ConsumerOffsetResetResult{Group: request.Group, Topic: request.Topic, Operation: "resetOffsetByTime"}, nil
}

func (p *blockingProvider) MessageChain(ctx context.Context, query rocketmq.MessageQuery) (rocketmq.MessageStatusChain, error) {
	return rocketmq.MessageStatusChain{}, nil
}

type blockingBrokerStatusProvider struct {
	fakeProvider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingBrokerStatusProvider) BrokerStatus(ctx context.Context, brokerAddr string) (rocketmq.BrokerStatus, error) {
	p.once.Do(func() {
		close(p.started)
	})
	<-p.release
	return p.fakeProvider.BrokerStatus(ctx, brokerAddr)
}

type blockingTopicRouteProvider struct {
	fakeProvider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingTopicRouteProvider) TopicRoute(ctx context.Context, topic string) (rocketmq.TopicRoute, error) {
	p.once.Do(func() {
		close(p.started)
	})
	<-p.release
	return p.fakeProvider.TopicRoute(ctx, topic)
}

type blockingTopicStatusProvider struct {
	fakeProvider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingTopicStatusProvider) TopicStatus(ctx context.Context, topic string) (rocketmq.TopicStatus, error) {
	p.once.Do(func() {
		close(p.started)
	})
	<-p.release
	return p.fakeProvider.TopicStatus(ctx, topic)
}

type blockingConsumerDetailProvider struct {
	fakeProvider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingConsumerDetailProvider) ConsumerDetail(ctx context.Context, group string, topic string) (rocketmq.ConsumerDetail, error) {
	p.once.Do(func() {
		close(p.started)
	})
	<-p.release
	return p.fakeProvider.ConsumerDetail(ctx, group, topic)
}

type blockingMessageChainProvider struct {
	fakeProvider
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingMessageChainProvider) MessageChain(ctx context.Context, query rocketmq.MessageQuery) (rocketmq.MessageStatusChain, error) {
	p.once.Do(func() {
		close(p.started)
	})
	<-p.release
	return p.fakeProvider.MessageChain(ctx, query)
}

type failingMessageChainProvider struct {
	fakeProvider
}

func (p *failingMessageChainProvider) MessageChain(ctx context.Context, query rocketmq.MessageQuery) (rocketmq.MessageStatusChain, error) {
	p.messageChainCalls++
	return rocketmq.MessageStatusChain{}, errors.New("message not found")
}

type failingTopicStatusProvider struct {
	fakeProvider
}

func (p *failingTopicStatusProvider) TopicStatus(ctx context.Context, topic string) (rocketmq.TopicStatus, error) {
	p.topicStatusCalls++
	return rocketmq.TopicStatus{}, errors.New("nameserver route timeout")
}

type flakyProvider struct {
	calls int
}

func (p *flakyProvider) ClusterList(ctx context.Context) ([]rocketmq.Cluster, error) {
	p.calls++
	if p.calls == 1 {
		return []rocketmq.Cluster{{Name: "DefaultCluster"}}, nil
	}
	return nil, errors.New("broker status timeout")
}

func (p *flakyProvider) BrokerStatus(ctx context.Context, brokerAddr string) (rocketmq.BrokerStatus, error) {
	return rocketmq.BrokerStatus{BrokerAddr: brokerAddr}, nil
}

func (p *flakyProvider) TopicList(ctx context.Context) ([]rocketmq.Topic, error) {
	return nil, nil
}

func (p *flakyProvider) TopicRoute(ctx context.Context, topic string) (rocketmq.TopicRoute, error) {
	return rocketmq.TopicRoute{}, nil
}

func (p *flakyProvider) TopicStatus(ctx context.Context, topic string) (rocketmq.TopicStatus, error) {
	return rocketmq.TopicStatus{}, nil
}

func (p *flakyProvider) TopicMessages(ctx context.Context, query rocketmq.MessageBrowseQuery) (rocketmq.TopicMessages, error) {
	return rocketmq.TopicMessages{}, nil
}

func (p *flakyProvider) UpsertTopic(ctx context.Context, request rocketmq.TopicConfigMutation) (rocketmq.TopicMutationResult, error) {
	return rocketmq.TopicMutationResult{Topic: request.Topic, Operation: "upsertTopic"}, nil
}

func (p *flakyProvider) DeleteTopic(ctx context.Context, request rocketmq.TopicDeleteRequest) (rocketmq.TopicMutationResult, error) {
	return rocketmq.TopicMutationResult{Topic: request.Topic, Operation: "deleteTopic"}, nil
}

func (p *flakyProvider) SendTopicMessage(ctx context.Context, request rocketmq.TopicMessageSendRequest) (rocketmq.TopicMessageSendResult, error) {
	return rocketmq.TopicMessageSendResult{Topic: request.Topic, Operation: "sendMessage", MessageID: "msg-001"}, nil
}

func (p *flakyProvider) ConsumerGroups(ctx context.Context) ([]rocketmq.ConsumerGroup, error) {
	return nil, nil
}

func (p *flakyProvider) ConsumerDetail(ctx context.Context, group string, topic string) (rocketmq.ConsumerDetail, error) {
	return rocketmq.ConsumerDetail{}, nil
}

func (p *flakyProvider) ResetConsumerOffset(ctx context.Context, request rocketmq.ConsumerOffsetResetRequest) (rocketmq.ConsumerOffsetResetResult, error) {
	return rocketmq.ConsumerOffsetResetResult{Group: request.Group, Topic: request.Topic, Operation: "resetOffsetByTime"}, nil
}

func (p *flakyProvider) MessageChain(ctx context.Context, query rocketmq.MessageQuery) (rocketmq.MessageStatusChain, error) {
	return rocketmq.MessageStatusChain{}, nil
}

func waitForSnapshot[T any](t *testing.T, store *snapshotStore[T]) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view := store.view(time.Now())
		if view.HasData && !view.Refreshing {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("snapshot did not load before deadline")
}

func waitForLastError[T any](t *testing.T, store *snapshotStore[T]) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view := store.view(time.Now())
		if view.LastError != "" && !view.Refreshing {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("snapshot refresh error did not arrive before deadline")
}

func waitForTopicMessageIncrementalCall(t *testing.T, provider *incrementalTopicMessagesProvider, store *snapshotStore[rocketmq.TopicMessages]) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view := store.view(time.Now())
		if provider.incrementalCalls > 0 && view.HasData && !view.Refreshing {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("incremental topic message refresh did not finish, calls=%d", provider.incrementalCalls)
}

func waitForProviderCalls(t *testing.T, provider *fakeProvider, clusters int, topics int, consumers int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if provider.clusterCalls >= clusters && provider.topicCalls >= topics && provider.consumerCalls >= consumers {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf(
		"provider calls did not reach expected counts, got clusters=%d topics=%d consumers=%d",
		provider.clusterCalls,
		provider.topicCalls,
		provider.consumerCalls,
	)
}

func waitForTopicCalls(t *testing.T, provider *fakeProvider, topics int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if provider.topicCalls >= topics {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("topic calls did not reach %d, got %d", topics, provider.topicCalls)
}

func waitForMessageChainCalls(t *testing.T, provider *failingMessageChainProvider, calls int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if provider.messageChainCalls >= calls {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("message chain calls did not reach %d, got %d", calls, provider.messageChainCalls)
}

func waitForTopicStatusCalls(t *testing.T, provider *failingTopicStatusProvider, calls int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if provider.topicStatusCalls >= calls {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("topic status calls did not reach %d, got %d", calls, provider.topicStatusCalls)
}
