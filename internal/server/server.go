package server

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

// staticFiles 内嵌前端资源，避免 dashboard 部署时再额外维护静态文件目录。
//
//go:embed public/*
var staticFiles embed.FS

// AppConfig 是 HTTP 服务的启动配置。
type AppConfig struct {
	Provider        rocketmq.Provider
	ProviderFactory func(nameServer string) rocketmq.Provider
	// Clusters 是启动时固定加载的多集群定义；每个请求通过 clusterId 选择其中之一。
	Clusters             []ClusterDefinition
	ClusterCacheTTL      time.Duration
	MessageChainCacheTTL time.Duration
	LatencyBudget        time.Duration
	// NameServer 和 NameServerOptions 保留为单集群及旧部署配置的启动兼容输入。
	NameServer        string
	NameServerOptions []string
	// RuntimeConfigEnabled 控制在线配置写入入口，部署环境需要显式开启。
	RuntimeConfigEnabled bool
	// ProxyRuntime 管理 Dashboard 容器内的官方 RocketMQ Proxy 进程。
	// 该字段仅保留单集群部署兼容；多集群部署请使用 ProxyRuntimes 显式绑定。
	ProxyRuntime ProxyRuntime
	// ProxyRuntimes 将 Proxy 运行器按固定 clusterId 绑定。
	ProxyRuntimes map[string]ProxyRuntime
	// Authenticator 认证所有写操作；未配置时写操作默认拒绝。
	Authenticator Authenticator
	// AuditStore 保存写操作的开始、完成、读回和回滚事件。
	AuditStore AuditStore
	// AuditLogPath 是未显式注入 AuditStore 时的 JSONL 持久化路径。
	AuditLogPath string
}

// App 承载 Dashboard HTTP 路由、RocketMQ Provider 和热点快照仓库。
type App struct {
	mux                  *http.ServeMux
	clusterMu            sync.RWMutex
	providerFactory      func(nameServer string) rocketmq.Provider
	clusters             map[string]*clusterRuntime
	clusterOrder         []string
	latencyBudget        time.Duration
	runtimeConfigEnabled bool
	authenticator        Authenticator
	auditStore           AuditStore
}

// responsePayload 是所有 API 的统一响应结构，方便前端展示耗时、快照状态和缓存命中状态。
type responsePayload[T any] struct {
	Code                 int    `json:"code"`
	Message              string `json:"message"`
	Data                 T      `json:"data"`
	LatencyMillis        int64  `json:"latencyMillis"`
	CacheHit             bool   `json:"cacheHit"`
	HasData              bool   `json:"hasData"`
	Stale                bool   `json:"stale"`
	Refreshing           bool   `json:"refreshing"`
	LastRefreshUnixMilli int64  `json:"lastRefreshUnixMilli"`
	LastError            string `json:"lastError,omitempty"`
}

// refreshTriggerPayload 描述本次强制刷新是否启动了对应快照任务。
type refreshTriggerPayload struct {
	Clusters  bool `json:"clusters"`
	Topics    bool `json:"topics"`
	Consumers bool `json:"consumers"`
	Features  bool `json:"features"`
}

// dashboardConfigPayload 返回启动时固定的集群列表，前端只在本会话中选择 clusterId。
type dashboardConfigPayload struct {
	Clusters []ClusterDefinition `json:"clusters"`
}

// topicMessagesIncrementalProvider 表示支持按旧快照复用历史消息 offset 的 Provider。
type topicMessagesIncrementalProvider interface {
	TopicMessagesIncremental(ctx context.Context, query rocketmq.MessageBrowseQuery, previous rocketmq.TopicMessages) (rocketmq.TopicMessages, error)
}

// clusterFeaturesProvider 表示可生成当前 NameServer 能力画像的 Provider。
type clusterFeaturesProvider interface {
	ClusterFeatures(ctx context.Context) (rocketmq.ClusterFeatureReport, error)
}

// New 创建 Dashboard HTTP 应用。
func New(config AppConfig) *App {
	ttl := config.ClusterCacheTTL
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	budget := config.LatencyBudget
	if budget <= 0 {
		budget = time.Second
	}
	providerFactory := config.ProviderFactory
	if providerFactory == nil {
		configuredProvider := config.Provider
		providerFactory = func(nameServer string) rocketmq.Provider {
			if configuredProvider != nil {
				return configuredProvider
			}
			return rocketmq.SampleProvider{}
		}
	}

	authenticator := config.Authenticator
	if authenticator == nil {
		authenticator = denyAuthenticator{}
	}
	auditStore := config.AuditStore
	if auditStore == nil {
		auditStore = NewFileAuditStore(config.AuditLogPath)
	}
	definitions := normalizeClusterDefinitions(config.Clusters, config.NameServer, config.NameServerOptions)
	app := &App{
		mux:                  http.NewServeMux(),
		providerFactory:      providerFactory,
		clusters:             make(map[string]*clusterRuntime, len(definitions)),
		clusterOrder:         make([]string, 0, len(definitions)),
		latencyBudget:        budget,
		runtimeConfigEnabled: config.RuntimeConfigEnabled,
		authenticator:        authenticator,
		auditStore:           auditStore,
	}
	chainTTL := messageChainCacheTTL(config.MessageChainCacheTTL, ttl)
	for _, definition := range definitions {
		runtime := newClusterRuntime(definition, providerFactory(definition.NameServer), ttl, chainTTL)
		runtime.proxyRuntime = config.ProxyRuntimes[definition.ID]
		if runtime.proxyRuntime == nil && len(definitions) == 1 {
			runtime.proxyRuntime = config.ProxyRuntime
		}
		app.clusters[definition.ID] = runtime
		app.clusterOrder = append(app.clusterOrder, definition.ID)
	}
	app.routes()
	for _, clusterID := range app.clusterOrder {
		app.clusters[clusterID].refreshSnapshots(context.Background())
	}
	return app
}

// ServeHTTP 将请求转交给内部路由器。
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

func messageChainCacheTTL(configured time.Duration, clusterTTL time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	if clusterTTL > 0 && clusterTTL > 30*time.Minute {
		return clusterTTL
	}
	return 30 * time.Minute
}

func (a *App) configPayload() dashboardConfigPayload {
	a.clusterMu.RLock()
	defer a.clusterMu.RUnlock()
	clusters := make([]ClusterDefinition, 0, len(a.clusterOrder))
	for _, clusterID := range a.clusterOrder {
		clusters = append(clusters, a.clusters[clusterID].definition)
	}
	return dashboardConfigPayload{Clusters: clusters}
}

// clusterScoped 将请求解析到固定集群运行时，后续 handler 不再读取任何全局 Provider。
func (a *App) clusterScoped(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runtime, err := a.clusterRuntimeForRequest(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		contextWithRuntime := context.WithValue(r.Context(), clusterRuntimeContextKey{}, runtime)
		next(w, r.WithContext(contextWithRuntime))
	}
}

// clusterRuntimeForRequest 只接受请求显式指定的 clusterId；单集群部署可安全推导唯一目标。
func (a *App) clusterRuntimeForRequest(r *http.Request) (*clusterRuntime, error) {
	queryID := strings.TrimSpace(r.URL.Query().Get("clusterId"))
	headerID := strings.TrimSpace(r.Header.Get("X-RMQD-Cluster-ID"))
	if queryID != "" && headerID != "" && queryID != headerID {
		return nil, errors.New("clusterId 查询参数与 X-RMQD-Cluster-ID 不一致")
	}
	clusterID := queryID
	if clusterID == "" {
		clusterID = headerID
	}
	a.clusterMu.RLock()
	defer a.clusterMu.RUnlock()
	if clusterID == "" {
		if len(a.clusterOrder) != 1 {
			return nil, errors.New("多集群部署必须显式指定 clusterId")
		}
		clusterID = a.clusterOrder[0]
	}
	runtime, ok := a.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("未知 clusterId: %s", clusterID)
	}
	return runtime, nil
}

// clusterRuntimeFromContext 返回 clusterScoped 预先绑定的请求运行时。
func clusterRuntimeFromContext(ctx context.Context) *clusterRuntime {
	runtime, _ := ctx.Value(clusterRuntimeContextKey{}).(*clusterRuntime)
	return runtime
}

func (a *App) routes() {
	a.mux.HandleFunc("/api/health", a.clusterScoped(a.handleHealth))
	a.mux.HandleFunc("/api/config", a.handleConfig)
	a.mux.HandleFunc("/api/runtime-config", a.clusterScoped(a.handleRuntimeConfig))
	a.mux.HandleFunc("/api/runtime-config/proxy", a.clusterScoped(a.handleProxyRuntimeConfig))
	a.mux.HandleFunc("/api/runtime-config/proxy/restart", a.clusterScoped(a.handleProxyRuntimeRestart))
	a.mux.HandleFunc("/api/refresh", a.clusterScoped(a.handleRefresh))
	a.mux.HandleFunc("/api/clusters", a.clusterScoped(a.handleClusters))
	a.mux.HandleFunc("/api/features", a.clusterScoped(a.handleFeatures))
	a.mux.HandleFunc("/api/topics", a.clusterScoped(a.handleTopics))
	a.mux.HandleFunc("/api/topic-route", a.clusterScoped(a.handleTopicRoute))
	a.mux.HandleFunc("/api/topic-status", a.clusterScoped(a.handleTopicStatus))
	a.mux.HandleFunc("/api/topic-messages", a.clusterScoped(a.handleTopicMessages))
	a.mux.HandleFunc("/api/topic-messages/send", a.clusterScoped(a.handleTopicMessageSend))
	a.mux.HandleFunc("/api/broker-status", a.clusterScoped(a.handleBrokerStatus))
	a.mux.HandleFunc("/api/consumers", a.clusterScoped(a.handleConsumers))
	a.mux.HandleFunc("/api/consumer-detail", a.clusterScoped(a.handleConsumerDetail))
	a.mux.HandleFunc("/api/consumer-offset/reset", a.clusterScoped(a.handleConsumerOffsetReset))
	a.mux.HandleFunc("/api/message-chain", a.clusterScoped(a.handleMessageChain))
	a.mux.HandleFunc("/api/audit", a.clusterScoped(a.handleAudit))
	a.mux.HandleFunc("/", a.handleStatic)
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}
	runtime := clusterRuntimeFromContext(r.Context())
	writeJSON(w, http.StatusOK, responsePayload[map[string]any]{
		Code:    0,
		Message: "ok",
		Data: map[string]any{
			"clusterId":           runtime.definition.ID,
			"nameServer":          runtime.definition.NameServer,
			"latencyBudgetMillis": a.latencyBudget.Milliseconds(),
			"mode":                "go-dashboard-mqadmin-provider",
		},
	})
}

func (a *App) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("集群选择仅由请求 clusterId 指定，仅支持 GET"))
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[dashboardConfigPayload]{
		Code:    0,
		Message: "ok",
		Data:    a.configPayload(),
	})
}

func (a *App) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST"))
		return
	}

	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	triggered := refreshTriggerPayload{
		// 每个 refreshAsync 都会拒绝重复并发任务，因此手动刷新不会放大 mqadmin 压力。
		Clusters:  runtime.clusterSnapshot.refreshAsync(context.Background()),
		Topics:    runtime.topicSnapshot.refreshAsync(context.Background()),
		Consumers: runtime.consumerSnapshot.refreshAsync(context.Background()),
		Features:  runtime.featureSnapshot.refreshAsync(context.Background()),
	}
	writeJSON(w, http.StatusOK, responsePayload[refreshTriggerPayload]{
		Code:          0,
		Message:       "ok",
		Data:          triggered,
		LatencyMillis: time.Since(start).Milliseconds(),
	})
}

func (a *App) handleClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	writeSnapshot(w, r, start, runtime.clusterSnapshot)
}

func (a *App) handleFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.featureSnapshot
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		store.refreshIfStale(r.Context(), start)
	}
	view := store.view(time.Now())
	report := view.Data
	if !view.HasData {
		report = rocketmq.ClusterFeatureReport{NameServer: runtime.definition.NameServer}
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.ClusterFeatureReport]{
		Code:                 0,
		Message:              "ok",
		Data:                 report,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

func (a *App) handleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		start := time.Now()
		runtime := clusterRuntimeFromContext(r.Context())
		writeSnapshot(w, r, start, runtime.topicSnapshot)
	case http.MethodPost, http.MethodPut:
		a.handleTopicUpsert(w, r)
	case http.MethodDelete:
		a.handleTopicDelete(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET/POST/PUT/DELETE"))
	}
}

// topicMutationRequest 是 Topic 写操作的统一请求体，前端用同一套表单即可完成创建、更新和删除。
type topicMutationRequest struct {
	Topic          string `json:"topic"`
	ClusterName    string `json:"clusterName"`
	BrokerAddr     string `json:"brokerAddr"`
	ReadQueueNums  int    `json:"readQueueNums"`
	WriteQueueNums int    `json:"writeQueueNums"`
	Perm           int    `json:"perm"`
	Order          bool   `json:"order"`
	Unit           bool   `json:"unit"`
	HasUnitSub     bool   `json:"hasUnitSub"`
	Attributes     string `json:"attributes"`
}

// topicDeleteRequest 是 Topic 删除操作的统一请求体，删除只保留 Topic 与 clusterName 两个关键字段。
type topicDeleteRequest struct {
	Topic       string `json:"topic"`
	ClusterName string `json:"clusterName"`
}

// topicMessageSendRequest 是 Topic 消息发送表单的请求体，字段直接映射 mqadmin sendMessage。
type topicMessageSendRequest struct {
	Topic       string `json:"topic"`
	Body        string `json:"body"`
	Keys        string `json:"keys"`
	Tags        string `json:"tags"`
	BrokerName  string `json:"brokerName"`
	QueueID     *int   `json:"queueId,omitempty"`
	TraceEnable bool   `json:"traceEnable"`
}

// consumerOffsetResetRequest 是 Consumer 页重置消费点表单的请求体。
type consumerOffsetResetRequest struct {
	Group      string `json:"group"`
	Topic      string `json:"topic"`
	Timestamp  string `json:"timestamp"`
	Force      bool   `json:"force"`
	BrokerAddr string `json:"brokerAddr"`
	QueueID    *int   `json:"queueId,omitempty"`
	Offset     *int64 `json:"offset,omitempty"`
}

// handleTopicUpsert 执行 updateTopic/createTopic 写操作，并在成功后清理 Topic 相关缓存。
func (a *App) handleTopicUpsert(w http.ResponseWriter, r *http.Request) {
	var request topicMutationRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("请求体必须是 JSON"))
		return
	}
	mutation := rocketmq.TopicConfigMutation{
		Topic:          request.Topic,
		ClusterName:    request.ClusterName,
		BrokerAddr:     request.BrokerAddr,
		ReadQueueNums:  request.ReadQueueNums,
		WriteQueueNums: request.WriteQueueNums,
		Perm:           request.Perm,
		Order:          request.Order,
		Unit:           request.Unit,
		HasUnitSub:     request.HasUnitSub,
		Attributes:     request.Attributes,
	}
	if err := mutation.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime := clusterRuntimeFromContext(r.Context())
	provider := runtime.provider
	if provider == nil {
		writeError(w, http.StatusInternalServerError, errors.New("当前 Provider 不可用"))
		return
	}
	audit, err := a.beginMutation(r, PermissionTopicWrite, "topic.upsert", mutation.TargetLabel(), mutation)
	if err != nil {
		writeMutationAdmissionError(w, err)
		return
	}
	w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
	start := time.Now()
	result, err := provider.UpsertTopic(r.Context(), mutation)
	if err != nil {
		if auditErr := audit.complete(r.Context(), nil, nil, err, false); auditErr != nil {
			log.Printf("topic upsert audit completion failed operationId=%s error=%v", audit.record.OperationID, auditErr)
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime.invalidateTopicCaches(mutation.Topic)
	runtime.topicSnapshot.refreshAsync(context.Background())
	verification := map[string]any{"providerOutput": result.Output, "topic": result.Topic, "target": result.Target}
	if err := audit.complete(r.Context(), result, verification, nil, false); err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("操作已执行，但审计完成记录失败: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.TopicMutationResult]{
		Code:          0,
		Message:       "ok",
		Data:          result,
		LatencyMillis: time.Since(start).Milliseconds(),
	})
}

// handleTopicDelete 执行 deleteTopic 写操作，并在成功后清理 Topic 相关缓存和列表快照。
func (a *App) handleTopicDelete(w http.ResponseWriter, r *http.Request) {
	var request topicDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("请求体必须是 JSON"))
		return
	}
	deleteRequest := rocketmq.TopicDeleteRequest{
		Topic:       request.Topic,
		ClusterName: request.ClusterName,
	}
	if err := deleteRequest.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime := clusterRuntimeFromContext(r.Context())
	provider := runtime.provider
	if provider == nil {
		writeError(w, http.StatusInternalServerError, errors.New("当前 Provider 不可用"))
		return
	}
	target := strings.TrimSpace(deleteRequest.ClusterName)
	if target == "" {
		target = "cluster"
	}
	audit, err := a.beginMutation(r, PermissionTopicWrite, "topic.delete", target+"/"+deleteRequest.Topic, deleteRequest)
	if err != nil {
		writeMutationAdmissionError(w, err)
		return
	}
	w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
	start := time.Now()
	result, err := provider.DeleteTopic(r.Context(), deleteRequest)
	if err != nil {
		if auditErr := audit.complete(r.Context(), nil, nil, err, false); auditErr != nil {
			log.Printf("topic delete audit completion failed operationId=%s error=%v", audit.record.OperationID, auditErr)
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime.invalidateTopicCaches(deleteRequest.Topic)
	runtime.topicSnapshot.refreshAsync(context.Background())
	verification := map[string]any{"providerOutput": result.Output, "topic": result.Topic, "target": result.Target}
	if err := audit.complete(r.Context(), result, verification, nil, false); err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("操作已执行，但审计完成记录失败: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.TopicMutationResult]{
		Code:          0,
		Message:       "ok",
		Data:          result,
		LatencyMillis: time.Since(start).Milliseconds(),
	})
}

// handleTopicMessageSend 执行 sendMessage 写操作，并在成功后刷新 Topic 消息浏览缓存。
func (a *App) handleTopicMessageSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST"))
		return
	}
	var request topicMessageSendRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("请求体必须是 JSON"))
		return
	}
	sendRequest := rocketmq.TopicMessageSendRequest{
		Topic:       request.Topic,
		Body:        request.Body,
		Keys:        request.Keys,
		Tags:        request.Tags,
		BrokerName:  request.BrokerName,
		QueueID:     request.QueueID,
		TraceEnable: request.TraceEnable,
	}
	if err := sendRequest.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime := clusterRuntimeFromContext(r.Context())
	provider := runtime.provider
	if provider == nil {
		writeError(w, http.StatusInternalServerError, errors.New("当前 Provider 不可用"))
		return
	}
	auditInput := map[string]any{
		"topic":       sendRequest.Topic,
		"keys":        sendRequest.Keys,
		"tags":        sendRequest.Tags,
		"brokerName":  sendRequest.BrokerName,
		"queueId":     sendRequest.QueueID,
		"traceEnable": sendRequest.TraceEnable,
		"bodyBytes":   len([]byte(sendRequest.Body)),
	}
	audit, err := a.beginMutation(r, PermissionTopicWrite, "message.send", sendRequest.TargetLabel(), auditInput)
	if err != nil {
		writeMutationAdmissionError(w, err)
		return
	}
	w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
	start := time.Now()
	log.Printf("topic message send started topic=%q target=%q trace=%t bodyBytes=%d", sendRequest.Topic, sendRequest.TargetLabel(), sendRequest.TraceEnable, len([]byte(sendRequest.Body)))
	result, err := provider.SendTopicMessage(r.Context(), sendRequest)
	latency := time.Since(start)
	if err != nil {
		log.Printf("topic message send failed topic=%q target=%q latency=%s error=%v", sendRequest.Topic, sendRequest.TargetLabel(), latency, err)
		if auditErr := audit.complete(r.Context(), nil, nil, err, false); auditErr != nil {
			log.Printf("message send audit completion failed operationId=%s error=%v", audit.record.OperationID, auditErr)
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	log.Printf("topic message send succeeded topic=%q target=%q messageId=%q status=%q latency=%s", sendRequest.Topic, sendRequest.TargetLabel(), result.MessageID, result.SendStatus, latency)
	runtime.invalidateTopicCaches(sendRequest.Topic)
	verification := map[string]any{"messageId": result.MessageID, "sendStatus": result.SendStatus, "brokerName": result.BrokerName, "queueId": result.QueueID}
	if err := audit.complete(r.Context(), result, verification, nil, false); err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("操作已执行，但审计完成记录失败: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.TopicMessageSendResult]{
		Code:          0,
		Message:       "ok",
		Data:          result,
		LatencyMillis: latency.Milliseconds(),
	})
}

func (a *App) handleTopicRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		writeError(w, http.StatusBadRequest, errors.New("topic 必填"))
		return
	}
	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.topicRouteSnapshots.snapshot(topic)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		refreshInteractiveSnapshot(store, start)
	}
	view := store.view(time.Now())
	route := view.Data
	if !view.HasData {
		route = rocketmq.TopicRoute{Topic: topic}
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.TopicRoute]{
		Code:                 0,
		Message:              "ok",
		Data:                 route,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

func (a *App) handleTopicStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if topic == "" {
		writeError(w, http.StatusBadRequest, errors.New("topic 必填"))
		return
	}
	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.topicStatusSnapshots.snapshot(topic)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		refreshInteractiveSnapshot(store, start)
	}
	view := store.view(time.Now())
	status := view.Data
	if !view.HasData {
		status = rocketmq.TopicStatus{Topic: topic}
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.TopicStatus]{
		Code:                 0,
		Message:              "ok",
		Data:                 status,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

func (a *App) handleTopicMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	query := messageBrowseQueryFromRequest(r)
	if query.Topic == "" {
		writeError(w, http.StatusBadRequest, errors.New("topic 必填"))
		return
	}
	start := time.Now()
	key, err := messageBrowseCacheKey(query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.topicMessageSnapshots.snapshot(key)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		refreshInteractiveSnapshot(store, start)
	}
	view := store.view(time.Now())
	messages := view.Data
	if !view.HasData {
		messages = topicMessagesPlaceholder(query)
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.TopicMessages]{
		Code:                 0,
		Message:              "ok",
		Data:                 messages,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

func (a *App) handleBrokerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	brokerAddr := strings.TrimSpace(r.URL.Query().Get("brokerAddr"))
	if brokerAddr == "" {
		writeError(w, http.StatusBadRequest, errors.New("brokerAddr 必填"))
		return
	}
	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.brokerStatusSnapshots.snapshot(brokerAddr)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		refreshInteractiveSnapshot(store, start)
	}
	view := store.view(time.Now())
	status := view.Data
	if !view.HasData {
		status = rocketmq.BrokerStatus{BrokerAddr: brokerAddr}
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.BrokerStatus]{
		Code:                 0,
		Message:              "ok",
		Data:                 status,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

func (a *App) handleConsumers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	start := time.Now()
	runtime := clusterRuntimeFromContext(r.Context())
	writeSnapshot(w, r, start, runtime.consumerSnapshot)
}

func (a *App) handleConsumerDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	group := strings.TrimSpace(r.URL.Query().Get("group"))
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	if group == "" {
		writeError(w, http.StatusBadRequest, errors.New("group 必填"))
		return
	}
	start := time.Now()
	key := consumerDetailCacheKey(group, topic)
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.consumerDetailSnapshots.snapshot(key)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		refreshInteractiveSnapshot(store, start)
	}
	view := store.view(time.Now())
	detail := view.Data
	if !view.HasData {
		detail = rocketmq.ConsumerDetail{Group: group, Topic: topic}
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.ConsumerDetail]{
		Code:                 0,
		Message:              "ok",
		Data:                 detail,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

// handleConsumerOffsetReset 执行 resetOffsetByTime 写操作，并刷新 Consumer 列表与指定详情缓存。
func (a *App) handleConsumerOffsetReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 POST"))
		return
	}
	var request consumerOffsetResetRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("请求体必须是 JSON"))
		return
	}
	resetRequest := rocketmq.ConsumerOffsetResetRequest{
		Group:      request.Group,
		Topic:      request.Topic,
		Timestamp:  request.Timestamp,
		Force:      request.Force,
		BrokerAddr: request.BrokerAddr,
		QueueID:    request.QueueID,
		Offset:     request.Offset,
	}
	if err := resetRequest.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	resetRequest = resetRequest.Normalized()
	runtime := clusterRuntimeFromContext(r.Context())
	provider := runtime.provider
	if provider == nil {
		writeError(w, http.StatusInternalServerError, errors.New("当前 Provider 不可用"))
		return
	}
	target := fmt.Sprintf("%s/%s", resetRequest.Group, resetRequest.Topic)
	audit, err := a.beginMutation(r, PermissionTopicWrite, "consumer-offset.reset", target, resetRequest)
	if err != nil {
		writeMutationAdmissionError(w, err)
		return
	}
	w.Header().Set("X-RMQD-Operation-ID", audit.record.OperationID)
	start := time.Now()
	result, err := provider.ResetConsumerOffset(r.Context(), resetRequest)
	if err != nil {
		if auditErr := audit.complete(r.Context(), nil, nil, err, false); auditErr != nil {
			log.Printf("consumer offset reset audit completion failed operationId=%s error=%v", audit.record.OperationID, auditErr)
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime.consumerDetailSnapshots.delete(consumerDetailCacheKey(resetRequest.Group, resetRequest.Topic))
	runtime.consumerDetailSnapshots.delete(consumerDetailCacheKey(resetRequest.Group, ""))
	runtime.consumerSnapshot.refreshAsync(context.Background())
	verification := map[string]any{"providerOutput": result.Output, "group": result.Group, "topic": result.Topic, "target": result.Target}
	if err := audit.complete(r.Context(), result, verification, nil, false); err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Errorf("操作已执行，但审计完成记录失败: %w", err))
		return
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.ConsumerOffsetResetResult]{
		Code:          0,
		Message:       "ok",
		Data:          result,
		LatencyMillis: time.Since(start).Milliseconds(),
	})
}

func (a *App) handleMessageChain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("仅支持 GET"))
		return
	}

	query := messageQueryFromRequest(r)
	if err := validateMessageQuery(query); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	start := time.Now()
	key, err := messageChainCacheKey(query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	runtime := clusterRuntimeFromContext(r.Context())
	store := runtime.messageChainSnapshots.snapshot(key)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("refresh")), "true") {
		store.refreshAsync(context.Background())
	} else {
		refreshInteractiveSnapshot(store, start)
	}
	view := store.view(time.Now())
	chain := view.Data
	if !view.HasData {
		chain = messageChainPlaceholder(query)
	}
	writeJSON(w, http.StatusOK, responsePayload[rocketmq.MessageStatusChain]{
		Code:                 0,
		Message:              "ok",
		Data:                 chain,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

func (a *App) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	if strings.Contains(path, "..") {
		http.NotFound(w, r)
		return
	}
	http.ServeFileFS(w, r, staticFiles, "public/"+path)
}

// writeSnapshot 只读取内存快照并按需触发后台刷新，保证 HTTP 热路径不被 RocketMQ 管理命令拖慢。
func writeSnapshot[T any](w http.ResponseWriter, r *http.Request, start time.Time, store *snapshotStore[T]) {
	store.refreshIfStale(r.Context(), start)
	view := store.view(time.Now())
	writeJSON(w, http.StatusOK, responsePayload[T]{
		Code:                 0,
		Message:              "ok",
		Data:                 view.Data,
		LatencyMillis:        time.Since(start).Milliseconds(),
		CacheHit:             view.HasData && !view.Stale,
		HasData:              view.HasData,
		Stale:                view.Stale,
		Refreshing:           view.Refreshing,
		LastRefreshUnixMilli: view.LastRefreshUnixMilli,
		LastError:            view.LastError,
	})
}

// refreshInteractiveSnapshot 用于行内详情和链路查询：已有数据时允许后台更新，无数据且已有错误时等待用户显式重试。
func refreshInteractiveSnapshot[T any](store *snapshotStore[T], start time.Time) {
	viewBeforeRefresh := store.view(start)
	if viewBeforeRefresh.HasData || viewBeforeRefresh.LastError == "" || viewBeforeRefresh.Refreshing {
		store.refreshIfStale(context.Background(), start)
	}
}

func writeJSON[T any](w http.ResponseWriter, status int, payload T) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, responsePayload[map[string]string]{
		Code:    status,
		Message: err.Error(),
		Data:    map[string]string{},
	})
}

func int64Query(r *http.Request, name string) int64 {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func optionalInt64Query(r *http.Request, name string, fallback int64) (int64, bool) {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback, false
	}
	return parsed, true
}

func intQuery(r *http.Request, name string) int {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func optionalIntQuery(r *http.Request, name string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func optionalIntQueryWithPresence(r *http.Request, name string, fallback int) (int, bool) {
	value := strings.TrimSpace(r.URL.Query().Get(name))
	if value == "" {
		return fallback, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback, false
	}
	return parsed, true
}

func normalizeNameServerOptions(current string, options []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(options)+1)
	for _, value := range append([]string{current}, options...) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		result = append(result, trimmed)
	}
	return result
}

// consumerDetailCacheKey 使用不可见分隔符组合 group/topic，确保同一消费者组不同 Topic 的进度缓存互不覆盖。
func consumerDetailCacheKey(group string, topic string) string {
	return group + "\x00" + topic
}

// splitConsumerDetailCacheKey 将缓存 key 拆回 Provider 需要的业务参数，topic 为空时交给 Provider 自动选择订阅。
func splitConsumerDetailCacheKey(key string) (string, string) {
	group, topic, ok := strings.Cut(key, "\x00")
	if !ok {
		return key, ""
	}
	return group, topic
}

// messageBrowseQueryFromRequest 从 query string 读取 Topic 消息浏览参数，默认跨队列聚合最近消息。
func messageBrowseQueryFromRequest(r *http.Request) rocketmq.MessageBrowseQuery {
	return rocketmq.MessageBrowseQuery{
		Topic:      strings.TrimSpace(r.URL.Query().Get("topic")),
		BrokerName: strings.TrimSpace(r.URL.Query().Get("brokerName")),
		QueueID:    optionalIntQuery(r, "queueId", -1),
		Limit:      optionalIntQuery(r, "limit", 12),
	}
}

func messageBrowseCacheKey(query rocketmq.MessageBrowseQuery) (string, error) {
	data, err := json.Marshal(query)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func messageBrowseQueryFromCacheKey(key string) (rocketmq.MessageBrowseQuery, error) {
	var query rocketmq.MessageBrowseQuery
	if err := json.Unmarshal([]byte(key), &query); err != nil {
		return rocketmq.MessageBrowseQuery{}, err
	}
	return query, nil
}

// messageQueryFromRequest 只从 query string 收集业务参数，保持前端、测试和接口约定都不使用路径变量。
func messageQueryFromRequest(r *http.Request) rocketmq.MessageQuery {
	queueID, hasQueueID := optionalIntQueryWithPresence(r, "queueId", 0)
	queueOffset, hasQueueOffset := optionalInt64Query(r, "queueOffset", 0)
	brokerName := strings.TrimSpace(r.URL.Query().Get("brokerName"))
	return rocketmq.MessageQuery{
		MessageID:      strings.TrimSpace(r.URL.Query().Get("messageId")),
		Topic:          strings.TrimSpace(r.URL.Query().Get("topic")),
		Key:            strings.TrimSpace(r.URL.Query().Get("key")),
		BrokerName:     brokerName,
		QueueID:        queueID,
		QueueOffset:    queueOffset,
		HasQueueOffset: brokerName != "" && hasQueueID && hasQueueOffset,
		ConsumerGroup:  strings.TrimSpace(r.URL.Query().Get("consumerGroup")),
		TraceTopic:     strings.TrimSpace(r.URL.Query().Get("traceTopic")),
		BeginTimestamp: int64Query(r, "beginTimestamp"),
		EndTimestamp:   int64Query(r, "endTimestamp"),
		MaxNum:         intQuery(r, "maxNum"),
	}
}

// validateMessageQuery 在启动后台 mqadmin 前做最小必要校验，避免无效查询被缓存成长期失败任务。
func validateMessageQuery(query rocketmq.MessageQuery) error {
	if query.Topic == "" {
		return errors.New("topic 必填")
	}
	if query.MessageID == "" && query.Key == "" {
		return errors.New("messageId 或 key 至少传一个")
	}
	return nil
}

// messageChainCacheKey 用 JSON 固定字段顺序保留完整查询维度，避免同一 messageId 的不同窗口互相覆盖。
func messageChainCacheKey(query rocketmq.MessageQuery) (string, error) {
	data, err := json.Marshal(query)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// messageQueryFromCacheKey 将缓存 key 还原成 Provider 入参，后台刷新只依赖同一个规范化查询对象。
func messageQueryFromCacheKey(key string) (rocketmq.MessageQuery, error) {
	var query rocketmq.MessageQuery
	if err := json.Unmarshal([]byte(key), &query); err != nil {
		return rocketmq.MessageQuery{}, err
	}
	return query, nil
}

// messageChainPlaceholder 生成无缓存首包的占位链路，让前端能展示查询目标并继续短轮询后台结果。
func messageChainPlaceholder(query rocketmq.MessageQuery) rocketmq.MessageStatusChain {
	keys := []string{}
	if query.Key != "" {
		keys = append(keys, query.Key)
	}
	return rocketmq.MessageStatusChain{
		MessageID: query.MessageID,
		Topic:     query.Topic,
		Keys:      keys,
		Detail: rocketmq.MessageDetail{
			MessageID: query.MessageID,
			Topic:     query.Topic,
			Keys:      keys,
		},
		OverallStatus: "REFRESHING",
	}
}

func topicMessagesPlaceholder(query rocketmq.MessageBrowseQuery) rocketmq.TopicMessages {
	return rocketmq.TopicMessages{
		Topic:      query.Topic,
		BrokerName: query.BrokerName,
		QueueID:    query.QueueID,
		Limit:      query.Limit,
		Rows:       []rocketmq.MessageDetail{},
	}
}
