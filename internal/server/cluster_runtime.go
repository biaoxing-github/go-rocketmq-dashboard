package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

// ClusterDefinition 描述一个可由 Dashboard 管理的固定 RocketMQ 集群。
// 集群列表仅在启动时加载，避免某个浏览器请求修改其他用户的运行目标。
type ClusterDefinition struct {
	// ID 是 API 请求携带的稳定集群标识。
	ID string `json:"id"`
	// Label 是页面中展示给操作人员的名称。
	Label string `json:"label"`
	// NameServer 是该集群的 NameServer 地址。
	NameServer string `json:"nameServer"`
}

// clusterRuntime 将 Provider 与所有快照缓存绑定到同一个 clusterId。
// 该对象在启动后不再替换，因此不同请求不会互相切换数据源。
type clusterRuntime struct {
	definition ClusterDefinition
	provider   rocketmq.Provider
	// proxyRuntime 只管理当前集群绑定的 Proxy，避免跨 clusterId 操作同一进程。
	proxyRuntime ProxyRuntime

	clusterSnapshot         *snapshotStore[[]rocketmq.Cluster]
	topicSnapshot           *snapshotStore[[]rocketmq.Topic]
	consumerSnapshot        *snapshotStore[[]rocketmq.ConsumerGroup]
	featureSnapshot         *snapshotStore[rocketmq.ClusterFeatureReport]
	topicRouteSnapshots     *keyedSnapshotStore[rocketmq.TopicRoute]
	topicStatusSnapshots    *keyedSnapshotStore[rocketmq.TopicStatus]
	topicMessageSnapshots   *keyedSnapshotStore[rocketmq.TopicMessages]
	brokerStatusSnapshots   *keyedSnapshotStore[rocketmq.BrokerStatus]
	consumerDetailSnapshots *keyedSnapshotStore[rocketmq.ConsumerDetail]
	messageChainSnapshots   *keyedSnapshotStore[rocketmq.MessageStatusChain]

	// runtimeConfigMu 仅串行化同一集群的动态配置读写与回滚。
	runtimeConfigMu sync.Mutex
}

// clusterRuntimeContextKey 是请求上下文中运行时对象的私有键。
type clusterRuntimeContextKey struct{}

// newClusterRuntime 构建一个集群独占的 Provider 和快照仓库。
func newClusterRuntime(definition ClusterDefinition, provider rocketmq.Provider, clusterTTL, messageChainTTL time.Duration) *clusterRuntime {
	if provider == nil {
		provider = rocketmq.SampleProvider{}
	}
	runtime := &clusterRuntime{
		definition: definition,
		provider:   provider,
	}
	runtime.clusterSnapshot = newSnapshotStore("clusters:"+definition.ID, clusterTTL, provider.ClusterList)
	runtime.topicSnapshot = newSnapshotStore("topics:"+definition.ID, clusterTTL, provider.TopicList)
	runtime.consumerSnapshot = newSnapshotStore("consumers:"+definition.ID, clusterTTL, provider.ConsumerGroups)
	if featureProvider, ok := provider.(clusterFeaturesProvider); ok {
		runtime.featureSnapshot = newSnapshotStore("features:"+definition.ID, clusterTTL, featureProvider.ClusterFeatures)
	} else {
		runtime.featureSnapshot = newSnapshotStore("features:"+definition.ID, clusterTTL, func(context.Context) (rocketmq.ClusterFeatureReport, error) {
			return rocketmq.ClusterFeatureReport{}, errors.New("当前 Provider 不支持能力画像")
		})
	}
	runtime.topicRouteSnapshots = newKeyedSnapshotStore("topic-route:"+definition.ID, clusterTTL, func(ctx context.Context, key string) (rocketmq.TopicRoute, error) {
		return provider.TopicRoute(ctx, key)
	})
	runtime.topicStatusSnapshots = newKeyedSnapshotStore("topic-status:"+definition.ID, clusterTTL, func(ctx context.Context, key string) (rocketmq.TopicStatus, error) {
		return provider.TopicStatus(ctx, key)
	})
	loadTopicMessages := func(ctx context.Context, key string) (rocketmq.TopicMessages, error) {
		query, err := messageBrowseQueryFromCacheKey(key)
		if err != nil {
			return rocketmq.TopicMessages{}, err
		}
		return provider.TopicMessages(ctx, query)
	}
	runtime.topicMessageSnapshots = newKeyedSnapshotStoreWithPrevious("topic-messages:"+definition.ID, clusterTTL, loadTopicMessages, func(ctx context.Context, key string, previous rocketmq.TopicMessages, hasPrevious bool) (rocketmq.TopicMessages, error) {
		query, err := messageBrowseQueryFromCacheKey(key)
		if err != nil {
			return rocketmq.TopicMessages{}, err
		}
		if hasPrevious {
			if incrementalProvider, ok := provider.(topicMessagesIncrementalProvider); ok {
				return incrementalProvider.TopicMessagesIncremental(ctx, query, previous)
			}
		}
		return provider.TopicMessages(ctx, query)
	})
	runtime.brokerStatusSnapshots = newKeyedSnapshotStore("broker-status:"+definition.ID, clusterTTL, func(ctx context.Context, key string) (rocketmq.BrokerStatus, error) {
		return provider.BrokerStatus(ctx, key)
	})
	runtime.consumerDetailSnapshots = newKeyedSnapshotStore("consumer-detail:"+definition.ID, clusterTTL, func(ctx context.Context, key string) (rocketmq.ConsumerDetail, error) {
		group, topic := splitConsumerDetailCacheKey(key)
		return provider.ConsumerDetail(ctx, group, topic)
	})
	runtime.messageChainSnapshots = newKeyedSnapshotStore("message-chain:"+definition.ID, messageChainTTL, func(ctx context.Context, key string) (rocketmq.MessageStatusChain, error) {
		query, err := messageQueryFromCacheKey(key)
		if err != nil {
			return rocketmq.MessageStatusChain{}, err
		}
		return provider.MessageChain(ctx, query)
	})
	return runtime
}

// refreshSnapshots 触发同一集群的核心快照后台刷新。
func (r *clusterRuntime) refreshSnapshots(ctx context.Context) {
	r.clusterSnapshot.refreshAsync(ctx)
	r.topicSnapshot.refreshAsync(ctx)
	r.consumerSnapshot.refreshAsync(ctx)
}

// invalidateTopicCaches 清理同一集群中受 Topic 写操作影响的按键缓存。
func (r *clusterRuntime) invalidateTopicCaches(topic string) {
	// Topic 列表快照由调用方显式触发后台刷新；其余快照按 Topic 键清理即可。
	r.topicRouteSnapshots.clear()
	r.topicStatusSnapshots.clear()
	r.topicMessageSnapshots.clear()
}

// normalizeClusterDefinitions 兼容单集群启动参数，并将旧 NameServer 列表转成稳定运行时定义。
func normalizeClusterDefinitions(definitions []ClusterDefinition, fallbackNameServer string, nameServerOptions []string) []ClusterDefinition {
	generatedSampleCluster := false
	if len(definitions) == 0 {
		nameServers := normalizeNameServerOptions(fallbackNameServer, nameServerOptions)
		if len(nameServers) == 0 {
			// 保留未配置 NameServer 时的样例 Provider 启动语义，仅限自动生成的默认集群。
			definitions = []ClusterDefinition{{ID: "default", Label: "默认集群"}}
			generatedSampleCluster = true
		} else {
			definitions = make([]ClusterDefinition, 0, len(nameServers))
			for index, nameServer := range nameServers {
				id := "default"
				label := "默认集群"
				if index > 0 {
					id = fmt.Sprintf("cluster-%d", index+1)
					label = fmt.Sprintf("集群 %d", index+1)
				}
				definitions = append(definitions, ClusterDefinition{ID: id, Label: label, NameServer: nameServer})
			}
		}
	}
	if len(definitions) == 0 {
		panic("至少需要配置一个 RocketMQ 集群")
	}
	seen := make(map[string]struct{}, len(definitions))
	normalized := make([]ClusterDefinition, 0, len(definitions))
	for _, definition := range definitions {
		definition.ID = strings.TrimSpace(definition.ID)
		definition.Label = strings.TrimSpace(definition.Label)
		definition.NameServer = strings.TrimSpace(definition.NameServer)
		if definition.ID == "" || (!generatedSampleCluster && definition.NameServer == "") {
			panic("集群 ID 和 NameServer 均为必填项")
		}
		if definition.Label == "" {
			definition.Label = definition.ID
		}
		if _, exists := seen[definition.ID]; exists {
			panic("集群 ID 不能重复: " + definition.ID)
		}
		seen[definition.ID] = struct{}{}
		normalized = append(normalized, definition)
	}
	sort.SliceStable(normalized, func(left, right int) bool {
		return normalized[left].ID < normalized[right].ID
	})
	return normalized
}
