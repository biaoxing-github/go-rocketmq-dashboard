package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/rocketmq"
)

// runtimeConfigTestProvider 使用内存配置模拟官方 get/update 配置命令。
type runtimeConfigTestProvider struct {
	*fakeProvider
	mu               sync.Mutex
	brokerValues     map[string]map[string]string
	nameServerValues map[string]map[string]string
	commands         [][]string
	failUpdateTarget string
}

// ClusterFeatures 根据内存配置生成允许修改的目标画像。
func (p *runtimeConfigTestProvider) ClusterFeatures(ctx context.Context) (rocketmq.ClusterFeatureReport, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	clusters := []rocketmq.Cluster{{Name: "DefaultCluster"}}
	brokerConfigs := make([]rocketmq.BrokerConfigSnapshot, 0, len(p.brokerValues))
	for _, address := range sortedStringMapKeys(p.brokerValues) {
		broker := rocketmq.Broker{Cluster: "DefaultCluster", Name: "broker-" + address, Address: address, Version: "V5_3_2", Activated: true}
		clusters[0].Brokers = append(clusters[0].Brokers, broker)
		brokerConfigs = append(brokerConfigs, rocketmq.BrokerConfigSnapshotFromEntries(broker, configEntriesFromMap(p.brokerValues[address])))
	}
	nameServerConfigs := make([]rocketmq.NameServerConfigSnapshot, 0, len(p.nameServerValues))
	for _, address := range sortedStringMapKeys(p.nameServerValues) {
		nameServerConfigs = append(nameServerConfigs, rocketmq.NameServerConfigSnapshot{NameServer: address, Entries: configEntriesFromMap(p.nameServerValues[address])})
	}
	return rocketmq.BuildClusterFeatureReport("127.0.0.1:9876", clusters, nil, brokerConfigs, nameServerConfigs, nil), nil
}

// RunCommand 解析测试使用的 getBrokerConfig、getNamesrvConfig 和 update 命令。
func (p *runtimeConfigTestProvider) RunCommand(ctx context.Context, args ...string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands = append(p.commands, append([]string(nil), args...))
	if len(args) == 0 {
		return "", errors.New("command required")
	}
	flags := testCommandFlags(args[1:])
	switch strings.ToLower(args[0]) {
	case "getbrokerconfig":
		address := flags["-b"]
		return configCommandOutput("Master: "+address, p.brokerValues[address]), nil
	case "getnamesrvconfig":
		address := flags["-n"]
		return configCommandOutput(address, p.nameServerValues[address]), nil
	case "updatebrokerconfig":
		address := flags["-b"]
		if address == p.failUpdateTarget {
			return "", errors.New("injected broker update failure")
		}
		p.brokerValues[address][flags["-k"]] = flags["-v"]
		return "update broker config success\n", nil
	case "updatenamesrvconfig":
		address := flags["-n"]
		if address == p.failUpdateTarget {
			return "", errors.New("injected nameserver update failure")
		}
		p.nameServerValues[address][flags["-k"]] = flags["-v"]
		return "update name server config success\n", nil
	default:
		return "", fmt.Errorf("unexpected command %s", args[0])
	}
}

// fakeProxyRuntime 记录 API 对 Proxy 运行器的调用。
type fakeProxyRuntime struct {
	snapshot  ProxyRuntimeSnapshot
	applied   ProxyRuntimeApplyRequest
	restarted int
}

// Snapshot 返回测试快照。
func (f *fakeProxyRuntime) Snapshot() ProxyRuntimeSnapshot {
	return f.snapshot
}

// Apply 保存请求并返回运行状态。
func (f *fakeProxyRuntime) Apply(ctx context.Context, request ProxyRuntimeApplyRequest) (ProxyRuntimeSnapshot, error) {
	f.applied = request
	f.snapshot.Enabled = request.Enabled
	f.snapshot.Running = request.Enabled
	f.snapshot.Healthy = request.Enabled
	f.snapshot.Status = "running"
	return f.snapshot, nil
}

// Restart 记录显式重启调用。
func (f *fakeProxyRuntime) Restart(ctx context.Context) (ProxyRuntimeSnapshot, error) {
	f.restarted++
	return f.snapshot, nil
}

func TestRuntimeConfigEndpointUpdatesAndReadsBackBrokerBoolean(t *testing.T) {
	provider := newRuntimeConfigTestProvider()
	proxy := &fakeProxyRuntime{snapshot: ProxyRuntimeSnapshot{Available: true, Status: "disabled"}}
	app := New(mutationTestAppConfig(t, AppConfig{Provider: provider, RuntimeConfigEnabled: true, ProxyRuntime: proxy, ClusterCacheTTL: time.Hour}))

	body := bytes.NewBufferString(`{"scope":"broker","target":"127.0.0.1:10911","key":"autoCreateTopicEnable","value":"true"}`)
	recorder := httptest.NewRecorder()
	request := authorizedMutationRequest(http.MethodPut, "/api/runtime-config", body)
	app.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload responsePayload[runtimeConfigApplyPayload]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Data.Results) != 1 || payload.Data.Results[0].PreviousValue != "false" || payload.Data.Results[0].Value != "true" {
		t.Fatalf("unexpected apply result %#v", payload.Data)
	}
	provider.mu.Lock()
	value := provider.brokerValues["127.0.0.1:10911"]["autoCreateTopicEnable"]
	provider.mu.Unlock()
	if value != "true" {
		t.Fatalf("expected broker config true, got %q", value)
	}
}

func TestRuntimeConfigEndpointUpdatesAllNameServers(t *testing.T) {
	provider := newRuntimeConfigTestProvider()
	provider.nameServerValues["127.0.0.2:9876"] = map[string]string{
		"clusterTest":  "false",
		"rocketmqHome": "/opt/rocketmq",
	}
	app := New(mutationTestAppConfig(t, AppConfig{Provider: provider, RuntimeConfigEnabled: true, ClusterCacheTTL: time.Hour}))

	body := bytes.NewBufferString(`{"scope":"nameserver","target":"*","key":"clusterTest","value":"true"}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, authorizedMutationRequest(http.MethodPut, "/api/runtime-config", body))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload responsePayload[runtimeConfigApplyPayload]
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Data.Results) != 2 {
		t.Fatalf("expected two nameserver readback results, got %#v", payload.Data)
	}
	provider.mu.Lock()
	first := provider.nameServerValues["127.0.0.1:9876"]["clusterTest"]
	second := provider.nameServerValues["127.0.0.2:9876"]["clusterTest"]
	provider.mu.Unlock()
	if first != "true" || second != "true" {
		t.Fatalf("expected both nameservers updated, got first=%q second=%q", first, second)
	}
}

func TestRuntimeConfigEndpointRejectsWrongValueType(t *testing.T) {
	provider := newRuntimeConfigTestProvider()
	app := New(mutationTestAppConfig(t, AppConfig{Provider: provider, RuntimeConfigEnabled: true, ClusterCacheTTL: time.Hour}))
	body := bytes.NewBufferString(`{"scope":"broker","target":"127.0.0.1:10911","key":"autoCreateTopicEnable","value":"sometimes"}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, authorizedMutationRequest(http.MethodPut, "/api/runtime-config", body))
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "true 或 false") {
		t.Fatalf("expected typed validation error, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestRuntimeConfigBatchFailureRollsBackEarlierBroker(t *testing.T) {
	provider := newRuntimeConfigTestProvider()
	provider.brokerValues["127.0.0.2:10911"] = map[string]string{"autoCreateTopicEnable": "false"}
	provider.failUpdateTarget = "127.0.0.2:10911"
	app := New(mutationTestAppConfig(t, AppConfig{Provider: provider, RuntimeConfigEnabled: true, ClusterCacheTTL: time.Hour}))
	body := bytes.NewBufferString(`{"scope":"broker","target":"*","key":"autoCreateTopicEnable","value":"true"}`)
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, authorizedMutationRequest(http.MethodPut, "/api/runtime-config", body))
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "已恢复原值") {
		t.Fatalf("expected rollback error, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	provider.mu.Lock()
	first := provider.brokerValues["127.0.0.1:10911"]["autoCreateTopicEnable"]
	second := provider.brokerValues["127.0.0.2:10911"]["autoCreateTopicEnable"]
	provider.mu.Unlock()
	if first != "false" || second != "false" {
		t.Fatalf("expected both brokers restored, got first=%q second=%q", first, second)
	}
}

func TestRuntimeConfigGetAndProxyActionsExposeManagedState(t *testing.T) {
	provider := newRuntimeConfigTestProvider()
	proxy := &fakeProxyRuntime{snapshot: ProxyRuntimeSnapshot{
		Available:                   true,
		Running:                     true,
		Healthy:                     true,
		Status:                      "running",
		GrpcProbeEndpoint:           "127.0.0.1:8081",
		GrpcProbeSuccessAtUnixMilli: 1721600000000,
		GrpcServices:                []string{rocketMQMessagingServiceName},
	}}
	app := New(mutationTestAppConfig(t, AppConfig{Provider: provider, RuntimeConfigEnabled: true, ProxyRuntime: proxy, ClusterCacheTTL: time.Hour}))

	getRecorder := httptest.NewRecorder()
	app.ServeHTTP(getRecorder, httptest.NewRequest(http.MethodGet, "/api/runtime-config", nil))
	for _, expected := range []string{
		`"clusterWritable":true`,
		`"grpcProbeEndpoint":"127.0.0.1:8081"`,
		`"grpcProbeSuccessAtUnixMilli":1721600000000`,
		`"grpcServices":["apache.rocketmq.v2.MessagingService"]`,
	} {
		if getRecorder.Code != http.StatusOK || !strings.Contains(getRecorder.Body.String(), expected) {
			t.Fatalf("runtime config GET missing %s: %d body=%s", expected, getRecorder.Code, getRecorder.Body.String())
		}
	}

	applyBody := bytes.NewBufferString(`{"enabled":true,"settings":{"namesrvAddr":"127.0.0.1:9876","grpcServerPort":8081}}`)
	applyRecorder := httptest.NewRecorder()
	app.ServeHTTP(applyRecorder, authorizedMutationRequest(http.MethodPut, "/api/runtime-config/proxy", applyBody))
	if applyRecorder.Code != http.StatusOK || !proxy.applied.Enabled || proxy.applied.Settings["namesrvAddr"] != "127.0.0.1:9876" {
		t.Fatalf("unexpected proxy apply: code=%d request=%#v body=%s", applyRecorder.Code, proxy.applied, applyRecorder.Body.String())
	}

	restartRecorder := httptest.NewRecorder()
	app.ServeHTTP(restartRecorder, authorizedMutationRequest(http.MethodPost, "/api/runtime-config/proxy/restart", nil))
	if restartRecorder.Code != http.StatusOK || proxy.restarted != 1 {
		t.Fatalf("unexpected proxy restart: code=%d count=%d body=%s", restartRecorder.Code, proxy.restarted, restartRecorder.Body.String())
	}
}

// TestProxyRuntimeActionsStayWithinSelectedCluster 验证多集群请求只读取和操作对应的 Proxy 运行器。
func TestProxyRuntimeActionsStayWithinSelectedCluster(t *testing.T) {
	provider := newRuntimeConfigTestProvider()
	proxyA := &fakeProxyRuntime{snapshot: ProxyRuntimeSnapshot{Available: true, Status: "disabled"}}
	proxyB := &fakeProxyRuntime{snapshot: ProxyRuntimeSnapshot{Available: true, Status: "running", Running: true}}
	app := New(mutationTestAppConfig(t, AppConfig{
		Provider:             provider,
		RuntimeConfigEnabled: true,
		Clusters: []ClusterDefinition{
			{ID: "cluster-a", Label: "集群 A", NameServer: "ns-a:9876"},
			{ID: "cluster-b", Label: "集群 B", NameServer: "ns-b:9876"},
		},
		ProxyRuntimes: map[string]ProxyRuntime{
			"cluster-a": proxyA,
			"cluster-b": proxyB,
		},
		ClusterCacheTTL: time.Hour,
	}))

	read := httptest.NewRecorder()
	app.ServeHTTP(read, httptest.NewRequest(http.MethodGet, "/api/runtime-config?clusterId=cluster-a", nil))
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), `"status":"disabled"`) {
		t.Fatalf("unexpected cluster-a proxy state: %d body=%s", read.Code, read.Body.String())
	}
	apply := httptest.NewRecorder()
	app.ServeHTTP(apply, authorizedMutationRequest(http.MethodPut, "/api/runtime-config/proxy?clusterId=cluster-b", bytes.NewBufferString(`{"enabled":true,"settings":{"namesrvAddr":"ns-b:9876","grpcServerPort":8081}}`)))
	if apply.Code != http.StatusOK {
		t.Fatalf("expected cluster-b proxy apply success, got %d body=%s", apply.Code, apply.Body.String())
	}
	if proxyA.applied.Enabled || !proxyB.applied.Enabled || proxyB.applied.Settings["namesrvAddr"] != "ns-b:9876" {
		t.Fatalf("proxy operation escaped selected cluster a=%#v b=%#v", proxyA.applied, proxyB.applied)
	}
}

func TestProxyRuntimeManagerPersistsDisabledConfigAndRejectsInvalidPorts(t *testing.T) {
	runtimeDir := t.TempDir()
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, "conf"), 0o755); err != nil {
		t.Fatal(err)
	}
	manager, err := NewProxyRuntimeManager(ProxyRuntimeOptions{
		RuntimeDir:           runtimeDir,
		JavaPath:             filepath.Join(runtimeDir, "missing-java"),
		RocketMQHome:         home,
		NameServer:           "127.0.0.1:9876",
		ExternalHost:         "172.168.1.93",
		ExternalGRPCPort:     18085,
		ExternalRemotingPort: 18080,
		StartTimeout:         50 * time.Millisecond,
		StopTimeout:          50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	settings := defaultProxyRuntimeSettings("127.0.0.1:9876")
	settings["grpcServerPort"] = 19081
	snapshot, err := manager.Apply(context.Background(), ProxyRuntimeApplyRequest{Enabled: false, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Enabled || snapshot.Status != "disabled" || proxySettingInt(manager.currentState().Settings, "grpcServerPort") != 19081 {
		t.Fatalf("unexpected disabled snapshot %#v", snapshot)
	}
	if snapshot.GrpcExternalEndpoint != "172.168.1.93:18085" || snapshot.RemotingExternalEndpoint != "172.168.1.93:18080" {
		t.Fatalf("unexpected proxy external endpoints %#v", snapshot)
	}
	data, err := os.ReadFile(filepath.Join(runtimeDir, "proxy-state.json"))
	if err != nil || !bytes.Contains(data, []byte(`"grpcServerPort": 19081`)) {
		t.Fatalf("expected persisted grpc port, err=%v data=%s", err, data)
	}

	settings["remotingListenPort"] = 19081
	if _, err := manager.Apply(context.Background(), ProxyRuntimeApplyRequest{Enabled: false, Settings: settings}); err == nil || !strings.Contains(err.Error(), "不能相同") {
		t.Fatalf("expected duplicate port validation error, got %v", err)
	}
}

func TestProxyRuntimeManagerPersistsConsecutiveDisabledUpdates(t *testing.T) {
	runtimeDir := t.TempDir()
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, "conf"), 0o755); err != nil {
		t.Fatal(err)
	}
	manager, err := NewProxyRuntimeManager(ProxyRuntimeOptions{
		RuntimeDir:   runtimeDir,
		JavaPath:     filepath.Join(runtimeDir, "missing-java"),
		RocketMQHome: home,
		NameServer:   "127.0.0.1:9876",
	})
	if err != nil {
		t.Fatal(err)
	}
	settings := defaultProxyRuntimeSettings("127.0.0.1:9876")
	settings["grpcThreadPoolNums"] = 33
	if _, err := manager.Apply(context.Background(), ProxyRuntimeApplyRequest{Enabled: false, Settings: settings}); err != nil {
		t.Fatal(err)
	}
	settings["grpcThreadPoolNums"] = 34
	snapshot, err := manager.Apply(context.Background(), ProxyRuntimeApplyRequest{Enabled: false, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	if value := proxySettingInt(manager.currentState().Settings, "grpcThreadPoolNums"); value != 34 {
		t.Fatalf("expected second persisted value 34, got %d", value)
	}
	if value := proxyFieldValue(snapshot.Fields, "grpcThreadPoolNums"); value != 34 {
		t.Fatalf("expected second snapshot value 34, got %#v", value)
	}
}

func TestProxyRuntimeManagerRestoresDisabledStateWhenStartIsUnavailable(t *testing.T) {
	runtimeDir := t.TempDir()
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, "conf"), 0o755); err != nil {
		t.Fatal(err)
	}
	manager, err := NewProxyRuntimeManager(ProxyRuntimeOptions{
		RuntimeDir:   runtimeDir,
		JavaPath:     filepath.Join(runtimeDir, "missing-java"),
		RocketMQHome: home,
		NameServer:   "127.0.0.1:9876",
		StartTimeout: 50 * time.Millisecond,
		StopTimeout:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, applyErr := manager.Apply(context.Background(), ProxyRuntimeApplyRequest{Enabled: true, Settings: defaultProxyRuntimeSettings("127.0.0.1:9876")})
	if applyErr == nil || !strings.Contains(applyErr.Error(), "已恢复旧配置") {
		t.Fatalf("expected restored startup failure, got %v", applyErr)
	}
	state := manager.currentState()
	if state.Enabled || manager.Snapshot().Status != "disabled" {
		t.Fatalf("expected previous disabled state, state=%#v snapshot=%#v", state, manager.Snapshot())
	}
}

// newRuntimeConfigTestProvider 创建包含一个 Broker 和一个 NameServer 的配置命令测试夹具。
func newRuntimeConfigTestProvider() *runtimeConfigTestProvider {
	return &runtimeConfigTestProvider{
		fakeProvider: &fakeProvider{},
		brokerValues: map[string]map[string]string{
			"127.0.0.1:10911": {
				"autoCreateTopicEnable": "false",
				"brokerRole":            "ASYNC_MASTER",
				"listenPort":            "10911",
			},
		},
		nameServerValues: map[string]map[string]string{
			"127.0.0.1:9876": {
				"clusterTest":  "false",
				"rocketmqHome": "/opt/rocketmq",
			},
		},
	}
}

// testCommandFlags 将测试命令的成对参数解析成 map。
func testCommandFlags(args []string) map[string]string {
	result := make(map[string]string)
	for index := 0; index+1 < len(args); index += 2 {
		result[args[index]] = args[index+1]
	}
	return result
}

// configCommandOutput 生成官方 getConfig 可被解析器读取的配置段。
func configCommandOutput(header string, values map[string]string) string {
	var builder strings.Builder
	builder.WriteString("============")
	builder.WriteString(header)
	builder.WriteString("============\n")
	for _, key := range sortedStringMapKeys(values) {
		builder.WriteString(key)
		builder.WriteString(" = ")
		builder.WriteString(values[key])
		builder.WriteByte('\n')
	}
	return builder.String()
}

// configEntriesFromMap 将内存配置转成稳定排序的快照 entries。
func configEntriesFromMap(values map[string]string) []rocketmq.ConfigEntry {
	entries := make([]rocketmq.ConfigEntry, 0, len(values))
	for _, key := range sortedStringMapKeys(values) {
		entries = append(entries, rocketmq.ConfigEntry{Key: key, Value: values[key]})
	}
	return entries
}

// sortedStringMapKeys 返回字符串 map 的稳定键顺序。
func sortedStringMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// proxyFieldValue 从 Proxy 快照中读取一个字段值，供持久化回归测试使用。
func proxyFieldValue(fields []ProxyRuntimeField, key string) any {
	for _, field := range fields {
		if field.Key == key {
			return field.Value
		}
	}
	return nil
}
