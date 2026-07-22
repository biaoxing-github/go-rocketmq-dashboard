package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// proxyProbeMessagingService 是 Reflection 测试服务的最小接口，服务名保持 RocketMQ Proxy 约定。
type proxyProbeMessagingService interface {
	proxyProbeMessagingService()
}

// proxyProbeMessagingServiceImpl 为服务发现测试提供无方法的实现。
type proxyProbeMessagingServiceImpl struct{}

func (proxyProbeMessagingServiceImpl) proxyProbeMessagingService() {}

// startProxyGRPCProbeTestServer 启动仅供本地单元测试使用的 plaintext gRPC Reflection 服务。
func startProxyGRPCProbeTestServer(t *testing.T, registerMessagingService bool) (int, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	if registerMessagingService {
		grpcServer.RegisterService(&grpc.ServiceDesc{
			ServiceName: rocketMQMessagingServiceName,
			HandlerType: (*proxyProbeMessagingService)(nil),
		}, proxyProbeMessagingServiceImpl{})
	}
	reflection.Register(grpcServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	return listener.Addr().(*net.TCPAddr).Port, func() {
		grpcServer.Stop()
		_ = listener.Close()
	}
}

// TestProbeProxyGRPCDiscoversRocketMQMessagingService 验证探针完成真实 HTTP/2 gRPC Reflection 服务发现。
func TestProbeProxyGRPCDiscoversRocketMQMessagingService(t *testing.T) {
	port, stop := startProxyGRPCProbeTestServer(t, true)
	defer stop()
	probeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := probeProxyGRPC(probeContext, port)
	if err != nil {
		t.Fatalf("probe gRPC reflection: %v", err)
	}
	if result.Endpoint != proxyGRPCProbeEndpoint(port) {
		t.Fatalf("unexpected probe endpoint %q", result.Endpoint)
	}
	if !containsProxyGRPCService(result.Services, rocketMQMessagingServiceName) {
		t.Fatalf("expected RocketMQ MessagingService in reflection response %#v", result.Services)
	}
}

// TestProbeProxyGRPCRejectsReflectionWithoutRocketMQService 防止只要任意 gRPC 服务存活就被误判为 Proxy 就绪。
func TestProbeProxyGRPCRejectsReflectionWithoutRocketMQService(t *testing.T) {
	port, stop := startProxyGRPCProbeTestServer(t, false)
	defer stop()
	probeContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := probeProxyGRPC(probeContext, port)
	if err == nil || !strings.Contains(err.Error(), rocketMQMessagingServiceName) {
		t.Fatalf("expected missing RocketMQ service error, result=%#v err=%v", result, err)
	}
	if result.Endpoint != proxyGRPCProbeEndpoint(port) {
		t.Fatalf("unexpected failed probe endpoint %q", result.Endpoint)
	}
}

// TestProxyRuntimeSnapshotReportsReflectionProbeDetails 验证面板快照带上探测端点、成功时间和发现的服务。
func TestProxyRuntimeSnapshotReportsReflectionProbeDetails(t *testing.T) {
	port, stop := startProxyGRPCProbeTestServer(t, true)
	defer stop()
	manager := &ProxyRuntimeManager{
		options: normalizeProxyRuntimeOptions(ProxyRuntimeOptions{ProbeTimeout: time.Second}),
		state: proxyRuntimeState{
			Enabled: true,
			Settings: map[string]any{
				"grpcServerPort":     port,
				"remotingListenPort": 8080,
			},
		},
		running: true,
		status:  "running",
	}

	snapshot := manager.Snapshot()
	if !snapshot.Healthy || snapshot.Status != "running" {
		t.Fatalf("expected reflection-ready snapshot %#v", snapshot)
	}
	if snapshot.GrpcProbeEndpoint != proxyGRPCProbeEndpoint(port) || snapshot.GrpcProbeSuccessAtUnixMilli == 0 || snapshot.GrpcProbeError != "" {
		t.Fatalf("unexpected gRPC probe snapshot %#v", snapshot)
	}
	if !containsProxyGRPCService(snapshot.GrpcServices, rocketMQMessagingServiceName) {
		t.Fatalf("expected discovered RocketMQ service %#v", snapshot.GrpcServices)
	}
}

// TestProxyRuntimeSnapshotReportsReflectionFailure 验证监听端口可失效时页面得到具体探测失败结论。
func TestProxyRuntimeSnapshotReportsReflectionFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	manager := &ProxyRuntimeManager{
		options: normalizeProxyRuntimeOptions(ProxyRuntimeOptions{ProbeTimeout: 100 * time.Millisecond}),
		state: proxyRuntimeState{
			Enabled: true,
			Settings: map[string]any{
				"grpcServerPort":     port,
				"remotingListenPort": 8080,
			},
		},
		running: true,
		status:  "running",
	}

	snapshot := manager.Snapshot()
	if snapshot.Healthy || snapshot.Status != "error" || snapshot.GrpcProbeError == "" {
		t.Fatalf("expected reflection failure snapshot %#v", snapshot)
	}
	if !strings.Contains(snapshot.LastError, "gRPC Reflection 探测失败") || snapshot.GrpcProbeEndpoint != proxyGRPCProbeEndpoint(port) {
		t.Fatalf("expected endpoint and failure reason %#v", snapshot)
	}
}

// containsProxyGRPCService 判断 Reflection 结果是否包含目标服务。
func containsProxyGRPCService(services []string, expected string) bool {
	for _, service := range services {
		if service == expected {
			return true
		}
	}
	return false
}
