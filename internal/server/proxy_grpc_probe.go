package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_reflection_v1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

const rocketMQMessagingServiceName = "apache.rocketmq.v2.MessagingService"

// proxyGRPCProbeResult 保存一次只读 Reflection 服务发现的实际结果。
type proxyGRPCProbeResult struct {
	// Endpoint 是 Dashboard 在容器内实际拨号的 gRPC 地址。
	Endpoint string
	// Services 是 Reflection 返回的服务全名，按名称稳定排序。
	Services []string
}

// probeProxyGRPC 通过 Reflection ListServices 验证 HTTP/2、gRPC 和 RocketMQ 服务均可用。
func probeProxyGRPC(ctx context.Context, port int) (proxyGRPCProbeResult, error) {
	result := proxyGRPCProbeResult{Endpoint: proxyGRPCProbeEndpoint(port)}
	if result.Endpoint == "" {
		return result, errors.New("gRPC 探测端口无效")
	}
	connection, err := grpc.DialContext(
		ctx,
		result.Endpoint,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return result, fmt.Errorf("连接 %s 失败: %w", result.Endpoint, err)
	}
	defer connection.Close()

	client := grpc_reflection_v1alpha.NewServerReflectionClient(connection)
	stream, err := client.ServerReflectionInfo(ctx)
	if err != nil {
		return result, fmt.Errorf("打开 Reflection 流失败: %w", err)
	}
	defer stream.CloseSend()
	if err := stream.Send(&grpc_reflection_v1alpha.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1alpha.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		return result, fmt.Errorf("请求 Reflection 服务列表失败: %w", err)
	}
	response, err := stream.Recv()
	if err != nil {
		return result, fmt.Errorf("读取 Reflection 服务列表失败: %w", err)
	}
	if reflectionError := response.GetErrorResponse(); reflectionError != nil {
		return result, fmt.Errorf("Reflection 返回错误: %s", reflectionError.GetErrorMessage())
	}
	serviceResponse := response.GetListServicesResponse()
	if serviceResponse == nil {
		return result, errors.New("Reflection 未返回服务列表")
	}
	services := make([]string, 0, len(serviceResponse.GetService()))
	foundMessagingService := false
	for _, service := range serviceResponse.GetService() {
		name := service.GetName()
		if name == "" {
			continue
		}
		services = append(services, name)
		if name == rocketMQMessagingServiceName {
			foundMessagingService = true
		}
	}
	sort.Strings(services)
	result.Services = services
	if len(services) == 0 {
		return result, errors.New("Reflection 返回的服务列表为空")
	}
	if !foundMessagingService {
		return result, fmt.Errorf("Reflection 未发现 RocketMQ 服务 %s", rocketMQMessagingServiceName)
	}
	return result, nil
}

// proxyGRPCProbeEndpoint 返回容器内只读探针的实际目标；0.0.0.0 仅是监听地址，不能用于拨号。
func proxyGRPCProbeEndpoint(port int) string {
	if port <= 0 {
		return ""
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
