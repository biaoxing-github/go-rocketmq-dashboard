package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"rocketmq-go-dashboard/internal/config"
	"rocketmq-go-dashboard/internal/server"
)

// TestResolveProxyRuntimeBinding 要求单个 Proxy 在多集群部署中明确归属一个 clusterId。
func TestResolveProxyRuntimeBinding(t *testing.T) {
	tests := []struct {
		name               string
		config             config.Config
		clusters           []server.ClusterDefinition
		fallbackNameServer string
		expectedID         string
		expectedNameServer string
		expectedEnabled    bool
		wantError          bool
	}{
		{
			name:               "legacy default cluster",
			fallbackNameServer: "legacy:9876",
			expectedID:         "default",
			expectedNameServer: "legacy:9876",
			expectedEnabled:    true,
		},
		{
			name:               "single configured cluster",
			clusters:           []server.ClusterDefinition{{ID: "prod-a", NameServer: "a:9876"}},
			expectedID:         "prod-a",
			expectedNameServer: "a:9876",
			expectedEnabled:    true,
		},
		{
			name:            "multiple clusters require explicit binding",
			clusters:        []server.ClusterDefinition{{ID: "prod-a", NameServer: "a:9876"}, {ID: "prod-b", NameServer: "b:9876"}},
			expectedEnabled: false,
		},
		{
			name:               "multiple clusters explicit binding",
			config:             config.Config{ProxyClusterID: "prod-b"},
			clusters:           []server.ClusterDefinition{{ID: "prod-a", NameServer: "a:9876"}, {ID: "prod-b", NameServer: "b:9876"}},
			expectedID:         "prod-b",
			expectedNameServer: "b:9876",
			expectedEnabled:    true,
		},
		{
			name:      "unknown configured binding",
			config:    config.Config{ProxyClusterID: "missing"},
			clusters:  []server.ClusterDefinition{{ID: "prod-a", NameServer: "a:9876"}},
			wantError: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			clusterID, nameServer, enabled, err := resolveProxyRuntimeBinding(testCase.config, testCase.clusters, testCase.fallbackNameServer)
			if testCase.wantError {
				if err == nil {
					t.Fatal("expected binding error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve proxy binding: %v", err)
			}
			if clusterID != testCase.expectedID || nameServer != testCase.expectedNameServer || enabled != testCase.expectedEnabled {
				t.Fatalf("unexpected binding id=%q nameserver=%q enabled=%t", clusterID, nameServer, enabled)
			}
		})
	}
}

func TestNativeTopicMessageByOffsetReaderMapsNativeOutput(t *testing.T) {
	var gotArgs []string
	reader := nativeTopicMessageByOffsetReader("native-namesrv:9876", 750*time.Millisecond, func(ctx context.Context, args []string, timeout time.Duration) (string, bool, error) {
		gotArgs = append([]string(nil), args...)
		if timeout != 750*time.Millisecond {
			t.Fatalf("unexpected native timeout %s", timeout)
		}
		return `OffsetID:            native-offset-id
Topic:               native_topic
Tags:                [notice]
Keys:                [native-key]
Queue ID:            2
Queue Offset:        42
Reconsume Times:     1
Born Timestamp:      2026-06-06 19:48:01
Born Host:           10.0.0.8:51111
Store Timestamp:     2026-06-06 19:48:02
Store Host:          127.0.0.1:10911
System Flag:         8
Properties:          {UNIQ_KEY=native-trace-id, TRAN_MSG=true, __transactionId__=native-transaction}
Message Body:        {"event":"native"}`, true, nil
	})

	message, err := reader(context.Background(), "native_topic", "broker-a", 2, 42)
	if err != nil {
		t.Fatalf("read native topic message: %v", err)
	}
	expectedArgs := []string{
		"queryMsgByOffset",
		"-n", "native-namesrv:9876",
		"-t", "native_topic",
		"-b", "broker-a",
		"-i", "2",
		"-o", "42",
		"-f", "UTF-8",
	}
	if !reflect.DeepEqual(gotArgs, expectedArgs) {
		t.Fatalf("native command args mismatch\nexpected=%#v\nactual=%#v", expectedArgs, gotArgs)
	}
	if message.MessageID != "native-offset-id" || message.BrokerName != "broker-a" || message.QueueID != 2 || message.QueueOffset != 42 {
		t.Fatalf("unexpected native message mapping %#v", message)
	}
	if message.TraceMessageID != "native-trace-id" || !message.Transaction.Enabled || message.Transaction.State != "COMMITTED" || message.Transaction.TransactionID != "native-transaction" {
		t.Fatalf("expected transaction and trace fields from native output, got %#v", message)
	}
}

func TestMQAdminProviderForModeAttachesNativeTopicMessageReader(t *testing.T) {
	provider := mqAdminProviderForMode(config.Config{RequestTimeout: time.Second}, "native-namesrv:9876")
	if provider.NativeMessageByOffset == nil {
		t.Fatal("expected Dashboard provider to attach native topic message reader")
	}
}
