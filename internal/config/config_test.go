package config

import (
	"testing"
	"time"
)

func TestLoadUsesConsumerFriendlyRequestTimeoutDefault(t *testing.T) {
	t.Setenv("RMQD_REQUEST_TIMEOUT_MS", "")

	cfg := Load()
	if cfg.RequestTimeout != 60*time.Second {
		t.Fatalf("expected request timeout default to be 60s, got %s", cfg.RequestTimeout)
	}
}

func TestLoadUsesLongerMessageChainCacheTTL(t *testing.T) {
	t.Setenv("RMQD_CLUSTER_CACHE_TTL_MS", "")
	t.Setenv("RMQD_MESSAGE_CHAIN_CACHE_TTL_MS", "")

	cfg := Load()
	if cfg.MessageChainCacheTTL <= cfg.ClusterCacheTTL {
		t.Fatalf("expected message chain ttl to outlive cluster ttl, chain=%s cluster=%s", cfg.MessageChainCacheTTL, cfg.ClusterCacheTTL)
	}
}

func TestLoadReadsMessageChainCacheAndSidecarSettings(t *testing.T) {
	t.Setenv("RMQD_MESSAGE_CHAIN_CACHE_TTL_MS", "900000")
	t.Setenv("RMQD_ADMIN_SIDECAR_ENABLED", "true")
	t.Setenv("RMQD_ADMIN_SIDECAR_ADDR", "127.0.0.1:19091")
	t.Setenv("RMQD_ADMIN_SIDECAR_CLASSPATH", "/app/rocketmq-admin-sidecar:/opt/rocketmq/lib/*")

	cfg := Load()
	if cfg.MessageChainCacheTTL != 15*time.Minute {
		t.Fatalf("expected message chain ttl from env, got %s", cfg.MessageChainCacheTTL)
	}
	if !cfg.AdminSidecarEnabled {
		t.Fatalf("expected admin sidecar to be enabled")
	}
	if cfg.AdminSidecarAddr != "127.0.0.1:19091" {
		t.Fatalf("unexpected sidecar addr %q", cfg.AdminSidecarAddr)
	}
	if cfg.AdminSidecarClasspath != "/app/rocketmq-admin-sidecar:/opt/rocketmq/lib/*" {
		t.Fatalf("unexpected sidecar classpath %q", cfg.AdminSidecarClasspath)
	}
}

func TestLoadReadsM6AdminProviderSettings(t *testing.T) {
	t.Setenv("RMQD_ADMIN_PROVIDER", "goadmin")
	t.Setenv("RMQD_GO_ADMIN_SHADOW", "true")
	t.Setenv("RMQD_GO_ADMIN_TIMEOUT_MS", "2500")

	cfg := Load()
	if cfg.AdminProvider != "goadmin" {
		t.Fatalf("expected admin provider from env, got %q", cfg.AdminProvider)
	}
	if !cfg.GoAdminShadow {
		t.Fatalf("expected goadmin shadow to be enabled")
	}
	if cfg.GoAdminTimeout != 2500*time.Millisecond {
		t.Fatalf("expected goadmin timeout from env, got %s", cfg.GoAdminTimeout)
	}
}

func TestLoadDefaultsM6AdminProviderToAuto(t *testing.T) {
	t.Setenv("RMQD_ADMIN_PROVIDER", "")

	cfg := Load()
	if cfg.AdminProvider != "auto" {
		t.Fatalf("expected default admin provider auto, got %q", cfg.AdminProvider)
	}
}

func TestLoadReadsRuntimeConfigAndProxySettings(t *testing.T) {
	t.Setenv("RMQD_RUNTIME_CONFIG_ENABLED", "true")
	t.Setenv("RMQD_PROXY_RUNTIME_DIR", "D:/tmp/rocketmq-proxy")
	t.Setenv("RMQD_PROXY_CLUSTER_ID", "prod-b")
	t.Setenv("RMQD_PROXY_ROCKETMQ_HOME", "/srv/rocketmq")
	t.Setenv("RMQD_PROXY_EXTERNAL_HOST", "172.168.1.93")
	t.Setenv("RMQD_PROXY_GRPC_HOST_PORT", "18085")
	t.Setenv("RMQD_PROXY_REMOTING_HOST_PORT", "18080")
	t.Setenv("RMQD_PROXY_HEAP_MB", "768")
	t.Setenv("RMQD_PROXY_START_TIMEOUT_MS", "45000")
	t.Setenv("RMQD_PROXY_STOP_TIMEOUT_MS", "12000")
	t.Setenv("RMQD_PROXY_PROBE_TIMEOUT_MS", "2500")

	cfg := Load()
	if !cfg.RuntimeConfigEnabled {
		t.Fatalf("expected runtime config writes enabled")
	}
	if cfg.ProxyRuntimeDir != "D:/tmp/rocketmq-proxy" || cfg.ProxyRocketMQHome != "/srv/rocketmq" || cfg.ProxyClusterID != "prod-b" {
		t.Fatalf("unexpected proxy paths: dir=%q home=%q cluster=%q", cfg.ProxyRuntimeDir, cfg.ProxyRocketMQHome, cfg.ProxyClusterID)
	}
	if cfg.ProxyExternalHost != "172.168.1.93" || cfg.ProxyGRPCHostPort != 18085 || cfg.ProxyRemotingHostPort != 18080 {
		t.Fatalf("unexpected proxy external endpoint: host=%q grpc=%d remoting=%d", cfg.ProxyExternalHost, cfg.ProxyGRPCHostPort, cfg.ProxyRemotingHostPort)
	}
	if cfg.ProxyHeapMB != 768 || cfg.ProxyStartTimeout != 45*time.Second || cfg.ProxyStopTimeout != 12*time.Second || cfg.ProxyProbeTimeout != 2500*time.Millisecond {
		t.Fatalf("unexpected proxy runtime limits: heap=%d start=%s stop=%s probe=%s", cfg.ProxyHeapMB, cfg.ProxyStartTimeout, cfg.ProxyStopTimeout, cfg.ProxyProbeTimeout)
	}
}

// TestLoadReadsFixedClustersAndAuditSettings 验证多集群和写操作审计配置由部署环境显式提供。
func TestLoadReadsFixedClustersAndAuditSettings(t *testing.T) {
	t.Setenv("RMQD_CLUSTERS_JSON", `[{"id":"prod-a","label":"生产 A","nameServer":"10.0.0.1:9876"},{"id":"prod-b","label":"生产 B","nameServer":"10.0.0.2:9876"}]`)
	t.Setenv("RMQD_AUTH_CREDENTIALS_FILE", "/run/secrets/dashboard-auth.json")
	t.Setenv("RMQD_AUDIT_LOG_PATH", "/var/lib/rmqdash/audit.jsonl")

	cfg := Load()
	if len(cfg.Clusters) != 2 || cfg.Clusters[0].ID != "prod-a" || cfg.Clusters[1].NameServer != "10.0.0.2:9876" {
		t.Fatalf("unexpected fixed clusters %#v", cfg.Clusters)
	}
	if cfg.AuthCredentialsFile != "/run/secrets/dashboard-auth.json" || cfg.AuditLogPath != "/var/lib/rmqdash/audit.jsonl" {
		t.Fatalf("unexpected write-operation paths credentials=%q audit=%q", cfg.AuthCredentialsFile, cfg.AuditLogPath)
	}
}

// TestLoadRejectsInvalidClusterJSON 防止错误的部署配置静默退化为其他集群。
func TestLoadRejectsInvalidClusterJSON(t *testing.T) {
	t.Setenv("RMQD_CLUSTERS_JSON", `{"id":"prod-a"}`)
	defer func() {
		if recover() == nil {
			t.Fatal("expected invalid cluster JSON to panic")
		}
	}()
	Load()
}
