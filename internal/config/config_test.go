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
	t.Setenv("RMQD_PROXY_ROCKETMQ_HOME", "/srv/rocketmq")
	t.Setenv("RMQD_PROXY_HEAP_MB", "768")
	t.Setenv("RMQD_PROXY_START_TIMEOUT_MS", "45000")
	t.Setenv("RMQD_PROXY_STOP_TIMEOUT_MS", "12000")

	cfg := Load()
	if !cfg.RuntimeConfigEnabled {
		t.Fatalf("expected runtime config writes enabled")
	}
	if cfg.ProxyRuntimeDir != "D:/tmp/rocketmq-proxy" || cfg.ProxyRocketMQHome != "/srv/rocketmq" {
		t.Fatalf("unexpected proxy paths: dir=%q home=%q", cfg.ProxyRuntimeDir, cfg.ProxyRocketMQHome)
	}
	if cfg.ProxyHeapMB != 768 || cfg.ProxyStartTimeout != 45*time.Second || cfg.ProxyStopTimeout != 12*time.Second {
		t.Fatalf("unexpected proxy runtime limits: heap=%d start=%s stop=%s", cfg.ProxyHeapMB, cfg.ProxyStartTimeout, cfg.ProxyStopTimeout)
	}
}
