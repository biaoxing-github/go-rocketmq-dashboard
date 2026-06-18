package cache

import (
	"testing"
	"time"
)

func TestTTLCacheReturnsValueUntilExpired(t *testing.T) {
	now := time.Unix(1717651200, 0)
	cache := NewTTLCache[string](time.Second)
	cache.Set(now, "clusters")

	value, ok := cache.Get(now.Add(900 * time.Millisecond))
	if !ok || value != "clusters" {
		t.Fatalf("expected cached value before ttl, got value=%q ok=%v", value, ok)
	}

	_, ok = cache.Get(now.Add(2 * time.Second))
	if ok {
		t.Fatalf("expected cache miss after ttl")
	}
}
