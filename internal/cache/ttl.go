package cache

import (
	"sync"
	"time"
)

// Entry 是一个带过期时间的缓存值，用于降低 Dashboard 高频刷新时对 RocketMQ 的压力。
type Entry[T any] struct {
	value     T
	expiresAt time.Time
}

// TTLCache 提供线程安全的短 TTL 缓存，适合 cluster/topic/consumer 这类秒级刷新数据。
type TTLCache[T any] struct {
	mu    sync.RWMutex
	entry *Entry[T]
	ttl   time.Duration
}

// NewTTLCache 创建固定 TTL 的单值缓存。
func NewTTLCache[T any](ttl time.Duration) *TTLCache[T] {
	return &TTLCache[T]{ttl: ttl}
}

// Get 返回未过期的缓存值；缓存不存在或过期时返回 false。
func (c *TTLCache[T]) Get(now time.Time) (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.entry == nil || now.After(c.entry.expiresAt) {
		var zero T
		return zero, false
	}
	return c.entry.value, true
}

// Set 写入缓存值并刷新过期时间。
func (c *TTLCache[T]) Set(now time.Time, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entry = &Entry[T]{
		value:     value,
		expiresAt: now.Add(c.ttl),
	}
}
