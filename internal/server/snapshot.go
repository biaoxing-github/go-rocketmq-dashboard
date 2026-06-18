package server

import (
	"context"
	"sync"
	"time"
)

// snapshotView 是 HTTP 层读取到的快照视图，字段直接参与接口元信息输出。
type snapshotView[T any] struct {
	Data                 T
	HasData              bool
	Stale                bool
	Refreshing           bool
	LastRefreshUnixMilli int64
	LastError            string
}

// snapshotStore 管理单类 RocketMQ 数据的后台刷新状态，避免用户请求阻塞在 mqadmin 进程上。
type snapshotStore[T any] struct {
	name               string
	ttl                time.Duration
	loader             func(context.Context) (T, error)
	loaderWithPrevious func(context.Context, T, bool) (T, error)
	mu                 sync.RWMutex
	data               T
	hasData            bool
	lastAt             time.Time
	lastErr            string
	running            bool
	refreshN           int64
}

// keyedSnapshotStore 按业务 key 管理一组独立快照，适合 Consumer 详情这类按 group/topic 查询的冷命令。
type keyedSnapshotStore[T any] struct {
	name               string
	ttl                time.Duration
	loader             func(context.Context, string) (T, error)
	loaderWithPrevious func(context.Context, string, T, bool) (T, error)
	mu                 sync.Mutex
	stores             map[string]*snapshotStore[T]
}

// newSnapshotStore 创建一个只读快照仓库，ttl 决定数据多久后标记为 stale 并触发后台刷新。
func newSnapshotStore[T any](name string, ttl time.Duration, loader func(context.Context) (T, error)) *snapshotStore[T] {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &snapshotStore[T]{
		name:   name,
		ttl:    ttl,
		loader: loader,
		loaderWithPrevious: func(ctx context.Context, _ T, _ bool) (T, error) {
			return loader(ctx)
		},
	}
}

// newKeyedSnapshotStore 创建按 key 懒加载的快照仓库；每个 key 内部仍复用 snapshotStore 的并发控制。
func newKeyedSnapshotStore[T any](name string, ttl time.Duration, loader func(context.Context, string) (T, error)) *keyedSnapshotStore[T] {
	return newKeyedSnapshotStoreWithPrevious(name, ttl, loader, func(ctx context.Context, key string, _ T, _ bool) (T, error) {
		return loader(ctx, key)
	})
}

// newKeyedSnapshotStoreWithPrevious 创建可增量刷新的 keyed 快照仓库，loader 可按旧快照跳过重复远端查询。
func newKeyedSnapshotStoreWithPrevious[T any](
	name string,
	ttl time.Duration,
	loader func(context.Context, string) (T, error),
	loaderWithPrevious func(context.Context, string, T, bool) (T, error),
) *keyedSnapshotStore[T] {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &keyedSnapshotStore[T]{
		name:               name,
		ttl:                ttl,
		loader:             loader,
		loaderWithPrevious: loaderWithPrevious,
		stores:             make(map[string]*snapshotStore[T]),
	}
}

// snapshot 返回指定 key 的快照仓库；首次访问时才创建，避免为未点击的详情浪费后台任务。
func (s *keyedSnapshotStore[T]) snapshot(key string) *snapshotStore[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	if store, ok := s.stores[key]; ok {
		return store
	}
	store := newSnapshotStore(s.name+":"+key, s.ttl, func(ctx context.Context) (T, error) {
		return s.loader(ctx, key)
	})
	store.loaderWithPrevious = func(ctx context.Context, previous T, hasPrevious bool) (T, error) {
		return s.loaderWithPrevious(ctx, key, previous, hasPrevious)
	}
	s.stores[key] = store
	return store
}

// delete 移除指定 key 的快照仓库，适合写操作后让相关读缓存失效。
func (s *keyedSnapshotStore[T]) delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.stores[key]; !ok {
		return false
	}
	delete(s.stores, key)
	return true
}

// clear 清空整组 keyed 快照，适合 Topic 写操作后让所有浏览参数重新回源。
func (s *keyedSnapshotStore[T]) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stores = make(map[string]*snapshotStore[T])
}

// view 立即返回当前快照和状态，不等待正在执行的远端 RocketMQ 查询。
func (s *snapshotStore[T]) view(now time.Time) snapshotView[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stale := true
	lastRefresh := int64(0)
	if !s.lastAt.IsZero() {
		stale = now.After(s.lastAt.Add(s.ttl))
		lastRefresh = s.lastAt.UnixMilli()
	}
	return snapshotView[T]{
		Data:                 s.data,
		HasData:              s.hasData,
		Stale:                stale,
		Refreshing:           s.running,
		LastRefreshUnixMilli: lastRefresh,
		LastError:            s.lastErr,
	}
}

// refreshIfStale 在快照过期或尚未加载时触发后台刷新；返回 true 表示本次确实启动了刷新。
func (s *snapshotStore[T]) refreshIfStale(ctx context.Context, now time.Time) bool {
	return s.refreshIfStaleWith(ctx, now, s.loaderWithPrevious)
}

// refreshIfStaleWith 仅在快照过期时执行可读取旧数据的刷新函数，避免未过期缓存被重复回源。
func (s *snapshotStore[T]) refreshIfStaleWith(ctx context.Context, now time.Time, loader func(context.Context, T, bool) (T, error)) bool {
	s.mu.RLock()
	needsRefresh := !s.hasData || now.After(s.lastAt.Add(s.ttl))
	s.mu.RUnlock()
	if !needsRefresh {
		return false
	}
	// HTTP 请求结束后上下文可能立刻取消；快照刷新需要继续完成，避免页面只拿到半截缓存。
	return s.refreshAsyncWith(context.Background(), loader)
}

// refreshAsync 保证同一类数据同时只有一个刷新任务在跑，防止页面并发请求放大 mqadmin 压力。
func (s *snapshotStore[T]) refreshAsync(ctx context.Context) bool {
	return s.refreshAsyncWith(ctx, s.loaderWithPrevious)
}

// refreshAsyncWith 在刷新时把旧快照交给调用方，适合历史消息这类可按 offset 增量复用的数据。
func (s *snapshotStore[T]) refreshAsyncWith(ctx context.Context, loader func(context.Context, T, bool) (T, error)) bool {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return false
	}
	previous := s.data
	hasPrevious := s.hasData
	s.running = true
	s.refreshN++
	s.mu.Unlock()

	go func() {
		data, err := loader(ctx, previous, hasPrevious)
		now := time.Now()

		s.mu.Lock()
		defer s.mu.Unlock()
		s.running = false
		if err != nil {
			s.lastErr = err.Error()
			return
		}
		s.data = data
		s.hasData = true
		s.lastAt = now
		s.lastErr = ""
	}()
	return true
}
