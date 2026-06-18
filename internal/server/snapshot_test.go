package server

import (
	"context"
	"testing"
	"time"
)

func TestSnapshotStoreReturnsImmediatelyWhileRefreshRuns(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	store := newSnapshotStore("clusters", time.Second, func(ctx context.Context) ([]string, error) {
		close(started)
		<-release
		return []string{"broker-a"}, nil
	})

	triggered := store.refreshAsync(context.Background())
	if !triggered {
		t.Fatalf("expected first refresh to start")
	}
	<-started

	before := time.Now()
	view := store.view(time.Now())
	if time.Since(before) > 50*time.Millisecond {
		t.Fatalf("snapshot view blocked on slow refresh")
	}
	if view.HasData || !view.Refreshing || !view.Stale {
		t.Fatalf("unexpected in-flight view: %#v", view)
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view = store.view(time.Now())
		if view.HasData && !view.Refreshing {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !view.HasData || view.Refreshing || view.Stale {
		t.Fatalf("expected refreshed snapshot, got %#v", view)
	}
	if len(view.Data) != 1 || view.Data[0] != "broker-a" {
		t.Fatalf("unexpected snapshot data: %#v", view.Data)
	}
	if view.LastRefreshUnixMilli <= 0 {
		t.Fatalf("expected refresh timestamp")
	}
}

func TestSnapshotStoreAllowsOnlyOneConcurrentRefresh(t *testing.T) {
	release := make(chan struct{})
	store := newSnapshotStore("topics", time.Second, func(ctx context.Context) ([]string, error) {
		<-release
		return []string{"topic-a"}, nil
	})

	if !store.refreshAsync(context.Background()) {
		t.Fatalf("expected first refresh to start")
	}
	if store.refreshAsync(context.Background()) {
		t.Fatalf("expected second refresh to be ignored while first is running")
	}
	close(release)
}

func TestSnapshotStoreRefreshAsyncWithReceivesPreviousSnapshot(t *testing.T) {
	store := newSnapshotStore("topic-messages", time.Second, func(ctx context.Context) ([]string, error) {
		return []string{"offset-1"}, nil
	})

	if !store.refreshAsync(context.Background()) {
		t.Fatalf("expected first refresh to start")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view := store.view(time.Now())
		if view.HasData && !view.Refreshing {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	seenPrevious := false
	triggered := store.refreshAsyncWith(context.Background(), func(ctx context.Context, previous []string, hasPrevious bool) ([]string, error) {
		if !hasPrevious || len(previous) != 1 || previous[0] != "offset-1" {
			t.Fatalf("expected previous snapshot, got hasPrevious=%v previous=%#v", hasPrevious, previous)
		}
		seenPrevious = true
		return append(previous, "offset-2"), nil
	})
	if !triggered {
		t.Fatalf("expected refresh with previous to start")
	}

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		view := store.view(time.Now())
		if view.HasData && !view.Refreshing && len(view.Data) == 2 {
			if !seenPrevious {
				t.Fatalf("expected loader to observe previous snapshot")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("snapshot did not refresh with previous data")
}
