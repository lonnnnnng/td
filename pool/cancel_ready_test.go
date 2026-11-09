package pool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCanceledConnectReturnsReadyConnectionToPool(t *testing.T) {
	created := make(chan mockConn, 1)
	p := NewDC(context.Background(), 2, func() Conn {
		conn := newMockConn(false)
		created <- conn
		return conn
	}, DCOptions{MaxOpenConnections: 1})
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.acquire(ctx); done <- err }()
	conn := <-created
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// long: 快进取消初次请求时连接仍在建立，随后就绪的连接必须回到池中供新位置使用。
	conn.ready.Signal()
	retry, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	got, err := p.acquire(retry)
	if err != nil {
		t.Fatalf("ready connection lost after canceled acquire: %v", err)
	}
	p.release(got)
	p.mu.Lock()
	total, free := p.total, len(p.free)
	p.mu.Unlock()
	if total != 1 || free != 1 {
		t.Fatalf("unexpected pool accounting: total=%d free=%d", total, free)
	}
}

func TestCanceledConnectCleanupStopsWhenPoolCloses(t *testing.T) {
	created := make(chan mockConn, 1)
	p := NewDC(context.Background(), 2, func() Conn {
		conn := newMockConn(false)
		created <- conn
		return conn
	}, DCOptions{MaxOpenConnections: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := p.acquire(ctx); done <- err }()
	<-created
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pool close waited for an abandoned connection to become ready")
	}
}
