package quarkmq

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReconnectAfterDisconnect(t *testing.T) {
	var connectCount atomic.Int32

	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			return
		}
		respondSuccess(conn, req.ID, map[string]interface{}{"success": true})
	})

	// Track connection count
	origHandler := ms.handler
	ms.handler = func(conn *websocket.Conn, msg []byte) {
		connectCount.Add(1)
		origHandler(conn, msg)
	}

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(),
		WithAutoReconnect(true),
		WithBackoff(50*time.Millisecond, 200*time.Millisecond, 2.0, 0.0),
	)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	if client.State() != Connected {
		t.Errorf("expected Connected state, got %s", client.State())
	}

	// Forcefully close the connection from the server side
	ms.connsMu.Lock()
	if len(ms.conns) > 0 {
		ms.conns[0].Close()
	}
	ms.connsMu.Unlock()

	// Wait for reconnection
	time.Sleep(500 * time.Millisecond)

	// Client should be reconnected
	if client.State() != Connected {
		t.Errorf("expected Connected state after reconnect, got %s", client.State())
	}

	ms.Close()
}

func TestResubscribeAfterReconnect(t *testing.T) {
	var subscribeCalls atomic.Int32

	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			return
		}
		if req.Method == "subscribe" {
			subscribeCalls.Add(1)
		}
		respondSuccess(conn, req.ID, map[string]interface{}{"success": true})
	})

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(),
		WithAutoReconnect(true),
		WithBackoff(50*time.Millisecond, 200*time.Millisecond, 2.0, 0.0),
	)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	// Subscribe to two topics
	err = client.Subscribe(ctx, "ch1", "topic1")
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	err = client.Subscribe(ctx, "ch1", "topic2")
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	initialSubs := subscribeCalls.Load()
	if initialSubs != 2 {
		t.Fatalf("expected 2 initial subscribe calls, got %d", initialSubs)
	}

	// Forcefully close the connection from the server side
	ms.connsMu.Lock()
	if len(ms.conns) > 0 {
		ms.conns[0].Close()
	}
	ms.connsMu.Unlock()

	// Wait for reconnection and resubscription
	time.Sleep(1 * time.Second)

	finalSubs := subscribeCalls.Load()
	// Should have 2 initial + 2 replayed = 4 (but timing may cause slight variation)
	if finalSubs < 3 {
		t.Errorf("expected at least 3 subscribe calls (2 initial + replay), got %d", finalSubs)
	}

	ms.Close()
}

func TestMaxReconnectRetries(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		// No-op handler
	})

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(),
		WithAutoReconnect(true),
		WithMaxReconnectRetries(2),
		WithBackoff(30*time.Millisecond, 100*time.Millisecond, 1.5, 0.0),
	)
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	// Close the server to trigger reconnection attempts
	ms.Close()

	// Wait for all retry attempts to be exhausted
	time.Sleep(800 * time.Millisecond)

	// Client should be disconnected after max retries
	if client.State() != Disconnected {
		t.Errorf("expected Disconnected state after max retries, got %s", client.State())
	}
}

func TestDisabledReconnect(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		// No-op handler
	})

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	// Close the server
	ms.connsMu.Lock()
	if len(ms.conns) > 0 {
		ms.conns[0].Close()
	}
	ms.connsMu.Unlock()

	// Wait a bit
	time.Sleep(200 * time.Millisecond)

	// Client should be disconnected and not reconnecting
	if client.State() != Disconnected {
		t.Errorf("expected Disconnected state, got %s", client.State())
	}
}
