package quarkmq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestPublishReturnsMessageID(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		if req.Method == "publish" {
			respondSuccess(conn, req.ID, map[string]interface{}{
				"message_id": "test-msg-123",
			})
		}
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	msgID, err := client.Publish(ctx, "test-channel", map[string]interface{}{"data": 1})
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}
	if msgID != "test-msg-123" {
		t.Errorf("expected message ID 'test-msg-123', got '%s'", msgID)
	}
}

func TestContextCancellation(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		// Don't respond — simulate a slow server
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	// Use a context with a very short timeout
	ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err = client.Publish(ctx, "test-channel", map[string]interface{}{"data": 1})
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestDeleteChannel(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		if req.Method == "delete_channel" {
			respondSuccess(conn, req.ID, map[string]interface{}{
				"success": true,
			})
		}
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	err = client.DeleteChannel(ctx, "test-channel")
	if err != nil {
		t.Fatalf("delete channel error: %v", err)
	}
}

func TestRPCErrorPropagation(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		respondError(conn, req.ID, -32000, "channel not found: ghost")
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	_, err = client.Publish(ctx, "ghost", map[string]interface{}{"data": 1})
	if err == nil {
		t.Fatal("expected RPC error")
	}

	rpcErr, ok := err.(*RpcError)
	if !ok {
		t.Fatalf("expected *RpcError, got %T: %v", err, err)
	}
	if rpcErr.Code != -32000 {
		t.Errorf("expected code -32000, got %d", rpcErr.Code)
	}
}

func TestConcurrentRPCCalls(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		respondSuccess(conn, req.ID, map[string]interface{}{
			"message_id": "concurrent-msg",
		})
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	const numCalls = 20
	var wg sync.WaitGroup
	errCh := make(chan error, numCalls)

	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.Publish(ctx, "test-channel", map[string]interface{}{"data": 1})
			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent publish error: %v", err)
	}
}

func TestMessagePushDelivery(t *testing.T) {
	var serverConn *websocket.Conn
	connCh := make(chan struct{}, 1)

	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			return
		}
		if req.Method == "subscribe" {
			serverConn = conn
			respondSuccess(conn, req.ID, map[string]interface{}{"success": true})
			select {
			case connCh <- struct{}{}:
			default:
			}
		}
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	err = client.Subscribe(ctx, "events", "all")
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Wait for subscription to be processed
	<-connCh

	// Send a message push from the server
	sendNotification(serverConn, "message", map[string]interface{}{
		"message_id": "push-msg-1",
		"channel":    "events",
		"payload":    map[string]interface{}{"data": "hello"},
		"attempt":    1,
	})

	// Receive from Messages channel
	select {
	case push := <-client.Messages:
		if push.MessageID != "push-msg-1" {
			t.Errorf("expected message ID 'push-msg-1', got '%s'", push.MessageID)
		}
		if push.Channel != "events" {
			t.Errorf("expected channel 'events', got '%s'", push.Channel)
		}
		if push.Attempt != 1 {
			t.Errorf("expected attempt 1, got %d", push.Attempt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message push")
	}
}

func TestListDlq(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		if req.Method == "list_dlq" {
			respondSuccess(conn, req.ID, map[string]interface{}{
				"messages": []map[string]interface{}{
					{
						"message_id": "dlq-msg-1",
						"channel":    "test-channel",
						"payload":    map[string]interface{}{"data": "dead"},
					},
				},
			})
		}
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	result, err := client.ListDlq(ctx, "test-channel")
	if err != nil {
		t.Fatalf("list dlq error: %v", err)
	}
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(result.Messages))
	}
	if result.Messages[0].MessageID != "dlq-msg-1" {
		t.Errorf("expected message ID 'dlq-msg-1', got '%s'", result.Messages[0].MessageID)
	}
}

func TestRetryDlq(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		if req.Method == "retry_dlq" {
			respondSuccess(conn, req.ID, map[string]interface{}{
				"success": true,
			})
		}
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	err = client.RetryDlq(ctx, "test-channel", "dlq-msg-1")
	if err != nil {
		t.Fatalf("retry dlq error: %v", err)
	}
}

func TestCloseClosesMessagesChannel(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		// No-op handler
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		// This should unblock when Messages is closed
		for range client.Messages {
		}
		close(done)
	}()

	client.Close()

	select {
	case <-done:
		// Success: range over Messages exited
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: range over Messages did not exit after Close()")
	}
}

func TestWriteErrorTriggersReconnect(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			return
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

	// Force close the underlying connection to make writes fail
	client.connMu.Lock()
	client.conn.Close()
	client.connMu.Unlock()

	// Attempt a call — should fail due to write error
	_, err = client.Publish(ctx, "test-channel", map[string]interface{}{"data": 1})
	if err == nil {
		t.Fatal("expected error from write on closed connection")
	}

	// Wait for reconnect to kick in
	time.Sleep(500 * time.Millisecond)

	// Client should have attempted to reconnect (state should be Connected or Connecting)
	state := client.State()
	if state == Disconnected {
		// It may still be reconnecting depending on timing, but it should not stay disconnected
		// if auto-reconnect is enabled. Give more time.
		time.Sleep(500 * time.Millisecond)
		state = client.State()
	}
	// With auto-reconnect enabled and the mock server still running,
	// the client should eventually reconnect
	if state != Connected && state != Connecting {
		t.Errorf("expected Connected or Connecting state after write error reconnect, got %s", state)
	}

	ms.Close()
}

func TestSerializationRoundtrip(t *testing.T) {
	ms := newMockServer(t, func(conn *websocket.Conn, msg []byte) {
		var req JsonRpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			t.Errorf("unmarshal error: %v", err)
			return
		}

		// Echo back the params as the result
		respondSuccess(conn, req.ID, map[string]interface{}{
			"channels": []map[string]interface{}{
				{
					"name":          "ch1",
					"topics":        []string{"t1", "t2"},
					"pending_count": 5,
					"dlq_count":     1,
				},
			},
		})
	})
	defer ms.Close()

	ctx := context.Background()
	client, err := Connect(ctx, ms.URL(), WithAutoReconnect(false))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}
	defer client.Close()

	result, err := client.ListChannels(ctx)
	if err != nil {
		t.Fatalf("list channels error: %v", err)
	}

	if len(result.Channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(result.Channels))
	}
	ch := result.Channels[0]
	if ch.Name != "ch1" {
		t.Errorf("expected name 'ch1', got '%s'", ch.Name)
	}
	if len(ch.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(ch.Topics))
	}
	if ch.PendingCount != 5 {
		t.Errorf("expected pending count 5, got %d", ch.PendingCount)
	}
	if ch.DlqCount != 1 {
		t.Errorf("expected dlq count 1, got %d", ch.DlqCount)
	}
}
