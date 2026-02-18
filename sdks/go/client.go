package quarkmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// JsonRpcRequest represents a JSON-RPC 2.0 request for outbound calls.
type JsonRpcRequest struct {
	Jsonrpc string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      interface{} `json:"id,omitempty"`
}

// jsonRpcNotification is used for inbound notifications with raw params.
type jsonRpcNotification struct {
	Jsonrpc string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id,omitempty"`
}

// JsonRpcResponse represents a JSON-RPC 2.0 response.
type JsonRpcResponse struct {
	Jsonrpc string           `json:"jsonrpc"`
	Result  *json.RawMessage `json:"result,omitempty"`
	Error   *RpcError        `json:"error,omitempty"`
	ID      interface{}      `json:"id"`
}

// RpcError represents a JSON-RPC error object.
type RpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *RpcError) Error() string {
	return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
}

// MessagePush is a server-push notification for a delivered message.
type MessagePush struct {
	MessageID string          `json:"message_id"`
	Channel   string          `json:"channel"`
	Payload   json.RawMessage `json:"payload"`
	Attempt   int             `json:"attempt"`
}

// PublishResult is returned by Publish.
type PublishResult struct {
	MessageID string `json:"message_id"`
}

// ChannelInfo represents a channel in the list.
type ChannelInfo struct {
	Name         string   `json:"name"`
	Topics       []string `json:"topics"`
	PendingCount int      `json:"pending_count"`
	DlqCount     int      `json:"dlq_count"`
}

// ListChannelsResult is returned by ListChannels.
type ListChannelsResult struct {
	Channels []ChannelInfo `json:"channels"`
}

// subscription records a subscribe call for replay after reconnect.
type subscription struct {
	channel string
	topic   string
}

// Client is a QuarkMQ client that communicates via WebSocket + JSON-RPC.
type Client struct {
	url  string
	opts *Options

	conn    *websocket.Conn
	connMu  sync.Mutex // protects conn writes
	nextID  atomic.Int64
	pending map[int64]chan *JsonRpcResponse

	pendingMu sync.Mutex // protects pending map

	Messages chan *MessagePush
	done     chan struct{}
	wg       sync.WaitGroup
	respPool sync.Pool

	state atomic.Int32

	subs   []subscription
	subsMu sync.Mutex

	reconnectCh chan struct{}
	closedMu    sync.Mutex
	closed      bool
}

// Connect establishes a WebSocket connection to the QuarkMQ server.
func Connect(ctx context.Context, url string, opts ...Option) (*Client, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	c := &Client{
		url:     url,
		opts:    options,
		pending: make(map[int64]chan *JsonRpcResponse),
		Messages: make(chan *MessagePush, options.MessageBufferSize),
		done:    make(chan struct{}),
		respPool: sync.Pool{
			New: func() interface{} {
				return make(chan *JsonRpcResponse, 1)
			},
		},
		reconnectCh: make(chan struct{}, 1),
	}
	c.nextID.Store(1)

	if err := c.dial(ctx); err != nil {
		return nil, err
	}

	c.setState(Connected)

	c.wg.Add(1)
	go c.readLoop()

	if options.AutoReconnect {
		c.wg.Add(1)
		go c.reconnectLoop()
	}

	return c, nil
}

// Close closes the WebSocket connection and waits for goroutines to exit.
func (c *Client) Close() error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return nil
	}
	c.closed = true
	c.closedMu.Unlock()

	close(c.done)
	c.connMu.Lock()
	err := c.conn.Close()
	c.connMu.Unlock()
	c.wg.Wait()
	close(c.Messages)
	c.setState(Disconnected)
	return err
}

// State returns the current connection state.
func (c *Client) State() ConnectionState {
	return ConnectionState(c.state.Load())
}

func (c *Client) dial(ctx context.Context) error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = c.opts.HandshakeTimeout

	conn, _, err := dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	return nil
}

func (c *Client) readLoop() {
	defer c.wg.Done()

	// Capture conn reference under lock to avoid racing with dial().
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-c.done:
				return
			default:
			}

			c.failAllPending(fmt.Errorf("connection lost: %w", err))
			c.setState(Disconnected)
			c.triggerReconnect()
			return
		}

		// Try as response
		var resp JsonRpcResponse
		if json.Unmarshal(msg, &resp) == nil && resp.ID != nil {
			if idFloat, ok := resp.ID.(float64); ok {
				id := int64(idFloat)
				c.pendingMu.Lock()
				ch, exists := c.pending[id]
				if exists {
					delete(c.pending, id)
				}
				c.pendingMu.Unlock()
				if exists {
					ch <- &resp
				}
				continue
			}
		}

		// Try as notification (server push)
		var notif jsonRpcNotification
		if json.Unmarshal(msg, &notif) == nil && notif.Method == "message" {
			var push MessagePush
			if json.Unmarshal(notif.Params, &push) == nil {
				select {
				case c.Messages <- &push:
				default:
					// Drop if channel full
				}
			}
		}
	}
}

func (c *Client) call(ctx context.Context, method string, params interface{}) (*JsonRpcResponse, error) {
	id := c.nextID.Add(1) - 1
	req := JsonRpcRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ch := c.respPool.Get().(chan *JsonRpcResponse)
	// Drain any stale data from recycled channel
	select {
	case <-ch:
	default:
	}

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	c.connMu.Lock()
	writeErr := c.conn.WriteMessage(websocket.TextMessage, data)
	c.connMu.Unlock()

	if writeErr != nil {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		c.respPool.Put(ch)
		// Trigger reconnect on write failure
		c.failAllPending(fmt.Errorf("write failed: %w", writeErr))
		c.setState(Disconnected)
		c.triggerReconnect()
		return nil, writeErr
	}

	// Determine timeout: use context deadline if set, otherwise default RPC timeout
	var timeoutCh <-chan time.Time
	if deadline, ok := ctx.Deadline(); ok {
		timeoutCh = time.After(time.Until(deadline))
	} else {
		timeoutCh = time.After(c.opts.DefaultRPCTimeout)
	}

	select {
	case resp := <-ch:
		c.respPool.Put(ch)
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp, nil
	case <-timeoutCh:
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		c.respPool.Put(ch)
		return nil, fmt.Errorf("timeout waiting for response")
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		c.respPool.Put(ch)
		return nil, ctx.Err()
	case <-c.done:
		c.respPool.Put(ch)
		return nil, fmt.Errorf("connection closed")
	}
}

// CreateChannel creates a new channel.
func (c *Client) CreateChannel(ctx context.Context, name string, ackTimeoutSecs int, maxDeliveryAttempts int) error {
	params := map[string]interface{}{
		"name":                  name,
		"ack_timeout_secs":      ackTimeoutSecs,
		"max_delivery_attempts": maxDeliveryAttempts,
	}
	_, err := c.call(ctx, "create_channel", params)
	return err
}

// DeleteChannel deletes a channel.
func (c *Client) DeleteChannel(ctx context.Context, name string) error {
	params := map[string]interface{}{
		"name": name,
	}
	_, err := c.call(ctx, "delete_channel", params)
	return err
}

// Publish sends a message to a channel.
func (c *Client) Publish(ctx context.Context, channel string, payload interface{}) (string, error) {
	params := map[string]interface{}{
		"channel": channel,
		"payload": payload,
	}
	resp, err := c.call(ctx, "publish", params)
	if err != nil {
		return "", err
	}

	var result PublishResult
	if resp.Result != nil {
		if err := json.Unmarshal(*resp.Result, &result); err != nil {
			return "", err
		}
	}
	return result.MessageID, nil
}

// Subscribe subscribes to a channel topic. The subscription is tracked for replay after reconnect.
func (c *Client) Subscribe(ctx context.Context, channel, topic string) error {
	params := map[string]interface{}{
		"channel": channel,
		"topic":   topic,
	}
	_, err := c.call(ctx, "subscribe", params)
	if err != nil {
		return err
	}

	// Track subscription for reconnect replay
	c.subsMu.Lock()
	c.subs = append(c.subs, subscription{channel: channel, topic: topic})
	c.subsMu.Unlock()

	return nil
}

// Ack acknowledges a message.
func (c *Client) Ack(ctx context.Context, messageID string) error {
	params := map[string]interface{}{
		"message_id": messageID,
	}
	_, err := c.call(ctx, "ack", params)
	return err
}

// Nack negatively acknowledges a message (triggers redelivery).
func (c *Client) Nack(ctx context.Context, messageID string) error {
	params := map[string]interface{}{
		"message_id": messageID,
	}
	_, err := c.call(ctx, "nack", params)
	return err
}

// DlqMessage represents a message in the dead-letter queue.
type DlqMessage struct {
	MessageID string          `json:"message_id"`
	Channel   string          `json:"channel"`
	Payload   json.RawMessage `json:"payload"`
}

// ListDlqResult is returned by ListDlq.
type ListDlqResult struct {
	Messages []DlqMessage `json:"messages"`
}

// ListDlq returns the dead-letter queue messages for a channel.
func (c *Client) ListDlq(ctx context.Context, channel string) (*ListDlqResult, error) {
	params := map[string]interface{}{
		"channel": channel,
	}
	resp, err := c.call(ctx, "list_dlq", params)
	if err != nil {
		return nil, err
	}

	var result ListDlqResult
	if resp.Result != nil {
		if err := json.Unmarshal(*resp.Result, &result); err != nil {
			return nil, err
		}
	}
	return &result, nil
}

// RetryDlq retries a dead-lettered message by moving it back to pending.
func (c *Client) RetryDlq(ctx context.Context, channel, messageID string) error {
	params := map[string]interface{}{
		"channel":    channel,
		"message_id": messageID,
	}
	_, err := c.call(ctx, "retry_dlq", params)
	return err
}

// ListChannels returns all channels.
func (c *Client) ListChannels(ctx context.Context) (*ListChannelsResult, error) {
	resp, err := c.call(ctx, "list_channels", map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	var result ListChannelsResult
	if resp.Result != nil {
		if err := json.Unmarshal(*resp.Result, &result); err != nil {
			return nil, err
		}
	}
	return &result, nil
}
