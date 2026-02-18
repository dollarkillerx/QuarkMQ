package quarkmq

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// JsonRpcRequest represents a JSON-RPC 2.0 request.
type JsonRpcRequest struct {
	Jsonrpc string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      interface{} `json:"id,omitempty"`
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
}

// ListChannelsResult is returned by ListChannels.
type ListChannelsResult struct {
	Channels []ChannelInfo `json:"channels"`
}

// Client is a QuarkMQ client that communicates via WebSocket + JSON-RPC.
type Client struct {
	conn     *websocket.Conn
	nextID   atomic.Int64
	pending  map[int64]chan *JsonRpcResponse
	mu       sync.Mutex
	Messages chan *MessagePush
	done     chan struct{}
}

// Connect establishes a WebSocket connection to the QuarkMQ server.
func Connect(url string) (*Client, error) {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	c := &Client{
		conn:     conn,
		pending:  make(map[int64]chan *JsonRpcResponse),
		Messages: make(chan *MessagePush, 1000),
		done:     make(chan struct{}),
	}
	c.nextID.Store(1)

	go c.readLoop()

	return c, nil
}

// Close closes the WebSocket connection.
func (c *Client) Close() error {
	close(c.done)
	return c.conn.Close()
}

func (c *Client) readLoop() {
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		// Try as response
		var resp JsonRpcResponse
		if json.Unmarshal(msg, &resp) == nil && resp.ID != nil {
			if idFloat, ok := resp.ID.(float64); ok {
				id := int64(idFloat)
				c.mu.Lock()
				ch, exists := c.pending[id]
				if exists {
					delete(c.pending, id)
				}
				c.mu.Unlock()
				if exists {
					ch <- &resp
				}
				continue
			}
		}

		// Try as notification (server push)
		var req JsonRpcRequest
		if json.Unmarshal(msg, &req) == nil && req.Method == "message" {
			paramsBytes, _ := json.Marshal(req.Params)
			var push MessagePush
			if json.Unmarshal(paramsBytes, &push) == nil {
				select {
				case c.Messages <- &push:
				default:
					// Drop if channel full
				}
			}
		}
	}
}

func (c *Client) call(method string, params interface{}) (*JsonRpcResponse, error) {
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

	ch := make(chan *JsonRpcResponse, 1)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case resp := <-ch:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp, nil
	case <-time.After(10 * time.Second):
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("timeout waiting for response")
	case <-c.done:
		return nil, fmt.Errorf("connection closed")
	}
}

// CreateChannel creates a new channel.
func (c *Client) CreateChannel(name string, ackTimeoutSecs int, maxDeliveryAttempts int) error {
	params := map[string]interface{}{
		"name":                   name,
		"ack_timeout_secs":       ackTimeoutSecs,
		"max_delivery_attempts":  maxDeliveryAttempts,
	}
	_, err := c.call("create_channel", params)
	return err
}

// Publish sends a message to a channel.
func (c *Client) Publish(channel string, payload interface{}) (string, error) {
	params := map[string]interface{}{
		"channel": channel,
		"payload": payload,
	}
	resp, err := c.call("publish", params)
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

// Subscribe subscribes to a channel topic.
func (c *Client) Subscribe(channel, topic string) error {
	params := map[string]interface{}{
		"channel": channel,
		"topic":   topic,
	}
	_, err := c.call("subscribe", params)
	return err
}

// Ack acknowledges a message.
func (c *Client) Ack(messageID string) error {
	params := map[string]interface{}{
		"message_id": messageID,
	}
	_, err := c.call("ack", params)
	return err
}

// Nack negatively acknowledges a message (triggers redelivery).
func (c *Client) Nack(messageID string) error {
	params := map[string]interface{}{
		"message_id": messageID,
	}
	_, err := c.call("nack", params)
	return err
}

// ListChannels returns all channels.
func (c *Client) ListChannels() (*ListChannelsResult, error) {
	resp, err := c.call("list_channels", map[string]interface{}{})
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
