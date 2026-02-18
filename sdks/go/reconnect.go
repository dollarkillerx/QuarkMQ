package quarkmq

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/gorilla/websocket"
)

// setState atomically updates the connection state and triggers the OnStateChange callback.
func (c *Client) setState(newState ConnectionState) {
	oldState := ConnectionState(c.state.Swap(int32(newState)))
	if oldState != newState && c.opts.OnStateChange != nil {
		c.opts.OnStateChange(oldState, newState)
	}
}

// triggerReconnect sends a signal to the reconnect loop.
func (c *Client) triggerReconnect() {
	select {
	case c.reconnectCh <- struct{}{}:
	default:
		// already signaled
	}
}

// reconnectLoop monitors the reconnect channel and performs reconnection with backoff.
func (c *Client) reconnectLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.done:
			return
		case <-c.reconnectCh:
			c.performReconnect()
		}
	}
}

// performReconnect attempts to reconnect with exponential backoff.
func (c *Client) performReconnect() {
	if !c.opts.AutoReconnect {
		return
	}

	c.setState(Connecting)

	backoff := c.opts.InitialBackoff
	attempts := 0

	for {
		select {
		case <-c.done:
			return
		default:
		}

		// Check max retries
		if c.opts.MaxReconnectRetries > 0 && attempts >= c.opts.MaxReconnectRetries {
			c.setState(Disconnected)
			return
		}

		// Wait with backoff
		jitter := 1.0
		if c.opts.BackoffJitter > 0 {
			jitter = 1.0 + (rand.Float64()*2-1)*c.opts.BackoffJitter
		}
		delay := time.Duration(float64(backoff) * jitter)

		select {
		case <-c.done:
			return
		case <-time.After(delay):
		}

		attempts++

		// Attempt connection
		ctx, cancel := context.WithTimeout(context.Background(), c.opts.HandshakeTimeout)
		err := c.dial(ctx)
		cancel()

		if err != nil {
			// Increase backoff
			backoff = time.Duration(float64(backoff) * c.opts.BackoffMultiplier)
			if backoff > c.opts.MaxBackoff {
				backoff = c.opts.MaxBackoff
			}
			continue
		}

		// Connection succeeded
		c.setState(Connected)

		// Replay subscriptions
		c.resubscribe()

		// Start new read loop
		c.wg.Add(1)
		go c.readLoop()

		return
	}
}

// resubscribe replays all recorded subscriptions after a successful reconnect.
func (c *Client) resubscribe() {
	c.subsMu.Lock()
	subs := make([]subscription, len(c.subs))
	copy(subs, c.subs)
	c.subsMu.Unlock()

	for _, sub := range subs {
		params := map[string]interface{}{
			"channel": sub.channel,
			"topic":   sub.topic,
		}
		// Use a short timeout for resubscribe — don't block forever
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.call(ctx, "subscribe", params)
		cancel()
		if err != nil {
			// Log but continue — channels may not exist yet after server restart
			_ = err
		}
	}
}

// failAllPending fails all pending RPC calls with the given error.
func (c *Client) failAllPending(err error) {
	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[int64]chan *JsonRpcResponse)
	c.pendingMu.Unlock()

	errMsg := err.Error()
	for _, ch := range pending {
		// Send a synthetic error response
		raw := json.RawMessage(`null`)
		ch <- &JsonRpcResponse{
			Error: &RpcError{
				Code:    -1,
				Message: errMsg,
			},
			Result: &raw,
		}
	}

	// Close the old websocket connection
	c.connMu.Lock()
	if c.conn != nil {
		_ = c.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		_ = c.conn.Close()
	}
	c.connMu.Unlock()
}

// isClosed returns true if Close() has been called.
func (c *Client) isClosed() bool {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	return c.closed
}
