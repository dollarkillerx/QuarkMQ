package quarkmq

import "time"

// StateChangeFunc is called when the client connection state changes.
type StateChangeFunc func(oldState, newState ConnectionState)

// ResubscribeErrorFunc is called when a subscription replay fails after reconnect.
type ResubscribeErrorFunc func(channel, topic string, err error)

// Options configures the QuarkMQ client behavior.
type Options struct {
	// HandshakeTimeout is the timeout for the WebSocket handshake.
	HandshakeTimeout time.Duration

	// DefaultRPCTimeout is the default timeout for RPC calls when no context deadline is set.
	DefaultRPCTimeout time.Duration

	// MessageBufferSize is the capacity of the Messages channel.
	MessageBufferSize int

	// AutoReconnect enables automatic reconnection on disconnect.
	AutoReconnect bool

	// MaxReconnectRetries is the maximum number of reconnection attempts.
	// 0 means unlimited retries.
	MaxReconnectRetries int

	// InitialBackoff is the initial delay before the first reconnection attempt.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between reconnection attempts.
	MaxBackoff time.Duration

	// BackoffMultiplier is the factor by which the backoff delay increases.
	BackoffMultiplier float64

	// BackoffJitter adds randomness to the backoff delay to prevent thundering herd.
	// Value between 0 and 1 (e.g., 0.1 = ±10% jitter).
	BackoffJitter float64

	// OnStateChange is called when the connection state changes.
	OnStateChange StateChangeFunc

	// OnResubscribeError is called when a subscription replay fails after reconnect.
	OnResubscribeError ResubscribeErrorFunc
}

// Option is a functional option for configuring the client.
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		HandshakeTimeout:    10 * time.Second,
		DefaultRPCTimeout:   10 * time.Second,
		MessageBufferSize:   1000,
		AutoReconnect:       true,
		MaxReconnectRetries: 0, // unlimited
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          30 * time.Second,
		BackoffMultiplier:   2.0,
		BackoffJitter:       0.1,
	}
}

// WithHandshakeTimeout sets the WebSocket handshake timeout.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.HandshakeTimeout = d
	}
}

// WithDefaultRPCTimeout sets the default RPC timeout when no context deadline is set.
func WithDefaultRPCTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.DefaultRPCTimeout = d
	}
}

// WithMessageBufferSize sets the capacity of the Messages channel.
func WithMessageBufferSize(size int) Option {
	return func(o *Options) {
		o.MessageBufferSize = size
	}
}

// WithAutoReconnect enables or disables automatic reconnection.
func WithAutoReconnect(enabled bool) Option {
	return func(o *Options) {
		o.AutoReconnect = enabled
	}
}

// WithMaxReconnectRetries sets the maximum number of reconnection attempts (0 = unlimited).
func WithMaxReconnectRetries(n int) Option {
	return func(o *Options) {
		o.MaxReconnectRetries = n
	}
}

// WithBackoff configures the reconnection backoff parameters.
func WithBackoff(initial, max time.Duration, multiplier, jitter float64) Option {
	return func(o *Options) {
		o.InitialBackoff = initial
		o.MaxBackoff = max
		o.BackoffMultiplier = multiplier
		o.BackoffJitter = jitter
	}
}

// WithOnStateChange sets the callback invoked on connection state changes.
func WithOnStateChange(fn StateChangeFunc) Option {
	return func(o *Options) {
		o.OnStateChange = fn
	}
}

// WithOnResubscribeError sets the callback invoked when subscription replay fails after reconnect.
func WithOnResubscribeError(fn ResubscribeErrorFunc) Option {
	return func(o *Options) {
		o.OnResubscribeError = fn
	}
}
