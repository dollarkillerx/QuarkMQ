package quarkmq

// ConnectionState represents the current state of the client connection.
type ConnectionState int32

const (
	// Disconnected indicates the client is not connected to the server.
	Disconnected ConnectionState = iota
	// Connecting indicates the client is attempting to connect or reconnect.
	Connecting
	// Connected indicates the client has an active connection to the server.
	Connected
)

// String returns a human-readable representation of the connection state.
func (s ConnectionState) String() string {
	switch s {
	case Disconnected:
		return "Disconnected"
	case Connecting:
		return "Connecting"
	case Connected:
		return "Connected"
	default:
		return "Unknown"
	}
}
