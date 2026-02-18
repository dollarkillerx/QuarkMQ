package quarkmq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

// mockServer is a mock WebSocket server for testing the QuarkMQ client.
type mockServer struct {
	server  *httptest.Server
	conns   []*websocket.Conn
	connsMu sync.Mutex
	handler func(conn *websocket.Conn, msg []byte)
	t       *testing.T
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// newMockServer creates a mock WebSocket server with a custom message handler.
func newMockServer(t *testing.T, handler func(conn *websocket.Conn, msg []byte)) *mockServer {
	t.Helper()
	ms := &mockServer{
		handler: handler,
		t:       t,
	}
	ms.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade error: %v", err)
			return
		}
		ms.connsMu.Lock()
		ms.conns = append(ms.conns, conn)
		ms.connsMu.Unlock()

		go func() {
			defer conn.Close()
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}
				if ms.handler != nil {
					ms.handler(conn, msg)
				}
			}
		}()
	}))
	return ms
}

// URL returns the WebSocket URL of the mock server.
func (ms *mockServer) URL() string {
	return "ws" + strings.TrimPrefix(ms.server.URL, "http")
}

// Close shuts down the mock server.
func (ms *mockServer) Close() {
	ms.connsMu.Lock()
	for _, conn := range ms.conns {
		conn.Close()
	}
	ms.connsMu.Unlock()
	ms.server.Close()
}

// respondSuccess sends a successful JSON-RPC response.
func respondSuccess(conn *websocket.Conn, id interface{}, result interface{}) {
	resultBytes, _ := json.Marshal(result)
	raw := json.RawMessage(resultBytes)
	resp := JsonRpcResponse{
		Jsonrpc: "2.0",
		Result:  &raw,
		ID:      id,
	}
	data, _ := json.Marshal(resp)
	_ = conn.WriteMessage(websocket.TextMessage, data)
}

// respondError sends an error JSON-RPC response.
func respondError(conn *websocket.Conn, id interface{}, code int, message string) {
	resp := JsonRpcResponse{
		Jsonrpc: "2.0",
		Error: &RpcError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}
	data, _ := json.Marshal(resp)
	_ = conn.WriteMessage(websocket.TextMessage, data)
}

// sendNotification sends a JSON-RPC notification (server push).
func sendNotification(conn *websocket.Conn, method string, params interface{}) {
	req := JsonRpcRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
	}
	data, _ := json.Marshal(req)
	_ = conn.WriteMessage(websocket.TextMessage, data)
}
