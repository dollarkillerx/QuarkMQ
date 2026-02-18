use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("RPC error (code={code}): {message}")]
    Rpc { code: i32, message: String },

    #[error("timeout waiting for response")]
    Timeout,

    #[error("connection closed")]
    ConnectionClosed,
}
