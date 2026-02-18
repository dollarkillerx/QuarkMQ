use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid JSON-RPC request: {0}")]
    InvalidRequest(String),

    #[error("unknown method: {0}")]
    UnknownMethod(String),

    #[error("invalid params: {0}")]
    InvalidParams(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
