use thiserror::Error;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("broker error: {0}")]
    Broker(#[from] quarkmq_broker::BrokerError),

    #[error("config error: {0}")]
    Config(String),

    #[error("bind error: {0}")]
    Bind(String),
}
