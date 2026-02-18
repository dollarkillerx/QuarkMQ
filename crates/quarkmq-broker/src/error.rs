use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("channel not found: {0}")]
    ChannelNotFound(String),

    #[error("channel already exists: {0}")]
    ChannelAlreadyExists(String),

    #[error("topic not found: {0}")]
    TopicNotFound(String),

    #[error("consumer not subscribed")]
    NotSubscribed,

    #[error("message not found: {0}")]
    MessageNotFound(uuid::Uuid),

    #[error("message not inflight: {0}")]
    MessageNotInflight(uuid::Uuid),

    #[error("message dead-lettered: {0}")]
    MessageDeadLettered(uuid::Uuid),

    #[error("storage error: {0}")]
    Storage(#[from] quarkmq_storage::StorageError),

    #[error("protocol error: {0}")]
    Protocol(#[from] quarkmq_protocol::ProtocolError),
}
