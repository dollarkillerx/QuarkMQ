use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("storage error: {0}")]
    Storage(#[from] quarkmq_storage::StorageError),
    #[error("topic not found: {0}")]
    TopicNotFound(String),
    #[error("topic already exists: {0}")]
    TopicAlreadyExists(String),
    #[error("partition not found: {topic} partition {partition}")]
    PartitionNotFound { topic: String, partition: i32 },
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, BrokerError>;
