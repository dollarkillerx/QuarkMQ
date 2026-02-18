use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt segment: {0}")]
    Corrupt(String),
    #[error("offset out of range: {0}")]
    OffsetOutOfRange(i64),
    #[error("invalid record batch: {0}")]
    InvalidRecordBatch(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
