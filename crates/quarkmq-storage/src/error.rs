use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC mismatch: expected {expected}, got {actual}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("corrupt WAL record at offset {offset}")]
    CorruptRecord { offset: u64 },

    #[error("segment full")]
    SegmentFull,

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
