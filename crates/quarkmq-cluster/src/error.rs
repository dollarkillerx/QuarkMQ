use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("replication failed: {0}")]
    ReplicationFailed(String),

    #[error("gossip error: {0}")]
    GossipError(String),

    #[error("not primary for channel: {0}")]
    NotPrimary(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
