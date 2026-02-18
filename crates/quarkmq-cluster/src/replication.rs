use crate::error::ClusterError;
use crate::gossip::NodeInfo;

/// Represents a WAL record to be replicated to replicas.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicationEntry {
    pub channel: String,
    pub sequence: u64,
    pub operation: u8,
    pub message_id: uuid::Uuid,
    pub data: Vec<u8>,
}

/// Handles async replication of WAL entries from primary to replicas.
/// In a full implementation, this would use TCP connections to replicas.
pub struct Replicator {
    #[allow(dead_code)]
    local_node_id: String,
}

impl Replicator {
    pub fn new(local_node_id: String) -> Self {
        Self { local_node_id }
    }

    /// Replicate a WAL entry to the given replicas.
    /// This is a placeholder for TCP-based async replication.
    pub async fn replicate(
        &self,
        entry: &ReplicationEntry,
        replicas: &[NodeInfo],
    ) -> Result<usize, ClusterError> {
        let mut success_count = 0;

        for replica in replicas {
            match self.send_to_replica(entry, replica).await {
                Ok(()) => {
                    success_count += 1;
                    tracing::debug!(
                        replica = %replica.id,
                        channel = %entry.channel,
                        sequence = entry.sequence,
                        "replicated entry"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        replica = %replica.id,
                        error = %e,
                        "failed to replicate entry"
                    );
                }
            }
        }

        Ok(success_count)
    }

    /// Send a single entry to a replica node.
    /// Placeholder — in production this opens a TCP connection to the replica's
    /// replication_addr and streams the serialized entry.
    async fn send_to_replica(
        &self,
        _entry: &ReplicationEntry,
        _replica: &NodeInfo,
    ) -> Result<(), ClusterError> {
        // TODO: Implement TCP-based replication
        // For now, this is a no-op that succeeds
        Ok(())
    }
}
