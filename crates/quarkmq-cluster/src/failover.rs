use crate::error::ClusterError;
use crate::gossip::ClusterState;
use crate::routing::Router;

/// Manages automatic failover when a primary node goes down.
pub struct FailoverManager {
    local_node_id: String,
    router: Router,
}

impl FailoverManager {
    pub fn new(local_node_id: String, router: Router) -> Self {
        Self {
            local_node_id,
            router,
        }
    }

    /// Handle a detected node failure.
    /// Checks if this node should become the new primary for any channels
    /// previously owned by the failed node.
    pub async fn handle_node_failure(
        &self,
        failed_node_id: &str,
        channels: &[String],
        cluster: &ClusterState,
    ) -> Result<Vec<String>, ClusterError> {
        let mut promoted_channels = Vec::new();

        for channel in channels {
            if let Some(new_primary) = self
                .router
                .elect_new_primary(channel, failed_node_id, cluster)
                .await
            {
                if new_primary.id == self.local_node_id {
                    tracing::info!(
                        channel = %channel,
                        failed_node = %failed_node_id,
                        "this node promoted to primary"
                    );
                    promoted_channels.push(channel.clone());
                }
            }
        }

        Ok(promoted_channels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::{GossipConfig, NodeInfo, NodeStatus};

    fn make_node(id: &str) -> NodeInfo {
        NodeInfo {
            id: id.to_string(),
            ws_addr: format!("127.0.0.1:987{}", id.chars().last().unwrap()),
            replication_addr: format!("127.0.0.1:988{}", id.chars().last().unwrap()),
            status: NodeStatus::Alive,
            channels: vec![],
            last_heartbeat_ms: 0,
        }
    }

    #[tokio::test]
    async fn test_failover_promotes_lowest_id() {
        let state = ClusterState::new("node-2".to_string(), GossipConfig::default());
        state.update_node(make_node("node-2")).await;
        state.update_node(make_node("node-3")).await;
        // node-1 has failed (not in alive nodes)

        let router = Router::new(2);
        let fm = FailoverManager::new("node-2".to_string(), router);

        let promoted = fm
            .handle_node_failure("node-1", &["orders".to_string()], &state)
            .await
            .unwrap();

        // node-2 is the lowest alive ID, so it should be promoted
        assert_eq!(promoted, vec!["orders"]);
    }

    #[tokio::test]
    async fn test_failover_does_not_promote_non_lowest() {
        let state = ClusterState::new("node-3".to_string(), GossipConfig::default());
        state.update_node(make_node("node-2")).await;
        state.update_node(make_node("node-3")).await;

        let router = Router::new(2);
        let fm = FailoverManager::new("node-3".to_string(), router);

        let promoted = fm
            .handle_node_failure("node-1", &["orders".to_string()], &state)
            .await
            .unwrap();

        // node-2 should be promoted, not node-3
        assert!(promoted.is_empty());
    }
}
