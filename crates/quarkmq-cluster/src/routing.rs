use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::gossip::{ClusterState, NodeInfo};

/// Routes channels to primary nodes using consistent hashing.
pub struct Router {
    replication_factor: usize,
}

impl Router {
    pub fn new(replication_factor: usize) -> Self {
        Self { replication_factor }
    }

    /// Determine which node is the primary for a given channel.
    /// Uses a simple hash-based assignment across alive nodes.
    pub async fn primary_for_channel(
        &self,
        channel_name: &str,
        cluster: &ClusterState,
    ) -> Option<NodeInfo> {
        let alive = cluster.alive_nodes().await;
        if alive.is_empty() {
            return None;
        }

        let mut sorted = alive;
        sorted.sort_by(|a, b| a.id.cmp(&b.id));

        let mut hasher = DefaultHasher::new();
        channel_name.hash(&mut hasher);
        let hash = hasher.finish();
        let index = (hash % sorted.len() as u64) as usize;

        Some(sorted[index].clone())
    }

    /// Determine replica nodes for a channel (excluding the primary).
    pub async fn replicas_for_channel(
        &self,
        channel_name: &str,
        cluster: &ClusterState,
    ) -> Vec<NodeInfo> {
        let alive = cluster.alive_nodes().await;
        if alive.len() <= 1 {
            return Vec::new();
        }

        let mut sorted = alive;
        sorted.sort_by(|a, b| a.id.cmp(&b.id));

        let mut hasher = DefaultHasher::new();
        channel_name.hash(&mut hasher);
        let hash = hasher.finish();
        let primary_index = (hash % sorted.len() as u64) as usize;

        let mut replicas = Vec::new();
        let count = (self.replication_factor - 1).min(sorted.len() - 1);
        for i in 1..=count {
            let replica_index = (primary_index + i) % sorted.len();
            replicas.push(sorted[replica_index].clone());
        }
        replicas
    }

    /// Check if the local node is the primary for a channel.
    pub async fn is_primary(
        &self,
        channel_name: &str,
        local_node_id: &str,
        cluster: &ClusterState,
    ) -> bool {
        if let Some(primary) = self.primary_for_channel(channel_name, cluster).await {
            primary.id == local_node_id
        } else {
            false
        }
    }

    /// Select the new primary after a node failure.
    /// Uses the lowest-ID alive replica.
    pub async fn elect_new_primary(
        &self,
        channel_name: &str,
        failed_node_id: &str,
        cluster: &ClusterState,
    ) -> Option<NodeInfo> {
        let alive = cluster.alive_nodes().await;
        let mut candidates: Vec<_> = alive
            .into_iter()
            .filter(|n| n.id != failed_node_id)
            .collect();
        candidates.sort_by(|a, b| a.id.cmp(&b.id));
        candidates.into_iter().next()
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
    async fn test_primary_assignment_deterministic() {
        let state = ClusterState::new("node-1".to_string(), GossipConfig::default());
        state.update_node(make_node("node-1")).await;
        state.update_node(make_node("node-2")).await;
        state.update_node(make_node("node-3")).await;

        let router = Router::new(2);

        let primary1 = router
            .primary_for_channel("orders", &state)
            .await
            .unwrap();
        let primary2 = router
            .primary_for_channel("orders", &state)
            .await
            .unwrap();

        assert_eq!(primary1.id, primary2.id, "primary should be deterministic");
    }

    #[tokio::test]
    async fn test_replicas_exclude_primary() {
        let state = ClusterState::new("node-1".to_string(), GossipConfig::default());
        state.update_node(make_node("node-1")).await;
        state.update_node(make_node("node-2")).await;
        state.update_node(make_node("node-3")).await;

        let router = Router::new(2);

        let primary = router
            .primary_for_channel("orders", &state)
            .await
            .unwrap();
        let replicas = router.replicas_for_channel("orders", &state).await;

        assert_eq!(replicas.len(), 1); // replication_factor=2 means 1 replica
        assert_ne!(replicas[0].id, primary.id);
    }

    #[tokio::test]
    async fn test_elect_new_primary_excludes_failed() {
        let state = ClusterState::new("node-1".to_string(), GossipConfig::default());
        state.update_node(make_node("node-1")).await;
        state.update_node(make_node("node-2")).await;
        state.update_node(make_node("node-3")).await;

        let router = Router::new(2);

        let new_primary = router
            .elect_new_primary("orders", "node-1", &state)
            .await
            .unwrap();
        assert_ne!(new_primary.id, "node-1");
        // Should be the lowest-ID alive node that isn't node-1
        assert_eq!(new_primary.id, "node-2");
    }
}
