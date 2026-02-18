use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Represents the state of a node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Alive,
    Suspect,
    Dead,
}

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub ws_addr: String,
    pub replication_addr: String,
    pub status: NodeStatus,
    pub channels: Vec<String>,
    pub last_heartbeat_ms: u64,
}

/// Configuration for the gossip protocol.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    pub bind_addr: String,
    pub seed_nodes: Vec<String>,
    pub heartbeat_interval_ms: u64,
    pub suspect_timeout_ms: u64,
    pub dead_timeout_ms: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9878".to_string(),
            seed_nodes: Vec::new(),
            heartbeat_interval_ms: 1000,
            suspect_timeout_ms: 5000,
            dead_timeout_ms: 15000,
        }
    }
}

/// Cluster state maintained by the gossip protocol.
/// In a full implementation, this would integrate with chitchat crate.
/// For now, it provides the interface for node discovery and state tracking.
pub struct ClusterState {
    local_node_id: String,
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    config: GossipConfig,
}

impl ClusterState {
    pub fn new(local_node_id: String, config: GossipConfig) -> Self {
        Self {
            local_node_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Register the local node.
    pub async fn register_local(&self, ws_addr: String, replication_addr: String) {
        let info = NodeInfo {
            id: self.local_node_id.clone(),
            ws_addr,
            replication_addr,
            status: NodeStatus::Alive,
            channels: Vec::new(),
            last_heartbeat_ms: current_time_ms(),
        };
        let mut nodes = self.nodes.write().await;
        nodes.insert(self.local_node_id.clone(), info);
    }

    /// Update a node's state (called when gossip messages arrive).
    pub async fn update_node(&self, info: NodeInfo) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(info.id.clone(), info);
    }

    /// Mark a node as dead.
    pub async fn mark_dead(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.status = NodeStatus::Dead;
        }
    }

    /// Get all alive nodes.
    pub async fn alive_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.status == NodeStatus::Alive)
            .cloned()
            .collect()
    }

    /// Get a specific node's info.
    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    /// Get all known nodes.
    pub async fn all_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Check for nodes that have timed out and should be marked suspect/dead.
    pub async fn check_health(&self) -> Vec<String> {
        let now = current_time_ms();
        let mut dead_nodes = Vec::new();
        let mut nodes = self.nodes.write().await;

        for (id, node) in nodes.iter_mut() {
            if *id == self.local_node_id {
                continue;
            }
            let elapsed = now.saturating_sub(node.last_heartbeat_ms);
            if elapsed > self.config.dead_timeout_ms && node.status != NodeStatus::Dead {
                node.status = NodeStatus::Dead;
                dead_nodes.push(id.clone());
                tracing::warn!(node_id = %id, "node marked as dead (no heartbeat for {}ms)", elapsed);
            } else if elapsed > self.config.suspect_timeout_ms
                && node.status == NodeStatus::Alive
            {
                node.status = NodeStatus::Suspect;
                tracing::warn!(node_id = %id, "node marked as suspect");
            }
        }

        dead_nodes
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_get_node() {
        let state = ClusterState::new("node-1".to_string(), GossipConfig::default());
        state
            .register_local("127.0.0.1:9876".to_string(), "127.0.0.1:9879".to_string())
            .await;

        let node = state.get_node("node-1").await.unwrap();
        assert_eq!(node.id, "node-1");
        assert_eq!(node.ws_addr, "127.0.0.1:9876");
        assert_eq!(node.status, NodeStatus::Alive);
    }

    #[tokio::test]
    async fn test_alive_nodes_excludes_dead() {
        let state = ClusterState::new("node-1".to_string(), GossipConfig::default());
        state
            .register_local("127.0.0.1:9876".to_string(), "127.0.0.1:9879".to_string())
            .await;

        state
            .update_node(NodeInfo {
                id: "node-2".to_string(),
                ws_addr: "127.0.0.1:9877".to_string(),
                replication_addr: "127.0.0.1:9880".to_string(),
                status: NodeStatus::Alive,
                channels: vec![],
                last_heartbeat_ms: current_time_ms(),
            })
            .await;

        assert_eq!(state.alive_nodes().await.len(), 2);

        state.mark_dead("node-2").await;
        assert_eq!(state.alive_nodes().await.len(), 1);
    }
}
