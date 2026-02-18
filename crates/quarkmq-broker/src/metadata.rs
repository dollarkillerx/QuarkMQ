use kafka_protocol::messages::metadata_response::*;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::StrBytes;

use crate::config::BrokerConfig;
use crate::topic_manager::TopicManager;

/// Builds Kafka MetadataResponse messages for the cluster.
///
/// In single-node mode, the cluster consists of one broker which is both
/// the controller and the leader of all partitions.
pub struct ClusterMetadata {
    config: BrokerConfig,
}

impl ClusterMetadata {
    pub fn new(config: BrokerConfig) -> Self {
        Self { config }
    }

    /// Build a MetadataResponse for the given topics, or all topics if `None`.
    pub fn build_response(
        &self,
        topic_manager: &TopicManager,
        requested_topics: Option<&[String]>,
    ) -> MetadataResponse {
        let mut response = MetadataResponse::default();

        // Add broker info (single node).
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(self.config.node_id);
        broker.host = StrBytes::from_static_str("localhost");
        broker.port = 9092;
        response.brokers = vec![broker];

        // Controller id.
        response.controller_id = BrokerId(self.config.node_id);

        // Determine which topics to include.
        let topics_to_list = match requested_topics {
            Some(names) => names.to_vec(),
            None => topic_manager.list_topics(),
        };

        // Build topic metadata.
        let mut topic_responses = Vec::new();
        for topic_name in &topics_to_list {
            let mut topic_metadata = MetadataResponseTopic::default();
            topic_metadata.name = Some(TopicName::from(
                StrBytes::from_string(topic_name.clone()),
            ));

            if let Some(partition_count) = topic_manager.partition_count(topic_name) {
                topic_metadata.error_code = 0; // No error
                let mut partition_metas = Vec::new();
                for i in 0..partition_count {
                    let mut pm = MetadataResponsePartition::default();
                    pm.partition_index = i;
                    pm.leader_id = BrokerId(self.config.node_id);
                    pm.replica_nodes = vec![BrokerId(self.config.node_id)];
                    pm.isr_nodes = vec![BrokerId(self.config.node_id)];
                    pm.error_code = 0;
                    partition_metas.push(pm);
                }
                topic_metadata.partitions = partition_metas;
            } else {
                topic_metadata.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                topic_metadata.partitions = vec![];
            }

            topic_responses.push(topic_metadata);
        }

        response.topics = topic_responses;
        response
    }
}
