use bytes::BytesMut;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::*;
use kafka_protocol::messages::{ApiKey, BrokerId, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_metadata(
    request: &KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let req = MetadataRequest::decode(&mut request.body.clone(), request.api_version)
        .map_err(|e| ProtocolError::Decode(format!("metadata request decode: {}", e)))?;

    let node_id = BrokerId(broker.config.node_id);

    let broker_meta = MetadataResponseBroker::default()
        .with_node_id(node_id)
        .with_host(StrBytes::from_static_str("localhost"))
        .with_port(9092);

    let requested_topics: Vec<String> = match &req.topics {
        Some(topics) => topics
            .iter()
            .filter_map(|t| t.name.as_ref().map(|n| n.0.to_string()))
            .collect(),
        None => broker.topic_manager.list_topics(),
    };

    let mut topic_responses = Vec::new();
    for topic_name in &requested_topics {
        let mut topic_meta = MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic_name.clone()))));

        if let Some(partition_count) = broker.topic_manager.partition_count(topic_name) {
            topic_meta.error_code = 0;
            let mut partitions = Vec::new();
            for i in 0..partition_count {
                let pm = MetadataResponsePartition::default()
                    .with_partition_index(i)
                    .with_leader_id(node_id)
                    .with_replica_nodes(vec![node_id])
                    .with_isr_nodes(vec![node_id])
                    .with_error_code(0);
                partitions.push(pm);
            }
            topic_meta.partitions = partitions;
        } else {
            topic_meta.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
        }

        topic_responses.push(topic_meta);
    }

    let response = MetadataResponse::default()
        .with_brokers(vec![broker_meta])
        .with_controller_id(node_id)
        .with_topics(topic_responses);

    encode_response(ApiKey::Metadata, request.api_version, request.header.correlation_id, &response)
}
