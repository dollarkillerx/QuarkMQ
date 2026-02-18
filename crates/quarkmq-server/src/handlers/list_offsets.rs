use bytes::BytesMut;
use kafka_protocol::messages::list_offsets_request::ListOffsetsRequest;
use kafka_protocol::messages::list_offsets_response::*;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_list_offsets(
    request: &KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let req = ListOffsetsRequest::decode(&mut request.body.clone(), request.api_version)
        .map_err(|e| ProtocolError::Decode(format!("list offsets decode: {}", e)))?;

    let mut topic_responses = Vec::new();

    for topic in &req.topics {
        let topic_name = topic.name.0.to_string();
        let mut partition_responses = Vec::new();

        for partition in &topic.partitions {
            let partition_id = partition.partition_index;
            let timestamp = partition.timestamp;

            let mut part_resp = ListOffsetsPartitionResponse::default()
                .with_partition_index(partition_id);

            match broker.handle_list_offsets(&topic_name, partition_id, timestamp) {
                Ok(offset) => {
                    part_resp.error_code = 0;
                    part_resp.offset = offset;
                    part_resp.timestamp = timestamp;
                }
                Err(quarkmq_broker::BrokerError::TopicNotFound(_)) => {
                    part_resp.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    part_resp.offset = -1;
                }
                Err(quarkmq_broker::BrokerError::PartitionNotFound { .. }) => {
                    part_resp.error_code = 3;
                    part_resp.offset = -1;
                }
                Err(e) => {
                    tracing::error!("ListOffsets error: {}", e);
                    part_resp.error_code = -1;
                    part_resp.offset = -1;
                }
            }

            partition_responses.push(part_resp);
        }

        let topic_resp = ListOffsetsTopicResponse::default()
            .with_name(TopicName(StrBytes::from_string(topic_name)))
            .with_partitions(partition_responses);

        topic_responses.push(topic_resp);
    }

    let response = ListOffsetsResponse::default().with_topics(topic_responses);

    encode_response(
        ApiKey::ListOffsets,
        request.api_version,
        request.header.correlation_id,
        &response,
    )
}
