use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::FetchRequest;
use kafka_protocol::messages::fetch_response::*;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_fetch(
    request: &KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let req = FetchRequest::decode(&mut request.body.clone(), request.api_version)
        .map_err(|e| ProtocolError::Decode(format!("fetch request decode: {}", e)))?;

    let max_bytes = req.max_bytes as usize;
    let mut topic_responses = Vec::new();

    for topic in &req.topics {
        let topic_name = topic.topic.0.to_string();
        let mut partition_responses = Vec::new();

        for partition in &topic.partitions {
            let partition_id = partition.partition;
            let fetch_offset = partition.fetch_offset;
            let partition_max_bytes = max_bytes.min(1024 * 1024); // cap per partition

            let mut part_data = PartitionData::default()
                .with_partition_index(partition_id);

            match broker.handle_fetch(&topic_name, partition_id, fetch_offset, partition_max_bytes) {
                Ok((records, high_watermark, log_start_offset)) => {
                    part_data.error_code = 0;
                    part_data.high_watermark = high_watermark;
                    part_data.log_start_offset = log_start_offset;
                    if records.is_empty() {
                        part_data.records = Some(Bytes::new());
                    } else {
                        part_data.records = Some(Bytes::from(records));
                    }
                }
                Err(quarkmq_broker::BrokerError::TopicNotFound(_)) => {
                    part_data.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }
                Err(quarkmq_broker::BrokerError::PartitionNotFound { .. }) => {
                    part_data.error_code = 3;
                }
                Err(quarkmq_broker::BrokerError::Storage(
                    quarkmq_storage::StorageError::OffsetOutOfRange(_),
                )) => {
                    part_data.error_code = 1; // OFFSET_OUT_OF_RANGE
                }
                Err(e) => {
                    tracing::error!("Fetch error: {}", e);
                    part_data.error_code = -1;
                }
            }

            partition_responses.push(part_data);
        }

        let topic_resp = FetchableTopicResponse::default()
            .with_topic(TopicName(StrBytes::from_string(topic_name)))
            .with_partitions(partition_responses);

        topic_responses.push(topic_resp);
    }

    let response = FetchResponse::default().with_responses(topic_responses);

    encode_response(
        ApiKey::Fetch,
        request.api_version,
        request.header.correlation_id,
        &response,
    )
}
