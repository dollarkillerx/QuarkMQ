use bytes::BytesMut;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::produce_response::*;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_produce(
    request: &KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let req = ProduceRequest::decode(&mut request.body.clone(), request.api_version)
        .map_err(|e| ProtocolError::Decode(format!("produce request decode: {}", e)))?;

    let mut topic_responses = Vec::new();

    for topic_data in &req.topic_data {
        let topic_name = topic_data.name.0.to_string();
        let mut partition_responses = Vec::new();

        for partition_data in &topic_data.partition_data {
            let partition_id = partition_data.index;
            let mut part_resp = PartitionProduceResponse::default()
                .with_index(partition_id);

            match &partition_data.records {
                Some(records) => {
                    let mut batch_bytes = records.to_vec();
                    // Parse record count from the batch header
                    let record_count = if batch_bytes.len() >= 61 {
                        i32::from_be_bytes(batch_bytes[57..61].try_into().unwrap())
                    } else {
                        part_resp.error_code = 87; // CORRUPT_MESSAGE
                        partition_responses.push(part_resp);
                        continue;
                    };

                    match broker.handle_produce(&topic_name, partition_id, &mut batch_bytes, record_count) {
                        Ok(base_offset) => {
                            part_resp.base_offset = base_offset;
                            part_resp.error_code = 0;
                            part_resp.log_append_time_ms = -1;
                        }
                        Err(quarkmq_broker::BrokerError::TopicNotFound(_)) => {
                            part_resp.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                            part_resp.base_offset = -1;
                        }
                        Err(quarkmq_broker::BrokerError::PartitionNotFound { .. }) => {
                            part_resp.error_code = 3;
                            part_resp.base_offset = -1;
                        }
                        Err(e) => {
                            tracing::error!("Produce error: {}", e);
                            part_resp.error_code = -1;
                            part_resp.base_offset = -1;
                        }
                    }
                }
                None => {
                    part_resp.error_code = 87; // CORRUPT_MESSAGE
                    part_resp.base_offset = -1;
                }
            }

            partition_responses.push(part_resp);
        }

        let topic_resp = TopicProduceResponse::default()
            .with_name(TopicName(StrBytes::from_string(topic_name)))
            .with_partition_responses(partition_responses);

        topic_responses.push(topic_resp);
    }

    let response = ProduceResponse::default().with_responses(topic_responses);

    encode_response(
        ApiKey::Produce,
        request.api_version,
        request.header.correlation_id,
        &response,
    )
}
