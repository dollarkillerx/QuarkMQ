use bytes::BytesMut;
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::delete_topics_response::*;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_delete_topics(
    request: &KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let req = DeleteTopicsRequest::decode(&mut request.body.clone(), request.api_version)
        .map_err(|e| ProtocolError::Decode(format!("delete topics decode: {}", e)))?;

    let mut results = Vec::new();
    for topic_name in &req.topic_names {
        let name_str = topic_name.0.to_string();
        let mut result = DeletableTopicResult::default()
            .with_name(Some(TopicName(StrBytes::from_string(name_str.clone()))));

        match broker.topic_manager.delete_topic(&name_str) {
            Ok(()) => {
                result.error_code = 0;
            }
            Err(quarkmq_broker::BrokerError::TopicNotFound(_)) => {
                result.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
            }
            Err(e) => {
                result.error_code = -1;
                result.error_message = Some(StrBytes::from_string(format!("{}", e)));
            }
        }

        results.push(result);
    }

    let response = DeleteTopicsResponse::default().with_responses(results);

    encode_response(
        ApiKey::DeleteTopics,
        request.api_version,
        request.header.correlation_id,
        &response,
    )
}
