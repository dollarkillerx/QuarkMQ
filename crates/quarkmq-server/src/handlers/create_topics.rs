use bytes::BytesMut;
use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::create_topics_response::*;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_create_topics(
    request: &KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let req = CreateTopicsRequest::decode(&mut request.body.clone(), request.api_version)
        .map_err(|e| ProtocolError::Decode(format!("create topics decode: {}", e)))?;

    let mut results = Vec::new();
    for topic in &req.topics {
        let topic_name = topic.name.0.to_string();
        let num_partitions = if topic.num_partitions <= 0 {
            broker.config.default_num_partitions
        } else {
            topic.num_partitions
        };

        let mut result = CreatableTopicResult::default()
            .with_name(TopicName(StrBytes::from_string(topic_name.clone())));

        match broker.topic_manager.create_topic(&topic_name, num_partitions) {
            Ok(()) => {
                result.error_code = 0;
                result.error_message = None;
            }
            Err(quarkmq_broker::BrokerError::TopicAlreadyExists(_)) => {
                result.error_code = 36; // TOPIC_ALREADY_EXISTS
                result.error_message =
                    Some(StrBytes::from_string(format!("Topic '{}' already exists", topic_name)));
            }
            Err(e) => {
                result.error_code = -1; // UNKNOWN_SERVER_ERROR
                result.error_message = Some(StrBytes::from_string(format!("{}", e)));
            }
        }

        results.push(result);
    }

    let response = CreateTopicsResponse::default().with_topics(results);

    encode_response(
        ApiKey::CreateTopics,
        request.api_version,
        request.header.correlation_id,
        &response,
    )
}
