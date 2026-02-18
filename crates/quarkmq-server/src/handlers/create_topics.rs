use bytes::BytesMut;
use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::create_topics_response::*;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

/// Validate a Kafka topic name.
/// Rules: non-empty, <= 249 chars, only [a-zA-Z0-9._-].
fn validate_topic_name(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("Topic name must not be empty".into());
    }
    if name.len() > 249 {
        return Some(format!(
            "Topic name is too long ({} > 249 characters)",
            name.len()
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Some(format!(
            "Topic name '{}' contains invalid characters (allowed: [a-zA-Z0-9._-])",
            name
        ));
    }
    None
}

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

        if let Some(err_msg) = validate_topic_name(&topic_name) {
            result.error_code = 17; // INVALID_TOPIC_EXCEPTION
            result.error_message = Some(StrBytes::from_string(err_msg));
            results.push(result);
            continue;
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_topic_name_valid() {
        assert!(validate_topic_name("my-topic").is_none());
        assert!(validate_topic_name("my.topic").is_none());
        assert!(validate_topic_name("my_topic").is_none());
        assert!(validate_topic_name("MyTopic123").is_none());
        assert!(validate_topic_name("a").is_none());
        assert!(validate_topic_name("test-topic.v2_final").is_none());
    }

    #[test]
    fn test_validate_topic_name_empty() {
        assert!(validate_topic_name("").is_some());
    }

    #[test]
    fn test_validate_topic_name_too_long() {
        let long_name = "a".repeat(250);
        assert!(validate_topic_name(&long_name).is_some());
        // Exactly 249 is OK
        let ok_name = "a".repeat(249);
        assert!(validate_topic_name(&ok_name).is_none());
    }

    #[test]
    fn test_validate_topic_name_invalid_chars() {
        assert!(validate_topic_name("my topic").is_some());
        assert!(validate_topic_name("my/topic").is_some());
        assert!(validate_topic_name("my@topic").is_some());
        assert!(validate_topic_name("my#topic").is_some());
        assert!(validate_topic_name("topic!").is_some());
    }
}
