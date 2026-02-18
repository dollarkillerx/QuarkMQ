use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::ApiKey;
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

pub async fn handle_api_versions(
    request: &KafkaRequest,
    _broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let api_versions = vec![
        ApiVersion::default()
            .with_api_key(ApiKey::Produce as i16)
            .with_min_version(3)
            .with_max_version(3),
        ApiVersion::default()
            .with_api_key(ApiKey::Fetch as i16)
            .with_min_version(4)
            .with_max_version(4),
        ApiVersion::default()
            .with_api_key(ApiKey::ListOffsets as i16)
            .with_min_version(1)
            .with_max_version(1),
        ApiVersion::default()
            .with_api_key(ApiKey::Metadata as i16)
            .with_min_version(1)
            .with_max_version(1),
        ApiVersion::default()
            .with_api_key(ApiKey::ApiVersions as i16)
            .with_min_version(0)
            .with_max_version(0),
        ApiVersion::default()
            .with_api_key(ApiKey::CreateTopics as i16)
            .with_min_version(2)
            .with_max_version(2),
        ApiVersion::default()
            .with_api_key(ApiKey::DeleteTopics as i16)
            .with_min_version(1)
            .with_max_version(1),
    ];

    let response = ApiVersionsResponse::default()
        .with_error_code(0)
        .with_api_keys(api_versions);

    encode_response(ApiKey::ApiVersions, request.api_version, request.header.correlation_id, &response)
}
