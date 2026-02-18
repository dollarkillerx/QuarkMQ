use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::ApiKey;
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::{encode_response, KafkaRequest};
use quarkmq_protocol::ProtocolError;

fn build_api_versions_list() -> Vec<ApiVersion> {
    vec![
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
            .with_max_version(3),
        ApiVersion::default()
            .with_api_key(ApiKey::CreateTopics as i16)
            .with_min_version(2)
            .with_max_version(2),
        ApiVersion::default()
            .with_api_key(ApiKey::DeleteTopics as i16)
            .with_min_version(1)
            .with_max_version(1),
    ]
}

pub async fn handle_api_versions(
    request: &KafkaRequest,
    _broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    // ApiVersions is special: we support v0-v3.
    // For unsupported versions, respond with UNSUPPORTED_VERSION using v0
    // encoding so the client can discover which versions we do support.
    let (version, error_code) = if request.api_version <= 3 {
        (request.api_version, 0i16)
    } else {
        (0i16, 35i16) // UNSUPPORTED_VERSION, fall back to v0 encoding
    };

    let response = ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(build_api_versions_list());

    encode_response(ApiKey::ApiVersions, version, request.header.correlation_id, &response)
}
