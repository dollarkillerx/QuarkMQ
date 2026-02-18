pub mod api_versions;
pub mod create_topics;
pub mod delete_topics;
pub mod fetch;
pub mod list_offsets;
pub mod metadata;
pub mod produce;

use bytes::BytesMut;
use kafka_protocol::messages::{ApiKey, ResponseHeader};
use kafka_protocol::protocol::Encodable;
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::KafkaRequest;
use quarkmq_protocol::ProtocolError;
use tracing::{debug, warn};

pub async fn dispatch_request(
    request: KafkaRequest,
    broker: &Broker,
) -> Result<BytesMut, ProtocolError> {
    let api_key = ApiKey::try_from(request.api_key)
        .map_err(|_| ProtocolError::UnsupportedApiKey(request.api_key))?;

    debug!(
        "Handling {:?} v{} correlation_id={}",
        api_key, request.api_version, request.header.correlation_id
    );

    match api_key {
        ApiKey::ApiVersions => api_versions::handle_api_versions(&request, broker).await,
        ApiKey::Metadata => metadata::handle_metadata(&request, broker).await,
        ApiKey::CreateTopics => create_topics::handle_create_topics(&request, broker).await,
        ApiKey::DeleteTopics => delete_topics::handle_delete_topics(&request, broker).await,
        ApiKey::Produce => produce::handle_produce(&request, broker).await,
        ApiKey::Fetch => fetch::handle_fetch(&request, broker).await,
        ApiKey::ListOffsets => list_offsets::handle_list_offsets(&request, broker).await,
        _ => {
            warn!(
                "Unsupported API {:?} (key={}) from client, returning UNSUPPORTED_VERSION",
                api_key, request.api_key
            );
            build_error_response(request.header.correlation_id, 35) // UNSUPPORTED_VERSION
        }
    }
}

/// Build a minimal error response with just a ResponseHeader and an error_code.
///
/// This is used for APIs that are known to Kafka but not yet implemented.
/// The response format is: ResponseHeader(v0) + error_code(i16).
/// Using header v0 (just correlation_id) is safe for all API versions.
pub fn build_error_response(correlation_id: i32, error_code: i16) -> Result<BytesMut, ProtocolError> {
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;

    let mut out = BytesMut::new();
    header
        .encode(&mut out, 0) // header version 0: just correlation_id
        .map_err(|e| ProtocolError::Decode(format!("error response header encode: {}", e)))?;
    out.extend_from_slice(&error_code.to_be_bytes());

    Ok(out)
}
