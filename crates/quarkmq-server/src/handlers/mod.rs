pub mod api_versions;
pub mod create_topics;
pub mod delete_topics;
pub mod fetch;
pub mod list_offsets;
pub mod metadata;
pub mod produce;

use bytes::BytesMut;
use kafka_protocol::messages::ApiKey;
use quarkmq_broker::Broker;
use quarkmq_protocol::handler::KafkaRequest;
use quarkmq_protocol::ProtocolError;
use tracing::debug;

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
        _ => Err(ProtocolError::UnsupportedApiKey(request.api_key)),
    }
}
