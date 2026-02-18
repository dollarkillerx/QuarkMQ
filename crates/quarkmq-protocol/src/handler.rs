use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};

use crate::error::{ProtocolError, Result};

/// A parsed Kafka request containing the decoded header and remaining body bytes.
#[derive(Debug)]
pub struct KafkaRequest {
    /// The fully decoded request header.
    pub header: RequestHeader,
    /// The raw API key value from the header.
    pub api_key: i16,
    /// The API version from the header.
    pub api_version: i16,
    /// The remaining bytes after the header has been consumed (the request body).
    pub body: BytesMut,
}

/// Parse a request frame body into a [`KafkaRequest`].
///
/// The frame is expected to be the raw body bytes (without the 4-byte length
/// prefix). This function peeks at the first 4 bytes to extract the API key
/// and version, determines the correct header version, decodes the header,
/// and returns the remaining bytes as the body.
pub fn parse_request(mut frame: BytesMut) -> Result<KafkaRequest> {
    if frame.len() < 4 {
        return Err(ProtocolError::Decode("frame too short".into()));
    }

    // Peek at api_key (first 2 bytes) and api_version (next 2 bytes)
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);

    // Determine header version from ApiKey
    let api_key_enum = ApiKey::try_from(api_key)
        .map_err(|_| ProtocolError::UnsupportedApiKey(api_key))?;
    let header_version = api_key_enum.request_header_version(api_version);

    // Decode the full request header
    let header = RequestHeader::decode(&mut frame, header_version)
        .map_err(|e| ProtocolError::Decode(format!("header decode: {}", e)))?;

    Ok(KafkaRequest {
        header,
        api_key,
        api_version,
        body: frame, // remaining bytes after header
    })
}

/// Encode a Kafka response into a frame body.
///
/// This serializes the [`ResponseHeader`] (with the given correlation ID)
/// followed by the response body. The returned bytes do **not** include the
/// 4-byte length prefix; the caller is responsible for framing.
pub fn encode_response<R: Encodable>(
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    response: &R,
) -> Result<BytesMut> {
    let resp_header_version = api_key.response_header_version(api_version);

    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;

    let mut out = BytesMut::new();
    header
        .encode(&mut out, resp_header_version)
        .map_err(|e| ProtocolError::Decode(format!("header encode: {}", e)))?;
    response
        .encode(&mut out, api_version)
        .map_err(|e| ProtocolError::Decode(format!("body encode: {}", e)))?;

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
    use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
    use kafka_protocol::messages::metadata_request::MetadataRequest;
    use kafka_protocol::protocol::Encodable;

    /// Helper: build a wire-format request frame (header + body) for the given
    /// API key, version, correlation ID, and body encoder.
    fn build_request_frame<B: Encodable>(
        api_key: ApiKey,
        api_version: i16,
        correlation_id: i32,
        body: &B,
    ) -> BytesMut {
        let header_version = api_key.request_header_version(api_version);

        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;

        let mut buf = BytesMut::new();
        header.encode(&mut buf, header_version).unwrap();
        body.encode(&mut buf, api_version).unwrap();
        buf
    }

    #[test]
    fn test_parse_request_api_versions() {
        let req_body = ApiVersionsRequest::default();
        let buf = build_request_frame(ApiKey::ApiVersions, 0, 42, &req_body);

        let parsed = parse_request(buf).unwrap();
        assert_eq!(parsed.api_key, 18); // ApiVersions = 18
        assert_eq!(parsed.api_version, 0);
        assert_eq!(parsed.header.correlation_id, 42);
    }

    #[test]
    fn test_parse_request_metadata() {
        let req_body = MetadataRequest::default();
        let buf = build_request_frame(ApiKey::Metadata, 0, 99, &req_body);

        let parsed = parse_request(buf).unwrap();
        assert_eq!(parsed.api_key, 3); // Metadata = 3
        assert_eq!(parsed.api_version, 0);
        assert_eq!(parsed.header.correlation_id, 99);
    }

    #[test]
    fn test_parse_request_frame_too_short() {
        let buf = BytesMut::from(&[0u8, 1][..]);
        let err = parse_request(buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Decode(_)));
    }

    #[test]
    fn test_parse_request_unsupported_api_key() {
        // Construct a frame with an invalid API key (999)
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&999i16.to_be_bytes());
        buf.extend_from_slice(&0i16.to_be_bytes());
        buf.extend_from_slice(&[0u8; 20]); // padding

        let err = parse_request(buf).unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedApiKey(999)));
    }

    #[test]
    fn test_encode_response_api_versions() {
        let mut response = ApiVersionsResponse::default();
        response.error_code = 0;

        let encoded =
            encode_response(ApiKey::ApiVersions, 0, 42, &response).unwrap();

        // The encoded bytes should start with the ResponseHeader containing
        // correlation_id = 42. Verify by decoding.
        let resp_header_version =
            ApiKey::ApiVersions.response_header_version(0);
        let mut buf = encoded.clone();
        let decoded_header =
            ResponseHeader::decode(&mut buf, resp_header_version).unwrap();
        assert_eq!(decoded_header.correlation_id, 42);

        // Decode the response body
        let decoded_body =
            ApiVersionsResponse::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded_body.error_code, 0);
    }

    #[test]
    fn test_roundtrip_request_parse() {
        // Encode an ApiVersions request, parse it, verify all fields match.
        let req_body = ApiVersionsRequest::default();
        let buf = build_request_frame(ApiKey::ApiVersions, 0, 123, &req_body);

        let parsed = parse_request(buf).unwrap();
        assert_eq!(parsed.api_key, 18);
        assert_eq!(parsed.api_version, 0);
        assert_eq!(parsed.header.correlation_id, 123);
        assert_eq!(parsed.header.request_api_key, 18);
        assert_eq!(parsed.header.request_api_version, 0);
    }

    #[test]
    fn test_encode_response_different_correlation_ids() {
        let response = ApiVersionsResponse::default();

        for cid in [0, 1, -1, i32::MAX, i32::MIN] {
            let encoded =
                encode_response(ApiKey::ApiVersions, 0, cid, &response)
                    .unwrap();

            let resp_header_version =
                ApiKey::ApiVersions.response_header_version(0);
            let mut buf = encoded;
            let decoded_header =
                ResponseHeader::decode(&mut buf, resp_header_version).unwrap();
            assert_eq!(decoded_header.correlation_id, cid);
        }
    }

    #[test]
    fn test_full_roundtrip_request_response() {
        // 1. Build a request
        let req_body = ApiVersionsRequest::default();
        let req_buf = build_request_frame(ApiKey::ApiVersions, 0, 77, &req_body);

        // 2. Parse the request
        let parsed = parse_request(req_buf).unwrap();
        assert_eq!(parsed.header.correlation_id, 77);

        // 3. Encode a response using the parsed correlation ID
        let mut resp = ApiVersionsResponse::default();
        resp.error_code = 0;
        let resp_buf = encode_response(
            ApiKey::ApiVersions,
            parsed.api_version,
            parsed.header.correlation_id,
            &resp,
        )
        .unwrap();

        // 4. Decode the response and verify
        let resp_header_version =
            ApiKey::ApiVersions.response_header_version(0);
        let mut buf = resp_buf;
        let decoded_header =
            ResponseHeader::decode(&mut buf, resp_header_version).unwrap();
        assert_eq!(decoded_header.correlation_id, 77);

        let decoded_resp = ApiVersionsResponse::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded_resp.error_code, 0);
    }
}
