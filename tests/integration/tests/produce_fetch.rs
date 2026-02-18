use std::sync::Arc;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::messages::produce_request::*;
use kafka_protocol::messages::fetch_request::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use quarkmq_broker::{Broker, BrokerConfig};
use tokio::net::TcpStream;

/// Create a minimal valid RecordBatch v2 with a single record
fn make_record_batch(key: &[u8], value: &[u8]) -> Vec<u8> {
    // Build a RecordBatch v2 (magic=2)
    // Record: varlen key + varlen value
    let key_len = key.len() as i32;
    let value_len = value.len() as i32;

    // Record body: attributes(1) + timestampDelta(varint) + offsetDelta(varint) +
    //              keyLength(varint) + key + valueLength(varint) + value + headersCount(varint)
    let mut record = Vec::new();
    record.push(0u8); // attributes
    record.push(0u8); // timestampDelta = 0 (varint)
    record.push(0u8); // offsetDelta = 0 (varint)
    // key length as varint
    encode_varint(&mut record, key_len);
    record.extend_from_slice(key);
    // value length as varint
    encode_varint(&mut record, value_len);
    record.extend_from_slice(value);
    record.push(0u8); // headers count = 0

    let record_size = record.len();
    let mut record_with_len = Vec::new();
    encode_varint(&mut record_with_len, record_size as i32);
    record_with_len.extend_from_slice(&record);

    let records_section = record_with_len;

    // RecordBatch header (61 bytes) + records
    let batch_length = (49 + records_section.len()) as i32; // 61 - 12 = 49 header + records

    let mut batch = Vec::new();
    // baseOffset (8 bytes) - will be overwritten by storage
    batch.extend_from_slice(&0i64.to_be_bytes());
    // batchLength (4 bytes)
    batch.extend_from_slice(&batch_length.to_be_bytes());
    // partitionLeaderEpoch (4 bytes)
    batch.extend_from_slice(&0i32.to_be_bytes());
    // magic (1 byte)
    batch.push(2u8);
    // crc (4 bytes) - we'll set to 0 since we don't validate CRC
    batch.extend_from_slice(&0u32.to_be_bytes());
    // attributes (2 bytes)
    batch.extend_from_slice(&0i16.to_be_bytes());
    // lastOffsetDelta (4 bytes)
    batch.extend_from_slice(&0i32.to_be_bytes());
    // baseTimestamp (8 bytes)
    batch.extend_from_slice(&1000i64.to_be_bytes());
    // maxTimestamp (8 bytes)
    batch.extend_from_slice(&1000i64.to_be_bytes());
    // producerId (8 bytes)
    batch.extend_from_slice(&(-1i64).to_be_bytes());
    // producerEpoch (2 bytes)
    batch.extend_from_slice(&(-1i16).to_be_bytes());
    // baseSequence (4 bytes)
    batch.extend_from_slice(&(-1i32).to_be_bytes());
    // recordCount (4 bytes)
    batch.extend_from_slice(&1i32.to_be_bytes());
    // records
    batch.extend_from_slice(&records_section);

    batch
}

fn encode_varint(buf: &mut Vec<u8>, value: i32) {
    let mut v = ((value << 1) ^ (value >> 31)) as u32;
    loop {
        if (v & !0x7F) == 0 {
            buf.push(v as u8);
            break;
        }
        buf.push((v & 0x7F | 0x80) as u8);
        v >>= 7;
    }
}

fn make_test_broker() -> Arc<Broker> {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig {
        node_id: 0,
        data_dir: dir.path().to_string_lossy().to_string(),
        segment_bytes: 1024 * 1024,
        index_interval_bytes: 256,
        default_num_partitions: 1,
        ..Default::default()
    };
    let broker = Broker::new(config);
    broker.start().unwrap();
    // Keep tempdir alive by leaking it (tests are short-lived)
    std::mem::forget(dir);
    Arc::new(broker)
}

#[tokio::test]
async fn test_produce_and_fetch_roundtrip() {
    let broker = make_test_broker();

    // Create topic
    broker.topic_manager.create_topic("test", 1).unwrap();

    // Produce a message
    let batch = make_record_batch(b"key1", b"hello world");
    let mut batch_bytes = batch.clone();
    let base_offset = broker
        .handle_produce("test", 0, &mut batch_bytes, 1)
        .unwrap();
    assert_eq!(base_offset, 0);

    // Fetch the message back
    let (data, hw, log_start) = broker.handle_fetch("test", 0, 0, 1024 * 1024).unwrap();
    assert!(!data.is_empty());
    assert_eq!(hw, 1);
    assert_eq!(log_start, 0);

    // Verify the base_offset was rewritten in the fetched data
    let fetched_base = i64::from_be_bytes(data[0..8].try_into().unwrap());
    assert_eq!(fetched_base, 0);
}

#[tokio::test]
async fn test_produce_multiple_messages() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("multi", 1).unwrap();

    // Produce 3 messages
    for i in 0..3 {
        let batch = make_record_batch(
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        );
        let mut batch_bytes = batch;
        let base_offset = broker
            .handle_produce("multi", 0, &mut batch_bytes, 1)
            .unwrap();
        assert_eq!(base_offset, i as i64);
    }

    // Fetch all messages from offset 0
    let (data, hw, _) = broker.handle_fetch("multi", 0, 0, 1024 * 1024).unwrap();
    assert!(!data.is_empty());
    assert_eq!(hw, 3);

    // Fetch from offset 1 should skip first message
    let (data2, hw2, _) = broker.handle_fetch("multi", 0, 1, 1024 * 1024).unwrap();
    assert!(!data2.is_empty());
    assert_eq!(hw2, 3);
    // Should be smaller than fetching all
    assert!(data2.len() <= data.len());
}

#[tokio::test]
async fn test_list_offsets() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("offsets", 1).unwrap();

    // Empty topic
    let earliest = broker.handle_list_offsets("offsets", 0, -2).unwrap();
    let latest = broker.handle_list_offsets("offsets", 0, -1).unwrap();
    assert_eq!(earliest, 0);
    assert_eq!(latest, 0);

    // Produce some messages
    for _ in 0..5 {
        let batch = make_record_batch(b"k", b"v");
        let mut batch_bytes = batch;
        broker
            .handle_produce("offsets", 0, &mut batch_bytes, 1)
            .unwrap();
    }

    let earliest = broker.handle_list_offsets("offsets", 0, -2).unwrap();
    let latest = broker.handle_list_offsets("offsets", 0, -1).unwrap();
    assert_eq!(earliest, 0);
    assert_eq!(latest, 5);
}

#[tokio::test]
async fn test_produce_to_multiple_partitions() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("partitioned", 3).unwrap();

    // Produce to partition 0
    let mut batch = make_record_batch(b"k0", b"v0");
    let offset0 = broker
        .handle_produce("partitioned", 0, &mut batch, 1)
        .unwrap();
    assert_eq!(offset0, 0);

    // Produce to partition 1
    let mut batch = make_record_batch(b"k1", b"v1");
    let offset1 = broker
        .handle_produce("partitioned", 1, &mut batch, 1)
        .unwrap();
    assert_eq!(offset1, 0); // Independent offset per partition

    // Produce more to partition 0
    let mut batch = make_record_batch(b"k0b", b"v0b");
    let offset0b = broker
        .handle_produce("partitioned", 0, &mut batch, 1)
        .unwrap();
    assert_eq!(offset0b, 1);

    // Fetch from each partition
    let (data0, _, _) = broker
        .handle_fetch("partitioned", 0, 0, 1024 * 1024)
        .unwrap();
    let (data1, _, _) = broker
        .handle_fetch("partitioned", 1, 0, 1024 * 1024)
        .unwrap();
    let (data2, _, _) = broker
        .handle_fetch("partitioned", 2, 0, 1024 * 1024)
        .unwrap();
    assert!(!data0.is_empty());
    assert!(!data1.is_empty());
    assert!(data2.is_empty()); // No messages produced to partition 2
}

#[tokio::test]
async fn test_end_to_end_via_tcp() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("e2e", 1).unwrap();

    // Start server
    let server = quarkmq_server::server::Server::bind("127.0.0.1:0", broker.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Give server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Connect as client
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.split();

    // Send ApiVersions request
    let mut req_buf = BytesMut::new();
    let mut header = RequestHeader::default();
    header.request_api_key = ApiKey::ApiVersions as i16;
    header.request_api_version = 0;
    header.correlation_id = 1;
    let header_ver = ApiKey::ApiVersions.request_header_version(0);
    header.encode(&mut req_buf, header_ver).unwrap();
    ApiVersionsRequest::default().encode(&mut req_buf, 0).unwrap();

    // Write frame
    let len = req_buf.len() as u32;
    use tokio::io::AsyncWriteExt;
    writer.write_all(&len.to_be_bytes()).await.unwrap();
    writer.write_all(&req_buf).await.unwrap();
    writer.flush().await.unwrap();

    // Read response frame
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(len_buf);
    let mut resp_body = vec![0u8; resp_len as usize];
    reader.read_exact(&mut resp_body).await.unwrap();

    // Decode response header
    let mut resp_buf = BytesMut::from(&resp_body[..]);
    let resp_header_ver = ApiKey::ApiVersions.response_header_version(0);
    let resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    assert_eq!(resp_header.correlation_id, 1);

    // Decode ApiVersionsResponse
    let resp = ApiVersionsResponse::decode(&mut resp_buf, 0).unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(!resp.api_keys.is_empty());

    // Verify we support the expected API keys
    let supported_keys: Vec<i16> = resp.api_keys.iter().map(|k| k.api_key).collect();
    assert!(supported_keys.contains(&0));  // Produce
    assert!(supported_keys.contains(&1));  // Fetch
    assert!(supported_keys.contains(&2));  // ListOffsets
    assert!(supported_keys.contains(&3));  // Metadata
    assert!(supported_keys.contains(&18)); // ApiVersions
    assert!(supported_keys.contains(&19)); // CreateTopics
    assert!(supported_keys.contains(&20)); // DeleteTopics
}

#[tokio::test]
async fn test_produce_fetch_via_tcp() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("tcp-test", 1).unwrap();

    let server = quarkmq_server::server::Server::bind("127.0.0.1:0", broker.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.split();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Helper to send a request frame
    async fn send_request(
        writer: &mut (impl tokio::io::AsyncWrite + Unpin),
        api_key: ApiKey,
        api_version: i16,
        correlation_id: i32,
        request: &(impl Encodable + ?Sized),
    ) {
        let mut buf = BytesMut::new();
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        let header_ver = api_key.request_header_version(api_version);
        header.encode(&mut buf, header_ver).unwrap();
        request.encode(&mut buf, api_version).unwrap();

        let len = buf.len() as u32;
        writer.write_all(&len.to_be_bytes()).await.unwrap();
        writer.write_all(&buf).await.unwrap();
        writer.flush().await.unwrap();
    }

    async fn read_response(
        reader: &mut (impl tokio::io::AsyncRead + Unpin),
    ) -> BytesMut {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await.unwrap();
        let resp_len = u32::from_be_bytes(len_buf);
        let mut body = BytesMut::zeroed(resp_len as usize);
        reader.read_exact(&mut body).await.unwrap();
        body
    }

    // Produce a message
    let batch = make_record_batch(b"tcp-key", b"tcp-value");
    let produce_req = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_static_str("tcp-test")))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(Some(Bytes::from(batch)))])]);

    send_request(&mut writer, ApiKey::Produce, 3, 2, &produce_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::Produce.response_header_version(3);
    let resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    assert_eq!(resp_header.correlation_id, 2);

    let produce_resp = ProduceResponse::decode(&mut resp_buf, 3).unwrap();
    assert_eq!(produce_resp.responses.len(), 1);
    assert_eq!(produce_resp.responses[0].partition_responses.len(), 1);
    assert_eq!(produce_resp.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(produce_resp.responses[0].partition_responses[0].base_offset, 0);

    // Fetch the message back
    let fetch_req = FetchRequest::default()
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![FetchTopic::default()
            .with_topic(TopicName(StrBytes::from_static_str("tcp-test")))
            .with_partitions(vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(0)])]);

    send_request(&mut writer, ApiKey::Fetch, 4, 3, &fetch_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::Fetch.response_header_version(4);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();

    let fetch_resp = FetchResponse::decode(&mut resp_buf, 4).unwrap();
    assert_eq!(fetch_resp.responses.len(), 1);
    assert_eq!(fetch_resp.responses[0].partitions.len(), 1);
    let partition_data = &fetch_resp.responses[0].partitions[0];
    assert_eq!(partition_data.error_code, 0);
    assert_eq!(partition_data.high_watermark, 1);
    assert!(partition_data.records.is_some());
    assert!(!partition_data.records.as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn test_unsupported_api_keeps_connection_alive() {
    let broker = make_test_broker();

    let server = quarkmq_server::server::Server::bind("127.0.0.1:0", broker.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.split();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Send a FindCoordinator request (api_key=10), which is known but not implemented.
    // The server should return an error response but keep the connection alive.
    let mut req_buf = BytesMut::new();
    let mut header = RequestHeader::default();
    header.request_api_key = 10; // FindCoordinator
    header.request_api_version = 0;
    header.correlation_id = 100;
    // FindCoordinator uses header version based on api version
    let header_ver = ApiKey::FindCoordinator.request_header_version(0);
    header.encode(&mut req_buf, header_ver).unwrap();
    // Minimal body: coordinator_key as empty string (2 bytes length + 0 bytes)
    req_buf.extend_from_slice(&0i16.to_be_bytes());

    let len = req_buf.len() as u32;
    writer.write_all(&len.to_be_bytes()).await.unwrap();
    writer.write_all(&req_buf).await.unwrap();
    writer.flush().await.unwrap();

    // Read the error response
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(len_buf);
    let mut resp_body = vec![0u8; resp_len as usize];
    reader.read_exact(&mut resp_body).await.unwrap();

    // Verify correlation_id is preserved
    let mut resp_buf = BytesMut::from(&resp_body[..]);
    let resp_header = ResponseHeader::decode(&mut resp_buf, 0).unwrap();
    assert_eq!(resp_header.correlation_id, 100);

    // Now send a valid ApiVersions request to prove the connection is still alive
    let mut req_buf2 = BytesMut::new();
    let mut header2 = RequestHeader::default();
    header2.request_api_key = ApiKey::ApiVersions as i16;
    header2.request_api_version = 0;
    header2.correlation_id = 101;
    let header_ver2 = ApiKey::ApiVersions.request_header_version(0);
    header2.encode(&mut req_buf2, header_ver2).unwrap();
    ApiVersionsRequest::default().encode(&mut req_buf2, 0).unwrap();

    let len2 = req_buf2.len() as u32;
    writer.write_all(&len2.to_be_bytes()).await.unwrap();
    writer.write_all(&req_buf2).await.unwrap();
    writer.flush().await.unwrap();

    // Read the ApiVersions response
    reader.read_exact(&mut len_buf).await.unwrap();
    let resp_len2 = u32::from_be_bytes(len_buf);
    let mut resp_body2 = vec![0u8; resp_len2 as usize];
    reader.read_exact(&mut resp_body2).await.unwrap();

    let mut resp_buf2 = BytesMut::from(&resp_body2[..]);
    let resp_header_ver = ApiKey::ApiVersions.response_header_version(0);
    let resp_header2 = ResponseHeader::decode(&mut resp_buf2, resp_header_ver).unwrap();
    assert_eq!(resp_header2.correlation_id, 101);

    let resp2 = ApiVersionsResponse::decode(&mut resp_buf2, 0).unwrap();
    assert_eq!(resp2.error_code, 0);
    assert!(!resp2.api_keys.is_empty());
}

#[tokio::test]
async fn test_api_versions_v3() {
    let broker = make_test_broker();

    let server = quarkmq_server::server::Server::bind("127.0.0.1:0", broker.clone())
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.split();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Send ApiVersions v3 request (what modern Kafka clients send first)
    let mut req_buf = BytesMut::new();
    let mut header = RequestHeader::default();
    header.request_api_key = ApiKey::ApiVersions as i16;
    header.request_api_version = 3;
    header.correlation_id = 1;
    let header_ver = ApiKey::ApiVersions.request_header_version(3);
    header.encode(&mut req_buf, header_ver).unwrap();
    ApiVersionsRequest::default().encode(&mut req_buf, 3).unwrap();

    let len = req_buf.len() as u32;
    writer.write_all(&len.to_be_bytes()).await.unwrap();
    writer.write_all(&req_buf).await.unwrap();
    writer.flush().await.unwrap();

    // Read response
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(len_buf);
    let mut resp_body = vec![0u8; resp_len as usize];
    reader.read_exact(&mut resp_body).await.unwrap();

    // Decode v3 response
    let mut resp_buf = BytesMut::from(&resp_body[..]);
    let resp_header_ver = ApiKey::ApiVersions.response_header_version(3);
    let resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    assert_eq!(resp_header.correlation_id, 1);

    let resp = ApiVersionsResponse::decode(&mut resp_buf, 3).unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(!resp.api_keys.is_empty());

    // Verify ApiVersions reports v0-v3 support
    let api_ver_entry = resp
        .api_keys
        .iter()
        .find(|k| k.api_key == ApiKey::ApiVersions as i16)
        .expect("ApiVersions entry should exist");
    assert_eq!(api_ver_entry.min_version, 0);
    assert_eq!(api_ver_entry.max_version, 3);
}
