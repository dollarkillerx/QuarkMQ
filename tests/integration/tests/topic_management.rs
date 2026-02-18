use std::sync::Arc;
use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::messages::create_topics_request::*;
use kafka_protocol::messages::metadata_request::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use quarkmq_broker::{Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    std::mem::forget(dir);
    Arc::new(broker)
}

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

async fn read_response(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> BytesMut {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u32::from_be_bytes(len_buf);
    let mut body = BytesMut::zeroed(resp_len as usize);
    reader.read_exact(&mut body).await.unwrap();
    body
}

#[tokio::test]
async fn test_create_and_delete_topics_via_tcp() {
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

    // Create topic "my-topic" with 3 partitions
    let create_req = CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("my-topic")))
            .with_num_partitions(3)
            .with_replication_factor(1)]);

    send_request(&mut writer, ApiKey::CreateTopics, 2, 1, &create_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::CreateTopics.response_header_version(2);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let create_resp = CreateTopicsResponse::decode(&mut resp_buf, 2).unwrap();

    assert_eq!(create_resp.topics.len(), 1);
    assert_eq!(create_resp.topics[0].error_code, 0);
    assert_eq!(create_resp.topics[0].name.0.to_string(), "my-topic");

    // Verify topic exists via metadata
    let metadata_req = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_static_str("my-topic")))),
    ]));

    send_request(&mut writer, ApiKey::Metadata, 1, 2, &metadata_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::Metadata.response_header_version(1);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let metadata_resp = MetadataResponse::decode(&mut resp_buf, 1).unwrap();

    assert_eq!(metadata_resp.topics.len(), 1);
    assert_eq!(metadata_resp.topics[0].error_code, 0);
    assert_eq!(metadata_resp.topics[0].partitions.len(), 3);

    // Verify broker info
    assert_eq!(metadata_resp.brokers.len(), 1);
    assert_eq!(metadata_resp.brokers[0].node_id.0, 0);

    // Create duplicate should fail
    let create_dup_req = CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(vec![CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("my-topic")))
            .with_num_partitions(1)
            .with_replication_factor(1)]);

    send_request(&mut writer, ApiKey::CreateTopics, 2, 3, &create_dup_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::CreateTopics.response_header_version(2);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let create_dup_resp = CreateTopicsResponse::decode(&mut resp_buf, 2).unwrap();
    assert_eq!(create_dup_resp.topics[0].error_code, 36); // TOPIC_ALREADY_EXISTS

    // Delete topic
    let delete_req = DeleteTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topic_names(vec![TopicName(StrBytes::from_static_str("my-topic"))]);

    send_request(&mut writer, ApiKey::DeleteTopics, 1, 4, &delete_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::DeleteTopics.response_header_version(1);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let delete_resp = DeleteTopicsResponse::decode(&mut resp_buf, 1).unwrap();
    assert_eq!(delete_resp.responses.len(), 1);
    assert_eq!(delete_resp.responses[0].error_code, 0);

    // Verify topic is gone via metadata
    let metadata_req2 = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default()
            .with_name(Some(TopicName(StrBytes::from_static_str("my-topic")))),
    ]));

    send_request(&mut writer, ApiKey::Metadata, 1, 5, &metadata_req2).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::Metadata.response_header_version(1);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let metadata_resp2 = MetadataResponse::decode(&mut resp_buf, 1).unwrap();
    assert_eq!(metadata_resp2.topics.len(), 1);
    assert_eq!(metadata_resp2.topics[0].error_code, 3); // UNKNOWN_TOPIC_OR_PARTITION
}

#[tokio::test]
async fn test_metadata_all_topics() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("topic-a", 1).unwrap();
    broker.topic_manager.create_topic("topic-b", 2).unwrap();

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

    // Request all topics (topics = None)
    let metadata_req = MetadataRequest::default().with_topics(None);

    send_request(&mut writer, ApiKey::Metadata, 1, 1, &metadata_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::Metadata.response_header_version(1);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let metadata_resp = MetadataResponse::decode(&mut resp_buf, 1).unwrap();

    assert_eq!(metadata_resp.topics.len(), 2);
    let mut topic_names: Vec<String> = metadata_resp
        .topics
        .iter()
        .map(|t| t.name.as_ref().unwrap().0.to_string())
        .collect();
    topic_names.sort();
    assert_eq!(topic_names, vec!["topic-a", "topic-b"]);
}

#[tokio::test]
async fn test_create_topic_invalid_name() {
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

    // Try to create topics with invalid names
    let create_req = CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(vec![
            // Empty name
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("")))
                .with_num_partitions(1)
                .with_replication_factor(1),
            // Invalid characters
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("bad/topic")))
                .with_num_partitions(1)
                .with_replication_factor(1),
            // Valid name for comparison
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("good-topic")))
                .with_num_partitions(1)
                .with_replication_factor(1),
        ]);

    send_request(&mut writer, ApiKey::CreateTopics, 2, 1, &create_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::CreateTopics.response_header_version(2);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let create_resp = CreateTopicsResponse::decode(&mut resp_buf, 2).unwrap();

    assert_eq!(create_resp.topics.len(), 3);
    // Empty name → INVALID_TOPIC_EXCEPTION (17)
    assert_eq!(create_resp.topics[0].error_code, 17);
    // Invalid chars → INVALID_TOPIC_EXCEPTION (17)
    assert_eq!(create_resp.topics[1].error_code, 17);
    // Valid name → success (0)
    assert_eq!(create_resp.topics[2].error_code, 0);
}

#[tokio::test]
async fn test_metadata_returns_correct_broker_address() {
    let broker = make_test_broker();
    broker.topic_manager.create_topic("addr-test", 1).unwrap();

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

    let metadata_req = MetadataRequest::default().with_topics(None);
    send_request(&mut writer, ApiKey::Metadata, 1, 1, &metadata_req).await;

    let mut resp_buf = read_response(&mut reader).await;
    let resp_header_ver = ApiKey::Metadata.response_header_version(1);
    let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver).unwrap();
    let metadata_resp = MetadataResponse::decode(&mut resp_buf, 1).unwrap();

    // The broker address should be "localhost" (since we bind to 127.0.0.1 which
    // is not a wildcard, but BrokerConfig defaults advertised_host to "localhost")
    assert_eq!(metadata_resp.brokers.len(), 1);
    assert_eq!(metadata_resp.brokers[0].host.to_string(), "localhost");
    // Port defaults to 9092 from BrokerConfig::default() since integration tests
    // construct BrokerConfig with ..Default::default()
    assert_eq!(metadata_resp.brokers[0].port, 9092);
}
