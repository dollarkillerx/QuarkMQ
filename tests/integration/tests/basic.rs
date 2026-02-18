use std::net::TcpListener;
use std::time::Duration;

use quarkmq_client::QuarkMQClient;
use quarkmq_protocol::rpc::MessagePush;
use quarkmq_server::config::Config;
use quarkmq_server::server::Server;
use tokio::time::timeout;

/// Find a free port by binding to port 0 and extracting the assigned port.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Start a real QuarkMQ server on a random available port with a temp data dir.
/// Returns the port, shutdown sender, and tempdir handle (must be kept alive).
async fn start_test_server() -> (u16, tokio::sync::broadcast::Sender<()>, tempfile::TempDir) {
    let port = free_port();
    let tmp_dir = tempfile::tempdir().expect("failed to create temp dir");

    let mut config = Config::default();
    config.server.ws_bind = format!("127.0.0.1:{}", port);
    config.channels.ack_timeout_secs = 30;
    config.node.data_dir = tmp_dir.path().to_string_lossy().to_string();

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    let shutdown = shutdown_tx.clone();

    let server = Server::new(config);
    tokio::spawn(async move {
        server.run(shutdown).await.unwrap();
    });

    // Give the server time to bind the listener
    tokio::time::sleep(Duration::from_millis(200)).await;

    (port, shutdown_tx, tmp_dir)
}

/// Start a server on the given port using an existing data dir (for recovery tests).
/// Returns the shutdown sender and the join handle.
async fn start_server_with_config(
    port: u16,
    data_dir: &str,
) -> tokio::sync::broadcast::Sender<()> {
    let mut config = Config::default();
    config.server.ws_bind = format!("127.0.0.1:{}", port);
    config.channels.ack_timeout_secs = 30;
    config.node.data_dir = data_dir.to_string();

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
    let shutdown = shutdown_tx.clone();

    let server = Server::new(config);
    tokio::spawn(async move {
        server.run(shutdown).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    shutdown_tx
}

/// Connect a new QuarkMQClient to the test server.
async fn connect_client(port: u16) -> QuarkMQClient {
    let url = format!("ws://127.0.0.1:{}", port);
    QuarkMQClient::connect(&url)
        .await
        .expect("client should connect to test server")
}

/// Receive up to `count` messages from a client, with a per-message timeout.
async fn recv_messages(
    client: &mut QuarkMQClient,
    count: usize,
) -> Vec<MessagePush> {
    let mut received = Vec::new();
    for _ in 0..count {
        match timeout(Duration::from_secs(5), client.recv_message()).await {
            Ok(Some(msg)) => received.push(msg),
            _ => break,
        }
    }
    received
}

// ---------------------------------------------------------------------------
// Test 1: Create a channel and verify it appears in list_channels
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_create_channel_and_list() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;
    let client = connect_client(port).await;

    // Create a channel
    let name = client
        .create_channel("orders", None, None)
        .await
        .expect("create_channel should succeed");
    assert_eq!(name, "orders");

    // List channels and verify it is present
    let result = client
        .list_channels()
        .await
        .expect("list_channels should succeed");
    let names: Vec<&str> = result.channels.iter().map(|c| c.name.as_str()).collect();
    assert!(
        names.contains(&"orders"),
        "channel list should contain 'orders', got: {:?}",
        names
    );

    // Verify it reports correct metadata
    let ch = result.channels.iter().find(|c| c.name == "orders").unwrap();
    assert_eq!(ch.pending_count, 0);

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 2: Publish / Subscribe / ACK end-to-end
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_publish_subscribe_ack() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;

    // Publisher client
    let publisher = connect_client(port).await;

    // Subscriber client (needs &mut for recv_message)
    let mut subscriber = connect_client(port).await;

    // Create channel
    publisher
        .create_channel("events", None, None)
        .await
        .expect("create_channel should succeed");

    // Subscribe before publishing so the topic exists for fan-out
    subscriber
        .subscribe("events", "all")
        .await
        .expect("subscribe should succeed");

    // Publish a message
    let payload = serde_json::json!({"event": "user.signup", "user_id": 42});
    let msg_id = publisher
        .publish("events", payload.clone())
        .await
        .expect("publish should succeed");

    // Receive the message on the subscriber
    let push = timeout(Duration::from_secs(5), subscriber.recv_message())
        .await
        .expect("should receive message within timeout")
        .expect("should get a message push");

    assert_eq!(push.message_id, msg_id);
    assert_eq!(push.channel, "events");
    assert_eq!(push.payload, payload);
    assert_eq!(push.attempt, 1);

    // ACK the message
    subscriber
        .ack(push.message_id)
        .await
        .expect("ack should succeed");

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 3: Competing consumers (round-robin within the same topic)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_competing_consumers() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;

    let admin = connect_client(port).await;
    admin
        .create_channel("work", None, None)
        .await
        .expect("create_channel should succeed");

    // Three competing consumers on the same topic
    let mut consumer1 = connect_client(port).await;
    let mut consumer2 = connect_client(port).await;
    let mut consumer3 = connect_client(port).await;

    consumer1.subscribe("work", "tasks").await.unwrap();
    consumer2.subscribe("work", "tasks").await.unwrap();
    consumer3.subscribe("work", "tasks").await.unwrap();

    // Small delay to let subscriptions register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 9 messages
    let publisher = connect_client(port).await;
    for i in 0..9 {
        publisher
            .publish("work", serde_json::json!({"task": i}))
            .await
            .expect("publish should succeed");
    }

    // Each consumer should get approximately 3 messages (round-robin).
    // Use tokio::join! with the standalone recv_messages helper.
    let (r1, r2, r3) = tokio::join!(
        recv_messages(&mut consumer1, 3),
        recv_messages(&mut consumer2, 3),
        recv_messages(&mut consumer3, 3),
    );

    let total = r1.len() + r2.len() + r3.len();
    assert_eq!(
        total, 9,
        "all 9 messages should be distributed across consumers, got {}",
        total
    );

    // Each consumer should have exactly 3 with perfect round-robin
    assert_eq!(r1.len(), 3, "consumer1 should receive 3 messages");
    assert_eq!(r2.len(), 3, "consumer2 should receive 3 messages");
    assert_eq!(r3.len(), 3, "consumer3 should receive 3 messages");

    // ACK all received messages
    for msg in &r1 {
        consumer1.ack(msg.message_id).await.unwrap();
    }
    for msg in &r2 {
        consumer2.ack(msg.message_id).await.unwrap();
    }
    for msg in &r3 {
        consumer3.ack(msg.message_id).await.unwrap();
    }

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 4: Fan-out across different topics
//
// Each consumer subscribes to a *different* topic.  When a message is
// published to the channel it is enqueued to every topic, so every
// consumer should receive every message.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fan_out() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;

    let admin = connect_client(port).await;
    admin
        .create_channel("notifications", None, None)
        .await
        .expect("create_channel should succeed");

    // Three consumers on different topics (fan-out means each topic gets every msg)
    let mut consumer_a = connect_client(port).await;
    let mut consumer_b = connect_client(port).await;
    let mut consumer_c = connect_client(port).await;

    consumer_a
        .subscribe("notifications", "email")
        .await
        .unwrap();
    consumer_b
        .subscribe("notifications", "sms")
        .await
        .unwrap();
    consumer_c
        .subscribe("notifications", "push")
        .await
        .unwrap();

    // Small delay to let subscriptions register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 3 messages
    let publisher = connect_client(port).await;
    let mut published_ids = Vec::new();
    for i in 0..3 {
        let id = publisher
            .publish("notifications", serde_json::json!({"n": i}))
            .await
            .expect("publish should succeed");
        published_ids.push(id);
    }

    let (ra, rb, rc) = tokio::join!(
        recv_messages(&mut consumer_a, 3),
        recv_messages(&mut consumer_b, 3),
        recv_messages(&mut consumer_c, 3),
    );

    // Each consumer should have received all 3 messages
    assert_eq!(
        ra.len(),
        3,
        "consumer_a (email topic) should receive all 3 messages, got {}",
        ra.len()
    );
    assert_eq!(
        rb.len(),
        3,
        "consumer_b (sms topic) should receive all 3 messages, got {}",
        rb.len()
    );
    assert_eq!(
        rc.len(),
        3,
        "consumer_c (push topic) should receive all 3 messages, got {}",
        rc.len()
    );

    // Verify message IDs match what was published
    let ra_ids: Vec<_> = ra.iter().map(|m| m.message_id).collect();
    let rb_ids: Vec<_> = rb.iter().map(|m| m.message_id).collect();
    let rc_ids: Vec<_> = rc.iter().map(|m| m.message_id).collect();
    for id in &published_ids {
        assert!(ra_ids.contains(id), "consumer_a should have message {}", id);
        assert!(rb_ids.contains(id), "consumer_b should have message {}", id);
        assert!(rc_ids.contains(id), "consumer_c should have message {}", id);
    }

    // ACK everything
    for msg in &ra {
        consumer_a.ack(msg.message_id).await.unwrap();
    }
    for msg in &rb {
        consumer_b.ack(msg.message_id).await.unwrap();
    }
    for msg in &rc {
        consumer_c.ack(msg.message_id).await.unwrap();
    }

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 5: NACK causes redelivery
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_nack_redelivery() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;

    let admin = connect_client(port).await;
    admin
        .create_channel("retry-ch", None, None)
        .await
        .expect("create_channel should succeed");

    let mut consumer = connect_client(port).await;
    consumer.subscribe("retry-ch", "work").await.unwrap();

    // Small delay to let subscriptions register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish one message
    let publisher = connect_client(port).await;
    let payload = serde_json::json!({"job": "process_payment"});
    let msg_id = publisher
        .publish("retry-ch", payload.clone())
        .await
        .expect("publish should succeed");

    // First delivery
    let push1 = timeout(Duration::from_secs(5), consumer.recv_message())
        .await
        .expect("should receive message within timeout")
        .expect("should get a message push");

    assert_eq!(push1.message_id, msg_id);
    assert_eq!(push1.attempt, 1);
    assert_eq!(push1.payload, payload);

    // NACK the message to trigger redelivery
    consumer.nack(push1.message_id).await.unwrap();

    // Wait for the dispatch loop to redeliver (runs every 10ms)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second delivery -- same message, higher attempt count
    let push2 = timeout(Duration::from_secs(5), consumer.recv_message())
        .await
        .expect("should receive redelivered message within timeout")
        .expect("should get the redelivered message push");

    assert_eq!(push2.message_id, msg_id, "redelivered message should have same ID");
    assert_eq!(push2.channel, "retry-ch");
    assert_eq!(push2.payload, payload);
    // With cumulative attempt tracking, attempt should be 2
    assert_eq!(
        push2.attempt, 2,
        "redelivered attempt should be 2 (cumulative tracking)"
    );

    // Now ACK to complete the cycle
    consumer.ack(push2.message_id).await.unwrap();

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 6: DLQ flow end-to-end
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_dlq_flow_end_to_end() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;

    let admin = connect_client(port).await;
    // Create channel with max_delivery_attempts=2
    admin
        .create_channel("dlq-test", None, Some(2))
        .await
        .expect("create_channel should succeed");

    let mut consumer = connect_client(port).await;
    consumer.subscribe("dlq-test", "work").await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish a message
    let publisher = connect_client(port).await;
    let payload = serde_json::json!({"task": "fail_me"});
    let msg_id = publisher
        .publish("dlq-test", payload.clone())
        .await
        .expect("publish should succeed");

    // Dispatch attempt 1 → nack
    let push1 = timeout(Duration::from_secs(5), consumer.recv_message())
        .await
        .expect("should receive message")
        .expect("should get push");
    assert_eq!(push1.message_id, msg_id);
    assert_eq!(push1.attempt, 1);
    consumer.nack(push1.message_id).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Dispatch attempt 2 → nack (should dead-letter)
    let push2 = timeout(Duration::from_secs(5), consumer.recv_message())
        .await
        .expect("should receive redelivered message")
        .expect("should get push");
    assert_eq!(push2.message_id, msg_id);
    assert_eq!(push2.attempt, 2);
    consumer.nack(push2.message_id).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify message is in DLQ
    let dlq_result = admin.list_dlq("dlq-test").await.expect("list_dlq should succeed");
    assert_eq!(
        dlq_result.messages.len(),
        1,
        "DLQ should contain 1 message, got {}",
        dlq_result.messages.len()
    );
    assert_eq!(dlq_result.messages[0].message_id, msg_id);

    // Retry the DLQ message
    admin
        .retry_dlq("dlq-test", msg_id)
        .await
        .expect("retry_dlq should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Message should be redelivered
    let push3 = timeout(Duration::from_secs(5), consumer.recv_message())
        .await
        .expect("should receive retried message")
        .expect("should get push");
    assert_eq!(push3.message_id, msg_id);

    // ACK to complete
    consumer.ack(push3.message_id).await.unwrap();

    // DLQ should be empty now
    let dlq_result = admin.list_dlq("dlq-test").await.expect("list_dlq should succeed");
    assert_eq!(
        dlq_result.messages.len(),
        0,
        "DLQ should be empty after retry+ack"
    );

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 7: Delete channel
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_delete_channel() {
    let (port, shutdown_tx, _tmp_dir) = start_test_server().await;
    let client = connect_client(port).await;

    // Create a channel
    client
        .create_channel("to-delete", None, None)
        .await
        .expect("create_channel should succeed");

    // Publish a message to it
    client
        .publish("to-delete", serde_json::json!({"data": 1}))
        .await
        .expect("publish should succeed");

    // Delete the channel
    client
        .delete_channel("to-delete")
        .await
        .expect("delete_channel should succeed");

    // Verify it's gone from list_channels
    let result = client.list_channels().await.expect("list_channels should succeed");
    let names: Vec<&str> = result.channels.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !names.contains(&"to-delete"),
        "channel list should not contain 'to-delete' after deletion, got: {:?}",
        names
    );

    // Publishing to deleted channel should fail
    let err = client
        .publish("to-delete", serde_json::json!({"data": 2}))
        .await;
    assert!(err.is_err(), "publish to deleted channel should fail");

    let _ = shutdown_tx.send(());
}

// ---------------------------------------------------------------------------
// Test 8: WAL recovery after restart
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_wal_recovery_after_restart() {
    let port = free_port();
    let tmp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let data_dir = tmp_dir.path().to_string_lossy().to_string();

    // Phase 1: Start server, create channel, publish 3 messages, then shutdown
    let shutdown_tx = start_server_with_config(port, &data_dir).await;

    let admin = connect_client(port).await;
    admin
        .create_channel("recovery-ch", None, None)
        .await
        .expect("create_channel should succeed");

    let publisher = connect_client(port).await;
    let mut published_ids = Vec::new();
    for i in 0..3 {
        let id = publisher
            .publish("recovery-ch", serde_json::json!({"msg": i}))
            .await
            .expect("publish should succeed");
        published_ids.push(id);
    }

    // Give the sync loop time to flush WAL
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Shutdown the server
    let _ = shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Phase 2: Restart with same data_dir, new port (old one might not be released yet)
    let port2 = free_port();
    let shutdown_tx2 = start_server_with_config(port2, &data_dir).await;

    // Subscribe and verify we can receive the 3 recovered messages
    let mut consumer = connect_client(port2).await;
    consumer
        .subscribe("recovery-ch", "work")
        .await
        .expect("subscribe should succeed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let received = recv_messages(&mut consumer, 3).await;
    assert_eq!(
        received.len(),
        3,
        "should recover all 3 messages, got {}",
        received.len()
    );

    let received_ids: Vec<_> = received.iter().map(|m| m.message_id).collect();
    for id in &published_ids {
        assert!(
            received_ids.contains(id),
            "recovered messages should contain published ID {}",
            id
        );
    }

    // ACK all
    for msg in &received {
        consumer.ack(msg.message_id).await.unwrap();
    }

    let _ = shutdown_tx2.send(());
}
