use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use quarkmq_protocol::message::Message;
use quarkmq_protocol::rpc::ConsumerId;
use quarkmq_protocol::MessageId;
use quarkmq_storage::wal::{Wal, WalOperation};

use crate::consumer::Consumer;
use crate::error::BrokerError;
use crate::topic::Topic;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChannelConfig {
    pub name: String,
    pub ack_timeout_secs: u64,
    pub max_delivery_attempts: u32,
    pub max_inflight_per_consumer: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            ack_timeout_secs: 30,
            max_delivery_attempts: 5,
            max_inflight_per_consumer: 100,
        }
    }
}

/// Dispatch represents a message that should be sent to a consumer.
#[derive(Debug, Clone)]
pub struct Dispatch {
    pub consumer_id: ConsumerId,
    pub message: Message,
}

#[derive(Debug)]
pub struct Channel {
    pub config: ChannelConfig,
    topics: HashMap<String, Topic>,
    messages: HashMap<MessageId, Arc<Message>>,
    wal: Option<Wal>,
    dlq: Vec<Arc<Message>>,
    data_dir: Option<PathBuf>,
    /// Message IDs recovered from WAL that need to be enqueued when topics are created.
    recovered_pending: Vec<MessageId>,
}

impl Channel {
    pub fn new(config: ChannelConfig) -> Self {
        Self {
            config,
            topics: HashMap::new(),
            messages: HashMap::new(),
            wal: None,
            dlq: Vec::new(),
            data_dir: None,
            recovered_pending: Vec::new(),
        }
    }

    /// Create a channel with WAL-backed storage.
    pub fn with_storage(config: ChannelConfig, data_dir: &Path) -> Result<Self, BrokerError> {
        let channel_dir = data_dir.join(&config.name);
        std::fs::create_dir_all(&channel_dir).map_err(|e| {
            BrokerError::Storage(quarkmq_storage::StorageError::Io(e))
        })?;

        // Write meta.json
        let meta_path = channel_dir.join("meta.json");
        let meta_json = serde_json::to_string_pretty(&config).map_err(|e| {
            BrokerError::Storage(quarkmq_storage::StorageError::Serialization(e))
        })?;
        std::fs::write(&meta_path, meta_json).map_err(|e| {
            BrokerError::Storage(quarkmq_storage::StorageError::Io(e))
        })?;

        let wal_path = channel_dir.join("wal.qmq");
        let wal = Wal::open(&wal_path)?;

        Ok(Self {
            config,
            topics: HashMap::new(),
            messages: HashMap::new(),
            wal: Some(wal),
            dlq: Vec::new(),
            data_dir: Some(channel_dir),
            recovered_pending: Vec::new(),
        })
    }

    /// Recover a channel from existing WAL data on disk.
    pub fn recover(config: ChannelConfig, channel_dir: &Path) -> Result<Self, BrokerError> {
        let wal_path = channel_dir.join("wal.qmq");
        let records = Wal::read_all_records(&wal_path)?;

        let mut messages: HashMap<MessageId, Arc<Message>> = HashMap::new();
        let mut acked: std::collections::HashSet<MessageId> = std::collections::HashSet::new();
        let mut dead_lettered: std::collections::HashSet<MessageId> = std::collections::HashSet::new();
        let mut dlq: Vec<Arc<Message>> = Vec::new();

        for record in &records {
            match record.operation {
                WalOperation::Append => {
                    if let Ok(msg) = serde_json::from_slice::<Message>(&record.data) {
                        messages.insert(record.message_id, Arc::new(msg));
                    }
                }
                WalOperation::Ack => {
                    acked.insert(record.message_id);
                }
                WalOperation::DeadLetter => {
                    dead_lettered.insert(record.message_id);
                    if let Some(msg) = messages.get(&record.message_id) {
                        dlq.push(msg.clone());
                    }
                }
                _ => {}
            }
        }

        // Messages that are neither acked nor dead-lettered are pending
        let recovered_pending: Vec<MessageId> = messages
            .keys()
            .filter(|id| !acked.contains(id) && !dead_lettered.contains(id))
            .copied()
            .collect();

        // Remove acked messages from memory (they're done)
        for id in &acked {
            messages.remove(id);
        }

        // Open WAL for continued writing
        let wal = Wal::open(&wal_path)?;

        tracing::info!(
            channel = %config.name,
            messages = messages.len(),
            pending = recovered_pending.len(),
            dlq = dlq.len(),
            "recovered channel from WAL"
        );

        Ok(Self {
            config,
            topics: HashMap::new(),
            messages,
            wal: Some(wal),
            dlq,
            data_dir: Some(channel_dir.to_path_buf()),
            recovered_pending,
        })
    }

    pub fn publish(&mut self, message: Message) -> Result<MessageId, BrokerError> {
        let id = message.id;

        // Write-ahead: persist to WAL before in-memory
        if let Some(wal) = &mut self.wal {
            let data = serde_json::to_vec(&message).map_err(|e| {
                BrokerError::Storage(quarkmq_storage::StorageError::Serialization(e))
            })?;
            wal.append(WalOperation::Append, id, data)?;
        }

        self.messages.insert(id, Arc::new(message));

        // Enqueue to all topics (fan-out)
        for topic in self.topics.values_mut() {
            topic.enqueue(id);
        }
        Ok(id)
    }

    pub fn subscribe(
        &mut self,
        topic_name: &str,
        consumer_id: ConsumerId,
    ) {
        let is_new_topic = !self.topics.contains_key(topic_name);

        let topic = self
            .topics
            .entry(topic_name.to_string())
            .or_insert_with(|| Topic::new(topic_name));

        let consumer = Consumer::new(consumer_id, self.config.max_inflight_per_consumer);
        topic.add_consumer(consumer);

        // When a new topic is created, enqueue any recovered pending messages
        if is_new_topic && !self.recovered_pending.is_empty() {
            for &msg_id in &self.recovered_pending {
                topic.enqueue(msg_id);
            }
        }
    }

    pub fn unsubscribe(&mut self, topic_name: &str, consumer_id: ConsumerId) -> Result<(), BrokerError> {
        let topic = self
            .topics
            .get_mut(topic_name)
            .ok_or_else(|| BrokerError::TopicNotFound(topic_name.to_string()))?;

        topic.remove_consumer(consumer_id);
        Ok(())
    }

    /// Remove a consumer from all topics in this channel.
    pub fn remove_consumer(&mut self, consumer_id: ConsumerId) {
        for topic in self.topics.values_mut() {
            topic.remove_consumer(consumer_id);
        }
    }

    /// Try to dispatch pending messages across all topics.
    /// Returns a list of dispatches (consumer_id + message).
    pub fn dispatch(&mut self) -> Vec<Dispatch> {
        let mut dispatches = Vec::new();

        for topic in self.topics.values_mut() {
            while let Some((consumer_id, msg_id)) = topic.try_dispatch() {
                if let Some(msg) = self.messages.get(&msg_id) {
                    let attempt = topic.get_inflight_attempt(&msg_id).unwrap_or(1);
                    // Build dispatch message with correct attempt, sharing payload via Arc
                    let dispatch_msg = Message {
                        id: msg.id,
                        channel: msg.channel.clone(),
                        payload: msg.payload.clone(),
                        created_at: msg.created_at,
                        attempt,
                    };
                    dispatches.push(Dispatch {
                        consumer_id,
                        message: dispatch_msg,
                    });
                }
            }
        }

        dispatches
    }

    /// ACK a message for a specific consumer's topic.
    pub fn ack(
        &mut self,
        _consumer_id: ConsumerId,
        message_id: &MessageId,
    ) -> Result<(), BrokerError> {
        let mut found = false;
        for topic in self.topics.values_mut() {
            if topic.ack(message_id) {
                found = true;
                break;
            }
        }

        if !found {
            return Err(BrokerError::MessageNotInflight(*message_id));
        }

        // If all topics have ACKed this message, we can remove it
        let all_acked = self.topics.values().all(|t| t.is_acked(message_id));
        if all_acked {
            // WAL: record ack
            if let Some(wal) = &mut self.wal {
                let _ = wal.append(WalOperation::Ack, *message_id, vec![]);
            }
            self.messages.remove(message_id);
        }

        Ok(())
    }

    /// NACK a message — return it to pending for redelivery, or dead-letter if max attempts exceeded.
    pub fn nack(
        &mut self,
        _consumer_id: ConsumerId,
        message_id: &MessageId,
    ) -> Result<(), BrokerError> {
        // Check delivery attempts across all topics
        let max_attempts = self.config.max_delivery_attempts;
        for topic in self.topics.values() {
            let attempts = topic.delivery_attempt_count(message_id);
            if attempts >= max_attempts {
                self.dead_letter_internal(message_id);
                return Ok(());
            }
        }

        let mut found = false;
        for topic in self.topics.values_mut() {
            if topic.nack(message_id) {
                found = true;
                break;
            }
        }

        if !found {
            return Err(BrokerError::MessageNotInflight(*message_id));
        }

        Ok(())
    }

    /// Check for ACK timeouts across all topics.
    pub fn check_timeouts(&mut self) -> Vec<Dispatch> {
        let mut dispatches = Vec::new();
        let timeout = self.config.ack_timeout_secs;
        let max_attempts = self.config.max_delivery_attempts;

        let mut to_dead_letter = Vec::new();

        for topic in self.topics.values_mut() {
            let timed_out = topic.check_timeouts(timeout);
            for (consumer_id, msg_id, attempt) in timed_out {
                tracing::warn!(
                    message_id = %msg_id,
                    consumer_id = %consumer_id,
                    attempt = attempt,
                    "message ACK timed out, returning to pending"
                );
                if attempt >= max_attempts {
                    to_dead_letter.push(msg_id);
                }
            }
        }

        // Dead-letter messages that exceeded max attempts
        for msg_id in to_dead_letter {
            self.dead_letter_internal(&msg_id);
        }

        // After returning to pending, try to re-dispatch
        dispatches.extend(self.dispatch());
        dispatches
    }

    /// Move a message to the dead-letter queue.
    fn dead_letter_internal(&mut self, message_id: &MessageId) {
        // Remove from all topics (inflight or pending)
        for topic in self.topics.values_mut() {
            topic.remove_inflight(message_id);
            topic.remove_from_pending(message_id);
        }

        // WAL: record dead-letter
        if let Some(wal) = &mut self.wal {
            let _ = wal.append(WalOperation::DeadLetter, *message_id, vec![]);
        }

        // Move to DLQ
        if let Some(msg) = self.messages.remove(message_id) {
            tracing::warn!(
                message_id = %message_id,
                "message dead-lettered after exceeding max delivery attempts"
            );
            self.dlq.push(msg);
        }
    }

    pub fn sync_wal(&mut self) -> Result<(), BrokerError> {
        if let Some(wal) = &mut self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    pub fn dlq_count(&self) -> usize {
        self.dlq.len()
    }

    pub fn dlq_messages(&self) -> &[Arc<Message>] {
        &self.dlq
    }

    /// Retry a dead-lettered message by moving it back to pending.
    pub fn retry_dlq(&mut self, message_id: &MessageId) -> Result<(), BrokerError> {
        let pos = self
            .dlq
            .iter()
            .position(|m| m.id == *message_id)
            .ok_or(BrokerError::MessageNotFound(*message_id))?;

        let msg = self.dlq.remove(pos);

        // Re-publish: WAL write + enqueue to all topics
        if let Some(wal) = &mut self.wal {
            let data = serde_json::to_vec(msg.as_ref()).map_err(|e| {
                BrokerError::Storage(quarkmq_storage::StorageError::Serialization(e))
            })?;
            wal.append(WalOperation::Append, msg.id, data)?;
        }

        let id = msg.id;
        self.messages.insert(id, msg);
        for topic in self.topics.values_mut() {
            topic.enqueue(id);
        }

        Ok(())
    }

    pub fn topic_names(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }

    pub fn pending_count(&self) -> usize {
        self.topics.values().map(|t| t.pending_count()).sum()
    }

    pub fn get_message(&self, id: &MessageId) -> Option<&Message> {
        self.messages.get(id).map(|arc| arc.as_ref())
    }

    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// Compact the WAL by keeping only active (non-acked, non-dead-lettered) messages.
    pub fn compact_wal(&mut self) -> Result<(), BrokerError> {
        if let Some(wal) = &mut self.wal {
            // Rebuild WAL with only current messages
            let mut records = Vec::new();
            let mut seq = 1u64;
            for (id, msg) in &self.messages {
                let data = serde_json::to_vec(msg.as_ref()).map_err(|e| {
                    BrokerError::Storage(quarkmq_storage::StorageError::Serialization(e))
                })?;
                records.push(quarkmq_storage::WalRecord::new(
                    seq,
                    WalOperation::Append,
                    *id,
                    data,
                ));
                seq += 1;
            }
            // DLQ messages: write Append first (for recovery to rebuild message body),
            // then DeadLetter to mark them as dead-lettered.
            for msg in &self.dlq {
                let data = serde_json::to_vec(msg.as_ref()).map_err(|e| {
                    BrokerError::Storage(quarkmq_storage::StorageError::Serialization(e))
                })?;
                records.push(quarkmq_storage::WalRecord::new(
                    seq,
                    WalOperation::Append,
                    msg.id,
                    data,
                ));
                seq += 1;
                records.push(quarkmq_storage::WalRecord::new(
                    seq,
                    WalOperation::DeadLetter,
                    msg.id,
                    vec![],
                ));
                seq += 1;
            }
            wal.compact(&records)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn default_config(name: &str) -> ChannelConfig {
        ChannelConfig {
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn make_message(channel: &str) -> Message {
        Message::new(channel, json!({"data": "test"}))
    }

    fn make_consumer_id() -> ConsumerId {
        Uuid::now_v7()
    }

    // ---- Publish adds message and enqueues to all topics (fan-out) ----

    #[test]
    fn publish_stores_message_and_fans_out_to_all_topics() {
        let mut ch = Channel::new(default_config("ch1"));

        let c1 = make_consumer_id();
        let c2 = make_consumer_id();
        ch.subscribe("topic-a", c1);
        ch.subscribe("topic-b", c2);

        let msg = make_message("ch1");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // Message should be stored
        assert!(ch.get_message(&msg_id).is_some());

        // Both topics should have 1 pending message
        assert_eq!(ch.pending_count(), 2); // 1 per topic
    }

    // ---- Single-topic competing consumers ----

    #[test]
    fn single_topic_competing_consumers_round_robin() {
        let mut ch = Channel::new(default_config("ch1"));

        let c1 = make_consumer_id();
        let c2 = make_consumer_id();
        let c3 = make_consumer_id();
        ch.subscribe("work", c1);
        ch.subscribe("work", c2);
        ch.subscribe("work", c3);

        // Publish 6 messages
        let mut msg_ids = Vec::new();
        for _ in 0..6 {
            let msg = make_message("ch1");
            msg_ids.push(msg.id);
            ch.publish(msg).unwrap();
        }

        let dispatches = ch.dispatch();
        assert_eq!(dispatches.len(), 6);

        // Verify round-robin distribution: c1, c2, c3, c1, c2, c3
        assert_eq!(dispatches[0].consumer_id, c1);
        assert_eq!(dispatches[1].consumer_id, c2);
        assert_eq!(dispatches[2].consumer_id, c3);
        assert_eq!(dispatches[3].consumer_id, c1);
        assert_eq!(dispatches[4].consumer_id, c2);
        assert_eq!(dispatches[5].consumer_id, c3);

        // Each message should match its published id
        for (i, d) in dispatches.iter().enumerate() {
            assert_eq!(d.message.id, msg_ids[i]);
        }
    }

    // ---- Multi-topic fan-out ----

    #[test]
    fn multi_topic_fan_out_each_topic_gets_all_messages() {
        let mut ch = Channel::new(default_config("ch1"));

        let c_a = make_consumer_id();
        let c_b = make_consumer_id();
        ch.subscribe("topic-a", c_a);
        ch.subscribe("topic-b", c_b);

        let m1 = make_message("ch1");
        let m2 = make_message("ch1");
        let m1_id = m1.id;
        let m2_id = m2.id;
        ch.publish(m1).unwrap();
        ch.publish(m2).unwrap();

        let dispatches = ch.dispatch();
        // Each topic should receive both messages: 2 topics * 2 messages = 4 dispatches
        assert_eq!(dispatches.len(), 4);

        // Collect dispatches by consumer
        let a_msgs: Vec<_> = dispatches
            .iter()
            .filter(|d| d.consumer_id == c_a)
            .map(|d| d.message.id)
            .collect();
        let b_msgs: Vec<_> = dispatches
            .iter()
            .filter(|d| d.consumer_id == c_b)
            .map(|d| d.message.id)
            .collect();

        assert_eq!(a_msgs.len(), 2);
        assert_eq!(b_msgs.len(), 2);
        assert!(a_msgs.contains(&m1_id));
        assert!(a_msgs.contains(&m2_id));
        assert!(b_msgs.contains(&m1_id));
        assert!(b_msgs.contains(&m2_id));
    }

    // ---- ACK lifecycle ----

    #[test]
    fn ack_lifecycle_message_removed_when_all_topics_ack() {
        let mut ch = Channel::new(default_config("ch1"));

        let c_a = make_consumer_id();
        let c_b = make_consumer_id();
        ch.subscribe("topic-a", c_a);
        ch.subscribe("topic-b", c_b);

        let msg = make_message("ch1");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // Dispatch to both topics
        let dispatches = ch.dispatch();
        assert_eq!(dispatches.len(), 2);

        // ACK from topic-a's consumer; message still exists (topic-b hasn't acked)
        ch.ack(c_a, &msg_id).unwrap();
        assert!(ch.get_message(&msg_id).is_some());

        // ACK from topic-b's consumer; now all topics acked, message should be removed
        ch.ack(c_b, &msg_id).unwrap();
        assert!(ch.get_message(&msg_id).is_none());
    }

    #[test]
    fn ack_nonexistent_inflight_returns_error() {
        let mut ch = Channel::new(default_config("ch1"));
        let c = make_consumer_id();
        ch.subscribe("topic", c);

        let fake_id = Uuid::now_v7();
        let result = ch.ack(c, &fake_id);
        assert!(result.is_err());
    }

    // ---- NACK redelivery ----

    #[test]
    fn nack_redelivery() {
        let mut ch = Channel::new(default_config("ch1"));

        let c = make_consumer_id();
        ch.subscribe("work", c);

        let msg = make_message("ch1");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // First dispatch
        let dispatches = ch.dispatch();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].consumer_id, c);
        assert_eq!(dispatches[0].message.id, msg_id);

        // NACK the message
        ch.nack(c, &msg_id).unwrap();

        // Message should be back in pending
        assert_eq!(ch.pending_count(), 1);

        // Re-dispatch — should get the same message again
        let dispatches = ch.dispatch();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].consumer_id, c);
        assert_eq!(dispatches[0].message.id, msg_id);
        // Now attempt tracking is cumulative: second dispatch = attempt 2
        assert_eq!(dispatches[0].message.attempt, 2);
    }

    #[test]
    fn nack_nonexistent_inflight_returns_error() {
        let mut ch = Channel::new(default_config("ch1"));
        let c = make_consumer_id();
        ch.subscribe("topic", c);

        let fake_id = Uuid::now_v7();
        let result = ch.nack(c, &fake_id);
        assert!(result.is_err());
    }

    // ---- Storage tests ----

    #[test]
    fn test_channel_with_storage_persists_to_wal() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ch = Channel::with_storage(default_config("persist-ch"), tmp.path()).unwrap();

        let c = make_consumer_id();
        ch.subscribe("work", c);

        let msg = make_message("persist-ch");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();
        ch.sync_wal().unwrap();

        // Verify WAL file was created with data
        let wal_path = tmp.path().join("persist-ch").join("wal.qmq");
        assert!(wal_path.exists());
        let records = quarkmq_storage::Wal::read_all_records(&wal_path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].message_id, msg_id);
    }

    #[test]
    fn test_channel_recovery_restores_pending_messages() {
        let tmp = tempfile::tempdir().unwrap();
        let config = default_config("recover-ch");
        let msg_id;

        // Phase 1: Create channel, publish, sync, drop
        {
            let mut ch = Channel::with_storage(config.clone(), tmp.path()).unwrap();
            let c = make_consumer_id();
            ch.subscribe("work", c);

            let msg = make_message("recover-ch");
            msg_id = msg.id;
            ch.publish(msg).unwrap();
            ch.sync_wal().unwrap();
        }

        // Phase 2: Recover and verify
        let channel_dir = tmp.path().join("recover-ch");
        let mut ch = Channel::recover(config, &channel_dir).unwrap();
        assert!(ch.get_message(&msg_id).is_some());

        // Subscribe to trigger enqueue of recovered pending
        let c = make_consumer_id();
        ch.subscribe("work", c);

        let dispatches = ch.dispatch();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].message.id, msg_id);
    }

    #[test]
    fn test_channel_recovery_excludes_acked() {
        let tmp = tempfile::tempdir().unwrap();
        let config = default_config("ack-recover-ch");

        // Phase 1: Publish, dispatch, ack, sync
        {
            let mut ch = Channel::with_storage(config.clone(), tmp.path()).unwrap();
            let c = make_consumer_id();
            ch.subscribe("work", c);

            let msg = make_message("ack-recover-ch");
            let msg_id = msg.id;
            ch.publish(msg).unwrap();

            ch.dispatch();
            ch.ack(c, &msg_id).unwrap();
            ch.sync_wal().unwrap();
        }

        // Phase 2: Recover — message should not exist
        let channel_dir = tmp.path().join("ack-recover-ch");
        let mut ch = Channel::recover(config, &channel_dir).unwrap();
        let c = make_consumer_id();
        ch.subscribe("work", c);
        let dispatches = ch.dispatch();
        assert_eq!(dispatches.len(), 0);
    }

    #[test]
    fn test_nack_exceeding_max_attempts_dead_letters() {
        let mut config = default_config("dlq-ch");
        config.max_delivery_attempts = 2;
        let mut ch = Channel::new(config);

        let c = make_consumer_id();
        ch.subscribe("work", c);

        let msg = make_message("dlq-ch");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // Dispatch attempt 1
        ch.dispatch();
        // NACK — still under limit
        ch.nack(c, &msg_id).unwrap();

        // Dispatch attempt 2
        ch.dispatch();
        // NACK — at limit, should dead-letter
        ch.nack(c, &msg_id).unwrap();

        assert_eq!(ch.dlq_count(), 1);
        assert_eq!(ch.pending_count(), 0);
        assert!(ch.get_message(&msg_id).is_none());
    }

    #[test]
    fn test_timeout_exceeding_max_attempts_dead_letters() {
        let mut config = default_config("timeout-dlq-ch");
        config.max_delivery_attempts = 1;
        config.ack_timeout_secs = 0; // immediate timeout
        let mut ch = Channel::new(config);

        let c = make_consumer_id();
        ch.subscribe("work", c);

        let msg = make_message("timeout-dlq-ch");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // Dispatch attempt 1
        ch.dispatch();

        // Check timeouts — message at limit, should dead-letter
        ch.check_timeouts();

        assert_eq!(ch.dlq_count(), 1);
        assert!(ch.get_message(&msg_id).is_none());
    }

    #[test]
    fn test_compact_wal_preserves_dlq_messages() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = default_config("compact-dlq-ch");
        config.max_delivery_attempts = 1;

        let mut ch = Channel::with_storage(config.clone(), tmp.path()).unwrap();
        let c = make_consumer_id();
        ch.subscribe("work", c);

        let msg = make_message("compact-dlq-ch");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // Dispatch and nack to dead-letter
        ch.dispatch();
        ch.nack(c, &msg_id).unwrap();
        assert_eq!(ch.dlq_count(), 1);

        // Compact WAL
        ch.compact_wal().unwrap();
        ch.sync_wal().unwrap();

        // Recover from compacted WAL and verify DLQ message is preserved
        let channel_dir = tmp.path().join("compact-dlq-ch");
        let recovered = Channel::recover(config, &channel_dir).unwrap();
        assert_eq!(recovered.dlq_count(), 1);
        assert_eq!(recovered.dlq_messages()[0].id, msg_id);
    }

    #[test]
    fn test_dlq_count_and_messages() {
        let mut config = default_config("dlq-info-ch");
        config.max_delivery_attempts = 1;
        let mut ch = Channel::new(config);

        let c = make_consumer_id();
        ch.subscribe("work", c);

        assert_eq!(ch.dlq_count(), 0);
        assert!(ch.dlq_messages().is_empty());

        let msg = make_message("dlq-info-ch");
        let msg_id = msg.id;
        ch.publish(msg).unwrap();

        // Dispatch and nack to exceed max attempts
        ch.dispatch();
        ch.nack(c, &msg_id).unwrap();

        assert_eq!(ch.dlq_count(), 1);
        assert_eq!(ch.dlq_messages()[0].id, msg_id);
    }
}
