use std::collections::HashMap;

use quarkmq_protocol::message::Message;
use quarkmq_protocol::rpc::ConsumerId;
use quarkmq_protocol::MessageId;

use crate::consumer::Consumer;
use crate::error::BrokerError;
use crate::topic::Topic;

#[derive(Debug, Clone)]
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
    messages: HashMap<MessageId, Message>,
}

impl Channel {
    pub fn new(config: ChannelConfig) -> Self {
        Self {
            config,
            topics: HashMap::new(),
            messages: HashMap::new(),
        }
    }

    pub fn publish(&mut self, message: Message) -> MessageId {
        let id = message.id;
        self.messages.insert(id, message);

        // Enqueue to all topics (fan-out)
        for topic in self.topics.values_mut() {
            topic.enqueue(id);
        }
        id
    }

    pub fn subscribe(
        &mut self,
        topic_name: &str,
        consumer_id: ConsumerId,
    ) {
        let topic = self
            .topics
            .entry(topic_name.to_string())
            .or_insert_with(|| Topic::new(topic_name));

        let consumer = Consumer::new(consumer_id, self.config.max_inflight_per_consumer);
        topic.add_consumer(consumer);
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
                    let mut msg_clone = msg.clone();
                    msg_clone.attempt = attempt;
                    dispatches.push(Dispatch {
                        consumer_id,
                        message: msg_clone,
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
            self.messages.remove(message_id);
        }

        Ok(())
    }

    /// NACK a message — return it to pending for redelivery.
    pub fn nack(
        &mut self,
        _consumer_id: ConsumerId,
        message_id: &MessageId,
    ) -> Result<(), BrokerError> {
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

        for topic in self.topics.values_mut() {
            let timed_out = topic.check_timeouts(timeout);
            for (consumer_id, msg_id) in timed_out {
                tracing::warn!(
                    message_id = %msg_id,
                    consumer_id = %consumer_id,
                    "message ACK timed out, returning to pending"
                );
            }
        }

        // After returning to pending, try to re-dispatch
        dispatches.extend(self.dispatch());
        dispatches
    }

    pub fn topic_names(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }

    pub fn pending_count(&self) -> usize {
        self.topics.values().map(|t| t.pending_count()).sum()
    }

    pub fn get_message(&self, id: &MessageId) -> Option<&Message> {
        self.messages.get(id)
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
        ch.publish(msg);

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
            ch.publish(msg);
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
        ch.publish(m1);
        ch.publish(m2);

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
        ch.publish(msg);

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
        ch.publish(msg);

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
        // Attempt resets to 1 because NACK removes the inflight entry
        // (attempt tracking only persists while inflight)
        assert_eq!(dispatches[0].message.attempt, 1);
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
}
