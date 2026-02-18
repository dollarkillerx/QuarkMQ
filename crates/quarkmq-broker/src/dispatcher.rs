use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use quarkmq_protocol::message::Message;
use quarkmq_protocol::rpc::ConsumerId;
use quarkmq_protocol::MessageId;

use crate::channel::{Channel, ChannelConfig, Dispatch};
use crate::error::BrokerError;

/// Subscription tracks which channel+topic a consumer is subscribed to.
#[derive(Debug, Clone)]
pub struct Subscription {
    pub channel: String,
    pub topic: String,
}

/// The Dispatcher is the central broker coordinating channels and consumers.
pub struct Dispatcher {
    channels: Arc<RwLock<HashMap<String, Channel>>>,
    /// Tracks consumer_id → list of subscriptions
    subscriptions: Arc<RwLock<HashMap<ConsumerId, Vec<Subscription>>>>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_channel(&self, config: ChannelConfig) -> Result<(), BrokerError> {
        let mut channels = self.channels.write();
        if channels.contains_key(&config.name) {
            return Err(BrokerError::ChannelAlreadyExists(config.name.clone()));
        }
        let name = config.name.clone();
        channels.insert(name, Channel::new(config));
        Ok(())
    }

    pub fn delete_channel(&self, name: &str) -> Result<(), BrokerError> {
        let mut channels = self.channels.write();
        if channels.remove(name).is_none() {
            return Err(BrokerError::ChannelNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn list_channels(&self) -> Vec<quarkmq_protocol::rpc::ChannelInfo> {
        let channels = self.channels.read();
        channels
            .values()
            .map(|ch| quarkmq_protocol::rpc::ChannelInfo {
                name: ch.config.name.clone(),
                topics: ch.topic_names(),
                pending_count: ch.pending_count(),
            })
            .collect()
    }

    pub fn publish(&self, channel_name: &str, message: Message) -> Result<MessageId, BrokerError> {
        let mut channels = self.channels.write();
        let channel = channels
            .get_mut(channel_name)
            .ok_or_else(|| BrokerError::ChannelNotFound(channel_name.to_string()))?;
        Ok(channel.publish(message))
    }

    pub fn subscribe(
        &self,
        channel_name: &str,
        topic_name: &str,
        consumer_id: ConsumerId,
    ) -> Result<(), BrokerError> {
        let mut channels = self.channels.write();
        let channel = channels
            .get_mut(channel_name)
            .ok_or_else(|| BrokerError::ChannelNotFound(channel_name.to_string()))?;

        channel.subscribe(topic_name, consumer_id);

        drop(channels);

        let mut subs = self.subscriptions.write();
        subs.entry(consumer_id).or_default().push(Subscription {
            channel: channel_name.to_string(),
            topic: topic_name.to_string(),
        });

        Ok(())
    }

    pub fn unsubscribe(
        &self,
        channel_name: &str,
        topic_name: &str,
        consumer_id: ConsumerId,
    ) -> Result<(), BrokerError> {
        let mut channels = self.channels.write();
        let channel = channels
            .get_mut(channel_name)
            .ok_or_else(|| BrokerError::ChannelNotFound(channel_name.to_string()))?;

        channel.unsubscribe(topic_name, consumer_id)?;

        drop(channels);

        let mut subs = self.subscriptions.write();
        if let Some(sub_list) = subs.get_mut(&consumer_id) {
            sub_list.retain(|s| !(s.channel == channel_name && s.topic == topic_name));
        }

        Ok(())
    }

    /// Remove a consumer from all their subscriptions (e.g., on disconnect).
    pub fn disconnect_consumer(&self, consumer_id: ConsumerId) {
        let subs = {
            let mut subs = self.subscriptions.write();
            subs.remove(&consumer_id).unwrap_or_default()
        };

        let mut channels = self.channels.write();
        for sub in subs {
            if let Some(channel) = channels.get_mut(&sub.channel) {
                let _ = channel.unsubscribe(&sub.topic, consumer_id);
            }
        }
    }

    /// Dispatch pending messages across all channels.
    /// Returns dispatches that need to be sent to consumers.
    pub fn dispatch_all(&self) -> Vec<Dispatch> {
        let mut channels = self.channels.write();
        let mut all_dispatches = Vec::new();
        for channel in channels.values_mut() {
            all_dispatches.extend(channel.dispatch());
        }
        all_dispatches
    }

    pub fn ack(
        &self,
        consumer_id: ConsumerId,
        message_id: &MessageId,
    ) -> Result<(), BrokerError> {
        let subs = self.subscriptions.read();
        let consumer_subs = subs.get(&consumer_id).ok_or(BrokerError::NotSubscribed)?;

        let mut channels = self.channels.write();
        for sub in consumer_subs {
            if let Some(channel) = channels.get_mut(&sub.channel) {
                match channel.ack(consumer_id, message_id) {
                    Ok(()) => return Ok(()),
                    Err(BrokerError::MessageNotInflight(_)) => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        Err(BrokerError::MessageNotInflight(*message_id))
    }

    pub fn nack(
        &self,
        consumer_id: ConsumerId,
        message_id: &MessageId,
    ) -> Result<(), BrokerError> {
        let subs = self.subscriptions.read();
        let consumer_subs = subs.get(&consumer_id).ok_or(BrokerError::NotSubscribed)?;

        let mut channels = self.channels.write();
        for sub in consumer_subs {
            if let Some(channel) = channels.get_mut(&sub.channel) {
                match channel.nack(consumer_id, message_id) {
                    Ok(()) => return Ok(()),
                    Err(BrokerError::MessageNotInflight(_)) => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        Err(BrokerError::MessageNotInflight(*message_id))
    }

    /// Check ACK timeouts across all channels. Returns dispatches to re-send.
    pub fn check_timeouts(&self) -> Vec<Dispatch> {
        let mut channels = self.channels.write();
        let mut all_dispatches = Vec::new();
        for channel in channels.values_mut() {
            all_dispatches.extend(channel.check_timeouts());
        }
        all_dispatches
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelConfig;
    use quarkmq_protocol::message::Message;
    use serde_json::json;
    use uuid::Uuid;

    fn make_config(name: &str) -> ChannelConfig {
        ChannelConfig {
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn make_message(channel: &str) -> Message {
        Message::new(channel, json!({"key": "value"}))
    }

    fn make_consumer_id() -> ConsumerId {
        Uuid::now_v7()
    }

    // ---- create_channel / delete_channel / list_channels ----

    #[test]
    fn create_and_list_channels() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("ch-alpha")).unwrap();
        disp.create_channel(make_config("ch-beta")).unwrap();

        let channels = disp.list_channels();
        assert_eq!(channels.len(), 2);

        let names: Vec<_> = channels.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"ch-alpha"));
        assert!(names.contains(&"ch-beta"));
    }

    #[test]
    fn create_duplicate_channel_returns_error() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("dup")).unwrap();
        let result = disp.create_channel(make_config("dup"));
        assert!(result.is_err());
    }

    #[test]
    fn delete_channel_removes_it() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("to-delete")).unwrap();
        assert_eq!(disp.list_channels().len(), 1);

        disp.delete_channel("to-delete").unwrap();
        assert_eq!(disp.list_channels().len(), 0);
    }

    #[test]
    fn delete_nonexistent_channel_returns_error() {
        let disp = Dispatcher::new();
        let result = disp.delete_channel("ghost");
        assert!(result.is_err());
    }

    // ---- publish to existing channel ----

    #[test]
    fn publish_to_existing_channel() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("pub-ch")).unwrap();

        let msg = make_message("pub-ch");
        let msg_id = msg.id;
        let result = disp.publish("pub-ch", msg).unwrap();
        assert_eq!(result, msg_id);
    }

    // ---- publish to non-existent channel returns ChannelNotFound ----

    #[test]
    fn publish_to_nonexistent_channel_returns_error() {
        let disp = Dispatcher::new();
        let msg = make_message("no-such-channel");
        let result = disp.publish("no-such-channel", msg);
        assert!(result.is_err());
        match result.unwrap_err() {
            BrokerError::ChannelNotFound(name) => assert_eq!(name, "no-such-channel"),
            other => panic!("expected ChannelNotFound, got {:?}", other),
        }
    }

    // ---- subscribe + dispatch_all round-robin ----

    #[test]
    fn subscribe_and_dispatch_all_round_robin() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("rr-ch")).unwrap();

        let c1 = make_consumer_id();
        let c2 = make_consumer_id();
        disp.subscribe("rr-ch", "work", c1).unwrap();
        disp.subscribe("rr-ch", "work", c2).unwrap();

        // Publish 4 messages
        let mut msg_ids = Vec::new();
        for _ in 0..4 {
            let msg = make_message("rr-ch");
            msg_ids.push(msg.id);
            disp.publish("rr-ch", msg).unwrap();
        }

        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 4);

        // Round-robin: c1, c2, c1, c2
        assert_eq!(dispatches[0].consumer_id, c1);
        assert_eq!(dispatches[1].consumer_id, c2);
        assert_eq!(dispatches[2].consumer_id, c1);
        assert_eq!(dispatches[3].consumer_id, c2);

        // Correct message ids
        for (i, d) in dispatches.iter().enumerate() {
            assert_eq!(d.message.id, msg_ids[i]);
        }
    }

    // ---- ack + nack through dispatcher ----

    #[test]
    fn ack_through_dispatcher() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("ack-ch")).unwrap();

        let c = make_consumer_id();
        disp.subscribe("ack-ch", "topic", c).unwrap();

        let msg = make_message("ack-ch");
        let msg_id = msg.id;
        disp.publish("ack-ch", msg).unwrap();

        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 1);

        let result = disp.ack(c, &msg_id);
        assert!(result.is_ok());
    }

    #[test]
    fn nack_through_dispatcher_allows_redelivery() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("nack-ch")).unwrap();

        let c = make_consumer_id();
        disp.subscribe("nack-ch", "topic", c).unwrap();

        let msg = make_message("nack-ch");
        let msg_id = msg.id;
        disp.publish("nack-ch", msg).unwrap();

        // Dispatch, then NACK
        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 1);

        disp.nack(c, &msg_id).unwrap();

        // Dispatch again — should get the same message
        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].consumer_id, c);
        assert_eq!(dispatches[0].message.id, msg_id);
        // Attempt resets to 1 because NACK removes the inflight entry
        // (attempt tracking only persists while inflight)
        assert_eq!(dispatches[0].message.attempt, 1);
    }

    #[test]
    fn ack_not_subscribed_returns_error() {
        let disp = Dispatcher::new();
        let unknown_consumer = make_consumer_id();
        let fake_msg = Uuid::now_v7();

        let result = disp.ack(unknown_consumer, &fake_msg);
        assert!(result.is_err());
        match result.unwrap_err() {
            BrokerError::NotSubscribed => {}
            other => panic!("expected NotSubscribed, got {:?}", other),
        }
    }

    // ---- disconnect_consumer removes from all subscriptions ----

    #[test]
    fn disconnect_consumer_removes_from_all_subscriptions() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("dc-ch1")).unwrap();
        disp.create_channel(make_config("dc-ch2")).unwrap();

        let c = make_consumer_id();
        disp.subscribe("dc-ch1", "topic-a", c).unwrap();
        disp.subscribe("dc-ch2", "topic-b", c).unwrap();

        // Publish messages to both channels
        let msg1 = make_message("dc-ch1");
        let msg2 = make_message("dc-ch2");
        disp.publish("dc-ch1", msg1).unwrap();
        disp.publish("dc-ch2", msg2).unwrap();

        // Should be able to dispatch before disconnect
        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 2);

        // Disconnect the consumer
        disp.disconnect_consumer(c);

        // ACK should fail since consumer is no longer subscribed
        let result = disp.ack(c, &dispatches[0].message.id);
        assert!(result.is_err());
    }

    #[test]
    fn disconnect_consumer_inflight_messages_return_to_pending() {
        let disp = Dispatcher::new();
        disp.create_channel(make_config("dc-ch")).unwrap();

        let c1 = make_consumer_id();
        let c2 = make_consumer_id();
        disp.subscribe("dc-ch", "work", c1).unwrap();
        disp.subscribe("dc-ch", "work", c2).unwrap();

        let msg = make_message("dc-ch");
        let msg_id = msg.id;
        disp.publish("dc-ch", msg).unwrap();

        // Dispatch — message goes to c1 (first in round-robin)
        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].consumer_id, c1);

        // Disconnect c1 — message should return to pending
        disp.disconnect_consumer(c1);

        // Dispatch again — c2 should now receive it
        let dispatches = disp.dispatch_all();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].consumer_id, c2);
        assert_eq!(dispatches[0].message.id, msg_id);
    }
}
