use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use quarkmq_protocol::MessageId;
use quarkmq_protocol::rpc::ConsumerId;

use crate::consumer::Consumer;

#[derive(Debug)]
pub struct InflightEntry {
    pub message_id: MessageId,
    pub consumer_id: ConsumerId,
    pub dispatched_at: DateTime<Utc>,
    pub attempt: u32,
}

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    consumers: HashMap<ConsumerId, Consumer>,
    /// Maintains round-robin ordering of consumer IDs.
    consumer_order: Vec<ConsumerId>,
    dispatch_index: usize,
    inflight: HashMap<MessageId, InflightEntry>,
    ack_set: HashSet<MessageId>,
    pending_queue: VecDeque<MessageId>,
    /// Cumulative delivery attempt counts, persisted across nack/re-dispatch.
    delivery_attempts: HashMap<MessageId, u32>,
}

impl Topic {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            consumers: HashMap::new(),
            consumer_order: Vec::new(),
            dispatch_index: 0,
            inflight: HashMap::new(),
            ack_set: HashSet::new(),
            pending_queue: VecDeque::new(),
            delivery_attempts: HashMap::new(),
        }
    }

    pub fn add_consumer(&mut self, consumer: Consumer) {
        let id = consumer.id;
        // H3: Don't add duplicate entries to consumer_order
        if self.consumers.contains_key(&id) {
            return;
        }
        self.consumers.insert(id, consumer);
        self.consumer_order.push(id);
    }

    pub fn remove_consumer(&mut self, consumer_id: ConsumerId) -> Vec<MessageId> {
        self.consumers.remove(&consumer_id);
        self.consumer_order.retain(|&id| id != consumer_id);
        if self.dispatch_index >= self.consumer_order.len() && !self.consumer_order.is_empty() {
            self.dispatch_index = 0;
        }

        // Return inflight messages for this consumer back to pending
        let mut returned = Vec::new();
        let to_return: Vec<MessageId> = self
            .inflight
            .iter()
            .filter(|(_, e)| e.consumer_id == consumer_id)
            .map(|(id, _)| *id)
            .collect();
        for msg_id in to_return {
            self.inflight.remove(&msg_id);
            self.pending_queue.push_front(msg_id);
            returned.push(msg_id);
        }
        returned
    }

    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    pub fn enqueue(&mut self, message_id: MessageId) {
        if !self.ack_set.contains(&message_id) {
            self.pending_queue.push_back(message_id);
        }
    }

    /// Try to dispatch the next pending message to a consumer via round-robin.
    /// Returns (consumer_id, message_id) if successful.
    pub fn try_dispatch(&mut self) -> Option<(ConsumerId, MessageId)> {
        if self.consumer_order.is_empty() || self.pending_queue.is_empty() {
            return None;
        }

        let num_consumers = self.consumer_order.len();
        for _ in 0..num_consumers {
            let idx = self.dispatch_index % num_consumers;
            self.dispatch_index = (self.dispatch_index + 1) % num_consumers;

            let consumer_id = self.consumer_order[idx];
            let consumer = self.consumers.get(&consumer_id)?;
            if consumer.can_accept() {
                if let Some(msg_id) = self.pending_queue.pop_front() {
                    self.consumers.get_mut(&consumer_id).unwrap().inflight_count += 1;

                    // Track cumulative delivery attempts
                    let attempt = self
                        .delivery_attempts
                        .entry(msg_id)
                        .and_modify(|a| *a += 1)
                        .or_insert(1);
                    let attempt_val = *attempt;

                    self.inflight.insert(
                        msg_id,
                        InflightEntry {
                            message_id: msg_id,
                            consumer_id,
                            dispatched_at: Utc::now(),
                            attempt: attempt_val,
                        },
                    );

                    return Some((consumer_id, msg_id));
                }
            }
        }
        None
    }

    /// Acknowledge a message. Returns true if the message was inflight.
    pub fn ack(&mut self, message_id: &MessageId) -> bool {
        if let Some(entry) = self.inflight.remove(message_id) {
            if let Some(c) = self.consumers.get_mut(&entry.consumer_id) {
                c.inflight_count = c.inflight_count.saturating_sub(1);
            }
            self.ack_set.insert(*message_id);
            self.delivery_attempts.remove(message_id);
            true
        } else {
            false
        }
    }

    /// NACK a message — put it back at the front of the pending queue.
    /// Does NOT clear delivery_attempts (preserves cumulative count).
    pub fn nack(&mut self, message_id: &MessageId) -> bool {
        if let Some(entry) = self.inflight.remove(message_id) {
            if let Some(c) = self.consumers.get_mut(&entry.consumer_id) {
                c.inflight_count = c.inflight_count.saturating_sub(1);
            }
            self.pending_queue.push_front(*message_id);
            true
        } else {
            false
        }
    }

    /// Check for timed-out inflight messages and return them to pending.
    /// Returns (consumer_id, message_id, attempt_count) for each timed-out message.
    pub fn check_timeouts(&mut self, timeout_secs: u64) -> Vec<(ConsumerId, MessageId, u32)> {
        let now = Utc::now();
        let mut timed_out = Vec::new();

        let expired: Vec<(MessageId, ConsumerId, u32)> = self
            .inflight
            .iter()
            .filter(|(_, entry)| {
                (now - entry.dispatched_at).num_seconds() >= timeout_secs as i64
            })
            .map(|(id, entry)| (*id, entry.consumer_id, entry.attempt))
            .collect();

        for (msg_id, consumer_id, attempt) in expired {
            self.inflight.remove(&msg_id);
            if let Some(c) = self.consumers.get_mut(&consumer_id) {
                c.inflight_count = c.inflight_count.saturating_sub(1);
            }
            self.pending_queue.push_front(msg_id);
            timed_out.push((consumer_id, msg_id, attempt));
        }

        timed_out
    }

    /// Remove a message from inflight without returning it to pending (for DLQ).
    pub fn remove_inflight(&mut self, message_id: &MessageId) -> bool {
        if let Some(entry) = self.inflight.remove(message_id) {
            if let Some(c) = self.consumers.get_mut(&entry.consumer_id) {
                c.inflight_count = c.inflight_count.saturating_sub(1);
            }
            self.delivery_attempts.remove(message_id);
            true
        } else {
            false
        }
    }

    /// Remove a message from the pending queue (for DLQ).
    pub fn remove_from_pending(&mut self, message_id: &MessageId) {
        self.pending_queue.retain(|id| id != message_id);
        self.delivery_attempts.remove(message_id);
    }

    /// Get the cumulative delivery attempt count for a message.
    pub fn delivery_attempt_count(&self, message_id: &MessageId) -> u32 {
        self.delivery_attempts.get(message_id).copied().unwrap_or(0)
    }

    pub fn is_acked(&self, message_id: &MessageId) -> bool {
        self.ack_set.contains(message_id)
    }

    /// H1: Remove a message ID from the ack_set (called when message is fully removed from channel).
    pub fn clear_ack(&mut self, message_id: &MessageId) {
        self.ack_set.remove(message_id);
    }

    /// C2: Set delivery attempts for a message (used during recovery).
    pub fn set_delivery_attempts(&mut self, message_id: MessageId, count: u32) {
        self.delivery_attempts.insert(message_id, count);
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    pub fn pending_count(&self) -> usize {
        self.pending_queue.len()
    }

    pub fn get_inflight_attempt(&self, message_id: &MessageId) -> Option<u32> {
        self.inflight.get(message_id).map(|e| e.attempt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::Consumer;
    use uuid::Uuid;

    fn make_consumer() -> Consumer {
        Consumer::new(Uuid::now_v7(), 100)
    }

    fn make_msg_id() -> MessageId {
        Uuid::now_v7()
    }

    // ---- Consumer add/remove ----

    #[test]
    fn add_consumer_increases_count() {
        let mut topic = Topic::new("test-topic");
        assert_eq!(topic.consumer_count(), 0);

        topic.add_consumer(make_consumer());
        assert_eq!(topic.consumer_count(), 1);

        topic.add_consumer(make_consumer());
        assert_eq!(topic.consumer_count(), 2);
    }

    #[test]
    fn remove_consumer_decreases_count() {
        let mut topic = Topic::new("test-topic");
        let c1 = make_consumer();
        let c2 = make_consumer();
        let c1_id = c1.id;

        topic.add_consumer(c1);
        topic.add_consumer(c2);
        assert_eq!(topic.consumer_count(), 2);

        topic.remove_consumer(c1_id);
        assert_eq!(topic.consumer_count(), 1);
    }

    #[test]
    fn remove_consumer_returns_inflight_to_pending() {
        let mut topic = Topic::new("test-topic");
        let c = make_consumer();
        let cid = c.id;
        topic.add_consumer(c);

        let msg = make_msg_id();
        topic.enqueue(msg);

        // Dispatch to the consumer so it becomes inflight
        let dispatched = topic.try_dispatch();
        assert!(dispatched.is_some());
        assert_eq!(topic.inflight_count(), 1);
        assert_eq!(topic.pending_count(), 0);

        // Remove the consumer — message should return to pending
        let returned = topic.remove_consumer(cid);
        assert_eq!(returned.len(), 1);
        assert_eq!(returned[0], msg);
        assert_eq!(topic.inflight_count(), 0);
        assert_eq!(topic.pending_count(), 1);
    }

    // ---- Enqueue and dispatch (round-robin across 2 consumers) ----

    #[test]
    fn enqueue_and_dispatch_round_robin() {
        let mut topic = Topic::new("rr-topic");
        let c1 = make_consumer();
        let c2 = make_consumer();
        let c1_id = c1.id;
        let c2_id = c2.id;

        topic.add_consumer(c1);
        topic.add_consumer(c2);

        let m1 = make_msg_id();
        let m2 = make_msg_id();
        let m3 = make_msg_id();
        let m4 = make_msg_id();
        topic.enqueue(m1);
        topic.enqueue(m2);
        topic.enqueue(m3);
        topic.enqueue(m4);

        let d1 = topic.try_dispatch().unwrap();
        let d2 = topic.try_dispatch().unwrap();
        let d3 = topic.try_dispatch().unwrap();
        let d4 = topic.try_dispatch().unwrap();

        // Messages should alternate between c1 and c2
        assert_eq!(d1.0, c1_id);
        assert_eq!(d1.1, m1);
        assert_eq!(d2.0, c2_id);
        assert_eq!(d2.1, m2);
        assert_eq!(d3.0, c1_id);
        assert_eq!(d3.1, m3);
        assert_eq!(d4.0, c2_id);
        assert_eq!(d4.1, m4);

        // No more pending messages
        assert!(topic.try_dispatch().is_none());
    }

    #[test]
    fn dispatch_returns_none_when_no_consumers() {
        let mut topic = Topic::new("empty");
        topic.enqueue(make_msg_id());
        assert!(topic.try_dispatch().is_none());
    }

    #[test]
    fn dispatch_returns_none_when_no_pending() {
        let mut topic = Topic::new("empty");
        topic.add_consumer(make_consumer());
        assert!(topic.try_dispatch().is_none());
    }

    // ---- ACK removes from inflight ----

    #[test]
    fn ack_removes_from_inflight() {
        let mut topic = Topic::new("ack-topic");
        topic.add_consumer(make_consumer());

        let msg = make_msg_id();
        topic.enqueue(msg);

        let (_, dispatched_msg) = topic.try_dispatch().unwrap();
        assert_eq!(topic.inflight_count(), 1);

        let result = topic.ack(&dispatched_msg);
        assert!(result);
        assert_eq!(topic.inflight_count(), 0);
    }

    #[test]
    fn ack_unknown_message_returns_false() {
        let mut topic = Topic::new("ack-topic");
        topic.add_consumer(make_consumer());
        let unknown = make_msg_id();
        assert!(!topic.ack(&unknown));
    }

    // ---- NACK returns to front of pending queue ----

    #[test]
    fn nack_returns_message_to_front_of_pending() {
        let mut topic = Topic::new("nack-topic");
        let c = make_consumer();
        let cid = c.id;
        topic.add_consumer(c);

        let m1 = make_msg_id();
        let m2 = make_msg_id();
        topic.enqueue(m1);
        topic.enqueue(m2);

        // Dispatch m1
        let (_, dispatched) = topic.try_dispatch().unwrap();
        assert_eq!(dispatched, m1);
        assert_eq!(topic.inflight_count(), 1);
        assert_eq!(topic.pending_count(), 1); // m2 still pending

        // NACK m1 — it should go to front of pending (before m2)
        let result = topic.nack(&m1);
        assert!(result);
        assert_eq!(topic.inflight_count(), 0);
        assert_eq!(topic.pending_count(), 2);

        // Next dispatch should return m1 again (it was pushed to front)
        let (consumer, redispatched) = topic.try_dispatch().unwrap();
        assert_eq!(consumer, cid);
        assert_eq!(redispatched, m1);
    }

    #[test]
    fn nack_unknown_message_returns_false() {
        let mut topic = Topic::new("nack-topic");
        topic.add_consumer(make_consumer());
        let unknown = make_msg_id();
        assert!(!topic.nack(&unknown));
    }

    // ---- Timeout detection returns messages to pending ----

    #[test]
    fn check_timeouts_returns_expired_to_pending() {
        let mut topic = Topic::new("timeout-topic");
        topic.add_consumer(make_consumer());

        let msg = make_msg_id();
        topic.enqueue(msg);
        topic.try_dispatch().unwrap();

        assert_eq!(topic.inflight_count(), 1);
        assert_eq!(topic.pending_count(), 0);

        // With timeout_secs = 0, the message should be considered expired immediately
        let timed_out = topic.check_timeouts(0);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].1, msg);
        assert_eq!(timed_out[0].2, 1); // attempt count
        assert_eq!(topic.inflight_count(), 0);
        assert_eq!(topic.pending_count(), 1);
    }

    #[test]
    fn check_timeouts_does_not_expire_fresh_messages() {
        let mut topic = Topic::new("timeout-topic");
        topic.add_consumer(make_consumer());

        let msg = make_msg_id();
        topic.enqueue(msg);
        topic.try_dispatch().unwrap();

        // With a very large timeout, nothing should expire
        let timed_out = topic.check_timeouts(999_999);
        assert!(timed_out.is_empty());
        assert_eq!(topic.inflight_count(), 1);
    }

    // ---- is_acked after ACK ----

    #[test]
    fn is_acked_true_after_ack() {
        let mut topic = Topic::new("acked-topic");
        topic.add_consumer(make_consumer());

        let msg = make_msg_id();
        topic.enqueue(msg);
        topic.try_dispatch().unwrap();

        assert!(!topic.is_acked(&msg));

        topic.ack(&msg);
        assert!(topic.is_acked(&msg));
    }

    #[test]
    fn enqueue_skips_already_acked_message() {
        let mut topic = Topic::new("acked-topic");
        topic.add_consumer(make_consumer());

        let msg = make_msg_id();
        topic.enqueue(msg);
        topic.try_dispatch().unwrap();
        topic.ack(&msg);

        // Enqueue same message again — should be skipped
        topic.enqueue(msg);
        assert_eq!(topic.pending_count(), 0);
    }

    // ---- Delivery attempts tracking ----

    #[test]
    fn delivery_attempts_accumulate_across_nack() {
        let mut topic = Topic::new("attempts-topic");
        let c = make_consumer();
        topic.add_consumer(c);

        let msg = make_msg_id();
        topic.enqueue(msg);

        // First dispatch: attempt 1
        topic.try_dispatch().unwrap();
        assert_eq!(topic.delivery_attempt_count(&msg), 1);

        // NACK preserves delivery_attempts
        topic.nack(&msg);
        assert_eq!(topic.delivery_attempt_count(&msg), 1);

        // Second dispatch: attempt 2
        topic.try_dispatch().unwrap();
        assert_eq!(topic.delivery_attempt_count(&msg), 2);

        // ACK clears delivery_attempts
        topic.ack(&msg);
        assert_eq!(topic.delivery_attempt_count(&msg), 0);
    }

    #[test]
    fn remove_inflight_for_dlq() {
        let mut topic = Topic::new("dlq-topic");
        let c = make_consumer();
        topic.add_consumer(c);

        let msg = make_msg_id();
        topic.enqueue(msg);
        topic.try_dispatch().unwrap();
        assert_eq!(topic.inflight_count(), 1);

        // remove_inflight should remove without adding to pending
        assert!(topic.remove_inflight(&msg));
        assert_eq!(topic.inflight_count(), 0);
        assert_eq!(topic.pending_count(), 0);
    }

    #[test]
    fn remove_from_pending_for_dlq() {
        let mut topic = Topic::new("dlq-topic");
        let msg = make_msg_id();
        topic.enqueue(msg);
        assert_eq!(topic.pending_count(), 1);

        topic.remove_from_pending(&msg);
        assert_eq!(topic.pending_count(), 0);
    }
}
