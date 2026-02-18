use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type MessageId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub id: MessageId,
    pub channel: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub attempt: u32,
}

impl Message {
    pub fn new(channel: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::now_v7(),
            channel: channel.into(),
            payload,
            created_at: Utc::now(),
            attempt: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_message_new_sets_fields_correctly() {
        let payload = json!({"key": "value"});
        let msg = Message::new("test-channel", payload.clone());

        assert_eq!(msg.channel, "test-channel");
        assert_eq!(msg.payload, payload);
        assert_eq!(msg.attempt, 0);
        // id should be a valid UUIDv7 (version nibble == 7)
        assert_eq!(msg.id.get_version_num(), 7);
        // created_at should be very recent
        let elapsed = Utc::now() - msg.created_at;
        assert!(elapsed.num_seconds() < 1);
    }

    #[test]
    fn test_message_new_accepts_into_string() {
        let msg = Message::new(String::from("owned-channel"), json!(null));
        assert_eq!(msg.channel, "owned-channel");
    }

    #[test]
    fn test_message_serialization_roundtrip() {
        let original = Message::new("roundtrip-ch", json!({"num": 42, "nested": [1, 2, 3]}));

        let serialized = serde_json::to_string(&original).expect("serialize");
        let deserialized: Message = serde_json::from_str(&serialized).expect("deserialize");

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_message_serialization_contains_expected_keys() {
        let msg = Message::new("ch", json!("hello"));
        let value: serde_json::Value = serde_json::to_value(&msg).expect("to_value");
        let obj = value.as_object().expect("should be object");

        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("channel"));
        assert!(obj.contains_key("payload"));
        assert!(obj.contains_key("created_at"));
        assert!(obj.contains_key("attempt"));
    }

    #[test]
    fn test_uuidv7_ordering() {
        let msg1 = Message::new("ch", json!(1));
        // UUIDv7 embeds a timestamp; sequential creation should yield ordered ids
        let msg2 = Message::new("ch", json!(2));

        assert!(
            msg1.id < msg2.id,
            "Expected id1 ({}) < id2 ({})",
            msg1.id,
            msg2.id
        );
    }
}
