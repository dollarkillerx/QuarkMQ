use std::collections::HashMap;

use quarkmq_protocol::MessageId;

/// Location of a message within storage.
#[derive(Debug, Clone, Copy)]
pub struct MessageLocation {
    pub segment_id: u64,
    pub offset: u64,
}

/// In-memory index mapping message IDs to their storage location.
/// Rebuilt from segments/WAL on startup.
pub struct MessageIndex {
    entries: HashMap<MessageId, MessageLocation>,
}

impl MessageIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, message_id: MessageId, segment_id: u64, offset: u64) {
        self.entries.insert(
            message_id,
            MessageLocation { segment_id, offset },
        );
    }

    pub fn get(&self, message_id: &MessageId) -> Option<&MessageLocation> {
        self.entries.get(message_id)
    }

    pub fn remove(&mut self, message_id: &MessageId) -> Option<MessageLocation> {
        self.entries.remove(message_id)
    }

    pub fn contains(&self, message_id: &MessageId) -> bool {
        self.entries.contains_key(message_id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all message IDs stored in a specific segment.
    pub fn messages_in_segment(&self, segment_id: u64) -> Vec<MessageId> {
        self.entries
            .iter()
            .filter(|(_, loc)| loc.segment_id == segment_id)
            .map(|(id, _)| *id)
            .collect()
    }
}

impl Default for MessageIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_insert_and_get() {
        let mut index = MessageIndex::new();
        let id = Uuid::now_v7();
        index.insert(id, 1, 0);

        let loc = index.get(&id).unwrap();
        assert_eq!(loc.segment_id, 1);
        assert_eq!(loc.offset, 0);
    }

    #[test]
    fn test_remove() {
        let mut index = MessageIndex::new();
        let id = Uuid::now_v7();
        index.insert(id, 1, 0);
        assert!(index.contains(&id));

        index.remove(&id);
        assert!(!index.contains(&id));
    }

    #[test]
    fn test_messages_in_segment() {
        let mut index = MessageIndex::new();
        let id1 = Uuid::now_v7();
        let id2 = Uuid::now_v7();
        let id3 = Uuid::now_v7();

        index.insert(id1, 1, 0);
        index.insert(id2, 1, 100);
        index.insert(id3, 2, 0);

        let seg1_msgs = index.messages_in_segment(1);
        assert_eq!(seg1_msgs.len(), 2);
        assert!(seg1_msgs.contains(&id1));
        assert!(seg1_msgs.contains(&id2));

        let seg2_msgs = index.messages_in_segment(2);
        assert_eq!(seg2_msgs.len(), 1);
        assert!(seg2_msgs.contains(&id3));
    }
}
