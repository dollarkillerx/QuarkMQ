use std::collections::HashSet;

use quarkmq_protocol::MessageId;

use crate::index::MessageIndex;
use crate::segment::SegmentManager;
use crate::StorageError;

/// Garbage collector that removes fully-ACKed messages from storage.
pub struct GarbageCollector;

impl GarbageCollector {
    /// Remove messages that have been ACKed by all topics.
    /// `acked_messages` should contain message IDs that are fully ACKed.
    /// After removing index entries, checks for empty segments and deletes their files.
    pub fn collect(
        acked_messages: &HashSet<MessageId>,
        index: &mut MessageIndex,
        segments: &mut SegmentManager,
    ) -> Result<GcStats, StorageError> {
        let mut removed_count = 0;
        let mut affected_segments = HashSet::new();

        for msg_id in acked_messages {
            if let Some(loc) = index.remove(msg_id) {
                removed_count += 1;
                affected_segments.insert(loc.segment_id);
            }
        }

        // Check if any affected segments are now empty and can be reclaimed
        let mut segments_removed = 0;
        for segment_id in affected_segments {
            if index.messages_in_segment(segment_id).is_empty() {
                segments.remove_segment(segment_id)?;
                segments_removed += 1;
            }
        }

        Ok(GcStats {
            messages_removed: removed_count,
            segments_removed,
        })
    }
}

#[derive(Debug, Clone)]
pub struct GcStats {
    pub messages_removed: usize,
    pub segments_removed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_gc_removes_acked_from_index() {
        let mut index = MessageIndex::new();
        let id1 = Uuid::now_v7();
        let id2 = Uuid::now_v7();
        let id3 = Uuid::now_v7();

        index.insert(id1, 1, 0);
        index.insert(id2, 1, 100);
        index.insert(id3, 2, 0);

        let mut acked = HashSet::new();
        acked.insert(id1);
        acked.insert(id2);

        let dir = tempfile::tempdir().unwrap();
        let mut segments = SegmentManager::open(dir.path(), 1).unwrap();

        let stats = GarbageCollector::collect(&acked, &mut index, &mut segments).unwrap();
        assert_eq!(stats.messages_removed, 2);
        assert!(!index.contains(&id1));
        assert!(!index.contains(&id2));
        assert!(index.contains(&id3));
    }
}
