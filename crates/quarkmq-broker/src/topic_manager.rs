use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::config::BrokerConfig;
use crate::error::{BrokerError, Result};
use crate::partition::Partition;

/// Holds all partitions for a single topic.
pub struct TopicState {
    pub partitions: Vec<Arc<Mutex<Partition>>>,
}

/// Manages all topics and their partitions.
///
/// Thread-safe: uses `DashMap` for per-topic locking so operations on
/// different topics do not contend.
pub struct TopicManager {
    topics: DashMap<String, TopicState>,
    config: BrokerConfig,
    data_dir: PathBuf,
}

impl TopicManager {
    /// Create a new, empty TopicManager.
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            data_dir: PathBuf::from(&config.data_dir),
            topics: DashMap::new(),
            config,
        }
    }

    /// Create a new topic with the given number of partitions.
    ///
    /// Each partition is stored in a directory named `{topic}-{partition_id}`
    /// under the data directory.
    pub fn create_topic(&self, name: &str, num_partitions: i32) -> Result<()> {
        if self.topics.contains_key(name) {
            return Err(BrokerError::TopicAlreadyExists(name.to_string()));
        }

        let mut partitions = Vec::new();
        for i in 0..num_partitions {
            let dir = self.partition_dir(name, i);
            let partition = Partition::new(name, i, &dir, &self.config)?;
            partitions.push(Arc::new(Mutex::new(partition)));
        }

        self.topics.insert(name.to_string(), TopicState { partitions });
        Ok(())
    }

    /// Delete a topic and remove it from the in-memory map.
    ///
    /// Note: this removes the topic from the manager but only makes a
    /// best-effort attempt to remove the on-disk data directory.
    pub fn delete_topic(&self, name: &str) -> Result<()> {
        if self.topics.remove(name).is_none() {
            return Err(BrokerError::TopicNotFound(name.to_string()));
        }
        // Best-effort cleanup of data directories.
        // We try to remove each partition directory for this topic.
        // Since we don't know the partition count anymore, we scan for matching dirs.
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().to_string();
                if let Some(last_dash) = fname.rfind('-') {
                    let topic_part = &fname[..last_dash];
                    if topic_part == name {
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                }
            }
        }
        Ok(())
    }

    /// Get a handle to a specific partition by topic name and partition id.
    pub fn get_partition(&self, topic: &str, partition: i32) -> Result<Arc<Mutex<Partition>>> {
        let entry = self
            .topics
            .get(topic)
            .ok_or_else(|| BrokerError::TopicNotFound(topic.to_string()))?;
        entry
            .partitions
            .get(partition as usize)
            .cloned()
            .ok_or_else(|| BrokerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })
    }

    /// Get the number of partitions for a topic, or `None` if the topic does not exist.
    pub fn partition_count(&self, topic: &str) -> Option<i32> {
        self.topics.get(topic).map(|t| t.partitions.len() as i32)
    }

    /// List all topic names.
    pub fn list_topics(&self) -> Vec<String> {
        self.topics.iter().map(|e| e.key().clone()).collect()
    }

    /// Check if a topic exists.
    pub fn topic_exists(&self, topic: &str) -> bool {
        self.topics.contains_key(topic)
    }

    /// Recover topics from the data directory at startup.
    ///
    /// Scans for directories matching the `{topic_name}-{partition_id}` pattern
    /// and opens each partition's CommitLog. The partition id is parsed from the
    /// last segment after the last '-' character in the directory name.
    pub fn recover(&self) -> Result<()> {
        if !self.data_dir.exists() {
            std::fs::create_dir_all(&self.data_dir)?;
            return Ok(());
        }

        let mut topic_partitions: std::collections::HashMap<String, Vec<(i32, PathBuf)>> =
            std::collections::HashMap::new();

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            // Parse "{topic}-{partition_id}" where partition_id is the last segment after '-'.
            if let Some(last_dash) = name.rfind('-') {
                let topic = &name[..last_dash];
                if let Ok(partition_id) = name[last_dash + 1..].parse::<i32>() {
                    topic_partitions
                        .entry(topic.to_string())
                        .or_default()
                        .push((partition_id, entry.path()));
                }
            }
        }

        for (topic, mut parts) in topic_partitions {
            parts.sort_by_key(|(id, _)| *id);
            let mut partitions = Vec::new();
            for (id, path) in parts {
                let partition = Partition::open(&topic, id, &path, &self.config)?;
                partitions.push(Arc::new(Mutex::new(partition)));
            }
            self.topics.insert(topic, TopicState { partitions });
        }

        Ok(())
    }

    /// Get the on-disk directory path for a specific partition.
    fn partition_dir(&self, topic: &str, partition_id: i32) -> PathBuf {
        self.data_dir.join(format!("{}-{}", topic, partition_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn test_config(dir: &Path) -> BrokerConfig {
        BrokerConfig {
            node_id: 0,
            data_dir: dir.to_string_lossy().to_string(),
            segment_bytes: 1024 * 1024,
            index_interval_bytes: 4096,
            default_num_partitions: 1,
        }
    }

    #[test]
    fn test_create_topic() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        tm.create_topic("my-topic", 3).unwrap();
        assert!(tm.topic_exists("my-topic"));
        assert_eq!(tm.partition_count("my-topic"), Some(3));
    }

    #[test]
    fn test_create_topic_already_exists() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        tm.create_topic("my-topic", 1).unwrap();
        let err = tm.create_topic("my-topic", 1).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_delete_topic() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        tm.create_topic("my-topic", 2).unwrap();
        assert!(tm.topic_exists("my-topic"));

        tm.delete_topic("my-topic").unwrap();
        assert!(!tm.topic_exists("my-topic"));
    }

    #[test]
    fn test_delete_topic_not_found() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        let err = tm.delete_topic("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_get_partition() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        tm.create_topic("my-topic", 3).unwrap();

        // Valid partition.
        let p = tm.get_partition("my-topic", 0);
        assert!(p.is_ok());

        let p = tm.get_partition("my-topic", 2);
        assert!(p.is_ok());

        // Invalid partition index.
        let err = tm.get_partition("my-topic", 3).unwrap_err();
        assert!(err.to_string().contains("partition not found"));

        // Invalid topic.
        let err = tm.get_partition("nonexistent", 0).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_list_topics() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        tm.create_topic("topic-a", 1).unwrap();
        tm.create_topic("topic-b", 2).unwrap();

        let mut topics = tm.list_topics();
        topics.sort();
        assert_eq!(topics, vec!["topic-a", "topic-b"]);
    }

    #[test]
    fn test_partition_count_nonexistent() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        assert_eq!(tm.partition_count("nonexistent"), None);
    }

    #[test]
    fn test_recover_empty_dir() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        // Should succeed with no topics recovered.
        tm.recover().unwrap();
        assert!(tm.list_topics().is_empty());
    }

    #[test]
    fn test_recover_nonexistent_dir() {
        let dir = tempdir().unwrap();
        let non_existent = dir.path().join("does-not-exist");
        let config = test_config(&non_existent);
        let tm = TopicManager::new(config);

        // Should create the directory and succeed.
        tm.recover().unwrap();
        assert!(non_existent.exists());
        assert!(tm.list_topics().is_empty());
    }

    #[test]
    fn test_recover_with_data() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());

        // First, create some topics and write data.
        {
            let tm = TopicManager::new(config.clone());
            tm.create_topic("orders", 2).unwrap();
            tm.create_topic("events", 1).unwrap();

            // Write a record to orders partition 0.
            let p = tm.get_partition("orders", 0).unwrap();
            let mut p = p.lock();
            let mut batch = make_batch(3);
            p.append(&mut batch, 3).unwrap();
            p.flush().unwrap();
        }

        // Now recover.
        let tm = TopicManager::new(config);
        tm.recover().unwrap();

        assert!(tm.topic_exists("orders"));
        assert!(tm.topic_exists("events"));
        assert_eq!(tm.partition_count("orders"), Some(2));
        assert_eq!(tm.partition_count("events"), Some(1));

        // Verify the recovered partition has the correct offset.
        let p = tm.get_partition("orders", 0).unwrap();
        let p = p.lock();
        assert_eq!(p.latest_offset(), 3);
    }

    #[test]
    fn test_write_and_read_through_topic_manager() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let tm = TopicManager::new(config);

        tm.create_topic("test", 1).unwrap();

        let p = tm.get_partition("test", 0).unwrap();
        {
            let mut p = p.lock();
            let mut batch = make_batch(5);
            let (base, next) = p.append(&mut batch, 5).unwrap();
            p.flush().unwrap();
            assert_eq!(base, 0);
            assert_eq!(next, 5);
        }

        {
            let mut p = p.lock();
            let data = p.fetch(0, 4096).unwrap();
            assert!(!data.is_empty());
            assert_eq!(p.latest_offset(), 5);
        }
    }

    /// Build a minimal valid RecordBatch for testing.
    fn make_batch(record_count: i32) -> Vec<u8> {
        let batch_length: i32 = 49;
        let total = 12 + batch_length as usize;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
        buf[16] = 2; // magic
        buf[25..33].copy_from_slice(&1000i64.to_be_bytes());
        buf[33..41].copy_from_slice(&1000i64.to_be_bytes());
        buf[57..61].copy_from_slice(&record_count.to_be_bytes());
        buf
    }
}
