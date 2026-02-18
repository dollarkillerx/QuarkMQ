use crate::config::BrokerConfig;
use crate::error::Result;
use crate::metadata::ClusterMetadata;
use crate::topic_manager::TopicManager;

/// Top-level broker coordinator.
///
/// The Broker owns the TopicManager and ClusterMetadata, and provides
/// high-level request handling methods for produce, fetch, and list offsets.
pub struct Broker {
    pub config: BrokerConfig,
    pub topic_manager: TopicManager,
    pub metadata: ClusterMetadata,
}

impl Broker {
    /// Create a new Broker with the given configuration.
    pub fn new(config: BrokerConfig) -> Self {
        let topic_manager = TopicManager::new(config.clone());
        let metadata = ClusterMetadata::new(config.clone());
        Self {
            config,
            topic_manager,
            metadata,
        }
    }

    /// Start the broker by recovering state from disk.
    ///
    /// This scans the data directory for existing topic partitions and
    /// opens their CommitLogs.
    pub fn start(&self) -> Result<()> {
        self.topic_manager.recover()
    }

    /// Handle a produce request for a single partition.
    ///
    /// Appends the record batch to the specified topic partition and returns
    /// the base offset assigned to the batch.
    pub fn handle_produce(
        &self,
        topic: &str,
        partition: i32,
        batch: &mut [u8],
        record_count: i32,
    ) -> Result<i64> {
        let p = self.topic_manager.get_partition(topic, partition)?;
        let mut p = p.lock();
        let (base_offset, _) = p.append(batch, record_count)?;
        Ok(base_offset)
    }

    /// Handle a fetch request for a single partition.
    ///
    /// Returns `(records_bytes, high_watermark, log_start_offset)`.
    pub fn handle_fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, i64, i64)> {
        let p = self.topic_manager.get_partition(topic, partition)?;
        let mut p = p.lock();
        let data = p.fetch(offset, max_bytes)?;
        let hw = p.high_watermark();
        let log_start = p.earliest_offset();
        Ok((data, hw, log_start))
    }

    /// Handle a list offsets request for a single partition.
    ///
    /// Special timestamp values:
    /// - `-2`: earliest offset (LOG_START)
    /// - `-1`: latest offset (LOG_END / high watermark)
    /// - Other: currently returns earliest offset (timestamp lookup not yet implemented)
    pub fn handle_list_offsets(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        let p = self.topic_manager.get_partition(topic, partition)?;
        let p = p.lock();
        match timestamp {
            -2 => Ok(p.earliest_offset()),
            -1 => Ok(p.latest_offset()),
            _ => Ok(p.earliest_offset()), // TODO: timestamp-based offset lookup
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_broker(dir: &std::path::Path) -> Broker {
        let config = BrokerConfig {
            node_id: 0,
            data_dir: dir.to_string_lossy().to_string(),
            segment_bytes: 1024 * 1024,
            index_interval_bytes: 4096,
            default_num_partitions: 1,
            ..Default::default()
        };
        Broker::new(config)
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

    #[test]
    fn test_broker_start_empty() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();
        assert!(broker.topic_manager.list_topics().is_empty());
    }

    #[test]
    fn test_broker_produce() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        broker.topic_manager.create_topic("test-topic", 1).unwrap();

        let mut batch = make_batch(3);
        let base = broker
            .handle_produce("test-topic", 0, &mut batch, 3)
            .unwrap();
        assert_eq!(base, 0);

        let mut batch2 = make_batch(2);
        let base2 = broker
            .handle_produce("test-topic", 0, &mut batch2, 2)
            .unwrap();
        assert_eq!(base2, 3);
    }

    #[test]
    fn test_broker_produce_topic_not_found() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        let mut batch = make_batch(1);
        let err = broker
            .handle_produce("nonexistent", 0, &mut batch, 1)
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_broker_produce_partition_not_found() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        broker.topic_manager.create_topic("test-topic", 1).unwrap();

        let mut batch = make_batch(1);
        let err = broker
            .handle_produce("test-topic", 5, &mut batch, 1)
            .unwrap_err();
        assert!(err.to_string().contains("partition not found"));
    }

    #[test]
    fn test_broker_fetch() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        broker.topic_manager.create_topic("test-topic", 1).unwrap();

        // Produce some data.
        let mut batch = make_batch(3);
        broker
            .handle_produce("test-topic", 0, &mut batch, 3)
            .unwrap();

        // Fetch (read auto-flushes).
        let (data, hw, log_start) = broker
            .handle_fetch("test-topic", 0, 0, 4096)
            .unwrap();
        assert!(!data.is_empty());
        assert_eq!(hw, 3);
        assert_eq!(log_start, 0);
    }

    #[test]
    fn test_broker_fetch_topic_not_found() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        let err = broker
            .handle_fetch("nonexistent", 0, 0, 4096)
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_broker_list_offsets_earliest() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        broker.topic_manager.create_topic("test-topic", 1).unwrap();

        let mut batch = make_batch(5);
        broker
            .handle_produce("test-topic", 0, &mut batch, 5)
            .unwrap();

        let offset = broker
            .handle_list_offsets("test-topic", 0, -2)
            .unwrap();
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_broker_list_offsets_latest() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        broker.topic_manager.create_topic("test-topic", 1).unwrap();

        let mut batch = make_batch(5);
        broker
            .handle_produce("test-topic", 0, &mut batch, 5)
            .unwrap();

        let offset = broker
            .handle_list_offsets("test-topic", 0, -1)
            .unwrap();
        assert_eq!(offset, 5);
    }

    #[test]
    fn test_broker_list_offsets_not_found() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        let err = broker
            .handle_list_offsets("nonexistent", 0, -1)
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_broker_recovery() {
        let dir = tempdir().unwrap();

        // Create a broker, write data, drop it.
        {
            let broker = test_broker(dir.path());
            broker.start().unwrap();
            broker.topic_manager.create_topic("orders", 2).unwrap();

            let mut batch = make_batch(4);
            broker
                .handle_produce("orders", 0, &mut batch, 4)
                .unwrap();

            // Flush.
            let p = broker.topic_manager.get_partition("orders", 0).unwrap();
            p.lock().flush().unwrap();
        }

        // Create a new broker and recover.
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        assert!(broker.topic_manager.topic_exists("orders"));
        assert_eq!(broker.topic_manager.partition_count("orders"), Some(2));

        let offset = broker
            .handle_list_offsets("orders", 0, -1)
            .unwrap();
        assert_eq!(offset, 4);
    }

    #[test]
    fn test_broker_multiple_partitions() {
        let dir = tempdir().unwrap();
        let broker = test_broker(dir.path());
        broker.start().unwrap();

        broker.topic_manager.create_topic("test-topic", 3).unwrap();

        // Write to different partitions.
        let mut batch0 = make_batch(2);
        let base0 = broker
            .handle_produce("test-topic", 0, &mut batch0, 2)
            .unwrap();

        let mut batch1 = make_batch(3);
        let base1 = broker
            .handle_produce("test-topic", 1, &mut batch1, 3)
            .unwrap();

        let mut batch2 = make_batch(1);
        let base2 = broker
            .handle_produce("test-topic", 2, &mut batch2, 1)
            .unwrap();

        assert_eq!(base0, 0);
        assert_eq!(base1, 0);
        assert_eq!(base2, 0);

        // Offsets are per-partition.
        let latest0 = broker.handle_list_offsets("test-topic", 0, -1).unwrap();
        let latest1 = broker.handle_list_offsets("test-topic", 1, -1).unwrap();
        let latest2 = broker.handle_list_offsets("test-topic", 2, -1).unwrap();

        assert_eq!(latest0, 2);
        assert_eq!(latest1, 3);
        assert_eq!(latest2, 1);
    }
}
