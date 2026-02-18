use std::path::Path;

use quarkmq_storage::CommitLog;

use crate::config::BrokerConfig;
use crate::error::Result;

/// A single partition of a topic, backed by a CommitLog.
///
/// Each partition is an ordered, immutable sequence of record batches.
/// The partition assigns monotonically increasing offsets to appended batches.
pub struct Partition {
    pub topic: String,
    pub id: i32,
    log: CommitLog,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Partition {
    /// Create a new partition, initializing a fresh CommitLog at the given directory.
    pub fn new(topic: &str, id: i32, dir: &Path, config: &BrokerConfig) -> Result<Self> {
        let log = CommitLog::new(dir, config.segment_bytes, config.index_interval_bytes)?;
        Ok(Self {
            topic: topic.to_string(),
            id,
            log,
        })
    }

    /// Open an existing partition, recovering its CommitLog from disk.
    pub fn open(topic: &str, id: i32, dir: &Path, config: &BrokerConfig) -> Result<Self> {
        let log = CommitLog::open(dir, config.segment_bytes, config.index_interval_bytes)?;
        Ok(Self {
            topic: topic.to_string(),
            id,
            log,
        })
    }

    /// Append a record batch to this partition.
    ///
    /// The batch bytes will have their base_offset rewritten by the underlying
    /// CommitLog. Returns `(base_offset, next_offset)`.
    pub fn append(&mut self, batch: &mut [u8], record_count: i32) -> Result<(i64, i64)> {
        Ok(self.log.append(batch, record_count)?)
    }

    /// Fetch raw record batch bytes starting from the given offset.
    ///
    /// Returns up to `max_bytes` of data. The returned bytes contain one or
    /// more complete RecordBatch structures.
    pub fn fetch(&mut self, offset: i64, max_bytes: usize) -> Result<Vec<u8>> {
        // If offset equals latest_offset, no data available yet - return empty.
        if offset >= self.log.latest_offset() {
            return Ok(Vec::new());
        }
        Ok(self.log.read(offset, max_bytes)?)
    }

    /// The earliest offset available in this partition (log start offset).
    pub fn earliest_offset(&self) -> i64 {
        self.log.earliest_offset()
    }

    /// The latest offset (log end offset). This is one past the last written record.
    ///
    /// In single-node mode, the high watermark equals the log end offset.
    pub fn latest_offset(&self) -> i64 {
        self.log.latest_offset()
    }

    /// The high watermark for this partition.
    ///
    /// In single-node mode this equals the latest offset (LEO), since all
    /// written data is immediately considered committed.
    pub fn high_watermark(&self) -> i64 {
        self.latest_offset()
    }

    /// Flush all buffered data to disk.
    pub fn flush(&mut self) -> Result<()> {
        Ok(self.log.flush()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BrokerConfig;
    use tempfile::tempdir;

    /// Build a minimal valid RecordBatch for testing.
    fn make_batch(record_count: i32) -> Vec<u8> {
        let batch_length: i32 = 49; // minimum: 61 header - 12 prefix
        let total = 12 + batch_length as usize;
        let mut buf = vec![0u8; total];
        // base_offset: bytes 0..8 (will be overwritten by CommitLog)
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        // batch_length: bytes 8..12
        buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
        // magic: byte 16
        buf[16] = 2;
        // first_timestamp: bytes 25..33
        buf[25..33].copy_from_slice(&1000i64.to_be_bytes());
        // max_timestamp: bytes 33..41
        buf[33..41].copy_from_slice(&1000i64.to_be_bytes());
        // record_count: bytes 57..61
        buf[57..61].copy_from_slice(&record_count.to_be_bytes());
        buf
    }

    fn test_config(dir: &std::path::Path) -> BrokerConfig {
        BrokerConfig {
            node_id: 0,
            data_dir: dir.to_string_lossy().to_string(),
            segment_bytes: 1024 * 1024,
            index_interval_bytes: 4096,
            default_num_partitions: 1,
        }
    }

    #[test]
    fn test_new_partition() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let part_dir = dir.path().join("test-topic-0");

        let partition = Partition::new("test-topic", 0, &part_dir, &config).unwrap();
        assert_eq!(partition.topic, "test-topic");
        assert_eq!(partition.id, 0);
        assert_eq!(partition.earliest_offset(), 0);
        assert_eq!(partition.latest_offset(), 0);
        assert_eq!(partition.high_watermark(), 0);
    }

    #[test]
    fn test_append_and_fetch() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let part_dir = dir.path().join("test-topic-0");

        let mut partition = Partition::new("test-topic", 0, &part_dir, &config).unwrap();

        let mut batch = make_batch(3);
        let (base, next) = partition.append(&mut batch, 3).unwrap();
        partition.flush().unwrap();

        assert_eq!(base, 0);
        assert_eq!(next, 3);
        assert_eq!(partition.latest_offset(), 3);

        let data = partition.fetch(0, 4096).unwrap();
        assert_eq!(data.len(), 61);
    }

    #[test]
    fn test_multiple_appends() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let part_dir = dir.path().join("test-topic-0");

        let mut partition = Partition::new("test-topic", 0, &part_dir, &config).unwrap();

        let mut batch1 = make_batch(2);
        let (b1, n1) = partition.append(&mut batch1, 2).unwrap();
        assert_eq!(b1, 0);
        assert_eq!(n1, 2);

        let mut batch2 = make_batch(3);
        let (b2, n2) = partition.append(&mut batch2, 3).unwrap();
        assert_eq!(b2, 2);
        assert_eq!(n2, 5);

        partition.flush().unwrap();
        assert_eq!(partition.latest_offset(), 5);
        assert_eq!(partition.earliest_offset(), 0);
    }

    #[test]
    fn test_offsets() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let part_dir = dir.path().join("test-topic-0");

        let mut partition = Partition::new("test-topic", 0, &part_dir, &config).unwrap();

        // Empty partition.
        assert_eq!(partition.earliest_offset(), 0);
        assert_eq!(partition.latest_offset(), 0);
        assert_eq!(partition.high_watermark(), 0);

        // After appending.
        let mut batch = make_batch(5);
        partition.append(&mut batch, 5).unwrap();

        assert_eq!(partition.earliest_offset(), 0);
        assert_eq!(partition.latest_offset(), 5);
        assert_eq!(partition.high_watermark(), 5);
    }

    #[test]
    fn test_open_existing_partition() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let part_dir = dir.path().join("test-topic-0");

        // Create and write data.
        {
            let mut partition = Partition::new("test-topic", 0, &part_dir, &config).unwrap();
            let mut batch = make_batch(3);
            partition.append(&mut batch, 3).unwrap();
            partition.flush().unwrap();
        }

        // Re-open and verify.
        let mut partition = Partition::open("test-topic", 0, &part_dir, &config).unwrap();
        assert_eq!(partition.topic, "test-topic");
        assert_eq!(partition.id, 0);
        assert_eq!(partition.earliest_offset(), 0);
        assert_eq!(partition.latest_offset(), 3);

        let data = partition.fetch(0, 4096).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_flush() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let part_dir = dir.path().join("test-topic-0");

        let mut partition = Partition::new("test-topic", 0, &part_dir, &config).unwrap();
        let mut batch = make_batch(1);
        partition.append(&mut batch, 1).unwrap();

        // flush should not error.
        assert!(partition.flush().is_ok());
    }
}
