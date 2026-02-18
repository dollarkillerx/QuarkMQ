use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::segment::{self, Segment};

/// A CommitLog manages an ordered sequence of Segments for a single partition.
///
/// Segments are rolled when they reach `max_segment_bytes`. The CommitLog
/// provides a unified view of all segments via `append` and `read`.
pub struct CommitLog {
    dir: PathBuf,
    segments: Vec<Segment>,
    max_segment_bytes: u64,
    index_interval_bytes: u64,
}

impl CommitLog {
    /// Create a new, empty CommitLog.
    pub fn new(dir: &Path, max_segment_bytes: u64, index_interval_bytes: u64) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let first_segment = Segment::new(dir, 0, index_interval_bytes)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            segments: vec![first_segment],
            max_segment_bytes,
            index_interval_bytes,
        })
    }

    /// Open an existing CommitLog, recovering all segments from disk.
    pub fn open(dir: &Path, max_segment_bytes: u64, index_interval_bytes: u64) -> Result<Self> {
        if !dir.exists() {
            return Self::new(dir, max_segment_bytes, index_interval_bytes);
        }

        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".log") {
                if let Some(offset) = segment::parse_base_offset(&name) {
                    base_offsets.push(offset);
                }
            }
        }

        base_offsets.sort();

        if base_offsets.is_empty() {
            return Self::new(dir, max_segment_bytes, index_interval_bytes);
        }

        let mut segments = Vec::new();
        for base_offset in base_offsets {
            let seg = Segment::open(dir, base_offset, index_interval_bytes)?;
            segments.push(seg);
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            segments,
            max_segment_bytes,
            index_interval_bytes,
        })
    }

    /// Append a record batch. Returns `(base_offset, next_offset)`.
    pub fn append(&mut self, batch: &mut [u8], record_count: i32) -> Result<(i64, i64)> {
        // Check if we need to roll to a new segment.
        if let Some(active) = self.segments.last() {
            if active.size() >= self.max_segment_bytes && active.size() > 0 {
                let new_base = active.next_offset();
                let new_seg = Segment::new(&self.dir, new_base, self.index_interval_bytes)?;
                self.segments.push(new_seg);
            }
        }

        let active = self.segments.last_mut()
            .ok_or_else(|| StorageError::Corrupt("no active segment".into()))?;
        active.append(batch, record_count)
    }

    /// Read raw bytes starting from `start_offset`, up to `max_bytes`.
    pub fn read(&mut self, start_offset: i64, max_bytes: usize) -> Result<Vec<u8>> {
        // Find the segment containing start_offset.
        let seg_idx = self.find_segment(start_offset)?;
        let mut result = Vec::new();
        let mut remaining_bytes = max_bytes;

        for seg in &mut self.segments[seg_idx..] {
            if remaining_bytes == 0 {
                break;
            }
            let offset = if result.is_empty() {
                start_offset
            } else {
                seg.base_offset()
            };
            if offset >= seg.next_offset() {
                continue;
            }
            match seg.read(offset, remaining_bytes) {
                Ok(data) => {
                    remaining_bytes = remaining_bytes.saturating_sub(data.len());
                    result.extend(data);
                }
                Err(StorageError::OffsetOutOfRange(_)) if !result.is_empty() => break,
                Err(e) => return Err(e),
            }
        }

        if result.is_empty() {
            return Err(StorageError::OffsetOutOfRange(start_offset));
        }

        Ok(result)
    }

    /// Earliest offset across all segments.
    pub fn earliest_offset(&self) -> i64 {
        self.segments.first().map(|s| s.base_offset()).unwrap_or(0)
    }

    /// Latest offset (log end offset) - one past the last written record.
    pub fn latest_offset(&self) -> i64 {
        self.segments.last().map(|s| s.next_offset()).unwrap_or(0)
    }

    /// Flush all segments to disk.
    pub fn flush(&mut self) -> Result<()> {
        for seg in &mut self.segments {
            seg.flush()?;
        }
        Ok(())
    }

    /// Find the segment index that contains the given offset.
    fn find_segment(&self, offset: i64) -> Result<usize> {
        if self.segments.is_empty() {
            return Err(StorageError::OffsetOutOfRange(offset));
        }
        // Binary search: find the last segment whose base_offset <= offset.
        let mut lo = 0;
        let mut hi = self.segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.segments[mid].base_offset() <= offset {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return Err(StorageError::OffsetOutOfRange(offset));
        }
        Ok(lo - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_batch;
    use tempfile::tempdir;

    fn make_batch(record_count: i32) -> Vec<u8> {
        let batch_length: i32 = 49;
        let total = 12 + batch_length as usize;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
        buf[16] = 2; // magic
        buf[25..33].copy_from_slice(&1000i64.to_be_bytes()); // first_timestamp
        buf[33..41].copy_from_slice(&1000i64.to_be_bytes()); // max_timestamp
        buf[57..61].copy_from_slice(&record_count.to_be_bytes());
        buf
    }

    #[test]
    fn test_new_commit_log() {
        let dir = tempdir().unwrap();
        let log = CommitLog::new(dir.path(), 1024, 4096).unwrap();
        assert_eq!(log.earliest_offset(), 0);
        assert_eq!(log.latest_offset(), 0);
    }

    #[test]
    fn test_append_and_read() {
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(dir.path(), 1024 * 1024, 4096).unwrap();

        let mut batch = make_batch(3);
        let (base, next) = log.append(&mut batch, 3).unwrap();
        log.flush().unwrap();

        assert_eq!(base, 0);
        assert_eq!(next, 3);
        assert_eq!(log.latest_offset(), 3);

        let data = log.read(0, 4096).unwrap();
        assert_eq!(data.len(), 61);
        assert_eq!(record_batch::get_batch_base_offset(&data), 0);
    }

    #[test]
    fn test_multiple_appends() {
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(dir.path(), 1024 * 1024, 4096).unwrap();

        let mut batch1 = make_batch(2);
        let (b1, n1) = log.append(&mut batch1, 2).unwrap();
        assert_eq!(b1, 0);
        assert_eq!(n1, 2);

        let mut batch2 = make_batch(3);
        let (b2, n2) = log.append(&mut batch2, 3).unwrap();
        assert_eq!(b2, 2);
        assert_eq!(n2, 5);

        log.flush().unwrap();
        assert_eq!(log.latest_offset(), 5);
    }

    #[test]
    fn test_segment_rolling() {
        let dir = tempdir().unwrap();
        // Use a small max_segment_bytes to force rolling.
        let mut log = CommitLog::new(dir.path(), 61, 4096).unwrap();

        let mut batch1 = make_batch(2);
        log.append(&mut batch1, 2).unwrap();

        // This should trigger a new segment since the first segment is now >= 61 bytes.
        let mut batch2 = make_batch(3);
        let (base, next) = log.append(&mut batch2, 3).unwrap();
        log.flush().unwrap();

        assert_eq!(base, 2);
        assert_eq!(next, 5);
        assert_eq!(log.segments.len(), 2);
    }

    #[test]
    fn test_open_recovery() {
        let dir = tempdir().unwrap();

        {
            let mut log = CommitLog::new(dir.path(), 1024 * 1024, 4096).unwrap();
            let mut batch1 = make_batch(3);
            log.append(&mut batch1, 3).unwrap();
            let mut batch2 = make_batch(2);
            log.append(&mut batch2, 2).unwrap();
            log.flush().unwrap();
        }

        let mut log = CommitLog::open(dir.path(), 1024 * 1024, 4096).unwrap();
        assert_eq!(log.earliest_offset(), 0);
        assert_eq!(log.latest_offset(), 5);

        let data = log.read(0, 4096).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_recovery_with_multiple_segments() {
        let dir = tempdir().unwrap();

        {
            // max_segment_bytes = 61 forces rolling after each batch.
            let mut log = CommitLog::new(dir.path(), 61, 4096).unwrap();
            let mut batch1 = make_batch(2);
            log.append(&mut batch1, 2).unwrap();
            let mut batch2 = make_batch(3);
            log.append(&mut batch2, 3).unwrap();
            let mut batch3 = make_batch(1);
            log.append(&mut batch3, 1).unwrap();
            log.flush().unwrap();
            assert_eq!(log.segments.len(), 3);
        }

        let mut log = CommitLog::open(dir.path(), 61, 4096).unwrap();
        assert_eq!(log.segments.len(), 3);
        assert_eq!(log.earliest_offset(), 0);
        assert_eq!(log.latest_offset(), 6);

        let data = log.read(2, 4096).unwrap();
        assert_eq!(record_batch::get_batch_base_offset(&data), 2);

        let data = log.read(5, 4096).unwrap();
        assert_eq!(record_batch::get_batch_base_offset(&data), 5);
    }

    #[test]
    fn test_open_empty_directory() {
        let dir = tempdir().unwrap();
        let empty_dir = dir.path().join("empty-partition");

        let log = CommitLog::open(&empty_dir, 1024, 4096).unwrap();
        assert_eq!(log.segments.len(), 1);
        assert_eq!(log.earliest_offset(), 0);
        assert_eq!(log.latest_offset(), 0);
    }

    #[test]
    fn test_read_offset_out_of_range() {
        let dir = tempdir().unwrap();
        let mut log = CommitLog::new(dir.path(), 1024, 4096).unwrap();

        let mut batch = make_batch(3);
        log.append(&mut batch, 3).unwrap();
        log.flush().unwrap();

        assert!(log.read(3, 4096).is_err());
        assert!(log.read(-1, 4096).is_err());
    }

    #[test]
    fn test_append_after_recovery() {
        let dir = tempdir().unwrap();

        {
            let mut log = CommitLog::new(dir.path(), 1024, 4096).unwrap();
            let mut batch = make_batch(3);
            log.append(&mut batch, 3).unwrap();
            log.flush().unwrap();
        }

        let mut log = CommitLog::open(dir.path(), 1024, 4096).unwrap();
        let mut batch = make_batch(2);
        let (base, next) = log.append(&mut batch, 2).unwrap();
        log.flush().unwrap();

        assert_eq!(base, 3);
        assert_eq!(next, 5);
        assert_eq!(log.latest_offset(), 5);
    }
}
