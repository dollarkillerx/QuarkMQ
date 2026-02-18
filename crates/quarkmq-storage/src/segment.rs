use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::offset_index::OffsetIndex;
use crate::record_batch;
use crate::time_index::TimeIndex;

/// A Segment manages a single .log file and its .index and .timeindex files.
///
/// The .log file stores raw RecordBatch bytes sequentially. The .index and
/// .timeindex files are sparse indexes that map offsets and timestamps to
/// file positions in the .log file.
pub struct Segment {
    base_offset: i64,
    next_offset: i64,
    dir: PathBuf,
    writer: BufWriter<File>,
    size: u64,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    bytes_since_last_index: u64,
    index_interval_bytes: u64,
}

impl Segment {
    /// Create a new empty segment at `base_offset` within `dir`.
    pub fn new(dir: &Path, base_offset: i64, index_interval_bytes: u64) -> Result<Self> {
        let log_path = dir.join(segment_file_name(base_offset, "log"));
        let index_path = dir.join(segment_file_name(base_offset, "index"));
        let timeindex_path = dir.join(segment_file_name(base_offset, "timeindex"));

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .append(false)
            .open(&log_path)?;
        let writer = BufWriter::new(file);

        let offset_index = OffsetIndex::new(&index_path, base_offset)?;
        let time_index = TimeIndex::new(&timeindex_path, base_offset)?;

        Ok(Self {
            base_offset,
            next_offset: base_offset,
            dir: dir.to_path_buf(),
            writer,
            size: 0,
            offset_index,
            time_index,
            bytes_since_last_index: 0,
            index_interval_bytes,
        })
    }

    /// Open an existing segment and recover state by scanning the .log file.
    pub fn open(dir: &Path, base_offset: i64, index_interval_bytes: u64) -> Result<Self> {
        let log_path = dir.join(segment_file_name(base_offset, "log"));
        let index_path = dir.join(segment_file_name(base_offset, "index"));
        let timeindex_path = dir.join(segment_file_name(base_offset, "timeindex"));

        // Read the entire log file to recover next_offset and size.
        let mut reader = File::open(&log_path)?;
        let file_len = reader.metadata()?.len();
        let mut buf = vec![0u8; file_len as usize];
        reader.read_exact(&mut buf)?;

        let mut next_offset = base_offset;
        let mut pos: usize = 0;

        while pos + record_batch::RECORD_BATCH_HEADER_SIZE <= buf.len() {
            let batch_slice = &buf[pos..];
            let batch_length = record_batch::get_batch_length(batch_slice);
            if batch_length < 0 {
                return Err(StorageError::Corrupt(format!(
                    "negative batch length {} at position {}",
                    batch_length, pos
                )));
            }
            let total_size = 12 + batch_length as usize;
            if pos + total_size > buf.len() {
                // Truncated batch at end of file; treat as end.
                break;
            }
            let record_count = record_batch::get_record_count(&batch_slice[..total_size])?;
            let batch_base = record_batch::get_batch_base_offset(batch_slice);
            next_offset = batch_base + record_count as i64;
            pos += total_size;
        }

        let size = pos as u64;

        // Open log file for appending (seek to end).
        let file = OpenOptions::new().append(true).open(&log_path)?;
        let writer = BufWriter::new(file);

        // Open indexes (recover from disk).
        let offset_index = if index_path.exists() {
            OffsetIndex::open(&index_path, base_offset)?
        } else {
            OffsetIndex::new(&index_path, base_offset)?
        };
        let time_index = if timeindex_path.exists() {
            TimeIndex::open(&timeindex_path, base_offset)?
        } else {
            TimeIndex::new(&timeindex_path, base_offset)?
        };

        Ok(Self {
            base_offset,
            next_offset,
            dir: dir.to_path_buf(),
            writer,
            size,
            offset_index,
            time_index,
            bytes_since_last_index: 0,
            index_interval_bytes,
        })
    }

    /// Append a record batch. Returns `(base_offset_for_batch, next_offset_after)`.
    ///
    /// The batch bytes will have their base_offset rewritten to the current
    /// `next_offset` of this segment.
    pub fn append(&mut self, batch: &mut [u8], record_count: i32) -> Result<(i64, i64)> {
        let batch_base = self.next_offset;
        record_batch::set_batch_base_offset(batch, batch_base);

        let position = self.size as u32;
        self.writer.write_all(batch)?;

        let batch_size = batch.len() as u64;
        self.size += batch_size;

        // Update sparse index if enough bytes have accumulated.
        if self.bytes_since_last_index >= self.index_interval_bytes {
            self.offset_index.append(batch_base, position)?;
            let timestamp = record_batch::get_first_timestamp(batch);
            self.time_index.append(timestamp, batch_base)?;
            self.bytes_since_last_index = 0;
        }
        self.bytes_since_last_index += batch_size;

        self.next_offset = batch_base + record_count as i64;
        Ok((batch_base, self.next_offset))
    }

    /// Read raw bytes starting from `start_offset`, up to `max_bytes`.
    ///
    /// Returns the raw bytes (RecordBatch data) that can be sent directly to a client.
    /// If `start_offset` falls within a batch (not at a batch boundary), the entire
    /// batch containing that offset is returned.
    pub fn read(&mut self, start_offset: i64, max_bytes: usize) -> Result<Vec<u8>> {
        // Flush buffered writes so the file on disk is up to date.
        self.writer.flush()?;
        if start_offset >= self.next_offset {
            return Err(StorageError::OffsetOutOfRange(start_offset));
        }
        if start_offset < self.base_offset {
            return Err(StorageError::OffsetOutOfRange(start_offset));
        }

        let log_path = self.dir.join(segment_file_name(self.base_offset, "log"));
        let mut reader = File::open(&log_path)?;

        // Use the offset index to find a starting position.
        let seek_pos = self.offset_index.lookup(start_offset).unwrap_or(0);
        reader.seek(SeekFrom::Start(seek_pos as u64))?;

        // Read from seek_pos to end of file into a buffer.
        let remaining = self.size - seek_pos as u64;
        let mut buf = vec![0u8; remaining as usize];
        reader.read_exact(&mut buf)?;

        // Scan forward through batches to find the one containing start_offset.
        let mut scan_pos: usize = 0;
        let mut result = Vec::new();
        let mut found = false;

        while scan_pos + record_batch::RECORD_BATCH_HEADER_SIZE <= buf.len() {
            let batch_slice = &buf[scan_pos..];
            let batch_length = record_batch::get_batch_length(batch_slice);
            if batch_length < 0 {
                break;
            }
            let total_size = 12 + batch_length as usize;
            if scan_pos + total_size > buf.len() {
                break;
            }

            let batch_base = record_batch::get_batch_base_offset(batch_slice);
            let record_count =
                record_batch::get_record_count(&batch_slice[..total_size])?;
            let batch_end_offset = batch_base + record_count as i64;

            // This batch contains start_offset if batch_base <= start_offset < batch_end_offset.
            // Or this batch starts at or after start_offset.
            if batch_end_offset > start_offset {
                if !found {
                    found = true;
                }
                if result.len() + total_size > max_bytes && !result.is_empty() {
                    // Adding this batch would exceed max_bytes and we already have data.
                    break;
                }
                result.extend_from_slice(&buf[scan_pos..scan_pos + total_size]);
                if result.len() >= max_bytes {
                    break;
                }
            }
            scan_pos += total_size;
        }

        Ok(result)
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }
}

/// Format offset as 20-digit zero-padded string with the given extension.
fn segment_file_name(base_offset: i64, ext: &str) -> String {
    format!("{:020}.{}", base_offset, ext)
}

/// Parse a base offset from a segment filename (e.g. "00000000000000000000.log").
pub fn parse_base_offset(filename: &str) -> Option<i64> {
    let stem = filename.split('.').next()?;
    stem.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_batch;
    use tempfile::tempdir;

    /// Build a minimal valid RecordBatch with the given parameters.
    fn make_batch(
        base_offset: i64,
        first_ts: i64,
        max_ts: i64,
        record_count: i32,
    ) -> Vec<u8> {
        // batch_length = 49 (minimum: 61 header - 12 prefix)
        let batch_length: i32 = 49;
        let total = 12 + batch_length as usize;
        let mut buf = vec![0u8; total];
        // base_offset: bytes 0..8
        buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
        // batch_length: bytes 8..12
        buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
        // magic: byte 16
        buf[16] = 2;
        // first_timestamp: bytes 25..33
        buf[25..33].copy_from_slice(&first_ts.to_be_bytes());
        // max_timestamp: bytes 33..41
        buf[33..41].copy_from_slice(&max_ts.to_be_bytes());
        // record_count: bytes 57..61
        buf[57..61].copy_from_slice(&record_count.to_be_bytes());
        buf
    }

    #[test]
    fn test_new_segment() {
        let dir = tempdir().unwrap();
        let seg = Segment::new(dir.path(), 0, 4096).unwrap();
        assert_eq!(seg.base_offset(), 0);
        assert_eq!(seg.next_offset(), 0);
        assert_eq!(seg.size(), 0);
    }

    #[test]
    fn test_append_single_batch() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        let mut batch = make_batch(0, 1000, 1000, 3);
        let (base, next) = seg.append(&mut batch, 3).unwrap();
        seg.flush().unwrap();

        assert_eq!(base, 0);
        assert_eq!(next, 3);
        assert_eq!(seg.next_offset(), 3);
        assert_eq!(seg.size(), 61);
    }

    #[test]
    fn test_append_multiple_batches() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        let mut batch1 = make_batch(0, 1000, 1000, 2);
        let (b1, n1) = seg.append(&mut batch1, 2).unwrap();
        assert_eq!(b1, 0);
        assert_eq!(n1, 2);

        let mut batch2 = make_batch(0, 2000, 2000, 3);
        let (b2, n2) = seg.append(&mut batch2, 3).unwrap();
        assert_eq!(b2, 2);
        assert_eq!(n2, 5);

        seg.flush().unwrap();
        assert_eq!(seg.next_offset(), 5);
        assert_eq!(seg.size(), 122); // 61 * 2
    }

    #[test]
    fn test_read_single_batch() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        let mut batch = make_batch(0, 1000, 1000, 3);
        seg.append(&mut batch, 3).unwrap();
        seg.flush().unwrap();

        let data = seg.read(0, 4096).unwrap();
        assert_eq!(data.len(), 61);
        assert_eq!(record_batch::get_batch_base_offset(&data), 0);
        assert_eq!(record_batch::get_record_count(&data).unwrap(), 3);
    }

    #[test]
    fn test_read_from_middle_offset() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        let mut batch1 = make_batch(0, 1000, 1000, 5);
        seg.append(&mut batch1, 5).unwrap();

        let mut batch2 = make_batch(0, 2000, 2000, 5);
        seg.append(&mut batch2, 5).unwrap();

        seg.flush().unwrap();

        // Read starting from offset 5 (second batch).
        let data = seg.read(5, 4096).unwrap();
        assert_eq!(data.len(), 61);
        assert_eq!(record_batch::get_batch_base_offset(&data), 5);
    }

    #[test]
    fn test_read_offset_within_batch() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        // Batch with 5 records, offsets 0..4
        let mut batch = make_batch(0, 1000, 1000, 5);
        seg.append(&mut batch, 5).unwrap();
        seg.flush().unwrap();

        // Read at offset 3 should still return the whole batch.
        let data = seg.read(3, 4096).unwrap();
        assert_eq!(data.len(), 61);
        assert_eq!(record_batch::get_batch_base_offset(&data), 0);
    }

    #[test]
    fn test_read_with_max_bytes_limit() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        for _ in 0..5 {
            let mut batch = make_batch(0, 1000, 1000, 1);
            seg.append(&mut batch, 1).unwrap();
        }
        seg.flush().unwrap();

        // Read from offset 0 with max_bytes = 100 (one batch is 61 bytes).
        let data = seg.read(0, 100).unwrap();
        // Should get exactly one batch (61 bytes) since 61 < 100 but 61+61=122 > 100.
        assert_eq!(data.len(), 61);
    }

    #[test]
    fn test_read_offset_out_of_range() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();

        let mut batch = make_batch(0, 1000, 1000, 3);
        seg.append(&mut batch, 3).unwrap();
        seg.flush().unwrap();

        assert!(seg.read(3, 4096).is_err());
        assert!(seg.read(-1, 4096).is_err());
    }

    #[test]
    fn test_open_recovery() {
        let dir = tempdir().unwrap();

        // Create segment and write batches.
        {
            let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();
            let mut batch1 = make_batch(0, 1000, 1000, 3);
            seg.append(&mut batch1, 3).unwrap();
            let mut batch2 = make_batch(0, 2000, 2000, 2);
            seg.append(&mut batch2, 2).unwrap();
            seg.flush().unwrap();
        }

        // Re-open and verify.
        let mut seg = Segment::open(dir.path(), 0, 4096).unwrap();
        assert_eq!(seg.base_offset(), 0);
        assert_eq!(seg.next_offset(), 5);
        assert_eq!(seg.size(), 122);

        // Verify read still works.
        let data = seg.read(0, 4096).unwrap();
        assert!(data.len() > 0);
    }

    #[test]
    fn test_open_recovery_continues_append() {
        let dir = tempdir().unwrap();

        {
            let mut seg = Segment::new(dir.path(), 0, 4096).unwrap();
            let mut batch = make_batch(0, 1000, 1000, 3);
            seg.append(&mut batch, 3).unwrap();
            seg.flush().unwrap();
        }

        let mut seg = Segment::open(dir.path(), 0, 4096).unwrap();
        let mut batch = make_batch(0, 2000, 2000, 2);
        let (base, next) = seg.append(&mut batch, 2).unwrap();
        seg.flush().unwrap();

        assert_eq!(base, 3);
        assert_eq!(next, 5);
        assert_eq!(seg.size(), 122);
    }

    #[test]
    fn test_segment_file_name() {
        assert_eq!(segment_file_name(0, "log"), "00000000000000000000.log");
        assert_eq!(
            segment_file_name(12345, "index"),
            "00000000000000012345.index"
        );
    }

    #[test]
    fn test_parse_base_offset() {
        assert_eq!(parse_base_offset("00000000000000000000.log"), Some(0));
        assert_eq!(parse_base_offset("00000000000000012345.log"), Some(12345));
        assert_eq!(parse_base_offset("not_a_number.log"), None);
    }

    #[test]
    fn test_index_entries_written() {
        let dir = tempdir().unwrap();
        // Use index_interval_bytes = 0 so every batch triggers an index entry
        // (after the first one, since bytes_since_last_index starts at 0).
        let mut seg = Segment::new(dir.path(), 0, 0).unwrap();

        // First batch: bytes_since_last_index starts at 0 which is >= 0, so index entry.
        let mut batch1 = make_batch(0, 1000, 1000, 2);
        seg.append(&mut batch1, 2).unwrap();

        let mut batch2 = make_batch(0, 2000, 2000, 3);
        seg.append(&mut batch2, 3).unwrap();

        seg.flush().unwrap();

        // Verify we can read using the index (offset 2 should be found via index).
        let data = seg.read(2, 4096).unwrap();
        assert_eq!(record_batch::get_batch_base_offset(&data), 2);
    }
}
