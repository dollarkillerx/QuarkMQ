use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::wal::WalRecord;
use crate::StorageError;

const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB

/// A single segment file containing WAL records.
pub struct Segment {
    path: PathBuf,
    id: u64,
    max_size: u64,
    current_size: u64,
    sealed: bool,
}

impl Segment {
    pub fn create(dir: &Path, id: u64, max_size: u64) -> Result<Self, StorageError> {
        let path = Self::segment_path(dir, id);
        // Create the file
        std::fs::File::create(&path)?;
        Ok(Self {
            path,
            id,
            max_size,
            current_size: 0,
            sealed: false,
        })
    }

    pub fn open(path: impl AsRef<Path>, id: u64, max_size: u64) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        let metadata = std::fs::metadata(&path)?;
        let current_size = metadata.len();
        let sealed = current_size >= max_size;

        Ok(Self {
            path,
            id,
            max_size,
            current_size,
            sealed,
        })
    }

    pub fn segment_path(dir: &Path, id: u64) -> PathBuf {
        dir.join(format!("segment-{:06}.qmq", id))
    }

    pub fn append_record(&mut self, record: &WalRecord) -> Result<u64, StorageError> {
        if self.sealed {
            return Err(StorageError::SegmentFull);
        }

        let encoded = record.encode();
        let record_size = encoded.len() as u64;

        if self.current_size + record_size > self.max_size {
            self.sealed = true;
            return Err(StorageError::SegmentFull);
        }

        let mut file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        let offset = self.current_size;
        file.write_all(&encoded)?;
        self.current_size += record_size;

        Ok(offset)
    }

    pub fn read_record_at(&self, offset: u64) -> Result<WalRecord, StorageError> {
        let file = std::fs::File::open(&self.path)?;
        let mut reader = io::BufReader::new(file);
        reader.seek(SeekFrom::Start(offset))?;
        WalRecord::decode(&mut reader)?.ok_or(StorageError::CorruptRecord { offset })
    }

    pub fn read_all_records(&self) -> Result<Vec<WalRecord>, StorageError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = std::fs::File::open(&self.path)?;
        let mut reader = io::BufReader::new(file);
        let mut records = Vec::new();

        loop {
            match WalRecord::decode(&mut reader) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break,
                Err(StorageError::CrcMismatch { .. }) => {
                    tracing::warn!(segment_id = self.id, "CRC mismatch in segment, stopping read");
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn seal(&mut self) {
        self.sealed = true;
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn current_size(&self) -> u64 {
        self.current_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Manages multiple segments for a channel.
pub struct SegmentManager {
    dir: PathBuf,
    segments: Vec<Segment>,
    max_segment_size: u64,
    next_segment_id: u64,
}

impl SegmentManager {
    pub fn open(dir: impl AsRef<Path>, max_segment_size_mb: u64) -> Result<Self, StorageError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let max_segment_size = max_segment_size_mb * 1024 * 1024;
        let mut segments = Vec::new();

        // Scan for existing segment files
        let mut entries: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("segment-") && n.ends_with(".qmq"))
                    .unwrap_or(false)
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());

        let mut next_id = 1u64;
        for entry in entries {
            let fname = entry.file_name();
            let name = fname.to_str().unwrap_or("");
            if let Some(id_str) = name.strip_prefix("segment-").and_then(|s| s.strip_suffix(".qmq"))
            {
                if let Ok(id) = id_str.parse::<u64>() {
                    let segment = Segment::open(entry.path(), id, max_segment_size)?;
                    next_id = id + 1;
                    segments.push(segment);
                }
            }
        }

        Ok(Self {
            dir,
            segments,
            max_segment_size,
            next_segment_id: next_id,
        })
    }

    /// Append a record. Creates a new segment if the current one is full.
    pub fn append(&mut self, record: &WalRecord) -> Result<(u64, u64), StorageError> {
        // Try the current (last) segment first
        if let Some(segment) = self.segments.last_mut() {
            match segment.append_record(record) {
                Ok(offset) => return Ok((segment.id(), offset)),
                Err(StorageError::SegmentFull) => {
                    segment.seal();
                }
                Err(e) => return Err(e),
            }
        }

        // Create a new segment
        let new_segment = Segment::create(&self.dir, self.next_segment_id, self.max_segment_size)?;
        self.next_segment_id += 1;
        self.segments.push(new_segment);

        let segment = self.segments.last_mut().unwrap();
        let offset = segment.append_record(record)?;
        Ok((segment.id(), offset))
    }

    /// Read a record by segment_id and offset.
    pub fn read(&self, segment_id: u64, offset: u64) -> Result<WalRecord, StorageError> {
        let segment = self
            .segments
            .iter()
            .find(|s| s.id() == segment_id)
            .ok_or(StorageError::CorruptRecord { offset })?;
        segment.read_record_at(offset)
    }

    /// Read all records from all segments.
    pub fn read_all(&self) -> Result<Vec<WalRecord>, StorageError> {
        let mut all_records = Vec::new();
        for segment in &self.segments {
            all_records.extend(segment.read_all_records()?);
        }
        Ok(all_records)
    }

    /// Remove empty or fully-GC'd segments.
    pub fn remove_segment(&mut self, segment_id: u64) -> Result<(), StorageError> {
        if let Some(pos) = self.segments.iter().position(|s| s.id() == segment_id) {
            let segment = &self.segments[pos];
            if segment.path().exists() {
                std::fs::remove_file(segment.path())?;
            }
            self.segments.remove(pos);
        }
        Ok(())
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{WalOperation, WalRecord};
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn test_segment_create_and_append() {
        let dir = tempdir().unwrap();
        let mut segment = Segment::create(dir.path(), 1, 1024 * 1024).unwrap();

        let record = WalRecord::new(1, WalOperation::Append, Uuid::now_v7(), b"hello".to_vec());
        let offset = segment.append_record(&record).unwrap();
        assert_eq!(offset, 0);

        let record2 = WalRecord::new(2, WalOperation::Append, Uuid::now_v7(), b"world".to_vec());
        let offset2 = segment.append_record(&record2).unwrap();
        assert!(offset2 > 0);
    }

    #[test]
    fn test_segment_read_back() {
        let dir = tempdir().unwrap();
        let msg_id = Uuid::now_v7();
        let mut segment = Segment::create(dir.path(), 1, 1024 * 1024).unwrap();

        let record = WalRecord::new(1, WalOperation::Append, msg_id, b"test data".to_vec());
        let offset = segment.append_record(&record).unwrap();

        let read_back = segment.read_record_at(offset).unwrap();
        assert_eq!(read_back.message_id, msg_id);
        assert_eq!(read_back.data, b"test data");
    }

    #[test]
    fn test_segment_full() {
        let dir = tempdir().unwrap();
        let mut segment = Segment::create(dir.path(), 1, 100).unwrap(); // Very small segment

        let record = WalRecord::new(1, WalOperation::Append, Uuid::now_v7(), vec![0u8; 80]);
        let result = segment.append_record(&record);
        // First record may or may not fit depending on header overhead
        // The point is that eventually it should return SegmentFull
        if result.is_ok() {
            let record2 = WalRecord::new(2, WalOperation::Append, Uuid::now_v7(), vec![0u8; 80]);
            let result2 = segment.append_record(&record2);
            assert!(matches!(result2, Err(StorageError::SegmentFull)));
        }
    }

    #[test]
    fn test_segment_manager_auto_rollover() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("segments");

        // Use a very small segment size to force rollover
        let mut mgr = SegmentManager::open(&data_dir, 0).unwrap();
        // max_segment_size = 0 MB, but minimum is 0 bytes, so every append creates a new segment
        // Actually let's use a reasonable small size
        drop(mgr);

        // Recreate with a custom tiny max
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut mgr = SegmentManager {
            dir: data_dir.clone(),
            segments: Vec::new(),
            max_segment_size: 100, // 100 bytes
            next_segment_id: 1,
        };

        // Append records until we get multiple segments
        for i in 1..=5 {
            let record = WalRecord::new(i, WalOperation::Append, Uuid::now_v7(), vec![0u8; 30]);
            mgr.append(&record).unwrap();
        }

        assert!(mgr.segment_count() > 1, "expected multiple segments due to rollover");
    }

    #[test]
    fn test_segment_manager_read_all() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("segments");

        let mut mgr = SegmentManager::open(&data_dir, 1).unwrap(); // 1MB segments

        let ids: Vec<_> = (0..3).map(|_| Uuid::now_v7()).collect();
        for (i, id) in ids.iter().enumerate() {
            let record = WalRecord::new(
                (i + 1) as u64,
                WalOperation::Append,
                *id,
                format!("msg-{}", i).into_bytes(),
            );
            mgr.append(&record).unwrap();
        }

        let all = mgr.read_all().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].message_id, ids[0]);
        assert_eq!(all[1].message_id, ids[1]);
        assert_eq!(all[2].message_id, ids[2]);
    }
}
