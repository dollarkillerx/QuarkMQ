use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;

/// Entry size in bytes: 8 bytes timestamp (i64 BE) + 4 bytes relative offset (u32 BE).
const ENTRY_SIZE: usize = 12;

/// Sparse timestamp-to-offset index for a segment.
///
/// Stored on disk as a sequence of 12-byte entries:
///   8 bytes timestamp (i64 big-endian) + 4 bytes relative offset (u32 big-endian).
pub struct TimeIndex {
    path: PathBuf,
    writer: BufWriter<File>,
    /// In-memory entries: (timestamp, relative_offset)
    entries: Vec<(i64, u32)>,
    base_offset: i64,
}

impl TimeIndex {
    /// Create a new, empty time index file.
    pub fn new(path: &Path, base_offset: i64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            entries: Vec::new(),
            base_offset,
        })
    }

    /// Open and recover an existing time index from file.
    pub fn open(path: &Path, base_offset: i64) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut entries = Vec::with_capacity(buf.len() / ENTRY_SIZE);
        let mut pos = 0;
        while pos + ENTRY_SIZE <= buf.len() {
            let timestamp =
                i64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
            let relative_offset =
                u32::from_be_bytes(buf[pos + 8..pos + 12].try_into().unwrap());
            entries.push((timestamp, relative_offset));
            pos += ENTRY_SIZE;
        }

        // Re-open for appending.
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            entries,
            base_offset,
        })
    }

    /// Add an entry mapping `timestamp` to `absolute_offset`.
    pub fn append(&mut self, timestamp: i64, offset: i64) -> Result<()> {
        let relative = (offset - self.base_offset) as u32;
        self.writer.write_all(&timestamp.to_be_bytes())?;
        self.writer.write_all(&relative.to_be_bytes())?;
        self.entries.push((timestamp, relative));
        Ok(())
    }

    /// Find the absolute offset of the first message with timestamp >= `target_timestamp`.
    ///
    /// Returns `None` if all entries have timestamps less than `target_timestamp`
    /// or the index is empty.
    pub fn lookup(&self, target_timestamp: i64) -> Option<i64> {
        if self.entries.is_empty() {
            return None;
        }
        // Binary search for first entry with timestamp >= target.
        let idx = self
            .entries
            .partition_point(|e| e.0 < target_timestamp);
        if idx < self.entries.len() {
            Some(self.base_offset + self.entries[idx].1 as i64)
        } else {
            None
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_new_and_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        let mut idx = TimeIndex::new(&path, 0).unwrap();
        idx.append(1000, 0).unwrap();
        idx.append(2000, 5).unwrap();
        idx.append(3000, 10).unwrap();
        idx.flush().unwrap();

        assert_eq!(idx.entries.len(), 3);
    }

    #[test]
    fn test_lookup_exact_match() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        let mut idx = TimeIndex::new(&path, 100).unwrap();
        idx.append(1000, 100).unwrap();
        idx.append(2000, 110).unwrap();
        idx.append(3000, 120).unwrap();

        assert_eq!(idx.lookup(1000), Some(100));
        assert_eq!(idx.lookup(2000), Some(110));
        assert_eq!(idx.lookup(3000), Some(120));
    }

    #[test]
    fn test_lookup_first_gte() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        let mut idx = TimeIndex::new(&path, 0).unwrap();
        idx.append(1000, 0).unwrap();
        idx.append(2000, 5).unwrap();
        idx.append(3000, 10).unwrap();

        // 1500 -> first entry with ts >= 1500 is ts=2000 at offset 5
        assert_eq!(idx.lookup(1500), Some(5));
        // 0 -> first entry with ts >= 0 is ts=1000 at offset 0
        assert_eq!(idx.lookup(0), Some(0));
        // 2500 -> first entry with ts >= 2500 is ts=3000 at offset 10
        assert_eq!(idx.lookup(2500), Some(10));
    }

    #[test]
    fn test_lookup_beyond_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        let mut idx = TimeIndex::new(&path, 0).unwrap();
        idx.append(1000, 0).unwrap();
        idx.append(2000, 5).unwrap();

        assert_eq!(idx.lookup(3000), None);
    }

    #[test]
    fn test_lookup_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        let idx = TimeIndex::new(&path, 0).unwrap();
        assert_eq!(idx.lookup(0), None);
    }

    #[test]
    fn test_open_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");

        {
            let mut idx = TimeIndex::new(&path, 0).unwrap();
            idx.append(1000, 0).unwrap();
            idx.append(2000, 5).unwrap();
            idx.append(3000, 10).unwrap();
            idx.flush().unwrap();
        }

        let idx = TimeIndex::open(&path, 0).unwrap();
        assert_eq!(idx.entries.len(), 3);
        assert_eq!(idx.lookup(1000), Some(0));
        assert_eq!(idx.lookup(1500), Some(5));
        assert_eq!(idx.lookup(3000), Some(10));
    }
}
