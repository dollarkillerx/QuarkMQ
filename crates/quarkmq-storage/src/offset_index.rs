use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;

/// Entry size in bytes: 4 bytes relative offset (u32 BE) + 4 bytes position (u32 BE).
const ENTRY_SIZE: usize = 8;

/// Sparse offset-to-file-position index for a segment.
///
/// Stored on disk as a sequence of 8-byte entries:
///   4 bytes relative offset (u32 big-endian) + 4 bytes position (u32 big-endian).
pub struct OffsetIndex {
    path: PathBuf,
    writer: BufWriter<File>,
    /// In-memory entries: (relative_offset, position)
    entries: Vec<(u32, u32)>,
    base_offset: i64,
}

impl OffsetIndex {
    /// Create a new, empty offset index file.
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

    /// Open and recover an existing offset index from file.
    pub fn open(path: &Path, base_offset: i64) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut entries = Vec::with_capacity(buf.len() / ENTRY_SIZE);
        let mut pos = 0;
        while pos + ENTRY_SIZE <= buf.len() {
            let relative_offset =
                u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
            let position =
                u32::from_be_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
            entries.push((relative_offset, position));
            pos += ENTRY_SIZE;
        }

        // Re-open the file for appending.
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            entries,
            base_offset,
        })
    }

    /// Add an entry mapping `absolute_offset` to a `position` in the .log file.
    pub fn append(&mut self, offset: i64, position: u32) -> Result<()> {
        let relative = (offset - self.base_offset) as u32;
        self.writer.write_all(&relative.to_be_bytes())?;
        self.writer.write_all(&position.to_be_bytes())?;
        self.entries.push((relative, position));
        Ok(())
    }

    /// Find the file position for the largest indexed offset that is <= `target_offset`.
    ///
    /// Returns `None` if the index is empty or all entries are above the target.
    pub fn lookup(&self, target_offset: i64) -> Option<u32> {
        if self.entries.is_empty() {
            return None;
        }
        let relative = (target_offset - self.base_offset) as u32;
        // Binary search for largest entry with relative_offset <= relative.
        match self.entries.binary_search_by_key(&relative, |e| e.0) {
            Ok(idx) => Some(self.entries[idx].1),
            Err(0) => None, // all entries are greater than target
            Err(idx) => Some(self.entries[idx - 1].1),
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
        let path = dir.path().join("00000000000000000000.index");

        let mut idx = OffsetIndex::new(&path, 0).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(5, 1000).unwrap();
        idx.append(10, 2000).unwrap();
        idx.flush().unwrap();

        assert_eq!(idx.entries.len(), 3);
    }

    #[test]
    fn test_lookup_exact_match() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");

        let mut idx = OffsetIndex::new(&path, 100).unwrap();
        idx.append(100, 0).unwrap();
        idx.append(110, 500).unwrap();
        idx.append(120, 1000).unwrap();

        assert_eq!(idx.lookup(100), Some(0));
        assert_eq!(idx.lookup(110), Some(500));
        assert_eq!(idx.lookup(120), Some(1000));
    }

    #[test]
    fn test_lookup_between_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");

        let mut idx = OffsetIndex::new(&path, 0).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(10, 500).unwrap();
        idx.append(20, 1000).unwrap();

        // Between 0 and 10 -> should return position for offset 0
        assert_eq!(idx.lookup(5), Some(0));
        // Between 10 and 20 -> should return position for offset 10
        assert_eq!(idx.lookup(15), Some(500));
        // Beyond last entry -> should return position for offset 20
        assert_eq!(idx.lookup(25), Some(1000));
    }

    #[test]
    fn test_lookup_before_first_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");

        let mut idx = OffsetIndex::new(&path, 100).unwrap();
        idx.append(105, 500).unwrap();

        // target_offset 100 has relative 0, but first entry is at relative 5
        assert_eq!(idx.lookup(100), None);
        assert_eq!(idx.lookup(104), None);
    }

    #[test]
    fn test_lookup_empty_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");

        let idx = OffsetIndex::new(&path, 0).unwrap();
        assert_eq!(idx.lookup(0), None);
    }

    #[test]
    fn test_open_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");

        // Write some entries
        {
            let mut idx = OffsetIndex::new(&path, 0).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(5, 500).unwrap();
            idx.append(10, 1000).unwrap();
            idx.flush().unwrap();
        }

        // Re-open and verify
        let idx = OffsetIndex::open(&path, 0).unwrap();
        assert_eq!(idx.entries.len(), 3);
        assert_eq!(idx.lookup(0), Some(0));
        assert_eq!(idx.lookup(5), Some(500));
        assert_eq!(idx.lookup(10), Some(1000));
        assert_eq!(idx.lookup(7), Some(500));
    }
}
