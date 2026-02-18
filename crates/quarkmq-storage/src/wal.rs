use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use quarkmq_protocol::MessageId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageError;

/// WAL Record layout:
/// [sequence: u64] [op: u8] [msg_id: 16B] [data_len: u32] [data: N bytes] [crc32: u32]
/// Total header: 8 + 1 + 16 + 4 = 29 bytes + data + 4 bytes CRC
const RECORD_HEADER_SIZE: usize = 8 + 1 + 16 + 4; // 29 bytes
const CRC_SIZE: usize = 4;
/// Maximum allowed data length per WAL record (64 MB).
const MAX_RECORD_DATA_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalOperation {
    Append = 1,
    Ack = 2,
    Nack = 3,
    Delete = 4,
    DeadLetter = 5,
}

impl WalOperation {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Append),
            2 => Some(Self::Ack),
            3 => Some(Self::Nack),
            4 => Some(Self::Delete),
            5 => Some(Self::DeadLetter),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub sequence: u64,
    pub operation: WalOperation,
    pub message_id: MessageId,
    pub data: Vec<u8>,
    pub crc: u32,
}

impl WalRecord {
    pub fn new(sequence: u64, operation: WalOperation, message_id: MessageId, data: Vec<u8>) -> Self {
        let crc = Self::compute_crc(sequence, operation, &message_id, &data);
        Self {
            sequence,
            operation,
            message_id,
            data,
            crc,
        }
    }

    fn compute_crc(sequence: u64, operation: WalOperation, message_id: &MessageId, data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&sequence.to_le_bytes());
        hasher.update(&[operation as u8]);
        hasher.update(message_id.as_bytes());
        hasher.update(&(data.len() as u32).to_le_bytes());
        hasher.update(data);
        hasher.finalize()
    }

    pub fn verify_crc(&self) -> bool {
        let expected = Self::compute_crc(self.sequence, self.operation, &self.message_id, &self.data);
        expected == self.crc
    }

    /// Encode this record into bytes for writing.
    pub fn encode(&self) -> Vec<u8> {
        let total_size = RECORD_HEADER_SIZE + self.data.len() + CRC_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.push(self.operation as u8);
        buf.extend_from_slice(self.message_id.as_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);
        buf.extend_from_slice(&self.crc.to_le_bytes());

        buf
    }

    /// Decode a record from a reader. Returns None on EOF, Err on corruption.
    pub fn decode<R: Read>(reader: &mut R) -> Result<Option<Self>, StorageError> {
        // Read header
        let mut header = [0u8; RECORD_HEADER_SIZE];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(StorageError::Io(e)),
        }

        let sequence = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let op_byte = header[8];
        let operation = WalOperation::from_u8(op_byte).ok_or(StorageError::CorruptRecord {
            offset: 0, // Caller should track offset
        })?;
        let message_id = Uuid::from_bytes(header[9..25].try_into().unwrap());
        let data_len = u32::from_le_bytes(header[25..29].try_into().unwrap()) as usize;

        // H4: Reject unreasonably large data_len to prevent OOM from corrupt WAL
        if data_len > MAX_RECORD_DATA_LEN {
            return Err(StorageError::CorruptRecord { offset: 0 });
        }

        // Read data
        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;

        // Read CRC
        let mut crc_bytes = [0u8; CRC_SIZE];
        reader.read_exact(&mut crc_bytes)?;
        let crc = u32::from_le_bytes(crc_bytes);

        let record = Self {
            sequence,
            operation,
            message_id,
            data,
            crc,
        };

        if !record.verify_crc() {
            return Err(StorageError::CrcMismatch {
                expected: Self::compute_crc(sequence, operation, &message_id, &record.data),
                actual: crc,
            });
        }

        Ok(Some(record))
    }
}

pub struct Wal {
    path: PathBuf,
    writer: BufWriter<std::fs::File>,
    next_sequence: u64,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("next_sequence", &self.next_sequence)
            .finish()
    }
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Determine next sequence by scanning existing records
        let next_sequence = if path.exists() {
            let records = Self::read_all_records(&path)?;
            records.last().map(|r| r.sequence + 1).unwrap_or(1)
        } else {
            1
        };

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            next_sequence,
        })
    }

    pub fn append(&mut self, operation: WalOperation, message_id: MessageId, data: Vec<u8>) -> Result<WalRecord, StorageError> {
        let record = WalRecord::new(self.next_sequence, operation, message_id, data);
        let encoded = record.encode();

        self.writer.write_all(&encoded)?;
        // Flush is deferred to sync() for batch efficiency.
        // Callers should invoke sync() periodically or after critical writes.

        self.next_sequence += 1;
        Ok(record)
    }

    pub fn sync(&mut self) -> Result<(), StorageError> {
        use std::io::Write;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read all valid records from the WAL file.
    pub fn read_all_records(path: &Path) -> Result<Vec<WalRecord>, StorageError> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = std::fs::File::open(path)?;
        let mut reader = io::BufReader::new(file);
        let mut records = Vec::new();

        loop {
            match WalRecord::decode(&mut reader) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break, // EOF
                Err(StorageError::CrcMismatch { expected, actual }) => {
                    tracing::warn!(
                        expected = expected,
                        actual = actual,
                        "CRC mismatch in WAL, skipping corrupt record"
                    );
                    // Try to skip to next record - this is a best-effort recovery
                    break;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "error reading WAL, stopping replay");
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Truncate and rewrite the WAL with only the given records.
    pub fn compact(&mut self, records: &[WalRecord]) -> Result<(), StorageError> {
        // Write to a temp file first
        let tmp_path = self.path.with_extension("qmq.tmp");
        {
            let file = std::fs::File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            for record in records {
                writer.write_all(&record.encode())?;
            }
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }

        // Atomic rename
        std::fs::rename(&tmp_path, &self.path)?;

        // Reopen
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.next_sequence = records.last().map(|r| r.sequence + 1).unwrap_or(1);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    #[test]
    fn test_wal_record_encode_decode_roundtrip() {
        let msg_id = Uuid::now_v7();
        let data = b"hello world".to_vec();
        let record = WalRecord::new(1, WalOperation::Append, msg_id, data.clone());

        assert!(record.verify_crc());

        let encoded = record.encode();
        let mut cursor = io::Cursor::new(encoded);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();

        assert_eq!(decoded.sequence, 1);
        assert_eq!(decoded.operation, WalOperation::Append);
        assert_eq!(decoded.message_id, msg_id);
        assert_eq!(decoded.data, data);
        assert!(decoded.verify_crc());
    }

    #[test]
    fn test_wal_record_crc_detects_corruption() {
        let msg_id = Uuid::now_v7();
        let record = WalRecord::new(1, WalOperation::Append, msg_id, b"test".to_vec());
        let mut encoded = record.encode();

        // Corrupt one data byte
        let data_offset = RECORD_HEADER_SIZE + 1;
        encoded[data_offset] ^= 0xFF;

        let mut cursor = io::Cursor::new(encoded);
        let result = WalRecord::decode(&mut cursor);
        assert!(matches!(result, Err(StorageError::CrcMismatch { .. })));
    }

    #[test]
    fn test_wal_append_and_read_back() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let msg1 = Uuid::now_v7();
        let msg2 = Uuid::now_v7();

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::Append, msg1, b"message 1".to_vec()).unwrap();
            wal.append(WalOperation::Append, msg2, b"message 2".to_vec()).unwrap();
            wal.append(WalOperation::Ack, msg1, vec![]).unwrap();
            wal.sync().unwrap();
        }

        let records = Wal::read_all_records(&wal_path).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].operation, WalOperation::Append);
        assert_eq!(records[0].message_id, msg1);
        assert_eq!(records[1].operation, WalOperation::Append);
        assert_eq!(records[1].message_id, msg2);
        assert_eq!(records[2].operation, WalOperation::Ack);
    }

    #[test]
    fn test_wal_reopen_continues_sequence() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(WalOperation::Append, Uuid::now_v7(), b"m1".to_vec()).unwrap();
            wal.append(WalOperation::Append, Uuid::now_v7(), b"m2".to_vec()).unwrap();
            wal.sync().unwrap();
        }

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            assert_eq!(wal.next_sequence(), 3);
            wal.append(WalOperation::Append, Uuid::now_v7(), b"m3".to_vec()).unwrap();
            wal.sync().unwrap();
        }

        let records = Wal::read_all_records(&wal_path).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].sequence, 3);
    }

    #[test]
    fn test_wal_compact() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let msg1 = Uuid::now_v7();
        let msg2 = Uuid::now_v7();

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.append(WalOperation::Append, msg1, b"m1".to_vec()).unwrap();
        wal.append(WalOperation::Append, msg2, b"m2".to_vec()).unwrap();
        wal.append(WalOperation::Ack, msg1, vec![]).unwrap();
        wal.sync().unwrap();

        // Compact: keep only msg2's append
        let records = vec![WalRecord::new(1, WalOperation::Append, msg2, b"m2".to_vec())];
        wal.compact(&records).unwrap();

        let read_back = Wal::read_all_records(&wal_path).unwrap();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].message_id, msg2);
    }

    #[test]
    fn test_wal_eof_returns_none() {
        let mut cursor = io::Cursor::new(Vec::new());
        let result = WalRecord::decode(&mut cursor).unwrap();
        assert!(result.is_none());
    }
}
