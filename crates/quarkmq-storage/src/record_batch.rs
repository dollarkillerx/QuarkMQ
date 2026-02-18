use crate::error::{Result, StorageError};

/// Minimum size of a RecordBatch header (before individual records).
pub const RECORD_BATCH_HEADER_SIZE: usize = 61;

/// Offsets within the batch for key fields.
const BASE_OFFSET_OFFSET: usize = 0;       // i64, bytes 0..8
const BATCH_LENGTH_OFFSET: usize = 8;       // i32, bytes 8..12
const MAGIC_OFFSET: usize = 16;             // i8, byte 16
const FIRST_TIMESTAMP_OFFSET: usize = 25;   // i64, bytes 25..33
const MAX_TIMESTAMP_OFFSET: usize = 33;     // i64, bytes 33..41
const RECORD_COUNT_OFFSET: usize = 57;      // i32, bytes 57..61

/// Set the base_offset field (first 8 bytes) of a RecordBatch.
pub fn set_batch_base_offset(batch: &mut [u8], offset: i64) {
    batch[BASE_OFFSET_OFFSET..BASE_OFFSET_OFFSET + 8]
        .copy_from_slice(&offset.to_be_bytes());
}

/// Read the base_offset from a RecordBatch.
pub fn get_batch_base_offset(batch: &[u8]) -> i64 {
    i64::from_be_bytes(
        batch[BASE_OFFSET_OFFSET..BASE_OFFSET_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Read the batch length field. This does NOT include the 12-byte prefix
/// (8 bytes base_offset + 4 bytes batch_length).
pub fn get_batch_length(batch: &[u8]) -> i32 {
    i32::from_be_bytes(
        batch[BATCH_LENGTH_OFFSET..BATCH_LENGTH_OFFSET + 4]
            .try_into()
            .unwrap(),
    )
}

/// Total size of the batch on disk:
///   8 (base_offset) + 4 (batch_length) + batch_length
pub fn get_batch_total_size(batch: &[u8]) -> usize {
    12 + get_batch_length(batch) as usize
}

/// Get the record count from the batch header.
pub fn get_record_count(batch: &[u8]) -> Result<i32> {
    if batch.len() < RECORD_BATCH_HEADER_SIZE {
        return Err(StorageError::InvalidRecordBatch(
            "batch too small".into(),
        ));
    }
    Ok(i32::from_be_bytes(
        batch[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + 4]
            .try_into()
            .unwrap(),
    ))
}

/// Get the first timestamp from the batch.
pub fn get_first_timestamp(batch: &[u8]) -> i64 {
    i64::from_be_bytes(
        batch[FIRST_TIMESTAMP_OFFSET..FIRST_TIMESTAMP_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Get the max timestamp from the batch.
pub fn get_max_timestamp(batch: &[u8]) -> i64 {
    i64::from_be_bytes(
        batch[MAX_TIMESTAMP_OFFSET..MAX_TIMESTAMP_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Validate that this looks like a valid RecordBatch (magic byte = 2).
pub fn validate_batch(batch: &[u8]) -> Result<()> {
    if batch.len() < RECORD_BATCH_HEADER_SIZE {
        return Err(StorageError::InvalidRecordBatch(
            "batch too small".into(),
        ));
    }
    let magic = batch[MAGIC_OFFSET] as i8;
    if magic != 2 {
        return Err(StorageError::InvalidRecordBatch(format!(
            "unsupported magic: {}",
            magic
        )));
    }
    let batch_length = get_batch_length(batch);
    if batch_length < 0 || (12 + batch_length as usize) > batch.len() {
        return Err(StorageError::InvalidRecordBatch(
            "invalid batch length".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a minimal valid RecordBatch buffer.
    fn make_batch(
        base_offset: i64,
        batch_length: i32,
        magic: i8,
        first_ts: i64,
        max_ts: i64,
        record_count: i32,
    ) -> Vec<u8> {
        // Total size on disk = 12 + batch_length.
        // batch_length must be >= 49 (header after first 12 bytes = 61 - 12 = 49).
        let total = 12 + batch_length as usize;
        let mut buf = vec![0u8; total];
        // base_offset: bytes 0..8
        buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
        // batch_length: bytes 8..12
        buf[8..12].copy_from_slice(&batch_length.to_be_bytes());
        // magic: byte 16
        buf[MAGIC_OFFSET] = magic as u8;
        // first_timestamp: bytes 25..33
        buf[25..33].copy_from_slice(&first_ts.to_be_bytes());
        // max_timestamp: bytes 33..41
        buf[33..41].copy_from_slice(&max_ts.to_be_bytes());
        // record_count: bytes 57..61
        buf[57..61].copy_from_slice(&record_count.to_be_bytes());
        buf
    }

    #[test]
    fn test_get_and_set_base_offset() {
        let mut batch = make_batch(42, 49, 2, 1000, 2000, 3);
        assert_eq!(get_batch_base_offset(&batch), 42);
        set_batch_base_offset(&mut batch, 99);
        assert_eq!(get_batch_base_offset(&batch), 99);
    }

    #[test]
    fn test_get_batch_length() {
        let batch = make_batch(0, 100, 2, 0, 0, 1);
        assert_eq!(get_batch_length(&batch), 100);
    }

    #[test]
    fn test_get_batch_total_size() {
        let batch = make_batch(0, 100, 2, 0, 0, 1);
        assert_eq!(get_batch_total_size(&batch), 112);
    }

    #[test]
    fn test_get_record_count() {
        let batch = make_batch(0, 49, 2, 0, 0, 7);
        assert_eq!(get_record_count(&batch).unwrap(), 7);
    }

    #[test]
    fn test_get_record_count_too_small() {
        let batch = vec![0u8; 10];
        assert!(get_record_count(&batch).is_err());
    }

    #[test]
    fn test_get_timestamps() {
        let batch = make_batch(0, 49, 2, 12345, 67890, 1);
        assert_eq!(get_first_timestamp(&batch), 12345);
        assert_eq!(get_max_timestamp(&batch), 67890);
    }

    #[test]
    fn test_validate_batch_valid() {
        let batch = make_batch(0, 49, 2, 0, 0, 1);
        assert!(validate_batch(&batch).is_ok());
    }

    #[test]
    fn test_validate_batch_too_small() {
        let batch = vec![0u8; 10];
        assert!(validate_batch(&batch).is_err());
    }

    #[test]
    fn test_validate_batch_wrong_magic() {
        let batch = make_batch(0, 49, 1, 0, 0, 1);
        let err = validate_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("unsupported magic"));
    }

    #[test]
    fn test_validate_batch_invalid_length() {
        // batch_length says 1000 but buffer is only 61 bytes
        let mut batch = make_batch(0, 49, 2, 0, 0, 1);
        // Overwrite batch_length to something huge
        batch[8..12].copy_from_slice(&1000i32.to_be_bytes());
        let err = validate_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("invalid batch length"));
    }

    #[test]
    fn test_validate_batch_negative_length() {
        let mut batch = make_batch(0, 49, 2, 0, 0, 1);
        batch[8..12].copy_from_slice(&(-1i32).to_be_bytes());
        let err = validate_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("invalid batch length"));
    }
}
