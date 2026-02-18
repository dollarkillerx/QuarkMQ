use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{ProtocolError, Result};

/// Maximum allowed frame size: 100 MB.
const MAX_FRAME_SIZE: u32 = 100 * 1024 * 1024;

/// Read one request frame from a TCP stream.
///
/// The Kafka wire protocol uses a 4-byte big-endian length prefix followed by
/// the body bytes. This function reads and validates the length prefix, then
/// reads exactly that many body bytes and returns them.
pub async fn read_request_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<BytesMut> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ProtocolError::ConnectionClosed);
        }
        Err(e) => return Err(e.into()),
    }
    let size = u32::from_be_bytes(len_buf);
    if size > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            size,
            max: MAX_FRAME_SIZE,
        });
    }
    let mut body = BytesMut::zeroed(size as usize);
    reader.read_exact(&mut body).await?;
    Ok(body)
}

/// Write a response frame to a TCP stream.
///
/// Prepends a 4-byte big-endian length prefix before writing the body bytes,
/// then flushes the writer.
pub async fn write_response_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
) -> Result<()> {
    let len = body.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_read_request_frame_basic() {
        let payload = b"hello world";
        let len = (payload.len() as u32).to_be_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&len);
        data.extend_from_slice(payload);

        let mut cursor = Cursor::new(data);
        let frame = read_request_frame(&mut cursor).await.unwrap();
        assert_eq!(&frame[..], payload);
    }

    #[tokio::test]
    async fn test_read_request_frame_empty_body() {
        let len = 0u32.to_be_bytes();
        let mut cursor = Cursor::new(len.to_vec());
        let frame = read_request_frame(&mut cursor).await.unwrap();
        assert!(frame.is_empty());
    }

    #[tokio::test]
    async fn test_read_request_frame_connection_closed() {
        // Empty stream -> UnexpectedEof -> ConnectionClosed
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let err = read_request_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, ProtocolError::ConnectionClosed));
    }

    #[tokio::test]
    async fn test_read_request_frame_partial_length() {
        // Only 2 bytes of the 4-byte length prefix -> UnexpectedEof -> ConnectionClosed
        let mut cursor = Cursor::new(vec![0u8, 1]);
        let err = read_request_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, ProtocolError::ConnectionClosed));
    }

    #[tokio::test]
    async fn test_read_request_frame_too_large() {
        let huge_size: u32 = MAX_FRAME_SIZE + 1;
        let len = huge_size.to_be_bytes();
        let mut cursor = Cursor::new(len.to_vec());
        let err = read_request_frame(&mut cursor).await.unwrap_err();
        match err {
            ProtocolError::FrameTooLarge { size, max } => {
                assert_eq!(size, huge_size);
                assert_eq!(max, MAX_FRAME_SIZE);
            }
            other => panic!("expected FrameTooLarge, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_write_response_frame_basic() {
        let payload = b"response data";
        let mut buf = Vec::new();
        write_response_frame(&mut buf, payload).await.unwrap();

        // First 4 bytes are the length prefix
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, payload.len() as u32);
        assert_eq!(&buf[4..], payload);
    }

    #[tokio::test]
    async fn test_write_response_frame_empty() {
        let mut buf = Vec::new();
        write_response_frame(&mut buf, b"").await.unwrap();

        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 0);
        assert_eq!(buf.len(), 4);
    }

    #[tokio::test]
    async fn test_roundtrip_frame() {
        let original = b"roundtrip test payload with \x00 binary \xff data";
        let mut wire = Vec::new();
        write_response_frame(&mut wire, original).await.unwrap();

        let mut cursor = Cursor::new(wire);
        let frame = read_request_frame(&mut cursor).await.unwrap();
        assert_eq!(&frame[..], &original[..]);
    }

    #[tokio::test]
    async fn test_multiple_frames_sequential() {
        let payloads: Vec<&[u8]> = vec![b"first", b"second", b"third"];
        let mut wire = Vec::new();
        for p in &payloads {
            write_response_frame(&mut wire, p).await.unwrap();
        }

        let mut cursor = Cursor::new(wire);
        for p in &payloads {
            let frame = read_request_frame(&mut cursor).await.unwrap();
            assert_eq!(&frame[..], *p);
        }
    }

    #[tokio::test]
    async fn test_roundtrip_with_duplex() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let payload = b"duplex roundtrip";

        let write_handle = tokio::spawn(async move {
            write_response_frame(&mut client, payload).await.unwrap();
        });

        let frame = read_request_frame(&mut server).await.unwrap();
        assert_eq!(&frame[..], &payload[..]);
        write_handle.await.unwrap();
    }
}
