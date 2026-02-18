use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: u32, max: u32 },
    #[error("unsupported API key: {0}")]
    UnsupportedApiKey(i16),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let proto_err: ProtocolError = io_err.into();
        assert!(matches!(proto_err, ProtocolError::Io(_)));
        assert!(proto_err.to_string().contains("pipe broken"));
    }

    #[test]
    fn test_frame_too_large_display() {
        let err = ProtocolError::FrameTooLarge {
            size: 200_000_000,
            max: 100_000_000,
        };
        assert!(err.to_string().contains("200000000"));
        assert!(err.to_string().contains("100000000"));
    }

    #[test]
    fn test_unsupported_api_key_display() {
        let err = ProtocolError::UnsupportedApiKey(999);
        assert!(err.to_string().contains("999"));
    }

    #[test]
    fn test_decode_error_display() {
        let err = ProtocolError::Decode("bad bytes".into());
        assert!(err.to_string().contains("bad bytes"));
    }

    #[test]
    fn test_connection_closed_display() {
        let err = ProtocolError::ConnectionClosed;
        assert_eq!(err.to_string(), "connection closed");
    }
}
