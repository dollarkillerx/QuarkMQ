use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use quarkmq_broker::Broker;
use quarkmq_protocol::frame::{read_request_frame, write_response_frame};
use quarkmq_protocol::handler::parse_request;
use quarkmq_protocol::ProtocolError;
use crate::handlers::{build_error_response, dispatch_request};
use tracing::{debug, error, info, warn};

pub async fn handle_connection<S>(stream: S, broker: Arc<Broker>, addr: std::net::SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    info!("New connection from {}", addr);

    loop {
        let frame = match read_request_frame(&mut reader).await {
            Ok(frame) => frame,
            Err(ProtocolError::ConnectionClosed) => {
                debug!("Connection closed by {}", addr);
                break;
            }
            Err(e) => {
                error!("Frame read error from {}: {}", addr, e);
                break;
            }
        };

        // Peek at correlation_id (bytes 4..8) before parsing, so we can
        // construct an error response even if parse_request fails for
        // unknown API keys.
        let correlation_id = if frame.len() >= 8 {
            i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]])
        } else {
            0
        };

        let request = match parse_request(frame) {
            Ok(req) => req,
            Err(ProtocolError::UnsupportedApiKey(key)) => {
                // Unknown API key — return error response, keep connection alive.
                warn!("Unknown API key {} from {}, returning error response", key, addr);
                let response = match build_error_response(correlation_id, 35) {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!("Failed to build error response: {}", e);
                        break;
                    }
                };
                if let Err(e) = write_response_frame(&mut writer, &response).await {
                    error!("Write error to {}: {}", addr, e);
                    break;
                }
                continue;
            }
            Err(e) => {
                error!("Request parse error from {}: {}", addr, e);
                break;
            }
        };

        let response = match dispatch_request(request, &broker).await {
            Ok(resp) => resp,
            Err(e) => {
                error!("Handler error from {}: {}", addr, e);
                break;
            }
        };

        if let Err(e) = write_response_frame(&mut writer, &response).await {
            error!("Write error to {}: {}", addr, e);
            break;
        }
    }

    info!("Connection from {} closed", addr);
}
