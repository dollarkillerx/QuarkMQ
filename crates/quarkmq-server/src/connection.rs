use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use quarkmq_broker::Broker;
use quarkmq_protocol::frame::{read_request_frame, write_response_frame};
use quarkmq_protocol::handler::parse_request;
use crate::handlers::dispatch_request;
use tracing::{debug, error, info};

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
            Err(quarkmq_protocol::ProtocolError::ConnectionClosed) => {
                debug!("Connection closed by {}", addr);
                break;
            }
            Err(e) => {
                error!("Frame read error from {}: {}", addr, e);
                break;
            }
        };

        let request = match parse_request(frame) {
            Ok(req) => req,
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
