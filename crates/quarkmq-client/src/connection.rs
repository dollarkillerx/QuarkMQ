use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use quarkmq_protocol::rpc::{JsonRpcRequest, JsonRpcResponse};

use crate::error::ClientError;

type PendingRequests = Arc<Mutex<HashMap<u64, oneshot::Sender<JsonRpcResponse>>>>;

pub struct Connection {
    outbound_tx: mpsc::Sender<String>,
    pending: PendingRequests,
    next_id: AtomicU64,
    /// Receiver for server-push notifications (method="message")
    pub notification_rx: mpsc::Receiver<JsonRpcRequest>,
}

impl Connection {
    pub async fn connect(url: &str) -> Result<Self, ClientError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))?;

        let (mut ws_sink, mut ws_read) = ws_stream.split();

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<String>(1024);
        let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));
        let (notification_tx, notification_rx) = mpsc::channel::<JsonRpcRequest>(1024);

        // Write task
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if ws_sink.send(WsMessage::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        });

        // Read task
        let pending_clone = pending.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_read.next().await {
                if let WsMessage::Text(text) = msg {
                    let text_str: &str = &text;
                    // Try parsing as response first (has result/error fields)
                    if let Ok(resp) = serde_json::from_str::<JsonRpcResponse>(text_str) {
                        if let Some(id) = &resp.id {
                            if let Some(id_num) = id.as_u64() {
                                let mut pending = pending_clone.lock().await;
                                if let Some(tx) = pending.remove(&id_num) {
                                    let _ = tx.send(resp);
                                }
                                continue;
                            }
                        }
                    }
                    // Otherwise treat as notification
                    if let Ok(notif) = serde_json::from_str::<JsonRpcRequest>(text_str) {
                        let _ = notification_tx.try_send(notif);
                    }
                }
            }
        });

        Ok(Self {
            outbound_tx,
            pending,
            next_id: AtomicU64::new(1),
            notification_rx,
        })
    }

    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<JsonRpcResponse, ClientError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: method.to_string(),
            params,
            id: Some(serde_json::Value::Number(id.into())),
        };

        let json = serde_json::to_string(&request)?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(id, tx);
        }

        self.outbound_tx
            .try_send(json)
            .map_err(|_| ClientError::ConnectionClosed)?;

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), rx)
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|_| ClientError::ConnectionClosed)?;

        if let Some(err) = &response.error {
            return Err(ClientError::Rpc {
                code: err.code,
                message: err.message.clone(),
            });
        }

        Ok(response)
    }
}
