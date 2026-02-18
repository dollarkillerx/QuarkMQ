use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::WebSocketStream;

use quarkmq_broker::Dispatcher;
use quarkmq_protocol::message::Message;
use quarkmq_protocol::rpc::{
    self, AckParams, ConsumerId, CreateChannelParams, JsonRpcRequest, JsonRpcResponse,
    MessagePush, NackParams, PublishParams, SubscribeParams,
    new_consumer_id,
};

use crate::config::Config;

pub struct Session {
    consumer_id: ConsumerId,
    dispatcher: Arc<Dispatcher>,
    config: Arc<Config>,
    /// Sender for outbound messages to this consumer
    outbound_tx: mpsc::UnboundedSender<String>,
}

impl Session {
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        config: Arc<Config>,
        outbound_tx: mpsc::UnboundedSender<String>,
    ) -> Self {
        Self {
            consumer_id: new_consumer_id(),
            dispatcher,
            config,
            outbound_tx,
        }
    }

    pub fn consumer_id(&self) -> ConsumerId {
        self.consumer_id
    }

    pub fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        match request.method.as_str() {
            "publish" => self.handle_publish(&request),
            "subscribe" => self.handle_subscribe(&request),
            "ack" => self.handle_ack(&request),
            "nack" => self.handle_nack(&request),
            "create_channel" => self.handle_create_channel(&request),
            "delete_channel" => self.handle_delete_channel(&request),
            "list_channels" => self.handle_list_channels(&request),
            _ => JsonRpcResponse::error(
                request.id,
                rpc::METHOD_NOT_FOUND,
                format!("unknown method: {}", request.method),
            ),
        }
    }

    fn handle_publish(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let params: PublishParams = match rpc::parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(req.id.clone(), rpc::INVALID_PARAMS, e.to_string())
            }
        };

        let message = Message::new(&params.channel, params.payload);
        match self.dispatcher.publish(&params.channel, message) {
            Ok(msg_id) => {
                let result = rpc::PublishResult { message_id: msg_id };
                JsonRpcResponse::success(
                    req.id.clone(),
                    serde_json::to_value(result).unwrap(),
                )
            }
            Err(e) => self.broker_error_to_rpc(req.id.clone(), e),
        }
    }

    fn handle_subscribe(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let params: SubscribeParams = match rpc::parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(req.id.clone(), rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self
            .dispatcher
            .subscribe(&params.channel, &params.topic, self.consumer_id)
        {
            Ok(()) => {
                let result = rpc::SubscribeResult { success: true };
                JsonRpcResponse::success(
                    req.id.clone(),
                    serde_json::to_value(result).unwrap(),
                )
            }
            Err(e) => self.broker_error_to_rpc(req.id.clone(), e),
        }
    }

    fn handle_ack(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let params: AckParams = match rpc::parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(req.id.clone(), rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.ack(self.consumer_id, &params.message_id) {
            Ok(()) => {
                let result = rpc::AckResult { success: true };
                JsonRpcResponse::success(
                    req.id.clone(),
                    serde_json::to_value(result).unwrap(),
                )
            }
            Err(e) => self.broker_error_to_rpc(req.id.clone(), e),
        }
    }

    fn handle_nack(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let params: NackParams = match rpc::parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(req.id.clone(), rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.nack(self.consumer_id, &params.message_id) {
            Ok(()) => {
                let result = rpc::NackResult { success: true };
                JsonRpcResponse::success(
                    req.id.clone(),
                    serde_json::to_value(result).unwrap(),
                )
            }
            Err(e) => self.broker_error_to_rpc(req.id.clone(), e),
        }
    }

    fn handle_create_channel(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let params: CreateChannelParams = match rpc::parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(req.id.clone(), rpc::INVALID_PARAMS, e.to_string())
            }
        };

        let config = quarkmq_broker::channel::ChannelConfig {
            name: params.name.clone(),
            ack_timeout_secs: params.ack_timeout_secs,
            max_delivery_attempts: params.max_delivery_attempts,
            max_inflight_per_consumer: self.config.channels.max_inflight_per_consumer,
        };

        match self.dispatcher.create_channel(config) {
            Ok(()) => {
                let result = rpc::CreateChannelResult {
                    name: params.name,
                };
                JsonRpcResponse::success(
                    req.id.clone(),
                    serde_json::to_value(result).unwrap(),
                )
            }
            Err(e) => self.broker_error_to_rpc(req.id.clone(), e),
        }
    }

    fn handle_delete_channel(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let params: rpc::DeleteChannelParams = match rpc::parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(req.id.clone(), rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.delete_channel(&params.name) {
            Ok(()) => {
                let result = rpc::DeleteChannelResult { success: true };
                JsonRpcResponse::success(
                    req.id.clone(),
                    serde_json::to_value(result).unwrap(),
                )
            }
            Err(e) => self.broker_error_to_rpc(req.id.clone(), e),
        }
    }

    fn handle_list_channels(&self, req: &JsonRpcRequest) -> JsonRpcResponse {
        let channels = self.dispatcher.list_channels();
        let result = rpc::ListChannelsResult { channels };
        JsonRpcResponse::success(
            req.id.clone(),
            serde_json::to_value(result).unwrap(),
        )
    }

    fn broker_error_to_rpc(
        &self,
        id: Option<serde_json::Value>,
        error: quarkmq_broker::BrokerError,
    ) -> JsonRpcResponse {
        use quarkmq_broker::BrokerError;
        let (code, msg) = match &error {
            BrokerError::ChannelNotFound(_) => (rpc::CHANNEL_NOT_FOUND, error.to_string()),
            BrokerError::ChannelAlreadyExists(_) => {
                (rpc::CHANNEL_ALREADY_EXISTS, error.to_string())
            }
            BrokerError::TopicNotFound(_) => (rpc::NOT_SUBSCRIBED, error.to_string()),
            BrokerError::NotSubscribed => (rpc::NOT_SUBSCRIBED, error.to_string()),
            BrokerError::MessageNotFound(_) => (rpc::MESSAGE_NOT_FOUND, error.to_string()),
            BrokerError::MessageNotInflight(_) => (rpc::MESSAGE_NOT_FOUND, error.to_string()),
            _ => (rpc::INTERNAL_ERROR, error.to_string()),
        };
        JsonRpcResponse::error(id, code, msg)
    }

    /// Send a message push notification to this consumer's WebSocket.
    pub fn send_message(&self, msg: &Message) {
        let push = MessagePush::from_message(msg);
        let notification = push.into_notification();
        if let Ok(json) = serde_json::to_string(&notification) {
            let _ = self.outbound_tx.send(json);
        }
    }
}

/// Run a WebSocket session: reads inbound, writes outbound, dispatches to broker.
pub async fn run_session(
    ws_stream: WebSocketStream<TcpStream>,
    dispatcher: Arc<Dispatcher>,
    config: Arc<Config>,
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::UnboundedSender<String>>>,
) {
    let (mut ws_sink, mut ws_stream_read) = ws_stream.split();

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<String>();

    let session = Arc::new(Session::new(dispatcher.clone(), config, outbound_tx.clone()));
    let consumer_id = session.consumer_id();

    sessions.insert(consumer_id, outbound_tx);

    tracing::info!(consumer_id = %consumer_id, "new session connected");

    // Task to forward outbound messages to WebSocket
    let write_task = tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            if ws_sink.send(WsMessage::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    // Read inbound messages
    while let Some(result) = ws_stream_read.next().await {
        match result {
            Ok(WsMessage::Text(text)) => {
                let text_str: &str = &text;
                match serde_json::from_str::<JsonRpcRequest>(text_str) {
                    Ok(request) => {
                        let response = session.handle_request(request);
                        if let Ok(json) = serde_json::to_string(&response) {
                            if let Some(tx) = sessions.get(&consumer_id) {
                                let _ = tx.send(json);
                            }
                        }
                    }
                    Err(e) => {
                        let response = JsonRpcResponse::error(
                            None,
                            rpc::PARSE_ERROR,
                            format!("invalid JSON: {e}"),
                        );
                        if let Ok(json) = serde_json::to_string(&response) {
                            if let Some(tx) = sessions.get(&consumer_id) {
                                let _ = tx.send(json);
                            }
                        }
                    }
                }
            }
            Ok(WsMessage::Close(_)) => break,
            Err(e) => {
                tracing::warn!(consumer_id = %consumer_id, error = %e, "WebSocket error");
                break;
            }
            _ => {} // Ignore ping/pong/binary
        }
    }

    tracing::info!(consumer_id = %consumer_id, "session disconnected");
    sessions.remove(&consumer_id);
    dispatcher.disconnect_consumer(consumer_id);
    write_task.abort();
}
