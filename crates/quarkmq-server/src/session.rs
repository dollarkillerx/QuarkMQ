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

/// Build a success response, falling back to an internal error if serialization fails.
fn success_or_internal_error(
    id: Option<serde_json::Value>,
    result: impl serde::Serialize,
) -> JsonRpcResponse {
    match serde_json::to_value(result) {
        Ok(v) => JsonRpcResponse::success(id, v),
        Err(e) => JsonRpcResponse::error(id, rpc::INTERNAL_ERROR, e.to_string()),
    }
}

pub struct Session {
    consumer_id: ConsumerId,
    dispatcher: Arc<Dispatcher>,
    config: Arc<Config>,
    /// Sender for outbound messages to this consumer
    #[allow(dead_code)]
    outbound_tx: mpsc::Sender<String>,
}

impl Session {
    pub fn new(
        dispatcher: Arc<Dispatcher>,
        config: Arc<Config>,
        outbound_tx: mpsc::Sender<String>,
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
        let id = request.id;
        let params = request.params;
        match request.method.as_str() {
            "publish" => self.handle_publish(id, params),
            "subscribe" => self.handle_subscribe(id, params),
            "ack" => self.handle_ack(id, params),
            "nack" => self.handle_nack(id, params),
            "create_channel" => self.handle_create_channel(id, params),
            "delete_channel" => self.handle_delete_channel(id, params),
            "list_channels" => self.handle_list_channels(id),
            "list_dlq" => self.handle_list_dlq(id, params),
            "retry_dlq" => self.handle_retry_dlq(id, params),
            _ => JsonRpcResponse::error(
                id,
                rpc::METHOD_NOT_FOUND,
                format!("unknown method: {}", request.method),
            ),
        }
    }

    fn handle_publish(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: PublishParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        let message = Message::new(&params.channel, params.payload);
        match self.dispatcher.publish(&params.channel, message) {
            Ok(msg_id) => {
                let result = rpc::PublishResult { message_id: msg_id };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_subscribe(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: SubscribeParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self
            .dispatcher
            .subscribe(&params.channel, &params.topic, self.consumer_id)
        {
            Ok(()) => {
                let result = rpc::SubscribeResult { success: true };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_ack(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: AckParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.ack(self.consumer_id, &params.message_id) {
            Ok(()) => {
                let result = rpc::AckResult { success: true };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_nack(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: NackParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.nack(self.consumer_id, &params.message_id) {
            Ok(()) => {
                let result = rpc::NackResult { success: true };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_create_channel(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: CreateChannelParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
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
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_delete_channel(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: rpc::DeleteChannelParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.delete_channel(&params.name) {
            Ok(()) => {
                let result = rpc::DeleteChannelResult { success: true };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_list_channels(&self, id: Option<serde_json::Value>) -> JsonRpcResponse {
        let channels = self.dispatcher.list_channels();
        let result = rpc::ListChannelsResult { channels };
        success_or_internal_error(id, result)
    }

    fn handle_list_dlq(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: rpc::ListDlqParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.list_dlq(&params.channel) {
            Ok(messages) => {
                let result = rpc::ListDlqResult { messages };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
    }

    fn handle_retry_dlq(&self, id: Option<serde_json::Value>, params: serde_json::Value) -> JsonRpcResponse {
        let params: rpc::RetryDlqParams = match rpc::parse_params(params) {
            Ok(p) => p,
            Err(e) => {
                return JsonRpcResponse::error(id, rpc::INVALID_PARAMS, e.to_string())
            }
        };

        match self.dispatcher.retry_dlq(&params.channel, &params.message_id) {
            Ok(()) => {
                let result = rpc::RetryDlqResult { success: true };
                success_or_internal_error(id, result)
            }
            Err(e) => self.broker_error_to_rpc(id, e),
        }
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
            BrokerError::MessageDeadLettered(_) => (rpc::MESSAGE_NOT_FOUND, error.to_string()),
            BrokerError::Storage(_) => (rpc::INTERNAL_ERROR, error.to_string()),
            _ => (rpc::INTERNAL_ERROR, error.to_string()),
        };
        JsonRpcResponse::error(id, code, msg)
    }

    /// Send a message push notification to this consumer's WebSocket.
    #[allow(dead_code)]
    pub fn send_message(&self, msg: &Message) {
        let push = MessagePush::from_message(msg);
        let notification = push.into_notification();
        if let Ok(json) = serde_json::to_string(&notification) {
            let _ = self.outbound_tx.try_send(json);
        }
    }
}

/// Run a WebSocket session: reads inbound, writes outbound, dispatches to broker.
pub async fn run_session(
    ws_stream: WebSocketStream<TcpStream>,
    dispatcher: Arc<Dispatcher>,
    config: Arc<Config>,
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::Sender<String>>>,
) {
    let (mut ws_sink, mut ws_stream_read) = ws_stream.split();

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<String>(1024);

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
                                let _ = tx.try_send(json);
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
                                let _ = tx.try_send(json);
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
