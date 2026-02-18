use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::message::MessageId;

// --- JSON-RPC 2.0 Base Types ---

const JSONRPC_VERSION: &str = "2.0";

fn default_jsonrpc() -> String {
    JSONRPC_VERSION.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default = "default_jsonrpc")]
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    #[serde(default = "default_jsonrpc")]
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl JsonRpcResponse {
    pub fn success(id: Option<serde_json::Value>, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Option<serde_json::Value>, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
                data: None,
            }),
            id,
        }
    }

    pub fn notification(method: &str, params: serde_json::Value) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method: method.to_string(),
            params,
            id: None,
        }
    }
}

// --- RPC Error Codes ---
pub const PARSE_ERROR: i32 = -32700;
pub const INVALID_REQUEST: i32 = -32600;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
pub const INTERNAL_ERROR: i32 = -32603;
pub const CHANNEL_NOT_FOUND: i32 = -32000;
pub const CHANNEL_ALREADY_EXISTS: i32 = -32001;
pub const NOT_SUBSCRIBED: i32 = -32002;
pub const MESSAGE_NOT_FOUND: i32 = -32003;

// --- Method Params ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishParams {
    pub channel: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeParams {
    pub channel: String,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckParams {
    pub message_id: MessageId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NackParams {
    pub message_id: MessageId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateChannelParams {
    pub name: String,
    #[serde(default = "default_ack_timeout")]
    pub ack_timeout_secs: u64,
    #[serde(default = "default_max_attempts")]
    pub max_delivery_attempts: u32,
}

fn default_ack_timeout() -> u64 {
    30
}

fn default_max_attempts() -> u32 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteChannelParams {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListChannelsParams {}

// --- Method Results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishResult {
    pub message_id: MessageId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NackResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateChannelResult {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteChannelResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub name: String,
    pub topics: Vec<String>,
    pub pending_count: usize,
    #[serde(default)]
    pub dlq_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListChannelsResult {
    pub channels: Vec<ChannelInfo>,
}

// --- DLQ types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDlqParams {
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    pub message_id: MessageId,
    pub channel: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDlqResult {
    pub messages: Vec<DlqMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryDlqParams {
    pub channel: String,
    pub message_id: MessageId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryDlqResult {
    pub success: bool,
}

// --- Server Push (notification) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePush {
    pub message_id: MessageId,
    pub channel: String,
    pub payload: serde_json::Value,
    pub attempt: u32,
}

impl MessagePush {
    pub fn from_message(msg: &crate::message::Message) -> Self {
        Self {
            message_id: msg.id,
            channel: msg.channel.clone(),
            payload: msg.payload.clone(),
            attempt: msg.attempt,
        }
    }

    pub fn into_notification(self) -> JsonRpcRequest {
        let params = match serde_json::to_value(&self) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("BUG: failed to serialize MessagePush: {e}");
                serde_json::Value::Null
            }
        };
        JsonRpcRequest {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method: "message".to_string(),
            params,
            id: None,
        }
    }
}

// --- Helper to parse method params ---

pub fn parse_params<T: serde::de::DeserializeOwned>(
    params: serde_json::Value,
) -> Result<T, crate::ProtocolError> {
    serde_json::from_value(params).map_err(|e| {
        crate::ProtocolError::InvalidParams(e.to_string())
    })
}

// --- Identify consumer sessions ---

pub type ConsumerId = Uuid;

pub fn new_consumer_id() -> ConsumerId {
    Uuid::now_v7()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- JsonRpcRequest serialization ---

    #[test]
    fn test_json_rpc_request_serialization_matches_spec() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "subtract".to_string(),
            params: json!({"minuend": 42, "subtrahend": 23}),
            id: Some(json!(1)),
        };

        let value = serde_json::to_value(&req).expect("serialize");
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["method"], "subtract");
        assert_eq!(value["id"], 1);
        assert!(value["params"].is_object());
    }

    #[test]
    fn test_json_rpc_request_notification_has_null_id() {
        let req = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "notify".to_string(),
            params: json!({}),
            id: None,
        };

        let value = serde_json::to_value(&req).expect("serialize");
        assert!(value["id"].is_null());
    }

    // --- JsonRpcResponse::success and ::error ---

    #[test]
    fn test_json_rpc_response_success() {
        let resp = JsonRpcResponse::success(Some(json!(1)), json!({"ok": true}));

        assert_eq!(resp.jsonrpc, "2.0");
        assert_eq!(resp.id, Some(json!(1)));
        assert_eq!(resp.result, Some(json!({"ok": true})));
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_json_rpc_response_success_serialization_omits_error() {
        let resp = JsonRpcResponse::success(Some(json!(1)), json!("result"));
        let value = serde_json::to_value(&resp).expect("serialize");

        assert!(value.get("error").is_none(), "error field should be omitted via skip_serializing_if");
        assert!(value.get("result").is_some());
    }

    #[test]
    fn test_json_rpc_response_error() {
        let resp = JsonRpcResponse::error(Some(json!(2)), INVALID_PARAMS, "bad params");

        assert_eq!(resp.jsonrpc, "2.0");
        assert_eq!(resp.id, Some(json!(2)));
        assert!(resp.result.is_none());
        let err = resp.error.as_ref().expect("should have error");
        assert_eq!(err.code, INVALID_PARAMS);
        assert_eq!(err.message, "bad params");
        assert!(err.data.is_none());
    }

    #[test]
    fn test_json_rpc_response_error_serialization_omits_result() {
        let resp = JsonRpcResponse::error(Some(json!(2)), METHOD_NOT_FOUND, "not found");
        let value = serde_json::to_value(&resp).expect("serialize");

        assert!(value.get("result").is_none(), "result field should be omitted via skip_serializing_if");
        assert!(value.get("error").is_some());
    }

    // --- PublishParams, SubscribeParams, AckParams deserialization ---

    #[test]
    fn test_publish_params_deserialization() {
        let input = json!({"channel": "orders", "payload": {"item": "widget"}});
        let params: PublishParams = serde_json::from_value(input).expect("deserialize");

        assert_eq!(params.channel, "orders");
        assert_eq!(params.payload, json!({"item": "widget"}));
    }

    #[test]
    fn test_subscribe_params_deserialization() {
        let input = json!({"channel": "events", "topic": "user.created"});
        let params: SubscribeParams = serde_json::from_value(input).expect("deserialize");

        assert_eq!(params.channel, "events");
        assert_eq!(params.topic, "user.created");
    }

    #[test]
    fn test_ack_params_deserialization() {
        let id = Uuid::now_v7();
        let input = json!({"message_id": id.to_string()});
        let params: AckParams = serde_json::from_value(input).expect("deserialize");

        assert_eq!(params.message_id, id);
    }

    // --- CreateChannelParams with defaults ---

    #[test]
    fn test_create_channel_params_defaults() {
        let input = json!({"name": "my-channel"});
        let params: CreateChannelParams = serde_json::from_value(input).expect("deserialize");

        assert_eq!(params.name, "my-channel");
        assert_eq!(params.ack_timeout_secs, 30, "default ack_timeout_secs should be 30");
        assert_eq!(params.max_delivery_attempts, 5, "default max_delivery_attempts should be 5");
    }

    #[test]
    fn test_create_channel_params_explicit_values_override_defaults() {
        let input = json!({"name": "ch", "ack_timeout_secs": 60, "max_delivery_attempts": 10});
        let params: CreateChannelParams = serde_json::from_value(input).expect("deserialize");

        assert_eq!(params.ack_timeout_secs, 60);
        assert_eq!(params.max_delivery_attempts, 10);
    }

    // --- MessagePush::into_notification ---

    #[test]
    fn test_message_push_into_notification_format() {
        let id = Uuid::now_v7();
        let push = MessagePush {
            message_id: id,
            channel: "test-ch".to_string(),
            payload: json!({"data": 1}),
            attempt: 2,
        };

        let notification = push.into_notification();

        assert_eq!(notification.jsonrpc, "2.0");
        assert_eq!(notification.method, "message");
        assert!(notification.id.is_none(), "notification must not have an id");
        // params should contain all the MessagePush fields
        assert_eq!(notification.params["message_id"], id.to_string());
        assert_eq!(notification.params["channel"], "test-ch");
        assert_eq!(notification.params["payload"], json!({"data": 1}));
        assert_eq!(notification.params["attempt"], 2);
    }

    #[test]
    fn test_message_push_from_message() {
        let msg = crate::message::Message::new("ch", json!("hello"));
        let push = MessagePush::from_message(&msg);

        assert_eq!(push.message_id, msg.id);
        assert_eq!(push.channel, msg.channel);
        assert_eq!(push.payload, msg.payload);
        assert_eq!(push.attempt, msg.attempt);
    }

    // --- parse_params helper ---

    #[test]
    fn test_parse_params_valid_input() {
        let input = json!({"channel": "ch", "payload": 42});
        let result: Result<PublishParams, _> = parse_params(input);

        assert!(result.is_ok());
        let params = result.unwrap();
        assert_eq!(params.channel, "ch");
        assert_eq!(params.payload, json!(42));
    }

    #[test]
    fn test_parse_params_missing_required_field() {
        let input = json!({"channel": "ch"}); // missing "payload"
        let result: Result<PublishParams, _> = parse_params(input);

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::ProtocolError::InvalidParams(msg) => {
                assert!(msg.contains("payload"), "error should mention missing field, got: {msg}");
            }
            other => panic!("expected InvalidParams, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_params_wrong_type() {
        let input = json!("not an object");
        let result: Result<PublishParams, _> = parse_params(input);

        assert!(result.is_err());
    }
}
