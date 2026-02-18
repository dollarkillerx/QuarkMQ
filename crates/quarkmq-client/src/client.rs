use quarkmq_protocol::rpc::{
    AckParams, CreateChannelParams, ListChannelsResult, MessagePush, NackParams,
    PublishParams, PublishResult, SubscribeParams,
};
use quarkmq_protocol::MessageId;

use crate::connection::Connection;
use crate::error::ClientError;

pub struct QuarkMQClient {
    connection: Connection,
}

impl QuarkMQClient {
    pub async fn connect(url: &str) -> Result<Self, ClientError> {
        let connection = Connection::connect(url).await?;
        Ok(Self { connection })
    }

    pub async fn create_channel(
        &self,
        name: &str,
        ack_timeout_secs: Option<u64>,
        max_delivery_attempts: Option<u32>,
    ) -> Result<String, ClientError> {
        let params = CreateChannelParams {
            name: name.to_string(),
            ack_timeout_secs: ack_timeout_secs.unwrap_or(30),
            max_delivery_attempts: max_delivery_attempts.unwrap_or(5),
        };
        let resp = self
            .connection
            .call("create_channel", serde_json::to_value(params).unwrap())
            .await?;
        Ok(resp
            .result
            .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(String::from))
            .unwrap_or_default())
    }

    pub async fn publish(
        &self,
        channel: &str,
        payload: serde_json::Value,
    ) -> Result<MessageId, ClientError> {
        let params = PublishParams {
            channel: channel.to_string(),
            payload,
        };
        let resp = self
            .connection
            .call("publish", serde_json::to_value(params).unwrap())
            .await?;
        let result: PublishResult =
            serde_json::from_value(resp.result.unwrap_or_default())?;
        Ok(result.message_id)
    }

    pub async fn subscribe(&self, channel: &str, topic: &str) -> Result<(), ClientError> {
        let params = SubscribeParams {
            channel: channel.to_string(),
            topic: topic.to_string(),
        };
        self.connection
            .call("subscribe", serde_json::to_value(params).unwrap())
            .await?;
        Ok(())
    }

    pub async fn ack(&self, message_id: MessageId) -> Result<(), ClientError> {
        let params = AckParams { message_id };
        self.connection
            .call("ack", serde_json::to_value(params).unwrap())
            .await?;
        Ok(())
    }

    pub async fn nack(&self, message_id: MessageId) -> Result<(), ClientError> {
        let params = NackParams { message_id };
        self.connection
            .call("nack", serde_json::to_value(params).unwrap())
            .await?;
        Ok(())
    }

    pub async fn list_channels(&self) -> Result<ListChannelsResult, ClientError> {
        let resp = self
            .connection
            .call("list_channels", serde_json::json!({}))
            .await?;
        let result: ListChannelsResult =
            serde_json::from_value(resp.result.unwrap_or_default())?;
        Ok(result)
    }

    /// Receive the next server-push message notification.
    pub async fn recv_message(&mut self) -> Option<MessagePush> {
        loop {
            let notif = self.connection.notification_rx.recv().await?;
            if notif.method == "message" {
                if let Ok(push) = serde_json::from_value::<MessagePush>(notif.params) {
                    return Some(push);
                }
            }
        }
    }
}
