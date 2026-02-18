use std::sync::Arc;
use tokio::net::TcpListener;
use quarkmq_broker::Broker;
use crate::connection::handle_connection;
use tracing::{info, error};

pub struct Server {
    listener: TcpListener,
    broker: Arc<Broker>,
}

impl Server {
    pub async fn bind(addr: &str, broker: Arc<Broker>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!("QuarkMQ listening on {}", addr);
        Ok(Self { listener, broker })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let broker = self.broker.clone();
                    tokio::spawn(async move {
                        handle_connection(stream, broker, addr).await;
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}
