use std::sync::Arc;
use clap::Parser;
use tracing::info;

use quarkmq_server::config::{CliArgs, ServerConfig};
use quarkmq_server::server::Server;
use quarkmq_broker::Broker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();
    let config = ServerConfig::load(&args)?;

    info!(
        "Starting QuarkMQ node_id={} data_dir={} bind={}",
        config.broker_config.node_id, config.broker_config.data_dir, config.bind
    );

    let broker = Broker::new(config.broker_config);
    broker.start()?;

    info!("Broker started, recovered state from disk");

    let broker = Arc::new(broker);
    let server = Server::bind(&config.bind, broker).await?;
    server.run().await?;

    Ok(())
}
