mod config;
mod error;
mod server;
mod session;

use clap::Parser;

#[derive(Parser)]
#[command(name = "quarkmq", version, about = "QuarkMQ - Lightweight Message Queue")]
struct Cli {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,

    /// WebSocket bind address (overrides config)
    #[arg(long)]
    bind: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let mut config = config::Config::load(cli.config.as_deref())?;

    if let Some(bind) = cli.bind {
        config.server.ws_bind = bind;
    }

    // Environment variable overrides
    if let Ok(bind) = std::env::var("QUARKMQ_WS_BIND") {
        config.server.ws_bind = bind;
    }
    if let Ok(id) = std::env::var("QUARKMQ_NODE_ID") {
        config.node.id = id;
    }

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("received Ctrl+C, initiating shutdown");
        let _ = shutdown_tx_clone.send(());
    });

    let server = server::Server::new(config);
    server.run(shutdown_tx).await?;

    tracing::info!("QuarkMQ server stopped");
    Ok(())
}
