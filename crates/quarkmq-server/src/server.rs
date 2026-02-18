use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;

use quarkmq_broker::channel::Dispatch;
use quarkmq_broker::Dispatcher;
use quarkmq_protocol::rpc::ConsumerId;

use crate::config::Config;
use crate::session;

pub struct Server {
    config: Arc<Config>,
    dispatcher: Arc<Dispatcher>,
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::UnboundedSender<String>>>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(config),
            dispatcher: Arc::new(Dispatcher::new()),
            sessions: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub async fn run(self, shutdown: tokio::sync::broadcast::Sender<()>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.ws_bind).await?;
        tracing::info!(bind = %self.config.server.ws_bind, "QuarkMQ server listening");

        let mut shutdown_rx = shutdown.subscribe();

        // Dispatch loop: periodically dispatch pending messages to consumers
        let dispatcher = self.dispatcher.clone();
        let sessions = self.sessions.clone();
        let dispatch_shutdown = shutdown.subscribe();
        tokio::spawn(dispatch_loop(dispatcher, sessions, dispatch_shutdown));

        // Timeout checker loop
        let dispatcher = self.dispatcher.clone();
        let sessions = self.sessions.clone();
        let timeout_shutdown = shutdown.subscribe();
        tokio::spawn(timeout_loop(dispatcher, sessions, timeout_shutdown));

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, addr)) => {
                            tracing::debug!(addr = %addr, "new TCP connection");
                            let dispatcher = self.dispatcher.clone();
                            let config = self.config.clone();
                            let sessions = self.sessions.clone();
                            tokio::spawn(async move {
                                match tokio_tungstenite::accept_async(stream).await {
                                    Ok(ws_stream) => {
                                        session::run_session(ws_stream, dispatcher, config, sessions).await;
                                    }
                                    Err(e) => {
                                        tracing::warn!(addr = %addr, error = %e, "WebSocket handshake failed");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "TCP accept error");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("shutting down server");
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn dispatcher(&self) -> Arc<Dispatcher> {
        self.dispatcher.clone()
    }
}

/// Periodically dispatch pending messages to subscribed consumers.
async fn dispatch_loop(
    dispatcher: Arc<Dispatcher>,
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::UnboundedSender<String>>>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(10));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                send_dispatches(&dispatcher, &sessions);
            }
            _ = shutdown.recv() => break,
        }
    }
}

/// Periodically check for ACK timeouts and re-dispatch.
async fn timeout_loop(
    dispatcher: Arc<Dispatcher>,
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::UnboundedSender<String>>>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let dispatches = dispatcher.check_timeouts();
                deliver_dispatches(&dispatches, &sessions);
            }
            _ = shutdown.recv() => break,
        }
    }
}

fn send_dispatches(
    dispatcher: &Dispatcher,
    sessions: &dashmap::DashMap<ConsumerId, mpsc::UnboundedSender<String>>,
) {
    let dispatches = dispatcher.dispatch_all();
    deliver_dispatches(&dispatches, sessions);
}

fn deliver_dispatches(
    dispatches: &[Dispatch],
    sessions: &dashmap::DashMap<ConsumerId, mpsc::UnboundedSender<String>>,
) {
    for dispatch in dispatches {
        if let Some(tx) = sessions.get(&dispatch.consumer_id) {
            let push =
                quarkmq_protocol::rpc::MessagePush::from_message(&dispatch.message);
            let notification = push.into_notification();
            if let Ok(json) = serde_json::to_string(&notification) {
                let _ = tx.send(json);
            }
        }
    }
}
