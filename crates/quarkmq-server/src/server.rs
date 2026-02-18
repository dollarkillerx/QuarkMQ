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
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::Sender<String>>>,
}

impl Server {
    pub fn new(config: Config) -> Self {
        let data_dir = config.node.data_dir.clone();
        let dispatcher = Arc::new(Dispatcher::with_storage(data_dir));

        Self {
            config: Arc::new(config),
            dispatcher,
            sessions: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub async fn run(self, shutdown: tokio::sync::broadcast::Sender<()>) -> anyhow::Result<()> {
        // Recover persisted channels from storage
        match self.dispatcher.recover() {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(count, "recovered channels from persistent storage");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to recover channels from storage");
            }
        }

        let listener = TcpListener::bind(&self.config.server.ws_bind).await?;
        tracing::info!(bind = %self.config.server.ws_bind, "QuarkMQ server listening");

        let mut shutdown_rx = shutdown.subscribe();

        // Dispatch loop: event-driven dispatch of pending messages to consumers
        let dispatcher = self.dispatcher.clone();
        let sessions = self.sessions.clone();
        let dispatch_shutdown = shutdown.subscribe();
        let dispatch_notify = self.dispatcher.dispatch_notify();
        tokio::spawn(dispatch_loop(dispatcher, sessions, dispatch_shutdown, dispatch_notify));

        // Timeout checker loop
        let dispatcher = self.dispatcher.clone();
        let sessions = self.sessions.clone();
        let timeout_shutdown = shutdown.subscribe();
        tokio::spawn(timeout_loop(dispatcher, sessions, timeout_shutdown));

        // WAL sync loop (500ms interval)
        let dispatcher = self.dispatcher.clone();
        let sync_shutdown = shutdown.subscribe();
        tokio::spawn(sync_loop(dispatcher, sync_shutdown));

        // WAL compaction / GC loop (5 min interval)
        let dispatcher = self.dispatcher.clone();
        let gc_shutdown = shutdown.subscribe();
        tokio::spawn(gc_loop(dispatcher, gc_shutdown));

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
                    // Final sync before shutdown
                    if let Err(e) = self.dispatcher.sync_all() {
                        tracing::error!(error = %e, "failed to sync WAL on shutdown");
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn dispatcher(&self) -> Arc<Dispatcher> {
        self.dispatcher.clone()
    }
}

/// Event-driven dispatch loop: wakes on publish/nack notifications or a fallback interval.
async fn dispatch_loop(
    dispatcher: Arc<Dispatcher>,
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::Sender<String>>>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
    notify: Arc<tokio::sync::Notify>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = notify.notified() => {
                send_dispatches(&dispatcher, &sessions);
            }
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
    sessions: Arc<dashmap::DashMap<ConsumerId, mpsc::Sender<String>>>,
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

/// Periodically flush WAL buffers to disk.
async fn sync_loop(
    dispatcher: Arc<Dispatcher>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = dispatcher.sync_all() {
                    tracing::error!(error = %e, "WAL sync failed");
                }
            }
            _ = shutdown.recv() => break,
        }
    }
}

/// Periodically compact WAL files to reclaim space.
async fn gc_loop(
    dispatcher: Arc<Dispatcher>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = dispatcher.compact_all() {
                    tracing::error!(error = %e, "WAL compaction failed");
                }
            }
            _ = shutdown.recv() => break,
        }
    }
}

fn send_dispatches(
    dispatcher: &Dispatcher,
    sessions: &dashmap::DashMap<ConsumerId, mpsc::Sender<String>>,
) {
    let dispatches = dispatcher.dispatch_all();
    deliver_dispatches(&dispatches, sessions);
}

fn deliver_dispatches(
    dispatches: &[Dispatch],
    sessions: &dashmap::DashMap<ConsumerId, mpsc::Sender<String>>,
) {
    for dispatch in dispatches {
        if let Some(tx) = sessions.get(&dispatch.consumer_id) {
            let push =
                quarkmq_protocol::rpc::MessagePush::from_message(&dispatch.message);
            let notification = push.into_notification();
            if let Ok(json) = serde_json::to_string(&notification) {
                if let Err(mpsc::error::TrySendError::Full(_)) = tx.try_send(json) {
                    tracing::warn!(
                        consumer_id = %dispatch.consumer_id,
                        "outbound channel full, dropping message"
                    );
                }
            }
        }
    }
}
