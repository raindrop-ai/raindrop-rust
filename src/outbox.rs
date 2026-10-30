//! Queue for complete events, signals and identify calls.
//!
//! `track_event`, `track_ai`, `track_signal` and `identify` are hot-path calls
//! (CONTRACT 1.1): they only append to this queue and return. A periodic
//! ticker and explicit `flush()` / `close()` drain it, coalescing each route's
//! items into one JSON array per request (CONTRACT 7.2).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;

use crate::client::ClientInner;

/// Most items sent in one request.
const MAX_BATCH: usize = 500;

/// The routes this queue sends to, in a fixed order.
pub(crate) const ROUTES: [&str; 3] = ["events/track", "signals/track", "users/identify"];

pub(crate) struct Outbox {
    queue: std::sync::Mutex<VecDeque<(&'static str, Value)>>,
    max_queue_size: usize,
    flush_every: Duration,
    /// One drain-and-send cycle at a time: an explicit flush waits for a
    /// background cycle in progress, so the batches it took out of the queue
    /// are on the wire before `flush()` returns (CONTRACT 1.2).
    send_lock: tokio::sync::Mutex<()>,
    stop_notify: Arc<Notify>,
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outbox")
            .field("flush_every", &self.flush_every)
            .field("max_queue_size", &self.max_queue_size)
            .finish()
    }
}

impl Outbox {
    pub(crate) fn new(flush_every: Duration, max_queue_size: usize) -> Self {
        Self {
            queue: std::sync::Mutex::new(VecDeque::new()),
            max_queue_size: max_queue_size.max(1),
            flush_every,
            send_lock: tokio::sync::Mutex::new(()),
            stop_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn flush_every(&self) -> Duration {
        self.flush_every
    }

    pub(crate) fn stop_notify(&self) -> Arc<Notify> {
        self.stop_notify.clone()
    }

    pub(crate) fn stop(&self) {
        self.stop_notify.notify_one();
    }

    /// Append one item. Never touches the network; drops the item with a
    /// rate-limited warning when the queue is full.
    pub(crate) fn push(&self, route: &'static str, item: Value) {
        let mut queue = match self.queue.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if queue.len() >= self.max_queue_size {
            if crate::helpers::should_log_rate_limited("outbox_full") {
                tracing::warn!(
                    route,
                    max = self.max_queue_size,
                    "raindrop: send queue is full; dropping the item (logged at most once per 30s)"
                );
            }
            return;
        }
        queue.push_back((route, item));
    }

    /// Send everything queued, one array per route (in chunks of at most
    /// `MAX_BATCH`). A batch that cannot be delivered is dropped with a
    /// warning; it never fails the caller.
    pub(crate) async fn flush(&self, client: &Arc<ClientInner>) {
        let _cycle = self.send_lock.lock().await;
        let drained: Vec<(&'static str, Value)> = {
            let mut queue = match self.queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            queue.drain(..).collect()
        };
        if drained.is_empty() {
            return;
        }
        for route in ROUTES {
            let items: Vec<Value> = drained
                .iter()
                .filter(|(r, _)| *r == route)
                .map(|(_, item)| item.clone())
                .collect();
            // `users/identify` keeps this SDK's single-object body (one user
            // per request); the other routes take one array per request.
            let batches: Vec<Value> = if route == "users/identify" {
                items
            } else {
                items
                    .chunks(MAX_BATCH)
                    .map(|chunk| Value::Array(chunk.to_vec()))
                    .collect()
            };
            for body in batches {
                let count = body.as_array().map_or(1, Vec::len);
                if let Err(err) = client.transport.post_json(route, &body).await {
                    if crate::helpers::should_log_rate_limited("outbox_send_failed") {
                        tracing::warn!(
                            route,
                            items = count,
                            error = %err,
                            "raindrop: could not deliver a batch; dropping it (logged at most once \
                             per 30s)"
                        );
                    }
                }
            }
        }
    }
}
