//! Queue for complete events, signals and identify calls.
//!
//! `track_event`, `track_ai`, `track_signal` and `identify` are hot-path calls
//! (CONTRACT 1.1): they only append to this queue and return. A periodic
//! ticker and explicit `flush()` / `close()` drain it, coalescing each route's
//! items into one JSON array per request (CONTRACT 7.2).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::Notify;

use crate::client::ClientInner;
use crate::http::SendFailure;

/// What a deadline-bounded flush did not hand to ingest (CONTRACT 1.2.1).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Undelivered {
    /// Never sent; still queued for a later send.
    pub(crate) kept: usize,
    /// Lost: cut off mid-request, retries ran out, or dropped at close.
    pub(crate) dropped: usize,
}

impl Undelivered {
    pub(crate) fn add(&mut self, other: Undelivered) {
        self.kept += other.kept;
        self.dropped += other.dropped;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.kept == 0 && self.dropped == 0
    }
}

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
    /// Items the cycle holding `send_lock` took out of the queue and has not
    /// finished with.
    in_flight: AtomicUsize,
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
            in_flight: AtomicUsize::new(0),
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

    /// Send everything queued (background cycle): no deadline.
    pub(crate) async fn flush(&self, client: &Arc<ClientInner>) {
        let _ = self.flush_until(client, None).await;
    }

    /// Send everything queued, one array per route (in chunks of at most
    /// `MAX_BATCH`), bounded by `deadline` (CONTRACT 1.2.1). Batches never
    /// sent before it stay queued, ahead of anything queued since; a batch
    /// cut off mid-request, or whose retries ran out, is dropped with a
    /// warning. Never fails the caller: it reports what was not delivered.
    pub(crate) async fn flush_until(
        &self,
        client: &Arc<ClientInner>,
        deadline: Option<Instant>,
    ) -> Undelivered {
        let _cycle = match deadline {
            Some(at) => match tokio::time::timeout_at(at.into(), self.send_lock.lock()).await {
                Ok(guard) => guard,
                // A background cycle still holds the batches it took (often
                // waiting out a Retry-After pause); they stay with it.
                Err(_) => {
                    return Undelivered {
                        kept: self.in_flight.load(Ordering::SeqCst).max(1),
                        dropped: 0,
                    }
                }
            },
            None => self.send_lock.lock().await,
        };
        let drained: Vec<(&'static str, Value)> = {
            let mut queue = self.lock_queue();
            queue.drain(..).collect()
        };
        if drained.is_empty() {
            return Undelivered::default();
        }
        self.in_flight.store(drained.len(), Ordering::SeqCst);

        // Batches in send order, each a route and its items.
        let mut batches: Vec<(&'static str, Vec<Value>)> = Vec::new();
        for route in ROUTES {
            let items: Vec<Value> = drained
                .iter()
                .filter(|(r, _)| *r == route)
                .map(|(_, item)| item.clone())
                .collect();
            if route == "users/identify" {
                // One user per request, in this SDK's single-object body.
                batches.extend(items.into_iter().map(|item| (route, vec![item])));
            } else {
                batches.extend(items.chunks(MAX_BATCH).map(|chunk| (route, chunk.to_vec())));
            }
        }

        let mut tally = Undelivered::default();
        let mut next = 0;
        while next < batches.len() {
            let (route, items) = &batches[next];
            let body = if *route == "users/identify" {
                items[0].clone()
            } else {
                Value::Array(items.clone())
            };
            match client
                .transport
                .post_json_until(route, &body, deadline)
                .await
            {
                Ok(()) => {}
                Err(failure) if failure.is_permanent_rejection() => {
                    // Delivered and rejected: dropped with a warning, but
                    // not a flush failure (CONTRACT 1.2.1).
                    warn_dropped(route, items.len(), &failure.into_error().to_string());
                }
                Err(SendFailure::Unsent) => break,
                Err(SendFailure::CutOff) => {
                    tally.dropped += items.len();
                    warn_dropped(route, items.len(), "cut off at the deadline");
                    next += 1;
                    break;
                }
                Err(SendFailure::Failed(err)) => {
                    tally.dropped += items.len();
                    warn_dropped(route, items.len(), &err.to_string());
                }
            }
            next += 1;
        }

        // Whatever was never sent goes back, ahead of newer items.
        let unsent: Vec<(&'static str, Value)> = batches
            .into_iter()
            .skip(next)
            .flat_map(|(route, items)| items.into_iter().map(move |item| (route, item)))
            .collect();
        tally.kept += unsent.len();
        self.restore(unsent);
        self.in_flight.store(0, Ordering::SeqCst);
        tally
    }

    /// Drop everything still queued (end of `close()`), with a warning.
    pub(crate) fn drop_all(&self) -> usize {
        let left = {
            let mut queue = self.lock_queue();
            let n = queue.len();
            queue.clear();
            n
        };
        if left > 0 {
            tracing::warn!(
                items = left,
                "raindrop: close() deadline passed; dropping queued telemetry that was not sent"
            );
        }
        left
    }

    fn lock_queue(&self) -> std::sync::MutexGuard<'_, VecDeque<(&'static str, Value)>> {
        match self.queue.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Put unsent items back at the front, keeping the queue bound: past it,
    /// the newest items are dropped with a warning.
    fn restore(&self, items: Vec<(&'static str, Value)>) {
        if items.is_empty() {
            return;
        }
        let overflow = {
            let mut queue = self.lock_queue();
            for item in items.into_iter().rev() {
                queue.push_front(item);
            }
            let overflow = queue.len().saturating_sub(self.max_queue_size);
            queue.truncate(self.max_queue_size);
            overflow
        };
        if overflow > 0 && crate::helpers::should_log_rate_limited("outbox_full") {
            tracing::warn!(
                items = overflow,
                max = self.max_queue_size,
                "raindrop: send queue is full; dropping the newest items (logged at most once per 30s)"
            );
        }
    }
}

fn warn_dropped(route: &str, items: usize, reason: &str) {
    if crate::helpers::should_log_rate_limited("outbox_send_failed") {
        tracing::warn!(
            route,
            items,
            reason,
            "raindrop: could not deliver a batch; dropping it (logged at most once per 30s)"
        );
    }
}
