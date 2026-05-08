use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

use crate::client::ClientInner;
use crate::error::Result;
use crate::otlp::{build_export_request, OtlpSpan};

/// Bounded, batched OTLP/JSON span buffer.
pub(crate) struct TraceBuffer {
    state: Mutex<VecDeque<OtlpSpan>>,
    flush_every: Duration,
    max_batch_size: usize,
    max_queue_size: usize,
    stop_notify: Arc<Notify>,
}

impl std::fmt::Debug for TraceBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceBuffer")
            .field("flush_every", &self.flush_every)
            .field("max_batch_size", &self.max_batch_size)
            .field("max_queue_size", &self.max_queue_size)
            .finish()
    }
}

impl TraceBuffer {
    pub(crate) fn new(flush_every: Duration, max_batch_size: usize, max_queue_size: usize) -> Self {
        Self {
            state: Mutex::new(VecDeque::new()),
            flush_every,
            max_batch_size,
            max_queue_size,
            stop_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn flush_every(&self) -> Duration {
        self.flush_every
    }

    pub(crate) fn stop_notify(&self) -> Arc<Notify> {
        self.stop_notify.clone()
    }

    /// Enqueue a span and trigger an immediate flush if the batch threshold is hit.
    pub(crate) async fn enqueue(self: &Arc<Self>, client: Arc<ClientInner>, span: OtlpSpan) {
        let flush_now;
        {
            let mut queue = self.state.lock().await;
            if queue.len() >= self.max_queue_size {
                queue.pop_front();
            }
            queue.push_back(span);
            flush_now = queue.len() >= self.max_batch_size;
        }

        if flush_now {
            // Best-effort; drop errors so a single failed flush doesn't propagate to the caller.
            let _ = self.flush(&client).await;
        }
    }

    pub(crate) async fn flush(self: &Arc<Self>, client: &Arc<ClientInner>) -> Result<()> {
        loop {
            let batch = self.take_batch().await;
            if batch.is_empty() {
                return Ok(());
            }
            let payload =
                build_export_request(batch.clone(), &client.service_name, &client.version);

            // Mirror OTLP exports to the local Workshop daemon (fire-and-forget).
            // Same payload, same path; Workshop accepts the OTLP/JSON envelope on
            // `/v1/traces`. We mirror BEFORE the cloud send so the local UI sees
            // streaming spans without waiting on the cloud round-trip.
            client.mirror_to_workshop("traces", &payload);

            match client.transport.post_json("traces", &payload).await {
                Ok(_) => continue,
                Err(err) => {
                    self.restore_batch(batch).await;
                    return Err(err);
                }
            }
        }
    }

    async fn take_batch(&self) -> Vec<OtlpSpan> {
        let mut queue = self.state.lock().await;
        if queue.is_empty() {
            return Vec::new();
        }
        let n = self.max_batch_size.min(queue.len()).max(1);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(s) = queue.pop_front() {
                out.push(s);
            } else {
                break;
            }
        }
        out
    }

    async fn restore_batch(&self, batch: Vec<OtlpSpan>) {
        if batch.is_empty() {
            return;
        }
        let mut queue = self.state.lock().await;
        // Restore at the front and trim if we're over the limit.
        for span in batch.into_iter().rev() {
            queue.push_front(span);
        }
        while queue.len() > self.max_queue_size {
            queue.pop_back();
        }
    }

    pub(crate) fn stop(&self) {
        self.stop_notify.notify_one();
    }
}
