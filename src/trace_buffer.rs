use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

use crate::client::ClientInner;
use crate::error::Result;
use crate::http::SendFailure;
use crate::otlp::{build_export_request, OtlpSpan};
use crate::outbox::Undelivered;

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
        let undelivered = self.flush_until(client, None).await;
        if undelivered.is_empty() {
            Ok(())
        } else {
            Err(crate::error::Error::Undelivered {
                count: undelivered.kept + undelivered.dropped,
                dropped: undelivered.dropped,
            })
        }
    }

    /// Send queued spans, bounded by `deadline` (CONTRACT 1.2.1). A batch
    /// never sent stays queued; one cut off mid-request, or permanently
    /// rejected, is dropped.
    pub(crate) async fn flush_until(
        self: &Arc<Self>,
        client: &Arc<ClientInner>,
        deadline: Option<Instant>,
    ) -> Undelivered {
        loop {
            let batch = self.take_batch().await;
            if batch.is_empty() {
                return Undelivered::default();
            }
            let payload =
                build_export_request(batch.clone(), &client.service_name, &client.version);
            match client
                .transport
                .post_json_until("traces", &payload, deadline)
                .await
            {
                Ok(()) => continue,
                Err(failure) if failure.is_permanent_rejection() => {
                    // Resending would fail the same way forever, so drop it
                    // rather than block later spans. Delivered and rejected:
                    // not a flush failure.
                    if crate::helpers::should_log_rate_limited("traces_rejected") {
                        tracing::warn!(
                            spans = batch.len(),
                            error = %failure.into_error(),
                            "raindrop: ingest rejected a trace batch; dropping it (logged at most \
                             once per 30s)"
                        );
                    }
                    continue;
                }
                Err(SendFailure::CutOff) => {
                    if crate::helpers::should_log_rate_limited("traces_cut_off") {
                        tracing::warn!(
                            spans = batch.len(),
                            "raindrop: a trace batch was cut off at the deadline; dropping it \
                             (logged at most once per 30s)"
                        );
                    }
                    let kept = self.state.lock().await.len();
                    return Undelivered {
                        kept,
                        dropped: batch.len(),
                    };
                }
                Err(_) => {
                    let n = batch.len();
                    self.restore_batch(batch).await;
                    let kept = self.state.lock().await.len().max(n);
                    return Undelivered { kept, dropped: 0 };
                }
            }
        }
    }

    /// Drop every queued span (end of `close()`), with a warning.
    pub(crate) async fn drop_all(&self) -> usize {
        let left = {
            let mut queue = self.state.lock().await;
            let n = queue.len();
            queue.clear();
            n
        };
        if left > 0 {
            tracing::warn!(
                spans = left,
                "raindrop: close() deadline passed; dropping spans that were not sent"
            );
        }
        left
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
