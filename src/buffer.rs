use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify};

use crate::app_git::AppGitSnapshot;
use crate::client::ClientInner;
use crate::error::Result;
use crate::events::Attachment;
use crate::helpers::{clone_map, merge_attachments, truncate_text_in_place};

/// In-flight patch state for a single event id.
#[derive(Debug, Default, Clone)]
pub(crate) struct EventPatch {
    pub event_name: String,
    pub user_id: String,
    pub convo_id: String,
    pub input: String,
    pub output: String,
    pub model: String,
    pub properties: BTreeMap<String, Value>,
    pub attachments: Vec<Attachment>,
    pub is_pending: Option<bool>,
    pub timestamp: Option<OffsetDateTime>,
    /// Frozen at operation start. `Some(default())` deliberately records that
    /// metadata was unavailable then, so a later patch cannot adopt it.
    pub app_git_snapshot: Option<AppGitSnapshot>,
    /// Set by an interaction continued by id ([`crate::Client::resume_interaction`]):
    /// the event began elsewhere, so its user, name and start live there.
    pub resumed: bool,
}

/// Sticky context preserved across patches.
#[derive(Debug, Default, Clone)]
pub(crate) struct StickyEventData {
    pub event_name: String,
    pub user_id: String,
    pub convo_id: String,
    pub is_pending: Option<bool>,
    pub app_git_snapshot: Option<AppGitSnapshot>,
    /// The event's start (CONTRACT 9.1): the caller's timestamp, else the
    /// clock when it was begun here. Sent unchanged on every request.
    pub started_at: Option<OffsetDateTime>,
    /// Continued by id; its updates are sent without a user (CONTRACT 9.2).
    pub resumed: bool,
    /// Input, output or attachments were sent for it from this process.
    pub had_content: bool,
}

/// Wire payload for `events/track_partial`.
#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct TrackPartialPayload {
    pub event_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ai_data: Option<AiDataPayload>,
    pub properties: BTreeMap<String, Value>,
    pub attachments: Vec<Attachment>,
    pub is_pending: bool,
}

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct AiDataPayload {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub input: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub output: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub model: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub convo_id: String,
}

/// Per-event-id buffer with sticky state and timer-driven flushing. Both maps
/// are bounded by `max_queue_size`: under sustained send failures (patches
/// are restored for retry) or abandoned interactions, the buffer must apply
/// backpressure — dropping new events with a rate-limited warning — instead
/// of growing host memory without limit.
pub(crate) struct EventBuffer {
    state: Mutex<EventBufferState>,
    flush_every: Duration,
    max_queue_size: usize,
    /// Notified when the buffer is told to stop (used by the periodic ticker).
    stop_notify: Arc<Notify>,
}

#[derive(Default)]
struct EventBufferState {
    buffers: BTreeMap<String, EventPatch>,
    sticky: BTreeMap<String, StickyEventData>,
    /// Recently finished event ids, so a second finish can be warned about
    /// (CONTRACT 10.2). Bounded; oldest first.
    finished: std::collections::VecDeque<String>,
}

/// How many finished event ids are remembered for the repeated-finish warning.
const FINISHED_MEMORY: usize = 1000;

impl std::fmt::Debug for EventBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBuffer")
            .field("flush_every", &self.flush_every)
            .field("max_queue_size", &self.max_queue_size)
            .finish()
    }
}

impl EventBuffer {
    pub(crate) fn new(flush_every: Duration, max_queue_size: usize) -> Self {
        Self {
            state: Mutex::new(EventBufferState::default()),
            flush_every,
            max_queue_size: max_queue_size.max(1),
            stop_notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn flush_every(&self) -> Duration {
        self.flush_every
    }

    pub(crate) fn stop_notify(&self) -> Arc<Notify> {
        self.stop_notify.clone()
    }

    /// Apply a patch and either flush immediately (if no longer pending) or rely on the periodic
    /// ticker.
    pub(crate) async fn patch(
        self: &Arc<Self>,
        client: &Arc<ClientInner>,
        event_id: &str,
        patch: EventPatch,
    ) -> Result<()> {
        let mut patch = patch;
        // Cap AI text fields at the single choke point every track_event /
        // track_ai / begin / patch / finish call funnels through, BEFORE the
        // payload is buffered or serialized: an oversized input/output costs
        // O(cap) here and ships truncated instead of being dropped wholesale
        // at the 1 MiB ingest limit after paying full serialization cost.
        truncate_text_in_place(&mut patch.input, client.max_text_field_chars);
        truncate_text_in_place(&mut patch.output, client.max_text_field_chars);
        let flush_now;
        {
            let mut state = self.state.lock().await;
            // Bounded buffer: NEW event ids are dropped once the cap is hit
            // (patches to already-buffered ids merge in place and don't grow
            // the map). Without this, a network outage — every flush failure
            // restores its patch — would grow these maps without limit.
            if !state.buffers.contains_key(event_id) && state.buffers.len() >= self.max_queue_size {
                if crate::helpers::should_log_rate_limited("event_buffer_full") {
                    tracing::warn!(
                        event_id,
                        max = self.max_queue_size,
                        "raindrop: event buffer is full; discarding event \
                         (logged at most once per 30s)"
                    );
                }
                client.app_git_operations.remove(event_id);
                return Ok(());
            }
            let existing = state.buffers.remove(event_id).unwrap_or_default();
            let known = state.sticky.contains_key(event_id);
            let mut sticky = state.sticky.get(event_id).cloned().unwrap_or_default();
            if !known && patch.resumed {
                sticky.resumed = true;
            }
            record_start(&mut sticky, patch.timestamp);
            if matches!(patch.is_pending, Some(false)) {
                if state.finished.iter().any(|id| id == event_id) {
                    // Still sent: ingest handles repeated finishes (CONTRACT
                    // 10.2), but two finishes usually mean two interactions
                    // share one event id.
                    if crate::helpers::should_log_rate_limited("repeated_finish") {
                        tracing::warn!(
                            event_id,
                            "raindrop: event was already finished in this process; sending the \
                             update anyway. Use a separate event id per interaction, or keep it \
                             pending until its last update (logged at most once per 30s)"
                        );
                    }
                } else {
                    state.finished.push_back(event_id.to_string());
                    while state.finished.len() > FINISHED_MEMORY {
                        state.finished.pop_front();
                    }
                }
            }

            let frozen_app_git = frozen_app_git_snapshot(&existing, &sticky, &patch);
            let mut merged = merge_event_patches(existing, patch);
            merged.app_git_snapshot = frozen_app_git;
            if merged.is_pending.is_none() {
                merged.is_pending = sticky.is_pending.or(Some(true));
            }

            let new_sticky = merge_sticky_event_data(&sticky, &merged);
            flush_now = matches!(merged.is_pending, Some(false));
            state.buffers.insert(event_id.to_string(), merged);
            state.sticky.insert(event_id.to_string(), new_sticky);
            // Sticky context can outlive its buffer entry (pending events
            // flushed but not yet finalized), so it gets its own bound;
            // evicting oldest-key context degrades a later finish to the
            // missing-user_id skip path instead of leaking memory.
            while state.sticky.len() > self.max_queue_size {
                state.sticky.pop_first();
            }
        }

        if flush_now {
            self.flush_one(client, event_id).await?;
        }
        Ok(())
    }

    /// Flush every buffered event (used on close + periodic timer).
    pub(crate) async fn flush(self: &Arc<Self>, client: &Arc<ClientInner>) -> Result<()> {
        let ids: Vec<String> = {
            let state = self.state.lock().await;
            state.buffers.keys().cloned().collect()
        };
        let mut first_err: Option<crate::error::Error> = None;
        for id in ids {
            if let Err(err) = self.flush_one(client, &id).await {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn flush_one(self: &Arc<Self>, client: &Arc<ClientInner>, event_id: &str) -> Result<()> {
        let (patch, sticky) = {
            let mut state = self.state.lock().await;
            match state.buffers.remove(event_id) {
                Some(p) => {
                    let sticky = state.sticky.get(event_id).cloned().unwrap_or_default();
                    (p, sticky)
                }
                None => return Ok(()),
            }
        };

        let payload = match build_track_partial_payload(client, event_id, &patch, &sticky) {
            Some(p) => p,
            None if matches!(patch.is_pending, Some(false)) => {
                // A finished event that never got a user is not sent
                // (CONTRACT 10.1): no user would ever arrive to ship it.
                if crate::helpers::should_log_rate_limited("missing_user_dropped") {
                    tracing::warn!(
                        event_id,
                        "raindrop: dropping an event finished without a user_id; set user_id in \
                         BeginOptions or AiEvent (logged at most once per 30s)"
                    );
                }
                let mut state = self.state.lock().await;
                if !state.buffers.contains_key(event_id) {
                    state.sticky.remove(event_id);
                    client.app_git_operations.remove(event_id);
                }
                return Ok(());
            }
            None => {
                // Cannot ship yet (missing user_id): restore until a user
                // arrives. Not sent, and warned about (CONTRACT 10.1).
                if crate::helpers::should_log_rate_limited("missing_user_held") {
                    tracing::warn!(
                        event_id,
                        "raindrop: holding an event that has no user_id; it is not sent until \
                         one is set (logged at most once per 30s)"
                    );
                }
                self.restore(client, event_id, patch).await;
                return Ok(());
            }
        };

        let has_content = payload_has_content(&payload);
        if has_content {
            let mut state = self.state.lock().await;
            if let Some(entry) = state.sticky.get_mut(event_id) {
                entry.had_content = true;
            }
        }
        if !payload.is_pending && !has_content && !sticky.had_content && !sticky.resumed {
            // Still sent: whether an empty event is kept is ingest's call
            // (CONTRACT 14.2, rule C5). A resumed event is exempt because
            // another process may have sent its content.
            if crate::helpers::should_log_rate_limited("empty_ai_event") {
                tracing::warn!(
                    event_id,
                    "raindrop: event finished with no input, output or attachments; sending it \
                     anyway. Populate input/output via BeginOptions/FinishOptions/AiEvent \
                     (logged at most once per 30s)"
                );
            }
        }
        if payload.user_id.is_none() && crate::helpers::should_log_rate_limited("resumed_no_user") {
            // Still sent: ingest uses the user it stored for the event (CONTRACT 9.2).
            tracing::warn!(
                event_id,
                "raindrop: sending an update for a resumed event without a user_id; ingest uses \
                 the user it already has for this event (logged at most once per 30s)"
            );
        }

        match client
            .transport
            .post_json("events/track_partial", &payload)
            .await
        {
            Ok(_) => {
                if payload.is_pending {
                    return Ok(());
                }
                let mut state = self.state.lock().await;
                if !state.buffers.contains_key(event_id) {
                    state.sticky.remove(event_id);
                    client.app_git_operations.remove(event_id);
                }
                Ok(())
            }
            Err(err) if crate::http::is_permanent_rejection(&err) => {
                // Never retried or restored: ingest refused the payload for
                // good (CONTRACT 3, 15.1).
                if crate::helpers::should_log_rate_limited("partial_rejected") {
                    tracing::warn!(
                        event_id,
                        error = %err,
                        "raindrop: ingest rejected an event update; dropping it (logged at most \
                         once per 30s)"
                    );
                }
                let mut state = self.state.lock().await;
                if !payload.is_pending && !state.buffers.contains_key(event_id) {
                    state.sticky.remove(event_id);
                    client.app_git_operations.remove(event_id);
                }
                Ok(())
            }
            Err(err) => {
                self.restore(client, event_id, patch).await;
                Err(err)
            }
        }
    }

    async fn restore(self: &Arc<Self>, client: &ClientInner, event_id: &str, patch: EventPatch) {
        let mut state = self.state.lock().await;
        // The cap applies to restores too: under a sustained outage every
        // failed send funnels back here, and honest backpressure (drop +
        // rate-limited warn) beats unbounded memory growth.
        if !state.buffers.contains_key(event_id) && state.buffers.len() >= self.max_queue_size {
            if crate::helpers::should_log_rate_limited("event_buffer_full") {
                tracing::warn!(
                    event_id,
                    max = self.max_queue_size,
                    "raindrop: event buffer is full; discarding unsent event \
                     (logged at most once per 30s)"
                );
            }
            client.app_git_operations.remove(event_id);
            return;
        }
        let current = state.buffers.remove(event_id).unwrap_or_default();
        state
            .buffers
            .insert(event_id.to_string(), merge_event_patches(patch, current));
    }

    /// Stop the periodic ticker.
    pub(crate) fn stop(&self) {
        self.stop_notify.notify_one();
    }
}

fn frozen_app_git_snapshot(
    existing: &EventPatch,
    sticky: &StickyEventData,
    patch: &EventPatch,
) -> Option<AppGitSnapshot> {
    patch
        .app_git_snapshot
        .clone()
        .or_else(|| existing.app_git_snapshot.clone())
        .or_else(|| sticky.app_git_snapshot.clone())
}

pub(crate) fn merge_event_patches(target: EventPatch, source: EventPatch) -> EventPatch {
    let mut out = target;
    if !source.event_name.is_empty() {
        out.event_name = source.event_name;
    }
    if !source.user_id.is_empty() {
        out.user_id = source.user_id;
    }
    if !source.convo_id.is_empty() {
        out.convo_id = source.convo_id;
    }
    if !source.input.is_empty() {
        out.input = source.input;
    }
    if !source.output.is_empty() {
        out.output = source.output;
    }
    if !source.model.is_empty() {
        out.model = source.model;
    }
    if source.timestamp.is_some() {
        out.timestamp = source.timestamp;
    }
    if source.is_pending.is_some() {
        out.is_pending = source.is_pending;
    }
    if out.app_git_snapshot.is_none() {
        out.app_git_snapshot = source.app_git_snapshot;
    }
    out.resumed |= source.resumed;
    if !source.properties.is_empty() {
        for (k, v) in source.properties {
            out.properties.insert(k, v);
        }
    }
    if !source.attachments.is_empty() {
        out.attachments = merge_attachments(&out.attachments, &source.attachments);
    }
    out
}

fn merge_sticky_event_data(existing: &StickyEventData, patch: &EventPatch) -> StickyEventData {
    let mut out = existing.clone();
    if !patch.event_name.is_empty() {
        out.event_name = patch.event_name.clone();
    }
    if !patch.user_id.is_empty() {
        out.user_id = patch.user_id.clone();
    }
    if !patch.convo_id.is_empty() {
        out.convo_id = patch.convo_id.clone();
    }
    if patch.is_pending.is_some() {
        out.is_pending = patch.is_pending;
    }
    if patch.app_git_snapshot.is_some() {
        out.app_git_snapshot = patch.app_git_snapshot.clone();
    }
    out
}

fn build_track_partial_payload(
    client: &ClientInner,
    event_id: &str,
    patch: &EventPatch,
    sticky: &StickyEventData,
) -> Option<TrackPartialPayload> {
    let user_id = if !patch.user_id.is_empty() {
        patch.user_id.clone()
    } else {
        sticky.user_id.clone()
    };
    if user_id.is_empty() && !sticky.resumed {
        if client.debug {
            tracing::debug!(event_id, "skipping track_partial: missing user_id");
        }
        return None;
    }

    // A resumed event carries only what this process supplied, so the
    // identity stored where it began is never overwritten (CONTRACT 9.2).
    let event_name = if !patch.event_name.is_empty() {
        Some(patch.event_name.clone())
    } else if !sticky.event_name.is_empty() {
        Some(sticky.event_name.clone())
    } else if sticky.resumed {
        None
    } else {
        Some(crate::DEFAULT_EVENT_NAME.to_string())
    };

    let convo_id = if !patch.convo_id.is_empty() {
        patch.convo_id.clone()
    } else {
        sticky.convo_id.clone()
    };

    let mut properties = clone_map(&patch.properties);
    if let Some(snapshot) = patch
        .app_git_snapshot
        .as_ref()
        .or(sticky.app_git_snapshot.as_ref())
    {
        snapshot.enrich_properties(&mut properties);
    }
    properties.insert("$context".to_string(), client.context_data.clone());

    let attachments = patch.attachments.clone();

    let is_pending = patch.is_pending.or(sticky.is_pending).unwrap_or(true);

    let mut payload = TrackPartialPayload {
        event_id: event_id.to_string(),
        user_id: (!user_id.is_empty()).then_some(user_id),
        event: event_name,
        timestamp: sticky.started_at.map(crate::helpers::iso8601_millis),
        ai_data: None,
        properties,
        attachments,
        is_pending,
    };

    if !patch.input.is_empty()
        || !patch.output.is_empty()
        || !patch.model.is_empty()
        || !convo_id.is_empty()
    {
        payload.ai_data = Some(AiDataPayload {
            input: patch.input.clone(),
            output: patch.output.clone(),
            model: patch.model.clone(),
            convo_id,
        });
    }

    Some(payload)
}

/// Keep the event's start (CONTRACT 9.1). The first value wins: the caller's
/// timestamp, else the clock when the event was begun here. A later explicit
/// timestamp is honoured only when it is earlier. A resumed event gets no
/// clock start: its start was recorded where it began.
fn record_start(sticky: &mut StickyEventData, explicit: Option<OffsetDateTime>) {
    match (sticky.started_at, explicit) {
        (None, Some(at)) => sticky.started_at = Some(at),
        (None, None) if !sticky.resumed => sticky.started_at = Some(OffsetDateTime::now_utc()),
        (Some(current), Some(at)) if at < current => sticky.started_at = Some(at),
        _ => {}
    }
}

/// Whether a payload carries any input, output or attachments.
fn payload_has_content(payload: &TrackPartialPayload) -> bool {
    !payload.attachments.is_empty()
        || payload
            .ai_data
            .as_ref()
            .is_some_and(|d| !d.input.is_empty() || !d.output.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_preserves_an_empty_frozen_git_snapshot() {
        let target = EventPatch {
            app_git_snapshot: Some(AppGitSnapshot::default()),
            ..Default::default()
        };
        let source = EventPatch {
            app_git_snapshot: Some(AppGitSnapshot {
                commit_sha: Some("a".repeat(40)),
                commit_dirty: Some(false),
                branch: Some("main".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            merge_event_patches(target, source).app_git_snapshot,
            Some(AppGitSnapshot::default())
        );
    }

    #[test]
    fn direct_event_id_patch_preserves_sticky_frozen_absence_after_flush() {
        let sticky = StickyEventData {
            app_git_snapshot: Some(AppGitSnapshot::default()),
            ..Default::default()
        };
        let later = EventPatch {
            app_git_snapshot: Some(AppGitSnapshot {
                commit_sha: Some("a".repeat(40)),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            frozen_app_git_snapshot(&EventPatch::default(), &sticky, &later),
            later.app_git_snapshot
        );
    }
}
