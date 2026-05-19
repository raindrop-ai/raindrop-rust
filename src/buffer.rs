use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify};

use crate::client::ClientInner;
use crate::error::Result;
use crate::events::Attachment;
use crate::helpers::{clone_map, iso8601_timestamp, merge_attachments};

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
}

/// Sticky context preserved across patches.
#[derive(Debug, Default, Clone)]
pub(crate) struct StickyEventData {
    pub event_name: String,
    pub user_id: String,
    pub convo_id: String,
    pub is_pending: Option<bool>,
}

/// Wire payload for `events/track_partial`.
#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct TrackPartialPayload {
    pub event_id: String,
    pub user_id: String,
    pub event: String,
    pub timestamp: String,
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

/// Per-event-id buffer with sticky state and timer-driven flushing.
pub(crate) struct EventBuffer {
    state: Mutex<EventBufferState>,
    flush_every: Duration,
    /// Notified when the buffer is told to stop (used by the periodic ticker).
    stop_notify: Arc<Notify>,
}

#[derive(Default)]
struct EventBufferState {
    buffers: BTreeMap<String, EventPatch>,
    sticky: BTreeMap<String, StickyEventData>,
}

impl std::fmt::Debug for EventBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBuffer")
            .field("flush_every", &self.flush_every)
            .finish()
    }
}

impl EventBuffer {
    pub(crate) fn new(flush_every: Duration) -> Self {
        Self {
            state: Mutex::new(EventBufferState::default()),
            flush_every,
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
        let flush_now;
        {
            let mut state = self.state.lock().await;
            let existing = state.buffers.remove(event_id).unwrap_or_default();
            let sticky = state.sticky.get(event_id).cloned().unwrap_or_default();

            let mut merged = merge_event_patches(existing, patch);
            if merged.is_pending.is_none() {
                merged.is_pending = sticky.is_pending.or(Some(true));
            }

            let new_sticky = merge_sticky_event_data(&sticky, &merged);
            flush_now = matches!(merged.is_pending, Some(false));
            state.buffers.insert(event_id.to_string(), merged);
            state.sticky.insert(event_id.to_string(), new_sticky);
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
            None => {
                // Cannot ship yet (missing user_id) — restore.
                self.restore(event_id, patch).await;
                return Ok(());
            }
        };

        if should_drop_empty_ai_event(&payload) {
            tracing::warn!(
                event_id,
                event_name = %payload.event,
                has_ai_data = payload.ai_data.is_some(),
                "raindrop: dropping finalized track_partial with empty ai_input and ai_output. \
                 Populate input/output via BeginOptions/FinishOptions/AiEvent, or record errored \
                 generations via `LlmSpan::set_error`."
            );
            let mut state = self.state.lock().await;
            if !state.buffers.contains_key(event_id) {
                state.sticky.remove(event_id);
            }
            return Ok(());
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
                }
                Ok(())
            }
            Err(err) => {
                self.restore(event_id, patch).await;
                Err(err)
            }
        }
    }

    async fn restore(self: &Arc<Self>, event_id: &str, patch: EventPatch) {
        let mut state = self.state.lock().await;
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
    if user_id.is_empty() {
        if client.debug {
            tracing::debug!(event_id, "skipping track_partial: missing user_id");
        }
        return None;
    }

    let event_name = if !patch.event_name.is_empty() {
        patch.event_name.clone()
    } else if !sticky.event_name.is_empty() {
        sticky.event_name.clone()
    } else {
        crate::DEFAULT_EVENT_NAME.to_string()
    };

    let convo_id = if !patch.convo_id.is_empty() {
        patch.convo_id.clone()
    } else {
        sticky.convo_id.clone()
    };

    let mut properties = clone_map(&patch.properties);
    properties.insert("$context".to_string(), client.context_data.clone());

    let attachments = patch.attachments.clone();

    let is_pending = patch.is_pending.or(sticky.is_pending).unwrap_or(true);

    let mut payload = TrackPartialPayload {
        event_id: event_id.to_string(),
        user_id,
        event: event_name,
        timestamp: iso8601_timestamp(patch.timestamp),
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

/// Whether to silently drop a built payload because it would be a phantom
/// finalized AI event with no prompt or response text. These are the events
/// that show up in the dashboard with empty `ai_input` / `ai_output` and
/// confuse users.
///
/// We only drop *finalized* (`is_pending=false`) payloads so that legitimate
/// in-flight interactions (pending patches that will be completed by a later
/// `finish` / `patch` call) still ship. Pending intermediates can have empty
/// text and that is expected.
///
/// Two shapes get dropped:
///   * `ai_data` is attached but both `input` and `output` are empty — the
///     wrapper recorded model / convo_id / token usage but never the prompt
///     or response. To record an errored generation instead, attach an
///     `LlmSpan` and call `set_error(...)` on it; Dawn will associate the
///     error span with this event by `event_id`.
///   * `ai_data` was not attached at all and the event name was never set by
///     the caller (so it defaulted to `ai_generation`). This is the classic
///     "forgot the event name" footgun in `track_event` / `track_ai` /
///     `begin`.
fn should_drop_empty_ai_event(payload: &TrackPartialPayload) -> bool {
    if payload.is_pending {
        return false;
    }
    match &payload.ai_data {
        Some(data) => data.input.is_empty() && data.output.is_empty(),
        None => payload.event == crate::DEFAULT_EVENT_NAME,
    }
}
