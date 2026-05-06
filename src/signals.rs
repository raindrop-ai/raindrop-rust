use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;

use crate::client::ClientInner;
use crate::error::Result;
use crate::helpers::optional_timestamp;

/// Signal payload (user feedback like thumbs up/down, edits, agent self-diagnostics, etc.).
///
/// Mirrors the canonical `SignalEventSchema` from `@raindrop-ai/schemas/ingest`. The accepted
/// `signal_type` values on the wire are:
///
/// - [`SignalKind::DEFAULT`] (default if `kind` is empty)
/// - [`SignalKind::STANDARD`] — synonym for `default` accepted by the backend
/// - [`SignalKind::FEEDBACK`] — usually paired with a non-empty `comment`
/// - [`SignalKind::EDIT`] — usually paired with `after` (the corrected text)
/// - [`SignalKind::AGENT`] — emitted by an agent itself (e.g. self-diagnostics)
/// - [`SignalKind::AGENT_INTERNAL`] — internal diagnostic signals not surfaced to end users
#[derive(Debug, Default, Clone)]
pub struct Signal {
    /// Event id this signal belongs to.
    pub event_id: String,
    /// Signal name (e.g. `thumbs_up`).
    pub name: String,
    /// Signal type — see [`Signal`] docs and [`SignalKind`] for accepted values. Empty defaults
    /// to `"default"`.
    pub kind: String,
    /// Optional sentiment. Accepted: `"POSITIVE"` or `"NEGATIVE"`. Empty omits the field.
    pub sentiment: String,
    /// Optional timestamp.
    pub timestamp: Option<OffsetDateTime>,
    /// Free-form properties merged with `comment` / `after` if those fields are set.
    pub properties: BTreeMap<String, Value>,
    /// Attachment id if the signal references one.
    pub attachment_id: String,
    /// Optional comment (typically used with `kind = "feedback"`).
    pub comment: String,
    /// Optional `after` text (typically used with `kind = "edit"`).
    pub after: String,
}

/// Accepted values for [`Signal::kind`]. The wire format accepts any string (forward
/// compatibility), but these constants document the canonical values from
/// `@raindrop-ai/schemas/ingest::SignalEventSchema`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SignalKind;

impl SignalKind {
    /// Default signal type (`"default"`). Used for plain feedback signals like `thumbs_up`.
    pub const DEFAULT: &'static str = "default";
    /// Synonym for `default` accepted by the backend (`"standard"`).
    pub const STANDARD: &'static str = "standard";
    /// Feedback signal — typically paired with a non-empty `comment` (`"feedback"`).
    pub const FEEDBACK: &'static str = "feedback";
    /// Edit signal — typically paired with `after` describing the corrected output (`"edit"`).
    pub const EDIT: &'static str = "edit";
    /// Signal emitted by an agent itself, e.g. for self-diagnostics (`"agent"`).
    pub const AGENT: &'static str = "agent";
    /// Internal agent signal not intended to surface to end users (`"agent_internal"`).
    pub const AGENT_INTERNAL: &'static str = "agent_internal";
}

#[derive(Debug, Default, Clone, Serialize)]
struct SignalPayload {
    event_id: String,
    signal_name: String,
    signal_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    timestamp: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    sentiment: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    attachment_id: String,
    properties: BTreeMap<String, Value>,
}

pub(crate) async fn track_signal(client: &ClientInner, signal: Signal) -> Result<()> {
    if !client.enabled {
        return Ok(());
    }

    let mut properties = signal.properties;
    if !signal.comment.is_empty() {
        properties.insert("comment".into(), Value::String(signal.comment.clone()));
    }
    if !signal.after.is_empty() {
        properties.insert("after".into(), Value::String(signal.after.clone()));
    }

    let kind = if signal.kind.is_empty() {
        "default".to_string()
    } else {
        signal.kind
    };

    let payload = vec![SignalPayload {
        event_id: signal.event_id,
        signal_name: signal.name,
        signal_type: kind,
        timestamp: optional_timestamp(signal.timestamp),
        sentiment: signal.sentiment,
        attachment_id: signal.attachment_id,
        properties,
    }];

    client.transport.post_json("signals/track", &payload).await
}
