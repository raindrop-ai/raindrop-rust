use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;

use crate::client::ClientInner;
use crate::error::Result;
use crate::helpers::optional_timestamp;

/// Signal payload (thumbs up/down, edit, feedback, agent…).
#[derive(Debug, Default, Clone)]
pub struct Signal {
    /// Event id this signal belongs to.
    pub event_id: String,
    /// Signal name (e.g. `thumbs_up`).
    pub name: String,
    /// Signal type. Defaults to `default`.
    pub kind: String,
    /// Optional sentiment.
    pub sentiment: String,
    /// Optional timestamp.
    pub timestamp: Option<OffsetDateTime>,
    /// Free-form properties.
    pub properties: BTreeMap<String, Value>,
    /// Attachment id if the signal references one.
    pub attachment_id: String,
    /// Optional comment (feedback).
    pub comment: String,
    /// Optional `after` text (edit).
    pub after: String,
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
