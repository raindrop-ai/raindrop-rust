//! `/v1/events/track` and `/v1/events/track_partial` body shape.
//!
//! Mirror of `@raindrop-ai/core/contract/v1/track.ts`. Both endpoints accept
//! the same envelope; `track` defaults `is_pending` to `false`, `track_partial`
//! defaults to `true`.
//!
//! `properties` is intentionally `serde_json::Value` (not a strict struct) so
//! callers can pass arbitrary user properties through; only the fields
//! Workshop interprets are documented here.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::workspace::LocalWorkspaceMetadata;

/// Single attachment shipped on a track event. Mirror of TS `AttachmentSchema`.
///
/// Note: `value` is required; everything else is optional. The Rust SDK uses
/// `crate::Attachment` as the public-facing struct; this struct exists so the
/// contract module is a self-contained mirror of the TS schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TrackAttachment {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    pub value: String,
    /// `"input"` or `"output"`. Empty defaults to `"output"` server-side.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub role: String,
    /// `"text"` | `"code"` | `"image"` | `"iframe"`.
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub language: String,
}

/// Shape of the `properties` block on track / track_partial events.
///
/// Workshop only interprets the listed fields; everything else (user-supplied
/// keys) round-trips through `extra` so passthrough is preserved.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrackProperties {
    /// Resolved trace id (preferred over `$trace_id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    /// Posthog-compatible `$trace_id` alias.
    #[serde(rename = "$trace_id", default, skip_serializing_if = "Option::is_none")]
    pub dollar_trace_id: Option<String>,
    /// Workspace identity, auto-stamped from env when not explicitly set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<LocalWorkspaceMetadata>,
    /// Replay echo id (matches `metadata.replayRunId` on the live endpoint).
    #[serde(
        rename = "replayRunId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub replay_run_id: Option<String>,
    /// Free-form user properties (passthrough).
    #[serde(flatten)]
    pub extra: std::collections::BTreeMap<String, Value>,
}

/// `ai_data` sub-object on track events. All fields optional.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrackAiData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convo_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

/// Common shape for `/v1/events/track_partial` and `/v1/events/track`. Both
/// accept the same envelope; the partial vs final distinction is the
/// `is_pending` flag (and the partial endpoint defaults it to `true`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackEvent {
    pub event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// Either an ISO-8601 string or a Unix-millisecond number; the canonical TS
    /// schema accepts both, so we use `serde_json::Value` to preserve either.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ai_data: Option<TrackAiData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<TrackProperties>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<TrackAttachment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_pending: Option<bool>,
}

/// `/v1/events/track` body shape: a single event or an array of events.
/// Workshop's `persistTrack` helper accepts either.
///
/// `Single` is boxed to keep the enum compact (a `TrackEvent` is ~400 bytes
/// because of the optional sub-objects; without the box, every `Vec<TrackBody>`
/// allocation would carry that overhead per slot).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrackBody {
    Single(Box<TrackEvent>),
    Batch(Vec<TrackEvent>),
}
