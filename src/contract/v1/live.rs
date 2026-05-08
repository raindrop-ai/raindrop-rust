//! `/v1/live` event shape.
//!
//! Mirror of `@raindrop-ai/core/contract/v1/live.ts`. Live events power
//! real-time streaming UI in Workshop while OTLP spans may not have arrived
//! yet. Workshop accepts the canonical types listed in [`LIVE_EVENT_TYPES`]
//! plus arbitrary `String` for forward-compat.

use serde::{Deserialize, Serialize};

/// Canonical live event types accepted by Workshop. Wire string is `snake_case`.
pub const LIVE_EVENT_TYPES: &[&str] = &[
    "text_delta",
    "reasoning_delta",
    "tool_start",
    "tool_result",
    "status",
];

/// Strongly-typed view of [`LIVE_EVENT_TYPES`]. Construct via [`LiveEventType::as_wire_str`]
/// when emitting; consumers that receive arbitrary strings should round-trip them
/// through `LiveEvent.type` directly to preserve forward-compat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveEventType {
    /// Streamed assistant text.
    TextDelta,
    /// Streamed reasoning/thinking text.
    ReasoningDelta,
    /// Tool call started. Workshop REQUIRES `span_id` on this event.
    ToolStart,
    /// Tool call completed. Workshop REQUIRES `span_id` on this event.
    ToolResult,
    /// Generic status text.
    Status,
}

impl LiveEventType {
    /// Wire string emitted as the `type` field.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            LiveEventType::TextDelta => "text_delta",
            LiveEventType::ReasoningDelta => "reasoning_delta",
            LiveEventType::ToolStart => "tool_start",
            LiveEventType::ToolResult => "tool_result",
            LiveEventType::Status => "status",
        }
    }
}

/// Single `/v1/live` event body.
///
/// `traceId` and `spanId` use the JSON camelCase wire form on the live endpoint
/// (the live endpoint historically predates the snake_case track endpoints).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEvent {
    /// 32-char lowercase-hex OTLP trace id.
    #[serde(rename = "traceId")]
    pub trace_id: String,
    /// 16-char lowercase-hex OTLP span id. REQUIRED for `tool_start` and
    /// `tool_result`.
    #[serde(rename = "spanId", skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    /// Event type. Use [`LiveEventType::as_wire_str`] for the canonical set or
    /// pass a custom string for forward-compat.
    pub r#type: String,
    /// Event content (e.g. the streamed text delta or a status message).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Optional Unix-epoch millisecond timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    /// Optional metadata object (`event_id`, `event_name`, `user_id`,
    /// `convo_id`, `workspace`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Validation error returned by [`validate_live_event`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveEventValidationError {
    /// `tool_start` and `tool_result` events MUST carry a `span_id`. Workshop
    /// rejects them otherwise (see `ingestion-contract.md` §4).
    #[error("span_id required on tool_start and tool_result events")]
    MissingSpanIdOnToolEvent,
}

/// Validate a [`LiveEvent`] against the same invariants the TS schema enforces.
///
/// Currently the only invariant is the `tool_start`/`tool_result` span_id
/// requirement; everything else is accepted to preserve forward-compat with
/// future event types.
pub fn validate_live_event(event: &LiveEvent) -> Result<(), LiveEventValidationError> {
    if (event.r#type == "tool_start" || event.r#type == "tool_result") && event.span_id.is_none() {
        return Err(LiveEventValidationError::MissingSpanIdOnToolEvent);
    }
    Ok(())
}
