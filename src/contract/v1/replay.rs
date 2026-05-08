//! Replay-echo contract.
//!
//! Mirror of `@raindrop-ai/core/contract/v1/replay.ts`. When Workshop replays
//! a trace it generates a `replayRunId` and expects the user's agent to echo
//! it back via one of three paths so Workshop can stitch the new OTLP trace
//! to the replay attempt:
//!
//!   1. canonical attribute  — `raindrop.replay.run_id`
//!   2. AI-SDK metadata      — `ai.telemetry.metadata.raindrop.replayRunId`
//!   3. JSON properties blob — `ai.telemetry.metadata.raindrop.properties`
//!      with `{"replayRunId": "..."}` (works for SDKs that only expose a
//!      free-form properties blob)

use std::collections::HashMap;

use super::attrs::{ai_sdk_metadata, attr_keys, traceloop_props};

/// Read `replayRunId` from a flat OTLP attribute map. Mirrors
/// `readReplayRunIdFromAttrs` in the TS contract: prefers the canonical
/// attribute, falls back to the AI SDK metadata key, then the traceloop
/// association property, then the JSON properties blob.
///
/// Empty-string values are treated as missing at every level so partially
/// populated keys fall through to the next fallback rather than masking it.
pub fn read_replay_run_id_from_attrs(attrs: &HashMap<String, String>) -> Option<String> {
    let get_non_empty = |key: &str| -> Option<String> {
        attrs
            .get(key)
            .and_then(|v| if v.is_empty() { None } else { Some(v.clone()) })
    };

    if let Some(v) = get_non_empty(attr_keys::REPLAY_RUN_ID) {
        return Some(v);
    }
    if let Some(v) = get_non_empty(ai_sdk_metadata::REPLAY_RUN_ID) {
        return Some(v);
    }
    if let Some(v) = get_non_empty(traceloop_props::REPLAY_RUN_ID) {
        return Some(v);
    }

    let props_raw = get_non_empty(ai_sdk_metadata::PROPERTIES)?;
    let parsed: serde_json::Value = serde_json::from_str(&props_raw).ok()?;
    let id = parsed.get("replayRunId").and_then(|v| v.as_str())?;
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}
