use std::collections::BTreeMap;

use base64::Engine;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::events::Attachment;

/// Format an `OffsetDateTime` as RFC3339 (with sub-second precision when present).
/// Returns the current UTC time in ISO 8601 if the input is `None`.
pub(crate) fn iso8601_timestamp(at: Option<OffsetDateTime>) -> String {
    let at = at.unwrap_or_else(OffsetDateTime::now_utc);
    at.format(&Rfc3339).unwrap_or_default()
}

/// Format an `OffsetDateTime` as RFC3339, returning an empty string if `None`.
pub(crate) fn optional_timestamp(at: Option<OffsetDateTime>) -> String {
    match at {
        Some(at) => at.format(&Rfc3339).unwrap_or_default(),
        None => String::new(),
    }
}

/// Convert any JSON value to a string the way the other SDKs do for tool inputs/outputs.
pub(crate) fn stringify_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Generate a UUID v4 string for event ids.
pub(crate) fn new_event_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Generate `length` random bytes encoded as standard base64 (not URL-safe).
pub(crate) fn random_id_b64(length: usize) -> String {
    let mut buf = vec![0u8; length];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut buf);
    base64::engine::general_purpose::STANDARD.encode(buf)
}

/// Format a `OffsetDateTime` as a unix-nanos string (matches OTLP/JSON wire format).
pub(crate) fn unix_nanos_string(at: Option<OffsetDateTime>) -> String {
    let at = at.unwrap_or_else(OffsetDateTime::now_utc);
    let nanos = at.unix_timestamp_nanos();
    nanos.to_string()
}

/// Clone a JSON map by reference (shallow).
pub(crate) fn clone_map(src: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    src.clone()
}

/// Append-merge attachment vectors.
pub(crate) fn merge_attachments(target: &[Attachment], source: &[Attachment]) -> Vec<Attachment> {
    let mut out = Vec::with_capacity(target.len() + source.len());
    out.extend_from_slice(target);
    out.extend_from_slice(source);
    out
}

/// Merge two property maps by overlaying `overlay` onto `base`.
pub(crate) fn merge_maps(
    base: &BTreeMap<String, Value>,
    overlay: &BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let mut merged = base.clone();
    for (k, v) in overlay {
        merged.insert(k.clone(), v.clone());
    }
    merged
}
