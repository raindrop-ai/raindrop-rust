use std::collections::BTreeMap;

use base64::Engine;
use serde::Serialize;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::events::Attachment;

/// Marker appended to truncated text fields. Matches the Python SDK's
/// `_TRUNCATION_MARKER` so downstream consumers detect truncation uniformly.
pub(crate) const TRUNCATION_MARKER: &str = "...[truncated by raindrop]";

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

// --- Bounded text / serialization ------------------------------------------
//
// Telemetry must never burn caller CPU proportional to the payload: text
// fields are capped BEFORE buffering/serialization and structured payloads
// are serialized through an output-budgeted writer, so the cost of an
// oversized payload is proportional to the configured cap — not the payload.
// The truncated result, marker included, never exceeds the limit (the limit
// may come from `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT`, which downstream
// consumers treat as a hard cap).

/// Byte offset of the `n`-th character (0-based), or `None` when the string
/// has `n` or fewer characters. Cost is O(n), never O(`s.len()`).
fn byte_index_of_char(s: &str, n: usize) -> Option<usize> {
    s.char_indices().nth(n).map(|(i, _)| i)
}

/// Hard-truncate `s` so the result — marker included — never exceeds `limit`
/// characters, and append the marker. When `limit` is too small to fit the
/// marker, hard-slice without it (python-sdk `_truncate_to_limit` parity).
fn enforce_limit_with_marker(s: &mut String, limit: usize) {
    if limit > TRUNCATION_MARKER.len() {
        let keep = limit - TRUNCATION_MARKER.len();
        if let Some(cut) = byte_index_of_char(s, keep) {
            s.truncate(cut);
        }
        s.push_str(TRUNCATION_MARKER);
    } else if let Some(cut) = byte_index_of_char(s, limit) {
        s.truncate(cut);
    }
}

/// Cap a text field in place BEFORE it is buffered or serialized.
///
/// The fast path is an O(1) byte-length check; the truncating path costs
/// O(`limit`) — never O(payload) — so multi-MB inputs/outputs cost the cap on
/// the calling task. The result, truncation marker included, never exceeds
/// `limit` characters, and cuts always land on `char` boundaries.
pub(crate) fn truncate_text_in_place(s: &mut String, limit: usize) {
    if s.len() <= limit {
        // bytes <= limit implies chars <= limit
        return;
    }
    if byte_index_of_char(s, limit).is_none() {
        return; // <= limit chars (multi-byte payload), nothing to do
    }
    enforce_limit_with_marker(s, limit);
}

/// Capped copy of `s`: like [`truncate_text_in_place`] but for borrowed
/// strings. Copies at most O(`limit`) when truncating — never clones the full
/// payload first.
pub(crate) fn capped_string(s: &str, limit: usize) -> String {
    if s.len() <= limit {
        return s.to_string();
    }
    match byte_index_of_char(s, limit) {
        None => s.to_string(),
        Some(cut) => {
            let mut out = s[..cut].to_string();
            enforce_limit_with_marker(&mut out, limit);
            out
        }
    }
}

/// `io::Write` sink that accepts at most `cap` bytes and then errors, making
/// `serde_json::to_writer` abort early. serde_json hands a string leaf to the
/// writer as one chunk, but only `cap - len` bytes of it are ever copied — so
/// a multi-MB leaf still costs O(cap), unlike serialize-then-truncate.
struct BoundedWriter {
    buf: Vec<u8>,
    cap: usize,
}

impl std::io::Write for BoundedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let remaining = self.cap.saturating_sub(self.buf.len());
        if remaining == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "raindrop payload budget exhausted",
            ));
        }
        let take = remaining.min(data.len());
        self.buf.extend_from_slice(&data[..take]);
        Ok(take)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Decode the longest valid UTF-8 prefix (the budget cut can split a
/// multi-byte character).
fn string_from_utf8_prefix(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(err) => {
            let valid = err.utf8_error().valid_up_to();
            let mut bytes = err.into_bytes();
            bytes.truncate(valid);
            String::from_utf8(bytes).unwrap_or_default()
        }
    }
}

/// JSON-serialize `value` with a hard output budget.
///
/// Unlike serialize-then-truncate, the cost here is proportional to the
/// budget, not the payload: serialization streams into a [`BoundedWriter`]
/// and aborts as soon as the budget is exhausted. The result — truncation
/// marker included — never exceeds `limit` characters. A truncated result may
/// not be valid JSON; like the python-sdk's `_dumps_bounded`, that is
/// expected for display purposes. Returns `None` when `value` genuinely fails
/// to serialize (budget exhaustion is NOT a failure).
pub(crate) fn to_string_bounded<T: ?Sized + Serialize>(value: &T, limit: usize) -> Option<String> {
    // Slack covers JSON syntax overhead so payloads near the limit aren't
    // truncated twice; the final enforcement below restores the hard limit.
    let cap_bytes = limit
        .saturating_add(TRUNCATION_MARKER.len())
        .saturating_add(64);
    let mut writer = BoundedWriter {
        buf: Vec::new(),
        cap: cap_bytes,
    };
    let result = serde_json::to_writer(&mut writer, value);
    let budget_hit = writer.buf.len() >= cap_bytes;
    let mut text = string_from_utf8_prefix(writer.buf);
    match result {
        Ok(()) => {
            truncate_text_in_place(&mut text, limit);
            Some(text)
        }
        Err(_) if budget_hit => {
            // Aborted by the budget: the marker is mandatory even when the
            // kept prefix is shorter than `limit` characters.
            enforce_limit_with_marker(&mut text, limit);
            Some(text)
        }
        Err(_) => None,
    }
}

/// Bounded equivalent of [`stringify_value`]: raw strings are capped without
/// JSON quoting, `null` stays empty, everything else serializes under the
/// budget.
pub(crate) fn stringify_value_bounded(value: &Value, limit: usize) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => capped_string(s, limit),
        other => to_string_bounded(other, limit).unwrap_or_default(),
    }
}

/// [`stringify_value`] semantics for any `Serialize` payload WITHOUT first
/// materializing a full `serde_json::Value` tree (which would copy the whole
/// payload before any bound applies): strings come out raw (unquoted), `null`
/// becomes empty, other JSON is budget-bounded. Returns `None` when the
/// payload fails to serialize.
pub(crate) fn stringify_serialize_bounded<T: ?Sized + Serialize>(
    value: &T,
    limit: usize,
) -> Option<String> {
    let text = to_string_bounded(value, limit)?;
    if text == "null" {
        return Some(String::new());
    }
    if text.starts_with('"') {
        // Whole string literals fit the budget; unquote for stringify_value
        // parity. Budget-truncated literals fail to parse and ship as-is
        // (display-only).
        if let Ok(Value::String(raw)) = serde_json::from_str::<Value>(&text) {
            return Some(raw);
        }
    }
    Some(text)
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

// --- Log rate limiting -------------------------------------------------------
//
// Failure-path warnings (oversized drops, empty-event drops, queue overflow)
// fire per event; under sustained backpressure that floods the host's log
// output. Cap each distinct failure family to one line per interval
// (python-sdk `_rate_limited_log` parity).

const RATE_LIMITED_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Returns true when the failure family identified by `key` is allowed to log
/// now, recording the emission; callers skip the log line on `false`.
pub(crate) fn should_log_rate_limited(key: &'static str) -> bool {
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;

    static LAST_EMITTED: OnceLock<Mutex<BTreeMap<&'static str, Instant>>> = OnceLock::new();
    let map = LAST_EMITTED.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = match map.lock() {
        Ok(g) => g,
        // A panic while holding this lock can't corrupt anything beyond the
        // timestamps; logging once more is the safe failure mode.
        Err(poisoned) => poisoned.into_inner(),
    };
    let now = Instant::now();
    match guard.get(key) {
        Some(last) if now.duration_since(*last) < RATE_LIMITED_LOG_INTERVAL => false,
        _ => {
            guard.insert(key, now);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chars(s: &str) -> usize {
        s.chars().count()
    }

    /// A payload whose `Serialize` impl genuinely fails (not budget-related).
    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("deliberate failure"))
        }
    }

    #[test]
    fn truncate_noop_under_limit() {
        let mut s = "hello".to_string();
        truncate_text_in_place(&mut s, 100);
        assert_eq!(s, "hello");
    }

    #[test]
    fn truncate_caps_with_marker_within_limit() {
        let mut s = "x".repeat(10_000);
        truncate_text_in_place(&mut s, 100);
        assert_eq!(chars(&s), 100, "marker must fit WITHIN the limit");
        assert!(s.ends_with(TRUNCATION_MARKER));
        assert!(s.starts_with("xxxx"));
    }

    #[test]
    fn truncate_limit_smaller_than_marker_hard_slices() {
        let mut s = "x".repeat(100);
        truncate_text_in_place(&mut s, 10);
        assert_eq!(s, "x".repeat(10), "no marker when it cannot fit");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        // 4-byte scorpions: byte-based slicing would panic mid-char.
        let mut s = "\u{1F982}".repeat(2_000);
        truncate_text_in_place(&mut s, 50);
        assert_eq!(chars(&s), 50);
        assert!(s.ends_with(TRUNCATION_MARKER));

        // Multi-byte string whose CHAR count is under the limit must be kept
        // even though its byte length exceeds the limit.
        let mut s = "\u{1F982}".repeat(30); // 120 bytes, 30 chars
        truncate_text_in_place(&mut s, 50);
        assert_eq!(chars(&s), 30, "char count is the unit, not bytes");
    }

    #[test]
    fn capped_string_copies_at_most_the_cap() {
        let big = "y".repeat(1_000_000);
        let capped = capped_string(&big, 100);
        assert_eq!(chars(&capped), 100);
        assert!(capped.ends_with(TRUNCATION_MARKER));
        assert_eq!(capped_string("small", 100), "small");
    }

    #[test]
    fn to_string_bounded_small_matches_serde_json() {
        let obj = json!({"q": "hello", "n": 3, "ok": true, "none": null});
        assert_eq!(
            to_string_bounded(&obj, 100_000).unwrap(),
            serde_json::to_string(&obj).unwrap()
        );
    }

    #[test]
    fn to_string_bounded_huge_string_leaf_is_bounded() {
        // A single multi-MB string LEAF reaches the writer as one chunk; only
        // O(limit) of it may ever be copied.
        let obj = json!({"text": "y".repeat(10_000_000)});
        let start = std::time::Instant::now();
        let out = to_string_bounded(&obj, 5_000).unwrap();
        let elapsed = start.elapsed();
        assert!(chars(&out) <= 5_000, "output exceeds limit: {}", out.len());
        assert!(out.ends_with(TRUNCATION_MARKER));
        // Generous bound: the point is that cost tracks the cap, not the
        // 10 MB payload (which would take far longer to fully encode).
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "bounded encode took {elapsed:?}"
        );
    }

    #[test]
    fn to_string_bounded_huge_collection_stops_early() {
        let huge: Vec<String> = (0..1_000_000).map(|i| format!("k{i}")).collect();
        let out = to_string_bounded(&huge, 2_000).unwrap();
        assert!(chars(&out) <= 2_000);
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn to_string_bounded_unserializable_returns_none() {
        assert!(to_string_bounded(&FailingSerialize, 100).is_none());
    }

    #[test]
    fn to_string_bounded_multibyte_cut_is_lossless_utf8() {
        let obj = json!({ "text": "\u{1F982}".repeat(100_000) });
        let out = to_string_bounded(&obj, 1_000).unwrap();
        assert!(chars(&out) <= 1_000);
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn stringify_value_bounded_parity() {
        assert_eq!(stringify_value_bounded(&Value::Null, 100), "");
        assert_eq!(stringify_value_bounded(&json!("raw"), 100), "raw");
        let capped = stringify_value_bounded(&json!("z".repeat(500)), 100);
        assert_eq!(chars(&capped), 100);
        assert!(capped.ends_with(TRUNCATION_MARKER));
        assert_eq!(stringify_value_bounded(&json!({"a": 1}), 100), r#"{"a":1}"#);
    }

    #[test]
    fn rate_limited_log_gate_emits_once_per_interval_per_family() {
        // Keys are unique to this test; the global map is shared process-wide.
        assert!(should_log_rate_limited("unit_test_family_a"));
        assert!(
            !should_log_rate_limited("unit_test_family_a"),
            "second emission within the interval must be suppressed"
        );
        assert!(
            should_log_rate_limited("unit_test_family_b"),
            "families are rate-limited independently"
        );
    }

    #[test]
    fn stringify_serialize_bounded_unquotes_string_results() {
        assert_eq!(
            stringify_serialize_bounded(&"plain result".to_string(), 100).unwrap(),
            "plain result"
        );
        assert_eq!(
            stringify_serialize_bounded(&Option::<u32>::None, 100).unwrap(),
            ""
        );
        let bounded = stringify_serialize_bounded(&"w".repeat(10_000), 100).unwrap();
        assert!(chars(&bounded) <= 100);
        assert!(bounded.ends_with(TRUNCATION_MARKER));
        assert!(stringify_serialize_bounded(&FailingSerialize, 100).is_none());
    }
}
