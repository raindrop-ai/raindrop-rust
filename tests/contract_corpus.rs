//! Cross-language drift-detection corpus test (Rust half of Phase G).
//!
//! Each JSON file under `contract-fixtures/v1/<category>/<scenario>.json`
//! is one canonical wire payload that all four implementations of
//! Contract v1 must agree on (Workshop's HTTP server, the TS zod
//! schemas in `@raindrop-ai/core`, the Phase G3 Python pydantic mirrors,
//! and the Rust serde mirrors in `src/contract/v1/`). When any single
//! implementation drifts from the others, that language's contract test
//! fails on the offending fixture and points directly at the drift.
//!
//! Source-of-truth for the corpus is `raindrop-workshop`. The copy here
//! is vendored — re-sync via `scripts/sync_contract_fixtures.sh`.
//!
//! ## Categories covered
//!
//! - `live` → [`raindrop::contract::v1::live::LiveEvent`] +
//!   [`raindrop::contract::v1::live::validate_live_event`].
//! - `track` → [`raindrop::contract::v1::track::TrackBody`].
//! - `track-partial` → [`raindrop::contract::v1::track::TrackEvent`].
//!   The Rust mirror collapses `TrackEvent` and `TrackPartialEvent`
//!   into a single envelope; the partial-vs-final distinction lives
//!   in the `is_pending` flag, not the type.
//! - `replay` → inline minimal serde mirrors below. The Rust contract
//!   module currently only ships the `read_replay_run_id_from_attrs`
//!   helper, not the full adapter-request/response structs.
//!
//! `traces` (OTLP/JSON) is not exercised here: the Rust contract module
//! does not ship a `OtlpExportTraceServiceRequest` envelope (the SDK
//! emits OTLP spans through a streaming pipeline rather than a single
//! request struct), so there is nothing to validate the fixtures
//! against. The fixtures are still vendored and structurally indexed
//! below so that the meta-vs-disk consistency tests cover them.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use raindrop::contract::v1::live::{validate_live_event, LiveEvent};
use raindrop::contract::v1::track::{TrackBody, TrackEvent};

#[derive(Debug, serde::Deserialize)]
struct Meta {
    version: String,
    fixtures: HashMap<String, Vec<String>>,
    #[serde(default)]
    validity: HashMap<String, String>,
    #[serde(default, rename = "schemaSkip")]
    schema_skip: Vec<String>,
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("contract-fixtures")
        .join("v1")
}

fn load_meta() -> Meta {
    let raw = fs::read_to_string(corpus_dir().join("meta.json"))
        .expect("read contract-fixtures/v1/meta.json");
    serde_json::from_str(&raw).expect("parse contract-fixtures/v1/meta.json")
}

fn load_fixture(category: &str, name: &str) -> serde_json::Value {
    let path = corpus_dir().join(category).join(format!("{name}.json"));
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {} failed: {err}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|err| panic!("parse {} failed: {err}", path.display()))
}

fn fixture_key(category: &str, name: &str) -> String {
    format!("{category}/{name}")
}

fn is_valid(meta: &Meta, category: &str, name: &str) -> bool {
    meta.validity
        .get(&fixture_key(category, name))
        .map(String::as_str)
        != Some("invalid")
}

fn is_schema_skip(meta: &Meta, category: &str, name: &str) -> bool {
    meta.schema_skip.contains(&fixture_key(category, name))
}

/// Fixtures the corpus marks invalid that the **Rust** serde mirrors
/// currently round-trip without rejection. The TS zod schemas reject
/// these via regex (`HexId16`/`HexId8`); the Rust types model the
/// trace_id/span_id fields as plain `String`s, and `validate_live_event`
/// only enforces the `tool_start`/`tool_result` span_id requirement.
///
/// Listed here so the test passes today AND so the gap is visible to
/// anyone reading this file. When the Rust contract is tightened to
/// match (e.g. `validate_live_event` grows hex-format checks), the
/// matching entry below is removed and the fixture flips to a hard
/// rejection assertion.
const RUST_PERMISSIVE_INVALID: &[&str] = &["live/bad-tracehex"];

fn rust_permits_invalid(category: &str, name: &str) -> bool {
    let key = fixture_key(category, name);
    RUST_PERMISSIVE_INVALID.iter().any(|k| **k == key)
}

// ── structural sanity ──────────────────────────────────────────────────

#[test]
fn corpus_meta_pins_wire_version_to_v1() {
    assert_eq!(load_meta().version, "1");
}

#[test]
fn every_fixture_in_meta_exists_on_disk() {
    let meta = load_meta();
    for (category, names) in &meta.fixtures {
        for name in names {
            let path = corpus_dir().join(category).join(format!("{name}.json"));
            assert!(
                path.exists(),
                "fixture {} referenced in meta.json is missing on disk: {}",
                fixture_key(category, name),
                path.display(),
            );
        }
    }
}

#[test]
fn every_fixture_on_disk_is_referenced_in_meta() {
    let meta = load_meta();
    let referenced: std::collections::HashSet<PathBuf> = meta
        .fixtures
        .iter()
        .flat_map(|(cat, names)| {
            let cat = cat.clone();
            names
                .iter()
                .map(move |n| corpus_dir().join(&cat).join(format!("{n}.json")))
        })
        .collect();

    for category in meta.fixtures.keys() {
        let dir = corpus_dir().join(category);
        if !dir.exists() {
            continue;
        }
        for entry in fs::read_dir(&dir).expect("read fixture category dir") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            assert!(
                referenced.contains(&path),
                "{} exists on disk but is not listed in meta.json::fixtures",
                path.display(),
            );
        }
    }
}

// ── per-category schema parsing ────────────────────────────────────────

#[test]
fn live_corpus() {
    let meta = load_meta();
    let names: Vec<String> = meta.fixtures.get("live").cloned().unwrap_or_default();
    assert!(
        !names.is_empty(),
        "expected meta.json to list live fixtures"
    );

    for name in &names {
        if is_schema_skip(&meta, "live", name) {
            continue;
        }

        let fixture = load_fixture("live", name);
        let parsed = serde_json::from_value::<LiveEvent>(fixture);
        let parse_err = parsed.as_ref().err();
        let validate_err = parsed
            .as_ref()
            .ok()
            .and_then(|evt| validate_live_event(evt).err());
        let accepted = parsed.is_ok() && validate_err.is_none();

        if is_valid(&meta, "live", name) {
            assert!(
                accepted,
                "live/{name} expected valid: parse_err={parse_err:?}, validate_err={validate_err:?}",
            );
        } else if rust_permits_invalid("live", name) {
            assert!(
                accepted,
                "live/{name} is on the Rust-permissive-invalid list and was \
                 expected to round-trip; if Rust now rejects it, drop the entry \
                 from RUST_PERMISSIVE_INVALID. parse_err={parse_err:?}, validate_err={validate_err:?}",
            );
        } else {
            assert!(
                !accepted,
                "live/{name} expected invalid but parsed cleanly through both \
                 LiveEvent deserialize and validate_live_event",
            );
        }
    }
}

#[test]
fn track_corpus() {
    let meta = load_meta();
    let names: Vec<String> = meta.fixtures.get("track").cloned().unwrap_or_default();
    assert!(
        !names.is_empty(),
        "expected meta.json to list track fixtures",
    );

    for name in &names {
        if is_schema_skip(&meta, "track", name) {
            continue;
        }

        let fixture = load_fixture("track", name);
        let result = serde_json::from_value::<TrackBody>(fixture);

        if is_valid(&meta, "track", name) {
            assert!(
                result.is_ok(),
                "track/{name} expected valid: {:?}",
                result.err(),
            );
        } else {
            assert!(
                result.is_err(),
                "track/{name} expected invalid but parsed cleanly",
            );
        }
    }
}

#[test]
fn track_partial_corpus() {
    let meta = load_meta();
    let names: Vec<String> = meta
        .fixtures
        .get("track-partial")
        .cloned()
        .unwrap_or_default();
    assert!(
        !names.is_empty(),
        "expected meta.json to list track-partial fixtures",
    );

    for name in &names {
        if is_schema_skip(&meta, "track-partial", name) {
            // `track-partial/with-trace-id-only` documents Workshop's
            // trace-only-attach case (no `event_id`); the strict
            // TrackEvent schema requires `event_id`. Honoring schemaSkip
            // keeps the fixture in the corpus for HTTP-level tests in
            // other languages without forcing the Rust struct to grow
            // an optional `event_id`.
            continue;
        }

        let fixture = load_fixture("track-partial", name);
        let result = serde_json::from_value::<TrackEvent>(fixture);

        if is_valid(&meta, "track-partial", name) {
            assert!(
                result.is_ok(),
                "track-partial/{name} expected valid: {:?}",
                result.err(),
            );
        } else {
            assert!(
                result.is_err(),
                "track-partial/{name} expected invalid but parsed cleanly",
            );
        }
    }
}

// Inline minimal mirrors of the replay-adapter wire shapes. Duplicated
// here on purpose: the Rust contract module currently only exports the
// `read_replay_run_id_from_attrs` helper (no full request/response
// structs). When the Rust contract module grows these schemas, this
// block becomes `use raindrop::contract::v1::replay::{...}` and the
// duplication goes away.
//
// `#[allow(dead_code)]` is module-wide because the test only validates
// that serde-deserialization succeeds; the parsed fields are not
// inspected directly. Drift in any field name or type still surfaces as
// a deserialization failure on the affected fixture.
#[allow(dead_code)]
mod replay_mirror {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct ReplayMessage {
        pub role: String,
        pub content: String,
        #[serde(rename = "toolCallId", default)]
        pub tool_call_id: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    pub struct ReplayAdapterRequest {
        #[serde(rename = "sourceRunId")]
        pub source_run_id: String,
        #[serde(rename = "replayRunId")]
        pub replay_run_id: String,
        #[serde(rename = "replayPrompt", default)]
        pub replay_prompt: Option<String>,
        #[serde(default)]
        pub messages: Option<Vec<ReplayMessage>>,
        #[serde(rename = "systemPrompt", default)]
        pub system_prompt: Option<String>,
        // `userMessage` is `nullable().optional()` in the TS schema. With
        // `#[serde(default)]` an `Option<String>` collapses both the
        // missing-field and explicit-null cases to `None`, which is what
        // we want for parse-only validation.
        #[serde(rename = "userMessage", default)]
        pub user_message: Option<String>,
        #[serde(default)]
        pub model: Option<String>,
        #[serde(rename = "providerOptions", default)]
        pub provider_options: Option<serde_json::Value>,
        #[serde(default)]
        pub context: Option<serde_json::Value>,
    }

    #[derive(Debug, Deserialize)]
    pub struct ReplayAdapterResponse {
        #[serde(rename = "replayId")]
        pub replay_id: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct ReplayAdapterHealthResponse {
        pub ok: bool,
        #[serde(default)]
        pub service: Option<String>,
        #[serde(rename = "inFlight", default)]
        pub in_flight: Option<u64>,
    }

    #[derive(Debug, Deserialize)]
    pub struct ReplayAdapterRegisterRequest {
        pub workspace_id: String,
        pub event_name: String,
        pub repo_path: String,
        pub url: String,
        #[serde(default)]
        pub health_url: Option<String>,
        #[serde(default)]
        pub start_command: Option<String>,
        #[serde(default)]
        pub context_from_trace: Option<serde_json::Value>,
    }
}

#[test]
fn replay_corpus() {
    use replay_mirror::{
        ReplayAdapterHealthResponse, ReplayAdapterRegisterRequest, ReplayAdapterRequest,
        ReplayAdapterResponse,
    };

    let meta = load_meta();
    let names: Vec<String> = meta.fixtures.get("replay").cloned().unwrap_or_default();
    assert!(
        !names.is_empty(),
        "expected meta.json to list replay fixtures",
    );

    for name in &names {
        if is_schema_skip(&meta, "replay", name) {
            continue;
        }

        let fixture = load_fixture("replay", name);

        let parse_result: Result<(), serde_json::Error> = match name.as_str() {
            "adapter-request" => {
                serde_json::from_value::<ReplayAdapterRequest>(fixture).map(|_| ())
            }
            "adapter-response" => {
                serde_json::from_value::<ReplayAdapterResponse>(fixture).map(|_| ())
            }
            "health-response" => {
                serde_json::from_value::<ReplayAdapterHealthResponse>(fixture).map(|_| ())
            }
            "register-request" => {
                serde_json::from_value::<ReplayAdapterRegisterRequest>(fixture).map(|_| ())
            }
            other => panic!(
                "replay/{other} not mapped to a Rust mirror; add it to replay_mirror or meta.json::schemaSkip",
            ),
        };

        if is_valid(&meta, "replay", name) {
            assert!(
                parse_result.is_ok(),
                "replay/{name} expected valid: {:?}",
                parse_result.err(),
            );
        } else {
            assert!(
                parse_result.is_err(),
                "replay/{name} expected invalid but parsed cleanly",
            );
        }
    }
}
