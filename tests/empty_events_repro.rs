//! Wire-level regression tests for the "empty `ai_generation` events" bug.
//!
//! Each test exercises a code path that used to ship a phantom
//! `ai_generation` track_partial payload with no `ai_input` / `ai_output`,
//! and asserts that after the buffer-level drop in
//! `should_drop_empty_ai_event` no such payload reaches the network.
//! Legitimate non-AI events and partial in-flight interactions are
//! exercised here too to guarantee the drop does not over-fire.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::json;
use wiremock::MockServer;

use raindrop::{AiEvent, BeginOptions, Event, FinishOptions, PatchOptions};

use crate::common::{fast_client_builder, mount_path};

/// The actual production failure mode (chisel/1.0.0): a wrapper that records
/// `model`, `convo_id`, and token usage in `properties` but never populates
/// `input` / `output`. Before the fix this shipped a finalized event with
/// `ai_data` attached but `ai_input`/`ai_output` empty.
#[tokio::test]
async fn chisel_style_track_ai_with_metadata_only_is_dropped() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    let mut props = BTreeMap::new();
    props.insert("total_input_tokens".into(), json!(123));
    props.insert("output_tokens".into(), json!(45));
    props.insert("total_time_ms".into(), json!(789));

    client
        .track_ai(AiEvent {
            event_id: "evt_chisel".into(),
            user_id: "user-chisel".into(),
            event: "ai_generation".into(),
            input: String::new(),
            output: String::new(),
            model: "swe-1-6-slow".into(),
            convo_id: "superb-october".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let _ = client.close().await;

    assert!(
        recorder.requests().is_empty(),
        "chisel-style empty-text track_ai must be dropped"
    );
}

/// `track_event` called with an empty `event` name silently defaults to
/// `ai_generation`. With no AI text fields populated, this used to ship as a
/// phantom event. The fix drops it.
#[tokio::test]
async fn track_event_with_empty_event_name_is_dropped() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    client
        .track_event(Event {
            event_id: "evt_empty_track".into(),
            user_id: "user-empty-1".into(),
            event: String::new(),
            ..Default::default()
        })
        .await
        .expect("track_event");

    let _ = client.close().await;

    assert!(recorder.requests().is_empty());
}

/// `track_ai` with every text field blank.
#[tokio::test]
async fn track_ai_with_all_empty_fields_is_dropped() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    client
        .track_ai(AiEvent {
            event_id: "evt_phantom_ai".into(),
            user_id: "user-empty-2".into(),
            event: String::new(),
            input: String::new(),
            output: String::new(),
            model: String::new(),
            convo_id: String::new(),
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let _ = client.close().await;

    assert!(recorder.requests().is_empty());
}

/// `begin()` + `finish()` lifecycle with every field defaulted. The pending
/// `begin` patch is buffered, the `finish` patch merges and flushes; before
/// the fix the single merged shipment was a phantom `ai_generation`. With
/// the fix it is dropped at flush time.
#[tokio::test]
async fn begin_finish_with_no_input_or_output_is_dropped() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_blank_interaction".into(),
            user_id: "user-empty-4".into(),
            event: String::new(),
            input: String::new(),
            model: String::new(),
            convo_id: String::new(),
            ..Default::default()
        })
        .await;
    interaction
        .finish(FinishOptions {
            output: String::new(),
            model: String::new(),
            ..Default::default()
        })
        .await
        .expect("finish");

    let _ = client.close().await;

    assert!(recorder.requests().is_empty());
}

/// Non-AI `track_event` calls with an explicit event name and no AI fields are
/// the canonical analytics-style event shape. They must NOT be dropped.
#[tokio::test]
async fn non_ai_track_event_with_custom_name_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    let mut props = BTreeMap::new();
    props.insert("path".into(), json!("/home"));

    client
        .track_event(Event {
            event_id: "evt_page_view".into(),
            user_id: "user-pv".into(),
            event: "page_view".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_event");

    let _ = client.close().await;

    let requests = recorder.requests();
    assert_eq!(requests.len(), 1, "non-AI events must still ship");
    let payload = requests[0].json();
    assert_eq!(payload["event"], "page_view");
    assert_eq!(payload["is_pending"], false);
    assert!(payload.get("ai_data").is_none() || payload["ai_data"].is_null());
}

/// A wrapper that captures the prompt but the model returned an empty
/// response (e.g. errored mid-stream). With `input` populated and `output`
/// empty, this is a legitimate "errored generation" shape and must still
/// ship — the wrapper can attach an `LlmSpan::set_error` to carry the error
/// detail through Dawn's `error_spans` extension.
#[tokio::test]
async fn errored_generation_with_input_only_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    client
        .track_ai(AiEvent {
            event_id: "evt_errored_gen".into(),
            user_id: "user-err".into(),
            event: "chat".into(),
            input: "Hello".into(),
            output: String::new(),
            model: "gpt-4o".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let _ = client.close().await;

    let requests = recorder.requests();
    assert_eq!(requests.len(), 1);
    let payload = requests[0].json();
    assert_eq!(payload["ai_data"]["input"], "Hello");
    assert_eq!(payload["is_pending"], false);
}

/// In-flight `begin()` whose periodic flush ships an `is_pending=true` patch
/// must NOT be dropped, even when the patch only has metadata. Pending
/// intermediates may legitimately have no output yet.
#[tokio::test]
async fn pending_begin_with_input_is_not_dropped() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server)
        .partial_flush_interval(Duration::from_millis(10))
        .build()
        .expect("build client");

    let _interaction = client
        .begin(BeginOptions {
            event_id: "evt_pending".into(),
            user_id: "user-pending".into(),
            event: "chat".into(),
            input: "Tell me a joke".into(),
            model: "gpt-4o".into(),
            ..Default::default()
        })
        .await;

    // Let the periodic ticker flush the pending begin patch.
    tokio::time::sleep(Duration::from_millis(120)).await;

    let _ = client.close().await;

    let requests = recorder.requests();
    assert!(!requests.is_empty(), "pending begin patch should ship");
    let payload = requests[0].json();
    assert_eq!(payload["is_pending"], true);
    assert_eq!(payload["ai_data"]["input"], "Tell me a joke");
}

/// `client.patch(...)` with only a `user_id` produces a pending phantom event
/// via the periodic flush. is_pending=true, so it is NOT dropped — the user
/// may follow up with a `finish` that finalizes the event. (Dropping pending
/// phantoms here would break legitimate `patch`-then-`finish` flows that
/// pass `user_id` first and the prompt later.)
#[tokio::test]
async fn patch_only_user_id_still_ships_as_pending() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server)
        .partial_flush_interval(Duration::from_millis(10))
        .build()
        .expect("build client");

    client
        .patch(
            "evt_patch_only",
            PatchOptions {
                user_id: "user-empty-3".into(),
                ..Default::default()
            },
        )
        .await
        .expect("patch");

    tokio::time::sleep(Duration::from_millis(120)).await;

    let _ = client.close().await;

    let requests = recorder.requests();
    assert!(!requests.is_empty());
    let payload = requests[0].json();
    assert_eq!(payload["is_pending"], true);
}

/// Sanity check: a fully populated `track_ai` ships verbatim. Mirrors the
/// happy path covered in `tests/events.rs` to guard against the drop
/// accidentally matching a legitimate payload.
#[tokio::test]
async fn full_ai_event_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    client
        .track_ai(AiEvent {
            event_id: "evt_happy".into(),
            user_id: "user-happy".into(),
            event: "chat".into(),
            input: "Hi".into(),
            output: "Hello!".into(),
            model: "gpt-4o".into(),
            convo_id: "conv".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let _ = client.close().await;

    let requests = recorder.requests();
    assert_eq!(requests.len(), 1);
    let payload = requests[0].json();
    assert_eq!(payload["ai_data"]["input"], "Hi");
    assert_eq!(payload["ai_data"]["output"], "Hello!");
    assert_eq!(payload["event"], "chat");
}
