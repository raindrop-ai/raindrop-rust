//! Wire-level tests for events with no `ai_input` / `ai_output`.
//!
//! Whether an empty event is kept is ingest's decision (decision D6): the SDK
//! sends it, and warns when an interaction is finished with no input, output
//! or attachments (CONTRACT 14.2, rule C5). Complete events (`track_ai`,
//! `track_event`) go to `events/track`; interactions to `events/track_partial`.

mod common;

use std::collections::BTreeMap;

use serde_json::json;
use wiremock::MockServer;

use raindrop::{AiEvent, Attachment, BeginOptions, Event, FinishOptions, PatchOptions};

use crate::common::{fast_client_builder, items, mount_path};

/// A wrapper that records `model`, `convo_id`, and token usage in
/// `properties` but never populates `input` / `output`: the event is still
/// sent, and ingest decides whether to keep it.
#[tokio::test]
async fn chisel_style_track_ai_with_metadata_only_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

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

    let events = items(&recorder);
    assert_eq!(events.len(), 1, "ingest decides whether to keep it (D6)");
    assert_eq!(events[0]["ai_data"]["model"], "swe-1-6-slow");
    assert!(events[0]["ai_data"].get("input").is_none());
}

/// `track_event` called with an empty `event` name defaults to
/// `ai_generation` and is still sent.
#[tokio::test]
async fn track_event_with_empty_event_name_ships_with_default_name() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

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

    let events = items(&recorder);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "ai_generation");
}

/// `track_ai` with every text field blank.
#[tokio::test]
async fn track_ai_with_all_empty_fields_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

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

    assert_eq!(
        items(&recorder).len(),
        1,
        "ingest decides whether to keep it (D6)"
    );
}

/// `begin()` + `finish()` lifecycle with every field defaulted. The merged
/// finish is sent (with a warning, see tests/hardening.rs); ingest decides
/// whether to keep it.
#[tokio::test]
async fn begin_finish_with_no_input_or_output_still_ships() {
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

    let requests = recorder.requests();
    assert_eq!(
        requests.len(),
        1,
        "sent with a warning; ingest decides (C5, D6)"
    );
    assert_eq!(requests[0].json()["is_pending"], false);
}

/// Non-AI `track_event` calls with an explicit event name and no AI fields are
/// the canonical analytics-style event shape. They must NOT be dropped.
#[tokio::test]
async fn non_ai_track_event_with_custom_name_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

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

    let requests = items(&recorder);
    assert_eq!(requests.len(), 1, "non-AI events must still ship");
    let payload = requests[0].clone();
    assert_eq!(payload["event"], "page_view");
    assert!(payload.get("ai_data").is_none() || payload["ai_data"].is_null());
}

/// Attachment-only event: image upload with no text fields. Even though
/// `event_name` resolves to the default `ai_generation` and `ai_data` is
/// `None` (no text fields populated), the payload is not phantom — the
/// attachment is real content. Must still ship.
#[tokio::test]
async fn attachment_only_event_with_default_name_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

    let client = fast_client_builder(&server).build().expect("build client");

    client
        .track_event(Event {
            event_id: "evt_attach".into(),
            user_id: "user-attach".into(),
            event: String::new(),
            attachments: vec![Attachment {
                kind: "image".into(),
                role: "input".into(),
                value: "https://example.com/cat.png".into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("track_event");

    let _ = client.close().await;

    let requests = items(&recorder);
    assert_eq!(
        requests.len(),
        1,
        "attachment-only event must still ship even with default event name"
    );
    let payload = requests[0].clone();
    assert_eq!(payload["attachments"][0]["type"], "image");
    assert_eq!(
        payload["attachments"][0]["value"],
        "https://example.com/cat.png"
    );
}

/// Attachment-only event where the caller ALSO set `model` (or `convo_id`),
/// causing `build_track_partial_payload` to attach `ai_data`. The drop gate
/// hits the `Some(data)` branch with empty `input`/`output`, but the
/// payload carries a real attachment so it must still ship. This is the
/// canonical "vision" or "image generation" shape: the model is named
/// (`gpt-4o`, `dall-e-3`, ...) but the prompt/response are represented as
/// attachments rather than text.
///
/// Regression for the Bugbot finding on 341f2c0: prior to the fix, the
/// `Some(data)` branch dropped any payload with empty text regardless of
/// attachments, silently discarding image-only AI events despite the
/// CHANGELOG claim that "attachment-only events with no AI text still ship".
#[tokio::test]
async fn attachment_only_ai_event_with_model_set_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

    let client = fast_client_builder(&server).build().expect("build client");

    client
        .track_ai(AiEvent {
            event_id: "evt_vision".into(),
            user_id: "user-vision".into(),
            event: "ai_generation".into(),
            input: String::new(),
            output: String::new(),
            model: "gpt-4o".into(),
            convo_id: "conv-vision".into(),
            attachments: vec![Attachment {
                kind: "image".into(),
                role: "input".into(),
                value: "https://example.com/screenshot.png".into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let _ = client.close().await;

    let requests = items(&recorder);
    assert_eq!(
        requests.len(),
        1,
        "vision/image-only AI event must ship even though ai_data is attached \
         (model+convo_id set) and input/output are both empty — attachments are payload"
    );
    let payload = requests[0].clone();
    assert_eq!(payload["ai_data"]["model"], "gpt-4o");
    assert_eq!(payload["ai_data"]["convo_id"], "conv-vision");
    assert_eq!(payload["attachments"][0]["type"], "image");
    assert_eq!(
        payload["attachments"][0]["value"],
        "https://example.com/screenshot.png"
    );
}

/// A wrapper that captures the prompt but the model returned an empty
/// response (e.g. errored mid-stream). With `input` populated and `output`
/// empty, this is a legitimate "errored generation" shape and must still
/// ship — the wrapper can attach an `LlmSpan::set_error` to carry the error
/// detail through Dawn's `error_spans` extension.
#[tokio::test]
async fn errored_generation_with_input_only_still_ships() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;

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

    let requests = items(&recorder);
    assert_eq!(requests.len(), 1);
    let payload = requests[0].clone();
    assert_eq!(payload["ai_data"]["input"], "Hello");
}

/// In-flight `begin()` whose flush ships an `is_pending=true` patch must NOT
/// be dropped, even when the patch only has metadata. Pending intermediates
/// may legitimately have no output yet. We force the flush via `close()`
/// rather than wall-clock sleeps so the test is deterministic on slow CI.
#[tokio::test]
async fn pending_begin_with_input_is_not_dropped() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

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

    let _ = client.close().await;

    let requests = recorder.requests();
    assert!(!requests.is_empty(), "pending begin patch should ship");
    let payload = requests[0].json();
    assert_eq!(payload["is_pending"], true);
    assert_eq!(payload["ai_data"]["input"], "Tell me a joke");
}

/// `client.patch(...)` with only a `user_id` produces a pending phantom event
/// at flush time. is_pending=true, so it is NOT dropped — the user may
/// follow up with a `finish` that finalizes the event. (Dropping pending
/// phantoms here would break legitimate `patch`-then-`finish` flows that
/// pass `user_id` first and the prompt later.) We force the flush via
/// `close()` so the test is deterministic on slow CI.
#[tokio::test]
async fn patch_only_user_id_still_ships_as_pending() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

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
    let recorder = mount_path(&server, "POST", "/events/track").await;

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

    let requests = items(&recorder);
    assert_eq!(requests.len(), 1);
    let payload = requests[0].clone();
    assert_eq!(payload["ai_data"]["input"], "Hi");
    assert_eq!(payload["ai_data"]["output"], "Hello!");
    assert_eq!(payload["event"], "chat");
}
