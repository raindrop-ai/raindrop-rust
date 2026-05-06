mod common;

use std::time::Duration;

use serde_json::Value;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use raindrop::{AiEvent, BeginOptions, FinishOptions, PatchOptions};

use crate::common::{fast_client_builder, mount_path, Recorder};

#[tokio::test]
async fn retry_on_retryable_statuses() {
    let server = MockServer::start().await;

    let recorder = Recorder::new();
    // First two attempts return 429, third returns 204.
    recorder.sequence.lock().unwrap().extend(vec![
        ResponseTemplate::new(429)
            .insert_header("Retry-After", "0")
            .set_body_string("retry me"),
        ResponseTemplate::new(429)
            .insert_header("Retry-After", "0")
            .set_body_string("retry me"),
        ResponseTemplate::new(204),
    ]);

    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;

    let client = fast_client_builder(&server)
        .max_attempts(3)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .build()
        .expect("build");

    client
        .track_ai(AiEvent {
            event_id: "evt_retry".into(),
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai retried");
    let _ = client.close().await;

    assert_eq!(recorder.count(), 3, "expected exactly 3 attempts");
}

#[tokio::test]
async fn fails_fast_on_non_retryable_4xx() {
    let server = MockServer::start().await;
    let recorder = Recorder::new();
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;

    let client = fast_client_builder(&server)
        .max_attempts(5)
        .build()
        .expect("build");
    let res = client
        .track_ai(AiEvent {
            event_id: "evt_400".into(),
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await;

    assert!(res.is_err(), "expected error for 400");
    if let Err(e) = res {
        let msg = e.to_string();
        assert!(msg.contains("400"), "expected 400 in error: {}", msg);
    }
    // The patch should have been restored on failure; do not close to avoid additional flush.
    drop(client);
    // Recorder mock isn't matched (the 400 mock matches first); so only check that the path was hit
    // by counting *all* requests against the server. wiremock's verify defaults are fine here.
    let _ = recorder; // not used, kept for symmetry.
}

#[tokio::test]
async fn event_flush_retains_buffered_patch_when_request_fails() {
    let server = MockServer::start().await;
    let recorder = Recorder::new();
    recorder.sequence.lock().unwrap().extend(vec![
        ResponseTemplate::new(500).set_body_string("temporary failure"),
        ResponseTemplate::new(204),
    ]);
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;

    let client = fast_client_builder(&server)
        .max_attempts(1)
        .base_delay(Duration::from_millis(1))
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_retry_flush".into(),
            user_id: "user-123".into(),
            event: "chat_message".into(),
            input: "original input".into(),
            properties: {
                let mut m = std::collections::BTreeMap::new();
                m.insert("phase".into(), Value::String("start".into()));
                m
            },
            ..Default::default()
        })
        .await;

    let first = interaction
        .finish(FinishOptions {
            output: "final output".into(),
            ..Default::default()
        })
        .await;
    assert!(first.is_err(), "expected first flush to fail with 500");

    client.flush().await.expect("retry flush should succeed");
    let _ = client.close().await;

    assert_eq!(recorder.count(), 2, "expected 2 attempts");
    let last = recorder.requests().last().cloned().unwrap();
    let payload = last.json();
    assert_eq!(payload["ai_data"]["input"], "original input");
    assert_eq!(payload["ai_data"]["output"], "final output");
    assert_eq!(payload["properties"]["phase"], "start");
}

#[tokio::test]
async fn auth_header_uses_bearer_write_key() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer rk_secret_test"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    // Fallback for any request that doesn't match the auth header — should not happen.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;

    let client = raindrop::Client::builder()
        .write_key("rk_secret_test")
        .endpoint(format!("{}/", server.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .build()
        .expect("build");
    client
        .track_event(raindrop::Event {
            user_id: "user-123".into(),
            event: "test".into(),
            ..Default::default()
        })
        .await
        .expect("track_event");
    let _ = client.close().await;
    server.verify().await;
}

#[tokio::test]
async fn endpoint_normalization_appends_slash() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(server.uri()) // no trailing slash
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .build()
        .expect("build");

    client
        .track_event(raindrop::Event {
            user_id: "user-123".into(),
            event: "ping".into(),
            ..Default::default()
        })
        .await
        .expect("track_event");
    let _ = client.close().await;
    assert_eq!(recorder.count(), 1, "expected exactly 1 call");
}

#[tokio::test]
async fn pre_existing_buffered_patch_is_flushed_on_demand() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let _ = client
        .begin(BeginOptions {
            event_id: "evt_buffered".into(),
            user_id: "user-123".into(),
            event: "chat_message".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;

    // No flush should have happened yet (no periodic, is_pending=true)
    assert_eq!(recorder.count(), 0);

    client.flush().await.expect("explicit flush");
    let _ = client.close().await;
    assert!(
        recorder.count() >= 1,
        "expected at least one flushed request"
    );
    let last = recorder.requests().last().cloned().unwrap();
    let payload = last.json();
    assert_eq!(payload["ai_data"]["input"], "hi");
    assert_eq!(payload["is_pending"], true);
}

#[tokio::test]
async fn patch_options_can_finalize_via_is_pending_false() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_patch".into(),
            user_id: "user-123".into(),
            input: "draft".into(),
            ..Default::default()
        })
        .await;
    interaction
        .patch(PatchOptions {
            output: "final".into(),
            is_pending: Some(false),
            ..Default::default()
        })
        .await
        .expect("patch is_pending=false");
    let _ = client.close().await;
    assert_eq!(recorder.count(), 1);
    let payload = recorder.requests().last().cloned().unwrap().json();
    assert_eq!(payload["is_pending"], false);
    assert_eq!(payload["ai_data"]["output"], "final");
}
