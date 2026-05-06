mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use raindrop::{AiEvent, Attachment, BeginOptions, Event, FinishOptions, PatchOptions};

use crate::common::{fast_client_builder, json_get, mount_any_post, mount_path};

#[tokio::test]
async fn track_ai_sends_track_partial_payload() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build client");

    let mut props = BTreeMap::new();
    props.insert("ai.usage.prompt_tokens".into(), json!(10));
    props.insert("ai.usage.completion_tokens".into(), json!(5));

    client
        .track_ai(AiEvent {
            event_id: "evt_123".into(),
            user_id: "user-123".into(),
            event: "ai_generation".into(),
            input: "What is the capital of France?".into(),
            output: "Paris".into(),
            model: "gpt-4o".into(),
            convo_id: "conv-123".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let payload = recorder.requests()[0].json();
    assert_eq!(payload["event_id"], "evt_123");
    assert_eq!(payload["ai_data"]["model"], "gpt-4o");
    assert_eq!(payload["is_pending"], false);
    let library_name = json_get(&payload, &["properties", "$context", "library", "name"])
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(library_name, "raindrop-rust");

    let _ = client.close().await;
}

#[tokio::test]
async fn interaction_buffers_and_flushes_on_finish() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server)
        .partial_flush_interval(Duration::from_millis(10))
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_interaction".into(),
            user_id: "user-123".into(),
            event: "chat_message".into(),
            input: "Hello".into(),
            model: "gpt-4o".into(),
            ..Default::default()
        })
        .await;

    interaction
        .set_property("stage", "processing")
        .await
        .expect("set property");
    interaction
        .finish(FinishOptions {
            output: "Hi there!".into(),
            ..Default::default()
        })
        .await
        .expect("finish");

    let _ = client.close().await;

    let requests = recorder.requests();
    assert_eq!(
        requests.len(),
        1,
        "expected exactly one request, got {:?}",
        requests.len()
    );
    let payload = requests[0].json();
    assert_eq!(payload["properties"]["stage"], "processing");
    assert_eq!(payload["ai_data"]["output"], "Hi there!");
    assert_eq!(payload["is_pending"], false);
}

#[tokio::test]
async fn interaction_helpers_merge_properties_and_attachments() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_helpers".into(),
            user_id: "user-123".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;
    assert_eq!(interaction.event_id(), "evt_helpers");
    interaction
        .set_property("stage", "planning")
        .await
        .expect("set property");
    interaction
        .add_attachments(vec![Attachment {
            kind: "text".into(),
            role: "output".into(),
            name: "reasoning-summary".into(),
            value: "Short summary".into(),
            ..Default::default()
        }])
        .await
        .expect("add attachments");
    interaction
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    let payload = recorder
        .requests()
        .last()
        .expect("at least one request")
        .json();
    assert_eq!(payload["properties"]["stage"], "planning");
    let attachments = payload["attachments"].as_array().expect("attachments");
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0]["name"], "reasoning-summary");
}

#[tokio::test]
async fn resume_interaction_and_set_input_finalize_event() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");

    let _ = client
        .begin(BeginOptions {
            event_id: "evt_resumed".into(),
            user_id: "user-123".into(),
            event: "chat_message".into(),
            input: "first input".into(),
            ..Default::default()
        })
        .await;
    let resumed = client.resume_interaction("evt_resumed");
    resumed.set_input("resumed input").await.expect("set input");
    resumed
        .set_property("stage", "resumed")
        .await
        .expect("set property");
    resumed
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    let payload = recorder.requests().last().expect("a request").json();
    assert_eq!(payload["event_id"], "evt_resumed");
    assert_eq!(payload["ai_data"]["input"], "resumed input");
    assert_eq!(payload["ai_data"]["output"], "done");
    assert_eq!(payload["properties"]["stage"], "resumed");
    assert_eq!(payload["is_pending"], false);
}

#[tokio::test]
async fn track_signal_and_identify_use_expected_endpoints() {
    let server = MockServer::start().await;
    let recorder = mount_any_post(&server).await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_signal(raindrop::Signal {
            event_id: "evt_123".into(),
            name: "thumbs_up".into(),
            kind: "feedback".into(),
            sentiment: "POSITIVE".into(),
            ..Default::default()
        })
        .await
        .expect("track_signal");

    client
        .identify(raindrop::User {
            user_id: "user-123".into(),
            traits: {
                let mut m = BTreeMap::new();
                m.insert("plan".into(), Value::String("paid".into()));
                m
            },
        })
        .await
        .expect("identify");

    let _ = client.close().await;

    let signal_calls = recorder
        .requests()
        .iter()
        .filter(|r| r.path == "/signals/track")
        .count();
    let identify_calls = recorder
        .requests()
        .iter()
        .filter(|r| r.path == "/users/identify")
        .count();
    assert_eq!(signal_calls, 1);
    assert_eq!(identify_calls, 1);
}

#[tokio::test]
async fn track_event_sends_non_ai_payload() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let mut props = BTreeMap::new();
    props.insert("entrypoint".into(), Value::String("dashboard".into()));

    client
        .track_event(Event {
            user_id: "user-123".into(),
            event: "session_started".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_event");
    let _ = client.close().await;

    let payload = recorder.requests().last().expect("a request").json();
    assert_eq!(payload["event"], "session_started");
    assert_eq!(payload["user_id"], "user-123");
    assert_eq!(payload["properties"]["entrypoint"], "dashboard");
    assert!(payload.get("ai_data").is_none() || payload["ai_data"].is_null());
}

#[tokio::test]
async fn pending_patch_without_user_id_stays_buffered() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_missing_user".into(),
            input: "draft input".into(),
            event: "chat_message".into(),
            ..Default::default()
        })
        .await;

    client.flush().await.expect("flush 1");
    assert_eq!(recorder.count(), 0, "expected no requests yet");

    interaction
        .patch(PatchOptions {
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await
        .expect("patch user");
    interaction
        .finish(FinishOptions {
            output: "completed output".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    assert_eq!(
        recorder.count(),
        1,
        "expected exactly one request after user attached"
    );
    let payload = recorder.requests()[0].json();
    assert_eq!(payload["ai_data"]["input"], "draft input");
    assert_eq!(payload["ai_data"]["output"], "completed output");
}

#[tokio::test]
async fn close_flushes_pending_events_and_spans() {
    let server = MockServer::start().await;
    let event_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server)
        .partial_flush_interval(Duration::from_millis(100))
        .trace_flush_interval(Duration::from_millis(100))
        .build()
        .expect("build");

    let _interaction = client
        .begin(BeginOptions {
            event_id: "evt_close".into(),
            user_id: "user-123".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;

    let span = client.start_span(raindrop::SpanOptions {
        name: "close_flush".into(),
        event_id: "evt_close".into(),
        ..Default::default()
    });
    span.end();

    client.close().await.expect("close");
    assert!(
        event_recorder.count() >= 1,
        "expected pending event to flush on close"
    );
    assert!(
        trace_recorder.count() >= 1,
        "expected spans to flush on close"
    );
}

#[tokio::test]
async fn periodic_flush_timer_works() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .partial_flush_interval(Duration::from_millis(15))
        .build()
        .expect("build");

    let _interaction = client
        .begin(BeginOptions {
            event_id: "evt_pending".into(),
            user_id: "user-123".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;

    // Wait for periodic flush
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        recorder.count() >= 1,
        "expected at least one flush from periodic timer, got 0"
    );
    let _ = client.close().await;
}

#[tokio::test]
async fn noop_client_without_write_key_makes_no_requests() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204).set_body_string(""))
        .expect(0)
        .mount(&server)
        .await;

    let client = raindrop::Client::builder()
        .endpoint(format!("{}/", server.uri()))
        .build()
        .expect("build");

    client
        .track_ai(AiEvent {
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai noop");
    client
        .track_signal(raindrop::Signal {
            name: "thumbs_up".into(),
            ..Default::default()
        })
        .await
        .expect("track_signal noop");
    client
        .identify(raindrop::User {
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await
        .expect("identify noop");
    let _ = client.close().await;
    server.verify().await;
}

#[tokio::test]
async fn begin_on_disabled_client_returns_safe_interaction() {
    let server = MockServer::start().await;
    let _ = server; // not used; we just need an arbitrary endpoint

    let client = raindrop::Client::builder().build().expect("build");
    let interaction = client
        .begin(BeginOptions {
            user_id: "user-123".into(),
            event: "chat_message".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;
    // Should succeed without panicking.
    interaction
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish on disabled client");
}
