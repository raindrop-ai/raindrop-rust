mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use wiremock::MockServer;

use raindrop::{AiEvent, Attribute, SpanOptions};

use crate::common::{fast_client_builder, mount_path, spans_of};

#[tokio::test]
async fn concurrent_track_ai_calls_are_thread_safe() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = Arc::new(fast_client_builder(&server).build().expect("build"));

    let mut handles = Vec::new();
    for i in 0..32 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.track_ai(AiEvent {
                event_id: format!("evt_{}", i),
                user_id: format!("user-{}", i),
                input: format!("input-{}", i),
                model: "gpt-4o".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
    client.close().await.expect("close");
    assert_eq!(recorder.count(), 32, "expected 32 distinct flushes");
}

#[tokio::test]
async fn concurrent_spans_share_one_buffer() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = Arc::new(fast_client_builder(&server).build().expect("build"));

    let mut handles = Vec::new();
    for i in 0..50 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let span = c.start_span(SpanOptions {
                name: format!("span_{}", i),
                event_id: format!("evt_{}", i),
                ..Default::default()
            });
            span.set_attributes([Attribute::int("worker", i)]);
            span.end();
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
    client.close().await.expect("close");

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    assert_eq!(
        all_spans.len(),
        50,
        "expected 50 spans, got {}",
        all_spans.len()
    );
}

#[tokio::test]
async fn idempotent_close_is_safe() {
    let server = MockServer::start().await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");
    client.close().await.expect("first close");
    client.close().await.expect("second close");
    assert!(client.is_closed());
}

#[tokio::test]
async fn span_clone_shares_state() {
    // Cloning a Span should produce a handle to the same span — calling end() on either
    // ends the span exactly once.
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server).build().expect("build");
    let span = client.start_span(SpanOptions {
        name: "shared".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    let span_clone = span.clone();
    span_clone.set_attributes([Attribute::string("from", "clone")]);
    span.end();
    // Second end on a different handle should be a no-op.
    span_clone.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all = Vec::new();
    for req in trace_recorder.requests() {
        all.extend(spans_of(&req.json()));
    }
    assert_eq!(
        all.len(),
        1,
        "expected exactly one span emission for shared clones"
    );
    let s = &all[0];
    let from = crate::common::span_attr(s, "from").unwrap();
    assert_eq!(from["stringValue"], "clone");
}

#[tokio::test]
async fn signals_skip_when_disabled() {
    let client = raindrop::Client::builder().build().expect("build");
    client
        .track_signal(raindrop::Signal {
            name: "thumbs_up".into(),
            ..Default::default()
        })
        .await
        .expect("ok on disabled client");
}

#[tokio::test]
async fn json_property_values_pass_through_unchanged() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let mut props = BTreeMap::new();
    props.insert(
        "metadata".into(),
        json!({"tier": "pro", "tokens": 10, "active": true}),
    );

    client
        .track_ai(AiEvent {
            event_id: "evt_props".into(),
            user_id: "user-1".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_ai");
    let _ = client.close().await;

    let payload = recorder.requests().last().cloned().unwrap().json();
    assert_eq!(payload["properties"]["metadata"]["tier"], "pro");
    assert_eq!(payload["properties"]["metadata"]["tokens"], 10);
    assert_eq!(payload["properties"]["metadata"]["active"], true);
}

#[tokio::test]
async fn periodic_trace_flush_batches_spans() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", server.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::from_millis(20))
        .trace_max_batch_size(100)
        .build()
        .expect("build");

    for i in 0..10 {
        let span = client.start_span(SpanOptions {
            name: format!("s{}", i),
            event_id: "evt".into(),
            ..Default::default()
        });
        span.end();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    client.close().await.expect("close");
    let count = trace_recorder.count();
    let mut all = Vec::new();
    for req in trace_recorder.requests() {
        all.extend(spans_of(&req.json()));
    }
    assert_eq!(all.len(), 10);
    // We should have shipped them in 1 or 2 requests (not 10).
    assert!(
        count <= 3,
        "expected batched shipping, got {} requests",
        count
    );
}
