//! Behaviour required by the SDK contract: stable start times, resuming an
//! event begun elsewhere, the batched non-blocking send queue, bounded
//! flush, retry rules, ingest response warnings, saved span parents and
//! detached sub-agents.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use time::macros::datetime;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use raindrop::{
    AiEvent, BeginOptions, Event, FinishOptions, PatchOptions, ResumeOptions,
    ResumeSubagentOptions, Span, SubagentOptions, TrackToolOptions, HANDOFF_HEADER,
};

use crate::common::{fast_client_builder, items, mount_path, spans_of, Recorder};

#[derive(Clone)]
struct VecWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for VecWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Run `f` on a current-thread runtime and return the WARN output it logged.
fn capture_warn_logs<F, Fut>(f: F) -> String
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let sink: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let writer_sink = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || VecWriter(writer_sink.clone()))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(f());
    });
    let captured = sink.lock().unwrap().clone();
    String::from_utf8(captured).expect("utf8 logs")
}

fn is_millis_utc(ts: &str) -> bool {
    // YYYY-MM-DDTHH:MM:SS.mmmZ
    ts.len() == 24 && ts.ends_with('Z') && ts.as_bytes()[19] == b'.'
}

// ======================================================================
// Start time (C1)
// ======================================================================

#[tokio::test]
async fn every_update_carries_the_events_start_time() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_start".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;
    client.flush().await.expect("flush begin");
    tokio::time::sleep(Duration::from_millis(20)).await;
    interaction
        .patch(PatchOptions {
            output: "partial".into(),
            ..Default::default()
        })
        .await
        .expect("patch");
    client.flush().await.expect("flush patch");
    tokio::time::sleep(Duration::from_millis(20)).await;
    interaction
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    let sent = items(&recorder);
    assert!(
        sent.len() >= 3,
        "expected begin, patch and finish: {sent:?}"
    );
    let first = sent[0]["timestamp"]
        .as_str()
        .expect("timestamp")
        .to_string();
    assert!(
        is_millis_utc(&first),
        "unexpected timestamp format: {first}"
    );
    for item in &sent {
        assert_eq!(
            item["timestamp"],
            first.as_str(),
            "start time must not move"
        );
    }
}

#[tokio::test]
async fn explicit_start_time_is_honoured_and_only_moves_earlier() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_explicit".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            timestamp: Some(datetime!(2026-01-02 03:04:05.678 UTC)),
            ..Default::default()
        })
        .await;
    // A later value does not replace the recorded start.
    interaction
        .patch(PatchOptions {
            timestamp: Some(datetime!(2026-01-02 09:00:00 UTC)),
            ..Default::default()
        })
        .await
        .expect("patch");
    interaction
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    for item in items(&recorder) {
        assert_eq!(item["timestamp"], "2026-01-02T03:04:05.678Z");
    }
}

// ======================================================================
// Resume (C2)
// ======================================================================

// One test, because the warning is rate-limited process-wide.
#[test]
fn resumed_update_without_identity_omits_user_event_and_timestamp_and_warns() {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let sent_in = sent.clone();
    let logs = capture_warn_logs(|| async move {
        let server = MockServer::start().await;
        let recorder = mount_path(&server, "POST", "/events/track_partial").await;
        let client = fast_client_builder(&server).build().expect("build");
        client
            .resume_interaction("evt_resumed")
            .finish(FinishOptions {
                output: "finished in another process".into(),
                ..Default::default()
            })
            .await
            .expect("finish");
        let _ = client.close().await;
        *sent_in.lock().unwrap() = items(&recorder);
    });

    let sent = sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1, "resumed finish must be sent: {sent:?}");
    let item = &sent[0];
    assert_eq!(item["event_id"], "evt_resumed");
    assert_eq!(item["is_pending"], false);
    assert_eq!(item["ai_data"]["output"], "finished in another process");
    for key in ["user_id", "event", "timestamp"] {
        assert!(item.get(key).is_none(), "{key} must be omitted: {item}");
    }
    assert!(
        logs.contains("resumed event without a user_id"),
        "expected a warning, logs: {logs}"
    );
}

#[tokio::test]
async fn resume_interaction_with_sends_the_supplied_identity() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .resume_interaction_with(
            "evt_resumed_id",
            ResumeOptions {
                user_id: "user-123".into(),
                convo_id: "convo-1".into(),
                event: "chat_message".into(),
            },
        )
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    let sent = items(&recorder);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["user_id"], "user-123");
    assert_eq!(sent[0]["event"], "chat_message");
    assert_eq!(sent[0]["ai_data"]["convo_id"], "convo-1");
    assert!(
        sent[0].get("timestamp").is_none(),
        "start time is unknown here"
    );
}

#[test]
fn finishing_twice_warns() {
    let logs = capture_warn_logs(|| async {
        let server = MockServer::start().await;
        let _recorder = mount_path(&server, "POST", "/events/track_partial").await;
        let client = fast_client_builder(&server).build().expect("build");
        let interaction = client
            .begin(BeginOptions {
                event_id: "evt_twice".into(),
                user_id: "user-123".into(),
                input: "hi".into(),
                ..Default::default()
            })
            .await;
        for _ in 0..2 {
            interaction
                .finish(FinishOptions {
                    output: "done".into(),
                    ..Default::default()
                })
                .await
                .expect("finish");
        }
        let _ = client.close().await;
    });
    assert!(
        logs.contains("already finished"),
        "expected a repeated-finish warning, logs: {logs}"
    );
}

// ======================================================================
// Send queue
// ======================================================================

#[tokio::test]
async fn track_calls_queue_without_waiting_for_the_network() {
    let server = MockServer::start().await;
    let recorder = Recorder::new();
    recorder
        .sequence
        .lock()
        .unwrap()
        .push(ResponseTemplate::new(204).set_delay(Duration::from_secs(2)));
    Mock::given(method("POST"))
        .and(path("/events/track"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;
    let client = fast_client_builder(&server).build().expect("build");

    let start = std::time::Instant::now();
    for i in 0..3 {
        client
            .track_event(Event {
                event_id: format!("evt_q_{i}"),
                user_id: "user-123".into(),
                event: "queued".into(),
                ..Default::default()
            })
            .await
            .expect("track_event");
    }
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "track_event must not wait for delivery ({:?})",
        start.elapsed()
    );
    client.flush().await.expect("flush");
    let _ = client.close().await;

    assert_eq!(recorder.count(), 1, "queued items ship in one batch");
    let batch = recorder.requests()[0].json();
    let list = batch.as_array().expect("events/track body is an array");
    assert_eq!(list.len(), 3);
    assert_eq!(list[0]["event_id"], "evt_q_0");
}

#[tokio::test]
async fn flush_returns_within_its_deadline_against_a_hung_network() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(30)))
        .mount(&server)
        .await;
    let client = fast_client_builder(&server)
        .flush_timeout(Duration::from_millis(300))
        .build()
        .expect("build");
    client
        .track_ai(AiEvent {
            event_id: "evt_hung".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            output: "ok".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");

    let start = std::time::Instant::now();
    let _ = client.flush().await;
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "flush took {:?} with a 300ms deadline",
        start.elapsed()
    );
}

// ======================================================================
// Retry rules
// ======================================================================

#[tokio::test]
async fn retry_after_is_honoured() {
    let server = MockServer::start().await;
    let recorder = Recorder::new();
    recorder.sequence.lock().unwrap().extend(vec![
        ResponseTemplate::new(429).insert_header("Retry-After", "1"),
        ResponseTemplate::new(204),
    ]);
    Mock::given(method("POST"))
        .and(path("/events/track"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;
    let client = fast_client_builder(&server)
        .max_attempts(3)
        .build()
        .expect("build");
    client
        .track_event(Event {
            user_id: "user-123".into(),
            event: "retry_after".into(),
            ..Default::default()
        })
        .await
        .expect("track_event");

    let start = std::time::Instant::now();
    client.flush().await.expect("flush");
    let elapsed = start.elapsed();
    let _ = client.close().await;

    assert_eq!(recorder.count(), 2);
    assert!(
        elapsed >= Duration::from_millis(900),
        "second attempt must wait for Retry-After ({elapsed:?})"
    );
}

#[tokio::test]
async fn overload_with_retry_after_pauses_later_sends() {
    let server = MockServer::start().await;
    let recorder = Recorder::new();
    recorder
        .sequence
        .lock()
        .unwrap()
        .push(ResponseTemplate::new(503).insert_header("Retry-After", "1"));
    Mock::given(method("POST"))
        .and(path("/events/track"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;
    let client = fast_client_builder(&server)
        .max_attempts(1)
        .build()
        .expect("build");

    for name in ["first", "second"] {
        client
            .track_event(Event {
                user_id: "user-123".into(),
                event: name.into(),
                ..Default::default()
            })
            .await
            .expect("track_event");
        let start = std::time::Instant::now();
        let _ = client.flush().await;
        if name == "second" {
            assert!(
                start.elapsed() >= Duration::from_millis(900),
                "the next send must wait out the pause ({:?})",
                start.elapsed()
            );
        }
    }
    let _ = client.close().await;
    assert_eq!(recorder.count(), 2);
}

#[tokio::test]
async fn permanent_rejection_of_an_event_update_is_dropped() {
    let server = MockServer::start().await;
    let recorder = Recorder::new();
    recorder
        .sequence
        .lock()
        .unwrap()
        .extend((0..5).map(|_| ResponseTemplate::new(422).set_body_string("invalid")));
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(recorder.clone())
        .mount(&server)
        .await;
    let client = fast_client_builder(&server)
        .max_attempts(3)
        .build()
        .expect("build");

    let _ = client
        .begin(BeginOptions {
            event_id: "evt_422".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;
    let _ = client.flush().await;
    let _ = client.flush().await;
    let _ = client.close().await;

    assert_eq!(recorder.count(), 1, "a 4xx is neither retried nor restored");
}

// ======================================================================
// Ingest response
// ======================================================================

#[test]
fn ingest_rejected_and_warnings_are_logged() {
    let logs = capture_warn_logs(|| async {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/events/track"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rejected": [{"index": 1, "issues": [{"path": "user_id", "message": "required"}]}],
                "warnings": [{"event_id": "evt_ok", "message": "unknown field ignored"}]
            })))
            .mount(&server)
            .await;
        let client = fast_client_builder(&server).build().expect("build");
        client
            .track_event(Event {
                event_id: "evt_ok".into(),
                user_id: "user-123".into(),
                event: "ingest_response".into(),
                ..Default::default()
            })
            .await
            .expect("track_event");
        let _ = client.close().await;
    });
    assert!(logs.contains("ingest rejected 1 item(s)"), "logs: {logs}");
    assert!(logs.contains("user_id: required"), "logs: {logs}");
    assert!(logs.contains("unknown field ignored"), "logs: {logs}");
}

// ======================================================================
// Saved span parents (C6)
// ======================================================================

#[tokio::test]
async fn track_tool_accepts_a_parent_from_saved_ids() {
    let server = MockServer::start().await;
    let traces = mount_path(&server, "POST", "/traces").await;
    let _events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let parent = Span::from_saved_ids("0102030405060708090a0b0c0d0e0f10", "1112131415161718")
        .expect("valid ids");
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_parent".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;
    interaction.track_tool(TrackToolOptions {
        name: "late_tool".into(),
        parent: Some(parent),
        ..Default::default()
    });
    client.flush().await.expect("flush");
    let _ = client.close().await;

    let spans: Vec<Value> = traces
        .requests()
        .iter()
        .flat_map(|r| spans_of(&r.json()))
        .collect();
    let span = spans
        .iter()
        .find(|s| s["name"] == "late_tool")
        .expect("tool span sent");
    assert_eq!(span["traceId"], "AQIDBAUGBwgJCgsMDQ4PEA==");
    assert_eq!(span["parentSpanId"], "ERITFBUWFxg=");
}

#[test]
fn saved_ids_must_be_well_formed() {
    assert!(Span::from_saved_ids("0102030405060708090a0b0c0d0e0f10", "1112131415161718").is_some());
    assert!(Span::from_saved_ids("not-hex", "1112131415161718").is_none());
    assert!(Span::from_saved_ids("0102", "1112131415161718").is_none());
    assert!(Span::from_saved_ids("00000000000000000000000000000000", "1112131415161718").is_none());
    assert!(Span::from_saved_ids("0102030405060708090a0b0c0d0e0f10", "0000000000000000").is_none());
}

// ======================================================================
// Detached sub-agents (C7)
// ======================================================================

fn string_attr(span: &Value, key: &str) -> Option<String> {
    crate::common::span_attr(span, key)
        .and_then(|v| v.get("stringValue"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[tokio::test]
async fn subagent_dispatch_and_child_are_linked() {
    let server = MockServer::start().await;
    let traces = mount_path(&server, "POST", "/traces").await;
    let events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let launcher = client
        .begin(BeginOptions {
            event_id: "evt_launcher".into(),
            user_id: "user-123".into(),
            input: "research topic X".into(),
            ..Default::default()
        })
        .await;
    let dispatch = launcher.subagent(SubagentOptions {
        name: "researcher".into(),
        child_event_id: "evt_child".into(),
        input: Some(json!("find sources")),
        ..Default::default()
    });
    assert_eq!(dispatch.child_event_id, "evt_child");
    assert!(dispatch.headers[HANDOFF_HEADER].contains("raindrop-child-event-id=evt_child"));
    assert_eq!(
        dispatch.headers["baggage"],
        dispatch.headers[HANDOFF_HEADER]
    );
    assert!(dispatch.headers["traceparent"].starts_with("00-"));
    launcher
        .finish(FinishOptions {
            output: "dispatched".into(),
            ..Default::default()
        })
        .await
        .expect("finish launcher");

    let child = client
        .resume_subagent(&dispatch.headers, ResumeSubagentOptions::default())
        .await;
    child
        .start_tool_span("web_search", Default::default())
        .end();
    child
        .finish(FinishOptions {
            output: "found 3 sources".into(),
            ..Default::default()
        })
        .await
        .expect("finish child");
    client.flush().await.expect("flush");
    let _ = client.close().await;

    let child_final = items(&events)
        .into_iter()
        .find(|e| e["event_id"] == "evt_child" && e["is_pending"] == false)
        .expect("child reports its own event");
    assert_eq!(
        child_final["user_id"], "user-123",
        "user comes from the carrier"
    );

    let spans: Vec<Value> = traces
        .requests()
        .iter()
        .flat_map(|r| spans_of(&r.json()))
        .collect();
    let dispatch_span = spans
        .iter()
        .find(|s| s["name"] == "dispatch_subagent")
        .expect("dispatch span");
    assert_eq!(
        string_attr(dispatch_span, "raindrop.handoff.mode").as_deref(),
        Some("detached")
    );
    assert_eq!(
        string_attr(dispatch_span, "raindrop.handoff.childEventId").as_deref(),
        Some("evt_child")
    );
    let child_span = spans
        .iter()
        .find(|s| s["name"] == "web_search")
        .expect("child span");
    assert_eq!(
        string_attr(child_span, "raindrop.handoff.parentEventId").as_deref(),
        Some("evt_launcher")
    );
    assert!(string_attr(child_span, "raindrop.handoff.parentSpanId").is_some());
    assert_ne!(
        child_span["traceId"], dispatch_span["traceId"],
        "the child keeps its own trace"
    );
}

#[tokio::test]
async fn subagent_without_a_carrier_runs_unlinked() {
    let server = MockServer::start().await;
    let traces = mount_path(&server, "POST", "/traces").await;
    let events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let headers =
        std::collections::BTreeMap::from([(HANDOFF_HEADER.to_string(), "%%%garbage".to_string())]);
    let child = client
        .resume_subagent(
            &headers,
            ResumeSubagentOptions {
                user_id: "user-fallback".into(),
                ..Default::default()
            },
        )
        .await;
    child.start_tool_span("work", Default::default()).end();
    child
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.flush().await.expect("flush");
    let _ = client.close().await;

    let sent = items(&events);
    assert!(sent
        .iter()
        .any(|e| e["user_id"] == "user-fallback" && e["is_pending"] == false));
    let spans: Vec<Value> = traces
        .requests()
        .iter()
        .flat_map(|r| spans_of(&r.json()))
        .collect();
    let span = spans.iter().find(|s| s["name"] == "work").expect("span");
    assert!(string_attr(span, "raindrop.handoff.parentEventId").is_none());
}
