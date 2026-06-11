//! Tests for the telemetry-hardening audit (Jun 2026).
//!
//! Class A — payload caps BEFORE serialization: AI input/output, tool span
//! I/O, and LLM span content are truncated (marker within the limit) before
//! they are buffered or serialized, so oversized payloads cost the cap — not
//! the payload — and land truncated instead of being silently dropped at the
//! 1 MiB ingest limit.
//!
//! Class B — bounded outbound HTTP and shutdown: every cloud POST carries a
//! per-request timeout (even for caller-injected reqwest clients), `flush()`
//! never blocks on the periodic tickers, the event buffer is bounded, and
//! `close()` runs under a hard deadline so a dead network can never wedge
//! process exit.
//!
//! Class D — log hygiene: per-event failure warnings (oversized drops,
//! empty-event drops) are rate-limited to one line per family per interval.

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use wiremock::MockServer;

use raindrop::{AiEvent, BeginOptions, FinishOptions, LlmMessage, LlmOptions, ToolOptions};

use crate::common::{fast_client_builder, mount_path, span_attr, spans_of};

const MARKER: &str = "...[truncated by raindrop]";

fn chars(s: &str) -> usize {
    s.chars().count()
}

fn attr_str(span: &Value, key: &str) -> String {
    span_attr(span, key)
        .and_then(|v| v.get("stringValue"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

// ======================================================================
// Class A: event text fields capped before buffering
// ======================================================================

#[tokio::test]
async fn track_ai_caps_huge_input_and_output_with_marker() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(1_000)
        .build()
        .expect("build");

    client
        .track_ai(AiEvent {
            event_id: "evt_big".into(),
            user_id: "user-123".into(),
            input: "All work and no play makes Jack a dull boy. ".repeat(20_000), // ~880 KB
            output: "o".repeat(500_000),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    let _ = client.close().await;

    assert_eq!(recorder.count(), 1, "capped event must still ship");
    let payload = recorder.requests()[0].json();
    let input = payload["ai_data"]["input"].as_str().unwrap();
    let output = payload["ai_data"]["output"].as_str().unwrap();
    assert_eq!(chars(input), 1_000, "marker must fit WITHIN the cap");
    assert!(input.ends_with(MARKER));
    assert!(input.starts_with("All work and no play"));
    assert_eq!(chars(output), 1_000);
    assert!(output.ends_with(MARKER));
}

#[tokio::test]
async fn begin_finish_caps_output_before_buffering() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(500)
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_finish_cap".into(),
            user_id: "user-123".into(),
            input: "i".repeat(100_000),
            ..Default::default()
        })
        .await;
    interaction
        .finish(FinishOptions {
            output: "z".repeat(100_000),
            ..Default::default()
        })
        .await
        .expect("finish");
    let _ = client.close().await;

    let payload = recorder.requests().last().cloned().unwrap().json();
    let input = payload["ai_data"]["input"].as_str().unwrap();
    let output = payload["ai_data"]["output"].as_str().unwrap();
    assert_eq!(chars(input), 500);
    assert!(input.ends_with(MARKER));
    assert_eq!(chars(output), 500);
    assert!(output.ends_with(MARKER));
}

#[tokio::test]
async fn cap_smaller_than_marker_hard_slices_without_marker() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(10)
        .build()
        .expect("build");

    client
        .track_ai(AiEvent {
            event_id: "evt_tiny_cap".into(),
            user_id: "user-123".into(),
            input: "x".repeat(100),
            output: "y".repeat(100),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    let _ = client.close().await;

    let payload = recorder.requests()[0].json();
    assert_eq!(payload["ai_data"]["input"], "x".repeat(10));
    assert_eq!(payload["ai_data"]["output"], "y".repeat(10));
}

#[tokio::test]
async fn small_payloads_round_trip_unchanged() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_ai(AiEvent {
            event_id: "evt_small".into(),
            user_id: "user-123".into(),
            input: "What is 2+2?".into(),
            output: "The answer is 4.".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    let _ = client.close().await;

    let payload = recorder.requests()[0].json();
    assert_eq!(payload["ai_data"]["input"], "What is 2+2?");
    assert_eq!(payload["ai_data"]["output"], "The answer is 4.");
}

#[tokio::test]
async fn multibyte_payloads_cap_on_char_boundaries() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(100)
        .build()
        .expect("build");

    client
        .track_ai(AiEvent {
            event_id: "evt_utf8".into(),
            user_id: "user-123".into(),
            input: "\u{1F982}".repeat(50_000), // 4-byte chars
            output: "ok".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    let _ = client.close().await;

    let payload = recorder.requests()[0].json();
    let input = payload["ai_data"]["input"].as_str().unwrap();
    assert_eq!(chars(input), 100);
    assert!(input.ends_with(MARKER));
}

// ======================================================================
// Class A: tool span I/O bounded during serialization
// ======================================================================

#[tokio::test]
async fn track_tool_bounds_huge_structured_output() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let event_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(1_000)
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_big_tool".into(),
            user_id: "user-123".into(),
            input: "tool probe".into(),
            ..Default::default()
        })
        .await;

    let rows: Vec<String> = (0..5_000)
        .map(|_| format!("order-timeline-entry {}", "z".repeat(200)))
        .collect(); // ~1.1 MB structured payload
    interaction.track_tool(raindrop::TrackToolOptions {
        name: "fetch_order_timeline".into(),
        input: Some(json!({"q": "probe-q"})),
        output: Some(json!({ "rows": rows })),
        duration: Some(Duration::from_millis(42)),
        ..Default::default()
    });

    interaction
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.flush().await.expect("flush");
    let _ = client.close().await;

    assert!(event_recorder.count() >= 1, "event must land");
    let mut tool_spans = Vec::new();
    for req in trace_recorder.requests() {
        for span in spans_of(&req.json()) {
            if span["name"] == "fetch_order_timeline" {
                tool_spans.push(span);
            }
        }
    }
    assert_eq!(tool_spans.len(), 1, "tool span must land");
    let output = attr_str(&tool_spans[0], "traceloop.entity.output");
    assert!(
        chars(&output) <= 1_000,
        "not bounded: {} chars",
        chars(&output)
    );
    assert!(output.ends_with(MARKER));
    let input = attr_str(&tool_spans[0], "traceloop.entity.input");
    assert!(input.contains("probe-q"), "small input must be intact");
}

#[tokio::test]
async fn tool_span_setters_bound_huge_values() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(200)
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_tool_span".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;
    let tool_span = interaction.start_tool_span(
        "lookup",
        ToolOptions {
            input: Some(json!("q".repeat(50_000))),
            ..Default::default()
        },
    );
    tool_span.set_output(&json!({"rows": ["r".repeat(50_000)]}));
    tool_span.end();
    client.flush().await.expect("flush");
    let _ = client.close().await;

    let mut spans = Vec::new();
    for req in trace_recorder.requests() {
        spans.extend(spans_of(&req.json()));
    }
    let span = spans.iter().find(|s| s["name"] == "lookup").expect("span");
    let input = attr_str(span, "traceloop.entity.input");
    let output = attr_str(span, "traceloop.entity.output");
    assert_eq!(chars(&input), 200, "raw string input capped");
    assert!(input.ends_with(MARKER));
    assert!(chars(&output) <= 200, "structured output bounded");
    assert!(output.ends_with(MARKER));
}

#[tokio::test]
async fn with_tool_bounds_result_and_keeps_string_parity() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(300)
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_with_tool".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;

    // Small string result: raw (unquoted) parity with the previous behavior.
    let res: Result<String, std::convert::Infallible> =
        raindrop::with_tool(&interaction, "small_tool", ToolOptions::default(), || {
            Ok("plain result".to_string())
        });
    assert_eq!(res.unwrap(), "plain result");

    // Huge result: bounded with marker.
    let res: Result<String, std::convert::Infallible> =
        raindrop::with_tool(&interaction, "big_tool", ToolOptions::default(), || {
            Ok("w".repeat(1_000_000))
        });
    assert_eq!(res.unwrap().len(), 1_000_000, "caller gets the full result");

    client.flush().await.expect("flush");
    let _ = client.close().await;

    let mut spans = Vec::new();
    for req in trace_recorder.requests() {
        spans.extend(spans_of(&req.json()));
    }
    let small = spans.iter().find(|s| s["name"] == "small_tool").unwrap();
    assert_eq!(attr_str(small, "traceloop.entity.output"), "plain result");
    let big = spans.iter().find(|s| s["name"] == "big_tool").unwrap();
    let big_out = attr_str(big, "traceloop.entity.output");
    assert!(chars(&big_out) <= 300);
    assert!(big_out.ends_with(MARKER));
}

// ======================================================================
// Class A: LLM span content bounded
// ======================================================================

#[tokio::test]
async fn llm_span_content_is_bounded() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(250)
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_llm".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;

    let llm = interaction.start_llm_span(
        "generate",
        LlmOptions {
            model: "gpt-4o".into(),
            input: Some("p".repeat(100_000)),
            output: Some("c".repeat(100_000)),
            ..Default::default()
        },
    );
    llm.end();

    let llm2 = interaction.start_llm_span("generate_messages", LlmOptions::default());
    llm2.set_messages([
        LlmMessage::system("be brief"),
        LlmMessage::user("m".repeat(100_000)),
    ]);
    llm2.set_output("a".repeat(100_000));
    llm2.end();

    client.flush().await.expect("flush");
    let _ = client.close().await;

    let mut spans = Vec::new();
    for req in trace_recorder.requests() {
        spans.extend(spans_of(&req.json()));
    }

    let gen = spans.iter().find(|s| s["name"] == "generate").unwrap();
    for key in ["ai.prompt", "gen_ai.prompt.0.content"] {
        let v = attr_str(gen, key);
        assert_eq!(chars(&v), 250, "{key} not capped");
        assert!(v.ends_with(MARKER), "{key} missing marker");
    }
    let completion = attr_str(gen, "gen_ai.completion.0.content");
    assert_eq!(chars(&completion), 250);
    assert!(completion.ends_with(MARKER));

    let msgs = spans
        .iter()
        .find(|s| s["name"] == "generate_messages")
        .unwrap();
    let content = attr_str(msgs, "gen_ai.prompt.1.content");
    assert_eq!(chars(&content), 250);
    assert!(content.ends_with(MARKER));
    let blob = attr_str(msgs, "ai.prompt.messages");
    assert!(chars(&blob) <= 250, "aggregate messages JSON bounded");
    let small = attr_str(msgs, "gen_ai.prompt.0.content");
    assert_eq!(small, "be brief", "small message content untouched");
    let out = attr_str(msgs, "gen_ai.completion.0.content");
    assert_eq!(chars(&out), 250);
}

// ======================================================================
// Class B: bounded transport, queues, and shutdown
// ======================================================================

/// A real TCP server that accepts connections and never responds — a hung
/// api.raindrop.ai. Sockets are held open (no RST) until the process exits.
fn start_hung_server() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            match stream {
                Ok(s) => held.push(s),
                Err(_) => break,
            }
        }
    });
    format!("http://{addr}/")
}

/// Pre-fix repro: the periodic ticker JoinHandles lived in the same task list
/// `flush()` drains and awaits, so any explicit `flush()` on a client with
/// default (non-zero) flush intervals blocked until `close()` — forever, in
/// practice. This test hung before the fix.
#[tokio::test]
async fn flush_with_periodic_tickers_returns_promptly() {
    let server = MockServer::start().await;
    let _recorder = mount_path(&server, "POST", "/events/track_partial").await;

    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", server.uri()))
        .disable_local_workshop()
        // Default-style intervals: periodic tickers ARE running.
        .partial_flush_interval(Duration::from_secs(1))
        .trace_flush_interval(Duration::from_secs(1))
        .build()
        .expect("build");

    let res = tokio::time::timeout(Duration::from_secs(5), client.flush()).await;
    assert!(
        res.is_ok(),
        "flush() must not block on the periodic ticker tasks"
    );
    res.unwrap().expect("flush");
    let _ = client.close().await;
}

#[tokio::test]
async fn close_is_bounded_against_hung_network() {
    let endpoint = start_hung_server();
    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(endpoint)
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .close_timeout(Duration::from_millis(500))
        .build()
        .expect("build");

    // Buffer several shippable events without sending (pending patches are
    // buffered; nothing POSTs until flush/close).
    for i in 0..3 {
        let _ = client
            .begin(BeginOptions {
                event_id: format!("evt_hung_{i}"),
                user_id: "user-123".into(),
                input: "hello".into(),
                ..Default::default()
            })
            .await;
    }

    let start = std::time::Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(10), client.close()).await;
    let elapsed = start.elapsed();
    assert!(res.is_ok(), "close() hung past its deadline");
    res.unwrap().expect("close returns Ok at deadline");
    assert!(
        elapsed < Duration::from_secs(5),
        "close() took {elapsed:?} against a hung network; expected ~500ms deadline"
    );
}

#[tokio::test]
async fn request_timeout_bounds_caller_injected_client_without_timeouts() {
    let endpoint = start_hung_server();
    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(endpoint)
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        // A reqwest client with NO timeouts configured: pre-fix, this hung
        // each attempt indefinitely.
        .http_client(reqwest::Client::new())
        .request_timeout(Duration::from_millis(200))
        .max_attempts(2)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .build()
        .expect("build");

    let start = std::time::Instant::now();
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        client.track_ai(AiEvent {
            event_id: "evt_timeout".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            output: "ok".into(),
            ..Default::default()
        }),
    )
    .await;
    let elapsed = start.elapsed();

    let send_result = res.expect("send must not hang: per-request timeout applies");
    assert!(send_result.is_err(), "hung server must surface as an error");
    assert!(
        elapsed < Duration::from_secs(5),
        "2 attempts with a 200ms request timeout took {elapsed:?}"
    );
}

#[tokio::test]
async fn event_buffer_drops_new_events_at_capacity() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .event_max_queue_size(3)
        .build()
        .expect("build");

    for i in 0..5 {
        // Must not error: overflow is dropped with a rate-limited warning.
        let _ = client
            .begin(BeginOptions {
                event_id: format!("evt_cap_{i}"),
                user_id: "user-123".into(),
                input: format!("input {i}"),
                ..Default::default()
            })
            .await;
    }
    client.flush().await.expect("flush");
    let _ = client.close().await;

    assert_eq!(
        recorder.count(),
        3,
        "only the first 3 buffered events may ship; the rest are dropped at the cap"
    );
}

// ======================================================================
// Class D: failure logs are rate-limited
// ======================================================================

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

/// Run `f` on a current-thread runtime with a scoped subscriber capturing all
/// WARN-level output emitted from this thread, and return the captured text.
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
    // Scoped (NOT global) subscriber + current-thread runtime: every SDK log
    // emitted while the future runs lands in the sink.
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

#[test]
fn oversized_payload_drop_warning_is_rate_limited() {
    // Oversized payloads are dropped in post_json BEFORE any HTTP request, so
    // no mock server is needed. Properties are deliberately uncapped (parity
    // with the python-sdk), which is exactly how a payload still exceeds the
    // 1 MiB ingest limit after text-field caps.
    let logs = capture_warn_logs(|| async {
        let client = raindrop::Client::builder()
            .write_key("rk_test")
            .disable_local_workshop()
            .partial_flush_interval(Duration::ZERO)
            .trace_flush_interval(Duration::ZERO)
            .build()
            .expect("build");
        for i in 0..5 {
            let mut props = BTreeMap::new();
            props.insert("blob".to_string(), json!("x".repeat(2 * 1024 * 1024)));
            client
                .track_ai(AiEvent {
                    event_id: format!("evt_oversized_{i}"),
                    user_id: "user-123".into(),
                    input: "hi".into(),
                    output: "ok".into(),
                    properties: props,
                    ..Default::default()
                })
                .await
                .expect("oversized payloads are dropped, not errors");
        }
    });

    assert_eq!(
        logs.matches("dropping oversized payload").count(),
        1,
        "expected exactly one rate-limited warning for 5 drops, logs: {logs}"
    );
}

#[test]
fn empty_ai_event_drop_warning_is_rate_limited() {
    // Empty finalized events are dropped at the buffer BEFORE any HTTP
    // request, so no mock server is needed.
    let logs = capture_warn_logs(|| async {
        let client = raindrop::Client::builder()
            .write_key("rk_test")
            .disable_local_workshop()
            .partial_flush_interval(Duration::ZERO)
            .trace_flush_interval(Duration::ZERO)
            .build()
            .expect("build");
        for i in 0..5 {
            let interaction = client
                .begin(BeginOptions {
                    event_id: format!("evt_empty_{i}"),
                    user_id: "user-123".into(),
                    ..Default::default()
                })
                .await;
            interaction
                .finish(FinishOptions::default())
                .await
                .expect("empty events are dropped, not errors");
        }
    });

    assert_eq!(
        logs.matches("empty ai_input and ai_output").count(),
        1,
        "expected exactly one rate-limited warning for 5 drops, logs: {logs}"
    );
}

// ======================================================================
// Class A: association property values bounded
// ======================================================================

#[tokio::test]
async fn association_property_values_are_bounded() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _events = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .max_text_field_chars(150)
        .build()
        .expect("build");

    let mut props = BTreeMap::new();
    props.insert("note".to_string(), json!("n".repeat(100_000)));
    props.insert("blob".to_string(), json!({"data": "d".repeat(100_000)}));
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_props".into(),
            user_id: "user-123".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;
    interaction.track_tool(raindrop::TrackToolOptions {
        name: "prop_tool".into(),
        properties: props,
        ..Default::default()
    });
    client.flush().await.expect("flush");
    let _ = client.close().await;

    let mut spans = Vec::new();
    for req in trace_recorder.requests() {
        spans.extend(spans_of(&req.json()));
    }
    let span = spans.iter().find(|s| s["name"] == "prop_tool").unwrap();
    let note = attr_str(span, "traceloop.association.properties.note");
    assert_eq!(chars(&note), 150);
    assert!(note.ends_with(MARKER));
    let blob = attr_str(span, "traceloop.association.properties.blob");
    assert!(chars(&blob) <= 150);
    assert!(blob.ends_with(MARKER));
}
