//! Integration tests for the local Workshop mirror.
//!
//! Spins up TWO mock servers (the cloud backend + the Workshop daemon) and
//! verifies that every track/trace post the SDK makes to the cloud is also
//! mirrored to the Workshop URL fire-and-forget. Mirror failures must NEVER
//! affect the cloud path.

mod common;

use std::time::Duration;

use raindrop::{AiEvent, BeginOptions, SpanOptions};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::common::{mount_path, spans_of, Recorder};

fn build_client(cloud: &MockServer, workshop_url: String) -> raindrop::Client {
    raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", cloud.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .workshop_url(workshop_url)
        .build()
        .expect("build")
}

#[tokio::test]
async fn track_partial_is_mirrored_to_workshop() {
    let cloud = MockServer::start().await;
    let workshop = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/events/track_partial").await;
    let workshop_recorder = mount_path(&workshop, "POST", "/events/track_partial").await;

    let client = build_client(&cloud, format!("{}/", workshop.uri()));
    client
        .track_ai(AiEvent {
            event_id: "evt_mirror".into(),
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    // Give the spawned mirror task a moment to land.
    wait_for(&workshop_recorder, 1, Duration::from_secs(2)).await;

    assert_eq!(cloud_recorder.count(), 1, "cloud got the request");
    assert_eq!(
        workshop_recorder.count(),
        1,
        "workshop got the mirrored request"
    );
    let cloud_body = cloud_recorder.requests()[0].json();
    let workshop_body = workshop_recorder.requests()[0].json();
    assert_eq!(
        cloud_body["event_id"], workshop_body["event_id"],
        "cloud and workshop bodies must match"
    );
    assert_eq!(workshop_body["event_id"], "evt_mirror");
}

#[tokio::test]
async fn otlp_traces_are_mirrored_to_workshop() {
    let cloud = MockServer::start().await;
    let workshop = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/traces").await;
    let workshop_recorder = mount_path(&workshop, "POST", "/traces").await;

    let client = build_client(&cloud, format!("{}/", workshop.uri()));
    let span = client.start_span(SpanOptions {
        name: "mirrored_span".into(),
        event_id: "evt_otlp_mirror".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    wait_for(&workshop_recorder, 1, Duration::from_secs(2)).await;

    assert_eq!(cloud_recorder.count(), 1);
    assert_eq!(workshop_recorder.count(), 1);
    let cloud_spans = spans_of(&cloud_recorder.requests()[0].json());
    let workshop_spans = spans_of(&workshop_recorder.requests()[0].json());
    assert_eq!(cloud_spans.len(), 1);
    assert_eq!(workshop_spans.len(), 1);
    assert_eq!(cloud_spans[0]["spanId"], workshop_spans[0]["spanId"]);
}

#[tokio::test]
async fn workshop_mirror_failure_does_not_affect_cloud_path() {
    let cloud = MockServer::start().await;
    let workshop = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/events/track_partial").await;
    // Workshop returns 500 every time — cloud must still succeed.
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&workshop)
        .await;

    let client = build_client(&cloud, format!("{}/", workshop.uri()));
    client
        .track_ai(AiEvent {
            event_id: "evt_workshop_5xx".into(),
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai must succeed even when workshop is broken");
    client.close().await.expect("close");

    assert_eq!(
        cloud_recorder.count(),
        1,
        "cloud path must succeed independently of workshop"
    );
}

#[tokio::test]
async fn workshop_mirror_includes_wire_version_header() {
    let cloud = MockServer::start().await;
    let workshop = MockServer::start().await;
    let _cloud_recorder = mount_path(&cloud, "POST", "/events/track_partial").await;
    let workshop_recorder = HeaderRecorder::default();
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(workshop_recorder.clone())
        .mount(&workshop)
        .await;

    let client = build_client(&cloud, format!("{}/", workshop.uri()));
    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    workshop_recorder.wait_for(1, Duration::from_secs(2)).await;
    let header = workshop_recorder.last_wire_version();
    assert_eq!(
        header.as_deref(),
        Some("1"),
        "Workshop mirror must carry the X-Raindrop-Contract-Version header"
    );
}

#[tokio::test]
async fn workshop_mirror_disabled_when_no_workshop_url_resolved() {
    let cloud = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/events/track_partial").await;

    // No workshop_url, no env vars (we explicitly pass enable_workshop:false to
    // sidestep auto-detection picking up an interactive TTY in the dev shell).
    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", cloud.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .enable_workshop(false)
        .build()
        .expect("build");

    assert_eq!(
        client.workshop_url(),
        None,
        "enable_workshop:false MUST hard-disable mirror"
    );

    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    assert_eq!(cloud_recorder.count(), 1);
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

async fn wait_for(recorder: &Recorder, n: usize, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while recorder.count() < n && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Default, Clone)]
struct HeaderRecorder {
    headers: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

impl HeaderRecorder {
    fn last_wire_version(&self) -> Option<String> {
        self.headers.lock().unwrap().last().cloned().flatten()
    }
    fn count(&self) -> usize {
        self.headers.lock().unwrap().len()
    }
    async fn wait_for(&self, n: usize, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while self.count() < n && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl wiremock::Respond for HeaderRecorder {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let v = request
            .headers
            .get("X-Raindrop-Contract-Version")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        self.headers.lock().unwrap().push(v);
        ResponseTemplate::new(204)
    }
}

/// Records every header on every request, used by the bearer-leak parity test.
#[derive(Default, Clone)]
struct AllHeadersRecorder {
    requests: std::sync::Arc<std::sync::Mutex<Vec<std::collections::HashMap<String, String>>>>,
}

impl AllHeadersRecorder {
    fn requests(&self) -> Vec<std::collections::HashMap<String, String>> {
        self.requests.lock().unwrap().clone()
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
    async fn wait_for(&self, n: usize, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while self.count() < n && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl wiremock::Respond for AllHeadersRecorder {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let mut map = std::collections::HashMap::new();
        for (name, value) in request.headers.iter() {
            if let Ok(v) = value.to_str() {
                map.insert(name.as_str().to_ascii_lowercase(), v.to_string());
            }
        }
        self.requests.lock().unwrap().push(map);
        ResponseTemplate::new(204)
    }
}

/// Parity with python-sdk `test_mirror_does_not_forward_cloud_bearer_token`:
/// the local Workshop daemon mirror MUST NOT carry the cloud `write_key`.
/// Mirror URLs are env- / option-driven and sit outside the cloud trust
/// boundary; forwarding the bearer to a mirror host the user (or an attacker)
/// controls is a credential-exfiltration vector.
#[tokio::test]
async fn workshop_mirror_does_not_forward_cloud_bearer_token() {
    let cloud = MockServer::start().await;
    let workshop = MockServer::start().await;

    let cloud_headers = AllHeadersRecorder::default();
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(cloud_headers.clone())
        .mount(&cloud)
        .await;

    let workshop_headers = AllHeadersRecorder::default();
    Mock::given(method("POST"))
        .and(path("/events/track_partial"))
        .respond_with(workshop_headers.clone())
        .mount(&workshop)
        .await;

    let client = build_client(&cloud, format!("{}/", workshop.uri()));
    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    workshop_headers.wait_for(1, Duration::from_secs(2)).await;
    assert_eq!(cloud_headers.count(), 1, "cloud got the request");
    assert_eq!(
        workshop_headers.count(),
        1,
        "workshop got the mirrored request"
    );

    let cloud_req = &cloud_headers.requests()[0];
    let workshop_req = &workshop_headers.requests()[0];

    assert_eq!(
        cloud_req.get("authorization").map(String::as_str),
        Some("Bearer rk_test"),
        "cloud path MUST carry the bearer write_key"
    );
    assert!(
        !workshop_req.contains_key("authorization"),
        "Workshop mirror MUST NOT carry the cloud bearer; got headers={workshop_req:?}",
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Userinfo log-sanitization parity test
// (python-sdk: `test_mirror_failure_log_strips_url_userinfo`)
// ────────────────────────────────────────────────────────────────────────────

/// Append-only log capture installed once as the global tracing default for
/// this test binary. Every test in this file shares the buffer; tests assert
/// against unique markers (e.g. a per-test secret string) to avoid coupling.
#[derive(Clone, Default)]
struct LogCapture {
    buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl LogCapture {
    fn snapshot(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock().unwrap()).to_string()
    }
}

struct LogWriter {
    buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl std::io::Write for LogWriter {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter {
            buf: self.buf.clone(),
        }
    }
}

static GLOBAL_LOG_CAPTURE: std::sync::OnceLock<LogCapture> = std::sync::OnceLock::new();

fn ensure_log_capture() -> LogCapture {
    GLOBAL_LOG_CAPTURE
        .get_or_init(|| {
            let capture = LogCapture::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                .with_max_level(tracing::Level::DEBUG)
                .with_ansi(false)
                .with_target(false)
                .finish();
            // First-call wins; if some other test in this binary already
            // installed a global subscriber the call returns Err and we still
            // hand back the capture (the tests gracefully no-op on empty).
            let _ = tracing::subscriber::set_global_default(subscriber);
            capture
        })
        .clone()
}

#[tokio::test]
async fn workshop_mirror_failure_log_strips_url_userinfo() {
    let capture = ensure_log_capture();

    // Snapshot the buffer length so we only assert on lines emitted from this
    // test's fire-and-forget mirror task.
    let baseline = capture.snapshot().len();

    let cloud = MockServer::start().await;
    let _cloud_recorder = mount_path(&cloud, "POST", "/events/track_partial").await;

    // Use a port we know nothing is listening on so the mirror request fails
    // and the failure-log path runs. The credentials we never want to see in
    // logs are `mirror_user:mirror_secret_value_42`.
    let workshop_url = "http://mirror_user:mirror_secret_value_42@127.0.0.1:1/v1/".to_string();

    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", cloud.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .debug(true)
        .workshop_url(workshop_url)
        .build()
        .expect("build");

    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai must succeed even when workshop is unreachable");
    client.close().await.expect("close");

    // Give the spawned mirror task time to fail and emit its debug log.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let snap = capture.snapshot();
        let new_lines = &snap[baseline.min(snap.len())..];
        if new_lines.contains("workshop mirror") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let snap = capture.snapshot();
    let new_lines = &snap[baseline.min(snap.len())..];

    assert!(
        new_lines.contains("workshop mirror"),
        "expected a `workshop mirror` debug log line; captured (post-baseline) was: {new_lines:?}",
    );
    assert!(
        !new_lines.contains("mirror_secret_value_42"),
        "userinfo password leaked into debug logs: {new_lines:?}",
    );
    assert!(
        !new_lines.contains("mirror_user:mirror_secret_value_42"),
        "full userinfo leaked into debug logs: {new_lines:?}",
    );
    assert!(
        !new_lines.contains("mirror_user@"),
        "userinfo username leaked into debug logs: {new_lines:?}",
    );
}

#[tokio::test]
async fn workshop_mirror_carries_workspace_property_when_set() {
    let cloud = MockServer::start().await;
    let workshop = MockServer::start().await;
    let cloud_recorder = mount_path(&cloud, "POST", "/events/track_partial").await;
    let workshop_recorder = mount_path(&workshop, "POST", "/events/track_partial").await;

    let client = raindrop::Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", cloud.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .workshop_url(format!("{}/", workshop.uri()))
        .workspace(raindrop::contract::v1::workspace::LocalWorkspaceMetadata {
            id: "ws_test".into(),
            name: "Test Workspace".into(),
            root: "/Users/me/code/test".into(),
        })
        .build()
        .expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_ws".into(),
            user_id: "u".into(),
            input: "x".into(),
            ..Default::default()
        })
        .await;
    interaction
        .finish(raindrop::FinishOptions {
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    wait_for(&workshop_recorder, 1, Duration::from_secs(2)).await;
    assert!(cloud_recorder.count() >= 1);
    assert!(workshop_recorder.count() >= 1);

    let body = cloud_recorder
        .requests()
        .last()
        .expect("at least one cloud request")
        .json();
    let ws = &body["properties"]["workspace"];
    assert_eq!(ws["id"], "ws_test");
    assert_eq!(ws["name"], "Test Workspace");
    assert_eq!(ws["root"], "/Users/me/code/test");
}
