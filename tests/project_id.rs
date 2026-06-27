//! First-class Projects: `X-Raindrop-Project-Id` routing header coverage.
//!
//! Verifies the header is attached to every outbound request when a valid slug
//! is configured, omitted when unset, and omitted (with a warning) when the
//! configured value is invalid, without ever breaking ingestion.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use raindrop::{Client, ClientBuilder, Event, User};

/// Lower-cased header name; `http::HeaderMap` lookups are case-insensitive.
const PROJECT_ID_HEADER: &str = "x-raindrop-project-id";

/// Records the `X-Raindrop-Project-Id` header value (or its absence) for every
/// request it answers, so a test can assert coverage across all outbound calls.
#[derive(Clone, Default)]
struct HeaderCapture {
    values: Arc<Mutex<Vec<Option<String>>>>,
}

impl HeaderCapture {
    fn captured(&self) -> Vec<Option<String>> {
        self.values.lock().unwrap().clone()
    }
}

impl Respond for HeaderCapture {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let value = request
            .headers
            .get(PROJECT_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        self.values.lock().unwrap().push(value);
        ResponseTemplate::new(204)
    }
}

async fn capture_for(server: &MockServer) -> HeaderCapture {
    let capture = HeaderCapture::default();
    Mock::given(method("POST"))
        .respond_with(capture.clone())
        .mount(server)
        .await;
    capture
}

fn cloud_builder(server: &MockServer) -> ClientBuilder {
    Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", server.uri()))
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
}

async fn track_event(client: &Client) {
    client
        .track_event(Event {
            user_id: "user-123".into(),
            event: "test".into(),
            ..Default::default()
        })
        .await
        .expect("track_event");
}

#[tokio::test]
async fn header_present_on_every_request_when_valid() {
    let server = MockServer::start().await;
    let capture = capture_for(&server).await;

    let client = cloud_builder(&server)
        .project_id("my-project")
        .build()
        .expect("build");

    // Exercise two distinct endpoints (events + users) to confirm the header is
    // attached centrally rather than at a single call site.
    track_event(&client).await;
    client
        .identify(User {
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await
        .expect("identify");
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let captured = capture.captured();
    assert!(captured.len() >= 2, "expected requests on both endpoints");
    assert!(
        captured.iter().all(|v| v.as_deref() == Some("my-project")),
        "every outbound request must carry the project id header: {captured:?}"
    );
}

#[tokio::test]
async fn header_absent_when_project_id_unset() {
    let server = MockServer::start().await;
    let capture = capture_for(&server).await;

    let client = cloud_builder(&server).build().expect("build");
    track_event(&client).await;
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let captured = capture.captured();
    assert!(!captured.is_empty(), "expected at least one request");
    assert!(
        captured.iter().all(Option::is_none),
        "no request may carry the project id header when unset: {captured:?}"
    );
}

#[tokio::test]
async fn header_omitted_when_invalid_but_ingestion_continues() {
    let server = MockServer::start().await;
    let capture = capture_for(&server).await;

    // Uppercase + underscore + `!` all violate the slug grammar.
    let client = cloud_builder(&server)
        .project_id("Invalid_Project!")
        .build()
        .expect("build");
    track_event(&client).await;
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let captured = capture.captured();
    assert!(
        !captured.is_empty(),
        "an invalid project id must NOT break ingestion: the request must still be sent"
    );
    assert!(
        captured.iter().all(Option::is_none),
        "an invalid project id must never reach the wire: {captured:?}"
    );
}

#[tokio::test]
async fn header_value_is_trimmed() {
    let server = MockServer::start().await;
    let capture = capture_for(&server).await;

    let client = cloud_builder(&server)
        .project_id("  my-project  ")
        .build()
        .expect("build");
    track_event(&client).await;
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let captured = capture.captured();
    assert!(
        captured.iter().all(|v| v.as_deref() == Some("my-project")),
        "surrounding whitespace must be trimmed before sending: {captured:?}"
    );
}

#[tokio::test]
async fn header_attached_to_local_workshop_mirror_too() {
    let cloud = MockServer::start().await;
    let local = MockServer::start().await;
    let cloud_capture = capture_for(&cloud).await;
    let local_capture = capture_for(&local).await;

    let client = Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", cloud.uri()))
        .local_workshop_url(format!("{}/", local.uri()))
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
        .project_id("my-project")
        .build()
        .expect("build");

    track_event(&client).await;
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let cloud_captured = cloud_capture.captured();
    let local_captured = local_capture.captured();
    assert!(
        !cloud_captured.is_empty()
            && cloud_captured
                .iter()
                .all(|v| v.as_deref() == Some("my-project")),
        "cloud request must carry the project id header: {cloud_captured:?}"
    );
    assert!(
        !local_captured.is_empty(),
        "expected the local workshop mirror to receive the request"
    );
    assert!(
        local_captured
            .iter()
            .all(|v| v.as_deref() == Some("my-project")),
        "local workshop mirror must also carry the project id header: {local_captured:?}"
    );
}

// --- invalid-slug warning ---------------------------------------------------

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

/// Capture WARN-level logs emitted from this thread while `f` runs against a
/// scoped (not global) subscriber.
fn capture_warn_logs<F: FnOnce()>(f: F) -> String {
    let sink: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let writer_sink = sink.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || VecWriter(writer_sink.clone()))
        .finish();
    tracing::subscriber::with_default(subscriber, f);
    let captured = sink.lock().unwrap().clone();
    String::from_utf8(captured).expect("utf8 logs")
}

#[test]
fn invalid_project_id_warns_at_build() {
    let logs = capture_warn_logs(|| {
        let _client = Client::builder()
            .write_key("rk_test")
            .disable_local_workshop()
            .partial_flush_interval(Duration::ZERO)
            .trace_flush_interval(Duration::ZERO)
            .project_id("Invalid_Project!")
            .build()
            .expect("build");
    });
    assert!(
        logs.contains("invalid project_id"),
        "expected a warning about the invalid project_id, logs: {logs}"
    );
}

#[test]
fn valid_project_id_does_not_warn_at_build() {
    let logs = capture_warn_logs(|| {
        let _client = Client::builder()
            .write_key("rk_test")
            .disable_local_workshop()
            .partial_flush_interval(Duration::ZERO)
            .trace_flush_interval(Duration::ZERO)
            .project_id("my-project")
            .build()
            .expect("build");
    });
    assert!(
        !logs.contains("invalid project_id"),
        "a valid project_id must not warn, logs: {logs}"
    );
}
