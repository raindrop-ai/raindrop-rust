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
