mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};

use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use raindrop::{
    AiEvent, AppGitConfig, Event, LlmOptions, SpanOptions, ToolOptions, TrackToolOptions,
};

use crate::common::{fast_client_builder, mount_path, span_attr, spans_of, Captured};

const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn configured_git(sha: &str) -> AppGitConfig {
    AppGitConfig::new()
        .commit_sha(sha)
        .commit_dirty(false)
        .branch("main")
}

fn event_payloads(recorder: &crate::common::Recorder, event_id: &str) -> Vec<Value> {
    recorder
        .requests()
        .into_iter()
        .map(|request| request.json())
        .filter(|payload| payload["event_id"] == event_id)
        .collect()
}

fn span_sha(span: &Value) -> Option<&str> {
    span_attr(span, "raindrop.app.commit_sha").and_then(|value| value["stringValue"].as_str())
}

#[derive(Clone, Default)]
struct BlockingRecorder {
    bodies: Arc<Mutex<Vec<Captured>>>,
    release: Arc<(Mutex<bool>, Condvar)>,
    count: Arc<(Mutex<usize>, Condvar)>,
}

impl BlockingRecorder {
    fn requests(&self) -> Vec<Captured> {
        self.bodies.lock().unwrap().clone()
    }

    fn wait_for_count(&self, expected: usize) {
        let (lock, condvar) = &*self.count;
        let mut count = lock.lock().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while *count < expected {
            let now = std::time::Instant::now();
            assert!(now < deadline, "timed out waiting for blocked request");
            let timeout = deadline.saturating_duration_since(now);
            count = condvar.wait_timeout(count, timeout).unwrap().0;
        }
    }

    fn release(&self) {
        let (lock, condvar) = &*self.release;
        *lock.lock().unwrap() = true;
        condvar.notify_all();
    }
}

impl Respond for BlockingRecorder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        self.bodies.lock().unwrap().push(Captured {
            path: request.url.path().to_string(),
            body: request.body.clone(),
        });
        let (count_lock, count_condvar) = &*self.count;
        *count_lock.lock().unwrap() += 1;
        count_condvar.notify_all();
        let (release_lock, release_condvar) = &*self.release;
        let mut released = release_lock.lock().unwrap();
        while !*released {
            released = release_condvar.wait(released).unwrap();
        }
        ResponseTemplate::new(204)
    }
}

async fn mount_blocking_path(
    server: &MockServer,
    request_method: &str,
    request_path: &str,
) -> BlockingRecorder {
    let recorder = BlockingRecorder::default();
    Mock::given(method(request_method))
        .and(path(request_path))
        .respond_with(recorder.clone())
        .mount(server)
        .await;
    recorder
}

#[tokio::test]
async fn application_git_is_observable_on_plain_ai_and_partial_events() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build");

    client
        .track_event(Event {
            event_id: "plain".into(),
            user_id: "user".into(),
            event: "signed_up".into(),
            ..Default::default()
        })
        .await
        .expect("track plain event");
    client
        .track_ai(AiEvent {
            event_id: "ai".into(),
            user_id: "user".into(),
            input: "hello".into(),
            output: "hi".into(),
            ..Default::default()
        })
        .await
        .expect("track ai event");
    let interaction = client
        .begin(raindrop::BeginOptions {
            event_id: "partial".into(),
            user_id: "user".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;
    interaction
        .finish(raindrop::FinishOptions {
            output: "hi".into(),
            ..Default::default()
        })
        .await
        .expect("finish partial event");

    for request in recorder.requests() {
        let payload = request.json();
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], SHA_A);
        assert_eq!(payload["properties"]["raindrop.app.commit_dirty"], false);
        assert_eq!(payload["properties"]["raindrop.app.branch"], "main");
    }
    assert_eq!(recorder.count(), 3);
    client.close().await.expect("close");
}

#[tokio::test]
async fn canonical_event_properties_win_even_when_empty_null_or_invalid() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build");
    let properties = BTreeMap::from([
        ("raindrop.app.commit_sha".into(), Value::Null),
        ("raindrop.app.commit_dirty".into(), json!("unknown")),
        ("raindrop.app.branch".into(), json!("")),
    ]);
    client
        .track_ai(AiEvent {
            user_id: "user".into(),
            input: "hello".into(),
            output: "hi".into(),
            properties,
            ..Default::default()
        })
        .await
        .expect("track ai event");

    let payload = recorder.requests()[0].json();
    assert!(payload["properties"]["raindrop.app.commit_sha"].is_null());
    assert_eq!(
        payload["properties"]["raindrop.app.commit_dirty"],
        "unknown"
    );
    assert_eq!(payload["properties"]["raindrop.app.branch"], "");
    client.close().await.expect("close");
}

#[tokio::test]
async fn application_git_is_on_every_public_otlp_span_path_and_properties_override() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build");

    let span = client.start_span(SpanOptions {
        name: "generic".into(),
        operation_id: "ai.workflow".into(),
        properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(SHA_B))]),
        ..Default::default()
    });
    span.end();
    let late_override = client.start_span(SpanOptions {
        name: "late-override".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    late_override.set_attributes([raindrop::Attribute::string(
        "raindrop.app.commit_sha",
        SHA_B,
    )]);
    late_override.end();
    client
        .start_llm_span("llm", LlmOptions::default(), "")
        .end();
    client
        .start_tool_span("tool", ToolOptions::default(), "")
        .end();
    client.tracer(BTreeMap::new()).track_tool(TrackToolOptions {
        name: "tracked-tool".into(),
        ..Default::default()
    });
    client.flush().await.expect("flush");

    let payload = recorder.requests()[0].json();
    let spans = spans_of(&payload);
    assert_eq!(spans.len(), 5);
    for span in &spans {
        let overridden = span["name"] == "generic" || span["name"] == "late-override";
        let expected_sha = if overridden { SHA_B } else { SHA_A };
        assert_eq!(
            span_attr(span, "raindrop.app.commit_sha")
                .and_then(|value| value["stringValue"].as_str()),
            Some(expected_sha)
        );
        assert_eq!(
            span_attr(span, "raindrop.app.commit_dirty")
                .and_then(|value| value["boolValue"].as_bool()),
            Some(false)
        );
        assert_eq!(
            span_attr(span, "raindrop.app.branch").and_then(|value| value["stringValue"].as_str()),
            Some("main")
        );
    }
    client.close().await.expect("close");
}

#[tokio::test]
async fn app_git_is_isolated_per_client_and_can_be_disabled() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client_a = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build a");
    let client_b = fast_client_builder(&server)
        .app_git(configured_git(SHA_B))
        .build()
        .expect("build b");
    let client_disabled = fast_client_builder(&server)
        .disable_app_git()
        .build()
        .expect("build disabled");

    for (client, event_id) in [
        (&client_a, "a"),
        (&client_b, "b"),
        (&client_disabled, "disabled"),
    ] {
        client
            .track_event(Event {
                event_id: event_id.into(),
                user_id: "user".into(),
                event: "test".into(),
                ..Default::default()
            })
            .await
            .expect("track event");
    }
    let requests = recorder.requests();
    assert_eq!(
        requests[0].json()["properties"]["raindrop.app.commit_sha"],
        SHA_A
    );
    assert_eq!(
        requests[1].json()["properties"]["raindrop.app.commit_sha"],
        SHA_B
    );
    assert!(requests[2].json()["properties"]
        .get("raindrop.app.commit_sha")
        .is_none());
    client_a.close().await.expect("close a");
    client_b.close().await.expect("close b");
    client_disabled.close().await.expect("close disabled");
}

#[tokio::test]
async fn terminal_flush_keeps_newer_same_id_app_git_context() {
    let server = MockServer::start().await;
    let event_recorder = mount_blocking_path(&server, "POST", "/events/track_partial").await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build");

    let interaction = client
        .begin(raindrop::BeginOptions {
            event_id: "terminal-race".into(),
            user_id: "user".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;
    let finishing = {
        let interaction = interaction.clone();
        tokio::spawn(async move {
            interaction
                .finish(raindrop::FinishOptions {
                    output: "old terminal".into(),
                    ..Default::default()
                })
                .await
        })
    };
    {
        let event_recorder = event_recorder.clone();
        tokio::task::spawn_blocking(move || event_recorder.wait_for_count(1))
            .await
            .expect("wait for blocked request");
    }

    client
        .patch(
            "terminal-race",
            raindrop::PatchOptions {
                model: "newer-pending".into(),
                properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(SHA_B))]),
                is_pending: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("newer patch while old terminal is in flight");
    event_recorder.release();
    finishing.await.expect("finish task").expect("old finish");

    let span = client.start_span(SpanOptions {
        name: "after-race-span".into(),
        event_id: "terminal-race".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush newer pending");
    client
        .finish(
            "terminal-race",
            raindrop::FinishOptions {
                output: "newer terminal".into(),
                ..Default::default()
            },
        )
        .await
        .expect("newer finish");
    client.flush().await.expect("flush traces");

    let payloads: Vec<Value> = event_recorder
        .requests()
        .into_iter()
        .map(|request| request.json())
        .collect();
    assert!(
        payloads.len() >= 2,
        "expected old terminal and newer same-id payloads"
    );
    assert_eq!(
        payloads[0]["properties"]["raindrop.app.commit_sha"], SHA_A,
        "older in-flight terminal keeps its captured identity"
    );
    for payload in &payloads[1..] {
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], SHA_B);
    }

    let trace = trace_recorder.requests()[0].json();
    let span = spans_of(&trace)
        .into_iter()
        .find(|span| span["name"] == "after-race-span")
        .expect("matching span");
    assert_eq!(span_sha(&span), Some(SHA_B));
    client.close().await.expect("close");
}

#[tokio::test]
async fn operation_raw_sha_override_persists_across_flush_resume_and_spans() {
    let server = MockServer::start().await;
    let event_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build");

    let interaction = client
        .begin(raindrop::BeginOptions {
            event_id: "sticky-b".into(),
            user_id: "user".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;
    let concurrent = client
        .begin(raindrop::BeginOptions {
            event_id: "concurrent-a".into(),
            user_id: "user".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;
    client.flush().await.expect("flush initial A");
    interaction
        .patch(raindrop::PatchOptions {
            properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(SHA_B))]),
            ..Default::default()
        })
        .await
        .expect("patch B");
    client.flush().await.expect("flush B override");

    interaction
        .patch(raindrop::PatchOptions {
            model: "model-after-b".into(),
            ..Default::default()
        })
        .await
        .expect("ordinary patch");
    client.flush().await.expect("flush ordinary patch");
    let resumed = client.resume_interaction("sticky-b");
    for span in [
        interaction.start_span(SpanOptions {
            name: "interaction-child".into(),
            operation_id: "ai.workflow".into(),
            ..Default::default()
        }),
        resumed.start_span(SpanOptions {
            name: "resumed-child".into(),
            operation_id: "ai.workflow".into(),
            ..Default::default()
        }),
        client.start_span(SpanOptions {
            name: "standalone-matching".into(),
            event_id: "sticky-b".into(),
            operation_id: "ai.workflow".into(),
            ..Default::default()
        }),
    ] {
        span.end();
    }
    interaction
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    concurrent
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish concurrent");
    client.flush().await.expect("flush traces");

    let payloads = event_payloads(&event_recorder, "sticky-b");
    assert!(payloads.len() >= 4, "expected flushed lifecycle payloads");
    assert_eq!(
        payloads[0]["properties"]["raindrop.app.commit_sha"], SHA_A,
        "initial payload uses client default"
    );
    for payload in &payloads[1..] {
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], SHA_B);
    }
    for payload in event_payloads(&event_recorder, "concurrent-a") {
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], SHA_A);
    }

    let trace = trace_recorder.requests()[0].json();
    for span in spans_of(&trace) {
        assert_eq!(span_sha(&span), Some(SHA_B));
    }
    client.close().await.expect("close");
}

#[tokio::test]
async fn operation_null_and_empty_sha_ownership_persists() {
    let server = MockServer::start().await;
    let event_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server)
        .app_git(configured_git(SHA_A))
        .build()
        .expect("build");

    let null_interaction = client
        .begin(raindrop::BeginOptions {
            event_id: "null-sha".into(),
            user_id: "user".into(),
            input: "hello".into(),
            properties: BTreeMap::from([("raindrop.app.commit_sha".into(), Value::Null)]),
            ..Default::default()
        })
        .await;
    client.flush().await.expect("flush null begin");
    null_interaction
        .patch(raindrop::PatchOptions {
            model: "still-null".into(),
            ..Default::default()
        })
        .await
        .expect("ordinary null patch");
    client.flush().await.expect("flush null ordinary");
    let null_span = client.start_span(SpanOptions {
        name: "null-span".into(),
        event_id: "null-sha".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    null_span.end();
    null_interaction
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish null");

    let empty_interaction = client
        .begin(raindrop::BeginOptions {
            event_id: "empty-sha".into(),
            user_id: "user".into(),
            input: "hello".into(),
            properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(""))]),
            ..Default::default()
        })
        .await;
    client.flush().await.expect("flush empty begin");
    empty_interaction
        .patch(raindrop::PatchOptions {
            model: "still-empty".into(),
            ..Default::default()
        })
        .await
        .expect("ordinary empty patch");
    client.flush().await.expect("flush empty ordinary");
    let empty_span = client.start_span(SpanOptions {
        name: "empty-span".into(),
        event_id: "empty-sha".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    empty_span.end();
    empty_interaction
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish empty");
    client.flush().await.expect("flush traces");

    for payload in event_payloads(&event_recorder, "null-sha") {
        assert!(payload["properties"]["raindrop.app.commit_sha"].is_null());
    }
    for payload in event_payloads(&event_recorder, "empty-sha") {
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], "");
    }
    let trace = trace_recorder.requests()[0].json();
    for span in spans_of(&trace) {
        match span["name"].as_str() {
            Some("null-span") => assert!(span_attr(&span, "raindrop.app.commit_sha").is_none()),
            Some("empty-span") => assert_eq!(span_sha(&span), Some("")),
            _ => {}
        }
    }
    client.close().await.expect("close");
}

#[tokio::test]
async fn inferred_companions_drop_on_operation_sha_override_but_explicit_branch_survives() {
    const CHILD: &str = "RAINDROP_APP_GIT_PUBLIC_COMPANION_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let mut command = std::process::Command::new(std::env::current_exe().expect("test exe"));
        command
            .args([
                "--exact",
                "inferred_companions_drop_on_operation_sha_override_but_explicit_branch_survives",
            ])
            .env(CHILD, "1")
            .env_remove("RAINDROP_COMMIT_SHA")
            .env_remove("RAINDROP_COMMIT_DIRTY")
            .env_remove("RAINDROP_BRANCH")
            .env_remove("RAINDROP_GIT_SOURCE_DIRECTORY")
            .env_remove("RAINDROP_GIT_AUTO_DETECT");
        assert!(command
            .status()
            .expect("run isolated companion test")
            .success());
        return;
    }

    std::env::set_var("VERCEL", "1");
    std::env::set_var("VERCEL_GIT_COMMIT_SHA", SHA_A);
    std::env::set_var("VERCEL_GIT_COMMIT_REF", "deploy-branch");
    let server = MockServer::start().await;
    let event_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let inferred_client = fast_client_builder(&server)
        .app_git(AppGitConfig::new().detect_branch(true))
        .build()
        .expect("build inferred client");
    let inferred = inferred_client
        .begin(raindrop::BeginOptions {
            event_id: "inferred-drop".into(),
            user_id: "user".into(),
            input: "hello".into(),
            properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(SHA_B))]),
            ..Default::default()
        })
        .await;
    inferred
        .start_span(SpanOptions {
            name: "inferred-span".into(),
            operation_id: "ai.workflow".into(),
            ..Default::default()
        })
        .end();
    inferred
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish inferred");

    let explicit_client = fast_client_builder(&server)
        .app_git(
            AppGitConfig::new()
                .commit_sha(SHA_A)
                .commit_dirty(false)
                .branch("explicit-branch"),
        )
        .build()
        .expect("build explicit client");
    let explicit = explicit_client
        .begin(raindrop::BeginOptions {
            event_id: "explicit-keep".into(),
            user_id: "user".into(),
            input: "hello".into(),
            properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(SHA_B))]),
            ..Default::default()
        })
        .await;
    explicit
        .start_span(SpanOptions {
            name: "explicit-span".into(),
            operation_id: "ai.workflow".into(),
            ..Default::default()
        })
        .end();
    explicit
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish explicit");
    inferred_client.flush().await.expect("flush inferred");
    explicit_client.flush().await.expect("flush explicit");

    let inferred_payload = event_payloads(&event_recorder, "inferred-drop")
        .last()
        .cloned()
        .expect("inferred event");
    assert_eq!(
        inferred_payload["properties"]["raindrop.app.commit_sha"],
        SHA_B
    );
    assert!(inferred_payload["properties"]
        .get("raindrop.app.branch")
        .is_none());
    let explicit_payload = event_payloads(&event_recorder, "explicit-keep")
        .last()
        .cloned()
        .expect("explicit event");
    assert_eq!(
        explicit_payload["properties"]["raindrop.app.commit_sha"],
        SHA_B
    );
    assert_eq!(
        explicit_payload["properties"]["raindrop.app.branch"],
        "explicit-branch"
    );
    for request in trace_recorder.requests() {
        for span in spans_of(&request.json()) {
            match span["name"].as_str() {
                Some("inferred-span") => {
                    assert_eq!(span_sha(&span), Some(SHA_B));
                    assert!(span_attr(&span, "raindrop.app.branch").is_none());
                }
                Some("explicit-span") => {
                    assert_eq!(span_sha(&span), Some(SHA_B));
                    assert_eq!(
                        span_attr(&span, "raindrop.app.branch")
                            .and_then(|value| value["stringValue"].as_str()),
                        Some("explicit-branch")
                    );
                }
                _ => {}
            }
        }
    }
    inferred_client.close().await.expect("close inferred");
    explicit_client.close().await.expect("close explicit");
}

#[tokio::test]
async fn begin_override_is_shared_by_event_child_and_tool_spans() {
    let server = MockServer::start().await;
    let event_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server)
        .app_git(
            AppGitConfig::new()
                .commit_sha(SHA_A)
                .commit_dirty(false)
                .branch("configured-branch"),
        )
        .build()
        .expect("build");
    let interaction = client
        .begin(raindrop::BeginOptions {
            event_id: "operation".into(),
            user_id: "user".into(),
            input: "hello".into(),
            properties: BTreeMap::from([("raindrop.app.commit_sha".into(), json!(SHA_B))]),
            ..Default::default()
        })
        .await;
    let child = interaction.start_span(SpanOptions {
        name: "child".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    child.end();
    interaction
        .start_tool_span("operation-tool", ToolOptions::default())
        .end();
    interaction
        .start_llm_span("operation-llm", LlmOptions::default())
        .end();
    interaction.track_tool(TrackToolOptions {
        name: "operation-tracked-tool".into(),
        ..Default::default()
    });
    interaction
        .finish(raindrop::FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.flush().await.expect("flush");

    let event = event_recorder
        .requests()
        .last()
        .expect("event request")
        .json();
    assert_eq!(event["properties"]["raindrop.app.commit_sha"], SHA_B);
    assert_eq!(event["properties"]["raindrop.app.commit_dirty"], false);
    assert_eq!(
        event["properties"]["raindrop.app.branch"],
        "configured-branch"
    );
    let trace = trace_recorder.requests()[0].json();
    for span in spans_of(&trace) {
        assert_eq!(
            span_attr(&span, "raindrop.app.commit_sha")
                .and_then(|value| value["stringValue"].as_str()),
            Some(SHA_B)
        );
        assert_eq!(
            span_attr(&span, "raindrop.app.branch").and_then(|value| value["stringValue"].as_str()),
            Some("configured-branch")
        );
    }
    client.close().await.expect("close");
}

#[test]
fn background_discovery_does_not_require_a_tokio_runtime_or_block_build() {
    let started = std::time::Instant::now();
    let client = raindrop::Client::builder()
        .disable_local_workshop()
        .app_git(
            AppGitConfig::new()
                .source_directory("/path/that/does/not/exist")
                .auto_detect(true),
        )
        .build()
        .expect("build disabled client outside a runtime");
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
    drop(client);
}
