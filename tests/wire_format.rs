//! Pedantic wire-format tests.
//!
//! These tests assert the exact bytes the SDK sends to `/v1/events/track_partial`,
//! `/v1/signals/track`, `/v1/users/identify`, and `/v1/traces`. They are intentionally
//! over-specified — every field name, every casing, every shape — so a regression that
//! changes the wire contract is caught immediately, even if the dashboard happens to
//! accept the malformed payload.
//!
//! Source of truth: `@raindrop-ai/schemas/ingest::*Schema` in the JS SDK and
//! `raindrop/models.py` in the Python SDK. Each test cross-references the canonical
//! schema field name.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};
use time::macros::datetime;
use wiremock::MockServer;

use raindrop::{
    AiEvent, Attachment, Attribute, BeginOptions, Event, FinishOptions, PatchOptions, Signal,
    SignalKind, SpanOptions, ToolOptions, TrackToolOptions, User,
};

use crate::common::{fast_client_builder, mount_any_post, mount_path, span_attr, spans_of};

// ────────────────────────────────────────────────────────────────────────────────────
// /events/track_partial wire format (AiTrackEventSchema)
// ────────────────────────────────────────────────────────────────────────────────────

/// Canonical track_ai payload shape: snake_case keys, ai_data sub-object, is_pending=false,
/// $context auto-injected with library + metadata. See `AiTrackEventSchema`.
#[tokio::test]
async fn track_ai_payload_uses_canonical_snake_case_shape() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_ai(AiEvent {
            event_id: "evt_pedantic".into(),
            user_id: "user_pedantic".into(),
            event: "ai_generation".into(),
            input: "the input".into(),
            output: "the output".into(),
            model: "gpt-4o".into(),
            convo_id: "conv_pedantic".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    // Top-level keys must be snake_case
    assert!(payload.get("event_id").is_some(), "event_id (snake_case)");
    assert!(payload.get("user_id").is_some(), "user_id (snake_case)");
    assert!(payload.get("ai_data").is_some(), "ai_data (snake_case)");
    assert!(
        payload.get("is_pending").is_some(),
        "is_pending (snake_case)"
    );
    assert_eq!(payload.get("eventId"), None, "MUST NOT use camelCase");
    assert_eq!(payload.get("aiData"), None, "MUST NOT use camelCase");

    // ai_data sub-object MUST use convo_id (snake_case) per the wire contract,
    // not convoId (which is the dashboard's transformed form)
    let ai_data = &payload["ai_data"];
    assert_eq!(ai_data["input"], "the input");
    assert_eq!(ai_data["output"], "the output");
    assert_eq!(ai_data["model"], "gpt-4o");
    assert_eq!(ai_data["convo_id"], "conv_pedantic");
    assert_eq!(ai_data.get("convoId"), None, "MUST NOT camelCase convo_id");

    // $context block must be present with library name + version + metadata
    let context = &payload["properties"]["$context"];
    assert_eq!(context["library"]["name"], "raindrop-rust");
    assert!(
        context["library"]["version"]
            .as_str()
            .is_some_and(|v| !v.is_empty()),
        "library.version must be non-empty"
    );
    assert_eq!(context["metadata"]["language"], "rust");

    // is_pending must be a boolean false (NOT the string "false" — Tinybird casts strictly)
    assert_eq!(payload["is_pending"], Value::Bool(false));
}

/// `event` defaults to `ai_generation` when not provided. The default value is owned by the
/// SDK, not the backend, so the wire payload MUST contain a non-empty `event` field.
#[tokio::test]
async fn track_ai_default_event_name_is_ai_generation() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_ai(AiEvent {
            user_id: "user".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    assert_eq!(payload["event"], "ai_generation");
}

/// All four attachment types must be accepted, role split must be preserved.
/// See `AttachmentSchema` in `@raindrop-ai/schemas/ingest`.
#[tokio::test]
async fn track_ai_attachments_serialize_with_type_role_and_optional_fields() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "i".into(),
            attachments: vec![
                Attachment {
                    kind: "code".into(),
                    role: "input".into(),
                    name: "snippet.py".into(),
                    value: "print('hi')".into(),
                    language: "python".into(),
                    ..Default::default()
                },
                Attachment {
                    kind: "text".into(),
                    role: "input".into(),
                    name: "extra".into(),
                    value: "long doc".into(),
                    ..Default::default()
                },
                Attachment {
                    kind: "image".into(),
                    role: "output".into(),
                    name: "screenshot".into(),
                    value: "https://example.com/img.png".into(),
                    ..Default::default()
                },
                Attachment {
                    kind: "iframe".into(),
                    role: "output".into(),
                    name: String::new(),
                    value: "<iframe src=\"...\"></iframe>".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    let atts = payload["attachments"]
        .as_array()
        .expect("attachments array");
    assert_eq!(atts.len(), 4);

    assert_eq!(atts[0]["type"], "code");
    assert_eq!(atts[0]["role"], "input");
    assert_eq!(atts[0]["language"], "python");
    assert_eq!(atts[0]["name"], "snippet.py");
    assert_eq!(atts[0]["value"], "print('hi')");

    assert_eq!(atts[1]["type"], "text");
    assert_eq!(atts[1]["role"], "input");
    // `language` is optional; for non-code attachments we skip-serialize the empty string
    assert!(
        atts[1].get("language").is_none() || atts[1]["language"].as_str() == Some(""),
        "non-code attachments shouldn't carry a non-empty language field"
    );

    assert_eq!(atts[2]["type"], "image");
    assert_eq!(atts[2]["role"], "output");
    assert_eq!(atts[3]["type"], "iframe");
    assert_eq!(atts[3]["role"], "output");
    // empty `name` should be omitted (skip_serializing_if = String::is_empty)
    assert!(
        atts[3].get("name").is_none() || atts[3]["name"].as_str() == Some(""),
        "empty name field should be skipped or empty string"
    );
}

/// The full begin → patch → finish lifecycle merges patches such that the final shipped
/// payload contains the SUM of all sticky data: input from begin, properties from patches,
/// and output from finish.
#[tokio::test]
async fn interaction_lifecycle_merges_all_patches_into_final_payload() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_lifecycle".into(),
            user_id: "user_lifecycle".into(),
            convo_id: "conv_lifecycle".into(),
            event: "agent_run".into(),
            input: "first".into(),
            ..Default::default()
        })
        .await;
    interaction
        .set_property("stage", "stage_1")
        .await
        .expect("set_property");
    interaction
        .set_property("retries", 3)
        .await
        .expect("set_property numeric");
    interaction
        .set_input("updated input")
        .await
        .expect("set_input");
    interaction
        .add_attachments(vec![Attachment {
            kind: "text".into(),
            role: "output".into(),
            name: "summary".into(),
            value: "tl;dr".into(),
            ..Default::default()
        }])
        .await
        .expect("add_attachments");
    interaction
        .finish(FinishOptions {
            output: "the answer".into(),
            model: "gpt-4o".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    // We expect ONE final flush after `finish` — partials may be buffered but the test client
    // disables periodic flush (interval=0), so only the final patch is shipped.
    let final_payload = recorder
        .requests()
        .last()
        .expect("at least one request")
        .json();
    assert_eq!(final_payload["event_id"], "evt_lifecycle");
    assert_eq!(final_payload["user_id"], "user_lifecycle");
    assert_eq!(final_payload["event"], "agent_run");
    assert_eq!(final_payload["ai_data"]["input"], "updated input");
    assert_eq!(final_payload["ai_data"]["output"], "the answer");
    assert_eq!(final_payload["ai_data"]["model"], "gpt-4o");
    assert_eq!(final_payload["ai_data"]["convo_id"], "conv_lifecycle");
    assert_eq!(final_payload["properties"]["stage"], "stage_1");
    assert_eq!(final_payload["properties"]["retries"], 3);
    assert_eq!(final_payload["is_pending"], false);
    assert_eq!(
        final_payload["attachments"][0]["name"], "summary",
        "attachment must survive into the final payload"
    );
}

/// Multiple events with the same `convo_id` must each carry the convo_id on the wire so the
/// backend can group them on the dashboard's convo_list pipe.
#[tokio::test]
async fn multiple_track_ai_calls_share_convo_id() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    for i in 0..3 {
        client
            .track_ai(AiEvent {
                user_id: "u".into(),
                input: format!("turn {}", i),
                output: format!("response {}", i),
                convo_id: "shared_convo".into(),
                ..Default::default()
            })
            .await
            .expect("track_ai");
    }
    client.close().await.expect("close");

    let reqs = recorder.requests();
    assert_eq!(reqs.len(), 3);
    for r in &reqs {
        let payload = r.json();
        assert_eq!(payload["ai_data"]["convo_id"], "shared_convo");
    }
}

// ────────────────────────────────────────────────────────────────────────────────────
// /signals/track wire format (SignalEventSchema)
// ────────────────────────────────────────────────────────────────────────────────────

/// Canonical signal payload shape: snake_case, signal_type field, comment merged into properties.
#[tokio::test]
async fn track_signal_payload_uses_canonical_shape() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/signals/track").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_signal(Signal {
            event_id: "evt_sig".into(),
            name: "thumbs_up".into(),
            kind: SignalKind::FEEDBACK.into(),
            sentiment: "POSITIVE".into(),
            comment: "great answer".into(),
            attachment_id: "att_42".into(),
            ..Default::default()
        })
        .await
        .expect("track_signal");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    // The signals endpoint expects an ARRAY of signal objects (matches JS SDK)
    let arr = payload.as_array().expect("signal payload is array");
    let sig = &arr[0];
    assert_eq!(sig["event_id"], "evt_sig");
    assert_eq!(sig["signal_name"], "thumbs_up");
    assert_eq!(sig["signal_type"], "feedback");
    assert_eq!(sig["sentiment"], "POSITIVE");
    assert_eq!(sig["attachment_id"], "att_42");
    // `comment` is merged INTO `properties.comment`, not at the top level
    assert_eq!(sig["properties"]["comment"], "great answer");
    assert!(
        sig.get("comment").is_none(),
        "comment must NOT appear as a top-level field"
    );
    // No camelCase
    assert_eq!(sig.get("signalType"), None);
    assert_eq!(sig.get("signalName"), None);
    assert_eq!(sig.get("eventId"), None);
}

/// All six accepted signal_type values must round-trip to the wire as-is.
#[tokio::test]
async fn track_signal_accepts_all_canonical_signal_types() {
    let server = MockServer::start().await;
    let recorder = mount_any_post(&server).await;
    let client = fast_client_builder(&server).build().expect("build");

    let kinds = [
        SignalKind::DEFAULT,
        SignalKind::STANDARD,
        SignalKind::FEEDBACK,
        SignalKind::EDIT,
        SignalKind::AGENT,
        SignalKind::AGENT_INTERNAL,
    ];

    for kind in kinds {
        client
            .track_signal(Signal {
                event_id: "evt".into(),
                name: format!("sig_{}", kind),
                kind: kind.to_string(),
                comment: if kind == SignalKind::FEEDBACK {
                    "c".to_string()
                } else {
                    String::new()
                },
                after: if kind == SignalKind::EDIT {
                    "a".to_string()
                } else {
                    String::new()
                },
                ..Default::default()
            })
            .await
            .expect("track_signal");
    }
    client.close().await.expect("close");

    let signal_requests: Vec<_> = recorder
        .requests()
        .into_iter()
        .filter(|r| r.path == "/signals/track")
        .collect();
    assert_eq!(signal_requests.len(), kinds.len());

    let mut seen = std::collections::HashSet::new();
    for req in &signal_requests {
        let arr = req.json();
        let sig = &arr[0];
        let signal_type = sig["signal_type"]
            .as_str()
            .expect("signal_type string")
            .to_string();
        seen.insert(signal_type);
    }
    for kind in kinds {
        assert!(
            seen.contains(kind),
            "signal_type {} was never sent on the wire",
            kind
        );
    }
}

/// Edit signals merge `after` into `properties.after`, mirroring `EditSignal._check_after_in_properties`.
#[tokio::test]
async fn track_signal_edit_merges_after_into_properties() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/signals/track").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_signal(Signal {
            event_id: "evt".into(),
            name: "user_edit".into(),
            kind: SignalKind::EDIT.into(),
            after: "the corrected output".into(),
            ..Default::default()
        })
        .await
        .expect("track_signal");
    client.close().await.expect("close");

    let arr = recorder.requests()[0].json();
    let sig = &arr[0];
    assert_eq!(sig["signal_type"], "edit");
    assert_eq!(sig["properties"]["after"], "the corrected output");
    assert!(
        sig.get("after").is_none(),
        "after must be in properties only"
    );
}

/// Empty `kind` defaults to `default` on the wire.
#[tokio::test]
async fn track_signal_empty_kind_defaults_to_default() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/signals/track").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_signal(Signal {
            event_id: "evt".into(),
            name: "thumbs_up".into(),
            ..Default::default()
        })
        .await
        .expect("track_signal");
    client.close().await.expect("close");

    let arr = recorder.requests()[0].json();
    assert_eq!(arr[0]["signal_type"], "default");
}

// ────────────────────────────────────────────────────────────────────────────────────
// /users/identify wire format (IdentifySchema)
// ────────────────────────────────────────────────────────────────────────────────────

/// Canonical identify payload shape: snake_case `user_id`, `traits` (NOT `properties`).
#[tokio::test]
async fn identify_payload_uses_canonical_shape() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/users/identify").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .identify(User {
            user_id: "u_canonical".into(),
            traits: BTreeMap::from([
                ("plan".into(), json!("pro")),
                ("created_at".into(), json!("2026-01-01T00:00:00Z")),
                ("seats".into(), json!(7)),
            ]),
        })
        .await
        .expect("identify");
    client.close().await.expect("close");

    let body = recorder.requests()[0].json();
    // The Rust SDK ships a single object (matching the Go SDK; the JS SDK accepts an array
    // and unwraps a single user). Both shapes are accepted by the backend.
    assert_eq!(body["user_id"], "u_canonical");
    assert_eq!(body["traits"]["plan"], "pro");
    assert_eq!(body["traits"]["seats"], 7);
    // No camelCase
    assert_eq!(body.get("userId"), None);
    assert!(
        body.get("properties").is_none(),
        "the wire field is `traits`, not `properties`"
    );
}

/// Empty `user_id` is dropped client-side (matches JS `EventShipper.identify`).
#[tokio::test]
async fn identify_with_empty_user_id_makes_no_request() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/users/identify").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .identify(User {
            user_id: String::new(),
            traits: BTreeMap::new(),
        })
        .await
        .expect("identify with empty user_id");
    client.close().await.expect("close");
    assert_eq!(
        recorder.count(),
        0,
        "identify with empty user_id should make no request"
    );
}

// ────────────────────────────────────────────────────────────────────────────────────
// /traces wire format (OTLP/JSON)
// ────────────────────────────────────────────────────────────────────────────────────

/// hasAIOperation filter: a span with ONLY `event_id` set must still pass the backend's
/// `hasAIOperation` filter. This is the bug we just fixed — without this, plain `start_span`
/// gets silently dropped at ingestion.
#[tokio::test]
async fn plain_span_passes_has_ai_operation_filter_via_traceloop_event_id() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "plain_span".into(),
        event_id: "evt_filter".into(),
        // No operation_id, no traceloop.span.kind, no association properties — relies on
        // `traceloop.association.properties.event_id` which IS in the filter list.
        ..Default::default()
    });
    span.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    let span_json = &spans_of(&payload)[0];

    // The hasAIOperation filter accepts any of: ai.operationId, traceloop.span.kind,
    // traceloop.workflow.name, traceloop.association.properties.{user_id,convo_id,event_id},
    // gen_ai.*. We always emit traceloop.association.properties.event_id when event_id is set.
    let assoc_event_id = span_attr(span_json, "traceloop.association.properties.event_id").expect(
        "traceloop.association.properties.event_id MUST be present (hasAIOperation filter)",
    );
    assert_eq!(assoc_event_id["stringValue"], "evt_filter");

    // The Vercel AI SDK metadata namespace mirror should also be present.
    let ai_sdk_md = span_attr(span_json, "ai.telemetry.metadata.raindrop.eventId").unwrap();
    assert_eq!(ai_sdk_md["stringValue"], "evt_filter");
}

/// Spans started from an Interaction inherit user_id, convo_id, and event as
/// `traceloop.association.properties.*` attributes — so they show up grouped under
/// the same user/convo/event in the dashboard's traces tab.
#[tokio::test]
async fn interaction_spans_inherit_user_convo_event_as_traceloop_assoc_props() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_assoc".into(),
            user_id: "user_assoc".into(),
            convo_id: "conv_assoc".into(),
            event: "agent_workflow".into(),
            input: "go".into(),
            ..Default::default()
        })
        .await;

    let span = interaction.start_span(SpanOptions {
        name: "step_1".into(),
        ..Default::default()
    });
    span.end();

    let tool = interaction.start_tool_span("lookup", ToolOptions::default());
    tool.end();

    let _ = client.close().await;

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }

    for span_json in &all_spans {
        let user_attr = span_attr(span_json, "traceloop.association.properties.user_id")
            .unwrap_or_else(|| panic!("missing user_id assoc on span {}", span_json["name"]));
        assert_eq!(user_attr["stringValue"], "user_assoc");

        let convo_attr = span_attr(span_json, "traceloop.association.properties.convo_id")
            .unwrap_or_else(|| panic!("missing convo_id assoc on span {}", span_json["name"]));
        assert_eq!(convo_attr["stringValue"], "conv_assoc");

        let event_attr = span_attr(span_json, "traceloop.association.properties.event")
            .unwrap_or_else(|| panic!("missing event assoc on span {}", span_json["name"]));
        assert_eq!(event_attr["stringValue"], "agent_workflow");
    }
}

/// User-supplied `properties` on SpanOptions take precedence over the interaction's sticky
/// `user_id`/`convo_id` defaults — matching how Python's `set_association_properties` works.
#[tokio::test]
async fn caller_supplied_properties_override_interaction_inherited_assoc_props() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_override".into(),
            user_id: "user_default".into(),
            convo_id: "conv_default".into(),
            ..Default::default()
        })
        .await;

    let mut props = BTreeMap::new();
    props.insert("user_id".into(), json!("user_OVERRIDE"));
    let span = interaction.start_span(SpanOptions {
        name: "override_span".into(),
        properties: props,
        ..Default::default()
    });
    span.end();
    let _ = client.close().await;

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    let span_json = all_spans
        .iter()
        .find(|s| s["name"] == "override_span")
        .unwrap();
    let user_attr = span_attr(span_json, "traceloop.association.properties.user_id").unwrap();
    assert_eq!(
        user_attr["stringValue"], "user_OVERRIDE",
        "caller-supplied user_id property MUST override interaction default"
    );
    let convo_attr = span_attr(span_json, "traceloop.association.properties.convo_id").unwrap();
    assert_eq!(
        convo_attr["stringValue"], "conv_default",
        "non-overridden convo_id should still inherit"
    );
}

/// Tool spans MUST have BOTH `traceloop.span.kind=tool` AND `ai.operationId=ai.toolCall`,
/// so the backend's `mapSpanType` correctly identifies them as TOOL_CALL regardless of
/// which detection branch runs first.
#[tokio::test]
async fn tool_span_emits_both_traceloop_and_ai_operation_id_attributes() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_tool_attrs".into(),
            user_id: "u".into(),
            ..Default::default()
        })
        .await;

    let tool = interaction.start_tool_span(
        "weather",
        ToolOptions {
            input: Some(json!({"city": "SF"})),
            ..Default::default()
        },
    );
    tool.set_output(&json!({"temp": 72}));
    tool.end();
    let _ = client.close().await;

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    let tool_span = all_spans.iter().find(|s| s["name"] == "weather").unwrap();

    // Both attributes are required because parseSpan.ts checks them in different branches
    let span_kind = span_attr(tool_span, "traceloop.span.kind").expect("traceloop.span.kind");
    assert_eq!(span_kind["stringValue"], "tool");
    let op_id = span_attr(tool_span, "ai.operationId").expect("ai.operationId");
    assert_eq!(op_id["stringValue"], "ai.toolCall");

    // Tool name attribute (the dashboard's toolCalls.tool_name comes from this)
    let tool_name = span_attr(tool_span, "traceloop.entity.name").expect("traceloop.entity.name");
    assert_eq!(tool_name["stringValue"], "weather");

    // Input/output payloads use the traceloop entity attribute names
    let input_attr =
        span_attr(tool_span, "traceloop.entity.input").expect("traceloop.entity.input");
    assert!(
        input_attr["stringValue"]
            .as_str()
            .unwrap()
            .contains("\"SF\""),
        "tool input should be JSON-stringified"
    );
    let output_attr =
        span_attr(tool_span, "traceloop.entity.output").expect("traceloop.entity.output");
    assert!(output_attr["stringValue"]
        .as_str()
        .unwrap()
        .contains("\"temp\":72"));
}

/// `traceloop.entity.duration_ms` is computed from start/end times for tool spans, in
/// milliseconds (NOT nanoseconds, NOT microseconds).
#[tokio::test]
async fn tool_span_duration_ms_attribute_uses_milliseconds() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_dur".into(),
            user_id: "u".into(),
            ..Default::default()
        })
        .await;

    let start = datetime!(2026-05-01 10:00:00 UTC);
    let end = start + Duration::from_millis(750);
    interaction.track_tool(TrackToolOptions {
        name: "lookup".into(),
        start_time: Some(start),
        end_time: Some(end),
        duration: Some(Duration::from_millis(750)),
        ..Default::default()
    });
    let _ = client.close().await;

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    let tool_span = all_spans.iter().find(|s| s["name"] == "lookup").unwrap();
    let dur =
        span_attr(tool_span, "traceloop.entity.duration_ms").expect("traceloop.entity.duration_ms");
    assert_eq!(dur["intValue"], "750");
}

/// Defensive clamp for anomalous callers: if a caller supplies end_time < start_time, the SDK
/// must not emit a negative duration. Real production traces from non-Rust producers have shown
/// negative `duration_ms`; Rust should never create that shape.
#[tokio::test]
async fn tool_span_end_before_start_is_clamped_to_zero_duration() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_negative_duration_guard".into(),
            user_id: "u".into(),
            ..Default::default()
        })
        .await;

    let start = datetime!(2026-05-01 10:00:00 UTC);
    let end = start - Duration::from_millis(250);
    interaction.track_tool(TrackToolOptions {
        name: "negative_duration_input".into(),
        start_time: Some(start),
        end_time: Some(end),
        ..Default::default()
    });
    let _ = client.close().await;

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    let tool_span = all_spans
        .iter()
        .find(|s| s["name"] == "negative_duration_input")
        .unwrap();
    let dur =
        span_attr(tool_span, "traceloop.entity.duration_ms").expect("traceloop.entity.duration_ms");
    assert_eq!(dur["intValue"], "0");

    let start_ns: u128 = tool_span["startTimeUnixNano"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let end_ns: u128 = tool_span["endTimeUnixNano"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        end_ns, start_ns,
        "end timestamp must be clamped up to start timestamp"
    );
}

/// OTLP/JSON encoding: span ids must be base64-encoded random bytes, trace_id 16 bytes,
/// span_id 8 bytes. start_time_unix_nano and end_time_unix_nano are decimal strings of
/// nanoseconds since epoch.
#[tokio::test]
async fn span_otlp_ids_and_timestamps_match_otlp_json_encoding() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let start = datetime!(2026-05-01 10:00:00 UTC);
    let end = start + Duration::from_millis(123);
    let span = client.start_span(SpanOptions {
        name: "encoded".into(),
        event_id: "evt".into(),
        start_time: Some(start),
        ..Default::default()
    });
    span.end_at(Some(end));
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = recorder_first_span(&trace_recorder);

    // trace_id is 16 random bytes -> 24 chars of base64 with up to 2 trailing '='
    let trace_id = payload["traceId"].as_str().unwrap();
    let trace_decoded = base64_decode(trace_id);
    assert_eq!(trace_decoded.len(), 16, "OTLP trace_id is 16 bytes");

    // span_id is 8 random bytes -> 12 chars of base64
    let span_id = payload["spanId"].as_str().unwrap();
    let span_decoded = base64_decode(span_id);
    assert_eq!(span_decoded.len(), 8, "OTLP span_id is 8 bytes");

    // OTLP/JSON spec requires nanoseconds as decimal strings
    let start_ns = payload["startTimeUnixNano"].as_str().unwrap();
    let end_ns = payload["endTimeUnixNano"].as_str().unwrap();
    let start_n: u128 = start_ns.parse().unwrap();
    let end_n: u128 = end_ns.parse().unwrap();
    assert_eq!(end_n - start_n, 123_000_000, "duration in ns");
    // 2026-05-01 10:00:00 UTC = 1777629600 unix seconds. The wire format is
    // nanoseconds since epoch, so multiply by 1_000_000_000.
    assert_eq!(start_n, 1_777_629_600_u128 * 1_000_000_000);
}

fn recorder_first_span(recorder: &crate::common::Recorder) -> Value {
    let payload = recorder.requests()[0].json();
    spans_of(&payload).into_iter().next().unwrap()
}

fn base64_decode(input: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .expect("valid base64")
}

/// Span attribute values use the OTLP/JSON `intValue` (string), `stringValue`, `boolValue`,
/// `doubleValue` keys — NOT the proto JSON shorthand `int_value`/`bool_value`.
#[tokio::test]
async fn span_attribute_values_use_otlp_json_encoding() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "attrs".into(),
        event_id: "evt".into(),
        attributes: vec![
            Attribute::string("ai.model.id", "gpt-4o"),
            Attribute::int("ai.usage.prompt_tokens", 42),
            Attribute::float("ai.cost.usd", 0.0042),
            Attribute::bool("debug", true),
            Attribute::string_array("tools", vec!["a".into(), "b".into()]),
        ],
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let span_json = recorder_first_span(&trace_recorder);

    let s = span_attr(&span_json, "ai.model.id").unwrap();
    assert_eq!(s["stringValue"], "gpt-4o");
    assert!(
        s.get("string_value").is_none(),
        "must be camelCase OTLP/JSON"
    );

    let i = span_attr(&span_json, "ai.usage.prompt_tokens").unwrap();
    assert_eq!(i["intValue"], "42", "OTLP/JSON ints are decimal STRINGS");

    let f = span_attr(&span_json, "ai.cost.usd").unwrap();
    assert!((f["doubleValue"].as_f64().unwrap() - 0.0042).abs() < 1e-9);

    let b = span_attr(&span_json, "debug").unwrap();
    assert_eq!(b["boolValue"], true);

    let arr = span_attr(&span_json, "tools").unwrap();
    let values = arr["arrayValue"]["values"].as_array().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0]["stringValue"], "a");
    assert_eq!(values[1]["stringValue"], "b");
}

/// Span status maps cleanly to the OTLP `status.code` enum values: 0=UNSET, 1=OK, 2=ERROR.
/// The dashboard's `mapStatusCode` uses this mapping.
#[tokio::test]
async fn span_status_codes_match_otlp_status_enum() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let ok_span = client.start_span(SpanOptions {
        name: "ok".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    ok_span.end();

    let err_span = client.start_span(SpanOptions {
        name: "err".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    err_span.set_error("boom");
    err_span.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all_spans = Vec::new();
    for r in trace_recorder.requests() {
        all_spans.extend(spans_of(&r.json()));
    }

    let ok = all_spans.iter().find(|s| s["name"] == "ok").unwrap();
    assert_eq!(ok["status"]["code"], 1, "OK = 1");

    let err = all_spans.iter().find(|s| s["name"] == "err").unwrap();
    assert_eq!(err["status"]["code"], 2, "ERROR = 2");
    assert_eq!(err["status"]["message"], "boom");
}

// ────────────────────────────────────────────────────────────────────────────────────
// Edge cases
// ────────────────────────────────────────────────────────────────────────────────────

/// `track_event` (non-AI event) must NOT carry an `ai_data` field on the wire.
#[tokio::test]
async fn track_event_non_ai_omits_ai_data() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let mut props = BTreeMap::new();
    props.insert("source".into(), json!("dashboard"));

    client
        .track_event(Event {
            user_id: "u".into(),
            event: "session_started".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_event");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    assert_eq!(payload["event"], "session_started");
    assert!(
        payload.get("ai_data").is_none() || payload["ai_data"].is_null(),
        "non-AI events must not have ai_data"
    );
    assert_eq!(payload["properties"]["source"], "dashboard");
}

/// PatchOptions with `is_pending: Some(false)` short-circuits to immediate flush — even when
/// the patch is the FIRST one (no prior begin).
#[tokio::test]
async fn patch_with_explicit_is_pending_false_flushes_immediately() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .patch(
            "evt_patched",
            PatchOptions {
                user_id: "u".into(),
                input: "first".into(),
                output: "last".into(),
                is_pending: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("patch");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    assert_eq!(payload["event_id"], "evt_patched");
    assert_eq!(payload["is_pending"], false);
    assert_eq!(payload["ai_data"]["input"], "first");
    assert_eq!(payload["ai_data"]["output"], "last");
}

/// A no-op (disabled) client must NOT make ANY HTTP requests, period.
#[tokio::test]
async fn disabled_client_skips_every_endpoint() {
    let server = MockServer::start().await;
    let recorder = mount_any_post(&server).await;
    // No write_key → disabled
    let client = raindrop::Client::builder()
        .endpoint(format!("{}/", server.uri()))
        .build()
        .expect("build disabled client");

    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            ..Default::default()
        })
        .await
        .expect("noop");
    client
        .track_event(Event {
            user_id: "u".into(),
            event: "e".into(),
            ..Default::default()
        })
        .await
        .expect("noop");
    client
        .track_signal(Signal {
            event_id: "e".into(),
            name: "s".into(),
            ..Default::default()
        })
        .await
        .expect("noop");
    client
        .identify(User {
            user_id: "u".into(),
            ..Default::default()
        })
        .await
        .expect("noop");

    let interaction = client
        .begin(BeginOptions {
            user_id: "u".into(),
            ..Default::default()
        })
        .await;
    let span = interaction.start_span(SpanOptions::default());
    span.end();
    let tool = interaction.start_tool_span("t", ToolOptions::default());
    tool.end();
    interaction
        .finish(FinishOptions::default())
        .await
        .expect("noop finish");

    client.close().await.expect("close");
    assert_eq!(
        recorder.count(),
        0,
        "disabled client must make zero HTTP requests across every endpoint"
    );
}

// ────────────────────────────────────────────────────────────────────────────────────
// Additional pedantic coverage discovered while sampling real production data.
// ────────────────────────────────────────────────────────────────────────────────────

/// Tool spans MUST NOT emit `traceloop.association.properties.event_id` more than once.
/// Two paths converge on the same span: `Client::start_tool_span` injects `event_id` into
/// the properties map (which `tool_property_attributes` lifts into the attributes vec),
/// and `Span::end_at` ALSO emits the attribute when `inner.event_id` is non-empty for the
/// `hasAIOperation` filter. Without dedupe, every tool span would carry the attribute
/// twice — same value, but a violation of OTLP's "attribute keys MUST be unique"
/// invariant. Regression test for the duplicate-attribute bug Devin Review caught.
#[tokio::test]
async fn tool_span_emits_traceloop_event_id_exactly_once() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_dedupe".into(),
            user_id: "u".into(),
            ..Default::default()
        })
        .await;
    let tool = interaction.start_tool_span("dedupe_tool", ToolOptions::default());
    tool.end();
    let _ = client.close().await;

    let mut tool_spans = Vec::new();
    for r in trace_recorder.requests() {
        for s in spans_of(&r.json()) {
            if s["name"] == "dedupe_tool" {
                tool_spans.push(s);
            }
        }
    }
    assert_eq!(tool_spans.len(), 1);
    let attrs = tool_spans[0]["attributes"].as_array().expect("attributes");
    let count = attrs
        .iter()
        .filter(|a| a["key"].as_str() == Some("traceloop.association.properties.event_id"))
        .count();
    assert_eq!(
        count, 1,
        "traceloop.association.properties.event_id must appear EXACTLY once on tool spans, got {}: {:?}",
        count, attrs
    );
    // Confirm the value is still our event_id (dedupe must not drop it entirely).
    let kept = attrs
        .iter()
        .find(|a| a["key"].as_str() == Some("traceloop.association.properties.event_id"))
        .unwrap();
    assert_eq!(kept["value"]["stringValue"], "evt_dedupe");
}

/// Manual (non-tool) spans started via `Client::start_span` should also emit the attribute
/// exactly once — this is the existing path covered by
/// `plain_span_passes_has_ai_operation_filter_via_traceloop_event_id`, but we re-verify
/// the count here to make sure dedupe didn't accidentally drop the canonical emission.
#[tokio::test]
async fn manual_span_emits_traceloop_event_id_exactly_once() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "plain".into(),
        event_id: "evt_plain".into(),
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let span_json = &spans_of(&payload)[0];
    let attrs = span_json["attributes"].as_array().expect("attributes");
    let count = attrs
        .iter()
        .filter(|a| a["key"].as_str() == Some("traceloop.association.properties.event_id"))
        .count();
    assert_eq!(
        count, 1,
        "manual span must emit traceloop.association.properties.event_id exactly once"
    );
}

/// Caller-supplied `attachment_id` round-trips on the wire so the backend can dedupe and
/// so a follow-up `Signal { attachment_id }` can reference the exact attachment.
///
/// Real-prod sample (across 50+ orgs): 350+ attachments carry a caller-set `attachment_id`.
/// The backend auto-generates a UUID v4 if missing; the field is OPTIONAL on the canonical
/// `BaseAttachmentSchema` (`@raindrop-ai/schemas/ingest`).
#[tokio::test]
async fn attachment_with_caller_supplied_attachment_id_round_trips_on_the_wire() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            attachments: vec![
                Attachment {
                    kind: "image".into(),
                    role: "input".into(),
                    name: "screenshot.png".into(),
                    value: "https://cdn.example/img.png".into(),
                    attachment_id: "att_caller_specific_uuid".into(),
                    ..Default::default()
                },
                Attachment {
                    kind: "text".into(),
                    role: "output".into(),
                    name: "summary".into(),
                    value: "ok".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    let atts = payload["attachments"].as_array().expect("attachments");
    assert_eq!(
        atts[0]["attachment_id"].as_str().unwrap_or(""),
        "att_caller_specific_uuid",
        "caller-supplied attachment_id MUST be preserved on the wire"
    );
    // Empty attachment_id must be skip-serialized so the backend can auto-generate a UUID
    // rather than rejecting an empty string against any future strict schema.
    assert!(
        atts[1].get("attachment_id").is_none() || atts[1]["attachment_id"].as_str() == Some(""),
        "empty attachment_id should be skipped on the wire so backend can default it; got {:?}",
        atts[1]
    );
}

/// Token-usage helper emits the canonical OpenTelemetry GenAI numeric attributes that the
/// Raindrop backend reads to populate `event.toolCalls[]` token metadata and the per-event
/// `aiData.usage`. The backend gates on the presence of `gen_ai.response.model` — without
/// it, token usage is silently dropped.
#[tokio::test]
async fn span_set_token_usage_emits_gen_ai_attributes() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "llm_call".into(),
        event_id: "evt".into(),
        operation_id: "ai.generateText".into(),
        ..Default::default()
    });
    span.set_token_usage("gpt-4o-mini", 47, 11);
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let span_json = recorder_first_span(&trace_recorder);
    // Backend gate: gen_ai.response.model must be present, otherwise tokens are dropped.
    let model = span_attr(&span_json, "gen_ai.response.model").expect("gen_ai.response.model");
    assert_eq!(model["stringValue"], "gpt-4o-mini");
    let input_tokens =
        span_attr(&span_json, "gen_ai.usage.input_tokens").expect("gen_ai.usage.input_tokens");
    assert_eq!(
        input_tokens["intValue"], "47",
        "OTLP/JSON encodes ints as decimal strings"
    );
    let output_tokens =
        span_attr(&span_json, "gen_ai.usage.output_tokens").expect("gen_ai.usage.output_tokens");
    assert_eq!(output_tokens["intValue"], "11");
}

/// `set_token_usage` with `0` for either count omits the corresponding attribute and (when
/// `model` is empty) omits the `gen_ai.response.model` gate. This mirrors the Python SDK's
/// `set_llm_span_io` semantics — only emit what the caller actually has.
#[tokio::test]
async fn span_set_token_usage_omits_zero_and_empty_model() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "llm_call".into(),
        event_id: "evt".into(),
        operation_id: "ai.generateText".into(),
        ..Default::default()
    });
    span.set_token_usage("", 0, 0);
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let span_json = recorder_first_span(&trace_recorder);
    assert!(span_attr(&span_json, "gen_ai.response.model").is_none());
    assert!(span_attr(&span_json, "gen_ai.usage.input_tokens").is_none());
    assert!(span_attr(&span_json, "gen_ai.usage.output_tokens").is_none());
}

/// A child span shares its parent's `traceId` and references the parent via `parentSpanId`.
/// A second top-level span gets a fresh `traceId`. This is the contract for the dashboard's
/// `traces.list` to render a connected tree.
#[tokio::test]
async fn parent_child_spans_share_trace_id_and_link_via_parent_span_id() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let root = client.start_span(SpanOptions {
        name: "root".into(),
        event_id: "evt_a".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    let child = client.start_span(SpanOptions {
        name: "child".into(),
        event_id: "evt_a".into(),
        operation_id: "ai.task".into(),
        parent: Some(root.clone()),
        ..Default::default()
    });
    let grandchild = client.start_span(SpanOptions {
        name: "grandchild".into(),
        event_id: "evt_a".into(),
        operation_id: "ai.task".into(),
        parent: Some(child.clone()),
        ..Default::default()
    });
    grandchild.end();
    child.end();
    root.end();

    let independent = client.start_span(SpanOptions {
        name: "independent".into(),
        event_id: "evt_b".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    independent.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all = Vec::new();
    for r in trace_recorder.requests() {
        all.extend(spans_of(&r.json()));
    }
    let by_name = |n: &str| all.iter().find(|s| s["name"] == n).expect(n);
    let root_s = by_name("root");
    let child_s = by_name("child");
    let grand_s = by_name("grandchild");
    let indep_s = by_name("independent");

    assert_eq!(
        root_s["traceId"], child_s["traceId"],
        "child must share trace_id with root"
    );
    assert_eq!(
        root_s["traceId"], grand_s["traceId"],
        "grandchild must share trace_id with root"
    );
    assert_ne!(
        root_s["traceId"], indep_s["traceId"],
        "independent root span must get a fresh trace_id"
    );
    // parentSpanId linkage: child.parent_span_id == root.span_id
    assert_eq!(
        child_s["parentSpanId"].as_str().unwrap_or(""),
        root_s["spanId"].as_str().unwrap_or(""),
        "child.parentSpanId must equal root.spanId"
    );
    assert_eq!(
        grand_s["parentSpanId"].as_str().unwrap_or(""),
        child_s["spanId"].as_str().unwrap_or(""),
        "grandchild.parentSpanId must equal child.spanId"
    );
    // root has no parent
    assert!(
        root_s
            .get("parentSpanId")
            .map(|v| v.as_str().unwrap_or("").is_empty())
            .unwrap_or(true),
        "root span must NOT serialize a non-empty parentSpanId, got {:?}",
        root_s.get("parentSpanId")
    );
    // independent root has no parent
    assert!(indep_s
        .get("parentSpanId")
        .map(|v| v.as_str().unwrap_or("").is_empty())
        .unwrap_or(true));
}

// ────────────────────────────────────────────────────────────────────────────────────
// Contract v1 canonical attributes — emitted alongside the upstream-owned namespaces.
// ────────────────────────────────────────────────────────────────────────────────────

/// Plain spans with an `event_id` MUST emit the canonical `raindrop.event.id`
/// attribute alongside `ai.telemetry.metadata.raindrop.eventId` (Vercel AI SDK
/// metadata namespace) and `traceloop.association.properties.event_id`
/// (Traceloop OpenLLMetry namespace). Workshop's parser reads the canonical key
/// preferentially; dawn ingestion continues to read the upstream namespaces.
#[tokio::test]
async fn span_emits_canonical_raindrop_event_id_alongside_upstream_namespaces() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "canonical_attrs".into(),
        event_id: "evt_canon".into(),
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let span_json = &spans_of(&payload)[0];

    let canonical = span_attr(span_json, "raindrop.event.id")
        .expect("raindrop.event.id MUST be emitted alongside the upstream namespaces");
    assert_eq!(canonical["stringValue"], "evt_canon");

    // Upstream-owned namespaces must still be there (dawn ingestion reads them).
    let ai_sdk_md = span_attr(span_json, "ai.telemetry.metadata.raindrop.eventId").unwrap();
    assert_eq!(ai_sdk_md["stringValue"], "evt_canon");
    let traceloop_props =
        span_attr(span_json, "traceloop.association.properties.event_id").unwrap();
    assert_eq!(traceloop_props["stringValue"], "evt_canon");
}

/// Tool spans get the canonical `raindrop.span.kind = tool_call` and
/// `raindrop.tool.name` keys alongside the upstream-owned Traceloop attributes.
#[tokio::test]
async fn tool_span_emits_canonical_raindrop_span_kind_and_tool_name() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_tool_canon".into(),
            user_id: "u".into(),
            ..Default::default()
        })
        .await;
    let tool = interaction.start_tool_span("lookup", ToolOptions::default());
    tool.end();
    let _ = client.close().await;

    let mut all = Vec::new();
    for r in trace_recorder.requests() {
        all.extend(spans_of(&r.json()));
    }
    let tool_span = all.iter().find(|s| s["name"] == "lookup").unwrap();

    let kind = span_attr(tool_span, "raindrop.span.kind").expect("raindrop.span.kind");
    assert_eq!(kind["stringValue"], "tool_call");
    let name = span_attr(tool_span, "raindrop.tool.name").expect("raindrop.tool.name");
    assert_eq!(name["stringValue"], "lookup");

    // Legacy keys still emitted for dawn.
    let legacy_kind = span_attr(tool_span, "traceloop.span.kind").unwrap();
    assert_eq!(legacy_kind["stringValue"], "tool");
    let legacy_name = span_attr(tool_span, "traceloop.entity.name").unwrap();
    assert_eq!(legacy_name["stringValue"], "lookup");
}

/// When the client is configured with workspace metadata, every OTLP span MUST
/// carry the canonical `raindrop.workspace.{id,name,root}` attributes so
/// Workshop can scope the dashboard view to that workspace.
#[tokio::test]
async fn span_emits_canonical_workspace_attributes_when_configured() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server)
        .workspace(raindrop::contract::v1::workspace::LocalWorkspaceMetadata {
            id: "ws_pedantic".into(),
            name: "Pedantic Workspace".into(),
            root: "/tmp/pedantic".into(),
        })
        .build()
        .expect("build");

    let span = client.start_span(SpanOptions {
        name: "ws_span".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let span_json = &spans_of(&trace_recorder.requests()[0].json())[0];
    assert_eq!(
        span_attr(span_json, "raindrop.workspace.id").unwrap()["stringValue"],
        "ws_pedantic"
    );
    assert_eq!(
        span_attr(span_json, "raindrop.workspace.name").unwrap()["stringValue"],
        "Pedantic Workspace"
    );
    assert_eq!(
        span_attr(span_json, "raindrop.workspace.root").unwrap()["stringValue"],
        "/tmp/pedantic"
    );
}

/// `track_partial` payloads must carry `properties.workspace` when the client
/// has workspace metadata configured. Workshop reads this to scope partial
/// events without needing OTLP attrs.
#[tokio::test]
async fn track_partial_auto_stamps_workspace_when_configured() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .workspace(raindrop::contract::v1::workspace::LocalWorkspaceMetadata {
            id: "ws_track".into(),
            name: "Track Workspace".into(),
            root: "/tmp/track".into(),
        })
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
        .expect("track_ai");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    let ws = &payload["properties"]["workspace"];
    assert_eq!(ws["id"], "ws_track");
    assert_eq!(ws["name"], "Track Workspace");
    assert_eq!(ws["root"], "/tmp/track");
}

/// Caller-supplied `properties.workspace` MUST NOT be overwritten by the
/// auto-stamp. The reader treats explicit caller intent as the source of truth.
#[tokio::test]
async fn track_partial_caller_supplied_workspace_overrides_auto_stamp() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server)
        .workspace(raindrop::contract::v1::workspace::LocalWorkspaceMetadata {
            id: "ws_default".into(),
            name: "Default".into(),
            root: "/tmp/default".into(),
        })
        .build()
        .expect("build");

    let mut props = BTreeMap::new();
    props.insert(
        "workspace".into(),
        json!({"id": "ws_caller", "name": "Caller", "root": "/tmp/caller"}),
    );
    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    let ws = &payload["properties"]["workspace"];
    assert_eq!(ws["id"], "ws_caller", "caller-supplied workspace MUST win");
    assert_eq!(ws["name"], "Caller");
}

/// Repeated tool name across the same event creates separate spans with distinct `spanId`s
/// and is preserved in the dashboard's `toolCalls[]` array (which dedupes by `span_id`, not
/// by name). Real production data shows tools repeated 5–25 times within a single event
/// (`PARALLEL_TOOLS`, `view_bulk`, `count_events`, etc).
#[tokio::test]
async fn repeated_tool_names_within_one_event_get_distinct_span_ids() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_repeat".into(),
            user_id: "u".into(),
            ..Default::default()
        })
        .await;

    for i in 0..4 {
        interaction.track_tool(TrackToolOptions {
            name: "search".into(),
            input: Some(json!({"q": format!("query_{}", i)})),
            duration: Some(Duration::from_millis(10 + i * 5)),
            ..Default::default()
        });
    }
    let _ = client.close().await;

    let mut tool_spans = Vec::new();
    for r in trace_recorder.requests() {
        for s in spans_of(&r.json()) {
            if s["name"] == "search" {
                tool_spans.push(s);
            }
        }
    }
    assert_eq!(
        tool_spans.len(),
        4,
        "expected 4 distinct tool spans, got {}",
        tool_spans.len()
    );
    let mut span_ids = std::collections::HashSet::new();
    for s in &tool_spans {
        let id = s["spanId"].as_str().unwrap_or("").to_string();
        assert!(!id.is_empty(), "span_id must be non-empty");
        assert!(
            span_ids.insert(id.clone()),
            "duplicate span_id {} for repeated tool name",
            id
        );
    }
    assert_eq!(span_ids.len(), 4, "all 4 span_ids must be unique");
}

/// `track_signal` with empty optional fields skip-serializes them so the backend's
/// `SignalEventSchema` (sentiment ∈ {POSITIVE, NEGATIVE}; attachment_id optional) doesn't
/// reject an empty string.
#[tokio::test]
async fn track_signal_with_empty_optional_fields_omits_them_on_the_wire() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/signals/track").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .track_signal(Signal {
            event_id: "evt".into(),
            name: "thumbs_up".into(),
            // sentiment, attachment_id, comment, after, timestamp, properties all empty
            ..Default::default()
        })
        .await
        .expect("track_signal");
    client.close().await.expect("close");

    let arr = recorder.requests()[0].json();
    let sig = &arr[0];
    assert!(
        sig.get("sentiment").is_none(),
        "empty sentiment must be skipped on the wire"
    );
    assert!(
        sig.get("attachment_id").is_none(),
        "empty attachment_id must be skipped on the wire"
    );
    assert!(
        sig.get("timestamp").is_none() || sig["timestamp"].as_str() == Some(""),
        "empty timestamp must be skipped"
    );
    // properties is always emitted as an object (possibly empty), never null
    assert!(
        sig["properties"].is_object(),
        "properties must be an object even when empty"
    );
}

/// `User.identify` with empty traits ships a `traits: {}` object — never `null`, never
/// absent. The dashboard's join from users → events expects a JSON object body.
#[tokio::test]
async fn identify_with_empty_traits_ships_empty_object() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/users/identify").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .identify(User {
            user_id: "u_empty".into(),
            traits: BTreeMap::new(),
        })
        .await
        .expect("identify");
    client.close().await.expect("close");

    let body = recorder.requests()[0].json();
    assert_eq!(body["user_id"], "u_empty");
    assert!(body["traits"].is_object(), "traits must be an object");
    assert_eq!(
        body["traits"].as_object().unwrap().len(),
        0,
        "traits should serialize as an empty object {{}}"
    );
}

/// `track_partial` with a missing `user_id` is silently buffered (cannot ship without one)
/// rather than erroring. A subsequent patch carrying `user_id` flushes the merged payload.
#[tokio::test]
async fn track_partial_without_user_id_is_buffered_and_flushes_on_later_user_id() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .patch(
            "evt_buffered",
            PatchOptions {
                input: "first".into(),
                ..Default::default()
            },
        )
        .await
        .expect("buffered patch");
    // No request yet — patch with user_id missing must be buffered.
    assert_eq!(recorder.count(), 0, "no request without user_id");

    client
        .patch(
            "evt_buffered",
            PatchOptions {
                user_id: "u".into(),
                output: "last".into(),
                is_pending: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("flush patch");
    client.close().await.expect("close");

    assert_eq!(recorder.count(), 1, "exactly one flushed request");
    let payload = recorder.requests()[0].json();
    assert_eq!(payload["event_id"], "evt_buffered");
    assert_eq!(
        payload["ai_data"]["input"], "first",
        "buffered input from earlier patch must survive"
    );
    assert_eq!(payload["ai_data"]["output"], "last");
    assert_eq!(payload["is_pending"], false);
}

/// `track_event` (non-AI) with properties: the wire payload preserves user-set property
/// keys EXACTLY (no flattening, no rename, no implicit prefixing) and adds the SDK's
/// `$context` block — matching the JS SDK's `EventShipper.trackEvent`.
#[tokio::test]
async fn track_event_preserves_user_properties_verbatim() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let mut props = BTreeMap::new();
    props.insert("source".into(), json!("dashboard"));
    props.insert("buildVersion".into(), json!("3.1.157"));
    props.insert("nested.dotted.key".into(), json!("ok"));
    // Real-prod sample: `tags` arrives as a JSON-stringified array on some orgs; the SDK
    // must not auto-decode or modify it.
    props.insert(
        "tags".into(),
        json!("[\"production\",\"plan:pro\",\"trigger:user\"]"),
    );

    client
        .track_event(Event {
            user_id: "u".into(),
            event: "session_started".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_event");
    client.close().await.expect("close");

    let payload = recorder.requests()[0].json();
    let p = &payload["properties"];
    assert_eq!(p["source"], "dashboard");
    assert_eq!(p["buildVersion"], "3.1.157");
    assert_eq!(
        p["nested.dotted.key"], "ok",
        "dotted keys must round-trip verbatim (not nest under .nested.dotted)"
    );
    assert_eq!(
        p["tags"], "[\"production\",\"plan:pro\",\"trigger:user\"]",
        "stringified tag arrays must round-trip without auto-decoding"
    );
    assert!(p.get("$context").is_some(), "$context auto-injected");
    assert!(
        payload.get("ai_data").is_none() || payload["ai_data"].is_null(),
        "non-AI event must not carry ai_data"
    );
}

/// `track_ai` properties merge into the same `properties` object as `$context` (i.e. SDK
/// must not clobber caller properties when injecting `$context`).
#[tokio::test]
async fn track_ai_user_properties_and_context_coexist() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let mut props = BTreeMap::new();
    props.insert("temperature".into(), json!(0.7));
    props.insert("organizationId".into(), json!("org_123"));

    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: "x".into(),
            output: "y".into(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    let p = &recorder.requests()[0].json()["properties"];
    assert_eq!(p["temperature"], 0.7);
    assert_eq!(p["organizationId"], "org_123");
    assert!(p["$context"].is_object());
}

/// Identify followed by a track_ai for the same `user_id`: the SDK must NOT auto-merge
/// traits onto subsequent track_ai payloads. Traits live ONLY on `/users/identify`. This
/// mirrors how the dashboard's user→event join works server-side.
#[tokio::test]
async fn identify_does_not_leak_traits_into_subsequent_track_ai() {
    let server = MockServer::start().await;
    let identify_recorder = mount_path(&server, "POST", "/users/identify").await;
    let track_recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    client
        .identify(User {
            user_id: "u_join".into(),
            traits: BTreeMap::from([
                ("plan".into(), json!("pro")),
                ("country".into(), json!("US")),
            ]),
        })
        .await
        .expect("identify");
    client
        .track_ai(AiEvent {
            user_id: "u_join".into(),
            input: "x".into(),
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    assert_eq!(identify_recorder.count(), 1);
    assert_eq!(track_recorder.count(), 1);
    let track_payload = track_recorder.requests()[0].json();
    let p = &track_payload["properties"];
    assert!(
        p.get("plan").is_none(),
        "traits must NOT leak into track_ai properties; got {:?}",
        p
    );
    assert!(
        p.get("country").is_none(),
        "traits must NOT leak into track_ai properties; got {:?}",
        p
    );
}

/// `traceloop.span.kind = workflow` and `traceloop.span.kind = task` survive ingestion.
/// Real production samples show these used for agent.root and subagent spans (e.g. Vercel
/// AI SDK + Raindrop) — our SDK accepts arbitrary kind values via [`Attribute::string`].
#[tokio::test]
async fn manual_span_with_workflow_or_task_kind_attribute_survives_filter() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");

    let workflow = client.start_span(SpanOptions {
        name: "agent.root".into(),
        event_id: "evt_workflow".into(),
        attributes: vec![Attribute::string("traceloop.span.kind", "workflow")],
        // Note: no operation_id — exercising that traceloop.span.kind alone passes the filter
        ..Default::default()
    });
    workflow.end();

    let task = client.start_span(SpanOptions {
        name: "subagent.planner".into(),
        event_id: "evt_workflow".into(),
        attributes: vec![Attribute::string("traceloop.span.kind", "task")],
        ..Default::default()
    });
    task.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all = Vec::new();
    for r in trace_recorder.requests() {
        all.extend(spans_of(&r.json()));
    }
    let workflow_span = all.iter().find(|s| s["name"] == "agent.root").unwrap();
    let kind = span_attr(workflow_span, "traceloop.span.kind").expect("traceloop.span.kind");
    assert_eq!(kind["stringValue"], "workflow");
    let task_span = all
        .iter()
        .find(|s| s["name"] == "subagent.planner")
        .unwrap();
    let kind = span_attr(task_span, "traceloop.span.kind").expect("traceloop.span.kind");
    assert_eq!(kind["stringValue"], "task");
}

/// Oversized payloads (> 1 MiB) are dropped client-side before the HTTP request is fired,
/// matching the JS and Python SDKs' `MAX_INGEST_SIZE_BYTES` / `max_ingest_size_bytes`
/// behavior. This prevents 413 storms on the gateway and protects host applications from
/// runaway memory when a caller accidentally streams a giant prompt.
#[tokio::test]
async fn oversized_track_ai_payload_is_dropped_client_side() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    // Build a payload that comfortably exceeds 1 MiB after JSON serialization.
    let huge_input: String = "x".repeat(2 * 1024 * 1024);
    client
        .track_ai(AiEvent {
            user_id: "u".into(),
            input: huge_input,
            output: "y".into(),
            ..Default::default()
        })
        .await
        .expect("track_ai must NOT error on oversized payload");
    client.close().await.expect("close");

    assert_eq!(
        recorder.count(),
        0,
        "oversized payload (> 1 MiB) must be dropped before hitting the wire"
    );
}

/// `add_attachments` across multiple patches in an interaction must accumulate (not replace)
/// the attachments list, with input/output ordering preserved. Mirrors the JS SDK's
/// `mergeAttachments` semantics.
#[tokio::test]
async fn add_attachments_across_patches_accumulates_in_order() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_attach_acc".into(),
            user_id: "u".into(),
            input: "go".into(),
            attachments: vec![Attachment {
                kind: "text".into(),
                role: "input".into(),
                name: "first".into(),
                value: "alpha".into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await;

    interaction
        .add_attachments(vec![Attachment {
            kind: "text".into(),
            role: "output".into(),
            name: "second".into(),
            value: "beta".into(),
            ..Default::default()
        }])
        .await
        .expect("add second");
    interaction
        .add_attachments(vec![Attachment {
            kind: "image".into(),
            role: "output".into(),
            name: "third".into(),
            value: "https://x/img.png".into(),
            ..Default::default()
        }])
        .await
        .expect("add third");

    interaction
        .finish(FinishOptions {
            output: "done".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    let final_payload = recorder
        .requests()
        .last()
        .expect("at least one request")
        .json();
    let atts = final_payload["attachments"]
        .as_array()
        .expect("attachments");
    assert_eq!(
        atts.len(),
        3,
        "all three attachments must accumulate, got {:?}",
        atts
    );
    assert_eq!(atts[0]["name"], "first");
    assert_eq!(atts[1]["name"], "second");
    assert_eq!(atts[2]["name"], "third");
}
