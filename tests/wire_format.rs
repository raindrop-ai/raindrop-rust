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
                },
                Attachment {
                    kind: "text".into(),
                    role: "input".into(),
                    name: "extra".into(),
                    value: "long doc".into(),
                    language: String::new(), // text doesn't have language
                },
                Attachment {
                    kind: "image".into(),
                    role: "output".into(),
                    name: "screenshot".into(),
                    value: "https://example.com/img.png".into(),
                    language: String::new(),
                },
                Attachment {
                    kind: "iframe".into(),
                    role: "output".into(),
                    name: String::new(),
                    value: "<iframe src=\"...\"></iframe>".into(),
                    language: String::new(),
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
/// gets silently dropped at ingestion. See `apps/dawn/lib/traces/parseSpan.ts::hasAIOperation`.
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

    // Legacy fallback attribute should also still be present for backward compat
    let legacy = span_attr(span_json, "ai.telemetry.metadata.raindrop.eventId").unwrap();
    assert_eq!(legacy["stringValue"], "evt_filter");
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
