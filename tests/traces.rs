mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};
use time::OffsetDateTime;
use wiremock::MockServer;

use raindrop::{Attribute, BeginOptions, SpanOptions, ToolOptions, TrackToolOptions};

use crate::common::{fast_client_builder, mount_path, span_attr, spans_of};

#[tokio::test]
async fn trace_shipping_uses_otlp_json() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "llm_call".into(),
        event_id: "evt_123".into(),
        ..Default::default()
    });
    span.set_attributes([
        Attribute::string("ai.model.id", "gpt-4o"),
        Attribute::int("ai.usage.prompt_tokens", 10),
    ]);
    span.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let spans = spans_of(&payload);
    assert_eq!(
        spans.len(),
        1,
        "expected 1 span, got {}: {:#?}",
        spans.len(),
        spans
    );
    assert!(spans[0]["traceId"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(spans[0]["spanId"].as_str().is_some_and(|s| !s.is_empty()));
    let status_code = spans[0]["status"]["code"].as_u64().unwrap_or(0);
    assert_eq!(status_code, 1, "expected OK status (1)");

    // intValue is serialized as a string per OTLP/JSON spec.
    let prompt_tokens_attr = span_attr(&spans[0], "ai.usage.prompt_tokens").expect("attr");
    assert_eq!(prompt_tokens_attr["intValue"], "10");
    let model_attr = span_attr(&spans[0], "ai.model.id").expect("attr");
    assert_eq!(model_attr["stringValue"], "gpt-4o");
}

#[tokio::test]
async fn tool_helpers_ship_tool_shaped_spans() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_tools".into(),
            user_id: "user-123".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;

    let mut props = BTreeMap::new();
    props.insert("stage".into(), json!("planning"));

    let reasoning = interaction.start_span(SpanOptions {
        name: "plan_synthesis".into(),
        properties: props,
        ..Default::default()
    });
    reasoning.end();

    let mut tool_props = BTreeMap::new();
    tool_props.insert("user_id".into(), json!("user-123"));

    let tool_span = interaction.start_tool_span(
        "weather_lookup",
        ToolOptions {
            input: Some(json!({"location": "San Francisco"})),
            properties: tool_props,
            ..Default::default()
        },
    );
    tool_span.set_output(&json!({"forecast": "sunny"}));
    tool_span.end();

    let start = time::macros::datetime!(2026-01-01 10:00:00 UTC);
    let end = start + Duration::from_millis(250);

    let mut coffee_props = BTreeMap::new();
    coffee_props.insert("convo_id".into(), json!("conv-123"));
    interaction.track_tool(TrackToolOptions {
        name: "coffee_search".into(),
        input: Some(json!({"query": "best coffee"})),
        output: Some(json!({"winner": "Ritual"})),
        properties: coffee_props,
        start_time: Some(start),
        end_time: Some(end),
        ..Default::default()
    });

    let result = raindrop::with_tool::<_, _, std::io::Error>(
        &interaction,
        "park_check",
        ToolOptions {
            input: Some(json!({"location": "Dolores Park"})),
            ..Default::default()
        },
        || Ok(json!({"recommendation": "yes"})),
    )
    .expect("with_tool");
    assert_eq!(result["recommendation"], "yes");

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all_spans: Vec<Value> = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    assert_eq!(
        all_spans.len(),
        4,
        "expected 4 spans, got {}: {:#?}",
        all_spans.len(),
        all_spans
    );

    let mut found_weather = false;
    let mut found_coffee = false;
    let mut found_reasoning = false;
    let mut found_park = false;
    for span in &all_spans {
        let name = span["name"].as_str().unwrap_or("");
        match name {
            "plan_synthesis" => found_reasoning = true,
            "weather_lookup" => {
                found_weather = true;
                let kind = span_attr(span, "traceloop.span.kind").unwrap();
                assert_eq!(kind["stringValue"], "tool");
                let evt_id_attr =
                    span_attr(span, "traceloop.association.properties.event_id").unwrap();
                assert_eq!(evt_id_attr["stringValue"], "evt_tools");
                let output_attr = span_attr(span, "traceloop.entity.output").unwrap();
                assert!(output_attr["stringValue"]
                    .as_str()
                    .unwrap_or("")
                    .contains("sunny"));
            }
            "coffee_search" => {
                found_coffee = true;
                let dur = span_attr(span, "traceloop.entity.duration_ms").unwrap();
                assert_eq!(dur["intValue"], "250");
                let convo = span_attr(span, "traceloop.association.properties.convo_id").unwrap();
                assert_eq!(convo["stringValue"], "conv-123");
            }
            "park_check" => {
                found_park = true;
                let output_attr = span_attr(span, "traceloop.entity.output").unwrap();
                assert!(output_attr["stringValue"]
                    .as_str()
                    .unwrap_or("")
                    .contains("yes"));
            }
            _ => {}
        }
    }

    assert!(
        found_weather && found_coffee && found_reasoning && found_park,
        "missing expected spans: {:#?}",
        all_spans
    );
}

#[tokio::test]
async fn with_span_and_tracer_carry_association_properties() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_span".into(),
            user_id: "user-123".into(),
            input: "hello".into(),
            ..Default::default()
        })
        .await;

    let mut llm_props = BTreeMap::new();
    llm_props.insert("convo_id".into(), json!("conv-123"));
    let interaction_for_child = interaction.clone();
    interaction
        .with_span::<_, _, _, std::io::Error>(
            SpanOptions {
                name: "llm_call".into(),
                properties: llm_props,
                ..Default::default()
            },
            |span| async move {
                let child = interaction_for_child.start_tool_span(
                    "lookup",
                    ToolOptions {
                        parent: Some(span.clone()),
                        input: Some(json!({"query": "coffee"})),
                        ..Default::default()
                    },
                );
                child.set_output(&json!({"winner": "Ritual"}));
                child.end();
                Ok(())
            },
        )
        .await
        .expect("with_span");

    let mut sticky = BTreeMap::new();
    sticky.insert("job_id".into(), json!("batch-123"));
    let tracer = client.tracer(sticky);

    let mut step_props = BTreeMap::new();
    step_props.insert("step".into(), json!("embed"));
    tracer
        .with_span::<_, _, _, std::io::Error>(
            SpanOptions {
                name: "batch_work".into(),
                properties: step_props,
                ..Default::default()
            },
            |span| async move {
                span.set_attributes([Attribute::string("job.kind", "offline")]);
                Ok(())
            },
        )
        .await
        .expect("tracer with_span");

    let mut tool_props = BTreeMap::new();
    tool_props.insert("step".into(), json!("tool"));
    tracer.track_tool(TrackToolOptions {
        name: "batch_lookup".into(),
        input: Some(json!({"query": "weather"})),
        output: Some(json!({"forecast": "sunny"})),
        properties: tool_props,
        duration: Some(Duration::from_millis(125)),
        ..Default::default()
    });

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    assert_eq!(
        all_spans.len(),
        4,
        "expected 4 spans, got {}",
        all_spans.len()
    );

    let mut found_interaction = false;
    let mut found_tracer = false;
    let mut found_tracer_tool = false;
    for span in &all_spans {
        let name = span["name"].as_str().unwrap_or("");
        match name {
            "llm_call" => {
                found_interaction = true;
                let convo = span_attr(span, "traceloop.association.properties.convo_id").unwrap();
                assert_eq!(convo["stringValue"], "conv-123");
                let evt = span_attr(span, "ai.telemetry.metadata.raindrop.eventId").unwrap();
                assert_eq!(evt["stringValue"], "evt_span");
            }
            "batch_work" => {
                found_tracer = true;
                let job = span_attr(span, "traceloop.association.properties.job_id").unwrap();
                assert_eq!(job["stringValue"], "batch-123");
                let step = span_attr(span, "traceloop.association.properties.step").unwrap();
                assert_eq!(step["stringValue"], "embed");
            }
            "batch_lookup" => {
                found_tracer_tool = true;
                let job = span_attr(span, "traceloop.association.properties.job_id").unwrap();
                assert_eq!(job["stringValue"], "batch-123");
                let dur = span_attr(span, "traceloop.entity.duration_ms").unwrap();
                assert_eq!(dur["intValue"], "125");
            }
            _ => {}
        }
    }
    assert!(
        found_interaction && found_tracer && found_tracer_tool,
        "missing spans: {:#?}",
        all_spans
    );
}

#[tokio::test]
async fn manual_span_parent_linkage() {
    // Verifies parent linkage via SpanOptions.parent.
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server).build().expect("build");

    let parent = client.start_span(SpanOptions {
        name: "outer".into(),
        event_id: "evt_parent".into(),
        ..Default::default()
    });

    let child = client.start_span(SpanOptions {
        name: "inner".into(),
        event_id: "evt_parent".into(),
        parent: Some(parent.clone()),
        ..Default::default()
    });
    child.end();
    parent.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    assert_eq!(all_spans.len(), 2);
    let outer = all_spans
        .iter()
        .find(|s| s["name"] == "outer")
        .expect("outer");
    let inner = all_spans
        .iter()
        .find(|s| s["name"] == "inner")
        .expect("inner");
    let outer_span_id = outer["spanId"].as_str().unwrap();
    let inner_parent_id = inner["parentSpanId"].as_str().unwrap();
    assert_eq!(
        outer_span_id, inner_parent_id,
        "child parent != outer span_id"
    );
    let outer_trace_id = outer["traceId"].as_str().unwrap();
    let inner_trace_id = inner["traceId"].as_str().unwrap();
    assert_eq!(
        outer_trace_id, inner_trace_id,
        "child trace_id != outer trace_id"
    );
}

#[tokio::test]
async fn manual_span_end_at_uses_caller_supplied_time() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server).build().expect("build");

    let start = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("start");
    let end = start + Duration::from_secs(2);

    let span = client.start_span(SpanOptions {
        name: "explicit_times".into(),
        event_id: "evt".into(),
        start_time: Some(start),
        ..Default::default()
    });
    span.end_at(Some(end));

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let spans = spans_of(&payload);
    let s = spans
        .iter()
        .find(|s| s["name"] == "explicit_times")
        .unwrap();
    let start_ns = s["startTimeUnixNano"].as_str().unwrap();
    let end_ns = s["endTimeUnixNano"].as_str().unwrap();
    let start_ns: u128 = start_ns.parse().unwrap();
    let end_ns: u128 = end_ns.parse().unwrap();
    assert_eq!(start_ns, 1_700_000_000_u128 * 1_000_000_000);
    assert_eq!(end_ns - start_ns, 2_000_000_000);
}

#[tokio::test]
async fn manual_span_set_error_marks_status() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "failed_op".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    span.set_error("network is on fire");
    span.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let spans = spans_of(&payload);
    assert_eq!(spans[0]["status"]["code"], 2, "ERROR status");
    assert_eq!(spans[0]["status"]["message"], "network is on fire");
}

#[tokio::test]
async fn manual_span_set_attributes_after_end_is_safe() {
    let server = MockServer::start().await;
    let _ = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).build().expect("build");
    let span = client.start_span(SpanOptions {
        name: "post_end".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    span.end();
    // Calling set_attributes after end must not panic.
    span.set_attributes([Attribute::string("late", "noop")]);
    let _ = client.close().await;
}

#[tokio::test]
async fn span_with_operation_id_emits_ai_operation_id_attribute() {
    // SKILL.md rule #25: spans missing `ai.operationId` are silently dropped by the backend
    // ingestion filter. Verify that setting `operation_id` adds the `ai.operationId` attribute.
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "agent.run".into(),
        event_id: "evt_op".into(),
        operation_id: "ai.workflow".into(),
        ..Default::default()
    });
    span.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let spans = spans_of(&payload);
    let op_attr = span_attr(&spans[0], "ai.operationId").expect("ai.operationId attribute");
    assert_eq!(op_attr["stringValue"], "ai.workflow");
}

#[tokio::test]
async fn span_without_operation_id_omits_attribute() {
    // Backward compatibility: when `operation_id` is empty (default), the attribute is not added.
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;

    let client = fast_client_builder(&server).build().expect("build");

    let span = client.start_span(SpanOptions {
        name: "no_op_id".into(),
        event_id: "evt".into(),
        ..Default::default()
    });
    span.end();

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let payload = trace_recorder.requests()[0].json();
    let spans = spans_of(&payload);
    assert!(
        span_attr(&spans[0], "ai.operationId").is_none(),
        "ai.operationId should not be present when operation_id is unset",
    );
}

#[tokio::test]
async fn tool_span_emits_ai_tool_call_operation_id() {
    // start_tool_span and track_tool MUST always set `ai.operationId=ai.toolCall` so tool spans
    // pass the backend ingestion filter (SKILL.md rule #25).
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_tool_op".into(),
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await;

    let tool = interaction.start_tool_span(
        "live_lookup",
        ToolOptions {
            input: Some(json!({"q": "weather"})),
            ..Default::default()
        },
    );
    tool.end();

    interaction.track_tool(TrackToolOptions {
        name: "retro_lookup".into(),
        input: Some(json!({"q": "news"})),
        duration: Some(Duration::from_millis(50)),
        ..Default::default()
    });

    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    assert!(all_spans.len() >= 2, "expected at least two tool spans");

    for span in &all_spans {
        let name = span["name"].as_str().unwrap_or("");
        if name == "live_lookup" || name == "retro_lookup" {
            let op_attr = span_attr(span, "ai.operationId").unwrap_or_else(|| {
                panic!("missing ai.operationId on tool span {}: {:#?}", name, span)
            });
            assert_eq!(
                op_attr["stringValue"], "ai.toolCall",
                "tool span {} must have ai.operationId=ai.toolCall",
                name
            );
        }
    }
}

#[tokio::test]
async fn empty_event_id_is_filled_when_starting_via_interaction() {
    let server = MockServer::start().await;
    let trace_recorder = mount_path(&server, "POST", "/traces").await;
    let _ = mount_path(&server, "POST", "/events/track_partial").await;

    let client = fast_client_builder(&server).build().expect("build");
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt_link".into(),
            user_id: "user-123".into(),
            ..Default::default()
        })
        .await;

    let span = interaction.start_span(SpanOptions {
        name: "auto_linked".into(),
        ..Default::default()
    });
    span.end();
    client.flush().await.expect("flush");
    client.close().await.expect("close");

    let mut all_spans = Vec::new();
    for req in trace_recorder.requests() {
        all_spans.extend(spans_of(&req.json()));
    }
    let s = all_spans
        .iter()
        .find(|s| s["name"] == "auto_linked")
        .unwrap();
    let evt_id_attr = span_attr(s, "ai.telemetry.metadata.raindrop.eventId").unwrap();
    assert_eq!(evt_id_attr["stringValue"], "evt_link");
}
