//! The `mask` client option: content passes through the caller's function
//! just before it is sent; it fails closed and never fails the caller.

mod common;

use serde_json::{json, Value};
use std::collections::BTreeMap;
use wiremock::MockServer;

use raindrop::{
    AiEvent, Attachment, BeginOptions, Event, FinishOptions, MaskContext, MaskError, MaskKind,
    PatchOptions, ResumeOptions, Signal, TrackToolOptions, User, MASK_FAILED,
};

use crate::common::{fast_client_builder, items, mount_path, span_attr, spans_of};

/// The conformance harness's `tag` test mask.
fn tag(value: Value, ctx: &MaskContext) -> Result<Value, MaskError> {
    Ok(match value {
        Value::String(_) => Value::String(format!("<MASKED {} {}>", ctx.kind, ctx.field)),
        Value::Array(_) => json!([]),
        _ => json!({ "masked": format!("{} {}", ctx.kind, ctx.field) }),
    })
}

fn props() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("plan".to_string(), json!("pro")),
        ("note".to_string(), json!("reach me at Jane Doe")),
    ])
}

fn attachment() -> Vec<Attachment> {
    vec![Attachment {
        kind: "text".into(),
        role: "input".into(),
        value: "Jane Doe".into(),
        ..Default::default()
    }]
}

#[tokio::test]
async fn track_ai_and_track_event_are_masked() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;
    let client = fast_client_builder(&server).mask(tag).build().unwrap();

    client
        .track_ai(AiEvent {
            event_id: "evt-ai".into(),
            user_id: "u1".into(),
            event: "chat".into(),
            input: "Contact me at Jane Doe".into(),
            output: "Card 4111 1111 1111 1111".into(),
            model: "mock-gpt".into(),
            convo_id: "c1".into(),
            properties: props(),
            attachments: attachment(),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .track_event(Event {
            event_id: "evt-plain".into(),
            user_id: "u1".into(),
            event: "signup".into(),
            properties: props(),
            ..Default::default()
        })
        .await
        .unwrap();
    client.flush().await.unwrap();

    let sent = items(&recorder);
    let ai = &sent[0];
    assert_eq!(ai["event_id"], "evt-ai");
    assert_eq!(ai["user_id"], "u1");
    assert_eq!(ai["event"], "chat");
    assert_eq!(ai["ai_data"]["input"], "<MASKED event ai_data.input>");
    assert_eq!(ai["ai_data"]["output"], "<MASKED event ai_data.output>");
    assert_eq!(ai["ai_data"]["model"], "mock-gpt");
    assert_eq!(ai["ai_data"]["convo_id"], "c1");
    assert_eq!(ai["properties"]["masked"], "event properties");
    assert!(ai["properties"].get("note").is_none());
    assert_eq!(
        ai["properties"]["$context"]["library"]["name"],
        "raindrop-rust"
    );
    assert_eq!(ai["attachments"], json!([]));
    let plain = &sent[1];
    assert_eq!(plain["event"], "signup");
    assert_eq!(plain["properties"]["masked"], "event properties");
    assert!(plain.get("ai_data").is_none());
    let _ = client.close().await;
}

#[tokio::test]
async fn every_partial_request_is_masked() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    let suffix = |value: Value, _: &MaskContext| -> Result<Value, MaskError> {
        Ok(match value {
            Value::String(s) => Value::String(format!("{s} [masked]")),
            Value::Object(mut map) => {
                map.remove("note");
                Value::Object(map)
            }
            other => other,
        })
    };
    let client = fast_client_builder(&server).mask(suffix).build().unwrap();

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt-partial".into(),
            user_id: "u1".into(),
            event: "chat".into(),
            input: "hello jane".into(),
            properties: props(),
            ..Default::default()
        })
        .await;
    client.flush().await.unwrap();
    interaction
        .patch(PatchOptions {
            input: "hello again".into(),
            properties: props(),
            ..Default::default()
        })
        .await
        .unwrap();
    client.flush().await.unwrap();
    interaction
        .finish(FinishOptions {
            output: "bye jane".into(),
            properties: props(),
            ..Default::default()
        })
        .await
        .unwrap();
    client.flush().await.unwrap();

    let resumed = client.resume_interaction_with("evt-resumed", ResumeOptions::default());
    resumed
        .finish(FinishOptions {
            output: "resumed output".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    client.flush().await.unwrap();

    let sent: Vec<Value> = recorder.requests().iter().map(|c| c.json()).collect();
    assert!(
        sent.len() >= 4,
        "begin, patch, finish and the resumed finish: {sent:?}"
    );
    for body in &sent {
        if let Some(input) = body["ai_data"]["input"].as_str() {
            assert!(input.ends_with(" [masked]"), "{input}");
            assert!(
                !input.ends_with(" [masked] [masked]"),
                "masked once: {input}"
            );
        }
        if let Some(output) = body["ai_data"]["output"].as_str() {
            assert!(output.ends_with(" [masked]"), "{output}");
        }
        assert!(body["properties"].get("note").is_none(), "{body}");
        assert!(body["properties"].get("$context").is_some());
    }
    let finish = sent
        .iter()
        .find(|b| b["event_id"] == "evt-partial" && b["is_pending"] == false)
        .unwrap();
    assert_eq!(finish["ai_data"]["output"], "bye jane [masked]");
    assert_eq!(finish["properties"]["plan"], "pro");
    let resumed = sent
        .iter()
        .find(|b| b["event_id"] == "evt-resumed")
        .unwrap();
    assert_eq!(resumed["ai_data"]["output"], "resumed output [masked]");
    let _ = client.close().await;
}

#[tokio::test]
async fn a_restored_partial_update_is_not_masked_twice() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track_partial").await;
    // The first attempt fails for good (retries exhausted); the next flush resends.
    for _ in 0..3 {
        recorder
            .sequence
            .lock()
            .unwrap()
            .push(wiremock::ResponseTemplate::new(500));
    }
    let client = fast_client_builder(&server)
        .max_attempts(3)
        .mask(|value, _| {
            Ok(match value {
                Value::String(s) => Value::String(format!("{s}!")),
                other => other,
            })
        })
        .build()
        .unwrap();
    let interaction = client
        .begin(BeginOptions {
            event_id: "evt-retry".into(),
            user_id: "u1".into(),
            event: "chat".into(),
            input: "hi".into(),
            ..Default::default()
        })
        .await;
    let _ = client.flush().await;
    client.flush().await.unwrap();
    let bodies: Vec<Value> = recorder.requests().iter().map(|c| c.json()).collect();
    assert!(bodies.len() >= 2);
    assert!(
        bodies.iter().all(|b| b["ai_data"]["input"] == "hi!"),
        "{bodies:?}"
    );
    interaction.finish(FinishOptions::default()).await.unwrap();
    let _ = client.close().await;
}

#[tokio::test]
async fn signal_and_identify_are_masked() {
    let server = MockServer::start().await;
    let signals = mount_path(&server, "POST", "/signals/track").await;
    let identify = mount_path(&server, "POST", "/users/identify").await;
    let client = fast_client_builder(&server).mask(tag).build().unwrap();

    client
        .track_signal(Signal {
            event_id: "evt-signal".into(),
            name: "feedback".into(),
            comment: "reach me at Jane Doe".into(),
            after: "call 555-123-4567".into(),
            properties: BTreeMap::from([("note".to_string(), json!("Bob Roe"))]),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .identify(User {
            user_id: "Jane Doe".into(),
            traits: BTreeMap::from([("email".to_string(), json!("Jane Doe"))]),
        })
        .await
        .unwrap();
    client.flush().await.unwrap();

    let signal = &items(&signals)[0];
    assert_eq!(signal["event_id"], "evt-signal");
    assert_eq!(signal["signal_name"], "feedback");
    assert_eq!(
        signal["properties"],
        json!({ "masked": "signal properties" })
    );
    assert!(signal.get("comment").is_none());
    let user = identify.requests()[0].json();
    assert_eq!(user["user_id"], "Jane Doe");
    assert_eq!(user["traits"], json!({ "masked": "identify traits" }));
    let _ = client.close().await;
}

#[tokio::test]
async fn retroactive_span_content_and_status_are_masked() {
    let server = MockServer::start().await;
    let _events = mount_path(&server, "POST", "/events/track_partial").await;
    let traces = mount_path(&server, "POST", "/traces").await;
    let client = fast_client_builder(&server).mask(tag).build().unwrap();

    let interaction = client
        .begin(BeginOptions {
            event_id: "evt-span".into(),
            user_id: "u1".into(),
            event: "chat".into(),
            ..Default::default()
        })
        .await;
    interaction.track_tool(TrackToolOptions {
        name: "lookup".into(),
        input: Some(json!({ "email": "Jane Doe" })),
        output: Some(json!("found jane")),
        error: Some("failed for Jane Doe".into()),
        ..Default::default()
    });
    interaction.finish(FinishOptions::default()).await.unwrap();
    client.flush().await.unwrap();

    let spans: Vec<Value> = traces
        .requests()
        .iter()
        .flat_map(|r| spans_of(&r.json()))
        .collect();
    let span = spans
        .iter()
        .find(|s| s["name"] == "lookup")
        .expect("tool span");
    let attr = |key: &str| span_attr(span, key).and_then(|v| v["stringValue"].as_str());
    assert_eq!(
        attr("traceloop.entity.input"),
        Some("<MASKED span traceloop.entity.input>")
    );
    assert_eq!(
        attr("traceloop.entity.output"),
        Some("<MASKED span traceloop.entity.output>")
    );
    assert_eq!(attr("traceloop.entity.name"), Some("lookup"));
    assert_eq!(
        attr("ai.telemetry.metadata.raindrop.eventId"),
        Some("evt-span")
    );
    assert_eq!(attr("ai.operationId"), Some("ai.toolCall"));
    assert_eq!(span["status"]["message"], "<MASKED span status.message>");
    let _ = client.close().await;
}

#[tokio::test]
async fn a_failing_mask_fails_closed_and_never_fails_the_caller() {
    for (name, mask) in [
        (
            "error",
            Box::new(|_: Value, _: &MaskContext| -> Result<Value, MaskError> {
                Err("leaks Jane Doe".into())
            })
                as Box<dyn Fn(Value, &MaskContext) -> Result<Value, MaskError> + Send + Sync>,
        ),
        (
            "panic",
            Box::new(|_: Value, _: &MaskContext| -> Result<Value, MaskError> {
                panic!("mask panicked on purpose")
            }),
        ),
        (
            "wrong type",
            Box::new(|_: Value, _: &MaskContext| -> Result<Value, MaskError> { Ok(json!(7)) }),
        ),
    ] {
        let server = MockServer::start().await;
        let events = mount_path(&server, "POST", "/events/track").await;
        let identify = mount_path(&server, "POST", "/users/identify").await;
        let client = fast_client_builder(&server).mask(mask).build().unwrap();
        client
            .track_ai(AiEvent {
                event_id: "evt-fail".into(),
                user_id: "u1".into(),
                event: "chat".into(),
                input: "Contact me at Jane Doe".into(),
                output: "Card 4111 1111 1111 1111".into(),
                model: "mock-gpt".into(),
                properties: props(),
                attachments: attachment(),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("{name}: track_ai failed: {e}"));
        client
            .identify(User {
                user_id: "u1".into(),
                traits: BTreeMap::from([("email".to_string(), json!("Jane Doe"))]),
            })
            .await
            .unwrap();
        client.flush().await.unwrap();

        let event = &items(&events)[0];
        assert_eq!(event["event_id"], "evt-fail", "{name}");
        assert_eq!(event["user_id"], "u1");
        assert_eq!(event["ai_data"]["input"], MASK_FAILED, "{name}");
        assert_eq!(event["ai_data"]["output"], MASK_FAILED, "{name}");
        assert_eq!(event["ai_data"]["model"], "mock-gpt");
        assert_eq!(event["properties"]["error"], MASK_FAILED, "{name}");
        assert!(event["properties"].get("note").is_none());
        assert!(event["properties"].get("$context").is_some());
        assert_eq!(event["attachments"], json!([]), "{name}");
        assert_eq!(
            identify.requests()[0].json()["traits"],
            json!({ "error": MASK_FAILED }),
            "{name}"
        );
        let _ = client.close().await;
    }
}

#[tokio::test]
async fn without_a_mask_nothing_changes() {
    let server = MockServer::start().await;
    let recorder = mount_path(&server, "POST", "/events/track").await;
    let client = fast_client_builder(&server).build().unwrap();
    client
        .track_ai(AiEvent {
            user_id: "u1".into(),
            event: "chat".into(),
            input: "Contact me at Jane Doe".into(),
            properties: props(),
            ..Default::default()
        })
        .await
        .unwrap();
    client.flush().await.unwrap();
    let event = &items(&recorder)[0];
    assert_eq!(event["ai_data"]["input"], "Contact me at Jane Doe");
    assert_eq!(event["properties"]["note"], "reach me at Jane Doe");
    let _ = client.close().await;
}

#[test]
fn mask_context_is_constructible_for_unit_tests() {
    let ctx = MaskContext::new(MaskKind::Event, "ai_data.input");
    assert_eq!(
        tag(json!("x"), &ctx).unwrap(),
        json!("<MASKED event ai_data.input>")
    );
    assert_eq!(MaskKind::Identify.to_string(), "identify");
}
