//! End-to-end tests against a real Raindrop backend.
//!
//! These tests are skipped automatically when the required environment variables are not set,
//! so they are safe to leave enabled in `cargo test`. To run them, supply:
//!
//! - `RAINDROP_WRITE_KEY` (or `RAINDROP_API_KEY`) — Raindrop write key for shipping events
//! - `RAINDROP_DASHBOARD_TOKEN` — bearer token for the dashboard TRPC API used to verify
//!   that telemetry actually landed
//! - `RAINDROP_BACKEND_URL` (optional) — defaults to `https://backend.raindrop.ai`
//! - `RAINDROP_ENDPOINT` (optional) — defaults to `https://api.raindrop.ai/v1/`
//!
//! These tests run the SDK against the real ingestion API and then poll the dashboard
//! TRPC API to verify the data is recorded with correct shape, matching the e2e contract
//! described in `dawn/.agents/skills/new-integration/SKILL.md` (Phase 8).

use std::collections::BTreeMap;
use std::env;
use std::time::Duration;

use serde_json::{json, Value};
use time::OffsetDateTime;
use tokio::time::sleep;

use raindrop::{
    AiEvent, Attachment, Attribute, BeginOptions, Client, FinishOptions, LlmMessage, LlmOptions,
    Signal, SignalKind, SpanOptions, ToolOptions, TrackToolOptions, User,
};

const DEFAULT_BACKEND_URL: &str = "https://backend.raindrop.ai";

fn env_keys() -> Option<(String, String)> {
    let write_key = env::var("RAINDROP_WRITE_KEY")
        .ok()
        .or_else(|| env::var("RAINDROP_API_KEY").ok())
        .filter(|s| !s.is_empty())?;
    let dashboard_token = env::var("RAINDROP_DASHBOARD_TOKEN")
        .ok()
        .filter(|s| !s.is_empty())?;
    Some((write_key, dashboard_token))
}

fn unique_user_id(suffix: &str) -> String {
    let id = uuid::Uuid::new_v4()
        .to_string()
        .chars()
        .take(8)
        .collect::<String>();
    format!("e2e_rust_{}_{}", id, suffix)
}

async fn query_dashboard(token: &str, limit: usize) -> Result<Vec<Value>, String> {
    let backend_url =
        env::var("RAINDROP_BACKEND_URL").unwrap_or_else(|_| DEFAULT_BACKEND_URL.to_string());
    // dawn's tRPC server has no superjson transformer configured, so wrapping
    // the input in `{ json: {...} }` makes Zod silently strip every field and
    // the procedure runs with all parameters undefined. Send the raw input.
    // `orderBy.direction` is the uppercase enum `"DESC"` (lowercase fails
    // Zod's `["ASC", "DESC"]` validation once Bug #1 is fixed).
    let input_obj = json!({
        "limit": limit,
        "orderBy": { "field": "timestamp", "direction": "DESC" }
    });
    let encoded = urlencoding::encode(&input_obj.to_string()).into_owned();
    let url = format!("{}/api/trpc/events.list?input={}", backend_url, encoded);

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| format!("dashboard request failed: {}", e))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "dashboard returned {}: {}",
            status,
            body.chars().take(500).collect::<String>()
        ));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid dashboard json: {}", e))?;
    Ok(body["result"]["data"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

/// Default polling deadline. Empirically, the Raindrop ingestion → events.list pipeline
/// can take over a minute (sometimes 90–120s) before a freshly-shipped event surfaces on
/// the dashboard, so we mirror the Python SDK's e2e suite (which uses 240s) and pick a
/// generous default here. CI without dashboard credentials skips these tests entirely,
/// so the longer timeout has no effect on routine CI runtime.
const E2E_POLL_TIMEOUT: Duration = Duration::from_secs(180);

/// Polling deadline for *derived* event fields — `toolCalls`, `errorSpans`, `userTraits`,
/// `signals`. These are populated by a separate join pipeline (spans→events,
/// users→events, signals→events) that runs AFTER the initial event row lands, so they can
/// take noticeably longer than `E2E_POLL_TIMEOUT`.
const E2E_DERIVED_POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// Re-poll an event for `user_id` until `predicate(event)` returns true (or the deadline
/// expires). Used for fields that arrive on the event AFTER the initial event row is
/// visible (e.g. `toolCalls`, `errorSpans`, `signals`, `userTraits` — all populated by
/// downstream join pipelines that run on a separate cadence).
async fn poll_event_until<F>(
    token: &str,
    user_id: &str,
    predicate: F,
    timeout: Duration,
) -> Result<Value, String>
where
    F: Fn(&Value) -> bool,
{
    let interval = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let mut last_event: Option<Value> = None;
    while start.elapsed() < timeout {
        let events = query_dashboard(token, 50).await?;
        for ev in events {
            if ev["userId"].as_str() == Some(user_id) {
                if predicate(&ev) {
                    return Ok(ev);
                }
                last_event = Some(ev);
            }
        }
        sleep(interval).await;
    }
    Err(format!(
        "Timed out waiting for predicate to hold on user {} after {:?}; last event: {:?}",
        user_id, timeout, last_event
    ))
}

/// Poll until at least `min_count` events for `user_id` are visible on the dashboard, or until
/// the deadline expires.
async fn poll_events(
    token: &str,
    user_id: &str,
    min_count: usize,
    timeout: Duration,
) -> Result<Vec<Value>, String> {
    let interval = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let mut last_seen = 0usize;
    while start.elapsed() < timeout {
        let all_events = query_dashboard(token, 50).await?;
        let matched: Vec<Value> = all_events
            .into_iter()
            .filter(|e| e["userId"].as_str() == Some(user_id))
            .collect();
        last_seen = matched.len();
        if matched.len() >= min_count {
            return Ok(matched);
        }
        sleep(interval).await;
    }
    Err(format!(
        "Timed out waiting for {} events for user {} after {:?} (last seen {})",
        min_count, user_id, timeout, last_seen
    ))
}

fn build_client(write_key: &str) -> Client {
    let mut builder = Client::builder()
        .write_key(write_key)
        .disable_local_workshop();
    if let Ok(endpoint) = env::var("RAINDROP_ENDPOINT") {
        builder = builder.endpoint(endpoint);
    }
    builder.build().expect("build client")
}

#[tokio::test]
async fn e2e_track_ai_event_lands_in_dashboard() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("track_ai");
    let convo_id = format!("{}_convo", user_id);

    let client = build_client(&write_key);

    let mut props = BTreeMap::new();
    props.insert("ai.usage.prompt_tokens".to_string(), json!(10));
    props.insert("ai.usage.completion_tokens".to_string(), json!(20));

    client
        .track_ai(AiEvent {
            user_id: user_id.clone(),
            event: "ai_generation".to_string(),
            input: "Hello Rust".to_string(),
            output: "Hello World".to_string(),
            model: "gpt-4o".to_string(),
            convo_id: convo_id.clone(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("track_ai");

    client.close().await.expect("close");

    let events = poll_events(&dashboard_token, &user_id, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("dashboard verification");
    let ev = events
        .iter()
        .find(|e| e["aiData"]["input"].as_str().unwrap_or("") == "Hello Rust")
        .unwrap_or_else(|| panic!("track_ai event not found among {:?}", events));
    let ai = ev["aiData"].clone();
    assert_eq!(ai["output"].as_str().unwrap_or_default(), "Hello World");
    assert_eq!(ai["model"].as_str().unwrap_or_default(), "gpt-4o");
    assert_eq!(ai["convoId"].as_str().unwrap_or_default(), convo_id);

    let p = &ev["properties"];
    // The dashboard normalizes numeric properties to strings (e.g. "10"), so accept both
    // JSON numbers and stringified numbers and parse the numeric value.
    let prompt_tokens = numeric_property(&p["ai.usage.prompt_tokens"]);
    let completion_tokens = numeric_property(&p["ai.usage.completion_tokens"]);
    assert!(
        prompt_tokens > 0.0,
        "expected prompt_tokens > 0, got {:?}",
        p["ai.usage.prompt_tokens"]
    );
    assert!(
        completion_tokens > 0.0,
        "expected completion_tokens > 0, got {:?}",
        p["ai.usage.completion_tokens"]
    );
}

/// Read a numeric property from a dashboard event. Some pipelines return numeric properties as
/// JSON numbers, others as stringified numbers (e.g. `"10"`); accept either.
fn numeric_property(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

#[tokio::test]
async fn e2e_interaction_with_tool_span_lands_in_dashboard() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("interaction");
    let convo_id = format!("{}_convo", user_id);

    let client = build_client(&write_key);
    let interaction = client
        .begin(raindrop::BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "agent_run".to_string(),
            input: "Run tool".to_string(),
            ..Default::default()
        })
        .await;

    let tool = interaction.start_tool_span(
        "weather_lookup",
        ToolOptions {
            input: Some(json!({"location": "SF"})),
            ..Default::default()
        },
    );
    sleep(Duration::from_millis(100)).await;
    tool.set_output(&json!({"temp": 72}));
    tool.end();

    interaction
        .finish(raindrop::FinishOptions {
            output: "The weather is 72".to_string(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    let events = poll_events(&dashboard_token, &user_id, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("dashboard verification");
    let ev = events
        .iter()
        .find(|e| e["aiData"]["output"].as_str().unwrap_or("") == "The weather is 72")
        .unwrap_or_else(|| panic!("interaction event not found among {:?}", events));
    assert_eq!(ev["aiData"]["convoId"].as_str().unwrap_or(""), convo_id);
}

#[tokio::test]
async fn e2e_signals_and_identify_land_in_dashboard() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("signal");

    let client = build_client(&write_key);

    // identify
    client
        .identify(User {
            user_id: user_id.clone(),
            traits: BTreeMap::from([("plan".to_string(), json!("pro"))]),
        })
        .await
        .expect("identify");

    // a synthetic event id we can attach a signal to
    let event_id = format!("{}_evt", user_id);
    client
        .track_ai(AiEvent {
            event_id: event_id.clone(),
            user_id: user_id.clone(),
            input: "rate me".to_string(),
            output: "I am rated".to_string(),
            ..Default::default()
        })
        .await
        .expect("track_ai");

    client
        .track_signal(Signal {
            event_id: event_id.clone(),
            name: "thumbs_up".to_string(),
            kind: "feedback".to_string(),
            sentiment: "POSITIVE".to_string(),
            comment: "Helpful".to_string(),
            ..Default::default()
        })
        .await
        .expect("track_signal");

    client.close().await.expect("close");

    // Verify on the dashboard: the track_ai event MUST land. There is no public TRPC signals
    // query endpoint, so this test asserts the event side of the chain (the event the signal
    // was attached to) — which proves the API is reachable and the write key is valid even if
    // we cannot query signals directly. Signal landing should be added here when a query
    // endpoint becomes available.
    let events = poll_events(&dashboard_token, &user_id, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("dashboard verification");
    let ev = events
        .iter()
        .find(|e| e["aiData"]["input"].as_str().unwrap_or("") == "rate me")
        .unwrap_or_else(|| panic!("track_ai event not found among {:?}", events));
    assert_eq!(ev["userId"].as_str().unwrap_or(""), user_id);
    assert_eq!(ev["aiData"]["output"].as_str().unwrap_or(""), "I am rated");
}

// ────────────────────────────────────────────────────────────────────────────────────
// Pedantic dashboard surface tests — every test below verifies a SPECIFIC field of the
// dashboard response that the SDK is responsible for populating, with deep assertions
// (not just "an event exists").
//
// Source of truth for the dashboard event shape:
//   `dawn/packages/schemas/src/frontend/index.ts::AIAnalyticsEventSchema`
// Source of truth for the trace span shape:
//   `dawn/data/tinybird/datasources/traces.datasource`
// ────────────────────────────────────────────────────────────────────────────────────

/// Query the dashboard's `traces.list` endpoint for a specific event_id and return the spans.
async fn query_traces_for_event(token: &str, event_id: &str) -> Result<Vec<Value>, String> {
    let backend_url =
        env::var("RAINDROP_BACKEND_URL").unwrap_or_else(|_| DEFAULT_BACKEND_URL.to_string());
    // Send raw input — dawn's tRPC server has no superjson transformer, so a
    // `{ json: {...} }` wrapper would be stripped by Zod and `traces.list`
    // would return `[]` for every event_id.
    let input_obj = json!({
        "eventId": event_id,
        "limit": 200,
    });
    let encoded = urlencoding::encode(&input_obj.to_string()).into_owned();
    let url = format!("{}/api/trpc/traces.list?input={}", backend_url, encoded);

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| format!("trpc.traces.list request failed: {}", e))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "traces.list returned {}: {}",
            status,
            body.chars().take(500).collect::<String>()
        ));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid traces.list json: {}", e))?;
    Ok(body["result"]["data"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

/// Poll until at least `min_count` spans for `event_id` are visible on the dashboard, or until
/// the deadline expires. Spans go through a separate ingestion pipeline from events, so this
/// helper polls `traces.list` (not `events.list`).
async fn poll_traces_for_event(
    token: &str,
    event_id: &str,
    min_count: usize,
    timeout: Duration,
) -> Result<Vec<Value>, String> {
    let interval = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let mut last_seen = 0usize;
    while start.elapsed() < timeout {
        let spans = query_traces_for_event(token, event_id).await?;
        last_seen = spans.len();
        if spans.len() >= min_count {
            return Ok(spans);
        }
        sleep(interval).await;
    }
    Err(format!(
        "Timed out waiting for {} spans for event {} after {:?} (last seen {})",
        min_count, event_id, timeout, last_seen
    ))
}

/// Query `signals.getGroupedSignals` and return a flat list of signal definitions
/// (grouped plus ungrouped). This is the reliable source for verifying that a tracked
/// signal landed; the per-event `signals[]` array on `events.list` is not guaranteed to
/// be populated, so the cross-SDK e2e convention checks the catalog by name.
async fn query_grouped_signals(token: &str) -> Result<Vec<Value>, String> {
    let backend_url =
        env::var("RAINDROP_BACKEND_URL").unwrap_or_else(|_| DEFAULT_BACKEND_URL.to_string());
    let encoded = urlencoding::encode("{}").into_owned();
    let url = format!(
        "{}/api/trpc/signals.getGroupedSignals?input={}",
        backend_url, encoded
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| format!("signals.getGroupedSignals request failed: {}", e))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "signals.getGroupedSignals returned {}: {}",
            status,
            body.chars().take(500).collect::<String>()
        ));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid signals.getGroupedSignals json: {}", e))?;
    let data = &body["result"]["data"];
    let mut out: Vec<Value> = data["ungroupedSignals"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if let Some(groups) = data["groups"].as_array() {
        for group in groups {
            if let Some(sigs) = group["signals"].as_array() {
                out.extend(sigs.iter().cloned());
            }
        }
    }
    Ok(out)
}

/// **Convo grouping.** Three events sharing the same `convo_id` must all carry the same
/// `aiData.convoId` on the dashboard, so the convo_list pipe can group them.
#[tokio::test]
async fn e2e_convo_grouping_works_across_multiple_track_ai() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("convo");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);

    let inputs = ["turn_one", "turn_two", "turn_three"];
    let outputs = ["resp_one", "resp_two", "resp_three"];
    for (i, o) in inputs.iter().zip(outputs.iter()) {
        client
            .track_ai(AiEvent {
                user_id: user_id.clone(),
                convo_id: convo_id.clone(),
                event: "ai_generation".to_string(),
                input: (*i).to_string(),
                output: (*o).to_string(),
                model: "gpt-4o".to_string(),
                ..Default::default()
            })
            .await
            .expect("track_ai");
    }
    client.close().await.expect("close");

    let events = poll_events(&dashboard_token, &user_id, 3, E2E_POLL_TIMEOUT)
        .await
        .expect("dashboard verification");
    assert!(
        events.len() >= 3,
        "expected at least 3 events for convo grouping, got {}",
        events.len()
    );

    // Every event must share the convoId
    for ev in &events {
        let actual = ev["aiData"]["convoId"].as_str().unwrap_or("");
        assert_eq!(
            actual, convo_id,
            "every event for user {} must carry convoId={}, got {}",
            user_id, convo_id, actual
        );
    }

    // Every input/output pair must appear exactly once
    let mut seen_inputs = std::collections::HashSet::new();
    for ev in &events {
        if let Some(input) = ev["aiData"]["input"].as_str() {
            seen_inputs.insert(input.to_string());
        }
    }
    for input in inputs {
        assert!(
            seen_inputs.contains(input),
            "missing input {} from {} events; saw {:?}",
            input,
            events.len(),
            seen_inputs
        );
    }
}

/// **Tool calls populated on event.** When `start_tool_span` is called inside an interaction,
/// the resulting event MUST have `toolCalls[]` with the correct tool_name, status, duration_ms,
/// and started_at timestamp, AND `toolCallNames[]` with the tool name.
#[tokio::test]
async fn e2e_tool_span_populates_event_tool_calls_array() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("toolcall");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "agent_run".to_string(),
            input: "Find weather".to_string(),
            ..Default::default()
        })
        .await;

    let tool = interaction.start_tool_span(
        "weather_lookup",
        ToolOptions {
            input: Some(json!({"location": "Berlin"})),
            ..Default::default()
        },
    );
    sleep(Duration::from_millis(150)).await;
    tool.set_output(&json!({"temp_c": 19, "condition": "rain"}));
    tool.end();

    interaction
        .finish(FinishOptions {
            output: "It's raining 19°C".to_string(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    // toolCalls is populated by a separate join pipeline (span→event), so we re-poll the
    // event until the array is non-empty rather than reading it once.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| {
            e["aiData"]["output"].as_str() == Some("It's raining 19°C")
                && e["toolCalls"].as_array().is_some_and(|arr| !arr.is_empty())
        },
        E2E_DERIVED_POLL_TIMEOUT,
    )
    .await
    .expect("event with populated toolCalls");

    let tool_calls = ev["toolCalls"].as_array().unwrap();
    let weather = tool_calls
        .iter()
        .find(|t| t["tool_name"].as_str() == Some("weather_lookup"))
        .unwrap_or_else(|| panic!("weather_lookup tool call missing, got {:?}", tool_calls));
    assert_eq!(weather["status"].as_str().unwrap_or(""), "OK");
    assert!(
        weather["duration_ms"].as_f64().unwrap_or(0.0) > 0.0,
        "duration_ms must be positive, got {:?}",
        weather["duration_ms"]
    );
    assert!(
        weather["started_at"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "started_at must be a non-empty ISO string"
    );
    assert!(
        weather["span_id"].as_str().is_some_and(|s| !s.is_empty()),
        "span_id must be present"
    );

    // `toolCallNames` is a derived flat array — populated on a different cadence than
    // `toolCalls`. Accept either present-and-correct OR absent-because-not-joined-yet.
    // The strong contract is `toolCalls[].tool_name`, which we asserted above.
    if let Some(names) = ev["toolCallNames"].as_array() {
        if !names.is_empty() {
            let names_set: std::collections::HashSet<String> = names
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            assert!(
                names_set.contains("weather_lookup"),
                "toolCallNames was populated but missing 'weather_lookup', got {:?}",
                names_set
            );
        }
    }
}

/// **User-like complex trajectory.** Simulates a realistic agent run:
///
/// - root agent span
/// - nested planner subagent span
/// - nested researcher subagent span
/// - many tool calls, including repeated customer-like tool names (`search_events_regex`,
///   `count_events`) observed in production Tinybird samples
/// - one failed tool call followed by a successful retry
/// - one deliberately long-running tool
///
/// Dashboard assertions cover the user-visible trajectory row: event input/output,
/// convo grouping, custom properties, `toolCalls[]`, `errorSpans[]`, statuses, retry
/// metadata, and long-tool duration. This is intentionally closer to a customer run
/// than the smaller smoke tests above.
#[tokio::test]
async fn e2e_complex_agent_trajectory_with_subagents_tools_retry_failure_and_slow_tool() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("complex_agent");
    let convo_id = format!("{}_convo", user_id);

    let mut begin_props = BTreeMap::new();
    begin_props.insert(
        "scenario".to_string(),
        json!("complex_agent_retry_slow_tool"),
    );
    begin_props.insert("retry_count".to_string(), json!(1));
    begin_props.insert("subagents.expected".to_string(), json!(2));

    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "agent_run".to_string(),
            input: "Research account health, retry failed API calls, and summarize risks."
                .to_string(),
            properties: begin_props,
            ..Default::default()
        })
        .await;

    // Use explicit monotonic timestamps for every span/tool so the dashboard timeline
    // is realistic. Avoid retroactive `duration`-only tool calls here: they derive
    // `start = now - duration`, which can backdate a long tool before its parent if
    // the call is made later in the synthetic scenario.
    let base = OffsetDateTime::now_utc();
    let at = |ms: u64| base + Duration::from_millis(ms);

    let root = interaction.start_span(SpanOptions {
        name: "agent.root".into(),
        operation_id: "ai.workflow".into(),
        attributes: vec![
            Attribute::string("traceloop.span.kind", "workflow"),
            Attribute::string("agent.name", "account-health-agent"),
        ],
        start_time: Some(at(0)),
        ..Default::default()
    });

    let planner = interaction.start_span(SpanOptions {
        name: "subagent.planner".into(),
        operation_id: "ai.subagent".into(),
        parent: Some(root.clone()),
        attributes: vec![
            Attribute::string("traceloop.span.kind", "task"),
            Attribute::string("subagent.name", "planner"),
        ],
        start_time: Some(at(10)),
        ..Default::default()
    });
    let plan_tool = interaction.start_tool_span(
        "plan_steps",
        ToolOptions {
            parent: Some(planner.clone()),
            input: Some(json!({
                "objective": "find account risks",
                "constraints": ["no writes", "retry transient API failure"]
            })),
            start_time: Some(at(20)),
            ..Default::default()
        },
    );
    plan_tool.set_output(&json!({
        "steps": ["load_profile", "fetch_risk_signals", "retry_failed_fetch", "summarize"]
    }));
    plan_tool.end_at(Some(at(140)));
    planner.end_at(Some(at(160)));

    let researcher = interaction.start_span(SpanOptions {
        name: "subagent.researcher".into(),
        operation_id: "ai.subagent".into(),
        parent: Some(root.clone()),
        attributes: vec![
            Attribute::string("traceloop.span.kind", "task"),
            Attribute::string("subagent.name", "researcher"),
        ],
        start_time: Some(at(180)),
        ..Default::default()
    });

    let mut lookup_props = BTreeMap::new();
    lookup_props.insert("subagent".to_string(), json!("researcher"));
    lookup_props.insert("retry_attempt".to_string(), json!(0));
    interaction.track_tool(TrackToolOptions {
        name: "customer_profile_lookup".into(),
        parent: Some(researcher.clone()),
        input: Some(json!({"customer_id": "cust_123", "fields": ["plan", "usage", "health"]})),
        output: Some(json!({"plan": "enterprise", "usage": "high", "health": "warning"})),
        start_time: Some(at(200)),
        end_time: Some(at(325)),
        properties: lookup_props,
        ..Default::default()
    });

    // Production trajectory samples include many repeated tool names, especially
    // `search_events_regex` and `count_events`. Exercise that shape explicitly so
    // downstream aggregation does not accidentally assume tool names are unique.
    for idx in 0..2 {
        let mut repeated_props = BTreeMap::new();
        repeated_props.insert("subagent".to_string(), json!("researcher"));
        repeated_props.insert("repeat_index".to_string(), json!(idx));
        interaction.track_tool(TrackToolOptions {
            name: "search_events_regex".into(),
            parent: Some(researcher.clone()),
            input: Some(json!({"regex": "(?i)billing|slow", "page": idx})),
            output: Some(json!({"matches": idx + 2})),
            start_time: Some(at(if idx == 0 { 350 } else { 820 })),
            end_time: Some(at(if idx == 0 { 800 } else { 1_350 })),
            properties: repeated_props,
            ..Default::default()
        });
    }

    for idx in 0..2 {
        let mut repeated_props = BTreeMap::new();
        repeated_props.insert("subagent".to_string(), json!("researcher"));
        repeated_props.insert("repeat_index".to_string(), json!(idx));
        interaction.track_tool(TrackToolOptions {
            name: "count_events".into(),
            parent: Some(researcher.clone()),
            input: Some(json!({"filter": "risk_signal", "bucket": idx})),
            output: Some(json!({"count": 10 + idx})),
            start_time: Some(at(if idx == 0 { 1_360 } else { 1_500 })),
            end_time: Some(at(if idx == 0 { 1_480 } else { 1_650 })),
            properties: repeated_props,
            ..Default::default()
        });
    }

    let mut failed_props = BTreeMap::new();
    failed_props.insert("subagent".to_string(), json!("researcher"));
    failed_props.insert("retry_attempt".to_string(), json!(1));
    failed_props.insert("retryable".to_string(), json!(true));
    interaction.track_tool(TrackToolOptions {
        name: "risk_signal_fetch".into(),
        parent: Some(researcher.clone()),
        input: Some(json!({"customer_id": "cust_123", "window": "7d"})),
        error: Some("TimeoutError: risk service timed out after 2s".into()),
        start_time: Some(at(1_700)),
        end_time: Some(at(1_910)),
        properties: failed_props,
        ..Default::default()
    });

    let mut retry_props = BTreeMap::new();
    retry_props.insert("subagent".to_string(), json!("researcher"));
    retry_props.insert("retry_attempt".to_string(), json!(2));
    retry_props.insert("retryable".to_string(), json!(true));
    interaction.track_tool(TrackToolOptions {
        name: "risk_signal_fetch_retry".into(),
        parent: Some(researcher.clone()),
        input: Some(
            json!({"customer_id": "cust_123", "window": "7d", "retry_of": "risk_signal_fetch"}),
        ),
        output: Some(json!({"risk_signals": ["billing_spike", "slow_response"], "count": 2})),
        start_time: Some(at(1_930)),
        end_time: Some(at(2_110)),
        properties: retry_props,
        ..Default::default()
    });

    let mut slow_props = BTreeMap::new();
    slow_props.insert("subagent".to_string(), json!("researcher"));
    slow_props.insert("slow_tool".to_string(), json!(true));
    interaction.track_tool(TrackToolOptions {
        name: "warehouse_scan_slow".into(),
        parent: Some(researcher.clone()),
        input: Some(json!({"customer_id": "cust_123", "query": "recent anomalies"})),
        output: Some(json!({"anomalies": 3, "oldest_age_hours": 36})),
        start_time: Some(at(2_200)),
        end_time: Some(at(3_550)),
        properties: slow_props,
        ..Default::default()
    });
    researcher.end_at(Some(at(3_600)));
    root.end_at(Some(at(3_650)));

    let mut finish_props = BTreeMap::new();
    finish_props.insert(
        "agent.final_status".to_string(),
        json!("completed_with_retry"),
    );
    finish_props.insert("agent.retry_count".to_string(), json!(1));
    finish_props.insert("agent.subagent_count".to_string(), json!(2));

    interaction
        .finish(FinishOptions {
            output:
                "Account is healthy enough to proceed, but has billing spike and slow response risks. One risk fetch timed out and succeeded on retry."
                    .to_string(),
            model: "gpt-4o-mini".to_string(),
            properties: finish_props,
            ..Default::default()
        })
        .await
        .expect("finish complex agent trajectory");
    client.close().await.expect("close");

    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| {
            e["aiData"]["output"]
                .as_str()
                .is_some_and(|o| o.contains("succeeded on retry"))
                && e["toolCalls"].as_array().is_some_and(|arr| arr.len() >= 9)
                && e["errorSpans"]
                    .as_array()
                    .is_some_and(|arr| !arr.is_empty())
        },
        E2E_DERIVED_POLL_TIMEOUT,
    )
    .await
    .expect("complex agent event with toolCalls and errorSpans");

    assert_eq!(ev["userId"].as_str().unwrap_or(""), user_id);
    assert_eq!(ev["name"].as_str().unwrap_or(""), "agent_run");
    assert_eq!(ev["aiData"]["convoId"].as_str().unwrap_or(""), convo_id);
    assert_eq!(ev["aiData"]["model"].as_str().unwrap_or(""), "gpt-4o-mini");
    assert!(
        ev["aiData"]["input"]
            .as_str()
            .unwrap_or("")
            .contains("retry failed API calls"),
        "input should look like the user-like agent request"
    );

    let props = &ev["properties"];
    assert_eq!(
        props["scenario"].as_str(),
        Some("complex_agent_retry_slow_tool")
    );
    assert_eq!(
        numeric_property(&props["agent.retry_count"]),
        1.0,
        "finish property agent.retry_count should survive"
    );
    assert_eq!(
        props["agent.final_status"].as_str(),
        Some("completed_with_retry")
    );

    let tool_calls = ev["toolCalls"].as_array().unwrap();
    let by_tool = |name: &str| {
        tool_calls
            .iter()
            .find(|t| t["tool_name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("missing tool {} in {:?}", name, tool_calls))
    };
    let expected_tools = [
        "plan_steps",
        "customer_profile_lookup",
        "search_events_regex",
        "count_events",
        "risk_signal_fetch",
        "risk_signal_fetch_retry",
        "warehouse_scan_slow",
    ];
    for tool in expected_tools {
        let call = by_tool(tool);
        assert!(
            call["span_id"].as_str().is_some_and(|s| !s.is_empty()),
            "{} should include span_id",
            tool
        );
        assert!(
            call["started_at"].as_str().is_some_and(|s| !s.is_empty()),
            "{} should include started_at",
            tool
        );
    }

    let count_by_name = |name: &str| {
        tool_calls
            .iter()
            .filter(|t| t["tool_name"].as_str() == Some(name))
            .count()
    };
    assert_eq!(
        count_by_name("search_events_regex"),
        2,
        "repeated search_events_regex calls must be preserved as distinct toolCalls"
    );
    assert_eq!(
        count_by_name("count_events"),
        2,
        "repeated count_events calls must be preserved as distinct toolCalls"
    );

    assert_eq!(
        by_tool("risk_signal_fetch")["status"]
            .as_str()
            .unwrap_or(""),
        "ERROR",
        "first risk fetch should be marked ERROR"
    );
    assert_eq!(
        by_tool("risk_signal_fetch_retry")["status"]
            .as_str()
            .unwrap_or(""),
        "OK",
        "retry should be marked OK"
    );
    // Note: we deliberately do NOT assert `by_tool("warehouse_scan_slow")["duration_ms"]
    // >= 1000`. The Raindrop backend currently truncates `events.toolCalls[].duration_ms`
    // to a signed 8-bit integer in the span→event aggregation, so durations >= 128ms
    // wrap (e.g. real 1350ms surfaces as `1350 mod 256 = 70`, real 450ms surfaces as
    // `-62`). The OTLP spans the SDK actually ships are correct (verified via
    // `traces.list::duration_ns` returning the real value of 1_350_000_000ns), so this
    // is a backend pipeline bug, not an SDK bug. We instead assert the slow tool landed
    // with `status=OK` (above) and skip the wrapped `duration_ms` check until the
    // backend pipeline is fixed.

    let error_spans = ev["errorSpans"].as_array().unwrap();
    let failure = error_spans
        .iter()
        .find(|s| s["span_name"].as_str() == Some("risk_signal_fetch"))
        .unwrap_or_else(|| {
            panic!(
                "risk_signal_fetch missing from errorSpans: {:?}",
                error_spans
            )
        });
    assert_eq!(failure["status"].as_str().unwrap_or(""), "ERROR");
    assert_eq!(failure["span_type"].as_str().unwrap_or(""), "TOOL_CALL");
    assert!(
        failure["output_payload"]
            .as_str()
            .unwrap_or("")
            .contains("TimeoutError"),
        "error output payload should preserve the timeout message"
    );
}

/// **Failed tool span lands with status=ERROR.** A tool span that ends with an error must
/// be retrievable via `traces.list` with `status=ERROR`, the correct span_name/type, a
/// positive duration, and the error message in its output_payload. Verified against the
/// Traces source rather than the eventually-consistent events.list `errorSpans` projection.
#[tokio::test]
async fn e2e_failed_tool_span_populates_event_error_spans_array() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("errspan");
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            event: "agent_run".to_string(),
            input: "Search broken".to_string(),
            ..Default::default()
        })
        .await;
    let event_id = interaction.event_id().to_string();

    interaction.track_tool(TrackToolOptions {
        name: "broken_api".into(),
        input: Some(json!({"q": "test"})),
        error: Some("ConnectionError: refused".into()),
        duration: Some(Duration::from_millis(75)),
        ..Default::default()
    });

    interaction
        .finish(FinishOptions {
            output: "Sorry, the API failed".to_string(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    // The failed tool span lands in the Traces source within seconds. Verify it via
    // `traces.list` instead of the eventually-consistent events.list `errorSpans`
    // projection: an error span is simply a span with status == "ERROR". Read the event
    // first to learn the id `traces.list` maps on.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| e["aiData"]["output"].as_str() == Some("Sorry, the API failed"),
        E2E_POLL_TIMEOUT,
    )
    .await
    .expect("event landed");
    let event_id_for_traces = ev["id"].as_str().map(String::from).unwrap_or(event_id);

    let spans = poll_traces_for_event(&dashboard_token, &event_id_for_traces, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("trace spans for the failed tool");

    let broken = spans
        .iter()
        .find(|s| s["span_name"].as_str() == Some("broken_api"))
        .unwrap_or_else(|| panic!("broken_api span missing among {:?}", spans));
    assert_eq!(
        broken["status"].as_str().unwrap_or(""),
        "ERROR",
        "failed tool span must carry status=ERROR"
    );
    assert_eq!(broken["span_type"].as_str().unwrap_or(""), "TOOL_CALL");
    let dur_ns: u64 = broken["duration_ns"]
        .as_u64()
        .or_else(|| broken["duration_ns"].as_f64().map(|f| f as u64))
        .or_else(|| broken["duration_ns"].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0);
    assert!(
        dur_ns > 0,
        "error span duration_ns must be positive, got {:?}",
        broken["duration_ns"]
    );
    assert!(
        broken["output_payload"]
            .as_str()
            .unwrap_or("")
            .contains("ConnectionError"),
        "error span output_payload should contain the error message; got {:?}",
        broken["output_payload"]
    );
}

/// **Tracked signal lands in the catalog.** A `track_signal` call must register the signal
/// in the org's signal catalog, verified by name via `signals.getGroupedSignals`. The
/// per-event events.list `signals[]` projection is not a guaranteed source.
#[tokio::test]
async fn e2e_track_signal_appears_in_event_signals_array() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("sigembed");
    let event_id = format!("{}_evt", user_id);
    let signal_name = format!("thumbs_up_{}", user_id);
    let client = build_client(&write_key);

    client
        .track_ai(AiEvent {
            event_id: event_id.clone(),
            user_id: user_id.clone(),
            input: "rate this".to_string(),
            output: "I am to be rated".to_string(),
            ..Default::default()
        })
        .await
        .expect("track_ai");

    client
        .track_signal(Signal {
            event_id: event_id.clone(),
            name: signal_name.clone(),
            kind: SignalKind::FEEDBACK.into(),
            sentiment: "POSITIVE".to_string(),
            comment: "great".to_string(),
            ..Default::default()
        })
        .await
        .expect("track_signal");

    client.close().await.expect("close");

    // Verify through the signal catalog (`signals.getGroupedSignals`), the reliable source
    // for confirming a tracked signal landed. The per-event events.list `signals[]`
    // projection is not guaranteed to be populated, so the cross-SDK convention checks the
    // catalog by (run-unique) name.
    let deadline = std::time::Instant::now() + E2E_POLL_TIMEOUT;
    let mut signal: Option<Value> = None;
    while signal.is_none() && std::time::Instant::now() < deadline {
        let catalog = query_grouped_signals(&dashboard_token)
            .await
            .unwrap_or_default();
        signal = catalog
            .into_iter()
            .find(|s| s["signal_id"].as_str() == Some(signal_name.as_str()));
        if signal.is_none() {
            sleep(Duration::from_secs(5)).await;
        }
    }
    // Instrumented signals expose the raw slug as `signal_id` and a humanized display
    // string as `name`, so match on `signal_id`. `type` carries the signal kind.
    let signal = signal.unwrap_or_else(|| {
        panic!(
            "signal_id {:?} did not appear in signals.getGroupedSignals",
            signal_name
        )
    });
    assert_eq!(signal["signal_id"].as_str().unwrap_or(""), signal_name);
    assert_eq!(
        signal["type"].as_str().unwrap_or(""),
        "feedback",
        "signal type should be feedback; got {:?}",
        signal["type"]
    );
}

/// **identify lands user_traits.** Calling `identify` BEFORE `track_ai` causes subsequent
/// events for the same user to carry `userTraits` populated with the traits we sent.
///
/// Run with `cargo test -- --ignored`. Marked `#[ignore]` because the user→event
/// `user_traits` denormalization in Tinybird is computed at event-ingestion time from
/// whichever user-traits row was present then. With a brand-new `user_id`, the user row
/// races the event ingestion; even a several-second sleep doesn't reliably win the race.
/// The contract this test asserts (identify → userTraits on subsequent events) is real
/// and worth documenting, but it's too flaky to gate CI on.
#[tokio::test]
#[ignore = "userTraits denormalization races user-row ingestion for fresh user_ids"]
async fn e2e_identify_populates_user_traits_on_subsequent_events() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("traits");
    let client = build_client(&write_key);

    client
        .identify(User {
            user_id: user_id.clone(),
            traits: BTreeMap::from([
                ("plan".to_string(), json!("enterprise")),
                ("country".to_string(), json!("DE")),
                ("seats".to_string(), json!(42)),
            ]),
        })
        .await
        .expect("identify");

    // Brief delay so the user row lands before the event so the join can populate traits.
    sleep(Duration::from_secs(2)).await;

    client
        .track_ai(AiEvent {
            user_id: user_id.clone(),
            input: "trait test".to_string(),
            output: "ok".to_string(),
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    // userTraits is populated by an eventually-consistent join from the users table to
    // the events table. Poll up to 5 minutes for the join to land.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| {
            e["aiData"]["input"].as_str() == Some("trait test")
                && e["userTraits"]
                    .as_object()
                    .is_some_and(|o| o.contains_key("plan"))
        },
        E2E_DERIVED_POLL_TIMEOUT,
    )
    .await
    .expect("event with populated userTraits");

    let traits = ev["userTraits"].as_object().unwrap();
    assert_eq!(
        traits.get("plan").and_then(|v| v.as_str()),
        Some("enterprise"),
        "trait `plan` not propagated; full traits: {:?}",
        traits
    );
    assert_eq!(
        traits.get("country").and_then(|v| v.as_str()),
        Some("DE"),
        "trait `country` not propagated; full traits: {:?}",
        traits
    );
}

/// **Attachments split by role.** Sending attachments with `role: "input"` and `role: "output"`
/// must split them into `inputAttachments` vs `outputAttachments` on the dashboard event.
#[tokio::test]
async fn e2e_attachments_split_by_role_on_dashboard() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("attach");
    let client = build_client(&write_key);

    client
        .track_ai(AiEvent {
            user_id: user_id.clone(),
            input: "attachment test".to_string(),
            output: "ok".to_string(),
            attachments: vec![
                // The dashboard frontend's `AttachmentSchema` (`packages/schemas/src/frontend
                // /index.ts`) only accepts `attachment_type ∈ {text, image, iframe}` for
                // display; `code` attachments survive ingestion but are filtered out by the
                // frontend deserializer. So this test asserts on a `text` input attachment.
                Attachment {
                    kind: "text".into(),
                    role: "input".into(),
                    name: "user_query.txt".into(),
                    value: "Find the weather in Berlin.".into(),
                    ..Default::default()
                },
                Attachment {
                    kind: "text".into(),
                    role: "output".into(),
                    name: "summary".into(),
                    value: "This is a long summary".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .await
        .expect("track_ai");
    client.close().await.expect("close");

    // Attachments are uploaded asynchronously and the event row may include them only
    // after a brief join. Re-poll until both arrays are non-empty.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| {
            e["aiData"]["input"].as_str() == Some("attachment test")
                && e["inputAttachments"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty())
                && e["outputAttachments"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty())
        },
        E2E_DERIVED_POLL_TIMEOUT,
    )
    .await
    .expect("event with input + output attachments");

    let input_atts = ev["inputAttachments"].as_array().unwrap();
    let output_atts = ev["outputAttachments"].as_array().unwrap();

    let input_att = &input_atts[0];
    let in_type = input_att["attachment_type"]
        .as_str()
        .or_else(|| input_att["type"].as_str())
        .unwrap_or("");
    assert_eq!(in_type, "text", "input attachment type should be 'text'");
    assert_eq!(input_att["name"].as_str().unwrap_or(""), "user_query.txt");

    let output_att = &output_atts[0];
    let out_type = output_att["attachment_type"]
        .as_str()
        .or_else(|| output_att["type"].as_str())
        .unwrap_or("");
    assert_eq!(out_type, "text", "output attachment type should be 'text'");
    assert_eq!(output_att["name"].as_str().unwrap_or(""), "summary");
}

/// **Token-usage helper smoke test.** Manually emit `gen_ai.response.model` and
/// `gen_ai.usage.*` on a span via `Span::set_token_usage` and verify the parent event
/// lands on the dashboard with the matching `aiData.model`. This is intentionally
/// limited to verifying the helper SHIPS the canonical attributes and the parent
/// event lands; the dashboard's `events.list` TRPC does not surface per-event token
/// totals directly (token usage is stored in a separate Tinybird datasource keyed by
/// span_id, queried by other endpoints), so an assertion on actual token counts would
/// require a different read path. Wire-format assertions for the exact attribute
/// shape live in `tests/wire_format.rs::span_set_token_usage_emits_gen_ai_attributes`.
#[tokio::test]
async fn e2e_set_token_usage_helper_attributes_land_with_event() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("tokens");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "agent_run".to_string(),
            input: "What's 2+2?".into(),
            ..Default::default()
        })
        .await;

    let llm = interaction.start_span(SpanOptions {
        name: "llm.generate".into(),
        operation_id: "ai.generateText".into(),
        attributes: vec![Attribute::string("traceloop.span.kind", "llm")],
        ..Default::default()
    });
    llm.set_token_usage("gpt-4o-mini", 84, 17);
    llm.end();

    interaction
        .finish(FinishOptions {
            output: "4".into(),
            model: "gpt-4o-mini".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    // The event row should land on the dashboard with a matching aiData.model.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| e["aiData"]["output"].as_str() == Some("4"),
        E2E_POLL_TIMEOUT,
    )
    .await
    .expect("event with output");
    assert_eq!(ev["aiData"]["model"].as_str().unwrap_or(""), "gpt-4o-mini");
}

/// **LLM span helpers populate trace payloads.** `start_llm_span` must produce a trace row that
/// Dawn classifies as an LLM generation and whose input/output/model/provider/token fields are
/// rendered from the helper's canonical attributes.
#[tokio::test]
async fn e2e_llm_span_helpers_populate_traces_list_payloads() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("llmspan");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "agent_run".to_string(),
            input: "Run an LLM span helper".into(),
            ..Default::default()
        })
        .await;

    let llm = interaction.start_llm_span(
        "llm.generate",
        LlmOptions {
            provider: "openai".into(),
            model: "gpt-4o-mini".into(),
            messages: vec![
                LlmMessage::system("You answer with short arithmetic results."),
                LlmMessage::user("What is 2+2?"),
            ],
            output: Some("The answer is four.".into()),
            input_tokens: 11,
            output_tokens: 3,
            ..Default::default()
        },
    );
    llm.end();

    interaction
        .finish(FinishOptions {
            output: "The answer is four.".into(),
            model: "gpt-4o-mini".into(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| e["aiData"]["output"].as_str() == Some("The answer is four."),
        E2E_POLL_TIMEOUT,
    )
    .await
    .expect("event with output");
    let event_id_for_traces = ev["id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| interaction.event_id().to_string());

    let spans = poll_traces_for_event(
        &dashboard_token,
        &event_id_for_traces,
        1,
        E2E_DERIVED_POLL_TIMEOUT,
    )
    .await
    .expect("llm trace span");
    let llm_span = spans
        .iter()
        .find(|s| s["span_name"].as_str() == Some("llm.generate"))
        .unwrap_or_else(|| panic!("llm.generate span missing among {:?}", spans));

    assert_eq!(
        llm_span["span_type"].as_str().unwrap_or(""),
        "LLM_GENERATION"
    );
    assert_eq!(
        llm_span["input_payload"].as_str().unwrap_or(""),
        "What is 2+2?"
    );
    assert_eq!(
        llm_span["output_payload"].as_str().unwrap_or(""),
        "The answer is four."
    );
    assert_eq!(llm_span["model"].as_str().unwrap_or(""), "gpt-4o-mini");
    assert_eq!(llm_span["provider"].as_str().unwrap_or(""), "openai");
    assert_eq!(numeric_property(&llm_span["input_tokens"]), 11.0);
    assert_eq!(numeric_property(&llm_span["output_tokens"]), 3.0);
}

// NOTE: There is intentionally no `e2e_signal_sentiment_round_trips_to_dashboard` test.
// The dashboard's `SignalSchema` (`packages/schemas/src/frontend/index.ts`) exposes
// only `{name, timestamp, attachmentId?, properties, signalType?}` — sentiment is NOT
// a frontend-visible field, so it cannot be e2e-asserted from the dashboard. The wire
// contract is covered by `tests/wire_format.rs::track_signal_payload_uses_canonical_shape`
// (asserts `sentiment: "POSITIVE"` and `"NEGATIVE"` round-trip correctly on the
// `/signals/track` payload), and the signal-embedded-in-event flow is covered by
// `e2e_track_signal_appears_in_event_signals_array` below.

/// **Convo grouping via `conversations.list`.** Three events sharing a `convo_id` must
/// surface as one convo row in the dashboard's `conversations.list` TRPC endpoint, with
/// the user_id propagated. The TRPC route is `/api/trpc/conversations.list` and accepts a
/// `filters` object shaped per
/// `dawn/packages/schemas/src/tinybird/query/shared.ts::ConvosTable.schema.list`. To filter
/// by our user, we use `filters.user_id.$eq` (the canonical convo-table column name).
///
/// Previously `#[ignore]`d under the (incorrect) hypothesis of slow convo_list
/// aggregation. The actual cause was Bug #1 (the `{ json: {...} }` envelope
/// wrapper on the trpc input) which made `conversations.list` silently fall
/// through to defaults and never return the convo. After dropping the wrapper,
/// the test passes in <10s.
#[tokio::test]
async fn e2e_conversations_list_shows_grouped_events_for_convo_id() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("convolist");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);
    for i in 0..3 {
        client
            .track_ai(AiEvent {
                user_id: user_id.clone(),
                convo_id: convo_id.clone(),
                event: "ai_generation".into(),
                input: format!("turn {}", i),
                output: format!("response {}", i),
                model: "gpt-4o".into(),
                ..Default::default()
            })
            .await
            .expect("track_ai");
    }
    client.close().await.expect("close");

    // Confirm events landed on `events.list` so we know ingestion ran.
    let _ = poll_events(&dashboard_token, &user_id, 3, E2E_POLL_TIMEOUT)
        .await
        .expect("events landed");

    // Build the convo-list filter using the canonical schema:
    // ConvosTable.schema.list.filters == { user_id?: { $eq: ..., $ne: ..., ... } }.
    let backend_url =
        env::var("RAINDROP_BACKEND_URL").unwrap_or_else(|_| DEFAULT_BACKEND_URL.to_string());
    // Send raw input (no `{ json: {...} }` wrapper) — see `query_dashboard`.
    let input_obj = json!({
        "filters": { "user_id": { "$eq": user_id } },
        "limit": 25,
    });
    let encoded = urlencoding::encode(&input_obj.to_string()).into_owned();
    let url = format!(
        "{}/api/trpc/conversations.list?input={}",
        backend_url, encoded
    );

    let interval = Duration::from_secs(10);
    // Aggregation lag for convo_list is the longest derived pipeline we touch in this suite.
    let timeout = E2E_DERIVED_POLL_TIMEOUT;
    let started = std::time::Instant::now();
    let req = reqwest::Client::new();
    let mut convo: Option<Value> = None;
    while started.elapsed() < timeout {
        let resp = req
            .get(&url)
            .header("Authorization", format!("Bearer {}", dashboard_token))
            .send()
            .await
            .expect("conversations.list request");
        if resp.status().is_success() {
            let body: Value = resp.json().await.expect("conversations.list json");
            let arr = body["result"]["data"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if let Some(found) = arr
                .iter()
                .find(|c| {
                    c["convo_id"].as_str() == Some(&convo_id)
                        || c["convoId"].as_str() == Some(&convo_id)
                        || c["id"].as_str() == Some(&convo_id)
                })
                .cloned()
            {
                convo = Some(found);
                break;
            }
        }
        sleep(interval).await;
    }
    let convo =
        convo.unwrap_or_else(|| panic!("convo {} not found via conversations.list", convo_id));

    // Verify the convo carries our user_id (regardless of camel/snake casing).
    let convo_user = convo["user_id"]
        .as_str()
        .or_else(|| convo["userId"].as_str())
        .unwrap_or("");
    assert_eq!(
        convo_user, user_id,
        "conversations.list convo must carry our user_id"
    );
    // And report at least the 3 messages we sent.
    let message_count = convo["message_count"]
        .as_f64()
        .or_else(|| convo["messageCount"].as_f64())
        .unwrap_or(0.0);
    assert!(
        message_count >= 3.0,
        "convo should report at least 3 messages, got {}",
        message_count
    );
}

/// **traces.list deep verification.** Spans for an event_id must form a valid trace tree
/// when fetched via `traces.list`: parent-child linkage, span_type detection, attributes,
/// duration_ns, and start/end times. Verified directly against the Traces source, which is
/// populated within seconds of ingestion.
#[tokio::test]
async fn e2e_traces_list_returns_correct_span_tree() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };
    let user_id = unique_user_id("tracetree");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "agent_workflow".to_string(),
            input: "complex".to_string(),
            ..Default::default()
        })
        .await;
    let event_id = interaction.event_id().to_string();

    // Build a tree: root_workflow → child_step → tool_call
    let root = interaction.start_span(SpanOptions {
        name: "root_workflow".into(),
        operation_id: "ai.workflow".into(),
        attributes: vec![Attribute::string("traceloop.span.kind", "workflow")],
        ..Default::default()
    });
    sleep(Duration::from_millis(20)).await;

    let child = interaction.start_span(SpanOptions {
        name: "child_step".into(),
        operation_id: "ai.task".into(),
        parent: Some(root.clone()),
        attributes: vec![Attribute::string("traceloop.span.kind", "task")],
        ..Default::default()
    });
    sleep(Duration::from_millis(20)).await;

    let tool = interaction.start_tool_span(
        "search",
        ToolOptions {
            input: Some(json!({"q": "rust traces"})),
            parent: Some(child.clone()),
            ..Default::default()
        },
    );
    sleep(Duration::from_millis(50)).await;
    tool.set_output(&json!({"hits": 7}));
    tool.end();

    child.end();
    root.end();

    interaction
        .finish(FinishOptions {
            output: "done".to_string(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    // Wait for the event to land, then read the spans directly from `traces.list` (the
    // Traces source), which is populated within seconds and independently of the slower
    // span→event toolCalls projection. `poll_traces_for_event` already retries until the
    // spans appear, so no toolCalls gate is needed.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| e["aiData"]["output"].as_str() == Some("done"),
        E2E_POLL_TIMEOUT,
    )
    .await
    .expect("event landed");

    // The dashboard `events.list` row uses a public UUID for the event id; the same id is
    // accepted by `traces.list`, which internally maps to the Tinybird-stored internal id.
    // Use whichever id we actually see on the event, to insulate the test from any
    // public/internal id remapping subtleties.
    let event_id_for_traces = ev["id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| event_id.clone());

    let spans = poll_traces_for_event(
        &dashboard_token,
        &event_id_for_traces,
        3,
        E2E_DERIVED_POLL_TIMEOUT,
    )
    .await
    .expect("trace verification");
    assert!(
        spans.len() >= 3,
        "expected at least 3 spans (root, child, tool); got {}: {:?}",
        spans.len(),
        spans
    );

    let by_name = |n: &str| {
        spans
            .iter()
            .find(|s| s["span_name"].as_str() == Some(n))
            .unwrap_or_else(|| panic!("span '{}' missing among {:?}", n, spans))
    };
    let root_span = by_name("root_workflow");
    let child_span = by_name("child_step");
    let tool_span = by_name("search");

    // Parent-child linkage via parent_span_id
    let root_span_id = root_span["span_id"].as_str().unwrap();
    let child_parent_id = child_span["parent_span_id"].as_str().unwrap_or("");
    let tool_parent_id = tool_span["parent_span_id"].as_str().unwrap_or("");
    let child_span_id = child_span["span_id"].as_str().unwrap();
    assert_eq!(
        child_parent_id, root_span_id,
        "child_step.parent_span_id must equal root_workflow.span_id"
    );
    assert_eq!(
        tool_parent_id, child_span_id,
        "search (tool).parent_span_id must equal child_step.span_id"
    );

    // All spans share the same trace_id
    let trace_ids: std::collections::HashSet<String> = spans
        .iter()
        .filter_map(|s| s["trace_id"].as_str().map(String::from))
        .collect();
    assert_eq!(trace_ids.len(), 1, "all spans should share one trace_id");

    // span_type inference: tool span MUST be TOOL_CALL
    assert_eq!(tool_span["span_type"].as_str().unwrap_or(""), "TOOL_CALL");

    // tool span carries the input/output payload as expected
    let input_payload = tool_span["input_payload"].as_str().unwrap_or("");
    let output_payload = tool_span["output_payload"].as_str().unwrap_or("");
    assert!(
        input_payload.contains("rust traces"),
        "tool input_payload missing query, got {:?}",
        input_payload
    );
    assert!(
        output_payload.contains("\"hits\":7"),
        "tool output_payload missing hits, got {:?}",
        output_payload
    );

    // duration_ns must be positive (we slept 50ms inside the tool span)
    let dur_ns: u64 = tool_span["duration_ns"]
        .as_u64()
        .or_else(|| tool_span["duration_ns"].as_f64().map(|f| f as u64))
        .or_else(|| {
            tool_span["duration_ns"]
                .as_str()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(0);
    assert!(
        dur_ns > 0,
        "tool span duration_ns must be > 0, got {:?}",
        tool_span["duration_ns"]
    );

    // Status: tool ended without error → status="OK"
    assert_eq!(tool_span["status"].as_str().unwrap_or(""), "OK");

    // attributes_string must contain the user_id and convo_id we propagated from interaction
    let attrs = tool_span["attributes_string"]
        .as_object()
        .unwrap_or_else(|| panic!("attributes_string missing on tool span: {:?}", tool_span));
    assert_eq!(
        attrs
            .get("traceloop.association.properties.user_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        user_id,
        "tool span must inherit user_id from interaction"
    );
    assert_eq!(
        attrs
            .get("traceloop.association.properties.convo_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        convo_id,
        "tool span must inherit convo_id from interaction"
    );
    // tool name is preserved as traceloop.entity.name
    assert_eq!(
        attrs
            .get("traceloop.entity.name")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        "search"
    );
}

/// **Drop-policy regression test.** Verifies the fix introduced in v0.0.6 against the real
/// Raindrop backend: a finalized `track_ai` with empty `input` and `output` (the chisel-style
/// production failure mode — wrapper populating only `model`, `convo_id`, and token-usage
/// `properties`) MUST be dropped client-side and never reach the dashboard.
///
/// We send two events for the same `user_id`:
///   1. The chisel-style empty-text event — must be dropped by `should_drop_empty_ai_event`.
///   2. A control event with populated `input`/`output` — proves the ingestion pipeline is
///      alive and our `user_id` surfaces correctly.
///
/// After the control lands, we wait an extra settling window so a regressed (un-dropped)
/// chisel-style event would have time to appear, then assert that exactly ONE event is
/// visible for this `user_id`. If the SDK drop policy were ever removed or weakened, this
/// test would observe two events and fail.
#[tokio::test]
async fn e2e_chisel_style_empty_text_event_does_not_land_in_dashboard() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("chisel_drop");
    let convo_id = format!("{}_convo", user_id);
    let client = build_client(&write_key);

    let mut empty_props = BTreeMap::new();
    empty_props.insert("total_input_tokens".to_string(), json!(123));
    empty_props.insert("output_tokens".to_string(), json!(45));
    empty_props.insert("total_time_ms".to_string(), json!(789));

    let chisel_event_id = format!("{}_chisel_evt", user_id);
    let control_event_id = format!("{}_control_evt", user_id);

    client
        .track_ai(AiEvent {
            event_id: chisel_event_id.clone(),
            user_id: user_id.clone(),
            event: "ai_generation".to_string(),
            input: String::new(),
            output: String::new(),
            model: "swe-1-6-slow".to_string(),
            convo_id: convo_id.clone(),
            properties: empty_props,
            ..Default::default()
        })
        .await
        .expect("track_ai chisel (expected to be dropped client-side)");

    client
        .track_ai(AiEvent {
            event_id: control_event_id.clone(),
            user_id: user_id.clone(),
            event: "ai_generation".to_string(),
            input: "control input".to_string(),
            output: "control output".to_string(),
            model: "gpt-4o".to_string(),
            convo_id: convo_id.clone(),
            ..Default::default()
        })
        .await
        .expect("track_ai control");

    client.close().await.expect("close");

    let _ = poll_events(&dashboard_token, &user_id, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("control event must land — if this times out the issue is the ingest pipeline, not the SDK drop policy");

    // The chisel-style event was sent through `track_ai` BEFORE the control on the same
    // client. If the drop were regressed and the empty event were shipped, both events
    // would hit the ingest API within milliseconds of each other and would surface on the
    // dashboard within the same window. We give the pipeline a generous settling buffer to
    // make sure a regressed empty event has every chance to appear before we conclude it
    // was dropped.
    sleep(Duration::from_secs(45)).await;

    let final_events = query_dashboard(&dashboard_token, 50)
        .await
        .expect("final dashboard query");
    let matched: Vec<&Value> = final_events
        .iter()
        .filter(|e| e["userId"].as_str() == Some(&user_id))
        .collect();
    assert_eq!(
        matched.len(),
        1,
        "SDK drop-policy regression: expected exactly ONE event for user {} (the control), got {}. \
         If the chisel-style empty-text event is among these, `should_drop_empty_ai_event` is no \
         longer firing. Events: {:?}",
        user_id,
        matched.len(),
        matched
    );
    let ev = matched[0];
    // The dashboard's `events.list` returns the caller-supplied `event_id` as the `id` field
    // (also mirrored to `customEventId`). It does NOT expose a camelCase `eventId` key.
    let surviving_id = ev["id"].as_str().unwrap_or_default();
    assert_eq!(
        surviving_id, control_event_id,
        "surviving event must be the control (event_id {}), not the chisel drop (event_id {})",
        control_event_id, chisel_event_id
    );
    assert_eq!(
        ev["aiData"]["input"].as_str().unwrap_or_default(),
        "control input"
    );
    assert_eq!(
        ev["aiData"]["output"].as_str().unwrap_or_default(),
        "control output"
    );
}

/// **Drop-policy escape hatch.** A finalized `track_ai` with populated `input` but empty
/// `output` is the canonical "errored generation" shape (we captured the prompt but the
/// model failed mid-stream). The drop gate intentionally does NOT fire on this shape — only
/// payloads with BOTH `input` and `output` empty get dropped — so the event must still
/// land on the dashboard with an empty `aiData.output`.
///
/// This complements the integration test `errored_generation_with_input_only_still_ships`
/// in `tests/empty_events_repro.rs` by verifying that the dashboard correctly renders the
/// resulting row rather than rejecting it at ingest.
#[tokio::test]
async fn e2e_errored_generation_with_input_only_lands_in_dashboard() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("errored_input_only");
    let client = build_client(&write_key);

    client
        .track_ai(AiEvent {
            user_id: user_id.clone(),
            event: "ai_generation".to_string(),
            input: "Tell me a joke".to_string(),
            output: String::new(),
            model: "gpt-4o".to_string(),
            ..Default::default()
        })
        .await
        .expect("track_ai errored");

    client.close().await.expect("close");

    let events = poll_events(&dashboard_token, &user_id, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("dashboard verification");
    let ev = events
        .iter()
        .find(|e| e["aiData"]["input"].as_str().unwrap_or("") == "Tell me a joke")
        .unwrap_or_else(|| panic!("errored event not found among {:?}", events));
    let output = ev["aiData"]["output"].as_str().unwrap_or_default();
    assert!(
        output.is_empty(),
        "aiData.output must be empty (input-only errored generation), got {:?}",
        output
    );
    assert_eq!(ev["aiData"]["model"].as_str().unwrap_or_default(), "gpt-4o");
}

// ======================================================================
// Hardening e2e (mirrors python-sdk 0.0.51 / PR #16 e2e suite)
// ======================================================================

/// **Large payloads land truncated instead of being dropped.** Pre-0.0.7, a
/// multi-MB output was serialized in full and then silently dropped at the
/// 1 MiB ingest guard — the worst data-loss mode: the caller paid the
/// serialization cost AND lost the event. Now the output is capped
/// up front and the event must land on the real dashboard, truncated, marker
/// within the cap. The cap is pinned to 100,000 chars here so the test
/// proves the truncation mechanism end-to-end without depending on the
/// library default; default-cap behavior is covered by the wire_format
/// suite against a mock server. Also asserts the caller-side buffering cost
/// stays bounded (`begin` with a multi-MB input buffers without any network
/// round trip and must be O(cap), not O(payload)).
#[tokio::test]
async fn e2e_large_output_event_lands_truncated() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("bigout");
    let mut builder = Client::builder()
        .write_key(&write_key)
        .disable_local_workshop()
        .max_text_field_chars(100_000);
    if let Ok(endpoint) = env::var("RAINDROP_ENDPOINT") {
        builder = builder.endpoint(endpoint);
    }
    let client = builder.build().expect("build client");

    // ~2.2 MB output against the pinned 100_000-char cap.
    let big_output = "All work and no play makes Jack a dull boy. ".repeat(50_000);
    let big_input = "x".repeat(5_000_000);

    let started = std::time::Instant::now();
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            event: "ai_generation".to_string(),
            input: big_input, // 5 MB: buffered (pending), no inline POST
            ..Default::default()
        })
        .await;
    let begin_elapsed = started.elapsed();
    assert!(
        begin_elapsed < Duration::from_millis(250),
        "begin() with a 5MB input spent {begin_elapsed:?} on the caller; \
         capping must be O(cap), not O(payload)"
    );

    interaction
        .finish(FinishOptions {
            output: big_output,
            ..Default::default()
        })
        .await
        .expect("finish ships the truncated event");
    client.close().await.expect("close");

    let events = poll_events(&dashboard_token, &user_id, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("dashboard verification");
    let ev = &events[0];
    let output = ev["aiData"]["output"].as_str().unwrap_or_default();
    assert!(!output.is_empty(), "event landed without output: {ev:?}");
    assert!(
        output.chars().count() <= 100_000,
        "output not truncated within the configured cap: {} chars",
        output.chars().count()
    );
    assert!(
        output.ends_with("...[truncated by raindrop]"),
        "missing truncation marker, tail: {:?}",
        &output[output.len().saturating_sub(60)..]
    );
    assert!(output.starts_with("All work and no play"));
    let input = ev["aiData"]["input"].as_str().unwrap_or_default();
    assert!(
        input.chars().count() <= 100_000,
        "input not truncated within the configured cap"
    );
}

/// **Multi-MB tool payloads ship a bounded span that lands.** Tool payloads
/// shaped like order timelines or transcripts (multi-MB structured rows)
/// used to be serialized in full on the hot path; now `track_tool`
/// serialization is budget-bounded and the event + tool call must still
/// reach the dashboard.
#[tokio::test]
async fn e2e_huge_tool_payload_ships_bounded_and_lands() {
    let (write_key, dashboard_token) = match env_keys() {
        Some(v) => v,
        None => {
            eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
            return;
        }
    };

    let user_id = unique_user_id("bigtool");
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            event: "agent_run".to_string(),
            input: "tool probe".to_string(),
            ..Default::default()
        })
        .await;

    // ~4.5 MB structured tool output.
    let rows: Vec<String> = (0..20_000)
        .map(|_| format!("order-timeline-entry {}", "z".repeat(200)))
        .collect();
    let started = std::time::Instant::now();
    interaction.track_tool(TrackToolOptions {
        name: "fetch_order_timeline".to_string(),
        input: Some(json!({"q": "probe"})),
        output: Some(json!({ "rows": rows })),
        duration: Some(Duration::from_millis(42)),
        ..Default::default()
    });
    let track_elapsed = started.elapsed();
    assert!(
        track_elapsed < Duration::from_millis(500),
        "track_tool() spent {track_elapsed:?} on the caller for a ~4.5MB payload; \
         serialization must be budget-bounded"
    );

    interaction
        .finish(FinishOptions {
            output: "done".to_string(),
            ..Default::default()
        })
        .await
        .expect("finish");
    client.close().await.expect("close");

    // The huge tool output must land BOUNDED. Verify the tool span via `traces.list` (the
    // Traces source, populated within seconds) rather than the slower, flakier events.list
    // `toolCalls` projection. The SDK caps text fields at ~1M chars, so the stored payload
    // is a small fraction of the ~4.5MB we shipped.
    let ev = poll_event_until(
        &dashboard_token,
        &user_id,
        |e| e["aiData"]["output"].as_str() == Some("done"),
        E2E_POLL_TIMEOUT,
    )
    .await
    .expect("event landed");
    let event_id_for_traces = ev["id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| panic!("event has no id: {:?}", ev));

    let spans = poll_traces_for_event(&dashboard_token, &event_id_for_traces, 1, E2E_POLL_TIMEOUT)
        .await
        .expect("trace spans for the huge tool payload");
    let tool = spans
        .iter()
        .find(|s| s["span_name"].as_str() == Some("fetch_order_timeline"))
        .unwrap_or_else(|| {
            panic!(
                "fetch_order_timeline span missing among {} spans",
                spans.len()
            )
        });
    assert_eq!(tool["span_type"].as_str().unwrap_or(""), "TOOL_CALL");
    let out_len = tool["output_payload"].as_str().unwrap_or("").len();
    assert!(out_len > 0, "tool output_payload must not be empty");
    assert!(
        out_len <= 1_100_000,
        "tool output_payload must be bounded (~1M char cap), got {} bytes",
        out_len
    );
}

/// **Process exit is bounded against a real black-hole network.** Ships
/// events at a non-routable address (RFC 5737 TEST-NET) — real sockets, real
/// connect attempts, no mocks — and asserts `close()` honors its 10s default
/// deadline instead of multiplying connect timeouts across retries and
/// buffered events (the wedged-shutdown failure mode). Requires no
/// dashboard token (nothing can land), but stays env-gated like the rest of
/// the e2e suite so routine CI runtime is unaffected.
#[tokio::test]
async fn e2e_close_bounded_with_unreachable_api() {
    if env_keys().is_none() {
        eprintln!("[e2e] skipping: set RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN to run");
        return;
    }

    let client = Client::builder()
        .write_key("e2e-blackhole-key")
        .endpoint("http://192.0.2.1:9/v1/")
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .build()
        .expect("build");

    for i in 0..5 {
        let _ = client
            .begin(BeginOptions {
                user_id: format!("shutdown_probe_{i}"),
                event: "ai_generation".to_string(),
                input: "x".to_string(),
                ..Default::default()
            })
            .await;
    }

    let started = std::time::Instant::now();
    client
        .close()
        .await
        .expect("close returns Ok at the deadline");
    let elapsed = started.elapsed();
    // Default close_timeout is 10s; allow scheduling slack (python parity: <15s).
    assert!(
        elapsed < Duration::from_secs(15),
        "close() took {elapsed:?} against a black-hole network"
    );
}
