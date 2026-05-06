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
use tokio::time::sleep;

use raindrop::{AiEvent, Client, Signal, ToolOptions, User};

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
    let input_obj = json!({
        "json": {
            "limit": limit,
            "orderBy": { "field": "timestamp", "direction": "desc" }
        }
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
            &body.chars().take(500).collect::<String>()
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
        "Timed out waiting for {} events for user {} (last seen {})",
        min_count, user_id, last_seen
    ))
}

fn build_client(write_key: &str) -> Client {
    let mut builder = Client::builder().write_key(write_key);
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

    let events = poll_events(&dashboard_token, &user_id, 1, Duration::from_secs(60))
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
    assert!(
        p["ai.usage.prompt_tokens"].as_u64().unwrap_or(0) > 0,
        "expected prompt_tokens > 0, got {:?}",
        p["ai.usage.prompt_tokens"]
    );
    assert!(
        p["ai.usage.completion_tokens"].as_u64().unwrap_or(0) > 0,
        "expected completion_tokens > 0, got {:?}",
        p["ai.usage.completion_tokens"]
    );
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

    let events = poll_events(&dashboard_token, &user_id, 1, Duration::from_secs(60))
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
    let events = poll_events(&dashboard_token, &user_id, 1, Duration::from_secs(60))
        .await
        .expect("dashboard verification");
    let ev = events
        .iter()
        .find(|e| e["aiData"]["input"].as_str().unwrap_or("") == "rate me")
        .unwrap_or_else(|| panic!("track_ai event not found among {:?}", events));
    assert_eq!(ev["userId"].as_str().unwrap_or(""), user_id);
    assert_eq!(ev["aiData"]["output"].as_str().unwrap_or(""), "I am rated");
}
