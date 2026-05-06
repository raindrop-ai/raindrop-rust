use std::collections::BTreeMap;
use std::env;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::sleep;

use raindrop::{AiEvent, Client, Signal, ToolOptions, User};

async fn query_dashboard(
    token: &str,
    limit: usize,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let backend_url = env::var("RAINDROP_BACKEND_URL")
        .unwrap_or_else(|_| "https://backend.raindrop.ai".to_string());
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
        .await?
        .error_for_status()?;

    let body: Value = resp.json().await?;
    let data = body["result"]["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    Ok(data)
}

async fn poll_events(
    token: &str,
    user_id: &str,
    min_count: usize,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(60);
    let interval = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        let all_events = query_dashboard(token, 50).await?;
        let matched: Vec<Value> = all_events
            .into_iter()
            .filter(|e| e["userId"].as_str() == Some(user_id))
            .collect();

        if matched.len() >= min_count {
            return Ok(matched);
        }
        sleep(interval).await;
    }
    Err(format!(
        "Timeout waiting for {} events for user {}",
        min_count, user_id
    )
    .into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let write_key = env::var("RAINDROP_WRITE_KEY").or_else(|_| env::var("RAINDROP_API_KEY"));
    let dashboard_token = env::var("RAINDROP_DASHBOARD_TOKEN");

    if write_key.is_err() || dashboard_token.is_err() {
        println!("Skipping e2e verification: RAINDROP_WRITE_KEY and RAINDROP_DASHBOARD_TOKEN are required.");
        return Ok(());
    }

    let write_key = write_key.unwrap();
    let dashboard_token = dashboard_token.unwrap();

    let run_id = uuid::Uuid::new_v4()
        .to_string()
        .chars()
        .take(8)
        .collect::<String>();
    let user_id = format!("e2e_rust_{}", run_id);
    let convo_id = format!("e2e_rust_convo_{}", run_id);

    println!("Starting E2E verification for user_id: {}", user_id);

    let client = Client::builder().write_key(write_key).build()?;

    // 1. Identify
    client
        .identify(User {
            user_id: user_id.clone(),
            traits: BTreeMap::from([("plan".to_string(), json!("pro"))]),
        })
        .await?;

    // 2. Track AI Event
    let mut props = BTreeMap::new();
    props.insert("ai.usage.prompt_tokens".to_string(), json!(10));
    props.insert("ai.usage.completion_tokens".to_string(), json!(20));

    client
        .track_ai(AiEvent {
            event_id: format!("evt_ai_{}", run_id),
            user_id: user_id.clone(),
            event: "ai_generation".to_string(),
            input: "Hello Rust".to_string(),
            output: "Hello World".to_string(),
            model: "gpt-4o".to_string(),
            convo_id: convo_id.clone(),
            properties: props,
            ..Default::default()
        })
        .await?;

    // 3. Track Signal
    client
        .track_signal(Signal {
            event_id: format!("evt_ai_{}", run_id),
            name: "thumbs_up".to_string(),
            kind: "feedback".to_string(),
            sentiment: "POSITIVE".to_string(),
            comment: "Great SDK".to_string(),
            ..Default::default()
        })
        .await?;

    // 4. Manual Spans & Tools
    let interaction = client
        .begin(raindrop::BeginOptions {
            event_id: format!("evt_interaction_{}", run_id),
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
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
        .await?;

    // Flush and close
    client.close().await?;
    println!("Telemetry shipped. Polling dashboard...");

    // Verify Dashboard
    let events = poll_events(&dashboard_token, &user_id, 2).await?;
    println!("Found {} events.", events.len());

    let mut found_ai = false;
    let mut found_interaction = false;

    for ev in events {
        let ai = &ev["aiData"];
        let input = ai["input"].as_str().unwrap_or("");

        if input == "Hello Rust" {
            found_ai = true;
            assert_eq!(ai["output"].as_str().unwrap(), "Hello World");
            assert_eq!(ai["model"].as_str().unwrap(), "gpt-4o");
            assert_eq!(ai["convoId"].as_str().unwrap(), convo_id);

            let props = &ev["properties"];
            assert_eq!(props["ai.usage.prompt_tokens"].as_u64().unwrap(), 10);
            assert_eq!(props["ai.usage.completion_tokens"].as_u64().unwrap(), 20);
        } else if input == "Run tool" {
            found_interaction = true;
            assert_eq!(ai["output"].as_str().unwrap(), "The weather is 72");
            assert_eq!(ai["convoId"].as_str().unwrap(), convo_id);
        }
    }

    assert!(found_ai, "Missing track_ai event");
    assert!(found_interaction, "Missing interaction event");

    println!("E2E verification passed!");
    Ok(())
}
