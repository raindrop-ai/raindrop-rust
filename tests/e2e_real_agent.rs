//! Ignored live e2e tests that run real provider tool-calling flows and verify Raindrop traces.
//!
//! Run with:
//!
//! ```sh
//! cargo test --test e2e_real_agent -- --ignored --nocapture
//! ```
//!
//! The test loads `.env` from the repository root, then expects:
//!
//! - `RAINDROP_WRITE_KEY` (or `RAINDROP_API_KEY`)
//! - `RAINDROP_DASHBOARD_TOKEN`
//! - `OPENAI_API_KEY` for the OpenAI test
//! - `ANTHROPIC_API_KEY` for the Anthropic test
//! - `RAINDROP_E2E_OPENAI_MODEL` or `OPENAI_MODEL` (optional, defaults to `gpt-5.5`)
//! - `RAINDROP_E2E_ANTHROPIC_MODEL` or `ANTHROPIC_MODEL` (optional, defaults to `claude-sonnet-4-5`)

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use tokio::time::sleep;

use raindrop::{
    BeginOptions, Client, FinishOptions, LlmMessage, LlmOptions, SpanOptions, ToolOptions,
};

const DEFAULT_BACKEND_URL: &str = "https://backend.raindrop.ai";
const DEFAULT_OPENAI_URL: &str = "https://api.openai.com/v1/responses";
const DEFAULT_ANTHROPIC_URL: &str = "https://api.anthropic.com/v1/messages";
const POLL_TIMEOUT: Duration = Duration::from_secs(180);

fn load_dotenv() {
    for path in [".env", "examples/.env"] {
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };
        for line in contents.lines() {
            let mut trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("export ") {
                trimmed = rest.trim();
            }
            let Some((key, value)) = trimmed.split_once('=') else {
                continue;
            };
            let key = key.trim();
            if key.is_empty() || env::var_os(key).is_some() {
                continue;
            }
            let value = value.trim().trim_matches('"').trim_matches('\'');
            env::set_var(key, value);
        }
    }
}

fn env_var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|s| !s.is_empty())
}

fn required_raindrop_env() -> Option<(String, String)> {
    load_dotenv();
    let write_key = env_var("RAINDROP_WRITE_KEY").or_else(|| env_var("RAINDROP_API_KEY"))?;
    let dashboard_token = env_var("RAINDROP_DASHBOARD_TOKEN")?;
    Some((write_key, dashboard_token))
}

fn required_openai_env() -> Option<(String, String, String, String)> {
    let (write_key, dashboard_token) = required_raindrop_env()?;
    let openai_key = env_var("OPENAI_API_KEY")?;
    let model = env_var("RAINDROP_E2E_OPENAI_MODEL")
        .or_else(|| env_var("OPENAI_MODEL"))
        .unwrap_or_else(|| "gpt-5.5".to_string());
    Some((write_key, dashboard_token, openai_key, model))
}

fn required_anthropic_env() -> Option<(String, String, String, String)> {
    let (write_key, dashboard_token) = required_raindrop_env()?;
    let anthropic_key = env_var("ANTHROPIC_API_KEY")?;
    let model = env_var("RAINDROP_E2E_ANTHROPIC_MODEL")
        .or_else(|| env_var("ANTHROPIC_MODEL"))
        .unwrap_or_else(|| "claude-sonnet-4-5".to_string());
    Some((write_key, dashboard_token, anthropic_key, model))
}

fn unique_user_id() -> String {
    let id = uuid::Uuid::new_v4()
        .to_string()
        .chars()
        .take(8)
        .collect::<String>();
    format!("e2e_rust_real_agent_{}", id)
}

fn build_client(write_key: &str) -> Client {
    let mut builder = Client::builder()
        .write_key(write_key)
        .disable_local_workshop();
    if let Some(endpoint) = env_var("RAINDROP_ENDPOINT") {
        builder = builder.endpoint(endpoint);
    }
    builder.build().expect("client")
}

async fn post_openai(openai_key: &str, body: Value) -> Result<Value, String> {
    let url = env_var("OPENAI_RESPONSES_URL").unwrap_or_else(|| DEFAULT_OPENAI_URL.to_string());
    let resp = reqwest::Client::new()
        .post(url)
        .header(AUTHORIZATION, format!("Bearer {}", openai_key))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("OpenAI request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("OpenAI response read failed: {}", e))?;
    if !status.is_success() {
        return Err(format!(
            "OpenAI returned {}: {}",
            status,
            text.chars().take(500).collect::<String>()
        ));
    }
    serde_json::from_str(&text).map_err(|e| format!("OpenAI response was not JSON: {}", e))
}

async fn post_anthropic(anthropic_key: &str, body: Value) -> Result<Value, String> {
    let url =
        env_var("ANTHROPIC_MESSAGES_URL").unwrap_or_else(|| DEFAULT_ANTHROPIC_URL.to_string());
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-api-key", anthropic_key)
        .header("anthropic-version", "2023-06-01")
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Anthropic request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Anthropic response read failed: {}", e))?;
    if !status.is_success() {
        return Err(format!(
            "Anthropic returned {}: {}",
            status,
            text.chars().take(500).collect::<String>()
        ));
    }
    serde_json::from_str(&text).map_err(|e| format!("Anthropic response was not JSON: {}", e))
}

async fn fetch_san_francisco_weather() -> Result<Value, String> {
    let url = "https://api.open-meteo.com/v1/forecast?latitude=37.7749&longitude=-122.4194&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&temperature_unit=fahrenheit";
    let resp = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map_err(|e| format!("weather request failed: {}", e))?
        .error_for_status()
        .map_err(|e| format!("weather request returned error: {}", e))?;
    resp.json()
        .await
        .map_err(|e| format!("weather response was not JSON: {}", e))
}

fn find_tool_call(response: &Value) -> Option<Value> {
    response["output"]
        .as_array()?
        .iter()
        .find(|item| item["type"].as_str() == Some("function_call"))
        .cloned()
}

fn find_anthropic_tool_use(response: &Value) -> Option<Value> {
    response["content"]
        .as_array()?
        .iter()
        .find(|item| item["type"].as_str() == Some("tool_use"))
        .cloned()
}

fn output_text(response: &Value) -> String {
    if let Some(text) = response["output_text"].as_str() {
        if !text.is_empty() {
            return text.to_string();
        }
    }

    let mut parts = Vec::new();
    if let Some(output) = response["output"].as_array() {
        for item in output {
            if let Some(content) = item["content"].as_array() {
                for part in content {
                    if let Some(text) = part["text"].as_str() {
                        parts.push(text.to_string());
                    }
                }
            }
        }
    }
    parts.join("\n")
}

fn anthropic_output_text(response: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(content) = response["content"].as_array() {
        for part in content {
            if part["type"].as_str() == Some("text") {
                if let Some(text) = part["text"].as_str() {
                    parts.push(text.to_string());
                }
            }
        }
    }
    parts.join("\n")
}

fn usage_tokens(response: &Value) -> (i64, i64) {
    let usage = &response["usage"];
    (
        usage["input_tokens"].as_i64().unwrap_or(0),
        usage["output_tokens"].as_i64().unwrap_or(0),
    )
}

async fn query_dashboard(token: &str, limit: usize) -> Result<Vec<Value>, String> {
    let backend_url = env_var("RAINDROP_BACKEND_URL").unwrap_or_else(|| DEFAULT_BACKEND_URL.into());
    let input_obj = json!({
        "limit": limit,
        "orderBy": { "field": "timestamp", "direction": "DESC" }
    });
    let encoded = urlencoding::encode(&input_obj.to_string()).into_owned();
    let url = format!("{}/api/trpc/events.list?input={}", backend_url, encoded);
    let resp = reqwest::Client::new()
        .get(url)
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| format!("events.list request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("events.list response read failed: {}", e))?;
    if !status.is_success() {
        return Err(format!(
            "events.list returned {}: {}",
            status,
            text.chars().take(500).collect::<String>()
        ));
    }
    let body: Value =
        serde_json::from_str(&text).map_err(|e| format!("events.list invalid JSON: {}", e))?;
    Ok(body["result"]["data"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

async fn poll_event_until<F>(token: &str, user_id: &str, predicate: F) -> Result<Value, String>
where
    F: Fn(&Value) -> bool,
{
    let started = std::time::Instant::now();
    let mut last_event = None;
    while started.elapsed() < POLL_TIMEOUT {
        let events = query_dashboard(token, 50).await?;
        for event in events {
            if event["userId"].as_str() == Some(user_id) {
                if predicate(&event) {
                    return Ok(event);
                }
                last_event = Some(event);
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
    Err(format!(
        "timed out waiting for event for user {}; last seen {:?}",
        user_id, last_event
    ))
}

async fn query_traces(token: &str, event_id: &str) -> Result<Vec<Value>, String> {
    let backend_url = env_var("RAINDROP_BACKEND_URL").unwrap_or_else(|| DEFAULT_BACKEND_URL.into());
    let input_obj = json!({ "eventId": event_id, "limit": 200 });
    let encoded = urlencoding::encode(&input_obj.to_string()).into_owned();
    let url = format!("{}/api/trpc/traces.list?input={}", backend_url, encoded);
    let resp = reqwest::Client::new()
        .get(url)
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| format!("traces.list request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("traces.list response read failed: {}", e))?;
    if !status.is_success() {
        return Err(format!(
            "traces.list returned {}: {}",
            status,
            text.chars().take(500).collect::<String>()
        ));
    }
    let body: Value =
        serde_json::from_str(&text).map_err(|e| format!("traces.list invalid JSON: {}", e))?;
    Ok(body["result"]["data"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

async fn poll_traces_until<F>(
    token: &str,
    event_id: &str,
    predicate: F,
) -> Result<Vec<Value>, String>
where
    F: Fn(&[Value]) -> bool,
{
    let started = std::time::Instant::now();
    let mut last_spans = Vec::new();
    while started.elapsed() < POLL_TIMEOUT {
        let spans = query_traces(token, event_id).await?;
        if predicate(&spans) {
            return Ok(spans);
        }
        last_spans = spans;
        sleep(Duration::from_secs(5)).await;
    }
    Err(format!(
        "timed out waiting for traces for event {}; last seen {:?}",
        event_id, last_spans
    ))
}

#[tokio::test]
#[ignore = "requires live OpenAI, weather, and Raindrop credentials"]
async fn e2e_real_gpt_weather_tool_agent_lands_in_raindrop() {
    let Some((write_key, dashboard_token, openai_key, model)) = required_openai_env() else {
        eprintln!(
            "[e2e_real_agent] skipping: set RAINDROP_WRITE_KEY, RAINDROP_DASHBOARD_TOKEN, and OPENAI_API_KEY"
        );
        return;
    };

    let user_id = unique_user_id();
    let convo_id = format!("{}_convo", user_id);
    let prompt = "Use the get_weather tool to get the current weather in San Francisco, then answer in one short sentence with the temperature.";
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "real_weather_agent".into(),
            input: prompt.into(),
            model: model.clone(),
            ..Default::default()
        })
        .await;

    let root = interaction.start_span(SpanOptions {
        name: "weather.agent".into(),
        operation_id: "ai.workflow".into(),
        attributes: vec![raindrop::Attribute::string(
            "traceloop.span.kind",
            "workflow",
        )],
        ..Default::default()
    });

    let first_llm = interaction.start_llm_span(
        "openai.responses.tool_choice",
        LlmOptions {
            parent: Some(root.clone()),
            provider: "openai".into(),
            model: model.clone(),
            messages: vec![LlmMessage::user(prompt)],
            ..Default::default()
        },
    );
    let first_response = post_openai(
        &openai_key,
        json!({
            "model": model,
            "input": [
                { "role": "user", "content": prompt }
            ],
            "tools": [
                {
                    "type": "function",
                    "name": "get_weather",
                    "description": "Get the current weather for a city.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "city": {
                                "type": "string",
                                "description": "City name, e.g. San Francisco"
                            }
                        },
                        "required": ["city"],
                        "additionalProperties": false
                    }
                }
            ],
            "tool_choice": "auto"
        }),
    )
    .await
    .expect("first OpenAI response");
    let tool_call = find_tool_call(&first_response).unwrap_or_else(|| {
        panic!(
            "expected OpenAI function_call output, got {}",
            serde_json::to_string_pretty(&first_response).unwrap_or_default()
        )
    });
    let tool_arguments = tool_call["arguments"]
        .as_str()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| json!({}));
    first_llm.set_output(format!(
        "Requested {} with {}",
        tool_call["name"].as_str().unwrap_or("function"),
        tool_arguments
    ));
    let (first_input_tokens, first_output_tokens) = usage_tokens(&first_response);
    first_llm.set_token_usage(&model, first_input_tokens, first_output_tokens);
    first_llm.end();

    let weather_tool = interaction.start_tool_span(
        "get_weather",
        ToolOptions {
            parent: Some(root.clone()),
            input: Some(json!({
                "city": tool_arguments["city"].as_str().unwrap_or("San Francisco")
            })),
            ..Default::default()
        },
    );
    let weather = fetch_san_francisco_weather().await.expect("weather fetch");
    weather_tool.set_output(&weather);
    weather_tool.end();

    let second_llm = interaction.start_llm_span(
        "openai.responses.final_answer",
        LlmOptions {
            parent: Some(root.clone()),
            provider: "openai".into(),
            model: model.clone(),
            messages: vec![
                LlmMessage::user(prompt),
                LlmMessage::assistant(format!(
                    "Tool get_weather returned {}",
                    serde_json::to_string(&weather).unwrap_or_default()
                )),
            ],
            ..Default::default()
        },
    );
    let final_response = post_openai(
        &openai_key,
        json!({
            "model": model,
            "previous_response_id": first_response["id"],
            "input": [
                {
                    "type": "function_call_output",
                    "call_id": tool_call["call_id"],
                    "output": serde_json::to_string(&weather).unwrap_or_default()
                }
            ]
        }),
    )
    .await
    .expect("final OpenAI response");
    let answer = output_text(&final_response);
    assert!(!answer.trim().is_empty(), "OpenAI final answer was empty");
    second_llm.set_output(answer.clone());
    let (second_input_tokens, second_output_tokens) = usage_tokens(&final_response);
    second_llm.set_token_usage(&model, second_input_tokens, second_output_tokens);
    second_llm.end();
    root.end();

    let mut props = BTreeMap::new();
    props.insert("agent".into(), json!("real_weather_tool"));
    interaction
        .finish(FinishOptions {
            output: answer.clone(),
            model: model.clone(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("finish interaction");
    client.close().await.expect("close client");

    let event = poll_event_until(&dashboard_token, &user_id, |event| {
        event["aiData"]["output"].as_str() == Some(answer.as_str())
    })
    .await
    .expect("dashboard event");
    let event_id = event["id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| interaction.event_id().to_string());
    let spans = poll_traces_until(&dashboard_token, &event_id, |spans| {
        let has_first_llm = spans
            .iter()
            .any(|span| span["span_name"].as_str() == Some("openai.responses.tool_choice"));
        let has_tool = spans.iter().any(|span| {
            span["span_name"].as_str() == Some("get_weather")
                && span["span_type"].as_str() == Some("TOOL_CALL")
                && span["output_payload"]
                    .as_str()
                    .is_some_and(|output| output.contains("temperature_2m"))
        });
        let has_final_llm = spans.iter().any(|span| {
            span["span_name"].as_str() == Some("openai.responses.final_answer")
                && span["span_type"].as_str() == Some("LLM_GENERATION")
                && span["output_payload"]
                    .as_str()
                    .is_some_and(|output| !output.is_empty())
        });
        has_first_llm && has_tool && has_final_llm
    })
    .await
    .expect("dashboard traces");

    assert!(
        spans.len() >= 4,
        "expected workflow, two LLM spans, and weather tool span; got {:?}",
        spans
    );
}

#[tokio::test]
#[ignore = "requires live Anthropic, weather, and Raindrop credentials"]
async fn e2e_real_anthropic_weather_tool_agent_lands_in_raindrop() {
    let Some((write_key, dashboard_token, anthropic_key, model)) = required_anthropic_env() else {
        eprintln!(
            "[e2e_real_agent] skipping: set RAINDROP_WRITE_KEY, RAINDROP_DASHBOARD_TOKEN, and ANTHROPIC_API_KEY"
        );
        return;
    };

    let user_id = unique_user_id();
    let convo_id = format!("{}_convo", user_id);
    let prompt = "Use the get_weather tool to get the current weather in San Francisco, then answer in one short sentence with the temperature.";
    let client = build_client(&write_key);
    let interaction = client
        .begin(BeginOptions {
            user_id: user_id.clone(),
            convo_id: convo_id.clone(),
            event: "real_anthropic_weather_agent".into(),
            input: prompt.into(),
            model: model.clone(),
            ..Default::default()
        })
        .await;

    let root = interaction.start_span(SpanOptions {
        name: "weather.agent".into(),
        operation_id: "ai.workflow".into(),
        attributes: vec![raindrop::Attribute::string(
            "traceloop.span.kind",
            "workflow",
        )],
        ..Default::default()
    });

    let first_llm = interaction.start_llm_span(
        "anthropic.messages.tool_choice",
        LlmOptions {
            parent: Some(root.clone()),
            provider: "anthropic".into(),
            model: model.clone(),
            messages: vec![LlmMessage::user(prompt)],
            ..Default::default()
        },
    );
    let first_response = post_anthropic(
        &anthropic_key,
        json!({
            "model": model,
            "max_tokens": 512,
            "messages": [
                { "role": "user", "content": prompt }
            ],
            "tools": [
                {
                    "name": "get_weather",
                    "description": "Get the current weather for a city.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "city": {
                                "type": "string",
                                "description": "City name, e.g. San Francisco"
                            }
                        },
                        "required": ["city"],
                        "additionalProperties": false
                    }
                }
            ],
            "tool_choice": { "type": "tool", "name": "get_weather" }
        }),
    )
    .await
    .expect("first Anthropic response");
    let tool_use = find_anthropic_tool_use(&first_response).unwrap_or_else(|| {
        panic!(
            "expected Anthropic tool_use content, got {}",
            serde_json::to_string_pretty(&first_response).unwrap_or_default()
        )
    });
    let tool_arguments = tool_use["input"].clone();
    first_llm.set_output(format!(
        "Requested {} with {}",
        tool_use["name"].as_str().unwrap_or("tool"),
        tool_arguments
    ));
    let (first_input_tokens, first_output_tokens) = usage_tokens(&first_response);
    first_llm.set_token_usage(&model, first_input_tokens, first_output_tokens);
    first_llm.end();

    let weather_tool = interaction.start_tool_span(
        "get_weather",
        ToolOptions {
            parent: Some(root.clone()),
            input: Some(json!({
                "city": tool_arguments["city"].as_str().unwrap_or("San Francisco")
            })),
            ..Default::default()
        },
    );
    let weather = fetch_san_francisco_weather().await.expect("weather fetch");
    weather_tool.set_output(&weather);
    weather_tool.end();

    let second_llm = interaction.start_llm_span(
        "anthropic.messages.final_answer",
        LlmOptions {
            parent: Some(root.clone()),
            provider: "anthropic".into(),
            model: model.clone(),
            messages: vec![
                LlmMessage::user(prompt),
                LlmMessage::assistant(format!(
                    "Tool get_weather returned {}",
                    serde_json::to_string(&weather).unwrap_or_default()
                )),
            ],
            ..Default::default()
        },
    );
    let final_response = post_anthropic(
        &anthropic_key,
        json!({
            "model": model,
            "max_tokens": 512,
            "messages": [
                { "role": "user", "content": prompt },
                { "role": "assistant", "content": first_response["content"] },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_use["id"],
                            "content": serde_json::to_string(&weather).unwrap_or_default()
                        }
                    ]
                }
            ]
        }),
    )
    .await
    .expect("final Anthropic response");
    let answer = anthropic_output_text(&final_response);
    assert!(
        !answer.trim().is_empty(),
        "Anthropic final answer was empty"
    );
    second_llm.set_output(answer.clone());
    let (second_input_tokens, second_output_tokens) = usage_tokens(&final_response);
    second_llm.set_token_usage(&model, second_input_tokens, second_output_tokens);
    second_llm.end();
    root.end();

    let mut props = BTreeMap::new();
    props.insert("agent".into(), json!("real_anthropic_weather_tool"));
    interaction
        .finish(FinishOptions {
            output: answer.clone(),
            model: model.clone(),
            properties: props,
            ..Default::default()
        })
        .await
        .expect("finish interaction");
    client.close().await.expect("close client");

    let event = poll_event_until(&dashboard_token, &user_id, |event| {
        event["aiData"]["output"].as_str() == Some(answer.as_str())
    })
    .await
    .expect("dashboard event");
    let event_id = event["id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| interaction.event_id().to_string());
    let spans = poll_traces_until(&dashboard_token, &event_id, |spans| {
        let has_first_llm = spans
            .iter()
            .any(|span| span["span_name"].as_str() == Some("anthropic.messages.tool_choice"));
        let has_tool = spans.iter().any(|span| {
            span["span_name"].as_str() == Some("get_weather")
                && span["span_type"].as_str() == Some("TOOL_CALL")
                && span["output_payload"]
                    .as_str()
                    .is_some_and(|output| output.contains("temperature_2m"))
        });
        let has_final_llm = spans.iter().any(|span| {
            span["span_name"].as_str() == Some("anthropic.messages.final_answer")
                && span["span_type"].as_str() == Some("LLM_GENERATION")
                && span["output_payload"]
                    .as_str()
                    .is_some_and(|output| !output.is_empty())
        });
        has_first_llm && has_tool && has_final_llm
    })
    .await
    .expect("dashboard traces");

    assert!(
        spans.len() >= 4,
        "expected workflow, two LLM spans, and weather tool span; got {:?}",
        spans
    );
}
