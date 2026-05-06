use std::collections::BTreeMap;

use raindrop::{AiEvent, Client};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .write_key(std::env::var("RAINDROP_WRITE_KEY").unwrap_or_default())
        .build()?;

    let mut props = BTreeMap::new();
    props.insert("ai.usage.prompt_tokens".into(), json!(10));
    props.insert("ai.usage.completion_tokens".into(), json!(5));

    client
        .track_ai(AiEvent {
            user_id: "user-123".into(),
            event: "ai_generation".into(),
            input: "What is the capital of France?".into(),
            output: "Paris".into(),
            model: "gpt-4o".into(),
            convo_id: "conv-123".into(),
            properties: props,
            ..Default::default()
        })
        .await?;

    client.close().await?;
    Ok(())
}
