use raindrop::{Attribute, Client, LlmMessage, LlmOptions, SpanOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .write_key(std::env::var("RAINDROP_WRITE_KEY").unwrap_or_default())
        .build()?;

    let agent_run = client.start_span(SpanOptions {
        name: "agent.run".into(),
        event_id: "evt_demo".into(),
        ..Default::default()
    });
    agent_run.set_attributes([Attribute::string("agent.kind", "planning")]);

    let llm_call = client.start_llm_span(
        "llm.call",
        LlmOptions {
            parent: Some(agent_run.clone()),
            model: "gpt-4o".into(),
            messages: vec![LlmMessage::user("Draft a plan for the user.")],
            ..Default::default()
        },
        "evt_demo",
    );
    llm_call.set_output("Here is a short plan.");
    llm_call.set_token_usage("gpt-4o", 10, 5);
    llm_call.end();

    let retrieval = client.start_span(SpanOptions {
        name: "retrieval.search".into(),
        event_id: "evt_demo".into(),
        parent: Some(agent_run.clone()),
        ..Default::default()
    });
    retrieval.set_attributes([Attribute::int("retrieval.results", 8)]);
    retrieval.end();

    agent_run.end();
    client.close().await?;
    Ok(())
}
