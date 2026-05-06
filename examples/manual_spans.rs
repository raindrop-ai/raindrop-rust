use raindrop::{Attribute, Client, SpanOptions};

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

    let llm_call = client.start_span(SpanOptions {
        name: "llm.call".into(),
        event_id: "evt_demo".into(),
        parent: Some(agent_run.clone()),
        ..Default::default()
    });
    llm_call.set_attributes([
        Attribute::string("ai.model.id", "gpt-4o"),
        Attribute::int("ai.usage.prompt_tokens", 10),
        Attribute::int("ai.usage.completion_tokens", 5),
    ]);
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
