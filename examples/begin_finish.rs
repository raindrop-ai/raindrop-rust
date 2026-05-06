use raindrop::{Attachment, BeginOptions, Client, FinishOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .write_key(std::env::var("RAINDROP_WRITE_KEY").unwrap_or_default())
        .build()?;

    let interaction = client
        .begin(BeginOptions {
            user_id: "user-123".into(),
            event: "chat_message".into(),
            input: "Hello!".into(),
            model: "gpt-4o".into(),
            convo_id: "conv-123".into(),
            ..Default::default()
        })
        .await;

    interaction.set_property("stage", "processing").await?;
    interaction
        .add_attachments(vec![Attachment {
            kind: "text".into(),
            role: "output".into(),
            name: "reasoning-summary".into(),
            value: "The user wants a simple friendly plan.".into(),
            ..Default::default()
        }])
        .await?;
    interaction
        .finish(FinishOptions {
            output: "Hi there!".into(),
            ..Default::default()
        })
        .await?;

    client.close().await?;
    Ok(())
}
