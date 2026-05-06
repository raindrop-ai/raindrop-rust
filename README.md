# Raindrop Rust SDK

The official Rust SDK for [Raindrop AI](https://raindrop.ai) — track AI events, collect user signals, and instrument LLM applications with OpenTelemetry-based tracing.


## Installation

> The crate is not yet published to crates.io. Install via git for now:

```toml
[dependencies]
raindrop-ai = { git = "https://github.com/invisible-tools/raindrop-rust" }
```

## Quick start

```rust
use raindrop::{AiEvent, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .write_key(std::env::var("RAINDROP_WRITE_KEY").unwrap_or_default())
        .build()?;

    client.track_ai(AiEvent {
        user_id: "user-123".into(),
        event: "ai_generation".into(),
        input: "What is the capital of France?".into(),
        output: "Paris".into(),
        model: "gpt-4o".into(),
        ..Default::default()
    }).await?;

    client.close().await?;
    Ok(())
}
```

## Interactions (begin → patch → finish)

For multi-stage interactions where the final response is not yet available:

```rust
use raindrop::{BeginOptions, FinishOptions};

let interaction = client.begin(BeginOptions {
    user_id: "user-123".into(),
    event: "chat_message".into(),
    input: "Hello!".into(),
    model: "gpt-4o".into(),
    ..Default::default()
}).await;

interaction.set_property("stage", "processing").await?;
interaction.finish(FinishOptions { output: "Hi there!".into(), ..Default::default() }).await?;
```

## Manual span instrumentation

Spans are first-class and **manual** — no callbacks required. You can build an arbitrarily deep
trace tree by passing the parent span via `SpanOptions::parent`:

```rust
use raindrop::{Attribute, SpanOptions};

let parent = client.start_span(SpanOptions {
    name: "agent.run".into(),
    event_id: "evt_123".into(),
    ..Default::default()
});

let child = client.start_span(SpanOptions {
    name: "llm.call".into(),
    event_id: "evt_123".into(),
    parent: Some(parent.clone()),
    ..Default::default()
});

child.set_attributes([
    Attribute::string("ai.model.id", "gpt-4o"),
    Attribute::int("ai.usage.prompt_tokens", 10),
]);
child.end();

// You can also end a span with an explicit time, e.g. when wrapping a call you've already made.
parent.end();
```

To attach a span to an in-flight interaction (so it inherits user/event/convo association
properties automatically):

```rust
let interaction = client.begin(BeginOptions {
    event_id: "evt_123".into(),
    user_id: "user-123".into(),
    ..Default::default()
}).await;

let span = interaction.start_span(SpanOptions {
    name: "rag.retrieve".into(),
    ..Default::default()
});
// ... do work ...
span.end();
```

Marking errors:

```rust
let span = client.start_span(SpanOptions { name: "ext.api".into(), event_id: "evt".into(), ..Default::default() });
match call_external_service().await {
    Ok(_) => span.end(),
    Err(err) => {
        span.set_error(err.to_string());
        span.end();
    }
}
```

### Optional callback-style helpers

If you prefer scoped instrumentation, use `with_span` (closure receives the span):

```rust
interaction.with_span::<_, _, _, std::io::Error>(
    SpanOptions { name: "summarize".into(), ..Default::default() },
    |span| async move {
        span.set_attributes([Attribute::string("phase", "draft")]);
        Ok::<_, std::io::Error>(())
    },
).await?;
```

## Tool spans

Tool calls have a dedicated wire format (`traceloop.span.kind=tool`) and helpers:

```rust
use raindrop::ToolOptions;
use serde_json::json;

let tool = interaction.start_tool_span("weather_lookup", ToolOptions {
    input: Some(json!({ "location": "San Francisco" })),
    ..Default::default()
});
tool.set_output(&json!({ "forecast": "sunny" }));
tool.end();
```

Or to retroactively log an already-completed call:

```rust
use raindrop::TrackToolOptions;
use std::time::Duration;

interaction.track_tool(TrackToolOptions {
    name: "coffee_search".into(),
    input: Some(json!({ "query": "best coffee" })),
    output: Some(json!({ "winner": "Ritual" })),
    duration: Some(Duration::from_millis(125)),
    ..Default::default()
});
```

For functional wrapping, use the free helper:

```rust
let result = raindrop::with_tool::<_, _, std::io::Error>(
    &interaction,
    "park_check",
    ToolOptions {
        input: Some(json!({ "location": "Dolores Park" })),
        ..Default::default()
    },
    || Ok(json!({ "recommendation": "yes" })),
)?;
```

## Standalone tracers

For background jobs that don't have an interaction id, use a `Tracer` with sticky association
properties merged into every span:

```rust
use std::collections::BTreeMap;
use serde_json::json;

let mut sticky = BTreeMap::new();
sticky.insert("job_id".into(), json!("batch-123"));
let tracer = client.tracer(sticky);

let span = tracer.start_span(SpanOptions { name: "embed".into(), ..Default::default() });
span.end();
```

## Signals and identify

```rust
use std::collections::BTreeMap;
use serde_json::json;
use raindrop::{Signal, User};

client.track_signal(Signal {
    event_id: "evt_123".into(),
    name: "thumbs_up".into(),
    kind: "feedback".into(),
    sentiment: "POSITIVE".into(),
    comment: "Great answer".into(),
    ..Default::default()
}).await?;

client.identify(User {
    user_id: "user-123".into(),
    traits: BTreeMap::from([("plan".into(), json!("pro"))]),
}).await?;
```

## Known Limitations

- **Nested Trace Spans:** The Rust SDK currently provides manual span instrumentation (`start_span`, `start_tool_span`). It does not yet automatically hook into Rust LLM frameworks (like `async-openai` or `langchain-rust`) to produce nested trace spans automatically. You must create spans manually.
- **PII Redaction:** Automatic PII redaction (which is available in the Python SDK via `set_redact_pii`) is not yet implemented in the Rust SDK.

## Configuration

| Builder method           | Default                       | Description                                       |
| ------------------------ | ----------------------------- | ------------------------------------------------- |
| `write_key`              | `""`                          | Empty/missing key → SDK is disabled (no-op)       |
| `endpoint`               | `https://api.raindrop.ai/v1/` | Base URL                                          |
| `debug`                  | `false`                       | Verbose debug logging via `tracing`               |
| `partial_flush_interval` | `1s`                          | Periodic event flush. `0` disables periodic flush |
| `trace_flush_interval`   | `1s`                          | Periodic span flush. `0` disables periodic flush  |
| `trace_max_batch_size`   | `50`                          | Max spans per export request                      |
| `trace_max_queue_size`   | `5000`                        | Backpressure threshold for spans                  |
| `max_attempts`           | `3`                           | HTTP retries (1 = no retries)                     |
| `base_delay`             | `1s`                          | Backoff base (exponential, ±20% jitter)           |
| `jitter_fraction`        | `0.2`                         | Backoff jitter fraction (0.0–1.0)                 |
| `service_name`           | `raindrop.rust-sdk`           | OTLP `resource.service.name`                      |
| `library_name`           | `raindrop-rust`               | `$context.library.name`                           |
| `library_version`        | crate version                 | `$context.library.version`                        |
| `http_client`            | new `reqwest::Client`         | Bring your own connection-pooled HTTP client      |

## Architecture

- **Buffering**: per-event-id map with sticky state. Patches with `is_pending=false` flush
  immediately; periodic ticker flushes pending patches at `partial_flush_interval`. If a payload
  cannot be built (no `user_id` yet), the patch is buffered and retried on the next flush.
- **Tracing**: bounded queue (`trace_max_queue_size`) with size-triggered (`trace_max_batch_size`)
  and time-triggered (`trace_flush_interval`) export. Spans are restored to the queue on transient
  failure.
- **HTTP**: retry on `5xx` and `429` only; `Retry-After` header is honored. Non-retryable `4xx`
  fail fast. Exponential backoff with ±20% jitter by default.
- **Crash protection**: every public method returns `Result` or is infallible. Telemetry calls
  never panic; serialization failures fall back to `String`/empty representations.

## Testing

The unit and integration test suite is built around `wiremock` and validates
the wire payload shape end-to-end. Run it locally with:

```bash
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
```

CI runs the full matrix on every push: `cargo test`, `cargo clippy`, `cargo fmt`,
`cargo doc` (with warnings as errors), MSRV (`1.75`), and feature combinations
(`rustls-tls`, `native-tls`).

### End-to-end tests against a live backend

`tests/e2e.rs` exercises `track_ai`, interactions, tool spans, signals, and
identify against a real Raindrop ingestion endpoint and verifies the data lands
on the dashboard. They are skipped automatically when the required environment
variables are not set, so they are safe to leave enabled in `cargo test`. To
opt in:

```bash
RAINDROP_WRITE_KEY=rk_... \
RAINDROP_DASHBOARD_TOKEN=eyJ... \
cargo test --test e2e
```

Optional overrides: `RAINDROP_ENDPOINT` (ingestion API base URL),
`RAINDROP_BACKEND_URL` (dashboard TRPC base URL).

## License

MIT
