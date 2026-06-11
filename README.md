# Raindrop Rust SDK (Beta)

The official Rust SDK for [Raindrop AI](https://raindrop.ai) — track AI events, collect user signals, and instrument LLM applications with OpenTelemetry-based tracing.

📖 **Full documentation:** [docs.raindrop.ai/sdk/rust](https://docs.raindrop.ai/sdk/rust). This README is the quick reference; the docs page is the canonical narrative tour.

> **Beta.** The crate is `0.0.6`. The wire contract against the Raindrop ingestion API is stable and verified end-to-end against the live backend on every push, but the crate API may still change in minor ways before `0.1.0`. We recommend pinning the git revision in your `Cargo.toml` and reviewing the [Known Limitations](#known-limitations) before using it in production.

## Installation

> The crate is not yet published to crates.io. Install via git for now:

```toml
[dependencies]
raindrop-ai = { git = "https://github.com/raindrop-ai/raindrop-rust", tag = "v0.0.6" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

Track the latest tagged release at [github.com/raindrop-ai/raindrop-rust/releases](https://github.com/raindrop-ai/raindrop-rust/releases). For development against the bleeding edge, drop the `tag` field to follow `main`.

The Rust SDK requires **Rust 1.88+** (MSRV). It is `async`-first and uses `tokio`. Most fallible methods return `Result<_, Error>` — `track_ai`, `track_event`, `identify`, `track_signal`, the `Interaction` mutators (`set_input`, `set_property`, `set_properties`, `add_attachments`, `patch`, `finish`), and `Client::flush` / `Client::close`. Propagate errors with `?` as you would for any fallible call. The two constructors — `Client::begin(...).await` and `Client::resume_interaction(...)` — are **infallible**: they always return an `Interaction` (a no-op handle when the client is disabled), so don't put a `?` on those.

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
use raindrop::{LlmMessage, LlmOptions, SpanOptions};

let parent = client.start_span(SpanOptions {
    name: "agent.run".into(),
    event_id: "evt_123".into(),
    ..Default::default()
});

let child = client.start_llm_span(
    "llm.call",
    LlmOptions {
        parent: Some(parent.clone()),
        model: "gpt-4o".into(),
        messages: vec![LlmMessage::user("What is the capital of France?")],
        ..Default::default()
    },
    "evt_123",
);

child.set_output("Paris");
child.set_token_usage("gpt-4o", /* input */ 47, /* output */ 11);
child.end();

// You can also end a span with an explicit time, e.g. when wrapping a call you've already made.
parent.end();
```

Use `LlmOptions::messages` when the provider call takes chat-style role/content messages.
`LlmMessage::system`, `LlmMessage::user`, and `LlmMessage::assistant` cover the common roles;
`LlmMessage::new(role, content)` handles provider-specific roles. If both `input` and `messages`
are set, `messages` wins. The dashboard stores the last user message as the span `input_payload`
and the full message array remains available to the frontend span renderer.

```rust
let llm = interaction.start_llm_span(
    "anthropic.messages",
    LlmOptions {
        provider: "anthropic".into(),
        model: "claude-sonnet-4-5".into(),
        ..Default::default()
    },
);

llm.set_messages([
    LlmMessage::system("You answer using the provided tool result."),
    LlmMessage::user("What is the weather in San Francisco?"),
    LlmMessage::assistant("I will call get_weather."),
    LlmMessage::user("The tool returned 67°F and windy."),
]);
llm.set_output("It is 67°F and windy in San Francisco.");
llm.end();
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
use raindrop::{Signal, SignalKind, User};

client.track_signal(Signal {
    event_id: "evt_123".into(),
    name: "thumbs_up".into(),
    kind: SignalKind::FEEDBACK.into(),  // also: DEFAULT, STANDARD, EDIT, AGENT, AGENT_INTERNAL
    sentiment: "POSITIVE".into(),
    comment: "Great answer".into(),
    ..Default::default()
}).await?;

client.identify(User {
    user_id: "user-123".into(),
    traits: BTreeMap::from([("plan".into(), json!("pro"))]),
}).await?;
```

The wire field is `signal_type` and the canonical accepted values are `default`,
`standard`, `feedback`, `edit`, `agent`, and `agent_internal` — typed constants
are exposed via the `SignalKind` re-export at the crate root.

## Span association properties (auto-propagated from interaction)

Every span started via `interaction.start_span(...)`, `interaction.start_llm_span(...)`, or `interaction.start_tool_span(...)`
automatically inherits the interaction's `user_id`, `convo_id`, and `event` as
`traceloop.association.properties.{user_id, convo_id, event}` attributes, so the dashboard
groups the span under the same user, conversation, and event as the parent. User-supplied
properties always take precedence:

```rust
let interaction = client.begin(BeginOptions {
    user_id: "user-123".into(),
    convo_id: "conv-456".into(),
    event: "agent_run".into(),
    ..Default::default()
}).await;

// This span carries `traceloop.association.properties.{user_id, convo_id, event}` automatically.
let span = interaction.start_span(SpanOptions { name: "step".into(), ..Default::default() });
span.end();
```

For standalone spans created via `client.start_span(...)`, set `operation_id` (e.g.
`"ai.workflow"`) or pass `properties` so the span has at least one of the attributes
accepted by the backend's ingestion filter (`ai.operationId`, `traceloop.span.kind`,
`traceloop.workflow.name`, `traceloop.association.properties.*`, or `gen_ai.*`). Spans
that don't pass that filter are silently dropped. Plain `client.start_span(...)` calls with
just `name` + `event_id` automatically emit `traceloop.association.properties.event_id` so
they pass.

## Attachments

Attachments are split by `role` into the dashboard's `inputAttachments[]` and
`outputAttachments[]`. The wire schema accepts four `kind` values: `"text"`, `"code"`,
`"image"`, `"iframe"`. (The dashboard's display schema only renders `text | image | iframe`
— `code` survives ingestion but is filtered from the visual attachments tab.)

```rust
use raindrop::Attachment;

let attachment = Attachment {
    kind: "image".into(),
    role: "input".into(),
    name: "screenshot.png".into(),
    value: "https://cdn.example/img.png".into(),
    // Optional: pre-assign an attachment_id so a follow-up `Signal { attachment_id }`
    // can reference it. If empty, the backend auto-assigns a UUID.
    attachment_id: "att-abc-123".into(),
    ..Default::default()
};
```

## Token usage

`Span::set_token_usage(model, input_tokens, output_tokens)` emits the canonical
OpenTelemetry GenAI semantic-convention attributes (`gen_ai.response.model`,
`gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`) so the Raindrop backend
correctly populates per-span and per-event token totals on the dashboard. Pass `0` for
either count or an empty `model` to omit the corresponding attribute.

## Payload size limits

As of `0.0.7`, text fields (AI input/output, tool span I/O, LLM span content)
are capped at **1,000,000 characters per field by default** and truncated with a
`...[truncated by raindrop]` marker (the marker fits within the cap). The cap
is enforced before (or during) serialization, so oversized payloads cost the
cap — not the payload — on your calling task, and large events now land
truncated instead of being silently dropped at the 1 MiB ingest limit. Tune it
via:

```rust
let client = raindrop::Client::builder()
    .write_key("rk_...")
    .max_text_field_chars(250_000)
    .build()?;
```

A stricter `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT` env var (read once at
build time) is also honored. All outbound HTTP carries finite per-request
timeouts (even with a caller-injected `http_client`), and `close()` runs under
a 10s deadline (`close_timeout`) so a dead network can never wedge your
process exit.

## Known Limitations

- **Nested Trace Spans:** The Rust SDK currently provides manual span instrumentation (`start_span`, `start_llm_span`, `start_tool_span`). It does not yet automatically hook into Rust LLM frameworks (like `async-openai` or `langchain-rust`) to produce nested trace spans automatically. You must create spans manually.
- **PII Redaction:** Automatic PII redaction (which is available in the Python SDK via `set_redact_pii` and the JS SDK via `redactPii`) is not yet implemented in the Rust SDK. If your application logs PII into events, redact at the call site or upstream of `track_ai` / `track_event`.
- **Oversized payload guard:** Payloads larger than 1 MiB after JSON serialization are dropped client-side (matching the JS / Python SDKs' `MAX_INGEST_SIZE_BYTES` / `max_ingest_size_bytes`) to avoid 413s on the gateway. With the default text-field caps this only triggers for oversized non-text content (e.g. huge property maps or attachments). Each drop emits a rate-limited `tracing::warn!` event so production callers can detect it without enabling `debug=true`.

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
| `event_max_queue_size`   | `5000`                        | Max distinct buffered event ids                   |
| `max_attempts`           | `3`                           | HTTP retries (1 = no retries)                     |
| `base_delay`             | `1s`                          | Backoff base (exponential, ±20% jitter)           |
| `jitter_fraction`        | `0.2`                         | Backoff jitter fraction (0.0–1.0)                 |
| `request_timeout`        | `10s`                         | Per-attempt bound on every cloud POST             |
| `close_timeout`          | `10s`                         | Overall `close()` deadline. `0` disables          |
| `max_text_field_chars`   | `1_000_000`                   | Per-field cap on AI text content                  |
| `service_name`           | `raindrop.rust-sdk`           | OTLP `resource.service.name`                      |
| `library_name`           | `raindrop-rust`               | `$context.library.name`                           |
| `library_version`        | crate version                 | `$context.library.version`                        |
| `http_client`            | new `reqwest::Client`         | Bring your own connection-pooled HTTP client      |
| `local_workshop_url`     | auto-detected localhost       | Mirror cloud-bound posts to a local Workshop      |
| `disable_local_workshop` | —                             | Disable env/probe-based local Workshop mirroring  |

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
`cargo doc` (with warnings as errors), MSRV (`1.88`), and feature combinations
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
