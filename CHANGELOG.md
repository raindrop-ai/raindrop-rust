# Changelog

All notable changes to this crate are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), versioning follows [SemVer](https://semver.org).

## [Unreleased]

## [0.0.9] - 2026-07-16

### Added

- **Public `feature_flags` surface on events.** `AiEvent`, `Event`,
  `BeginOptions`, `PatchOptions`, and `FinishOptions` gain an optional
  `feature_flags: BTreeMap<String, String>` field, plus
  `Interaction::set_feature_flags` / `set_feature_flag` convenience methods.
  Flags serialize verbatim as a top-level `feature_flags` string→string object
  on the wire — a sibling of `ai_data` / `properties`, matching the JS SDK's
  event-shipper (dawn ingest `TrackEventSchema.feature_flags`). Flags supplied
  across a `begin`→`patch`→`finish` lifecycle merge like `properties` (last
  write wins per key). This is **additive-only**: callers that pass no flags
  omit the key entirely, so their request bodies are byte-identical to before
  (covered by `omitted_feature_flags_leave_body_unchanged`). The conformance
  driver declares the `events.feature_flags` capability and maps the harness
  `feature_flags` step arg through to the public API. (DEV-1214)

## [0.0.8] - 2026-06-26

### Added

- **`project_id` routing.** New `ClientBuilder::project_id(...)` attaches an
  `X-Raindrop-Project-Id` header to every outbound request (including the local
  Workshop mirror), routing telemetry to a named project. When unset the wire
  format is unchanged and the backend uses the org's default project. The slug
  is trimmed and validated against `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$` once
  at build time; an invalid value is logged and the header is omitted so a typo
  never breaks ingestion.

## [0.0.7] - 2026-06-11

Hardening release from the cross-SDK parity audit (mirrors python-sdk
`0.0.51`): telemetry must never stall the host's hot path, wedge its shutdown,
grow its memory without bound, or flood its logs.

### Fixed

- **Text fields are capped BEFORE serialization.** AI input/output, tool span
  I/O, LLM span content, and association property values are truncated up
  front with a `...[truncated by raindrop]` marker that fits **within** the
  cap (default 1,000,000 chars; new `ClientBuilder::max_text_field_chars`; a
  stricter `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT` is honored at build
  time). Structured payloads serialize through an output-budgeted writer that
  aborts when the budget is exhausted, so the cost of an oversized payload is
  proportional to the cap — not the payload — on the calling task. Oversized
  events now land truncated instead of being serialized in full and then
  silently dropped at the 1 MiB ingest limit. `with_tool` /
  `with_tool_async` no longer materialize an unbounded `serde_json::Value`
  of the result.
- **`flush()` no longer hangs on clients with periodic flushing enabled.**
  The never-ending periodic ticker tasks lived in the same task list that
  `flush()` drains and awaits, so any explicit `flush()` with default
  (non-zero) flush intervals blocked until `close()`. Tickers now live in a
  dedicated slot awaited only during `close()`.
- **Every cloud POST is bounded.** A per-request timeout (new
  `ClientBuilder::request_timeout`, default 30s) applies even when a
  caller-injected `http_client` was built without timeouts; the SDK-built
  client also sets a 5s connect timeout.
- **`close()` runs under a hard deadline.** New `ClientBuilder::close_timeout`
  (default 10s): stop signals, in-flight task draining, and the final flush
  share the budget; at the deadline remaining telemetry is dropped with a
  warning instead of wedging process exit on a dead or slow network.
- **Bounded memory under backpressure.** Completed fire-and-forget task
  handles (span enqueues, local-mirror POSTs) are pruned as new ones are
  spawned instead of accumulating until an explicit `flush()`. The event
  buffer's patch and sticky-context maps are bounded by new
  `ClientBuilder::event_max_queue_size` (default 5000): at capacity, new
  event ids are dropped with a rate-limited warning.
- **Failure logs are rate-limited.** Oversized-payload drops, empty-AI-event
  drops, and event-buffer overflow each log at most once per 30s per family
  instead of once per event.

## [0.0.6] - 2026-05-19

### Fixed

- **Drop phantom `ai_generation` events with empty `ai_input` and `ai_output`.**
  Finalized `track_partial` payloads (`is_pending=false`) that would have
  shipped to the backend with both `ai_input` and `ai_output` empty are now
  dropped at the buffer level with a single `tracing::warn!`. This catches
  wrapper authors that record `model` / `convo_id` / token-usage `properties`
  but never populate the prompt or response text, which previously surfaced
  in the dashboard as rows of empty `ai_generation` events. The drop is
  observable only via the warning log — there is no wire change and no
  public API change.

  Legitimate adjacent shapes are unaffected:
  * non-AI `track_event` calls with an explicit event name still ship,
  * attachment-only events with no AI text still ship (image upload events),
  * errored generations that ship the prompt as `input` (only `output` empty)
    still ship — the wrapper should attach an `LlmSpan` and call
    `set_error(...)` on it to carry the error detail; Dawn associates the
    error span with the event row by `event_id` via the `error_spans`
    extension, mirroring the JS SDK's `liveInteraction.setError` path,
  * pending intermediates (`is_pending=true`) still ship — the caller may
    follow up with a `finish` that populates the missing text fields.

## [0.0.5] - 2026-05-14

### Added

- **LLM span helpers.** `Client::start_llm_span`, `Interaction::start_llm_span`,
  `Tracer::start_llm_span`, `LlmSpan`, `LlmOptions`, and `LlmMessage` emit the
  prompt, completion, model, provider, and token attributes that Dawn's trace
  parser and span renderer understand.

<!--
Release process (no automation; everything is manual + reviewable):

1. Bump `version` in `Cargo.toml` and run `cargo build` so `Cargo.lock` updates.
2. Move the contents of `[Unreleased]` above into a new `[X.Y.Z] - YYYY-MM-DD`
   section and update the link refs at the bottom.
3. Run the full local quality gate:
     cargo fmt --check
     cargo clippy --all-targets --all-features -- -D warnings
     cargo test --all-features
     RUSTDOCFLAGS=-D\ warnings cargo doc --no-deps --all-features
4. Open a PR with the version bump + changelog. Merge once CI is green.
5. From `main` after the merge:
     git tag -a vX.Y.Z -m "..."
     git push origin vX.Y.Z
     gh release create vX.Y.Z --title "..." --notes "..."
6. Update the README install snippet's `tag = "vX.Y.Z"`.

When ready to publish to crates.io: flip `publish = true` in `Cargo.toml`,
add `CRATES_IO_TOKEN` to repo secrets, and `cargo publish` (or re-introduce
release-plz at that point — it's overkill until then).
-->

## [0.0.4] - 2026-05-11

### Added

- **Local Workshop mirroring (`local_workshop_url`).** New additive config slot
  for fanning every cloud-bound POST out to a local Raindrop Workshop daemon
  in addition to (not instead of) the cloud endpoint. Resolution precedence,
  highest first: `.local_workshop_url(...)` builder call → `.disable_local_workshop()`
  builder call (explicit opt-out) → `RAINDROP_LOCAL_DEBUGGER` env (URL) →
  `RAINDROP_WORKSHOP` env (URL or boolean truthy/falsy: `1`/`true`/`yes`/`on`
  enables the default `http://localhost:5899/v1/`; `0`/`false`/`no`/`off`
  disables) → 100 ms TCP probe of `127.0.0.1:5899` → disabled. Local POSTs use
  a 2 s timeout, no retries, errors only at `tracing::debug!` so a missing
  daemon never bubbles up. Mirrors the Python and TS contract (`raindrop-js`
  PR #52, Python SDK `raindrop/local_debugger.py`).
- **No-cloud-without-key.** Empty `write_key` + a resolved `local_workshop_url`
  ships to local only and skips the cloud entirely; the no-key + no-local case
  remains a no-op.

## [0.0.3] - 2026-05-09

Dependency cleanup. No consumer-facing API or feature-flag changes.

### Changed

- **Drop the `futures` umbrella dependency.** The crate previously pulled
  `futures = "0.3"` (and its transitive `futures-channel`,
  `futures-executor`, `futures-io`, `futures-macro`, `futures-sink`,
  `futures-task` sub-crates) just for one `futures::future::BoxFuture`
  type alias inside an internal sleep-hook signature in `src/http.rs`.
  Inline the underlying
  `Pin<Box<dyn Future<Output = ()> + Send + 'static>>` directly so
  consumers no longer pull `futures` (and its sub-crates) for one alias.
  No public-API change — the `SleepFn` alias is a private (module-local)
  type used only by the test-injection hook.

## [0.0.2] - 2026-05-07

Dependency upgrade: bump `reqwest` from `0.12` to `0.13`. No consumer-facing
API or feature-flag changes.

### Changed

- **`reqwest` 0.12 → 0.13.** Picks up the new TLS backend defaults: rustls'
  crypto provider is now `aws-lc-rs` (was `ring`), and root certificates come
  from `rustls-platform-verifier` (replacing the previous webpki/native roots
  features). No SDK code changes were required — all reqwest API surface we
  use (`Client::builder`, `post`, `header`, `body`, `send`, `status`,
  `headers`, `text`, `HeaderMap`, `StatusCode`) is unchanged. See
  [reqwest's 0.13 release notes](https://github.com/seanmonstar/reqwest/blob/master/CHANGELOG.md#v0130)
  for the full list of upstream breaking changes.
- **Public Cargo features are unchanged** (`rustls-tls`, `native-tls`).
  Reqwest renamed its internal `rustls-tls` feature to `rustls` in 0.13, but
  this crate's feature names are part of its public API surface and are kept
  stable across reqwest upgrades — only the RHS of the feature mapping
  follows the rename. Consumers' `Cargo.toml` files do not need to change.

## [0.0.1] - 2026-05-06

Initial **beta** release. The wire contract against the Raindrop ingestion API is stable and verified end-to-end against the live backend on every push; the Rust crate API may still change in minor ways before `0.1.0`.

### Added

- **Client + buffering.** `Client::builder()` with the full configuration surface (`write_key`, `endpoint`, `debug`, `partial_flush_interval`, `trace_flush_interval`, `trace_max_batch_size`, `trace_max_queue_size`, `max_attempts`, `base_delay`, `jitter_fraction`, `service_name`, `library_name`, `library_version`, `http_client`). Empty `write_key` makes the client a no-op (zero HTTP).
- **Events API.** `Client::track_ai`, `Client::track_event`, `Client::begin` → `Interaction { set_input, set_property, set_properties, add_attachments, finish, patch }` lifecycle, `Client::resume_interaction`, and `Client::patch` / `Client::finish` for direct partial control.
- **Tracing API.** `Client::start_span` (manual spans with parent linkage), `Interaction::start_span` / `start_tool_span` (auto-inheriting `traceloop.association.properties.{user_id,convo_id,event}`), `Interaction::track_tool` (retroactive tool calls), `Client::tracer` for standalone tracers, and the closure-style `with_span`, `with_tool`, `with_tool_async` helpers.
- **`Span::set_token_usage`** / **`ToolSpan::set_token_usage`** — emit canonical OpenTelemetry GenAI attributes (`gen_ai.response.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`) so backend per-event token totals populate.
- **`Attachment.attachment_id`** (optional) — round-trips on the wire so callers can pre-assign UUIDs and follow-up `Signal { attachment_id }` can reference the attachment.
- **`SignalKind` constants** documenting all six accepted `signal_type` values: `default`, `standard`, `feedback`, `edit`, `agent`, `agent_internal`.
- **Crash protection.** Every public method returns `Result` or is infallible. Telemetry serialization failures fall back to `String`/empty representations.
- **HTTP transport.** `reqwest`-based with bearer auth, exponential backoff with ±20% jitter, `Retry-After` honoring (numeric + IMF-fixdate), and configurable retry / timeout knobs.
- **OTLP/JSON encoding.** OTLP-compatible spans with proper `intValue` string encoding, 16-byte `traceId` / 8-byte `spanId` base64, parent linkage via `parentSpanId`.
- **1 MiB max-ingest-size guard** matching the Python and JS SDKs (`max_ingest_size_bytes` / `MAX_INGEST_SIZE_BYTES`). Oversized payloads are dropped client-side with an unconditional `tracing::warn!`.
- **Defensive duration clamp.** Spans with `end_time < start_time` are clamped to zero duration so the SDK never emits negative `duration_ns`.
- **Tests.** 94 unit / integration tests + 14 e2e dashboard-verification tests (env-gated). Live e2e run: 10 / 11 active tests pass (1 documents a backend `events.toolCalls[].duration_ms` i8-truncation bug; SDK ships correct timestamps).
- **CI.** Test (stable), MSRV (1.88, `--locked`), feature combinations (`rustls-tls`, `native-tls`), `cargo doc -D warnings`, `cargo clippy --all-targets --all-features -D warnings`, Cursor Bugbot, Cursor Security Reviewer, Devin Review — all green. Third-party actions pinned to immutable commit SHAs.

### Known Limitations

- No automatic LLM-client instrumentation (no `async-openai` / `langchain-rust` hooks). All spans are manual.
- No client-side PII redaction (Python's `set_redact_pii` and JS's `redactPii` have no Rust equivalent yet).
- No local-debugger mirroring (no `RAINDROP_LOCAL_DEBUGGER` support).

[Unreleased]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.8...HEAD
[0.0.8]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/raindrop-ai/raindrop-rust/releases/tag/v0.0.1
