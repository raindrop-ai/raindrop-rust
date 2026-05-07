# Changelog

All notable changes to this crate are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), versioning follows [SemVer](https://semver.org).

## [Unreleased]

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

[Unreleased]: https://github.com/raindrop-ai/raindrop-rust/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/raindrop-ai/raindrop-rust/releases/tag/v0.0.1
