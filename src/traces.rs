use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;

use crate::client::Client;
use crate::helpers::{
    capped_string, merge_maps, stringify_serialize_bounded, stringify_value_bounded,
    to_string_bounded, truncate_text_in_place, unix_nanos_string,
};
use crate::otlp::{Attribute, OtlpKeyValue, OtlpSpan, OtlpStatus, SpanIds, SpanStatusCode};

/// Options for [`Client::start_span`].
#[derive(Debug, Default, Clone)]
pub struct SpanOptions {
    /// Span display name.
    pub name: String,
    /// Optional event id this span belongs to.
    pub event_id: String,
    /// Optional operation id (e.g. "ai.toolCall", "ai.workflow"). Required for the span to survive backend ingestion filters if no other `ai.*` or `traceloop.*` attributes are present.
    pub operation_id: String,
    /// Optional parent span. If `None`, a new trace is created.
    pub parent: Option<Span>,
    /// Association properties (will be flattened to `traceloop.association.properties.<key>`).
    pub properties: BTreeMap<String, Value>,
    /// Initial attributes.
    pub attributes: Vec<Attribute>,
    /// Override start time. Defaults to `now()`.
    pub start_time: Option<OffsetDateTime>,
}

/// Options for [`crate::events::Interaction::start_tool_span`].
#[derive(Debug, Default, Clone)]
pub struct ToolOptions {
    /// Optional parent span.
    pub parent: Option<Span>,
    /// Association properties.
    pub properties: BTreeMap<String, Value>,
    /// Tool input (any JSON value).
    pub input: Option<Value>,
    /// Override start time.
    pub start_time: Option<OffsetDateTime>,
}

/// Options for retroactive tool tracking via [`crate::events::Interaction::track_tool`] /
/// [`Tracer::track_tool`].
#[derive(Debug, Default, Clone)]
pub struct TrackToolOptions {
    /// Tool name (becomes the span name).
    pub name: String,
    /// Optional parent span.
    pub parent: Option<Span>,
    /// Tool input.
    pub input: Option<Value>,
    /// Tool output.
    pub output: Option<Value>,
    /// Optional error string. If set, the span is marked as ERROR.
    pub error: Option<String>,
    /// Association properties.
    pub properties: BTreeMap<String, Value>,
    /// Optional start time. Defaults to `end - duration` if `duration` is set, else `now() - duration`.
    pub start_time: Option<OffsetDateTime>,
    /// Optional explicit end time.
    pub end_time: Option<OffsetDateTime>,
    /// Optional explicit duration. Used to derive missing start/end times and the
    /// `traceloop.entity.duration_ms` attribute.
    pub duration: Option<std::time::Duration>,
}

/// A chat message recorded on an LLM span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LlmMessage {
    pub role: String,
    pub content: String,
}

impl LlmMessage {
    pub fn new(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self::new("system", content)
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::new("user", content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new("assistant", content)
    }
}

/// Options for [`crate::events::Interaction::start_llm_span`].
#[derive(Debug, Default, Clone)]
pub struct LlmOptions {
    /// Optional parent span.
    pub parent: Option<Span>,
    /// Association properties.
    pub properties: BTreeMap<String, Value>,
    /// Operation id. Defaults to `ai.generateText`.
    pub operation_id: String,
    /// Optional model name.
    pub model: String,
    /// Optional provider/system name.
    pub provider: String,
    /// Optional single user input.
    pub input: Option<String>,
    /// Optional chat-style prompt messages. Takes precedence over `input`.
    pub messages: Vec<LlmMessage>,
    /// Optional assistant output.
    pub output: Option<String>,
    /// Optional input token count. `0` omits the attribute.
    pub input_tokens: i64,
    /// Optional output token count. `0` omits the attribute.
    pub output_tokens: i64,
    /// Override start time.
    pub start_time: Option<OffsetDateTime>,
}

/// Manually-managed span. Cheap to clone (internally an `Arc`); safe to call from multiple tasks.
#[derive(Debug, Clone)]
pub struct Span {
    inner: Option<Arc<SpanInner>>,
}

#[derive(Debug)]
struct SpanInner {
    client: Client,
    ids: SpanIds,
    name: String,
    event_id: String,
    start: OffsetDateTime,
    state: Mutex<SpanState>,
}

#[derive(Debug, Default)]
struct SpanState {
    attrs: Vec<Attribute>,
    status: Option<OtlpStatus>,
    ended: bool,
}

impl Span {
    /// Construct a no-op span (used when the client is disabled).
    pub fn noop() -> Self {
        Self { inner: None }
    }

    pub(crate) fn new(
        client: Client,
        ids: SpanIds,
        name: String,
        event_id: String,
        start: OffsetDateTime,
        attrs: Vec<Attribute>,
    ) -> Self {
        Self {
            inner: Some(Arc::new(SpanInner {
                client,
                ids,
                name,
                event_id,
                start,
                state: Mutex::new(SpanState {
                    attrs,
                    status: None,
                    ended: false,
                }),
            })),
        }
    }

    /// Returns true if this span is a no-op (disabled client).
    pub fn is_noop(&self) -> bool {
        self.inner.is_none()
    }

    /// Span name as configured.
    pub fn name(&self) -> Option<&str> {
        self.inner.as_ref().map(|i| i.name.as_str())
    }

    pub(crate) fn ids(&self) -> Option<SpanIds> {
        self.inner.as_ref().map(|i| i.ids.clone())
    }

    /// Effective per-field character cap of the owning client (default for
    /// no-op spans, which never serialize anything anyway).
    pub(crate) fn text_limit(&self) -> usize {
        self.inner
            .as_ref()
            .map(|i| i.client.max_text_field_chars())
            .unwrap_or(crate::client::DEFAULT_MAX_TEXT_FIELD_CHARS)
    }

    /// Append attributes to the span. Safe to call after `end()` (no-op).
    pub fn set_attributes<I>(&self, attrs: I)
    where
        I: IntoIterator<Item = Attribute>,
    {
        if let Some(inner) = &self.inner {
            let mut state = inner.state.lock().expect("span lock poisoned");
            if state.ended {
                return;
            }
            state.attrs.extend(attrs);
        }
    }

    fn set_attributes_replacing<I>(&self, exact_keys: &[&str], prefixes: &[&str], attrs: I)
    where
        I: IntoIterator<Item = Attribute>,
    {
        if let Some(inner) = &self.inner {
            let mut state = inner.state.lock().expect("span lock poisoned");
            if state.ended {
                return;
            }
            state.attrs.retain(|attr| {
                !exact_keys.iter().any(|key| attr.key == *key)
                    && !prefixes.iter().any(|prefix| attr.key.starts_with(prefix))
            });
            state.attrs.extend(attrs);
        }
    }

    /// Mark the span as failed with the given message.
    pub fn set_error(&self, message: impl Into<String>) {
        if let Some(inner) = &self.inner {
            let mut state = inner.state.lock().expect("span lock poisoned");
            if state.ended {
                return;
            }
            state.status = Some(OtlpStatus {
                code: SpanStatusCode::Error as u8,
                message: message.into(),
            });
        }
    }

    /// Record LLM token usage on this span using the canonical OpenTelemetry GenAI semantic
    /// conventions. The Raindrop backend derives the per-event `input_tokens`,
    /// `output_tokens`, and `model` columns from these attributes:
    ///
    /// - `gen_ai.response.model` — required gate (the backend silently drops token usage
    ///   when this is missing)
    /// - `gen_ai.usage.input_tokens` (preferred) or `gen_ai.usage.prompt_tokens`
    /// - `gen_ai.usage.output_tokens` (preferred) or `gen_ai.usage.completion_tokens`
    ///
    /// Pass `0` for either token count to omit it. Pass an empty `model` to skip the gate
    /// (the SDK will not emit `gen_ai.response.model`, so the backend will treat tokens as 0
    /// for this span — useful when the caller wants to set tokens on a manual span without
    /// claiming a model).
    pub fn set_token_usage(&self, model: impl AsRef<str>, input_tokens: i64, output_tokens: i64) {
        let attrs = llm_token_usage_attributes(model, input_tokens, output_tokens);
        if !attrs.is_empty() {
            self.set_attributes_replacing(
                &[
                    "gen_ai.response.model",
                    "gen_ai.usage.prompt_tokens",
                    "gen_ai.usage.completion_tokens",
                    "gen_ai.usage.input_tokens",
                    "gen_ai.usage.output_tokens",
                ],
                &[],
                attrs,
            );
        }
    }

    /// End the span at the current time.
    pub fn end(&self) {
        self.end_at(None)
    }

    /// End the span at a specific time.
    pub fn end_at(&self, end_time: Option<OffsetDateTime>) {
        let inner = match &self.inner {
            Some(i) => i.clone(),
            None => return,
        };
        if !inner.client.is_enabled() {
            return;
        }

        let requested_end = end_time.unwrap_or_else(OffsetDateTime::now_utc);
        // Defensive clamp: some external producers have emitted spans with end < start, which
        // creates negative durations downstream. Tinybird stores duration_ns as UInt64, so keep
        // Rust SDK spans internally consistent even when a caller supplies an anomalous end time.
        let end = requested_end.max(inner.start);

        let (attrs, status) = {
            let mut state = inner.state.lock().expect("span lock poisoned");
            if state.ended {
                return;
            }
            state.ended = true;
            // Pre-allocate for both the eventId attribute we always add and the
            // `traceloop.association.properties.event_id` attribute we add when event_id is set.
            // The latter is critical: the backend's `hasAIOperation` filter silently DROPS spans
            // that don't have one of {ai.operationId, traceloop.span.kind, traceloop.workflow.name,
            // traceloop.association.properties.{user_id,convo_id,event_id}, gen_ai.*}. A plain
            // `start_span` with only `ai.telemetry.metadata.raindrop.eventId` would be discarded.
            let mut attributes: Vec<OtlpKeyValue> = Vec::with_capacity(state.attrs.len() + 2);
            if !inner.event_id.is_empty() {
                attributes.push(OtlpKeyValue::from(Attribute::string(
                    "ai.telemetry.metadata.raindrop.eventId",
                    &inner.event_id,
                )));
                // Also emit the traceloop association property so the span passes ingestion.
                // The backend's `getCustomEventId` already prefers this key over the
                // `ai.telemetry.metadata.raindrop.eventId` fallback, so this is the canonical
                // representation and is safe to always emit.
                //
                // Dedupe: tool spans created via `Client::start_tool_span` already inject
                // `event_id` into their `properties` map (so it propagates through
                // `tool_property_attributes` as `traceloop.association.properties.event_id`).
                // Without this guard, every tool span would emit the attribute twice — same
                // value, but a violation of OTLP's "attribute keys MUST be unique" invariant.
                let already_emitted = state
                    .attrs
                    .iter()
                    .any(|a| a.key == "traceloop.association.properties.event_id");
                if !already_emitted {
                    attributes.push(OtlpKeyValue::from(Attribute::string(
                        "traceloop.association.properties.event_id",
                        &inner.event_id,
                    )));
                }
            }
            for attr in state.attrs.drain(..) {
                attributes.push(OtlpKeyValue::from(attr));
            }
            let status = state.status.take().unwrap_or(OtlpStatus {
                code: SpanStatusCode::Ok as u8,
                message: String::new(),
            });
            (attributes, status)
        };

        let span = OtlpSpan {
            trace_id: inner.ids.trace_id_b64.clone(),
            span_id: inner.ids.span_id_b64.clone(),
            parent_span_id: inner.ids.parent_span_id_b64.clone().unwrap_or_default(),
            name: inner.name.clone(),
            start_time_unix_nano: unix_nanos_string(Some(inner.start)),
            end_time_unix_nano: unix_nanos_string(Some(end)),
            attributes: attrs,
            status: Some(status),
        };

        inner.client.enqueue_span(span);
    }
}

/// LLM-specific wrapper around [`Span`] that records Dawn/frontend-compatible prompt,
/// completion, model, provider, and token attributes.
#[derive(Debug, Clone)]
pub struct LlmSpan {
    pub(crate) span: Span,
}

impl LlmSpan {
    pub(crate) fn from_span(span: Span) -> Self {
        Self { span }
    }

    /// Construct a no-op LLM span.
    pub fn noop() -> Self {
        Self { span: Span::noop() }
    }

    /// Returns true if this LLM span is backed by a no-op span.
    pub fn is_noop(&self) -> bool {
        self.span.is_noop()
    }

    /// Borrow the underlying [`Span`] for advanced usage.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Record the LLM model using both OpenTelemetry GenAI and Raindrop keys.
    pub fn set_model(&self, model: impl AsRef<str>) {
        let model = model.as_ref();
        if model.is_empty() {
            return;
        }
        self.span.set_attributes_replacing(
            &[
                "ai.model.id",
                "gen_ai.request.model",
                "gen_ai.response.model",
            ],
            &[],
            llm_model_attributes(model),
        );
    }

    /// Record the LLM provider/system.
    pub fn set_provider(&self, provider: impl AsRef<str>) {
        let provider = provider.as_ref();
        if provider.is_empty() {
            return;
        }
        self.span.set_attributes_replacing(
            &["ai.model.provider", "gen_ai.system"],
            &[],
            llm_provider_attributes(provider),
        );
    }

    /// Record a single user input. Content longer than the client's
    /// `max_text_field_chars` is truncated before serialization.
    pub fn set_input(&self, input: impl Into<String>) {
        let limit = self.span.text_limit();
        self.span.set_attributes_replacing(
            &["ai.prompt", "ai.prompt.messages"],
            &["gen_ai.prompt."],
            llm_input_attributes(input.into(), limit),
        );
    }

    /// Record chat-style prompt messages. Message content longer than the
    /// client's `max_text_field_chars` is truncated before serialization.
    pub fn set_messages<I>(&self, messages: I)
    where
        I: IntoIterator<Item = LlmMessage>,
    {
        let messages: Vec<LlmMessage> = messages.into_iter().collect();
        if messages.is_empty() {
            return;
        }
        let limit = self.span.text_limit();
        self.span.set_attributes_replacing(
            &["ai.prompt", "ai.prompt.messages"],
            &["gen_ai.prompt."],
            llm_message_attributes(messages, limit),
        );
    }

    /// Record a single assistant output. Content longer than the client's
    /// `max_text_field_chars` is truncated before serialization.
    pub fn set_output(&self, output: impl Into<String>) {
        let limit = self.span.text_limit();
        self.span.set_attributes_replacing(
            &["ai.response.text"],
            &["gen_ai.completion."],
            llm_output_attributes(output.into(), limit),
        );
    }

    /// Convenience helper for a plain text prompt/response pair.
    pub fn set_io(&self, input: impl Into<String>, output: impl Into<String>) {
        self.set_input(input);
        self.set_output(output);
    }

    /// See [`Span::set_token_usage`]. Forwarded to the underlying span.
    pub fn set_token_usage(&self, model: impl AsRef<str>, input_tokens: i64, output_tokens: i64) {
        self.span
            .set_token_usage(model, input_tokens, output_tokens)
    }

    /// Mark the LLM span as failed.
    pub fn set_error(&self, message: impl Into<String>) {
        self.span.set_error(message)
    }

    /// End the LLM span at the current time.
    pub fn end(&self) {
        self.span.end()
    }

    /// End the LLM span at a specific time.
    pub fn end_at(&self, end_time: Option<OffsetDateTime>) {
        self.span.end_at(end_time)
    }
}

/// Tool-specific wrapper around [`Span`] that records `traceloop.entity.input/output/duration_ms`
/// attributes on close.
#[derive(Debug, Clone)]
pub struct ToolSpan {
    pub(crate) span: Span,
    pub(crate) start: Option<OffsetDateTime>,
}

impl ToolSpan {
    pub(crate) fn from_span(span: Span, start: Option<OffsetDateTime>) -> Self {
        Self { span, start }
    }

    /// Construct a no-op tool span.
    pub fn noop() -> Self {
        Self {
            span: Span::noop(),
            start: None,
        }
    }

    /// Returns true if this tool span is backed by a no-op span.
    pub fn is_noop(&self) -> bool {
        self.span.is_noop()
    }

    /// Borrow the underlying [`Span`] for advanced usage.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Update the input. Serialization is bounded by the client's
    /// `max_text_field_chars`, so multi-MB payloads cost the cap, not the
    /// payload.
    pub fn set_input(&self, input: &Value) {
        self.span.set_attributes([Attribute::string(
            "traceloop.entity.input",
            stringify_value_bounded(input, self.span.text_limit()),
        )]);
    }

    /// Update the output. Serialization is bounded by the client's
    /// `max_text_field_chars`, so multi-MB payloads cost the cap, not the
    /// payload.
    pub fn set_output(&self, output: &Value) {
        self.span.set_attributes([Attribute::string(
            "traceloop.entity.output",
            stringify_value_bounded(output, self.span.text_limit()),
        )]);
    }

    /// Serialize any `Serialize` result directly onto the span's output
    /// attribute under the text budget — without materializing an unbounded
    /// `serde_json::Value` first. Unserializable values are skipped.
    pub(crate) fn record_output_bounded<T: ?Sized + serde::Serialize>(&self, value: &T) {
        if self.span.is_noop() {
            return;
        }
        if let Some(text) = stringify_serialize_bounded(value, self.span.text_limit()) {
            self.span
                .set_attributes([Attribute::string("traceloop.entity.output", text)]);
        }
    }

    /// Mark the tool span as failed.
    pub fn set_error(&self, message: impl Into<String>) {
        self.span.set_error(message)
    }

    /// See [`Span::set_token_usage`]. Forwarded to the underlying span.
    pub fn set_token_usage(&self, model: impl AsRef<str>, input_tokens: i64, output_tokens: i64) {
        self.span
            .set_token_usage(model, input_tokens, output_tokens)
    }

    /// End the tool span. Computes a `traceloop.entity.duration_ms` attribute when the start time
    /// is known.
    pub fn end(&self) {
        self.end_at(None)
    }

    /// End the tool span at a specific time.
    pub fn end_at(&self, end_time: Option<OffsetDateTime>) {
        if let Some(start) = self.start {
            let end = end_time.unwrap_or_else(OffsetDateTime::now_utc);
            let dur = end - start;
            let ms = (dur.whole_milliseconds()).max(0) as i64;
            self.span
                .set_attributes([Attribute::int("traceloop.entity.duration_ms", ms)]);
        }
        self.span.end_at(end_time);
    }
}

/// Standalone tracer with sticky association properties. Mirrors `Client.Tracer` in the Go SDK.
#[derive(Debug, Clone)]
pub struct Tracer {
    pub(crate) client: Option<Client>,
    pub(crate) properties: BTreeMap<String, Value>,
}

impl Tracer {
    /// Construct a no-op tracer (used when the client is disabled).
    pub fn noop() -> Self {
        Self {
            client: None,
            properties: BTreeMap::new(),
        }
    }

    /// Start a manually-managed span carrying this tracer's sticky properties.
    pub fn start_span(&self, mut opts: SpanOptions) -> Span {
        match &self.client {
            Some(client) => {
                opts.properties = merge_maps(&self.properties, &opts.properties);
                client.start_span(opts)
            }
            None => Span::noop(),
        }
    }

    /// Start a manually-managed LLM span carrying this tracer's sticky properties.
    pub fn start_llm_span(&self, name: impl Into<String>, mut opts: LlmOptions) -> LlmSpan {
        match &self.client {
            Some(client) => {
                opts.properties = merge_maps(&self.properties, &opts.properties);
                client.start_llm_span(name, opts, "")
            }
            None => LlmSpan::noop(),
        }
    }

    /// Run an async closure inside a manually-managed span.
    pub async fn with_span<F, Fut, T, E>(
        &self,
        opts: SpanOptions,
        fn_: F,
    ) -> std::result::Result<T, E>
    where
        F: FnOnce(Span) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: std::fmt::Display,
    {
        let span = self.start_span(opts);
        let span_for_fn = span.clone();
        let result = fn_(span_for_fn).await;
        if let Err(err) = &result {
            span.set_error(err.to_string());
        }
        span.end();
        result
    }

    /// Retroactively log a tool call.
    pub fn track_tool(&self, opts: TrackToolOptions) {
        if let Some(client) = &self.client {
            let mut opts = opts;
            opts.properties = merge_maps(&self.properties, &opts.properties);
            client.track_tool_standalone(opts);
        }
    }
}

pub(crate) fn build_llm_attributes(opts: &LlmOptions, limit: usize) -> Vec<Attribute> {
    let mut attrs = vec![Attribute::string("traceloop.span.kind", "llm")];
    if !opts.model.is_empty() {
        attrs.extend(llm_model_attributes(&opts.model));
    }
    if !opts.provider.is_empty() {
        attrs.extend(llm_provider_attributes(&opts.provider));
    }
    if !opts.messages.is_empty() {
        // Cap during the clone so a multi-MB message body is never copied at
        // full size just to be truncated afterwards.
        let capped: Vec<LlmMessage> = opts
            .messages
            .iter()
            .map(|m| LlmMessage::new(m.role.clone(), capped_string(&m.content, limit)))
            .collect();
        attrs.extend(llm_message_attributes(capped, limit));
    } else if let Some(input) = &opts.input {
        attrs.extend(llm_input_attributes(capped_string(input, limit), limit));
    }
    if let Some(output) = &opts.output {
        attrs.extend(llm_output_attributes(capped_string(output, limit), limit));
    }
    attrs.extend(llm_token_usage_attributes(
        "",
        opts.input_tokens,
        opts.output_tokens,
    ));
    attrs.extend(tool_property_attributes(&opts.properties, limit));
    attrs
}

fn llm_model_attributes(model: &str) -> Vec<Attribute> {
    vec![
        Attribute::string("ai.model.id", model),
        Attribute::string("gen_ai.request.model", model),
        Attribute::string("gen_ai.response.model", model),
    ]
}

fn llm_provider_attributes(provider: &str) -> Vec<Attribute> {
    vec![
        Attribute::string("ai.model.provider", provider),
        Attribute::string("gen_ai.system", provider),
    ]
}

fn llm_input_attributes(mut input: String, limit: usize) -> Vec<Attribute> {
    truncate_text_in_place(&mut input, limit);
    vec![
        Attribute::string("ai.prompt", input.clone()),
        Attribute::string("gen_ai.prompt.0.role", "user"),
        Attribute::string("gen_ai.prompt.0.content", input),
    ]
}

fn llm_message_attributes(mut messages: Vec<LlmMessage>, limit: usize) -> Vec<Attribute> {
    for message in &mut messages {
        truncate_text_in_place(&mut message.content, limit);
    }
    // Aggregate JSON blob is bounded too: capped per-message content keeps the
    // serialization cost proportional to what we ship, and the budget stops a
    // long message LIST from exceeding the limit.
    let messages_json =
        to_string_bounded(&messages, limit).expect("serializing LLM messages cannot fail");
    let mut attrs = vec![Attribute::string("ai.prompt.messages", messages_json)];
    for (idx, message) in messages.into_iter().enumerate() {
        attrs.push(Attribute::string(
            format!("gen_ai.prompt.{}.role", idx),
            message.role,
        ));
        attrs.push(Attribute::string(
            format!("gen_ai.prompt.{}.content", idx),
            message.content,
        ));
    }
    attrs
}

fn llm_output_attributes(mut output: String, limit: usize) -> Vec<Attribute> {
    truncate_text_in_place(&mut output, limit);
    vec![
        Attribute::string("ai.response.text", output.clone()),
        Attribute::string("gen_ai.completion.0.role", "assistant"),
        Attribute::string("gen_ai.completion.0.content", output),
    ]
}

fn llm_token_usage_attributes(
    model: impl AsRef<str>,
    input_tokens: i64,
    output_tokens: i64,
) -> Vec<Attribute> {
    let model = model.as_ref();
    let mut attrs: Vec<Attribute> = Vec::with_capacity(3);
    if !model.is_empty() {
        attrs.push(Attribute::string("gen_ai.response.model", model));
    }
    if input_tokens > 0 {
        attrs.push(Attribute::int("gen_ai.usage.input_tokens", input_tokens));
    }
    if output_tokens > 0 {
        attrs.push(Attribute::int("gen_ai.usage.output_tokens", output_tokens));
    }
    attrs
}

/// Build the standard set of tool attributes (kind, name, input/output, duration, association
/// properties). Tool I/O serialization is bounded by `limit` so multi-MB payloads cost the cap
/// — not the payload — on the calling task.
pub(crate) fn build_tool_attributes(
    name: &str,
    input: Option<&Value>,
    output: Option<&Value>,
    duration: Option<std::time::Duration>,
    properties: &BTreeMap<String, Value>,
    limit: usize,
) -> Vec<Attribute> {
    let mut attrs = vec![
        Attribute::string("traceloop.span.kind", "tool"),
        Attribute::string("traceloop.entity.name", name),
    ];
    if let Some(input) = input {
        attrs.push(Attribute::string(
            "traceloop.entity.input",
            stringify_value_bounded(input, limit),
        ));
    }
    if let Some(output) = output {
        attrs.push(Attribute::string(
            "traceloop.entity.output",
            stringify_value_bounded(output, limit),
        ));
    }
    if let Some(d) = duration {
        attrs.push(Attribute::int(
            "traceloop.entity.duration_ms",
            d.as_millis() as i64,
        ));
    }
    attrs.extend(tool_property_attributes(properties, limit));
    attrs
}

/// Convert a property map into `traceloop.association.properties.*` attributes. String and
/// JSON-serialized values are capped at `limit` characters.
pub(crate) fn tool_property_attributes(
    properties: &BTreeMap<String, Value>,
    limit: usize,
) -> Vec<Attribute> {
    let mut out = Vec::new();
    for (key, value) in properties {
        if key.is_empty() || matches!(value, Value::Null) {
            continue;
        }
        let attr_key = format!("traceloop.association.properties.{}", key);
        let attr = match value {
            Value::String(s) => Attribute::string(attr_key, capped_string(s, limit)),
            Value::Bool(b) => Attribute::bool(attr_key, *b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Attribute::int(attr_key, i)
                } else if let Some(f) = n.as_f64() {
                    Attribute::float(attr_key, f)
                } else {
                    Attribute::string(attr_key, n.to_string())
                }
            }
            Value::Array(arr) if arr.iter().all(|v| matches!(v, Value::String(_))) => {
                let strings = arr
                    .iter()
                    .filter_map(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                Attribute::string_array(attr_key, strings)
            }
            other => Attribute::string(attr_key, stringify_value_bounded(other, limit)),
        };
        out.push(attr);
    }
    out
}

/// Run a closure inside a tool span linked to an interaction. The closure's return value is
/// JSON-serialized (best-effort) and recorded on the span as `traceloop.entity.output`.
///
/// If the interaction's underlying client is disabled, the closure runs without instrumentation
/// and the result is returned as-is.
pub fn with_tool<F, T, E>(
    interaction: &crate::events::Interaction,
    name: impl Into<String>,
    opts: ToolOptions,
    fn_: F,
) -> std::result::Result<T, E>
where
    F: FnOnce() -> std::result::Result<T, E>,
    T: serde::Serialize,
    E: std::fmt::Display,
{
    let name = name.into();
    if interaction.client.is_none() {
        return fn_();
    }
    let tool_span = interaction.start_tool_span(name, opts);
    match fn_() {
        Ok(result) => {
            // Bounded: serializes at most the text cap, never the full result.
            tool_span.record_output_bounded(&result);
            tool_span.end();
            Ok(result)
        }
        Err(err) => {
            tool_span.set_error(err.to_string());
            tool_span.end();
            Err(err)
        }
    }
}

/// Async variant of [`with_tool`].
pub async fn with_tool_async<F, Fut, T, E>(
    interaction: &crate::events::Interaction,
    name: impl Into<String>,
    opts: ToolOptions,
    fn_: F,
) -> std::result::Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    T: serde::Serialize,
    E: std::fmt::Display,
{
    let name = name.into();
    if interaction.client.is_none() {
        return fn_().await;
    }
    let tool_span = interaction.start_tool_span(name, opts);
    match fn_().await {
        Ok(result) => {
            // Bounded: serializes at most the text cap, never the full result.
            tool_span.record_output_bounded(&result);
            tool_span.end();
            Ok(result)
        }
        Err(err) => {
            tool_span.set_error(err.to_string());
            tool_span.end();
            Err(err)
        }
    }
}
