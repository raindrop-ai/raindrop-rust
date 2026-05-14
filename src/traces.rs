use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use time::OffsetDateTime;

use crate::client::Client;
use crate::helpers::{merge_maps, stringify_value, unix_nanos_string};
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

    /// Record the span's input payload as the canonical `raindrop.input` attribute.
    ///
    /// The Raindrop backend reads this attribute first (ahead of any
    /// operation-kind-specific extraction like Vercel AI SDK or Traceloop tool
    /// spans) to populate the `input_payload` column. Works on any span kind,
    /// not just tool spans.
    pub fn set_input(&self, input: &Value) {
        self.set_attributes([Attribute::string("raindrop.input", stringify_value(input))]);
    }

    /// Record the span's output payload as the canonical `raindrop.output` attribute.
    ///
    /// See [`Span::set_input`] for the precedence rules.
    pub fn set_output(&self, output: &Value) {
        self.set_attributes([Attribute::string(
            "raindrop.output",
            stringify_value(output),
        )]);
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
        if !attrs.is_empty() {
            self.set_attributes(attrs);
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

/// Tool-specific wrapper around [`Span`] that records `raindrop.input` / `raindrop.output`
/// plus `traceloop.entity.duration_ms` on close, alongside the `traceloop.span.kind=tool` flag
/// that marks the span as a tool call for the UI.
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

    /// Update the input. Delegates to [`Span::set_input`].
    pub fn set_input(&self, input: &Value) {
        self.span.set_input(input);
    }

    /// Update the output. Delegates to [`Span::set_output`].
    pub fn set_output(&self, output: &Value) {
        self.span.set_output(output);
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

/// Build the standard set of tool attributes (kind, name, input/output, duration, association
/// properties).
pub(crate) fn build_tool_attributes(
    name: &str,
    input: Option<&Value>,
    output: Option<&Value>,
    duration: Option<std::time::Duration>,
    properties: &BTreeMap<String, Value>,
) -> Vec<Attribute> {
    let mut attrs = vec![
        Attribute::string("traceloop.span.kind", "tool"),
        Attribute::string("traceloop.entity.name", name),
    ];
    if let Some(input) = input {
        attrs.push(Attribute::string("raindrop.input", stringify_value(input)));
    }
    if let Some(output) = output {
        attrs.push(Attribute::string(
            "raindrop.output",
            stringify_value(output),
        ));
    }
    if let Some(d) = duration {
        attrs.push(Attribute::int(
            "traceloop.entity.duration_ms",
            d.as_millis() as i64,
        ));
    }
    attrs.extend(tool_property_attributes(properties));
    attrs
}

/// Convert a property map into `traceloop.association.properties.*` attributes.
pub(crate) fn tool_property_attributes(properties: &BTreeMap<String, Value>) -> Vec<Attribute> {
    let mut out = Vec::new();
    for (key, value) in properties {
        if key.is_empty() || matches!(value, Value::Null) {
            continue;
        }
        let attr_key = format!("traceloop.association.properties.{}", key);
        let attr = match value {
            Value::String(s) => Attribute::string(attr_key, s),
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
            other => Attribute::from_json(attr_key, other),
        };
        out.push(attr);
    }
    out
}

/// Run a closure inside a tool span linked to an interaction. The closure's return value is
/// JSON-serialized (best-effort) and recorded on the span as `raindrop.output`.
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
            if let Ok(value) = serde_json::to_value(&result) {
                tool_span.set_output(&value);
            }
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
            if let Ok(value) = serde_json::to_value(&result) {
                tool_span.set_output(&value);
            }
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
