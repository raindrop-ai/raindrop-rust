use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use time::OffsetDateTime;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::buffer::{EventBuffer, EventPatch};
use crate::error::{Error, Result};
use crate::events::{AiEvent, BeginOptions, Event, FinishOptions, Interaction, PatchOptions};
use crate::helpers::new_event_id;
use crate::http::{format_endpoint, RetryingHttpClient, TransportConfig};
use crate::local_debugger::{resolve_local_workshop_url, LocalWorkshopUrlConfig};
use crate::otlp::{create_span_ids, Attribute, OtlpKeyValue, OtlpSpan, OtlpStatus};
use crate::signals::{track_signal, Signal};
use crate::trace_buffer::TraceBuffer;
use crate::traces::{
    build_llm_attributes, build_tool_attributes, tool_property_attributes, LlmOptions, LlmSpan,
    Span, SpanOptions, ToolOptions, ToolSpan, Tracer, TrackToolOptions,
};
use crate::users::{identify, User};

/// Default per-field character cap for AI text content (input/output, tool
/// span I/O, LLM span content). Mirrors the python-sdk's
/// `max_text_field_chars` default.
pub(crate) const DEFAULT_MAX_TEXT_FIELD_CHARS: usize = 1_000_000;

/// Default per-attempt bound for every cloud POST (connect through body).
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Default connect-phase bound for the SDK-built `reqwest::Client`.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default overall deadline for `close()` (python-sdk shutdown parity: a dead
/// or slow network must never wedge the host process's exit).
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default cap on distinct buffered event ids (matches the trace buffer's
/// `trace_max_queue_size` default).
const DEFAULT_EVENT_MAX_QUEUE_SIZE: usize = 5000;

/// Shared inner state for the client.
pub(crate) struct ClientInner {
    pub(crate) transport: RetryingHttpClient,
    pub(crate) enabled: bool,
    pub(crate) debug: bool,
    pub(crate) service_name: String,
    pub(crate) version: String,
    pub(crate) context_data: Value,
    pub(crate) event_buffer: Arc<EventBuffer>,
    pub(crate) trace_buffer: Arc<TraceBuffer>,
    pub(crate) max_text_field_chars: usize,
    pub(crate) close_timeout: Duration,
    pub(crate) closed: AtomicBool,
    /// One-shot fire-and-forget tasks (span enqueues). Drained by `flush()`.
    pub(crate) flush_tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Long-running periodic flusher tasks. They only complete after their
    /// buffer's stop notify fires, so they MUST NOT live in `flush_tasks`:
    /// awaiting them from `flush()` would block until `close()`.
    pub(crate) periodic_tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for ClientInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientInner")
            .field("enabled", &self.enabled)
            .field("debug", &self.debug)
            .field("service_name", &self.service_name)
            .finish()
    }
}

/// The Raindrop SDK client.
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

/// Builder for [`Client`]. Construct via [`Client::builder`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    write_key: String,
    endpoint: String,
    project_id: Option<String>,
    local_workshop_url_config: LocalWorkshopUrlConfig,
    auto_detect_local_workshop: bool,
    debug: bool,
    partial_flush_interval: Duration,
    trace_flush_interval: Duration,
    trace_max_batch_size: usize,
    trace_max_queue_size: usize,
    event_max_queue_size: usize,
    max_attempts: u32,
    base_delay: Duration,
    jitter_fraction: f64,
    request_timeout: Duration,
    close_timeout: Duration,
    max_text_field_chars: usize,
    service_name: String,
    library_name: String,
    library_version: String,
    http_client: Option<reqwest::Client>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            write_key: String::new(),
            endpoint: crate::DEFAULT_ENDPOINT.to_string(),
            project_id: None,
            local_workshop_url_config: LocalWorkshopUrlConfig::Inherit,
            auto_detect_local_workshop: true,
            debug: false,
            partial_flush_interval: Duration::from_secs(1),
            trace_flush_interval: Duration::from_secs(1),
            trace_max_batch_size: 50,
            trace_max_queue_size: 5000,
            event_max_queue_size: DEFAULT_EVENT_MAX_QUEUE_SIZE,
            max_attempts: 3,
            base_delay: Duration::from_secs(1),
            jitter_fraction: 0.2,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            close_timeout: DEFAULT_CLOSE_TIMEOUT,
            max_text_field_chars: DEFAULT_MAX_TEXT_FIELD_CHARS,
            service_name: crate::DEFAULT_SERVICE_NAME.to_string(),
            library_name: crate::DEFAULT_LIBRARY_NAME.to_string(),
            library_version: crate::VERSION.to_string(),
            http_client: None,
        }
    }
}

impl ClientBuilder {
    /// Set the write key. If empty, the client runs in disabled (no-op) mode.
    pub fn write_key(mut self, write_key: impl Into<String>) -> Self {
        self.write_key = write_key.into().trim().to_string();
        self
    }

    /// Set the ingestion endpoint. Defaults to [`DEFAULT_ENDPOINT`](crate::DEFAULT_ENDPOINT).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Route telemetry to a specific Raindrop project. When set to a valid
    /// slug, every outbound request carries an `X-Raindrop-Project-Id: <slug>`
    /// header so the backend files events under that project; when unset, no
    /// header is sent and the backend uses the org's `default` project (so this
    /// is fully backward compatible).
    ///
    /// The value is trimmed and validated against
    /// `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$` once, at [`build`](Self::build)
    /// time. An empty, whitespace-only, or invalid value never breaks
    /// ingestion: it is logged via `tracing::warn!` and the header is omitted.
    pub fn project_id(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Mirror every cloud-bound POST to a local Raindrop Workshop daemon at
    /// `url` (in addition to — not instead of — the cloud endpoint). Forces
    /// the URL even when env vars or the TCP probe would resolve differently.
    /// Pass a fully qualified base URL such as `http://localhost:5899/v1/`;
    /// a trailing `/` is appended if missing.
    pub fn local_workshop_url(mut self, url: impl Into<String>) -> Self {
        self.local_workshop_url_config = LocalWorkshopUrlConfig::Url(url.into());
        self
    }

    /// Explicitly disable local Workshop mirroring, suppressing both the
    /// `RAINDROP_LOCAL_DEBUGGER` / `RAINDROP_WORKSHOP` env vars and the
    /// localhost TCP probe. Use when running tests or a packaged binary that
    /// must never fan out to a developer-machine daemon.
    pub fn disable_local_workshop(mut self) -> Self {
        self.local_workshop_url_config = LocalWorkshopUrlConfig::Disabled;
        self.auto_detect_local_workshop = false;
        self
    }

    /// Enable verbose debug logging.
    pub fn debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    /// Set the periodic event flush interval. `0` disables the periodic ticker.
    pub fn partial_flush_interval(mut self, interval: Duration) -> Self {
        self.partial_flush_interval = interval;
        self
    }

    /// Set the periodic trace flush interval. `0` disables the periodic ticker.
    pub fn trace_flush_interval(mut self, interval: Duration) -> Self {
        self.trace_flush_interval = interval;
        self
    }

    /// Maximum number of spans per trace export request (default 50).
    pub fn trace_max_batch_size(mut self, size: usize) -> Self {
        self.trace_max_batch_size = size.max(1);
        self
    }

    /// Maximum number of spans buffered before back-pressuring (default 5000).
    pub fn trace_max_queue_size(mut self, size: usize) -> Self {
        self.trace_max_queue_size = size.max(1);
        self
    }

    /// Maximum number of distinct event ids buffered at once (default 5000).
    /// When the network is down (patches are restored for retry) or
    /// interactions are abandoned, the buffer stops growing at this bound and
    /// new events are dropped with a rate-limited warning instead of growing
    /// host memory without limit.
    pub fn event_max_queue_size(mut self, size: usize) -> Self {
        self.event_max_queue_size = size.max(1);
        self
    }

    /// Number of HTTP attempts (default 3). Set to 1 to disable retries.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    /// Base delay between retries. Default 1s.
    pub fn base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    /// Jitter fraction applied to retry delays (0.0–1.0). Default 0.2.
    pub fn jitter_fraction(mut self, fraction: f64) -> Self {
        self.jitter_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// Per-attempt bound applied to EVERY cloud POST, from connect through
    /// response body (default 30s). Applied at the request level, so it holds
    /// even for a caller-injected `http_client` built without timeouts; note
    /// that per-request timeouts override the injected client's own
    /// `ClientBuilder::timeout`, so set this knob to match if you need a
    /// stricter bound.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Overall deadline for [`Client::close`] (default 10s): stop signals,
    /// in-flight task draining, and the final flush all share this budget, so
    /// a dead or slow network can never wedge the host process's shutdown.
    /// Payloads that don't make it out before the deadline are dropped with a
    /// warning. `Duration::ZERO` disables the deadline.
    pub fn close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Per-field character cap applied to AI input/output and serialized
    /// tool/LLM span content BEFORE buffering or serialization, so oversized
    /// payloads cost the cap — not the payload — on the calling task, and
    /// land truncated (marker `...[truncated by raindrop]`, included within
    /// the cap) instead of being silently dropped at the 1 MiB ingest limit.
    /// Defaults to 1,000,000. A stricter `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT`
    /// env var (read once at build time) is also honored. Values of 0 are
    /// ignored.
    pub fn max_text_field_chars(mut self, limit: usize) -> Self {
        if limit > 0 {
            self.max_text_field_chars = limit;
        } else {
            tracing::warn!("raindrop: max_text_field_chars(0) ignored; must be > 0");
        }
        self
    }

    /// Service name reported in OTLP `resource.service.name`. Default `raindrop.rust-sdk`.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Library name reported in `$context.library.name`.
    pub fn library_name(mut self, name: impl Into<String>) -> Self {
        self.library_name = name.into();
        self
    }

    /// Library version reported in `$context.library.version`.
    pub fn library_version(mut self, version: impl Into<String>) -> Self {
        self.library_version = version.into();
        self
    }

    /// Inject a custom `reqwest::Client`. Defaults to a fresh client with sane
    /// timeouts. Note the SDK additionally applies [`Self::request_timeout`]
    /// per request, so even a client built without timeouts cannot hang a
    /// flush indefinitely.
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Build the [`Client`].
    pub fn build(self) -> Result<Client> {
        let endpoint = format_endpoint(&self.endpoint);
        let project_id = crate::project_id::resolve(self.project_id.as_deref());
        let local_workshop_url = resolve_local_workshop_url(
            &self.local_workshop_url_config,
            self.auto_detect_local_workshop,
        );
        let has_write_key = !self.write_key.is_empty();
        let enabled = has_write_key || local_workshop_url.is_some();

        let http = match self.http_client {
            Some(c) => c,
            None => reqwest::Client::builder()
                .timeout(DEFAULT_REQUEST_TIMEOUT)
                .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                .build()
                .map_err(|e| Error::Config(format!("could not build http client: {}", e)))?,
        };

        let transport = RetryingHttpClient::new(
            TransportConfig {
                base_url: endpoint,
                write_key: self.write_key,
                project_id,
                local_workshop_url,
                max_attempts: self.max_attempts,
                base_delay: self.base_delay,
                jitter_fraction: self.jitter_fraction,
                request_timeout: self.request_timeout,
                debug: self.debug,
            },
            http,
        );

        let event_buffer = Arc::new(EventBuffer::new(
            self.partial_flush_interval,
            self.event_max_queue_size,
        ));
        let trace_buffer = Arc::new(TraceBuffer::new(
            self.trace_flush_interval,
            self.trace_max_batch_size,
            self.trace_max_queue_size,
        ));

        let context_data = json!({
            "library": { "name": self.library_name, "version": self.library_version },
            "metadata": {
                "language": "rust",
                "rustcChannel": option_env!("CARGO_BUILD_TARGET").unwrap_or("stable"),
            }
        });

        let inner = Arc::new(ClientInner {
            transport,
            enabled,
            debug: self.debug,
            service_name: self.service_name,
            version: self.library_version,
            context_data,
            event_buffer,
            trace_buffer,
            max_text_field_chars: effective_text_field_limit(self.max_text_field_chars),
            close_timeout: self.close_timeout,
            closed: AtomicBool::new(false),
            flush_tasks: std::sync::Mutex::new(Vec::new()),
            periodic_tasks: std::sync::Mutex::new(Vec::new()),
        });

        let client = Client { inner };

        // Start periodic flushers if intervals > 0 and enabled.
        if enabled {
            client.start_periodic_flushers();
        }

        Ok(client)
    }
}

impl Client {
    /// Start a new builder with default options.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Returns true if the client has at least one resolved destination —
    /// either a non-empty write key (cloud) or a resolved
    /// `local_workshop_url` (local Workshop mirror), or both.
    pub fn is_enabled(&self) -> bool {
        self.inner.enabled
    }

    /// Whether the client is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Effective per-field character cap for AI text content.
    pub(crate) fn max_text_field_chars(&self) -> usize {
        self.inner.max_text_field_chars
    }

    /// Track a non-AI event.
    pub async fn track_event(&self, event: Event) -> Result<()> {
        if !self.inner.enabled {
            return Ok(());
        }
        let event_id = if event.event_id.is_empty() {
            new_event_id()
        } else {
            event.event_id
        };
        let patch = EventPatch {
            event_name: event.event,
            user_id: event.user_id,
            convo_id: String::new(),
            input: String::new(),
            output: String::new(),
            model: String::new(),
            properties: event.properties,
            attachments: event.attachments,
            feature_flags: event.feature_flags,
            is_pending: Some(false),
            timestamp: event.timestamp,
        };
        self.inner
            .event_buffer
            .clone()
            .patch(&self.inner, &event_id, patch)
            .await
    }

    /// Track an AI event in one shot (no interaction lifecycle).
    pub async fn track_ai(&self, event: AiEvent) -> Result<()> {
        if !self.inner.enabled {
            return Ok(());
        }
        let event_id = if event.event_id.is_empty() {
            new_event_id()
        } else {
            event.event_id
        };
        let patch = EventPatch {
            event_name: event.event,
            user_id: event.user_id,
            convo_id: event.convo_id,
            input: event.input,
            output: event.output,
            model: event.model,
            properties: event.properties,
            attachments: event.attachments,
            feature_flags: event.feature_flags,
            is_pending: Some(false),
            timestamp: event.timestamp,
        };
        self.inner
            .event_buffer
            .clone()
            .patch(&self.inner, &event_id, patch)
            .await
    }

    /// Begin a new in-progress interaction. The returned [`Interaction`] can accumulate further
    /// patches before [`Interaction::finish`]. The initial pending patch is applied (and either
    /// buffered or flushed) before this future resolves.
    pub async fn begin(&self, opts: BeginOptions) -> Interaction {
        if !self.inner.enabled {
            return Interaction::noop();
        }
        let event_id = if opts.event_id.is_empty() {
            new_event_id()
        } else {
            opts.event_id
        };

        let patch = EventPatch {
            event_name: opts.event.clone(),
            user_id: opts.user_id.clone(),
            convo_id: opts.convo_id.clone(),
            input: opts.input,
            output: String::new(),
            model: opts.model,
            properties: opts.properties,
            attachments: opts.attachments,
            feature_flags: opts.feature_flags,
            is_pending: Some(true),
            timestamp: opts.timestamp,
        };
        let _ = self
            .inner
            .event_buffer
            .clone()
            .patch(&self.inner, &event_id, patch)
            .await;
        Interaction::new_with_context(
            self.clone(),
            event_id,
            opts.user_id,
            opts.convo_id,
            opts.event,
        )
    }

    /// Resume a previously-started interaction by event id. The returned `Interaction` shares the
    /// same buffer state, so subsequent patches are merged with the in-flight payload.
    pub fn resume_interaction(&self, event_id: impl Into<String>) -> Interaction {
        if !self.inner.enabled {
            return Interaction::noop();
        }
        Interaction::new(self.clone(), event_id.into())
    }

    /// Apply a patch directly to a buffered interaction by id.
    pub async fn patch(&self, event_id: &str, opts: PatchOptions) -> Result<()> {
        if !self.inner.enabled {
            return Ok(());
        }
        let patch = EventPatch {
            event_name: opts.event,
            user_id: opts.user_id,
            convo_id: opts.convo_id,
            input: opts.input,
            output: opts.output,
            model: opts.model,
            properties: opts.properties,
            attachments: opts.attachments,
            feature_flags: opts.feature_flags,
            is_pending: opts.is_pending,
            timestamp: opts.timestamp,
        };
        self.inner
            .event_buffer
            .clone()
            .patch(&self.inner, event_id, patch)
            .await
    }

    /// Finalize an interaction directly (rarely used; prefer [`Interaction::finish`]).
    pub async fn finish(&self, event_id: &str, opts: FinishOptions) -> Result<()> {
        self.finish_with_context(event_id, opts, "", "", "").await
    }

    /// Finalize with the interaction's captured association context. Carrying
    /// `user_id`/`convo_id`/`event` in the final patch keeps a `finish()`
    /// shippable even when the sticky-context entry for this event id was
    /// evicted under the queue bound; otherwise the patch would restore
    /// forever on the missing-user_id path and occupy a buffer slot.
    pub(crate) async fn finish_with_context(
        &self,
        event_id: &str,
        opts: FinishOptions,
        user_id: &str,
        convo_id: &str,
        event_name: &str,
    ) -> Result<()> {
        if !self.inner.enabled {
            return Ok(());
        }
        let patch = EventPatch {
            event_name: event_name.to_string(),
            user_id: user_id.to_string(),
            convo_id: convo_id.to_string(),
            output: opts.output,
            model: opts.model,
            properties: opts.properties,
            attachments: opts.attachments,
            feature_flags: opts.feature_flags,
            is_pending: Some(false),
            timestamp: opts.timestamp,
            ..Default::default()
        };
        self.inner
            .event_buffer
            .clone()
            .patch(&self.inner, event_id, patch)
            .await
    }

    pub(crate) fn forget_interaction(&self, _event_id: &str) {
        // Currently a no-op; sticky data is cleared after a successful flush of a final patch.
    }

    /// Start a manually-managed span. The returned [`Span`] **must** have `end()` called or it
    /// will leak (the span won't be shipped). For convenience, drop-on-end is not implemented to
    /// match Go semantics (manual control over end time).
    pub fn start_span(&self, opts: SpanOptions) -> Span {
        if !self.inner.enabled {
            return Span::noop();
        }
        let parent_ids = opts.parent.as_ref().and_then(|p| p.ids());
        let ids = create_span_ids(parent_ids.as_ref());
        let mut attrs = opts.attributes;
        if !opts.operation_id.is_empty() {
            attrs.push(Attribute::string("ai.operationId", &opts.operation_id));
        }
        attrs.extend(tool_property_attributes(
            &opts.properties,
            self.inner.max_text_field_chars,
        ));
        Span::new(
            self.clone(),
            ids,
            opts.name,
            opts.event_id,
            opts.start_time.unwrap_or_else(OffsetDateTime::now_utc),
            attrs,
        )
    }

    /// Start an LLM span linked to an event id.
    pub fn start_llm_span(
        &self,
        name: impl Into<String>,
        opts: LlmOptions,
        event_id: &str,
    ) -> LlmSpan {
        let name = name.into();
        if !self.inner.enabled {
            return LlmSpan::noop();
        }
        let start = opts.start_time.unwrap_or_else(OffsetDateTime::now_utc);
        let mut opts = opts;
        if !event_id.is_empty() {
            opts.properties
                .entry("event_id".to_string())
                .or_insert_with(|| Value::String(event_id.to_string()));
        }
        let operation_id = if opts.operation_id.is_empty() {
            "ai.generateText".to_string()
        } else {
            opts.operation_id.clone()
        };
        let attrs = build_llm_attributes(&opts, self.inner.max_text_field_chars);
        let span_opts = SpanOptions {
            name,
            event_id: event_id.to_string(),
            operation_id,
            parent: opts.parent,
            properties: BTreeMap::new(),
            attributes: attrs,
            start_time: Some(start),
        };
        LlmSpan::from_span(self.start_span(span_opts))
    }

    /// Start a tool span linked to an event id.
    pub fn start_tool_span(
        &self,
        name: impl Into<String>,
        opts: ToolOptions,
        event_id: &str,
    ) -> ToolSpan {
        let name = name.into();
        if !self.inner.enabled {
            return ToolSpan::noop();
        }
        let start = opts.start_time.unwrap_or_else(OffsetDateTime::now_utc);
        let mut properties = opts.properties;
        if !event_id.is_empty() {
            properties
                .entry("event_id".to_string())
                .or_insert_with(|| Value::String(event_id.to_string()));
        }
        let attrs = build_tool_attributes(
            &name,
            opts.input.as_ref(),
            None,
            None,
            &properties,
            self.inner.max_text_field_chars,
        );
        let span_opts = SpanOptions {
            name,
            event_id: event_id.to_string(),
            operation_id: "ai.toolCall".to_string(),
            parent: opts.parent,
            properties: BTreeMap::new(),
            attributes: attrs,
            start_time: Some(start),
        };
        ToolSpan::from_span(self.start_span(span_opts), Some(start))
    }

    pub(crate) fn track_tool_for_interaction(&self, event_id: &str, opts: TrackToolOptions) {
        if !self.inner.enabled {
            return;
        }
        let mut opts = opts;
        if opts.name.is_empty() {
            return;
        }
        let (start, end) = derive_times(&mut opts);
        if !event_id.is_empty() {
            opts.properties
                .entry("event_id".to_string())
                .or_insert_with(|| Value::String(event_id.to_string()));
        }
        let attrs = build_tool_attributes(
            &opts.name,
            opts.input.as_ref(),
            opts.output.as_ref(),
            opts.duration,
            &opts.properties,
            self.inner.max_text_field_chars,
        );
        let parent_ids = opts.parent.as_ref().and_then(|p| p.ids());
        let ids = create_span_ids(parent_ids.as_ref());
        let mut otlp_attrs: Vec<OtlpKeyValue> = Vec::with_capacity(attrs.len() + 2);
        otlp_attrs.push(OtlpKeyValue::from(Attribute::string(
            "ai.operationId",
            "ai.toolCall",
        )));
        if !event_id.is_empty() {
            otlp_attrs.push(OtlpKeyValue::from(Attribute::string(
                "ai.telemetry.metadata.raindrop.eventId",
                event_id,
            )));
        }
        for a in attrs {
            otlp_attrs.push(OtlpKeyValue::from(a));
        }
        let mut status = OtlpStatus {
            code: crate::otlp::SpanStatusCode::Ok as u8,
            ..Default::default()
        };
        if let Some(err) = &opts.error {
            status.code = crate::otlp::SpanStatusCode::Error as u8;
            status.message = err.clone();
        }
        let span = OtlpSpan {
            trace_id: ids.trace_id_b64,
            span_id: ids.span_id_b64,
            parent_span_id: ids.parent_span_id_b64.unwrap_or_default(),
            name: opts.name,
            start_time_unix_nano: crate::helpers::unix_nanos_string(Some(start)),
            end_time_unix_nano: crate::helpers::unix_nanos_string(Some(end)),
            attributes: otlp_attrs,
            status: Some(status),
        };
        self.enqueue_span(span);
    }

    pub(crate) fn track_tool_standalone(&self, opts: TrackToolOptions) {
        self.track_tool_for_interaction("", opts)
    }

    pub(crate) fn enqueue_span(&self, span: OtlpSpan) {
        if !self.inner.enabled {
            return;
        }
        let buffer = self.inner.trace_buffer.clone();
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            buffer.enqueue(inner, span).await;
        });
        if let Ok(mut tasks) = self.inner.flush_tasks.lock() {
            // Prune completed handles so an app that emits spans steadily but
            // never calls flush() doesn't accumulate one JoinHandle per span
            // forever.
            tasks.retain(|t| !t.is_finished());
            tasks.push(task);
        }
    }

    /// Track a user feedback signal (thumbs up/down, edit, etc.).
    pub async fn track_signal(&self, signal: Signal) -> Result<()> {
        track_signal(&self.inner, signal).await
    }

    /// Identify a user.
    pub async fn identify(&self, user: User) -> Result<()> {
        identify(&self.inner, user).await
    }

    /// Construct a standalone tracer with sticky association properties.
    pub fn tracer(&self, properties: BTreeMap<String, Value>) -> Tracer {
        if !self.inner.enabled {
            return Tracer::noop();
        }
        Tracer {
            client: Some(self.clone()),
            properties,
        }
    }

    /// Force-flush all buffered events and traces.
    pub async fn flush(&self) -> Result<()> {
        if !self.inner.enabled {
            return Ok(());
        }
        // Drain any pending fire-and-forget tasks so spans/events have actually been enqueued.
        let pending_tasks: Vec<JoinHandle<()>> = {
            let mut guard = self
                .inner
                .flush_tasks
                .lock()
                .expect("flush task lock poisoned");
            std::mem::take(&mut *guard)
        };
        for task in pending_tasks {
            let _ = task.await;
        }

        let event_res = self.inner.event_buffer.clone().flush(&self.inner).await;
        let trace_res = self.inner.trace_buffer.clone().flush(&self.inner).await;
        // Drain after the buffer flush so any mirror POSTs spawned from inside
        // `flush_one` / `trace_buffer.flush` are observable to the caller.
        self.inner.transport.await_pending_mirrors().await;
        match (event_res, trace_res) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
        }
    }

    /// Close the client. Cancels periodic timers, awaits any in-flight tasks, and force-flushes
    /// remaining buffers — all under the [`ClientBuilder::close_timeout`]
    /// deadline (default 10s), so a dead or slow network can never wedge the
    /// host process's shutdown. Payloads that don't make it out before the
    /// deadline are dropped with a warning and `Ok(())` is returned.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if !self.inner.enabled {
            return Ok(());
        }
        self.inner.event_buffer.stop();
        self.inner.trace_buffer.stop();

        let deadline = self.inner.close_timeout;
        if deadline.is_zero() {
            self.drain_periodic_tasks().await;
            return self.flush().await;
        }
        match tokio::time::timeout(deadline, async {
            self.drain_periodic_tasks().await;
            self.flush().await
        })
        .await
        {
            Ok(res) => res,
            Err(_) => {
                tracing::warn!(
                    timeout_secs = deadline.as_secs_f64(),
                    "raindrop: close() deadline exceeded; dropping remaining buffered telemetry \
                     so process shutdown is not blocked"
                );
                Ok(())
            }
        }
    }

    /// Await the periodic flusher tasks. Called after the buffers' stop
    /// notifies fire, so the tickers exit on their next select; a task
    /// mid-flush finishes its in-flight cycle first (bounded by `close()`'s
    /// deadline).
    async fn drain_periodic_tasks(&self) {
        let tasks: Vec<JoinHandle<()>> = {
            let mut guard = self
                .inner
                .periodic_tasks
                .lock()
                .expect("periodic task lock poisoned");
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }

    fn start_periodic_flushers(&self) {
        let inner = self.inner.clone();
        let event_buffer = self.inner.event_buffer.clone();
        let trace_buffer = self.inner.trace_buffer.clone();

        let event_interval = event_buffer.flush_every();
        let trace_interval = trace_buffer.flush_every();

        // Periodic tickers run until their stop notify fires at close(); they
        // must live in `periodic_tasks`, NOT `flush_tasks` — flush() awaits
        // every flush_tasks handle, so a never-ending ticker there would make
        // any explicit flush() call block until close().
        if event_interval > Duration::ZERO {
            let stop = event_buffer.stop_notify();
            let inner_event = inner.clone();
            let buffer = event_buffer.clone();
            let task = tokio::spawn(async move {
                periodic_flush(stop, event_interval, move || {
                    let inner = inner_event.clone();
                    let buf = buffer.clone();
                    async move {
                        let _ = buf.flush(&inner).await;
                    }
                })
                .await;
            });
            if let Ok(mut tasks) = self.inner.periodic_tasks.lock() {
                tasks.push(task);
            }
        }

        if trace_interval > Duration::ZERO {
            let stop = trace_buffer.stop_notify();
            let inner_trace = inner.clone();
            let buffer = trace_buffer.clone();
            let task = tokio::spawn(async move {
                periodic_flush(stop, trace_interval, move || {
                    let inner = inner_trace.clone();
                    let buf = buffer.clone();
                    async move {
                        let _ = buf.flush(&inner).await;
                    }
                })
                .await;
            });
            if let Ok(mut tasks) = self.inner.periodic_tasks.lock() {
                tasks.push(task);
            }
        }
    }
}

/// Character budget for one serialized text field: the configured cap, with a
/// stricter `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT` env var also honored
/// (python-sdk `_effective_field_limit` parity). Read once at build time.
fn effective_text_field_limit(configured: usize) -> usize {
    match std::env::var("OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT") {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(env_limit) if env_limit > 0 => configured.min(env_limit),
            _ => configured,
        },
        Err(_) => configured,
    }
}

fn derive_times(opts: &mut TrackToolOptions) -> (OffsetDateTime, OffsetDateTime) {
    let now = OffsetDateTime::now_utc();
    let (start, end) = match (opts.start_time, opts.end_time, opts.duration) {
        (Some(s), Some(e), _) => (s, e),
        (Some(s), None, Some(d)) => (s, s + d),
        (Some(s), None, None) => (s, now),
        (None, Some(e), Some(d)) => (e - d, e),
        (None, Some(e), None) => (e, e),
        (None, None, Some(d)) => (now - d, now),
        (None, None, None) => (now, now),
    };
    let end = end.max(start);
    if opts.duration.is_none() {
        opts.duration = Some((end - start).try_into().unwrap_or(Duration::ZERO));
    }
    (start, end)
}

async fn periodic_flush<F, Fut>(stop: Arc<Notify>, interval: Duration, run: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // skip the immediate fire
    loop {
        tokio::select! {
            _ = stop.notified() => break,
            _ = ticker.tick() => {
                run().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OTEL_LIMIT_VAR: &str = "OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT";

    #[test]
    fn otel_env_limit_applies_only_when_stricter() {
        // This is the only test in the lib suite touching this env var, so no
        // cross-test lock is needed (RAINDROP_* vars have their own).
        std::env::remove_var(OTEL_LIMIT_VAR);
        assert_eq!(effective_text_field_limit(1_000), 1_000);

        std::env::set_var(OTEL_LIMIT_VAR, "50");
        assert_eq!(effective_text_field_limit(1_000), 50);

        std::env::set_var(OTEL_LIMIT_VAR, "999999");
        assert_eq!(effective_text_field_limit(1_000), 1_000);

        std::env::set_var(OTEL_LIMIT_VAR, "not-a-number");
        assert_eq!(effective_text_field_limit(1_000), 1_000);

        std::env::set_var(OTEL_LIMIT_VAR, "0");
        assert_eq!(effective_text_field_limit(1_000), 1_000);

        std::env::remove_var(OTEL_LIMIT_VAR);
    }

    #[tokio::test]
    async fn enqueue_span_prunes_completed_task_handles() {
        // No flush is ever called: pre-fix, every enqueued span leaked its
        // JoinHandle into flush_tasks until an explicit flush()/close().
        let client = Client::builder()
            .write_key("rk_test")
            .disable_local_workshop()
            .partial_flush_interval(Duration::ZERO)
            .trace_flush_interval(Duration::ZERO)
            // Keep enqueues from triggering a batch flush (no server here).
            .trace_max_batch_size(100_000)
            .build()
            .expect("build");

        for i in 0..200 {
            let span = client.start_span(crate::traces::SpanOptions {
                name: format!("s{i}"),
                event_id: "evt".into(),
                operation_id: "ai.toolCall".into(),
                ..Default::default()
            });
            span.end();
        }

        // Wait until the spawned enqueue tasks have completed.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let all_done = {
                let tasks = client.inner.flush_tasks.lock().unwrap();
                tasks.iter().all(|t| t.is_finished())
            };
            if all_done || std::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The next enqueue prunes all completed handles.
        let span = client.start_span(crate::traces::SpanOptions {
            name: "final".into(),
            event_id: "evt".into(),
            operation_id: "ai.toolCall".into(),
            ..Default::default()
        });
        span.end();

        let retained = client.inner.flush_tasks.lock().unwrap().len();
        assert!(
            retained <= 2,
            "completed task handles must be pruned on push; retained {retained} of 201"
        );
    }
}
