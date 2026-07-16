use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::client::Client;
use crate::error::Result;
use crate::traces::{
    LlmOptions, LlmSpan, Span, SpanOptions, ToolOptions, ToolSpan, TrackToolOptions,
};

/// Attachment shape shared across event payloads. Mirrors the canonical
/// `BaseAttachmentSchema` from `@raindrop-ai/schemas/ingest` and the Go SDK's `Attachment`
/// struct.
///
/// On the dashboard, attachments are split by `role` into `inputAttachments[]` vs
/// `outputAttachments[]`. Backend ingestion auto-generates an `attachment_id` (UUID v4) when
/// not supplied, but callers can pass their own — useful for retries and for cross-referencing
/// the same attachment from a follow-up `Signal::attachment_id`.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attachment {
    /// Type of attachment. Accepted values: `"text"`, `"code"`, `"image"`, `"iframe"`.
    /// Note: the dashboard frontend's `AttachmentSchema` only displays
    /// `"text" | "image" | "iframe"` — `"code"` attachments survive ingestion but are
    /// filtered from the dashboard's attachment view.
    #[serde(rename = "type")]
    pub kind: String,
    /// Whether the attachment belongs to the model `"input"` or `"output"`. Any value other
    /// than `"input"` is treated as `"output"` by the backend.
    pub role: String,
    /// Optional UUID identifying this attachment. If empty on the wire, the backend
    /// auto-assigns one. Set explicitly to round-trip with
    /// [`Signal::attachment_id`](crate::signals::Signal::attachment_id) or for idempotent retries.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub attachment_id: String,
    /// Optional logical name (e.g. `"snippet.py"`, `"summary.md"`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Attachment value (typically a string body or URL).
    pub value: String,
    /// Optional language hint. Only emitted for `kind == "code"` attachments per the
    /// canonical schema's discriminated union.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub language: String,
}

/// A non-AI event ("user signed up", "session started", …).
#[derive(Debug, Default, Clone)]
pub struct Event {
    /// Optional event id; one is generated if empty.
    pub event_id: String,
    /// User the event belongs to.
    pub user_id: String,
    /// Event name, defaults to `ai_generation` if empty.
    pub event: String,
    /// Optional timestamp; defaults to `now()`.
    pub timestamp: Option<OffsetDateTime>,
    /// Free-form properties.
    pub properties: BTreeMap<String, Value>,
    /// Attachments to ship with the event.
    pub attachments: Vec<Attachment>,
    /// Feature flags active for this event (flag name → value). Serialized
    /// verbatim as the top-level `feature_flags` string→string object on the
    /// wire (matching the JS SDK's event-shipper). Empty → key omitted.
    pub feature_flags: BTreeMap<String, String>,
}

/// An AI event (model invocation).
#[derive(Debug, Default, Clone)]
pub struct AiEvent {
    /// Optional event id; one is generated if empty.
    pub event_id: String,
    /// User the event belongs to.
    pub user_id: String,
    /// Event name, defaults to `ai_generation` if empty.
    pub event: String,
    /// Optional timestamp; defaults to `now()`.
    pub timestamp: Option<OffsetDateTime>,
    /// Model input.
    pub input: String,
    /// Model output.
    pub output: String,
    /// Model identifier (e.g. `gpt-4o`).
    pub model: String,
    /// Conversation/thread identifier for grouping events.
    pub convo_id: String,
    /// Free-form properties.
    pub properties: BTreeMap<String, Value>,
    /// Attachments.
    pub attachments: Vec<Attachment>,
    /// Feature flags active for this event (flag name → value). Serialized
    /// verbatim as the top-level `feature_flags` string→string object on the
    /// wire (matching the JS SDK's event-shipper). Empty → key omitted.
    pub feature_flags: BTreeMap<String, String>,
}

/// Options for [`Client::begin`].
#[derive(Debug, Default, Clone)]
pub struct BeginOptions {
    /// Optional event id; one is generated if empty.
    pub event_id: String,
    /// User the interaction belongs to.
    pub user_id: String,
    /// Event name, defaults to `ai_generation` if empty.
    pub event: String,
    /// Optional timestamp; defaults to `now()`.
    pub timestamp: Option<OffsetDateTime>,
    /// Initial input (model prompt).
    pub input: String,
    /// Model identifier.
    pub model: String,
    /// Conversation/thread identifier.
    pub convo_id: String,
    /// Initial properties.
    pub properties: BTreeMap<String, Value>,
    /// Initial attachments.
    pub attachments: Vec<Attachment>,
    /// Feature flags active for this interaction (flag name → value). Serialized
    /// verbatim as the top-level `feature_flags` string→string object on the
    /// wire (matching the JS SDK's event-shipper). Empty → key omitted.
    pub feature_flags: BTreeMap<String, String>,
}

/// Options for [`Interaction::patch`] / [`Client::patch`].
#[derive(Debug, Default, Clone)]
pub struct PatchOptions {
    /// Update the user id.
    pub user_id: String,
    /// Update the event name.
    pub event: String,
    /// Optional timestamp.
    pub timestamp: Option<OffsetDateTime>,
    /// Update the input.
    pub input: String,
    /// Update the output.
    pub output: String,
    /// Update the model.
    pub model: String,
    /// Update the conversation id.
    pub convo_id: String,
    /// Properties to merge into the patch.
    pub properties: BTreeMap<String, Value>,
    /// Attachments to append.
    pub attachments: Vec<Attachment>,
    /// Feature flags to merge into the patch (flag name → value). Merged like
    /// [`properties`](Self::properties) — last write wins per key. Serialized
    /// verbatim as the top-level `feature_flags` string→string object on the
    /// wire (matching the JS SDK's event-shipper). Empty → no change.
    pub feature_flags: BTreeMap<String, String>,
    /// Override the `is_pending` flag.
    pub is_pending: Option<bool>,
}

/// Options for [`Interaction::finish`].
#[derive(Debug, Default, Clone)]
pub struct FinishOptions {
    /// Final timestamp.
    pub timestamp: Option<OffsetDateTime>,
    /// Final output.
    pub output: String,
    /// Final model identifier.
    pub model: String,
    /// Final properties.
    pub properties: BTreeMap<String, Value>,
    /// Final attachments to append.
    pub attachments: Vec<Attachment>,
    /// Feature flags to merge into the final patch (flag name → value). Merged
    /// like [`properties`](Self::properties) — last write wins per key.
    /// Serialized verbatim as the top-level `feature_flags` string→string
    /// object on the wire (matching the JS SDK's event-shipper). Empty → no
    /// change.
    pub feature_flags: BTreeMap<String, String>,
}

/// In-progress interaction returned by [`Client::begin`]. Holds an `event_id` and forwards
/// patches to the underlying [`Client`].
#[derive(Debug, Clone)]
pub struct Interaction {
    pub(crate) client: Option<Client>,
    pub(crate) event_id: String,
    /// Sticky `user_id` captured from `BeginOptions`. Auto-attached to spans started via this
    /// interaction so they appear under the same user in the dashboard's traces view.
    pub(crate) user_id: String,
    /// Sticky `convo_id` captured from `BeginOptions`. Auto-attached to spans started via this
    /// interaction so they're grouped under the same conversation in the dashboard.
    pub(crate) convo_id: String,
    /// Sticky `event` (event name) captured from `BeginOptions`. Mostly informational — used by
    /// matching the Python/Go/JS SDK shape that exposes interaction metadata to spans.
    pub(crate) event: String,
}

impl Interaction {
    /// Create a no-op interaction (used when the client is disabled).
    pub(crate) fn noop() -> Self {
        Self {
            client: None,
            event_id: String::new(),
            user_id: String::new(),
            convo_id: String::new(),
            event: String::new(),
        }
    }

    /// Internal constructor for resumed/standalone interactions where only `event_id` is known.
    pub(crate) fn new(client: Client, event_id: String) -> Self {
        Self {
            client: Some(client),
            event_id,
            user_id: String::new(),
            convo_id: String::new(),
            event: String::new(),
        }
    }

    /// Internal constructor used by [`Client::begin`] to capture sticky association properties
    /// (`user_id`, `convo_id`, `event`) so they propagate to spans started via this interaction.
    pub(crate) fn new_with_context(
        client: Client,
        event_id: String,
        user_id: String,
        convo_id: String,
        event: String,
    ) -> Self {
        Self {
            client: Some(client),
            event_id,
            user_id,
            convo_id,
            event,
        }
    }

    /// Returns the event id this interaction is keyed on.
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Alias for [`Interaction::event_id`] (parity with the JS/Go SDK naming).
    pub fn get_event_id(&self) -> &str {
        &self.event_id
    }

    /// Apply a patch to this interaction.
    pub async fn patch(&self, opts: PatchOptions) -> Result<()> {
        if let Some(client) = &self.client {
            client.patch(&self.event_id, opts).await
        } else {
            Ok(())
        }
    }

    /// Merge properties into the interaction.
    pub async fn set_properties(&self, properties: BTreeMap<String, Value>) -> Result<()> {
        self.patch(PatchOptions {
            properties,
            ..Default::default()
        })
        .await
    }

    /// Set a single property.
    pub async fn set_property(
        &self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Result<()> {
        let key = key.into();
        if key.is_empty() {
            return Ok(());
        }
        let mut props = BTreeMap::new();
        props.insert(key, value.into());
        self.set_properties(props).await
    }

    /// Append attachments.
    pub async fn add_attachments(&self, attachments: Vec<Attachment>) -> Result<()> {
        self.patch(PatchOptions {
            attachments,
            ..Default::default()
        })
        .await
    }

    /// Merge feature flags (flag name → value) into the interaction. They ride
    /// along on the next flushed patch as the top-level `feature_flags`
    /// string→string object on the wire (matching the JS SDK's event-shipper).
    /// An empty map is a no-op.
    pub async fn set_feature_flags(&self, feature_flags: BTreeMap<String, String>) -> Result<()> {
        self.patch(PatchOptions {
            feature_flags,
            ..Default::default()
        })
        .await
    }

    /// Set a single feature flag (flag name → value). An empty key is a no-op.
    pub async fn set_feature_flag(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<()> {
        let key = key.into();
        if key.is_empty() {
            return Ok(());
        }
        let mut flags = BTreeMap::new();
        flags.insert(key, value.into());
        self.set_feature_flags(flags).await
    }

    /// Update the input.
    pub async fn set_input(&self, input: impl Into<String>) -> Result<()> {
        self.patch(PatchOptions {
            input: input.into(),
            ..Default::default()
        })
        .await
    }

    /// Finalize the interaction and ship the final patch.
    ///
    /// The interaction's captured `user_id`/`convo_id`/`event` ride along in
    /// the final patch, so finishing works even if the buffer's sticky
    /// context for this event id was evicted under the queue bound.
    pub async fn finish(&self, opts: FinishOptions) -> Result<()> {
        if let Some(client) = &self.client {
            let res = client
                .finish_with_context(
                    &self.event_id,
                    opts,
                    &self.user_id,
                    &self.convo_id,
                    &self.event,
                )
                .await;
            client.forget_interaction(&self.event_id);
            res
        } else {
            Ok(())
        }
    }

    /// Start a manually-managed span linked to this interaction's event id.
    ///
    /// The span automatically inherits the interaction's sticky `user_id` and `convo_id` (set
    /// in [`BeginOptions`]) as `traceloop.association.properties.{user_id,convo_id}` attributes,
    /// so downstream span queries on the dashboard correctly group the span under the same
    /// user and conversation as its parent event. User-supplied properties always override
    /// these defaults.
    pub fn start_span(&self, mut opts: SpanOptions) -> Span {
        if opts.event_id.is_empty() {
            opts.event_id = self.event_id.clone();
        }
        self.inject_association_properties(&mut opts.properties);
        match &self.client {
            Some(client) => client.start_span(opts),
            None => Span::noop(),
        }
    }

    /// Start a manually-managed tool span linked to this interaction's event id.
    ///
    /// As with [`start_span`](Self::start_span), the underlying span inherits the interaction's
    /// `user_id` and `convo_id` association properties.
    pub fn start_tool_span(&self, name: impl Into<String>, mut opts: ToolOptions) -> ToolSpan {
        self.inject_association_properties(&mut opts.properties);
        match &self.client {
            Some(client) => client.start_tool_span(name, opts, &self.event_id),
            None => ToolSpan::noop(),
        }
    }

    /// Start a manually-managed LLM span linked to this interaction's event id.
    ///
    /// As with [`start_span`](Self::start_span), the underlying span inherits the interaction's
    /// `user_id` and `convo_id` association properties.
    pub fn start_llm_span(&self, name: impl Into<String>, mut opts: LlmOptions) -> LlmSpan {
        self.inject_association_properties(&mut opts.properties);
        match &self.client {
            Some(client) => client.start_llm_span(name, opts, &self.event_id),
            None => LlmSpan::noop(),
        }
    }

    /// Inject the interaction's sticky `user_id`, `convo_id`, and `event` into a properties
    /// map without overwriting caller-supplied values. Each property becomes a
    /// `traceloop.association.properties.<key>` attribute on the underlying span. Mirrors the
    /// Python SDK's `Interaction.start_span` which propagates the same four keys.
    fn inject_association_properties(&self, properties: &mut BTreeMap<String, Value>) {
        if !self.user_id.is_empty() {
            properties
                .entry("user_id".to_string())
                .or_insert_with(|| Value::String(self.user_id.clone()));
        }
        if !self.convo_id.is_empty() {
            properties
                .entry("convo_id".to_string())
                .or_insert_with(|| Value::String(self.convo_id.clone()));
        }
        if !self.event.is_empty() {
            properties
                .entry("event".to_string())
                .or_insert_with(|| Value::String(self.event.clone()));
        }
    }

    /// Run an async closure inside a manually-managed span. The span is ended when the closure
    /// returns. Errors set the span status to `ERROR` automatically. The span itself is also
    /// passed to the closure (for adding attributes, child spans, etc.).
    pub async fn with_span<F, Fut, T, E>(
        &self,
        opts: SpanOptions,
        fn_: F,
    ) -> std::result::Result<T, E>
    where
        F: FnOnce(Span) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
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

    /// Retroactively log a tool call (with start/end times or a duration). Mirrors the Go SDK's
    /// `Interaction.TrackTool`. The span inherits the interaction's `user_id` and `convo_id`
    /// (see [`start_span`](Self::start_span) for details).
    pub fn track_tool(&self, mut opts: TrackToolOptions) {
        if let Some(client) = &self.client {
            self.inject_association_properties(&mut opts.properties);
            client.track_tool_for_interaction(&self.event_id, opts);
        }
    }
}
