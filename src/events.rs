use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::client::Client;
use crate::error::Result;
use crate::traces::{Span, SpanOptions, ToolOptions, ToolSpan, TrackToolOptions};

/// Attachment shape shared across event payloads. Mirrors the Go SDK's `Attachment` struct.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attachment {
    /// Type of attachment, e.g. "text", "code", "image".
    #[serde(rename = "type")]
    pub kind: String,
    /// Whether the attachment belongs to the model "input" or "output".
    pub role: String,
    /// Optional logical name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Attachment value (typically a string body or URL).
    pub value: String,
    /// Optional language hint for code attachments.
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
}

/// In-progress interaction returned by [`Client::begin`]. Holds an `event_id` and forwards
/// patches to the underlying [`Client`].
#[derive(Debug, Clone)]
pub struct Interaction {
    pub(crate) client: Option<Client>,
    pub(crate) event_id: String,
}

impl Interaction {
    /// Create a no-op interaction (used when the client is disabled).
    pub(crate) fn noop() -> Self {
        Self {
            client: None,
            event_id: String::new(),
        }
    }

    /// Internal constructor.
    pub(crate) fn new(client: Client, event_id: String) -> Self {
        Self {
            client: Some(client),
            event_id,
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

    /// Update the input.
    pub async fn set_input(&self, input: impl Into<String>) -> Result<()> {
        self.patch(PatchOptions {
            input: input.into(),
            ..Default::default()
        })
        .await
    }

    /// Finalize the interaction and ship the final patch.
    pub async fn finish(&self, opts: FinishOptions) -> Result<()> {
        if let Some(client) = &self.client {
            let res = client.finish(&self.event_id, opts).await;
            client.forget_interaction(&self.event_id);
            res
        } else {
            Ok(())
        }
    }

    /// Start a manually-managed span linked to this interaction's event id.
    pub fn start_span(&self, mut opts: SpanOptions) -> Span {
        if opts.event_id.is_empty() {
            opts.event_id = self.event_id.clone();
        }
        match &self.client {
            Some(client) => client.start_span(opts),
            None => Span::noop(),
        }
    }

    /// Start a manually-managed tool span linked to this interaction's event id.
    pub fn start_tool_span(&self, name: impl Into<String>, opts: ToolOptions) -> ToolSpan {
        match &self.client {
            Some(client) => client.start_tool_span(name, opts, &self.event_id),
            None => ToolSpan::noop(),
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
    /// `Interaction.TrackTool`.
    pub fn track_tool(&self, opts: TrackToolOptions) {
        if let Some(client) = &self.client {
            client.track_tool_for_interaction(&self.event_id, opts);
        }
    }
}
