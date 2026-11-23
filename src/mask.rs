//! The `mask` client option: a function the caller writes that replaces
//! sensitive parts of content before it leaves the process.
//!
//! It runs on every event, signal, identify and span payload just before the
//! payload is queued or sent. It sees only content fields: identity and
//! routing fields (ids, user id, event name, model, timing) and the SDK's own
//! properties are never passed to it.
//!
//! Fail-closed: when the function panics, returns an error, or returns a value
//! the field can't carry, the field ships as [`MASK_FAILED`] (an object as
//! `{"error": "<MASK FAILED>"}`, attachments as `[]`) and the rest of the
//! payload still ships. The warning names only what went wrong, never the
//! content or the error's message.

use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::otlp::{AttributeValue, OtlpSpan};

/// Sent in place of a field the mask function failed on.
pub const MASK_FAILED: &str = "<MASK FAILED>";

/// The error a mask function may return. Its message is never logged, since
/// it may quote the content being masked.
pub type MaskError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Which payload a masked value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MaskKind {
    /// An event (`events/track` or `events/track_partial`).
    Event,
    /// A signal (`signals/track`).
    Signal,
    /// An identify call (`users/identify`).
    Identify,
    /// A trace span.
    Span,
}

impl MaskKind {
    /// The kind as it appears in logs and contract text: `event`, `signal`,
    /// `identify` or `span`.
    pub fn as_str(self) -> &'static str {
        match self {
            MaskKind::Event => "event",
            MaskKind::Signal => "signal",
            MaskKind::Identify => "identify",
            MaskKind::Span => "span",
        }
    }
}

impl fmt::Display for MaskKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where a masked value came from.
///
/// | `kind` | `field` | Value |
/// | --- | --- | --- |
/// | [`MaskKind::Event`] | `ai_data.input`, `ai_data.output` | string |
/// | [`MaskKind::Event`] | `properties`, `attachments` | object, array |
/// | [`MaskKind::Signal`] | `properties` (with `comment` and `after` in it) | object |
/// | [`MaskKind::Identify`] | `traits` | object |
/// | [`MaskKind::Span`] | a content attribute's name, or `status.message` | string |
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaskContext {
    /// The payload kind.
    pub kind: MaskKind,
    /// The wire field, or for a span the attribute name or `status.message`.
    pub field: String,
}

impl MaskContext {
    /// A context, e.g. to unit test a mask function.
    pub fn new(kind: MaskKind, field: impl Into<String>) -> Self {
        Self {
            kind,
            field: field.into(),
        }
    }
}

type MaskFn = dyn Fn(Value, &MaskContext) -> Result<Value, MaskError> + Send + Sync;

/// A caller's mask function, as the client holds it.
#[derive(Clone)]
pub(crate) struct Mask(Arc<MaskFn>);

impl fmt::Debug for Mask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Mask(..)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    String,
    Object,
    Array,
}

impl Shape {
    fn of(value: &Value) -> Option<Shape> {
        match value {
            Value::String(_) => Some(Shape::String),
            Value::Object(_) => Some(Shape::Object),
            Value::Array(_) => Some(Shape::Array),
            _ => None,
        }
    }

    fn failed(self) -> Value {
        match self {
            Shape::String => Value::String(MASK_FAILED.to_string()),
            Shape::Object => serde_json::json!({ "error": MASK_FAILED }),
            Shape::Array => Value::Array(Vec::new()),
        }
    }

    fn wrong_type(self) -> &'static str {
        match self {
            Shape::String => "it returned a value that is not a string",
            Shape::Object => "it returned a value that is not an object",
            Shape::Array => "it returned a value that is not an array",
        }
    }
}

/// Keys the SDK sets inside event `properties`: not caller content, so never
/// passed to the mask and always kept.
fn is_sdk_property(key: &str) -> bool {
    key == "$context" || crate::app_git::is_canonical_property(key)
}

impl Mask {
    pub(crate) fn new<F>(f: F) -> Self
    where
        F: Fn(Value, &MaskContext) -> Result<Value, MaskError> + Send + Sync + 'static,
    {
        Mask(Arc::new(f))
    }

    /// One field through the mask function, or its fail-closed stand-in.
    /// Never panics.
    fn value(&self, value: Value, shape: Shape, context: &MaskContext) -> Value {
        let reason = match catch_unwind(AssertUnwindSafe(|| (self.0)(value, context))) {
            Ok(Ok(out)) if Shape::of(&out) == Some(shape) => return out,
            Ok(Ok(_)) => shape.wrong_type(),
            Ok(Err(_)) => "it returned an error",
            Err(payload) => {
                // Dropping a panic payload can itself panic; never let it out.
                let _ = catch_unwind(AssertUnwindSafe(move || drop(payload)));
                "it panicked"
            }
        };
        if crate::helpers::should_log_rate_limited("mask_failed") {
            tracing::warn!(
                kind = context.kind.as_str(),
                field = context.field.as_str(),
                "raindrop: mask function failed ({reason}); sending {MASK_FAILED} in its place \
                 (logged at most once per 30s)"
            );
        }
        shape.failed()
    }

    fn string(&self, value: String, context: &MaskContext) -> String {
        match self.value(Value::String(value), Shape::String, context) {
            Value::String(s) => s,
            _ => MASK_FAILED.to_string(),
        }
    }

    /// An event, signal or identify payload (the JSON body item) with its
    /// content fields passed through the mask. Identity and routing fields
    /// are never passed.
    pub(crate) fn payload(&self, kind: MaskKind, payload: &mut Value) {
        let fields: &[&[&str]] = match kind {
            MaskKind::Event => &[
                &["ai_data", "input"],
                &["ai_data", "output"],
                &["properties"],
                &["attachments"],
            ],
            MaskKind::Signal => &[&["properties"]],
            MaskKind::Identify => &[&["traits"]],
            MaskKind::Span => &[],
        };
        for path in fields {
            let context = MaskContext::new(kind, path.join("."));
            self.mask_at(payload, path, &context);
        }
    }

    fn mask_at(&self, node: &mut Value, path: &[&str], context: &MaskContext) {
        let Some((head, rest)) = path.split_first() else {
            return;
        };
        let Some(child) = node.as_object_mut().and_then(|map| map.get_mut(*head)) else {
            return;
        };
        if !rest.is_empty() {
            self.mask_at(child, rest, context);
            return;
        }
        let Some(shape) = Shape::of(child) else {
            // Null, numbers and booleans carry no content to mask.
            return;
        };
        let value = std::mem::take(child);
        *child = match value {
            Value::Object(map) if context.kind == MaskKind::Event && *head == "properties" => {
                let (sdk, own): (Map<String, Value>, Map<String, Value>) =
                    map.into_iter().partition(|(key, _)| is_sdk_property(key));
                let mut out = match self.value(Value::Object(own), Shape::Object, context) {
                    Value::Object(masked) => masked,
                    _ => Map::new(),
                };
                out.extend(sdk);
                Value::Object(out)
            }
            other => self.value(other, shape, context),
        };
    }

    /// A span with its content attributes (prompts, completions, tool input
    /// and output, copies of the caller's event properties) and its status
    /// message passed through the mask, one string at a time. Identifiers,
    /// model names, token counts and timing are never passed.
    pub(crate) fn span(&self, span: &mut OtlpSpan) {
        for attr in &mut span.attributes {
            if is_span_content_key(&attr.key) {
                let context = MaskContext::new(MaskKind::Span, attr.key.clone());
                self.attribute_value(&mut attr.value, &context);
            }
        }
        if let Some(status) = span.status.as_mut() {
            if !status.message.is_empty() {
                let context = MaskContext::new(MaskKind::Span, "status.message");
                status.message = self.string(std::mem::take(&mut status.message), &context);
            }
        }
    }

    fn attribute_value(&self, value: &mut AttributeValue, context: &MaskContext) {
        if let Some(s) = value.string_value.take() {
            value.string_value = Some(self.string(s, context));
        }
        if let Some(array) = value.array_value.as_mut() {
            for item in &mut array.values {
                self.attribute_value(item, context);
            }
        }
    }
}

/// Last attribute-name segments that always name content.
const CONTENT_SUFFIXES: &[&str] = &[
    "input",
    "output",
    "inputs",
    "outputs",
    "args",
    "arguments",
    "result",
    "results",
    "content",
    "text",
    "messages",
    "prompt",
    "completion",
    "instructions",
    "system_instructions",
    "reasoning",
    "reasoningText",
    "thinking",
    "toolCalls",
    "tool_calls",
    "object",
    "value",
    "values",
    "intent",
    "query",
    "tool_input",
];

/// Content attributes whose last segment is ambiguous on its own.
const CONTENT_KEYS: &[&str] = &[
    "ai.prompt.system",
    "ai.subagent.result",
    "agent.session.title",
    "error.message",
    "exception.message",
    "exception.stacktrace",
];

/// Prefixes that hold identifiers and metadata, never content.
const NEVER_TOUCHED_PREFIXES: &[&str] = &[
    "traceloop.association.properties.",
    "ai.telemetry.metadata.",
    "ai.settings.context.",
    "raindrop.app.",
];

/// Prefixes under which a span carries copies of the caller's properties.
const PROPERTY_COPY_PREFIXES: &[&str] = &[
    "traceloop.association.properties.",
    "ai.telemetry.metadata.traceloop.association.properties.",
];

/// The whole properties object as JSON.
const PROPERTY_COPY_KEYS: &[&str] = &["ai.telemetry.metadata.raindrop.properties"];

/// Association properties the SDK writes itself.
const SDK_ASSOCIATION_NAMES: &[&str] = &["user_id", "event_id", "convo_id", "event_name"];

/// Whether a span attribute carries content: a copy of a caller's property,
/// or (outside the identifier prefixes) a known content attribute.
pub(crate) fn is_span_content_key(key: &str) -> bool {
    if PROPERTY_COPY_KEYS.contains(&key) {
        return true;
    }
    for prefix in PROPERTY_COPY_PREFIXES {
        if let Some(name) = key.strip_prefix(prefix) {
            if !name.is_empty()
                && !SDK_ASSOCIATION_NAMES.contains(&name)
                && !name.starts_with("raindrop.")
            {
                return true;
            }
        }
    }
    if NEVER_TOUCHED_PREFIXES.iter().any(|p| key.starts_with(p)) {
        return false;
    }
    let last = key.rsplit('.').next().unwrap_or(key);
    CONTENT_KEYS.contains(&key) || CONTENT_SUFFIXES.contains(&last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::{Attribute, OtlpKeyValue, OtlpStatus};
    use serde_json::json;

    /// The conformance harness's `tag` test mask.
    fn tag() -> Mask {
        Mask::new(|value, ctx| {
            Ok(match value {
                Value::String(_) => Value::String(format!("<MASKED {} {}>", ctx.kind, ctx.field)),
                Value::Array(_) => json!([]),
                _ => json!({ "masked": format!("{} {}", ctx.kind, ctx.field) }),
            })
        })
    }

    fn failing_with(f: fn(Value) -> Result<Value, MaskError>) -> Mask {
        Mask::new(move |value, _| f(value))
    }

    fn event() -> Value {
        json!({
            "event_id": "evt-1",
            "user_id": "Jane Doe",
            "event": "chat",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "ai_data": {
                "input": "Contact Jane Doe",
                "output": "Card 4111 1111 1111 1111",
                "model": "mock-gpt",
                "convo_id": "c-1"
            },
            "properties": {
                "plan": "pro",
                "note": "Jane Doe",
                "$context": { "library": { "name": "raindrop-rust" } },
                "raindrop.app.commit_sha": "abc123"
            },
            "attachments": [{ "type": "text", "role": "input", "value": "secret" }]
        })
    }

    #[test]
    fn event_content_fields_get_kind_and_field() {
        let mut payload = event();
        tag().payload(MaskKind::Event, &mut payload);
        assert_eq!(payload["ai_data"]["input"], "<MASKED event ai_data.input>");
        assert_eq!(
            payload["ai_data"]["output"],
            "<MASKED event ai_data.output>"
        );
        assert_eq!(payload["properties"]["masked"], "event properties");
        assert!(payload["properties"].get("note").is_none());
        assert!(payload["properties"].get("plan").is_none());
        assert_eq!(payload["attachments"], json!([]));
    }

    #[test]
    fn identity_routing_and_sdk_properties_are_never_passed() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = seen.clone();
        let mask = Mask::new(move |value, ctx| {
            record
                .lock()
                .unwrap()
                .push((ctx.field.clone(), value.clone()));
            Ok(value)
        });
        let mut payload = event();
        let before = payload.clone();
        mask.payload(MaskKind::Event, &mut payload);
        assert_eq!(payload, before, "an identity mask changes nothing");
        let seen = seen.lock().unwrap();
        let fields: Vec<&str> = seen.iter().map(|(f, _)| f.as_str()).collect();
        assert_eq!(
            fields,
            [
                "ai_data.input",
                "ai_data.output",
                "properties",
                "attachments"
            ]
        );
        let props = &seen[2].1;
        assert_eq!(props, &json!({ "plan": "pro", "note": "Jane Doe" }));
    }

    #[test]
    fn sdk_properties_survive_a_masked_or_failed_properties_object() {
        let mut payload = event();
        tag().payload(MaskKind::Event, &mut payload);
        assert_eq!(
            payload["properties"]["$context"]["library"]["name"],
            "raindrop-rust"
        );
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], "abc123");

        let mut payload = event();
        failing_with(|_| Err("boom".into())).payload(MaskKind::Event, &mut payload);
        assert_eq!(payload["properties"]["error"], MASK_FAILED);
        assert_eq!(payload["properties"]["raindrop.app.commit_sha"], "abc123");
        assert!(payload["properties"].get("note").is_none());
    }

    #[test]
    fn absent_and_non_content_fields_are_skipped() {
        let mut payload = json!({ "event_id": "e", "properties": {}, "attachments": [] });
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        let mask = Mask::new(move |value, _| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(value)
        });
        mask.payload(MaskKind::Event, &mut payload);
        // properties and attachments are present (if empty); ai_data is not.
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn error_fails_closed_for_every_shape() {
        let mut payload = event();
        failing_with(|_| Err("contains Jane Doe".into())).payload(MaskKind::Event, &mut payload);
        assert_eq!(payload["ai_data"]["input"], MASK_FAILED);
        assert_eq!(payload["ai_data"]["output"], MASK_FAILED);
        assert_eq!(payload["properties"]["error"], MASK_FAILED);
        assert_eq!(payload["attachments"], json!([]));
        // Identity is untouched and the payload still exists to ship.
        assert_eq!(payload["event_id"], "evt-1");
        assert_eq!(payload["user_id"], "Jane Doe");
        assert_eq!(payload["ai_data"]["model"], "mock-gpt");
    }

    #[test]
    fn panic_fails_closed() {
        let mut payload = event();
        failing_with(|_| panic!("mask panicked")).payload(MaskKind::Event, &mut payload);
        assert_eq!(payload["ai_data"]["input"], MASK_FAILED);
        assert_eq!(payload["properties"]["error"], MASK_FAILED);
        assert_eq!(payload["attachments"], json!([]));
        assert_eq!(payload["event"], "chat");
    }

    #[test]
    fn wrong_type_fails_closed() {
        // A string field returned as an object, objects as strings, arrays as objects.
        let swap = Mask::new(|value, _| {
            Ok(match value {
                Value::String(_) => json!({ "not": "a string" }),
                Value::Object(_) => json!("not an object"),
                _ => json!({ "not": "an array" }),
            })
        });
        let mut payload = event();
        swap.payload(MaskKind::Event, &mut payload);
        assert_eq!(payload["ai_data"]["input"], MASK_FAILED);
        assert_eq!(payload["properties"]["error"], MASK_FAILED);
        assert_eq!(payload["attachments"], json!([]));

        let null = Mask::new(|_, _| Ok(Value::Null));
        let mut payload = json!({ "traits": { "email": "Jane Doe" } });
        null.payload(MaskKind::Identify, &mut payload);
        assert_eq!(payload["traits"], json!({ "error": MASK_FAILED }));
    }

    #[test]
    fn signal_properties_include_comment_and_after() {
        let seen = Arc::new(std::sync::Mutex::new(Value::Null));
        let record = seen.clone();
        let mask = Mask::new(move |value, ctx| {
            assert_eq!(
                (ctx.kind, ctx.field.as_str()),
                (MaskKind::Signal, "properties")
            );
            *record.lock().unwrap() = value;
            Ok(json!({ "masked": "signal properties" }))
        });
        let mut payload = json!({
            "event_id": "evt-1",
            "signal_name": "feedback",
            "signal_type": "default",
            "properties": { "note": "x", "comment": "reach me", "after": "call me" }
        });
        mask.payload(MaskKind::Signal, &mut payload);
        assert_eq!(
            *seen.lock().unwrap(),
            json!({ "note": "x", "comment": "reach me", "after": "call me" })
        );
        assert_eq!(
            payload["properties"],
            json!({ "masked": "signal properties" })
        );
        assert_eq!(payload["event_id"], "evt-1");
        assert_eq!(payload["signal_name"], "feedback");
    }

    #[test]
    fn identify_traits_are_masked_user_id_is_not() {
        let mut payload = json!({
            "user_id": "Jane Doe",
            "traits": { "email": "Jane Doe", "plan": "pro" }
        });
        tag().payload(MaskKind::Identify, &mut payload);
        assert_eq!(payload["user_id"], "Jane Doe");
        assert_eq!(payload["traits"], json!({ "masked": "identify traits" }));
    }

    fn span() -> OtlpSpan {
        let attrs = vec![
            Attribute::string("ai.operationId", "ai.toolCall"),
            Attribute::string("ai.telemetry.metadata.raindrop.eventId", "evt-1"),
            Attribute::string("traceloop.association.properties.event_id", "evt-1"),
            Attribute::string("traceloop.association.properties.note", "Jane Doe"),
            Attribute::string(
                "traceloop.association.properties.raindrop.app.branch",
                "main",
            ),
            Attribute::string("traceloop.entity.name", "search"),
            Attribute::string("traceloop.entity.input", "{\"q\":\"Jane Doe\"}"),
            Attribute::string("traceloop.entity.output", "found"),
            Attribute::string("gen_ai.prompt.0.content", "hi"),
            Attribute::string("gen_ai.request.model", "mock-gpt"),
            Attribute::int("gen_ai.usage.input_tokens", 11),
            Attribute::string_array("ai.response.text", vec!["a".into(), "b".into()]),
        ];
        OtlpSpan {
            name: "search".into(),
            attributes: attrs.into_iter().map(OtlpKeyValue::from).collect(),
            status: Some(OtlpStatus {
                code: 2,
                message: "failed for Jane Doe".into(),
            }),
            ..Default::default()
        }
    }

    fn attr<'a>(span: &'a OtlpSpan, key: &str) -> &'a AttributeValue {
        &span.attributes.iter().find(|a| a.key == key).unwrap().value
    }

    #[test]
    fn span_content_and_status_message_are_masked() {
        let mut s = span();
        tag().span(&mut s);
        let string = |key: &str| attr(&s, key).string_value.clone().unwrap();
        assert_eq!(
            string("traceloop.entity.input"),
            "<MASKED span traceloop.entity.input>"
        );
        assert_eq!(
            string("traceloop.entity.output"),
            "<MASKED span traceloop.entity.output>"
        );
        assert_eq!(
            string("gen_ai.prompt.0.content"),
            "<MASKED span gen_ai.prompt.0.content>"
        );
        assert_eq!(
            string("traceloop.association.properties.note"),
            "<MASKED span traceloop.association.properties.note>"
        );
        let array = attr(&s, "ai.response.text").array_value.clone().unwrap();
        assert!(array
            .values
            .iter()
            .all(|v| v.string_value.as_deref() == Some("<MASKED span ai.response.text>")));
        assert_eq!(
            s.status.as_ref().unwrap().message,
            "<MASKED span status.message>"
        );
        assert_eq!(s.status.as_ref().unwrap().code, 2);
        // Not content: identifiers, names, model, usage and SDK association properties.
        assert_eq!(string("ai.operationId"), "ai.toolCall");
        assert_eq!(string("ai.telemetry.metadata.raindrop.eventId"), "evt-1");
        assert_eq!(string("traceloop.association.properties.event_id"), "evt-1");
        assert_eq!(
            string("traceloop.association.properties.raindrop.app.branch"),
            "main"
        );
        assert_eq!(string("traceloop.entity.name"), "search");
        assert_eq!(string("gen_ai.request.model"), "mock-gpt");
        assert_eq!(
            attr(&s, "gen_ai.usage.input_tokens").int_value.as_deref(),
            Some("11")
        );
        assert_eq!(s.name, "search");
    }

    #[test]
    fn span_fails_closed() {
        let mut s = span();
        failing_with(|_| panic!("nope")).span(&mut s);
        assert_eq!(
            attr(&s, "traceloop.entity.input").string_value.as_deref(),
            Some(MASK_FAILED)
        );
        assert_eq!(s.status.as_ref().unwrap().message, MASK_FAILED);
        assert_eq!(
            attr(&s, "gen_ai.request.model").string_value.as_deref(),
            Some("mock-gpt")
        );

        let mut s = span();
        Mask::new(|_, _| Ok(json!(42))).span(&mut s);
        assert_eq!(
            attr(&s, "traceloop.entity.output").string_value.as_deref(),
            Some(MASK_FAILED)
        );
    }

    #[test]
    fn empty_status_message_is_not_passed() {
        let mut s = span();
        s.status = Some(OtlpStatus {
            code: 1,
            message: String::new(),
        });
        let mask = Mask::new(|value, ctx| {
            assert_ne!(ctx.field, "status.message");
            Ok(value)
        });
        mask.span(&mut s);
        assert_eq!(s.status.unwrap().message, "");
    }

    #[test]
    fn span_content_keys_follow_the_contract_rule() {
        for key in [
            "ai.prompt.messages",
            "ai.response.text",
            "ai.prompt",
            "traceloop.entity.input",
            "gen_ai.completion.0.content",
            "error.message",
            "ai.telemetry.metadata.raindrop.properties",
            "ai.telemetry.metadata.traceloop.association.properties.plan",
            "input",
        ] {
            assert!(is_span_content_key(key), "{key} is content");
        }
        for key in [
            "ai.telemetry.metadata.raindrop.eventId",
            "traceloop.association.properties.user_id",
            "traceloop.association.properties.raindrop.x",
            "ai.telemetry.metadata.input",
            "raindrop.app.branch",
            "gen_ai.request.model",
            "traceloop.entity.name",
            "ai.settings.context.output",
            "gen_ai.usage.input_tokens",
            "Input",
        ] {
            assert!(!is_span_content_key(key), "{key} is not content");
        }
    }
}
