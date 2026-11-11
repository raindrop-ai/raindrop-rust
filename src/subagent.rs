//! Detached sub-agent hand-off (CONTRACT 13).
//!
//! A detached sub-agent is work launched from one event that reports as its
//! own event, possibly in another process or service. Nothing is nested: the
//! launcher records a dispatch span, the child its own event and trace, and
//! the two are joined by attributes and a carrier passed with the launch.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde_json::{json, Value};

use crate::client::Client;
use crate::events::{BeginOptions, Interaction};
use crate::otlp::Attribute;
use crate::traces::{Span, ToolOptions};

/// Header that carries the hand-off fields in W3C baggage form. The carrier
/// also holds them as `baggage`, plus a `traceparent`.
pub const HANDOFF_HEADER: &str = "x-raindrop-handoff";

const CARRIER_EVENT_ID: &str = "raindrop-event-id";
const CARRIER_CHILD_EVENT_ID: &str = "raindrop-child-event-id";
const CARRIER_NAME: &str = "raindrop-handoff-name";
const CARRIER_CONVO_ID: &str = "raindrop-convo-id";
const CARRIER_USER_ID: &str = "raindrop-user-id";
const CARRIER_TRACE_ID: &str = "raindrop-trace-id";
const CARRIER_DISPATCH_SPAN_ID: &str = "raindrop-dispatch-span-id";

const HANDOFF_MODE: &str = "raindrop.handoff.mode";
const HANDOFF_CHILD_EVENT_ID: &str = "raindrop.handoff.childEventId";
const HANDOFF_NAME: &str = "raindrop.handoff.name";
const HANDOFF_PARENT_EVENT_ID: &str = "raindrop.handoff.parentEventId";
const HANDOFF_PARENT_SPAN_ID: &str = "raindrop.handoff.parentSpanId";
const AGENT_ROLE: &str = "raindrop.agent.role";

const DEFAULT_DISPATCH_TOOL_NAME: &str = "dispatch_subagent";
const DEFAULT_SUBAGENT_NAME: &str = "subagent";

/// A detached sub-agent launch, for [`Interaction::subagent`].
#[derive(Debug, Default, Clone)]
pub struct SubagentOptions {
    /// The sub-agent's name, shown on the launcher's event.
    pub name: String,
    /// The child's own event id. It must exist before the launch so the link
    /// resolves while the child is queued; one is generated when empty (a
    /// job id works too).
    pub child_event_id: String,
    /// What the sub-agent was asked to do, recorded on the dispatch span.
    pub input: Option<Value>,
    /// Name of the dispatch span; defaults to `dispatch_subagent`.
    pub tool_name: String,
}

/// A recorded launch: the child's event id and the carrier headers to pass
/// to it (`x-raindrop-handoff`, `baggage` and, when available,
/// `traceparent`).
#[derive(Debug, Default, Clone)]
pub struct SubagentDispatch {
    /// The child's event id.
    pub child_event_id: String,
    /// Carrier headers for the child.
    pub headers: BTreeMap<String, String>,
}

/// The child's own event fields, for [`Client::resume_subagent`]. Values the
/// carrier names win; these fill in what it leaves out.
#[derive(Debug, Default, Clone)]
pub struct ResumeSubagentOptions {
    /// User id, used when the carrier names none.
    pub user_id: String,
    /// Event name; defaults to the sub-agent's name.
    pub event: String,
    /// The child's input.
    pub input: String,
    /// Conversation id, used when the carrier names none.
    pub convo_id: String,
}

impl Interaction {
    /// Record a detached sub-agent dispatch on this interaction's event and
    /// return the carrier for the child (CONTRACT 13.1). The dispatch span
    /// carries the hand-off attributes; pass `headers` to the child, which
    /// opens its own event with [`Client::resume_subagent`].
    pub fn subagent(&self, opts: SubagentOptions) -> SubagentDispatch {
        let child_event_id = if opts.child_event_id.is_empty() {
            crate::helpers::new_event_id()
        } else {
            opts.child_event_id
        };
        let name = if opts.name.is_empty() {
            DEFAULT_SUBAGENT_NAME.to_string()
        } else {
            opts.name
        };
        let mut dispatch = SubagentDispatch {
            child_event_id: child_event_id.clone(),
            headers: BTreeMap::new(),
        };
        if self.client.is_none() {
            return dispatch;
        }
        let tool_name = if opts.tool_name.is_empty() {
            DEFAULT_DISPATCH_TOOL_NAME.to_string()
        } else {
            opts.tool_name
        };

        let tool = self.start_tool_span(
            tool_name,
            ToolOptions {
                input: opts.input,
                ..Default::default()
            },
        );
        tool.span().set_attributes([
            Attribute::string(HANDOFF_MODE, "detached"),
            Attribute::string(HANDOFF_CHILD_EVENT_ID, child_event_id.as_str()),
            Attribute::string(HANDOFF_NAME, name.as_str()),
        ]);
        // The dispatch's result is the job handle; the child's answer belongs
        // to the child's own event.
        tool.set_output(&json!({"jobId": child_event_id, "status": "accepted"}));
        let ids = hex_ids(tool.span());
        tool.end();

        let mut fields: Vec<(&str, &str)> = vec![
            (CARRIER_EVENT_ID, self.event_id.as_str()),
            (CARRIER_CHILD_EVENT_ID, child_event_id.as_str()),
            (CARRIER_NAME, name.as_str()),
            (CARRIER_CONVO_ID, self.convo_id.as_str()),
            (CARRIER_USER_ID, self.user_id.as_str()),
        ];
        if let Some((trace_id, span_id)) = &ids {
            fields.push((CARRIER_TRACE_ID, trace_id));
            fields.push((CARRIER_DISPATCH_SPAN_ID, span_id));
        }
        let baggage = encode_baggage(&fields);
        dispatch
            .headers
            .insert(HANDOFF_HEADER.to_string(), baggage.clone());
        dispatch.headers.insert("baggage".to_string(), baggage);
        if let Some((trace_id, span_id)) = ids {
            dispatch.headers.insert(
                "traceparent".to_string(),
                format!("00-{trace_id}-{span_id}-01"),
            );
        }
        dispatch
    }
}

impl Client {
    /// Open a detached sub-agent's own event from the carrier it was launched
    /// with (CONTRACT 13.2): the headers from [`Interaction::subagent`], from
    /// any Raindrop SDK. Every span the returned interaction records links
    /// back to the launcher. A missing or unreadable carrier never fails the
    /// work: the child runs as an unlinked event.
    pub async fn resume_subagent<'a, I>(
        &self,
        headers: I,
        opts: ResumeSubagentOptions,
    ) -> Interaction
    where
        I: IntoIterator<Item = (&'a String, &'a String)>,
    {
        let carrier = parse_carrier(headers);
        let field = |key: &str| carrier.get(key).cloned().unwrap_or_default();
        let first = |a: String, b: String| if a.is_empty() { b } else { a };

        let event_id = first(field(CARRIER_CHILD_EVENT_ID), String::new());
        let name = first(field(CARRIER_NAME), DEFAULT_SUBAGENT_NAME.to_string());
        let mut interaction = self
            .begin(BeginOptions {
                event_id,
                user_id: first(field(CARRIER_USER_ID), opts.user_id),
                event: first(opts.event, name.clone()),
                input: opts.input,
                convo_id: first(field(CARRIER_CONVO_ID), opts.convo_id),
                ..Default::default()
            })
            .await;
        let parent = field(CARRIER_EVENT_ID);
        if !parent.is_empty() {
            let mut attrs = vec![
                Attribute::string(HANDOFF_PARENT_EVENT_ID, parent),
                Attribute::string(HANDOFF_NAME, name),
                Attribute::string(AGENT_ROLE, "subagent"),
            ];
            let span_id = field(CARRIER_DISPATCH_SPAN_ID);
            if !span_id.is_empty() {
                attrs.push(Attribute::string(HANDOFF_PARENT_SPAN_ID, span_id));
            }
            interaction.span_attributes = attrs;
        }
        interaction
    }
}

/// Read the hand-off fields from `x-raindrop-handoff`, else from `baggage`,
/// filling the trace ids from `traceparent` when the baggage has none.
/// Header names match case-insensitively.
fn parse_carrier<'a, I>(headers: I) -> BTreeMap<String, String>
where
    I: IntoIterator<Item = (&'a String, &'a String)>,
{
    let lookup: BTreeMap<String, &String> = headers
        .into_iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
        .collect();
    let get = |key: &str| lookup.get(key).map(|v| v.as_str()).unwrap_or("");
    let mut fields = decode_baggage(get(HANDOFF_HEADER));
    if !fields.contains_key(CARRIER_EVENT_ID) {
        fields = decode_baggage(get("baggage"));
    }
    let parts: Vec<&str> = get("traceparent").trim().split('-').collect();
    if parts.len() == 4 && parts[1].len() == 32 && parts[2].len() == 16 {
        fields
            .entry(CARRIER_TRACE_ID.to_string())
            .or_insert_with(|| parts[1].to_string());
        fields
            .entry(CARRIER_DISPATCH_SPAN_ID.to_string())
            .or_insert_with(|| parts[2].to_string());
    }
    fields
}

fn encode_baggage(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| format!("{key}={}", percent_encode(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_baggage(header: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for member in header.split(',') {
        let member = member.split(';').next().unwrap_or("").trim();
        let Some((key, value)) = member.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if !key.starts_with("raindrop-") {
            continue;
        }
        if let Some(value) = percent_decode(value.trim()) {
            if !value.is_empty() {
                out.insert(key.to_string(), value);
            }
        }
    }
    out
}

fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = value.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The span's W3C-shaped hex ids, when it has real ones.
fn hex_ids(span: &Span) -> Option<(String, String)> {
    let ids = span.ids()?;
    let engine = base64::engine::general_purpose::STANDARD;
    let trace = engine.decode(ids.trace_id_b64).ok()?;
    let span = engine.decode(ids.span_id_b64).ok()?;
    if trace.len() != 16 || span.len() != 8 {
        return None;
    }
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    Some((hex(&trace), hex(&span)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baggage_round_trips_escaped_values() {
        let encoded = encode_baggage(&[
            (CARRIER_EVENT_ID, "evt 1,2;3=4%"),
            (CARRIER_NAME, "researcher"),
            (CARRIER_USER_ID, ""),
        ]);
        assert!(!encoded.contains(CARRIER_USER_ID));
        let decoded = decode_baggage(&encoded);
        assert_eq!(decoded[CARRIER_EVENT_ID], "evt 1,2;3=4%");
        assert_eq!(decoded[CARRIER_NAME], "researcher");
    }

    #[test]
    fn carrier_falls_back_to_baggage_and_traceparent() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "Baggage".to_string(),
            "other=1,raindrop-event-id=evt-a;prop=x,raindrop-child-event-id=evt-b".to_string(),
        );
        headers.insert(
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        );
        let fields = parse_carrier(&headers);
        assert_eq!(fields[CARRIER_EVENT_ID], "evt-a");
        assert_eq!(fields[CARRIER_CHILD_EVENT_ID], "evt-b");
        assert_eq!(fields[CARRIER_DISPATCH_SPAN_ID], "00f067aa0ba902b7");
        assert!(!fields.contains_key("other"));
    }

    #[test]
    fn unreadable_carrier_yields_no_fields() {
        let mut headers = BTreeMap::new();
        headers.insert(HANDOFF_HEADER.to_string(), "%%%,=,garbage".to_string());
        assert!(parse_carrier(&headers).is_empty());
    }
}
