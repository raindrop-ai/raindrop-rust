//! Canonical Raindrop OTLP attribute namespace.
//!
//! Mirror of `@raindrop-ai/core/contract/v1/attrs.ts`. The new `raindrop.*`
//! keys are emitted alongside the historical `ai.telemetry.metadata.raindrop.*`
//! and `traceloop.association.properties.*` keys (additive, never replacing).
//!
//! Workshop's parser reads in this order:
//!   1. `raindrop.*` (new, preferred)
//!   2. `ai.telemetry.metadata.raindrop.*` (existing)
//!   3. `traceloop.association.properties.*` (existing)
//!   4. heuristics for non-Raindrop SDKs
//!
//! Production backend (dawn) currently reads (2) and (3) — see
//! `dawn/apps/dawn/lib/traces/parseSpan.ts` `hasAIOperation` filter at L543.
//! We never stop emitting those. Dropping any of them silently deletes traces
//! in production until the backend kernel migrates, which is post-launch work.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::workspace::LocalWorkspaceMetadata;
use crate::otlp::Attribute;

/// Canonical Raindrop span kinds (mirror of TS `RaindropSpanKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaindropSpanKind {
    /// Outermost agent invocation span.
    AgentRoot,
    /// Generic trace span (top-level workflow, sub-agent, etc).
    Trace,
    /// Single LLM generation call.
    LlmCall,
    /// Single tool invocation.
    ToolCall,
}

impl RaindropSpanKind {
    /// Wire string emitted under `raindrop.span.kind`. Matches `RAINDROP_SPAN_KINDS`
    /// in the TS contract.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            RaindropSpanKind::AgentRoot => "agent_root",
            RaindropSpanKind::Trace => "trace",
            RaindropSpanKind::LlmCall => "llm_call",
            RaindropSpanKind::ToolCall => "tool_call",
        }
    }

    /// Traceloop `traceloop.span.kind` value, emitted alongside the canonical
    /// kind. The `dawn` ingestion kernel's `mapSpanType` filter reads this
    /// upstream-owned attribute to classify the span.
    pub fn as_traceloop_str(self) -> &'static str {
        match self {
            RaindropSpanKind::AgentRoot => "workflow",
            RaindropSpanKind::Trace => "workflow",
            RaindropSpanKind::LlmCall => "llm",
            RaindropSpanKind::ToolCall => "tool",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "agent_root" => Some(RaindropSpanKind::AgentRoot),
            "trace" => Some(RaindropSpanKind::Trace),
            "llm_call" => Some(RaindropSpanKind::LlmCall),
            "tool_call" => Some(RaindropSpanKind::ToolCall),
            _ => None,
        }
    }

    /// Parse a Traceloop span kind. The Traceloop namespace only has 3 kinds
    /// (`workflow` / `llm` / `tool`), so this mapping is information-lossy on
    /// the [`Trace`](Self::Trace) leg: both [`AgentRoot`](Self::AgentRoot) and
    /// [`Trace`](Self::Trace) emit `"workflow"` via
    /// [`as_traceloop_str`](Self::as_traceloop_str), and this reader collapses
    /// `"workflow"` to [`AgentRoot`](Self::AgentRoot). Callers who need to
    /// preserve the [`AgentRoot`](Self::AgentRoot)/[`Trace`](Self::Trace)
    /// distinction must read the canonical
    /// [`SPAN_KIND`](super::attr_keys::SPAN_KIND) attribute, which IS distinct.
    fn from_traceloop(s: &str) -> Option<Self> {
        match s {
            "llm" => Some(RaindropSpanKind::LlmCall),
            "tool" => Some(RaindropSpanKind::ToolCall),
            "workflow" => Some(RaindropSpanKind::AgentRoot),
            _ => None,
        }
    }
}

/// Canonical Raindrop metadata attached to first-party spans.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RaindropMeta {
    /// Span classification.
    pub kind: Option<RaindropSpanKind>,
    /// SDK interaction id (correlation key for partial events).
    pub event_id: Option<String>,
    /// Human-readable event name.
    pub event_name: Option<String>,
    /// User the span belongs to.
    pub user_id: Option<String>,
    /// Conversation/thread the span belongs to.
    pub convo_id: Option<String>,
    /// Workspace identity.
    pub workspace: Option<LocalWorkspaceMetadata>,
    /// Replay echo: id of the workshop replay attempt that originated this span.
    pub replay_run_id: Option<String>,
    /// Tool name, set on `tool_call` spans.
    pub tool_name: Option<String>,
}

/// Canonical `raindrop.*` attribute keys. Single source of truth for the
/// new namespace.
pub mod attr_keys {
    /// Span classification (`agent_root` | `trace` | `llm_call` | `tool_call`).
    pub const SPAN_KIND: &str = "raindrop.span.kind";
    /// SDK interaction id.
    pub const EVENT_ID: &str = "raindrop.event.id";
    /// Human-readable event name.
    pub const EVENT_NAME: &str = "raindrop.event.name";
    /// User the span belongs to.
    pub const USER_ID: &str = "raindrop.user.id";
    /// Conversation id.
    pub const CONVO_ID: &str = "raindrop.convo.id";
    /// Workspace id.
    pub const WORKSPACE_ID: &str = "raindrop.workspace.id";
    /// Workspace name.
    pub const WORKSPACE_NAME: &str = "raindrop.workspace.name";
    /// Workspace root directory on disk.
    pub const WORKSPACE_ROOT: &str = "raindrop.workspace.root";
    /// Replay echo id.
    pub const REPLAY_RUN_ID: &str = "raindrop.replay.run_id";
    /// Tool name.
    pub const TOOL_NAME: &str = "raindrop.tool.name";
    /// Whether the span payload was truncated to fit the ingestion cap.
    pub const PAYLOAD_TRUNCATED: &str = "raindrop.payload.truncated";
    /// Original (pre-truncation) payload size in bytes.
    pub const PAYLOAD_ORIGINAL_BYTES: &str = "raindrop.payload.original_bytes";
}

/// Vercel AI SDK metadata namespace (`ai.telemetry.metadata.raindrop.*`).
/// Upstream-owned convention; the `dawn` ingestion kernel's `hasAIOperation`
/// filter reads these keys, so they are emitted on every Raindrop span
/// alongside the canonical `raindrop.*` namespace.
pub mod ai_sdk_metadata {
    pub const EVENT_ID: &str = "ai.telemetry.metadata.raindrop.eventId";
    pub const EVENT_NAME: &str = "ai.telemetry.metadata.raindrop.eventName";
    pub const USER_ID: &str = "ai.telemetry.metadata.raindrop.userId";
    pub const USER_ID_AI: &str = "ai.telemetry.metadata.raindrop.ai.userId";
    pub const CONVO_ID: &str = "ai.telemetry.metadata.raindrop.convoId";
    pub const REPLAY_RUN_ID: &str = "ai.telemetry.metadata.raindrop.replayRunId";
    pub const PROPERTIES: &str = "ai.telemetry.metadata.raindrop.properties";
}

/// Traceloop OpenLLMetry attribute namespace (`traceloop.association.properties.*`).
/// Upstream-owned convention; the `dawn` ingestion kernel reads these in
/// addition to the canonical `raindrop.*` namespace.
pub mod traceloop_props {
    pub const SPAN_KIND: &str = "traceloop.span.kind";
    pub const EVENT_ID: &str = "traceloop.association.properties.event_id";
    pub const EVENT_NAME: &str = "traceloop.association.properties.event_name";
    pub const USER_ID: &str = "traceloop.association.properties.user_id";
    pub const CONVO_ID: &str = "traceloop.association.properties.convo_id";
    pub const REPLAY_RUN_ID: &str = "traceloop.association.properties.replayRunId";
}

/// Build the OTLP attribute set for a Raindrop-instrumented span, emitting
/// the canonical `raindrop.*` keys alongside the upstream-owned namespaces
/// (Vercel AI SDK metadata + Traceloop association props).
///
/// Mirrors `buildRaindropAttrs` in the TS contract. SDK call sites pass the
/// resulting vec to whatever attribute builder they use (e.g. extending an
/// existing `Vec<Attribute>` before `.end()`).
pub fn build_raindrop_attrs(meta: &RaindropMeta) -> Vec<Attribute> {
    let mut out: Vec<Attribute> = Vec::new();

    if let Some(kind) = meta.kind {
        out.push(Attribute::string(attr_keys::SPAN_KIND, kind.as_wire_str()));
        out.push(Attribute::string(
            traceloop_props::SPAN_KIND,
            kind.as_traceloop_str(),
        ));
    }
    if let Some(event_id) = meta.event_id.as_deref() {
        out.push(Attribute::string(attr_keys::EVENT_ID, event_id));
        out.push(Attribute::string(ai_sdk_metadata::EVENT_ID, event_id));
        out.push(Attribute::string(traceloop_props::EVENT_ID, event_id));
    }
    if let Some(event_name) = meta.event_name.as_deref() {
        out.push(Attribute::string(attr_keys::EVENT_NAME, event_name));
        out.push(Attribute::string(ai_sdk_metadata::EVENT_NAME, event_name));
        out.push(Attribute::string(traceloop_props::EVENT_NAME, event_name));
    }
    if let Some(user_id) = meta.user_id.as_deref() {
        out.push(Attribute::string(attr_keys::USER_ID, user_id));
        out.push(Attribute::string(ai_sdk_metadata::USER_ID, user_id));
        out.push(Attribute::string(traceloop_props::USER_ID, user_id));
    }
    if let Some(convo_id) = meta.convo_id.as_deref() {
        out.push(Attribute::string(attr_keys::CONVO_ID, convo_id));
        out.push(Attribute::string(ai_sdk_metadata::CONVO_ID, convo_id));
        out.push(Attribute::string(traceloop_props::CONVO_ID, convo_id));
    }
    if let Some(ws) = meta.workspace.as_ref() {
        out.push(Attribute::string(attr_keys::WORKSPACE_ID, &ws.id));
        out.push(Attribute::string(attr_keys::WORKSPACE_NAME, &ws.name));
        out.push(Attribute::string(attr_keys::WORKSPACE_ROOT, &ws.root));
    }
    if let Some(replay_run_id) = meta.replay_run_id.as_deref() {
        out.push(Attribute::string(attr_keys::REPLAY_RUN_ID, replay_run_id));
        out.push(Attribute::string(
            ai_sdk_metadata::REPLAY_RUN_ID,
            replay_run_id,
        ));
        out.push(Attribute::string(
            traceloop_props::REPLAY_RUN_ID,
            replay_run_id,
        ));
    }
    if let Some(tool_name) = meta.tool_name.as_deref() {
        out.push(Attribute::string(attr_keys::TOOL_NAME, tool_name));
    }
    out
}

/// Build the **canonical-only** subset of attributes (just the `raindrop.*`
/// keys, none of the upstream-owned namespaces). Used by call sites that
/// already emit the AI-SDK and Traceloop attribute namespaces via a
/// separate code path and only need the canonical layer bolted on.
pub fn build_raindrop_canonical_attrs(meta: &RaindropMeta) -> Vec<Attribute> {
    let mut out: Vec<Attribute> = Vec::new();
    if let Some(kind) = meta.kind {
        out.push(Attribute::string(attr_keys::SPAN_KIND, kind.as_wire_str()));
    }
    if let Some(event_id) = meta.event_id.as_deref() {
        out.push(Attribute::string(attr_keys::EVENT_ID, event_id));
    }
    if let Some(event_name) = meta.event_name.as_deref() {
        out.push(Attribute::string(attr_keys::EVENT_NAME, event_name));
    }
    if let Some(user_id) = meta.user_id.as_deref() {
        out.push(Attribute::string(attr_keys::USER_ID, user_id));
    }
    if let Some(convo_id) = meta.convo_id.as_deref() {
        out.push(Attribute::string(attr_keys::CONVO_ID, convo_id));
    }
    if let Some(ws) = meta.workspace.as_ref() {
        out.push(Attribute::string(attr_keys::WORKSPACE_ID, &ws.id));
        out.push(Attribute::string(attr_keys::WORKSPACE_NAME, &ws.name));
        out.push(Attribute::string(attr_keys::WORKSPACE_ROOT, &ws.root));
    }
    if let Some(replay_run_id) = meta.replay_run_id.as_deref() {
        out.push(Attribute::string(attr_keys::REPLAY_RUN_ID, replay_run_id));
    }
    if let Some(tool_name) = meta.tool_name.as_deref() {
        out.push(Attribute::string(attr_keys::TOOL_NAME, tool_name));
    }
    out
}

/// Read [`RaindropMeta`] from a flat OTLP-style attribute map, with the same
/// canonical-then-upstream-namespace fallback ladder as the TS reader
/// (`raindrop.*` first, then AI SDK metadata, then Traceloop association
/// properties).
pub fn read_raindrop_attrs(attrs: &HashMap<String, String>) -> RaindropMeta {
    let get = |k: &str| -> Option<String> {
        attrs
            .get(k)
            .and_then(|v| if v.is_empty() { None } else { Some(v.clone()) })
    };

    let kind = get(attr_keys::SPAN_KIND)
        .as_deref()
        .and_then(RaindropSpanKind::parse)
        .or_else(|| {
            get(traceloop_props::SPAN_KIND)
                .as_deref()
                .and_then(RaindropSpanKind::from_traceloop)
        });

    let event_id = get(attr_keys::EVENT_ID)
        .or_else(|| get(ai_sdk_metadata::EVENT_ID))
        .or_else(|| get(traceloop_props::EVENT_ID));
    let event_name = get(attr_keys::EVENT_NAME)
        .or_else(|| get(ai_sdk_metadata::EVENT_NAME))
        .or_else(|| get(traceloop_props::EVENT_NAME));
    let user_id = get(attr_keys::USER_ID)
        .or_else(|| get(ai_sdk_metadata::USER_ID))
        .or_else(|| get(ai_sdk_metadata::USER_ID_AI))
        .or_else(|| get(traceloop_props::USER_ID));
    let convo_id = get(attr_keys::CONVO_ID)
        .or_else(|| get(ai_sdk_metadata::CONVO_ID))
        .or_else(|| get(traceloop_props::CONVO_ID));
    let replay_run_id = get(attr_keys::REPLAY_RUN_ID)
        .or_else(|| get(ai_sdk_metadata::REPLAY_RUN_ID))
        .or_else(|| get(traceloop_props::REPLAY_RUN_ID));

    let ws_id = get(attr_keys::WORKSPACE_ID);
    let ws_name = get(attr_keys::WORKSPACE_NAME);
    let ws_root = get(attr_keys::WORKSPACE_ROOT);
    let workspace = match (ws_id, ws_name, ws_root) {
        (Some(id), Some(name), Some(root)) => Some(LocalWorkspaceMetadata { id, name, root }),
        _ => None,
    };
    let tool_name = get(attr_keys::TOOL_NAME);

    RaindropMeta {
        kind,
        event_id,
        event_name,
        user_id,
        convo_id,
        workspace,
        replay_run_id,
        tool_name,
    }
}
