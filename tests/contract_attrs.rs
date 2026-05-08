//! Tests for `contract::v1::attrs`.
//!
//! Locks down the attribute namespace + builder/reader behavior. Mirrors the
//! TS contract tests in `packages/core/src/contract/v1/contract.test.ts`.

use std::collections::HashMap;

use raindrop::contract::v1::attrs::{
    ai_sdk_metadata, attr_keys, build_raindrop_attrs, build_raindrop_canonical_attrs,
    read_raindrop_attrs, traceloop_props, RaindropMeta, RaindropSpanKind,
};
use raindrop::contract::v1::workspace::LocalWorkspaceMetadata;

fn key_value_pairs(attrs: &[raindrop::Attribute]) -> Vec<(&str, &str)> {
    attrs
        .iter()
        .map(|a| {
            (
                a.key.as_str(),
                a.value.string_value.as_deref().unwrap_or(""),
            )
        })
        .collect()
}

#[test]
fn canonical_attribute_keys_match_the_contract_namespace() {
    assert_eq!(attr_keys::SPAN_KIND, "raindrop.span.kind");
    assert_eq!(attr_keys::EVENT_ID, "raindrop.event.id");
    assert_eq!(attr_keys::EVENT_NAME, "raindrop.event.name");
    assert_eq!(attr_keys::USER_ID, "raindrop.user.id");
    assert_eq!(attr_keys::CONVO_ID, "raindrop.convo.id");
    assert_eq!(attr_keys::WORKSPACE_ID, "raindrop.workspace.id");
    assert_eq!(attr_keys::WORKSPACE_NAME, "raindrop.workspace.name");
    assert_eq!(attr_keys::WORKSPACE_ROOT, "raindrop.workspace.root");
    assert_eq!(attr_keys::REPLAY_RUN_ID, "raindrop.replay.run_id");
    assert_eq!(attr_keys::TOOL_NAME, "raindrop.tool.name");
}

#[test]
fn span_kind_serializes_to_canonical_wire_strings() {
    assert_eq!(RaindropSpanKind::AgentRoot.as_wire_str(), "agent_root");
    assert_eq!(RaindropSpanKind::Trace.as_wire_str(), "trace");
    assert_eq!(RaindropSpanKind::LlmCall.as_wire_str(), "llm_call");
    assert_eq!(RaindropSpanKind::ToolCall.as_wire_str(), "tool_call");
    // Traceloop kind values (upstream-owned namespace), read by dawn's
    // `mapSpanType` filter to classify the span.
    assert_eq!(RaindropSpanKind::AgentRoot.as_traceloop_str(), "workflow");
    assert_eq!(RaindropSpanKind::Trace.as_traceloop_str(), "workflow");
    assert_eq!(RaindropSpanKind::LlmCall.as_traceloop_str(), "llm");
    assert_eq!(RaindropSpanKind::ToolCall.as_traceloop_str(), "tool");
}

#[test]
fn build_raindrop_attrs_emits_canonical_alongside_upstream_namespaces() {
    let meta = RaindropMeta {
        kind: Some(RaindropSpanKind::LlmCall),
        event_id: Some("evt_1".into()),
        event_name: Some("chat".into()),
        user_id: Some("u".into()),
        convo_id: Some("c".into()),
        workspace: Some(LocalWorkspaceMetadata {
            id: "ws_1".into(),
            name: "ws".into(),
            root: "/r".into(),
        }),
        replay_run_id: Some("rep_1".into()),
        tool_name: Some("search".into()),
    };
    let attrs = build_raindrop_attrs(&meta);
    let pairs = key_value_pairs(&attrs);

    // Span kind: canonical + Traceloop mapping.
    assert!(pairs.contains(&(attr_keys::SPAN_KIND, "llm_call")));
    assert!(pairs.contains(&(traceloop_props::SPAN_KIND, "llm")));

    // event_id triple-emit: canonical + ai-sdk + traceloop.
    assert!(pairs.contains(&(attr_keys::EVENT_ID, "evt_1")));
    assert!(pairs.contains(&(ai_sdk_metadata::EVENT_ID, "evt_1")));
    assert!(pairs.contains(&(traceloop_props::EVENT_ID, "evt_1")));

    // event_name triple-emit.
    assert!(pairs.contains(&(attr_keys::EVENT_NAME, "chat")));
    assert!(pairs.contains(&(ai_sdk_metadata::EVENT_NAME, "chat")));
    assert!(pairs.contains(&(traceloop_props::EVENT_NAME, "chat")));

    // user_id, convo_id triple-emit.
    assert!(pairs.contains(&(attr_keys::USER_ID, "u")));
    assert!(pairs.contains(&(traceloop_props::USER_ID, "u")));
    assert!(pairs.contains(&(attr_keys::CONVO_ID, "c")));
    assert!(pairs.contains(&(traceloop_props::CONVO_ID, "c")));

    // workspace canonical-only (no upstream namespace mirror for workspace).
    assert!(pairs.contains(&(attr_keys::WORKSPACE_ID, "ws_1")));
    assert!(pairs.contains(&(attr_keys::WORKSPACE_NAME, "ws")));
    assert!(pairs.contains(&(attr_keys::WORKSPACE_ROOT, "/r")));

    // replay run id triple-emit.
    assert!(pairs.contains(&(attr_keys::REPLAY_RUN_ID, "rep_1")));
    assert!(pairs.contains(&(ai_sdk_metadata::REPLAY_RUN_ID, "rep_1")));
    assert!(pairs.contains(&(traceloop_props::REPLAY_RUN_ID, "rep_1")));

    // tool name canonical-only.
    assert!(pairs.contains(&(attr_keys::TOOL_NAME, "search")));
}

#[test]
fn build_raindrop_attrs_skips_missing_fields() {
    let meta = RaindropMeta {
        event_id: Some("evt_only".into()),
        ..Default::default()
    };
    let attrs = build_raindrop_attrs(&meta);
    let keys: Vec<&str> = attrs.iter().map(|a| a.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            attr_keys::EVENT_ID,
            ai_sdk_metadata::EVENT_ID,
            traceloop_props::EVENT_ID,
        ]
    );
}

#[test]
fn build_raindrop_canonical_attrs_emits_only_new_namespace() {
    let meta = RaindropMeta {
        kind: Some(RaindropSpanKind::ToolCall),
        event_id: Some("evt_1".into()),
        replay_run_id: Some("rep_1".into()),
        ..Default::default()
    };
    let attrs = build_raindrop_canonical_attrs(&meta);
    let keys: Vec<&str> = attrs.iter().map(|a| a.key.as_str()).collect();
    for k in &keys {
        assert!(
            k.starts_with("raindrop."),
            "canonical builder must NOT emit upstream-namespace keys, got {}",
            k
        );
    }
    // Each set field maps to exactly one canonical attribute.
    assert_eq!(keys.len(), 3);
}

#[test]
fn read_raindrop_attrs_prefers_canonical_keys() {
    let mut attrs: HashMap<String, String> = HashMap::new();
    attrs.insert(attr_keys::EVENT_ID.into(), "canonical".into());
    attrs.insert(ai_sdk_metadata::EVENT_ID.into(), "ai_sdk".into());
    attrs.insert(traceloop_props::EVENT_ID.into(), "traceloop".into());
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(meta.event_id.as_deref(), Some("canonical"));
}

#[test]
fn read_raindrop_attrs_falls_back_to_ai_sdk_then_traceloop() {
    let mut attrs: HashMap<String, String> = HashMap::new();
    attrs.insert(ai_sdk_metadata::EVENT_ID.into(), "ai_sdk".into());
    attrs.insert(traceloop_props::EVENT_ID.into(), "traceloop".into());
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(
        meta.event_id.as_deref(),
        Some("ai_sdk"),
        "ai-sdk metadata namespace wins over traceloop association properties"
    );

    attrs.remove(ai_sdk_metadata::EVENT_ID);
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(meta.event_id.as_deref(), Some("traceloop"));
}

#[test]
fn read_raindrop_attrs_maps_traceloop_span_kind_when_canonical_absent() {
    let mut attrs: HashMap<String, String> = HashMap::new();
    attrs.insert(traceloop_props::SPAN_KIND.into(), "tool".into());
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(meta.kind, Some(RaindropSpanKind::ToolCall));

    attrs.insert(traceloop_props::SPAN_KIND.into(), "llm".into());
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(meta.kind, Some(RaindropSpanKind::LlmCall));

    attrs.insert(traceloop_props::SPAN_KIND.into(), "workflow".into());
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(meta.kind, Some(RaindropSpanKind::AgentRoot));
}

#[test]
fn read_raindrop_attrs_treats_partial_workspace_as_missing() {
    let mut attrs: HashMap<String, String> = HashMap::new();
    attrs.insert(attr_keys::WORKSPACE_ID.into(), "ws".into());
    attrs.insert(attr_keys::WORKSPACE_NAME.into(), "n".into());
    // root missing
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(meta.workspace, None, "partial workspace = no workspace");

    attrs.insert(attr_keys::WORKSPACE_ROOT.into(), "/r".into());
    let meta = read_raindrop_attrs(&attrs);
    assert_eq!(
        meta.workspace,
        Some(LocalWorkspaceMetadata {
            id: "ws".into(),
            name: "n".into(),
            root: "/r".into(),
        })
    );
}
