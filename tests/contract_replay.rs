//! Tests for `contract::v1::replay::read_replay_run_id_from_attrs`.

use std::collections::HashMap;

use raindrop::contract::v1::attrs::{ai_sdk_metadata, attr_keys, traceloop_props};
use raindrop::contract::v1::replay::read_replay_run_id_from_attrs;

#[test]
fn returns_none_when_no_replay_attrs_present() {
    let attrs = HashMap::new();
    assert_eq!(read_replay_run_id_from_attrs(&attrs), None);
}

#[test]
fn prefers_canonical_attr_over_upstream_namespaces() {
    let mut attrs = HashMap::new();
    attrs.insert(attr_keys::REPLAY_RUN_ID.into(), "canonical".into());
    attrs.insert(ai_sdk_metadata::REPLAY_RUN_ID.into(), "ai_sdk".into());
    attrs.insert(traceloop_props::REPLAY_RUN_ID.into(), "traceloop".into());
    assert_eq!(
        read_replay_run_id_from_attrs(&attrs).as_deref(),
        Some("canonical")
    );
}

#[test]
fn falls_back_to_ai_sdk_then_traceloop_then_properties_blob() {
    let mut attrs = HashMap::new();
    attrs.insert(ai_sdk_metadata::REPLAY_RUN_ID.into(), "ai_sdk".into());
    attrs.insert(traceloop_props::REPLAY_RUN_ID.into(), "traceloop".into());
    assert_eq!(
        read_replay_run_id_from_attrs(&attrs).as_deref(),
        Some("ai_sdk")
    );

    attrs.remove(ai_sdk_metadata::REPLAY_RUN_ID);
    assert_eq!(
        read_replay_run_id_from_attrs(&attrs).as_deref(),
        Some("traceloop")
    );

    attrs.remove(traceloop_props::REPLAY_RUN_ID);
    attrs.insert(
        ai_sdk_metadata::PROPERTIES.into(),
        r#"{"replayRunId":"from_blob"}"#.into(),
    );
    assert_eq!(
        read_replay_run_id_from_attrs(&attrs).as_deref(),
        Some("from_blob")
    );
}

#[test]
fn properties_blob_with_invalid_json_is_ignored() {
    let mut attrs = HashMap::new();
    attrs.insert(ai_sdk_metadata::PROPERTIES.into(), "not json".into());
    assert_eq!(read_replay_run_id_from_attrs(&attrs), None);
}

#[test]
fn properties_blob_without_replay_run_id_field_is_ignored() {
    let mut attrs = HashMap::new();
    attrs.insert(
        ai_sdk_metadata::PROPERTIES.into(),
        r#"{"otherField":"x"}"#.into(),
    );
    assert_eq!(read_replay_run_id_from_attrs(&attrs), None);
}

#[test]
fn properties_blob_with_empty_replay_run_id_is_ignored() {
    let mut attrs = HashMap::new();
    attrs.insert(
        ai_sdk_metadata::PROPERTIES.into(),
        r#"{"replayRunId":""}"#.into(),
    );
    assert_eq!(read_replay_run_id_from_attrs(&attrs), None);
}

#[test]
fn empty_string_attribute_value_is_treated_as_missing() {
    let mut attrs = HashMap::new();
    attrs.insert(attr_keys::REPLAY_RUN_ID.into(), "".into());
    attrs.insert(ai_sdk_metadata::REPLAY_RUN_ID.into(), "ai_sdk".into());
    assert_eq!(
        read_replay_run_id_from_attrs(&attrs).as_deref(),
        Some("ai_sdk"),
        "empty canonical attribute should fall through to the upstream namespace"
    );
}
