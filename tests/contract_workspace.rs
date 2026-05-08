//! Tests for `contract::v1::workspace`.
//!
//! These tests serialize on a process-local mutex because env vars are
//! globally shared within a test binary; cargo runs tests in parallel by
//! default and the env-reader is observable to every parallel test.

use raindrop::contract::v1::workspace::{
    read_workspace_metadata_from_env, LocalWorkspaceMetadata, WORKSPACE_ID_ENV_VAR,
    WORKSPACE_NAME_ENV_VAR, WORKSPACE_ROOT_ENV_VAR,
};

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn clear_workspace_env() {
    std::env::remove_var(WORKSPACE_ID_ENV_VAR);
    std::env::remove_var(WORKSPACE_NAME_ENV_VAR);
    std::env::remove_var(WORKSPACE_ROOT_ENV_VAR);
}

#[test]
fn returns_none_when_no_env_vars_set() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_workspace_env();
    assert_eq!(read_workspace_metadata_from_env(), None);
}

#[test]
fn returns_none_when_only_id_is_set() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_workspace_env();
    std::env::set_var(WORKSPACE_ID_ENV_VAR, "ws_partial");
    assert_eq!(read_workspace_metadata_from_env(), None);
    clear_workspace_env();
}

#[test]
fn returns_none_when_id_or_name_or_root_is_empty_string() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_workspace_env();
    std::env::set_var(WORKSPACE_ID_ENV_VAR, "");
    std::env::set_var(WORKSPACE_NAME_ENV_VAR, "name");
    std::env::set_var(WORKSPACE_ROOT_ENV_VAR, "/r");
    assert_eq!(
        read_workspace_metadata_from_env(),
        None,
        "empty id treated as missing"
    );

    std::env::set_var(WORKSPACE_ID_ENV_VAR, "id");
    std::env::set_var(WORKSPACE_NAME_ENV_VAR, "");
    assert_eq!(
        read_workspace_metadata_from_env(),
        None,
        "empty name treated as missing"
    );

    std::env::set_var(WORKSPACE_NAME_ENV_VAR, "name");
    std::env::set_var(WORKSPACE_ROOT_ENV_VAR, "");
    assert_eq!(
        read_workspace_metadata_from_env(),
        None,
        "empty root treated as missing"
    );

    clear_workspace_env();
}

#[test]
fn returns_metadata_when_all_three_env_vars_set() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_workspace_env();
    std::env::set_var(WORKSPACE_ID_ENV_VAR, "ws_full");
    std::env::set_var(WORKSPACE_NAME_ENV_VAR, "demo workspace");
    std::env::set_var(WORKSPACE_ROOT_ENV_VAR, "/Users/me/code/demo");

    assert_eq!(
        read_workspace_metadata_from_env(),
        Some(LocalWorkspaceMetadata {
            id: "ws_full".into(),
            name: "demo workspace".into(),
            root: "/Users/me/code/demo".into(),
        })
    );
    clear_workspace_env();
}

#[test]
fn metadata_round_trips_through_serde_json() {
    let ws = LocalWorkspaceMetadata {
        id: "id".into(),
        name: "name".into(),
        root: "/r".into(),
    };
    let json = serde_json::to_value(&ws).unwrap();
    assert_eq!(json["id"], "id");
    assert_eq!(json["name"], "name");
    assert_eq!(json["root"], "/r");
    let back: LocalWorkspaceMetadata = serde_json::from_value(json).unwrap();
    assert_eq!(back, ws);
}
