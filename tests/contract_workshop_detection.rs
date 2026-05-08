//! Tests for `contract::v1::workshop_detection::resolve_workshop_url`.
//!
//! Mirror of the TS contract tests in `packages/core/src/contract/v1/contract.test.ts`.
//! Serializes on a process-local mutex because the resolver reads global env
//! vars and cargo runs tests in parallel within a single test binary.

use raindrop::contract::v1::workshop_detection::{
    normalize_workshop_base_url, resolve_workshop_url, WorkshopUrlOptions, DEFAULT_WORKSHOP_URL,
    LOCAL_DEBUGGER_ENV_VAR, WORKSHOP_ENV_VAR,
};

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn clear_env() {
    std::env::remove_var(LOCAL_DEBUGGER_ENV_VAR);
    std::env::remove_var(WORKSHOP_ENV_VAR);
    std::env::remove_var("NODE_ENV");
    std::env::remove_var("PYTHON_ENV");
}

#[test]
fn override_url_takes_precedence_over_everything() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    // Even with conflicting env vars, the explicit override wins.
    std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://from-env:9999/");
    std::env::set_var(WORKSHOP_ENV_VAR, "false");
    let resolved = resolve_workshop_url(WorkshopUrlOptions {
        override_workshop_url: Some("http://override:1234".into()),
        enable_workshop: Some(false),
    });
    assert_eq!(resolved.as_deref(), Some("http://override:1234/"));
    clear_env();
}

#[test]
fn enable_workshop_false_disables_even_with_env_var_set() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://localhost:5899/v1/");
    let resolved = resolve_workshop_url(WorkshopUrlOptions {
        enable_workshop: Some(false),
        ..Default::default()
    });
    assert_eq!(
        resolved, None,
        "enable_workshop:false MUST hard-disable even with env URL set"
    );
    clear_env();
}

#[test]
fn enable_workshop_true_uses_env_url_when_set() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://localhost:7777/v1");
    let resolved = resolve_workshop_url(WorkshopUrlOptions {
        enable_workshop: Some(true),
        ..Default::default()
    });
    assert_eq!(resolved.as_deref(), Some("http://localhost:7777/v1/"));
    clear_env();
}

#[test]
fn enable_workshop_true_falls_back_to_default_url_when_env_unset() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    let resolved = resolve_workshop_url(WorkshopUrlOptions {
        enable_workshop: Some(true),
        ..Default::default()
    });
    assert_eq!(resolved.as_deref(), Some(DEFAULT_WORKSHOP_URL));
    clear_env();
}

#[test]
fn local_debugger_env_var_is_normalized_with_trailing_slash() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://localhost:6000/v1");
    let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
    assert_eq!(resolved.as_deref(), Some("http://localhost:6000/v1/"));
    clear_env();
}

#[test]
fn workshop_env_var_accepts_url_form() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var(WORKSHOP_ENV_VAR, "http://workshop.local:8080");
    let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
    assert_eq!(resolved.as_deref(), Some("http://workshop.local:8080/"));
    clear_env();
}

#[test]
fn workshop_env_var_accepts_truthy_strings() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for truthy in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] {
        clear_env();
        std::env::set_var(WORKSHOP_ENV_VAR, truthy);
        let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
        assert_eq!(
            resolved.as_deref(),
            Some(DEFAULT_WORKSHOP_URL),
            "{} should enable workshop",
            truthy
        );
    }
    clear_env();
}

#[test]
fn workshop_env_var_accepts_falsy_strings_and_disables() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for falsy in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] {
        clear_env();
        std::env::set_var(WORKSHOP_ENV_VAR, falsy);
        // Even with auto-detect signals (NODE_ENV=development), explicit
        // RAINDROP_WORKSHOP=false must win.
        std::env::set_var("NODE_ENV", "development");
        let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
        assert_eq!(
            resolved, None,
            "{} should disable workshop even with NODE_ENV=development",
            falsy
        );
    }
    clear_env();
}

#[test]
fn local_debugger_env_var_takes_precedence_over_workshop_env_var() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://from-debugger:1111/");
    std::env::set_var(WORKSHOP_ENV_VAR, "http://from-workshop:2222/");
    let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
    assert_eq!(
        resolved.as_deref(),
        Some("http://from-debugger:1111/"),
        "RAINDROP_LOCAL_DEBUGGER MUST win over RAINDROP_WORKSHOP"
    );
    clear_env();
}

#[test]
fn auto_enable_via_node_env_development() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var("NODE_ENV", "development");
    let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
    assert_eq!(resolved.as_deref(), Some(DEFAULT_WORKSHOP_URL));
    clear_env();
}

#[test]
fn auto_enable_via_python_env_development() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    std::env::set_var("PYTHON_ENV", "development");
    let resolved = resolve_workshop_url(WorkshopUrlOptions::default());
    assert_eq!(resolved.as_deref(), Some(DEFAULT_WORKSHOP_URL));
    clear_env();
}

#[test]
fn normalize_workshop_base_url_appends_slash() {
    assert_eq!(
        normalize_workshop_base_url("http://x:1"),
        "http://x:1/",
        "missing trailing slash must be appended"
    );
    assert_eq!(
        normalize_workshop_base_url("http://x:1/v1/"),
        "http://x:1/v1/",
        "existing trailing slash must be preserved"
    );
}
