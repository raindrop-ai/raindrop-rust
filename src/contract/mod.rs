//! Versioned wire contract between the Rust SDK and Workshop.
//!
//! Currently exposes [`v1`] only. Future versions will be added as sibling
//! modules; v1 schemas are additive-only within their lifetime.

pub mod v1;

pub use v1::{
    attr_keys, build_raindrop_attrs, build_raindrop_canonical_attrs, normalize_workshop_base_url,
    read_raindrop_attrs, read_replay_run_id_from_attrs, read_workspace_metadata_from_env,
    resolve_workshop_url, sanitize_workshop_url_for_log, validate_live_event, LiveEvent,
    LiveEventType, LiveEventValidationError, LocalWorkspaceMetadata, RaindropMeta,
    RaindropSpanKind, TrackAiData, TrackAttachment, TrackBody, TrackEvent, TrackProperties,
    WorkshopUrlOptions, DEFAULT_WORKSHOP_URL, LOCAL_DEBUGGER_ENV_VAR, WIRE_VERSION,
    WIRE_VERSION_HEADER, WORKSHOP_ENV_VAR, WORKSPACE_ID_ENV_VAR, WORKSPACE_NAME_ENV_VAR,
    WORKSPACE_ROOT_ENV_VAR,
};
