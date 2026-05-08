//! Contract v1 — typed wire contract between the Rust SDK and Workshop.
//!
//! Mirror of `@raindrop-ai/core/contract/v1`. Source of truth for:
//!
//!   - `/v1/live` body shape ([`live::LiveEvent`])
//!   - `/v1/events/track_partial` body shape ([`track::TrackEvent`])
//!   - `/v1/events/track` body shape ([`track::TrackBody`])
//!   - the `raindrop.*` OTLP attribute namespace ([`attrs::attr_keys`] +
//!     [`attrs::build_raindrop_attrs`])
//!   - workspace identity stamping ([`workspace::read_workspace_metadata_from_env`])
//!   - replay echo ([`replay::read_replay_run_id_from_attrs`])
//!   - workshop URL resolution ([`workshop_detection::resolve_workshop_url`])
//!
//! ## Versioning
//!
//! - Wire version: `"1"`. SDKs MAY send an `X-Raindrop-Contract-Version: 1`
//!   request header on every `/v1/*` request. Workshop reads it for drift
//!   telemetry. Missing → assume 1. Unknown future values → accept with
//!   passthrough.
//! - Within v1, schema changes are additive only. Breaking changes ship under
//!   `/v2/*` routes and a v2 contract module.

pub mod attrs;
pub mod live;
pub mod replay;
pub mod track;
pub mod workshop_detection;
pub mod workspace;

/// Wire version emitted on the `X-Raindrop-Contract-Version` header.
pub const WIRE_VERSION: &str = "1";

/// HTTP header name carrying the wire version.
pub const WIRE_VERSION_HEADER: &str = "X-Raindrop-Contract-Version";

pub use attrs::{
    ai_sdk_metadata, attr_keys, build_raindrop_attrs, build_raindrop_canonical_attrs,
    read_raindrop_attrs, traceloop_props, RaindropMeta, RaindropSpanKind,
};
pub use live::{validate_live_event, LiveEvent, LiveEventType, LiveEventValidationError};
pub use replay::read_replay_run_id_from_attrs;
pub use track::{TrackAiData, TrackAttachment, TrackBody, TrackEvent, TrackProperties};
pub use workshop_detection::{
    normalize_workshop_base_url, resolve_workshop_url, sanitize_workshop_url_for_log,
    WorkshopUrlOptions, DEFAULT_WORKSHOP_URL, LOCAL_DEBUGGER_ENV_VAR, WORKSHOP_ENV_VAR,
};
pub use workspace::{
    read_workspace_metadata_from_env, LocalWorkspaceMetadata, WORKSPACE_ID_ENV_VAR,
    WORKSPACE_NAME_ENV_VAR, WORKSPACE_ROOT_ENV_VAR,
};
