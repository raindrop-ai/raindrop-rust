//! Workspace identity stamping.
//!
//! Mirror of `@raindrop-ai/core/contract/v1/workspace.ts`. When the host process
//! is launched by Workshop (or any orchestrator that wants to attribute Raindrop
//! telemetry to a known on-disk workspace), the orchestrator stamps three env
//! vars and the SDK reads them via [`read_workspace_metadata_from_env`].
//!
//! The result is forwarded:
//!   - on `track_partial` payloads as `properties.workspace`,
//!   - on OTLP spans as the canonical `raindrop.workspace.{id,name,root}` attrs.
//!
//! Workshop reads either path and uses it to filter the dashboard view to a
//! single project / monorepo workspace.

use serde::{Deserialize, Serialize};

/// Env var carrying the orchestrator-stable workspace id.
pub const WORKSPACE_ID_ENV_VAR: &str = "RAINDROP_WORKSPACE_ID";
/// Env var carrying a human-readable workspace name.
pub const WORKSPACE_NAME_ENV_VAR: &str = "RAINDROP_WORKSPACE_NAME";
/// Env var carrying the absolute path of the workspace root.
pub const WORKSPACE_ROOT_ENV_VAR: &str = "RAINDROP_WORKSPACE_ROOT";

/// Local workspace metadata, mirroring the TS `LocalWorkspaceMetadata` shape.
///
/// All three fields are required: workshop's parser treats partial workspace
/// metadata as if it weren't there at all (matches `WorkspaceMetadataSchema`
/// in the TS contract).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalWorkspaceMetadata {
    /// Stable workspace identifier set by the orchestrator.
    pub id: String,
    /// Human-readable workspace name (typically the directory basename).
    pub name: String,
    /// Absolute path of the workspace root on disk.
    pub root: String,
}

/// Read [`LocalWorkspaceMetadata`] from process env vars. Returns `None` unless
/// all three vars are present and non-empty (mirrors the TS reader's "all or
/// nothing" strict-mode semantics).
pub fn read_workspace_metadata_from_env() -> Option<LocalWorkspaceMetadata> {
    let id = std::env::var(WORKSPACE_ID_ENV_VAR)
        .ok()
        .filter(|v| !v.is_empty())?;
    let name = std::env::var(WORKSPACE_NAME_ENV_VAR)
        .ok()
        .filter(|v| !v.is_empty())?;
    let root = std::env::var(WORKSPACE_ROOT_ENV_VAR)
        .ok()
        .filter(|v| !v.is_empty())?;
    Some(LocalWorkspaceMetadata { id, name, root })
}
