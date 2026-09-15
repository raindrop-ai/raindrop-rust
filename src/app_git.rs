use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::io::AsyncReadExt;

use crate::otlp::Attribute;

pub(crate) const COMMIT_SHA_PROPERTY: &str = "raindrop.app.commit_sha";
pub(crate) const COMMIT_DIRTY_PROPERTY: &str = "raindrop.app.commit_dirty";
pub(crate) const BRANCH_PROPERTY: &str = "raindrop.app.branch";

const DISCOVERY_TIMEOUT: Duration = Duration::from_millis(750);
const MAX_GIT_OUTPUT_BYTES: usize = 4096;
const PROCESS_CLEANUP_TIMEOUT: Duration = Duration::from_millis(50);

// These variables can redirect `git -C <application>` to a different
// repository, change its object/index view, stop normal repository discovery,
// or inject config such as `core.worktree`. They are removed only from the Git
// child command; the customer's process environment is never mutated.
const GIT_IDENTITY_ENV_VARS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_NAMESPACE",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    "GIT_PREFIX",
    "GIT_IMPLICIT_WORK_TREE",
    "GIT_SHALLOW_FILE",
    "GIT_GRAFT_FILE",
    "GIT_REPLACE_REF_BASE",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_QUARANTINE_PATH",
    "GIT_CONFIG",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_SYSTEM",
    "GIT_CONFIG_NOSYSTEM",
];

pub(crate) fn is_canonical_property(key: &str) -> bool {
    matches!(
        key,
        COMMIT_SHA_PROPERTY | COMMIT_DIRTY_PROPERTY | BRANCH_PROPERTY
    )
}

pub(crate) fn canonical_properties(
    properties: &BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    properties
        .iter()
        .filter(|(key, _)| is_canonical_property(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Application Git metadata configuration.
///
/// This identifies the instrumented application, not this SDK crate. Explicit
/// values are used exactly as supplied and take precedence over Raindrop
/// environment variables and automatic discovery.
#[derive(Debug, Default, Clone)]
pub struct AppGitConfig {
    pub(crate) commit_sha: Option<String>,
    pub(crate) commit_dirty: Option<bool>,
    pub(crate) branch: Option<String>,
    pub(crate) source_directory: Option<PathBuf>,
    pub(crate) detect_branch: Option<bool>,
    pub(crate) auto_detect: Option<bool>,
}

impl AppGitConfig {
    /// Create an application Git configuration with automatic detection enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the application's commit SHA explicitly.
    pub fn commit_sha(mut self, commit_sha: impl Into<String>) -> Self {
        self.commit_sha = Some(commit_sha.into());
        self
    }

    /// Set whether the application's checkout is dirty explicitly.
    pub fn commit_dirty(mut self, commit_dirty: bool) -> Self {
        self.commit_dirty = Some(commit_dirty);
        self
    }

    /// Set the application's branch explicitly.
    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Select the application source directory used for local Git discovery.
    pub fn source_directory(mut self, source_directory: impl Into<PathBuf>) -> Self {
        self.source_directory = Some(source_directory.into());
        self
    }

    /// Opt in or out of automatic branch detection. Defaults to `false`.
    pub fn detect_branch(mut self, detect_branch: bool) -> Self {
        self.detect_branch = Some(detect_branch);
        self
    }

    /// Enable or disable automatic build/deploy, local Git, and contextual CI detection.
    /// Explicit config and `RAINDROP_COMMIT_*` / `RAINDROP_BRANCH` values are unaffected.
    pub fn auto_detect(mut self, auto_detect: bool) -> Self {
        self.auto_detect = Some(auto_detect);
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct AppGitSnapshot {
    pub(crate) commit_sha: Option<String>,
    pub(crate) commit_dirty: Option<bool>,
    pub(crate) branch: Option<String>,
    pub(crate) commit_dirty_inferred: bool,
    pub(crate) branch_inferred: bool,
    pub(crate) raw_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct InferredAttributeFlags {
    pub(crate) commit_dirty: bool,
    pub(crate) branch: bool,
}

impl AppGitSnapshot {
    pub(crate) fn with_canonical_properties(
        mut self,
        properties: &BTreeMap<String, Value>,
    ) -> Self {
        let owns_sha = properties.contains_key(COMMIT_SHA_PROPERTY);
        if owns_sha {
            if self.commit_dirty_inferred
                && !self.raw_properties.contains_key(COMMIT_DIRTY_PROPERTY)
            {
                self.commit_dirty = None;
            }
            if self.branch_inferred && !self.raw_properties.contains_key(BRANCH_PROPERTY) {
                self.branch = None;
            }
        }
        for (key, value) in canonical_properties(properties) {
            self.raw_properties.insert(key, value);
        }
        self
    }

    pub(crate) fn enrich_properties(&self, properties: &mut BTreeMap<String, Value>) {
        for (key, value) in &self.raw_properties {
            properties
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
        // A caller-selected SHA owns its context. Never combine automatic
        // dirty/branch values from the client's different SHA source with it.
        let owns_sha = properties.contains_key(COMMIT_SHA_PROPERTY);
        if let Some(value) = &self.commit_sha {
            if !owns_sha {
                properties
                    .entry(COMMIT_SHA_PROPERTY.to_string())
                    .or_insert_with(|| Value::String(value.clone()));
            }
        }
        if let Some(value) = self
            .commit_dirty
            .filter(|_| !owns_sha || !self.commit_dirty_inferred)
        {
            properties
                .entry(COMMIT_DIRTY_PROPERTY.to_string())
                .or_insert(Value::Bool(value));
        }
        if let Some(value) = self
            .branch
            .as_ref()
            .filter(|_| !owns_sha || !self.branch_inferred)
        {
            properties
                .entry(BRANCH_PROPERTY.to_string())
                .or_insert_with(|| Value::String(value.clone()));
        }
    }

    pub(crate) fn enrich_attributes(
        &self,
        properties: &BTreeMap<String, Value>,
        attributes: &mut Vec<Attribute>,
    ) -> InferredAttributeFlags {
        let mut inferred = InferredAttributeFlags::default();
        let has_commit_sha = properties.contains_key(COMMIT_SHA_PROPERTY)
            || self.raw_properties.contains_key(COMMIT_SHA_PROPERTY)
            || attributes
                .iter()
                .any(|attribute| attribute.key == COMMIT_SHA_PROPERTY);
        let has_commit_dirty = properties.contains_key(COMMIT_DIRTY_PROPERTY)
            || self.raw_properties.contains_key(COMMIT_DIRTY_PROPERTY)
            || attributes
                .iter()
                .any(|attribute| attribute.key == COMMIT_DIRTY_PROPERTY);
        let has_branch = properties.contains_key(BRANCH_PROPERTY)
            || self.raw_properties.contains_key(BRANCH_PROPERTY)
            || attributes
                .iter()
                .any(|attribute| attribute.key == BRANCH_PROPERTY);
        for (key, value) in &self.raw_properties {
            if properties.contains_key(key)
                || attributes.iter().any(|attribute| attribute.key == *key)
            {
                continue;
            }
            if let Some(attribute) = canonical_attribute(key, value) {
                attributes.push(attribute);
            }
        }
        if !has_commit_sha {
            if let Some(value) = &self.commit_sha {
                attributes.push(Attribute::string(COMMIT_SHA_PROPERTY, value));
            }
        }
        if !has_commit_dirty && (!has_commit_sha || !self.commit_dirty_inferred) {
            if let Some(value) = self.commit_dirty {
                attributes.push(Attribute::bool(COMMIT_DIRTY_PROPERTY, value));
                inferred.commit_dirty = self.commit_dirty_inferred;
            }
        }
        if !has_branch && (!has_commit_sha || !self.branch_inferred) {
            if let Some(value) = &self.branch {
                attributes.push(Attribute::string(BRANCH_PROPERTY, value));
                inferred.branch = self.branch_inferred;
            }
        }
        inferred
    }
}

fn canonical_attribute(key: &str, value: &Value) -> Option<Attribute> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(Attribute::string(key, value)),
        Value::Bool(value) => Some(Attribute::bool(key, *value)),
        Value::Number(value) => value
            .as_i64()
            .map(|integer| Attribute::int(key, integer))
            .or_else(|| value.as_f64().map(|float| Attribute::float(key, float))),
        Value::Array(values) if values.iter().all(|value| matches!(value, Value::String(_))) => {
            Some(Attribute::string_array(
                key,
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(ToString::to_string))
                    .collect(),
            ))
        }
        _ => Some(Attribute::from_json(key, value)),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AppGitOperationContext {
    inner: Arc<Mutex<AppGitSnapshot>>,
}

impl AppGitOperationContext {
    fn new(snapshot: AppGitSnapshot) -> Self {
        Self {
            inner: Arc::new(Mutex::new(snapshot)),
        }
    }

    pub(crate) fn snapshot(&self) -> AppGitSnapshot {
        self.inner
            .lock()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }

    pub(crate) fn update(&self, properties: &BTreeMap<String, Value>) -> AppGitSnapshot {
        let Ok(mut snapshot) = self.inner.lock() else {
            return AppGitSnapshot::default().with_canonical_properties(properties);
        };
        let updated = snapshot.clone().with_canonical_properties(properties);
        *snapshot = updated.clone();
        updated
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AppGitOperationRegistry {
    inner: Arc<Mutex<AppGitOperationRegistryState>>,
    max_entries: usize,
}

#[derive(Debug, Default)]
struct AppGitOperationRegistryState {
    contexts: BTreeMap<String, AppGitOperationContext>,
    saturated: bool,
}

impl AppGitOperationRegistry {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AppGitOperationRegistryState::default())),
            max_entries: max_entries.max(1),
        }
    }

    #[cfg(test)]
    pub(crate) fn begin(
        &self,
        event_id: &str,
        snapshot: AppGitSnapshot,
        properties: &BTreeMap<String, Value>,
    ) -> AppGitSnapshot {
        self.begin_context(event_id, snapshot, properties)
            .snapshot()
    }

    pub(crate) fn begin_context(
        &self,
        event_id: &str,
        snapshot: AppGitSnapshot,
        properties: &BTreeMap<String, Value>,
    ) -> AppGitOperationContext {
        self.update_context(event_id, snapshot, properties)
    }

    pub(crate) fn update(
        &self,
        event_id: &str,
        fallback: AppGitSnapshot,
        properties: &BTreeMap<String, Value>,
    ) -> AppGitSnapshot {
        self.update_context(event_id, fallback, properties)
            .snapshot()
    }

    pub(crate) fn update_context(
        &self,
        event_id: &str,
        fallback: AppGitSnapshot,
        properties: &BTreeMap<String, Value>,
    ) -> AppGitOperationContext {
        if event_id.is_empty() {
            return AppGitOperationContext::new(fallback.with_canonical_properties(properties));
        }
        let context = {
            let Ok(mut state) = self.inner.lock() else {
                return AppGitOperationContext::new(
                    AppGitSnapshot::default().with_canonical_properties(properties),
                );
            };
            if let Some(existing) = state.contexts.get(event_id) {
                existing.clone()
            } else {
                if state.saturated || state.contexts.len() >= self.max_entries {
                    state.saturated = true;
                    return AppGitOperationContext::new(
                        AppGitSnapshot::default().with_canonical_properties(properties),
                    );
                }
                let context = AppGitOperationContext::new(fallback);
                state.contexts.insert(event_id.to_string(), context.clone());
                context
            }
        };
        context.update(properties);
        context
    }

    #[cfg(test)]
    pub(crate) fn context_for_event(
        &self,
        event_id: &str,
        fallback: AppGitSnapshot,
    ) -> AppGitSnapshot {
        self.context_handle_for_event(event_id, fallback).snapshot()
    }

    pub(crate) fn context_handle_for_event(
        &self,
        event_id: &str,
        fallback: AppGitSnapshot,
    ) -> AppGitOperationContext {
        if event_id.is_empty() {
            return AppGitOperationContext::new(fallback);
        }
        let Ok(mut state) = self.inner.lock() else {
            return AppGitOperationContext::new(AppGitSnapshot::default());
        };
        if let Some(context) = state.contexts.get(event_id) {
            return context.clone();
        }
        if state.saturated || state.contexts.len() >= self.max_entries {
            state.saturated = true;
            return AppGitOperationContext::new(AppGitSnapshot::default());
        }
        let context = AppGitOperationContext::new(fallback);
        state.contexts.insert(event_id.to_string(), context.clone());
        context
    }

    pub(crate) fn remove(&self, event_id: &str) {
        if event_id.is_empty() {
            return;
        }
        if let Ok(mut state) = self.inner.lock() {
            state.contexts.remove(event_id);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AppGitProvider {
    explicit: AppGitSnapshot,
    automatic: Arc<RwLock<Option<AppGitSnapshot>>>,
}

impl AppGitProvider {
    pub(crate) fn disabled() -> Self {
        Self {
            explicit: AppGitSnapshot::default(),
            automatic: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) fn new(config: AppGitConfig) -> Self {
        let explicit = AppGitSnapshot {
            commit_sha: config
                .commit_sha
                .or_else(|| env_value("RAINDROP_COMMIT_SHA")),
            commit_dirty: config
                .commit_dirty
                .or_else(|| bool_env("RAINDROP_COMMIT_DIRTY")),
            branch: config.branch.or_else(|| env_value("RAINDROP_BRANCH")),
            commit_dirty_inferred: false,
            branch_inferred: false,
            raw_properties: BTreeMap::new(),
        };
        let automatic = Arc::new(RwLock::new(None));

        let auto_detect = config
            .auto_detect
            .unwrap_or_else(|| bool_env("RAINDROP_GIT_AUTO_DETECT").unwrap_or(true));
        if auto_detect && explicit.commit_sha.is_none() {
            let selected_source_directory = config
                .source_directory
                .filter(|directory| !directory.as_os_str().is_empty())
                .or_else(|| nonempty_env("RAINDROP_GIT_SOURCE_DIRECTORY").map(PathBuf::from));
            let source_was_selected = selected_source_directory.is_some();
            let source_directory =
                selected_source_directory.or_else(|| std::env::current_dir().ok());
            let detect_branch = config
                .detect_branch
                .unwrap_or_else(|| bool_env("RAINDROP_GIT_DETECT_BRANCH").unwrap_or(false));
            let build_metadata = (!source_was_selected)
                .then(|| reliable_build_metadata(detect_branch))
                .flatten();
            let ci_metadata = (!source_was_selected)
                .then(|| contextual_ci_metadata(detect_branch))
                .flatten();
            if build_metadata.is_some() {
                if let Ok(mut slot) = automatic.write() {
                    *slot = build_metadata;
                }
                return Self {
                    explicit,
                    automatic,
                };
            }
            let destination = automatic.clone();
            // A standard thread works whether or not the caller has entered a Tokio runtime.
            // It is intentionally detached: telemetry and shutdown never await metadata.
            let _ = std::thread::Builder::new()
                .name("raindrop-app-git".to_string())
                .spawn(move || {
                    let discovered = source_directory
                        .as_deref()
                        .and_then(|directory| discover_local_git(directory, detect_branch));
                    let discovered = discovered.or(ci_metadata);
                    if let Ok(mut slot) = destination.write() {
                        *slot = discovered;
                    }
                });
        }

        Self {
            explicit,
            automatic,
        }
    }

    /// Return immediately with the metadata currently available. An unresolved
    /// background lookup is indistinguishable from unavailable metadata by design.
    pub(crate) fn snapshot(&self) -> AppGitSnapshot {
        let mut snapshot = self
            .automatic
            .try_read()
            .ok()
            .and_then(|value| value.clone())
            .unwrap_or_default();
        if let Some(value) = &self.explicit.commit_sha {
            snapshot.commit_sha = Some(value.clone());
            // Do not attach automatic dirty/branch data to an explicitly selected SHA.
            snapshot.commit_dirty = None;
            snapshot.branch = None;
        }
        if let Some(value) = self.explicit.commit_dirty {
            snapshot.commit_dirty = Some(value);
        }
        if let Some(value) = &self.explicit.branch {
            snapshot.branch = Some(value.clone());
        }
        snapshot
    }
}

fn nonempty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

fn env_value(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn bool_env(key: &str) -> Option<bool> {
    let raw = std::env::var(key).ok()?;
    if raw.eq_ignore_ascii_case("true") || raw == "1" {
        Some(true)
    } else if raw.eq_ignore_ascii_case("false") || raw == "0" {
        Some(false)
    } else {
        None
    }
}

fn is_automatic_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn automatic_snapshot(sha: Option<String>, branch: Option<String>) -> Option<AppGitSnapshot> {
    let commit_sha = sha.filter(|value| is_automatic_sha(value))?;
    Some(AppGitSnapshot {
        commit_sha: Some(commit_sha),
        branch: branch.filter(|value| !value.is_empty()),
        commit_dirty: None,
        commit_dirty_inferred: true,
        branch_inferred: true,
        raw_properties: BTreeMap::new(),
    })
}

fn truthy_marker(key: &str) -> bool {
    std::env::var(key).ok().is_some_and(|raw| {
        raw == "1"
            || raw.eq_ignore_ascii_case("true")
            || raw.eq_ignore_ascii_case("yes")
            || raw.eq_ignore_ascii_case("on")
    })
}

fn reliable_build_metadata(detect_branch: bool) -> Option<AppGitSnapshot> {
    if truthy_marker("VERCEL") {
        return automatic_snapshot(
            nonempty_env("VERCEL_GIT_COMMIT_SHA"),
            detect_branch
                .then(|| nonempty_env("VERCEL_GIT_COMMIT_REF"))
                .flatten(),
        );
    }
    if truthy_marker("RENDER") {
        return automatic_snapshot(
            nonempty_env("RENDER_GIT_COMMIT"),
            detect_branch
                .then(|| nonempty_env("RENDER_GIT_BRANCH"))
                .flatten(),
        );
    }
    if nonempty_env("DYNO").is_some_and(|value| !value.eq_ignore_ascii_case("false")) {
        return automatic_snapshot(nonempty_env("SOURCE_VERSION"), None);
    }
    None
}

fn contextual_ci_metadata(detect_branch: bool) -> Option<AppGitSnapshot> {
    let sources = [
        ("GITHUB_ACTIONS", "GITHUB_SHA", ["GITHUB_REF_NAME", ""]),
        ("GITLAB_CI", "CI_COMMIT_SHA", ["CI_COMMIT_REF_NAME", ""]),
        ("CIRCLECI", "CIRCLE_SHA1", ["CIRCLE_BRANCH", ""]),
        ("BUILDKITE", "BUILDKITE_COMMIT", ["BUILDKITE_BRANCH", ""]),
    ];
    for (marker, sha, branches) in sources {
        if !truthy_marker(marker) {
            continue;
        }
        let branch = detect_branch.then(|| {
            branches
                .into_iter()
                .filter(|key| !key.is_empty())
                .find_map(nonempty_env)
        });
        if let Some(metadata) = automatic_snapshot(nonempty_env(sha), branch.flatten()) {
            return Some(metadata);
        }
    }
    None
}

fn discover_local_git(directory: &std::path::Path, detect_branch: bool) -> Option<AppGitSnapshot> {
    discover_local_git_with_program(directory, detect_branch, std::ffi::OsStr::new("git"))
}

fn discover_local_git_with_program(
    directory: &std::path::Path,
    detect_branch: bool,
    program: &std::ffi::OsStr,
) -> Option<AppGitSnapshot> {
    let deadline = Instant::now() + DISCOVERY_TIMEOUT;
    let sha = run_git(
        program,
        directory,
        &["rev-parse", "--verify", "HEAD"],
        deadline,
    )?;
    if !sha.complete || !sha.status.success() {
        return None;
    }
    let commit_sha = String::from_utf8(sha.output).ok()?.trim().to_string();
    if !is_automatic_sha(&commit_sha) {
        return None;
    }

    let dirty = run_git(
        program,
        directory,
        &["status", "--porcelain=v1", "--untracked-files=normal"],
        deadline,
    )
    .and_then(dirty_from_status);
    let branch = if detect_branch {
        run_git(
            program,
            directory,
            &["symbolic-ref", "--quiet", "--short", "HEAD"],
            deadline,
        )
        .filter(|result| result.complete && result.status.success())
        .and_then(|result| String::from_utf8(result.output).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    } else {
        None
    };

    // Status and branch belong only to the revision whose SHA was captured.
    // If HEAD moved concurrently, retain the known SHA but omit companions.
    let revision_stable = run_git(
        program,
        directory,
        &["rev-parse", "--verify", "HEAD"],
        deadline,
    )
    .filter(|result| result.complete && result.status.success())
    .and_then(|result| String::from_utf8(result.output).ok())
    .is_some_and(|value| value.trim() == commit_sha);
    let (dirty, branch) = if revision_stable {
        (dirty, branch)
    } else {
        (None, None)
    };

    Some(AppGitSnapshot {
        commit_sha: Some(commit_sha),
        commit_dirty: dirty,
        branch,
        commit_dirty_inferred: true,
        branch_inferred: true,
        raw_properties: BTreeMap::new(),
    })
}

struct CommandResult {
    status: ExitStatus,
    output: Vec<u8>,
    complete: bool,
}

fn dirty_from_status(result: CommandResult) -> Option<bool> {
    (result.complete && result.status.success()).then_some(!result.output.is_empty())
}

fn run_git(
    program: &std::ffi::OsStr,
    directory: &std::path::Path,
    arguments: &[&str],
    deadline: Instant,
) -> Option<CommandResult> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .ok()?;
    runtime.block_on(run_git_async(program, directory, arguments, deadline))
}

async fn run_git_async(
    program: &std::ffi::OsStr,
    directory: &std::path::Path,
    arguments: &[&str],
    deadline: Instant,
) -> Option<CommandResult> {
    let remaining = deadline.checked_duration_since(Instant::now())?;
    let mut command = tokio::process::Command::new(program);
    sanitize_git_environment(&mut command);
    let mut child = command
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let timeout = tokio::time::sleep(remaining);
    tokio::pin!(timeout);
    let mut status = None;
    let mut output = Vec::new();
    let mut buffer = [0_u8; 512];
    let mut stdout_open = true;
    loop {
        if status.is_none() {
            match child.try_wait() {
                Ok(Some(exit)) => status = Some(exit),
                Ok(None) => {}
                Err(_) => return None,
            }
            if status.is_some() && !stdout_open {
                return status.map(|status| CommandResult {
                    status,
                    output,
                    complete: true,
                });
            }
        }
        tokio::select! {
            _ = &mut timeout => {
                if status.is_none() {
                    status = child.try_wait().ok().flatten();
                }
                if status.is_none() {
                    terminate_child(&mut child).await;
                }
                return status.map(|status| CommandResult {
                    status,
                    output,
                    complete: false,
                });
            }
            result = child.wait(), if status.is_none() => {
                status = Some(result.ok()?);
                if !stdout_open {
                    return status.map(|status| CommandResult {
                        status,
                        output,
                        complete: true,
                    });
                }
            }
            result = stdout.read(&mut buffer), if stdout_open => {
                match result {
                    Ok(0) => {
                        stdout_open = false;
                        if let Some(status) = status {
                            return Some(CommandResult {
                                status,
                                output,
                                complete: true,
                            });
                        }
                    }
                    Ok(count) => {
                        let remaining = MAX_GIT_OUTPUT_BYTES.saturating_sub(output.len());
                        output.extend_from_slice(&buffer[..count.min(remaining)]);
                        if count > remaining || output.len() == MAX_GIT_OUTPUT_BYTES {
                            if status.is_none() {
                                terminate_child(&mut child).await;
                            }
                            return status.map(|status| CommandResult {
                                status,
                                output,
                                complete: false,
                            });
                        }
                    }
                    Err(_) => {
                        return status.map(|status| CommandResult {
                            status,
                            output,
                            complete: false,
                        })
                    }
                }
            }
        }
    }
}

fn sanitize_git_environment(command: &mut tokio::process::Command) {
    for key in GIT_IDENTITY_ENV_VARS {
        command.env_remove(key);
    }
    // `GIT_CONFIG_COUNT` uses numbered key/value variables. Remove them too,
    // even though clearing the count already makes Git ignore them.
    for (key, _) in std::env::vars_os() {
        let key_lossy = key.to_string_lossy();
        if key_lossy.starts_with("GIT_CONFIG_KEY_") || key_lossy.starts_with("GIT_CONFIG_VALUE_") {
            command.env_remove(key);
        }
    }
}

async fn terminate_child(child: &mut tokio::process::Child) {
    let _ = child.start_kill();
    let _ = tokio::time::timeout(PROCESS_CLEANUP_TIMEOUT, child.wait()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[cfg(unix)]
    fn fake_git(script: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("raindrop-fake-git-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, script).expect("write fake git");
        let mut permissions = std::fs::metadata(&path)
            .expect("fake git metadata")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).expect("make fake git executable");
        path
    }

    fn git(directory: &std::path::Path, arguments: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(arguments)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("launch git");
        assert!(status.success(), "git {arguments:?} failed");
    }

    fn create_repository(directory: &std::path::Path, contents: &str) -> String {
        std::fs::create_dir(directory).expect("create repository directory");
        git(directory, &["init"]);
        git(
            directory,
            &["config", "user.email", "sdk-test@raindrop.invalid"],
        );
        git(directory, &["config", "user.name", "Raindrop SDK Test"]);
        std::fs::write(directory.join("app.txt"), contents).expect("write repository source");
        git(directory, &["add", "app.txt"]);
        git(directory, &["commit", "-m", "application commit"]);
        let output = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("read repository sha");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("utf8 repository sha")
            .trim()
            .to_string()
    }

    fn wait_for_snapshot(provider: &AppGitProvider) -> AppGitSnapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = provider.snapshot();
            if snapshot.commit_sha.is_some() {
                return snapshot;
            }
            assert!(
                Instant::now() < deadline,
                "application Git discovery timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn automatic_sha_requires_full_hex() {
        assert!(is_automatic_sha(&"a".repeat(40)));
        assert!(is_automatic_sha(&"B".repeat(64)));
        assert!(!is_automatic_sha(&"a".repeat(39)));
        assert!(!is_automatic_sha(&format!("{}z", "a".repeat(39))));
    }

    #[test]
    fn operation_registry_reclaims_finished_ids() {
        let registry = AppGitOperationRegistry::new(10_000);
        let empty = BTreeMap::new();
        let sha_a = "a".repeat(40);
        let sha_b = "b".repeat(40);
        for index in 0..10_050 {
            let event_id = format!("finished-{index}");
            let snapshot = registry.begin(
                &event_id,
                AppGitSnapshot {
                    commit_sha: Some(sha_a.clone()),
                    ..Default::default()
                },
                &empty,
            );
            assert_eq!(snapshot.commit_sha.as_deref(), Some(sha_a.as_str()));
            registry.remove(&event_id);
        }
        let snapshot = registry.begin(
            "after-reclaim",
            AppGitSnapshot {
                commit_sha: Some(sha_b.clone()),
                ..Default::default()
            },
            &empty,
        );
        assert_eq!(snapshot.commit_sha.as_deref(), Some(sha_b.as_str()));
    }

    #[test]
    fn operation_registry_freezes_missing_absence_and_capacity_pressure() {
        let registry = AppGitOperationRegistry::new(1);
        let empty = BTreeMap::new();
        let snapshot_a = AppGitSnapshot {
            commit_sha: Some("a".repeat(40)),
            ..Default::default()
        };
        let snapshot_b = AppGitSnapshot {
            commit_sha: Some("b".repeat(40)),
            ..Default::default()
        };
        let snapshot_c = AppGitSnapshot {
            commit_sha: Some("c".repeat(40)),
            ..Default::default()
        };

        assert_eq!(
            registry
                .context_for_event("missing-absence", AppGitSnapshot::default())
                .commit_sha
                .as_deref(),
            None
        );
        assert_eq!(
            registry.context_for_event("missing-absence", snapshot_b.clone()),
            AppGitSnapshot::default()
        );

        registry.remove("missing-absence");

        let active = registry.begin("active", snapshot_a.clone(), &empty);
        assert_eq!(
            active.commit_sha.as_deref(),
            snapshot_a.commit_sha.as_deref()
        );
        let full = registry.update("overflow", snapshot_b.clone(), &empty);
        assert_eq!(full, AppGitSnapshot::default());
        assert_eq!(
            registry
                .context_for_event("active", snapshot_c.clone())
                .commit_sha
                .as_deref(),
            snapshot_a.commit_sha.as_deref()
        );
        assert_eq!(
            registry.context_for_event("unknown-after-full", snapshot_c),
            AppGitSnapshot::default()
        );
    }

    #[test]
    fn operation_registry_concurrent_updates_preserve_operation_context() {
        let registry = AppGitOperationRegistry::new(10_000);
        let empty = BTreeMap::new();
        registry.begin("operation", AppGitSnapshot::default(), &empty);
        let mut handles = Vec::new();
        for _ in 0..16 {
            let registry = registry.clone();
            handles.push(std::thread::spawn(move || {
                let raw = BTreeMap::from([(
                    COMMIT_SHA_PROPERTY.to_string(),
                    Value::String("b".repeat(40)),
                )]);
                registry.update(
                    "operation",
                    AppGitSnapshot {
                        commit_sha: Some("c".repeat(40)),
                        ..Default::default()
                    },
                    &raw,
                );
            }));
        }
        for handle in handles {
            handle.join().expect("registry update thread");
        }

        let snapshot = registry.context_for_event(
            "operation",
            AppGitSnapshot {
                commit_sha: Some("c".repeat(40)),
                ..Default::default()
            },
        );
        let mut properties = BTreeMap::new();
        snapshot.enrich_properties(&mut properties);
        assert_eq!(properties[COMMIT_SHA_PROPERTY], "b".repeat(40));
    }

    #[test]
    fn operation_registry_poison_fails_closed_without_panicking() {
        let registry = AppGitOperationRegistry::new(10_000);
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind({
            let registry = registry.clone();
            move || {
                let _lock = registry.inner.lock().expect("registry lock");
                panic!("poison registry for test");
            }
        });
        std::panic::set_hook(previous_hook);
        assert!(result.is_err());

        let live = AppGitSnapshot {
            commit_sha: Some("c".repeat(40)),
            ..Default::default()
        };
        assert_eq!(
            registry.context_for_event("poisoned", live.clone()),
            AppGitSnapshot::default()
        );
        let raw = BTreeMap::from([(
            COMMIT_SHA_PROPERTY.to_string(),
            Value::String("b".repeat(40)),
        )]);
        let snapshot = registry.update("poisoned", live, &raw);
        let mut properties = BTreeMap::new();
        snapshot.enrich_properties(&mut properties);
        assert_eq!(properties[COMMIT_SHA_PROPERTY], "b".repeat(40));
    }

    #[test]
    fn explicit_null_property_blocks_automatic_attribute() {
        let snapshot = AppGitSnapshot {
            commit_sha: Some("a".repeat(40)),
            commit_dirty: Some(false),
            branch: Some("main".to_string()),
            commit_dirty_inferred: true,
            branch_inferred: true,
            raw_properties: BTreeMap::new(),
        };
        let properties = BTreeMap::from([(COMMIT_SHA_PROPERTY.to_string(), Value::Null)]);
        let mut attributes = Vec::new();
        snapshot.enrich_attributes(&properties, &mut attributes);
        assert!(!attributes
            .iter()
            .any(|attribute| attribute.key == COMMIT_SHA_PROPERTY));
        assert!(!attributes
            .iter()
            .any(|attribute| attribute.key == COMMIT_DIRTY_PROPERTY));
        assert!(!attributes
            .iter()
            .any(|attribute| attribute.key == BRANCH_PROPERTY));
    }

    #[test]
    fn local_discovery_reports_the_application_checkout() {
        let directory =
            std::env::temp_dir().join(format!("raindrop-app-git-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("create temporary repository");
        git(&directory, &["init"]);
        git(
            &directory,
            &["config", "user.email", "sdk-test@raindrop.invalid"],
        );
        git(&directory, &["config", "user.name", "Raindrop SDK Test"]);
        std::fs::write(directory.join("app.txt"), "one\n").expect("write app source");
        git(&directory, &["add", "app.txt"]);
        git(&directory, &["commit", "-m", "application commit"]);
        let expected = std::process::Command::new("git")
            .arg("-C")
            .arg(&directory)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("read expected sha");
        let expected = String::from_utf8(expected.stdout)
            .expect("utf8 sha")
            .trim()
            .to_string();
        std::fs::write(directory.join("app.txt"), "two\n").expect("dirty app source");

        let snapshot = discover_local_git(&directory, true).expect("discover application git");
        assert_eq!(snapshot.commit_sha.as_deref(), Some(expected.as_str()));
        assert_eq!(snapshot.commit_dirty, Some(true));
        assert!(snapshot.branch.is_some());
        std::fs::remove_dir_all(directory).expect("remove temporary repository");
    }

    #[test]
    fn repository_selectors_cannot_redirect_explicit_or_cwd_discovery() {
        const CHILD: &str = "RAINDROP_APP_GIT_SELECTOR_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let mut command =
                Command::new(std::env::current_exe().expect("current test executable"));
            command
                .args([
                    "--exact",
                    "app_git::tests::repository_selectors_cannot_redirect_explicit_or_cwd_discovery",
                ])
                .env(CHILD, "1");
            for key in [
                "RAINDROP_COMMIT_SHA",
                "RAINDROP_COMMIT_DIRTY",
                "RAINDROP_BRANCH",
                "VERCEL",
                "RENDER",
                "DYNO",
            ] {
                command.env_remove(key);
            }
            let status = command.status().expect("run isolated selector test");
            assert!(status.success());
            return;
        }

        let original_cwd = std::env::current_dir().expect("current directory");
        let root = std::env::temp_dir().join(format!(
            "raindrop-app-git-selectors-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).expect("create fixture root");
        let application = root.join("application-a");
        let observer = root.join("observer-b");
        let application_sha = create_repository(&application, "application\n");
        let observer_sha = create_repository(&observer, "observer\n");
        assert_ne!(application_sha, observer_sha);

        let observer_git = observer.join(".git");
        std::env::set_var("GIT_DIR", &observer_git);
        std::env::set_var("GIT_WORK_TREE", &observer);
        std::env::set_var("GIT_COMMON_DIR", &observer_git);
        std::env::set_var("GIT_INDEX_FILE", observer_git.join("index"));
        std::env::set_var("GIT_OBJECT_DIRECTORY", observer_git.join("objects"));
        std::env::set_var(
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            observer_git.join("objects"),
        );
        std::env::set_var("GIT_NAMESPACE", "observer");
        std::env::set_var("GIT_CEILING_DIRECTORIES", &application);
        std::env::set_var("GIT_DISCOVERY_ACROSS_FILESYSTEM", "false");
        std::env::set_var("GIT_CONFIG_COUNT", "1");
        std::env::set_var("GIT_CONFIG_KEY_0", "core.worktree");
        std::env::set_var("GIT_CONFIG_VALUE_0", &observer);
        std::env::set_var("VERCEL", "1");
        std::env::set_var("VERCEL_GIT_COMMIT_SHA", &observer_sha);
        std::env::set_var("VERCEL_GIT_COMMIT_REF", "observer-deploy");
        std::env::set_var("GITHUB_ACTIONS", "true");
        std::env::set_var("GITHUB_SHA", &observer_sha);
        std::env::set_var("GITHUB_REF_NAME", "observer-ci");

        let explicit = wait_for_snapshot(&AppGitProvider::new(
            AppGitConfig::new()
                .source_directory(&application)
                .detect_branch(false)
                .auto_detect(true),
        ));
        assert_eq!(
            explicit.commit_sha.as_deref(),
            Some(application_sha.as_str())
        );

        std::env::set_var("RAINDROP_GIT_SOURCE_DIRECTORY", &application);
        let from_environment = wait_for_snapshot(&AppGitProvider::new(
            AppGitConfig::new().detect_branch(false).auto_detect(true),
        ));
        assert_eq!(
            from_environment.commit_sha.as_deref(),
            Some(application_sha.as_str())
        );

        let unavailable = root.join("unavailable-application-a");
        let unavailable_provider = AppGitProvider::new(
            AppGitConfig::new()
                .source_directory(&unavailable)
                .detect_branch(false)
                .auto_detect(true),
        );
        std::thread::sleep(DISCOVERY_TIMEOUT + Duration::from_millis(100));
        assert_eq!(unavailable_provider.snapshot(), AppGitSnapshot::default());

        std::env::set_current_dir(&application).expect("enter application directory");
        let cwd = wait_for_snapshot(&AppGitProvider::new(
            AppGitConfig::new()
                .source_directory(".")
                .detect_branch(false)
                .auto_detect(true),
        ));
        assert_eq!(cwd.commit_sha.as_deref(), Some(application_sha.as_str()));

        // Sanitization is child-command-only; customer process state remains intact.
        assert_eq!(
            std::env::var_os("GIT_DIR").as_deref(),
            Some(observer_git.as_os_str())
        );
        assert_eq!(std::env::var("GIT_CONFIG_COUNT").as_deref(), Ok("1"));
        std::env::set_current_dir(original_cwd).expect("restore current directory");
        std::fs::remove_dir_all(root).expect("remove selector fixture");
    }

    #[cfg(unix)]
    #[test]
    fn command_observation_is_bounded_for_retained_stdout_missing_noisy_and_exit() {
        let retained = fake_git(&format!(
            "#!/bin/sh\nprintf '{}'\n(sleep 5) &\nexit 0\n",
            "a".repeat(40)
        ));
        let started = Instant::now();
        let result = run_git(
            retained.as_os_str(),
            std::path::Path::new("."),
            &["rev-parse"],
            Instant::now() + Duration::from_millis(500),
        );
        if let Some(result) = result {
            assert!(result.status.success());
            assert!(!result.complete, "retained stdout must be incomplete");
            assert_eq!(String::from_utf8(result.output).unwrap(), "a".repeat(40));
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        std::fs::remove_file(retained).expect("remove retained fake");

        use std::os::unix::process::ExitStatusExt;
        let incomplete_clean = CommandResult {
            status: ExitStatus::from_raw(0),
            output: Vec::new(),
            complete: false,
        };
        assert_eq!(dirty_from_status(incomplete_clean), None);

        assert!(run_git(
            std::ffi::OsStr::new("/definitely/missing/raindrop-git"),
            std::path::Path::new("."),
            &["rev-parse"],
            Instant::now() + Duration::from_millis(500),
        )
        .is_none());

        let noisy =
            fake_git("#!/bin/sh\nwhile :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done\n");
        let started = Instant::now();
        assert!(run_git(
            noisy.as_os_str(),
            std::path::Path::new("."),
            &["rev-parse"],
            Instant::now() + Duration::from_millis(500),
        )
        .is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        std::fs::remove_file(noisy).expect("remove noisy fake");

        let exited = fake_git("#!/bin/sh\nprintf 'done'\nexit 0\n");
        let result = run_git(
            exited.as_os_str(),
            std::path::Path::new("."),
            &["rev-parse"],
            Instant::now() + Duration::from_millis(500),
        )
        .expect("exited process result");
        assert!(result.status.success());
        assert!(result.complete);
        assert_eq!(result.output, b"done");
        std::fs::remove_file(exited).expect("remove exited fake");

        let truncated = fake_git(&format!(
            "#!/bin/sh\nprintf '{}'\nexit 0\n",
            "f".repeat(MAX_GIT_OUTPUT_BYTES + 100)
        ));
        let result = run_git(
            truncated.as_os_str(),
            std::path::Path::new("."),
            &["rev-parse"],
            Instant::now() + Duration::from_millis(500),
        );
        assert!(result.is_none_or(|result| !result.complete));
        std::fs::remove_file(truncated).expect("remove truncated fake");
    }

    #[cfg(unix)]
    #[test]
    fn dirty_timeout_preserves_a_previously_discovered_sha() {
        let program = fake_git(&format!(
            "#!/bin/sh\ncase \"$3\" in\n  rev-parse) printf '{}'; exit 0;;\n  status) sleep 5;;\nesac\nexit 1\n",
            "b".repeat(40)
        ));
        let started = Instant::now();
        let snapshot =
            discover_local_git_with_program(std::path::Path::new("."), false, program.as_os_str())
                .expect("SHA survives dirty timeout");
        assert_eq!(
            snapshot.commit_sha.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(snapshot.commit_dirty, None);
        assert!(started.elapsed() < Duration::from_secs(2));
        std::fs::remove_file(program).expect("remove timeout fake");
    }

    #[cfg(unix)]
    #[test]
    fn incomplete_clean_status_and_concurrent_checkout_omit_companions() {
        let retained_status = fake_git(&format!(
            "#!/bin/sh\ncase \"$3\" in\n  rev-parse) printf '{}'; exit 0;;\n  status) (sleep 5) & exit 0;;\nesac\nexit 1\n",
            "a".repeat(40)
        ));
        let snapshot = discover_local_git_with_program(
            std::path::Path::new("."),
            false,
            retained_status.as_os_str(),
        )
        .expect("completed SHA remains available");
        assert_eq!(
            snapshot.commit_sha.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(snapshot.commit_dirty, None);
        std::fs::remove_file(retained_status).expect("remove retained-status fake");

        let state =
            std::env::temp_dir().join(format!("raindrop-fake-git-state-{}", uuid::Uuid::new_v4()));
        let changed_head = fake_git(&format!(
            "#!/bin/sh\ncase \"$3\" in\n  rev-parse) if test -f '{state}'; then printf '{second}'; else : > '{state}'; printf '{first}'; fi;;\n  status) exit 0;;\n  symbolic-ref) printf 'main';;\nesac\nexit 0\n",
            state = state.display(),
            first = "b".repeat(40),
            second = "c".repeat(40),
        ));
        let snapshot = discover_local_git_with_program(
            std::path::Path::new("."),
            true,
            changed_head.as_os_str(),
        )
        .expect("initial SHA remains available");
        assert_eq!(
            snapshot.commit_sha.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(snapshot.commit_dirty, None);
        assert_eq!(snapshot.branch, None);
        std::fs::remove_file(changed_head).expect("remove changed-head fake");
        std::fs::remove_file(state).expect("remove fake state");
    }

    #[test]
    fn explicit_env_and_deployment_metadata_are_immediate_and_markers_are_truthy() {
        const CHILD: &str = "RAINDROP_APP_GIT_ENV_TEST_CHILD";
        if let Ok(mode) = std::env::var(CHILD) {
            let config = if mode == "deploy" {
                AppGitConfig::new()
            } else {
                AppGitConfig::new().source_directory("/missing")
            };
            let provider = AppGitProvider::new(config);
            let snapshot = provider.snapshot();
            match mode.as_str() {
                "empty" => assert_eq!(snapshot.commit_sha.as_deref(), Some("")),
                "deploy" => assert_eq!(
                    snapshot.commit_sha.as_deref(),
                    Some("cccccccccccccccccccccccccccccccccccccccc")
                ),
                "false-marker" => assert_eq!(snapshot.commit_sha, None),
                "github" => {
                    let metadata = contextual_ci_metadata(true).expect("GitHub metadata");
                    assert_eq!(
                        metadata.commit_sha.as_deref(),
                        Some("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                    );
                    assert_eq!(metadata.branch.as_deref(), Some("42/merge"));
                }
                _ => panic!("unexpected child mode"),
            }
            return;
        }
        let executable = std::env::current_exe().expect("current test executable");
        let exact = "app_git::tests::explicit_env_and_deployment_metadata_are_immediate_and_markers_are_truthy";
        for mode in ["empty", "deploy", "false-marker", "github"] {
            let mut command = Command::new(&executable);
            command.args(["--exact", exact]).env(CHILD, mode);
            command
                .env_remove("RAINDROP_COMMIT_SHA")
                .env_remove("VERCEL")
                .env_remove("VERCEL_GIT_COMMIT_SHA");
            match mode {
                "empty" => {
                    command
                        .env("RAINDROP_COMMIT_SHA", "")
                        .env("GITHUB_ACTIONS", "true")
                        .env("GITHUB_SHA", "d".repeat(40));
                }
                "deploy" => {
                    command
                        .env("VERCEL", "1")
                        .env("VERCEL_GIT_COMMIT_SHA", "c".repeat(40));
                }
                "false-marker" => {
                    command
                        .env("VERCEL", "false")
                        .env("VERCEL_GIT_COMMIT_SHA", "c".repeat(40))
                        .env_remove("GITHUB_ACTIONS");
                }
                "github" => {
                    command
                        .env("GITHUB_ACTIONS", "true")
                        .env("GITHUB_SHA", "e".repeat(40))
                        .env("GITHUB_HEAD_REF", "feature-head")
                        .env("GITHUB_REF_NAME", "42/merge");
                }
                _ => unreachable!(),
            };
            assert!(command.status().expect("run isolated env test").success());
        }
    }
}
