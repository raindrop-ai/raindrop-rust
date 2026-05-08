//! Workshop URL resolution.
//!
//! Mirror of `@raindrop-ai/core/contract/v1/workshop-detection.ts`. Resolves
//! whether the SDK should mirror telemetry to a locally running Workshop
//! daemon, and at which URL.
//!
//! Failure mode: every consumer of this helper sends to the resolved URL
//! fire-and-forget; mirror failures are swallowed so a missing daemon never
//! affects the cloud path. That makes "auto-enable when local" a free upgrade.

use is_terminal::IsTerminal;

/// Env var holding an explicit Workshop URL (the JS SDK's existing escape hatch).
pub const LOCAL_DEBUGGER_ENV_VAR: &str = "RAINDROP_LOCAL_DEBUGGER";
/// Env var holding either an explicit URL or a boolean enable/disable flag.
pub const WORKSHOP_ENV_VAR: &str = "RAINDROP_WORKSHOP";

/// Default URL used when Workshop is enabled without an explicit URL override.
pub const DEFAULT_WORKSHOP_URL: &str = "http://localhost:5899/v1/";

/// Constructor-time options that control [`resolve_workshop_url`]. Mirror of
/// the TS `WorkshopUrlOptions` interface.
#[derive(Debug, Clone, Default)]
pub struct WorkshopUrlOptions {
    /// Explicit URL. Bypasses env vars and auto-detection. Implies enabled.
    pub override_workshop_url: Option<String>,
    /// Explicit yes/no. `false` disables; `true` enables (env-var URL or default).
    pub enable_workshop: Option<bool>,
}

/// Normalize a Workshop URL by ensuring it ends with `/`, so callers can build
/// `{base}live`, `{base}events/track_partial`, etc., without re-checking the
/// trailing slash.
pub fn normalize_workshop_base_url(url: &str) -> String {
    if url.ends_with('/') {
        url.to_string()
    } else {
        format!("{url}/")
    }
}

/// Strip the `userinfo` component (`user:pass@`) from a URL before logging it.
///
/// Workshop URLs come from env vars (`RAINDROP_LOCAL_DEBUGGER` /
/// `RAINDROP_WORKSHOP`) or the `Client::builder().workshop_url(...)` option, so
/// they MAY include `http://user:pass@host/...` style credentials we don't
/// want to leak into centralized logs even at debug level. Mirrors the
/// `_sanitize_url_for_log` helper in the python SDK
/// (`raindrop/analytics.py::_sanitize_url_for_log`).
///
/// Best-effort: on any unexpected shape returns the literal `"<workshop-url>"`
/// rather than risk emitting userinfo. Output is `scheme://host[:port]/path`
/// (query / fragment preserved).
pub fn sanitize_workshop_url_for_log(url: &str) -> String {
    const FALLBACK: &str = "<workshop-url>";

    // Split scheme.
    let (scheme, rest) = match url.split_once("://") {
        Some(parts) => parts,
        None => return FALLBACK.to_string(),
    };
    if scheme.is_empty() || rest.is_empty() {
        return FALLBACK.to_string();
    }

    // Split authority from the rest of the URL at the first '/', '?', or '#'.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, path_query_fragment) = rest.split_at(authority_end);

    // Drop userinfo: keep only the part after the LAST '@' in the authority.
    // (`user:p@ss@host` is malformed; preferring last-'@' matches RFC 3986
    // §3.2.1 host parsing.)
    let host_and_port = match authority.rsplit_once('@') {
        Some((_userinfo, host)) => host,
        None => authority,
    };
    if host_and_port.is_empty() {
        return FALLBACK.to_string();
    }

    format!("{scheme}://{host_and_port}{path_query_fragment}")
}

fn read_env_var(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

enum WorkshopEnv {
    Enable,
    Disable,
    Url(String),
}

fn read_workshop_env() -> Option<WorkshopEnv> {
    let raw = read_env_var(WORKSHOP_ENV_VAR)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        return Some(WorkshopEnv::Url(trimmed.to_string()));
    }
    if matches!(lower.as_str(), "1" | "true" | "yes" | "on") {
        return Some(WorkshopEnv::Enable);
    }
    if matches!(lower.as_str(), "0" | "false" | "no" | "off") {
        return Some(WorkshopEnv::Disable);
    }
    None
}

/// Heuristic: should the SDK auto-enable Workshop mirroring when no explicit
/// signal (env var, builder flag) is given?
///
/// Triggers on `NODE_ENV=development`, `PYTHON_ENV=development`, or an
/// interactive (TTY-attached) stdout. Mirrors the TS contract.
///
/// Worst-case false positive (e.g. a container with `tty: true` in production)
/// produces a fire-and-forget connection-refused per flush interval against
/// `http://localhost:5899/v1/`; it never affects the cloud telemetry path
/// because mirror failures are swallowed inside `mirror_to_workshop`.
fn should_auto_enable_workshop() -> bool {
    if read_env_var("NODE_ENV").as_deref() == Some("development") {
        return true;
    }
    if read_env_var("PYTHON_ENV").as_deref() == Some("development") {
        return true;
    }
    // Interactive stdout is a strong signal that the host is a developer's
    // local CLI rather than a production server. Mirrors the TS check on
    // `process.stdout.isTTY`.
    if std::io::stdout().is_terminal() {
        return true;
    }
    false
}

/// Resolve the Workshop URL using the same precedence as the TS contract:
///
///   1. `override_workshop_url` constructor option (explicit URL)
///   2. `enable_workshop: false` (explicit disable)
///   3. `enable_workshop: true` + `RAINDROP_LOCAL_DEBUGGER` env (explicit URL)
///   4. `enable_workshop: true` (default URL)
///   5. `RAINDROP_LOCAL_DEBUGGER` env (explicit URL)
///   6. `RAINDROP_WORKSHOP` env (URL, boolean enable, or boolean disable)
///   7. auto-detect (NODE_ENV=development, PYTHON_ENV=development, isTTY)
pub fn resolve_workshop_url(opts: WorkshopUrlOptions) -> Option<String> {
    if let Some(url) = opts.override_workshop_url.as_ref() {
        if !url.is_empty() {
            return Some(normalize_workshop_base_url(url));
        }
    }

    if opts.enable_workshop == Some(false) {
        return None;
    }

    let explicit_url_env = read_env_var(LOCAL_DEBUGGER_ENV_VAR);
    if opts.enable_workshop == Some(true) {
        return Some(match explicit_url_env {
            Some(url) => normalize_workshop_base_url(&url),
            None => DEFAULT_WORKSHOP_URL.to_string(),
        });
    }

    if let Some(url) = explicit_url_env {
        return Some(normalize_workshop_base_url(&url));
    }

    match read_workshop_env() {
        Some(WorkshopEnv::Disable) => return None,
        Some(WorkshopEnv::Enable) => return Some(DEFAULT_WORKSHOP_URL.to_string()),
        Some(WorkshopEnv::Url(url)) => return Some(normalize_workshop_base_url(&url)),
        None => {}
    }

    if should_auto_enable_workshop() {
        return Some(DEFAULT_WORKSHOP_URL.to_string());
    }
    None
}

#[cfg(test)]
mod sanitize_tests {
    use super::sanitize_workshop_url_for_log;

    #[test]
    fn strips_userinfo_with_password() {
        assert_eq!(
            sanitize_workshop_url_for_log("http://user:secret@workshop.local/v1/"),
            "http://workshop.local/v1/"
        );
    }

    #[test]
    fn strips_userinfo_without_password() {
        assert_eq!(
            sanitize_workshop_url_for_log("https://token@host:8080/v1/"),
            "https://host:8080/v1/"
        );
    }

    #[test]
    fn passes_through_url_without_userinfo() {
        assert_eq!(
            sanitize_workshop_url_for_log("http://localhost:5899/v1/"),
            "http://localhost:5899/v1/"
        );
    }

    #[test]
    fn preserves_query_and_fragment() {
        assert_eq!(
            sanitize_workshop_url_for_log("http://u:p@h/path?a=1&b=2#frag"),
            "http://h/path?a=1&b=2#frag"
        );
    }

    #[test]
    fn handles_at_in_path_after_authority() {
        // `@` inside the path must not be treated as userinfo.
        assert_eq!(
            sanitize_workshop_url_for_log("http://h/path/with@symbol"),
            "http://h/path/with@symbol"
        );
    }

    #[test]
    fn falls_back_on_missing_scheme() {
        assert_eq!(
            sanitize_workshop_url_for_log("user:pass@host/v1/"),
            "<workshop-url>"
        );
    }

    #[test]
    fn falls_back_on_empty_authority() {
        assert_eq!(sanitize_workshop_url_for_log("http://"), "<workshop-url>");
    }

    #[test]
    fn falls_back_on_empty_host_after_userinfo() {
        assert_eq!(
            sanitize_workshop_url_for_log("http://user:pass@/path"),
            "<workshop-url>"
        );
    }
}
