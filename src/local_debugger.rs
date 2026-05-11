use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

pub const LOCAL_DEBUGGER_ENV_VAR: &str = "RAINDROP_LOCAL_DEBUGGER";
pub const WORKSHOP_ENV_VAR: &str = "RAINDROP_WORKSHOP";
pub const DEFAULT_LOCAL_WORKSHOP_URL: &str = "http://localhost:5899/v1/";

const PROBE_HOST: &str = "127.0.0.1";
const PROBE_PORT: u16 = 5899;
const PROBE_TIMEOUT: Duration = Duration::from_millis(100);

/// Tri-state for the builder slot: not called, explicit URL, explicit opt-out.
/// Mirrors Python's `UNSET` sentinel (`Inherit`) vs `None` (`Disabled`) vs
/// `str` (`Url(..)`).
#[derive(Debug, Clone, Default)]
pub enum LocalWorkshopUrlConfig {
    #[default]
    Inherit,
    Disabled,
    Url(String),
}

#[derive(Debug, PartialEq)]
enum WorkshopEnv {
    Unset,
    Enable,
    Disable,
    Url(String),
}

fn format_endpoint(url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("http://") && !lowered.starts_with("https://") {
        return None;
    }
    if trimmed.ends_with('/') {
        Some(trimmed.to_string())
    } else {
        Some(format!("{}/", trimmed))
    }
}

fn read_workshop_env() -> WorkshopEnv {
    let raw = match std::env::var(WORKSHOP_ENV_VAR) {
        Ok(v) => v,
        Err(_) => return WorkshopEnv::Unset,
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return WorkshopEnv::Unset;
    }
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("http://") || lowered.starts_with("https://") {
        return WorkshopEnv::Url(trimmed.to_string());
    }
    match lowered.as_str() {
        "1" | "true" | "yes" | "on" => WorkshopEnv::Enable,
        "0" | "false" | "no" | "off" => WorkshopEnv::Disable,
        _ => WorkshopEnv::Unset,
    }
}

pub(crate) fn probe_default_workshop() -> bool {
    probe_workshop(PROBE_HOST, PROBE_PORT)
}

fn probe_workshop(host: &str, port: u16) -> bool {
    let addr: SocketAddr = match (host, port).to_socket_addrs() {
        Ok(mut iter) => match iter.next() {
            Some(a) => a,
            None => return false,
        },
        Err(_) => return false,
    };
    TcpStream::connect_timeout(&addr, PROBE_TIMEOUT).is_ok()
}

pub fn resolve_local_workshop_url(
    config: &LocalWorkshopUrlConfig,
    auto_detect: bool,
) -> Option<String> {
    match config {
        LocalWorkshopUrlConfig::Disabled => return None,
        LocalWorkshopUrlConfig::Url(s) => return format_endpoint(s),
        LocalWorkshopUrlConfig::Inherit => {}
    }

    if let Ok(v) = std::env::var(LOCAL_DEBUGGER_ENV_VAR) {
        if let Some(formatted) = format_endpoint(&v) {
            return Some(formatted);
        }
    }

    match read_workshop_env() {
        WorkshopEnv::Disable => return None,
        WorkshopEnv::Enable => return Some(DEFAULT_LOCAL_WORKSHOP_URL.to_string()),
        WorkshopEnv::Url(s) => {
            if let Some(formatted) = format_endpoint(&s) {
                return Some(formatted);
            }
        }
        WorkshopEnv::Unset => {}
    }

    if auto_detect && probe_default_workshop() {
        return Some(DEFAULT_LOCAL_WORKSHOP_URL.to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// Process-wide lock to serialize tests that mutate `RAINDROP_*` env vars.
    /// `cargo test` runs in parallel by default and env state is global.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        std::env::remove_var(LOCAL_DEBUGGER_ENV_VAR);
        std::env::remove_var(WORKSHOP_ENV_VAR);
    }

    #[test]
    fn explicit_url_wins() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://from-env:9999/v1/");
        let cfg = LocalWorkshopUrlConfig::Url("http://kwarg:8888/v1/".into());
        assert_eq!(
            resolve_local_workshop_url(&cfg, false),
            Some("http://kwarg:8888/v1/".to_string())
        );
        clear_env();
    }

    #[test]
    fn explicit_url_appends_trailing_slash() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let cfg = LocalWorkshopUrlConfig::Url("http://kwarg:8888/v1".into());
        assert_eq!(
            resolve_local_workshop_url(&cfg, false),
            Some("http://kwarg:8888/v1/".to_string())
        );
    }

    #[test]
    fn explicit_disabled_opts_out() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://from-env:9999/v1/");
        assert_eq!(
            resolve_local_workshop_url(&LocalWorkshopUrlConfig::Disabled, false),
            None
        );
        clear_env();
    }

    #[test]
    fn local_debugger_env_used_when_inherit() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://from-env:9999/v1/");
        assert_eq!(
            resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
            Some("http://from-env:9999/v1/".to_string())
        );
        clear_env();
    }

    #[test]
    fn workshop_env_url_used() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var(WORKSHOP_ENV_VAR, "http://workshop:7777/v1/");
        assert_eq!(
            resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
            Some("http://workshop:7777/v1/".to_string())
        );
        clear_env();
    }

    #[test]
    fn workshop_env_truthy_strings_use_default() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        for v in ["1", "true", "True", "yes", "on", "ON"] {
            std::env::set_var(WORKSHOP_ENV_VAR, v);
            assert_eq!(
                resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
                Some(DEFAULT_LOCAL_WORKSHOP_URL.to_string()),
                "value {v}"
            );
        }
        clear_env();
    }

    #[test]
    fn workshop_env_falsy_strings_disable() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        for v in ["0", "false", "no", "off", "OFF"] {
            std::env::set_var(WORKSHOP_ENV_VAR, v);
            assert_eq!(
                resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
                None,
                "value {v}"
            );
        }
        clear_env();
    }

    #[test]
    fn local_debugger_env_beats_workshop_env() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "http://specific:1111/v1/");
        std::env::set_var(WORKSHOP_ENV_VAR, "1");
        assert_eq!(
            resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
            Some("http://specific:1111/v1/".to_string())
        );
        clear_env();
    }

    #[test]
    fn invalid_url_returns_none() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let cfg = LocalWorkshopUrlConfig::Url("not a url".into());
        assert_eq!(resolve_local_workshop_url(&cfg, false), None);
        std::env::set_var(LOCAL_DEBUGGER_ENV_VAR, "ftp://nope");
        assert_eq!(
            resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
            None
        );
        clear_env();
    }

    #[test]
    fn no_url_no_env_no_probe_returns_none() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        assert_eq!(
            resolve_local_workshop_url(&LocalWorkshopUrlConfig::Inherit, false),
            None
        );
    }

    #[test]
    fn tcp_probe_finds_listener() {
        let _g = ENV_LOCK.lock().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        assert!(probe_workshop("127.0.0.1", port));
    }

    #[test]
    fn tcp_probe_misses_when_nothing_listens() {
        let _g = ENV_LOCK.lock().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        assert!(!probe_workshop("127.0.0.1", port));
    }
}
