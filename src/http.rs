use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use rand::Rng;
use reqwest::header::HeaderMap;
use reqwest::Client as ReqwestClient;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::error::Error;

/// Per-request timeout for fire-and-forget local Workshop mirror POSTs. Short
/// because a missing daemon must not delay the cloud retry loop.
const LOCAL_MIRROR_TIMEOUT: Duration = Duration::from_secs(2);

/// Longest `Retry-After` wait honored (CONTRACT 3 norm).
pub(crate) const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Overload pauses (CONTRACT 15.2): a 429 or 503 with `Retry-After` pauses
/// ALL sending to that server until the time it names, not just the retry of
/// the rejected request. Process-wide, keyed by base URL, like the other SDKs.
fn send_pauses() -> &'static std::sync::Mutex<std::collections::HashMap<String, Instant>> {
    static PAUSES: OnceLock<std::sync::Mutex<std::collections::HashMap<String, Instant>>> =
        OnceLock::new();
    PAUSES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn note_send_pause(base_url: &str, status: u16, retry_after: Option<Duration>) {
    let Some(wait) = retry_after else { return };
    if wait.is_zero() || !(status == 429 || status == 503) {
        return;
    }
    let until = Instant::now() + wait;
    let mut pauses = match send_pauses().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = pauses.entry(base_url.to_string()).or_insert(until);
    if until > *entry {
        *entry = until;
    }
}

fn send_pause_remaining(base_url: &str) -> Option<Duration> {
    let pauses = match send_pauses().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    pauses
        .get(base_url)
        .and_then(|until| until.checked_duration_since(Instant::now()))
}

/// 408 and 429 are the transient 4xx; every other 4xx is a permanent
/// rejection that must not be retried (CONTRACT 3, 15.2).
pub(crate) fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || status >= 500
}

/// Whether an error is a permanent rejection by ingest (a non-transient 4xx).
pub(crate) fn is_permanent_rejection(err: &Error) -> bool {
    matches!(err, Error::HttpStatus { status, .. } if (400..500).contains(status) && !is_retryable_status(*status))
}

/// Warn about what ingest rejected or changed in an accepted request.
///
/// A 2xx is delivered whatever its body says (CONTRACT 7.1). Ingest lists
/// invalid items it did not store under `rejected` and input it accepted with
/// changes (such as ignored fields) under `warnings`; both warn, rate-limited
/// (CONTRACT 14.2). An empty, non-JSON or unexpected body is ignored.
pub(crate) fn report_ingest_response(path: &str, body: &str) {
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(body) else {
        return;
    };
    let label = |item: &Value| -> String {
        match (
            item.get("event_id").and_then(Value::as_str),
            item.get("index").and_then(Value::as_u64),
        ) {
            (Some(id), _) if !id.is_empty() => format!("event {id}"),
            (_, Some(i)) => format!("item {i}"),
            _ => "item".to_string(),
        }
    };
    if let Some(rejected) = map.get("rejected").and_then(Value::as_array) {
        let rejected: Vec<&Value> = rejected.iter().filter(|v| v.is_object()).collect();
        if !rejected.is_empty() && crate::helpers::should_log_rate_limited("ingest_rejected") {
            let quoted: Vec<String> = rejected
                .iter()
                .take(3)
                .map(|item| {
                    let issues: Vec<String> = item
                        .get("issues")
                        .and_then(Value::as_array)
                        .map(|list| {
                            list.iter()
                                .map(|i| {
                                    let path = i.get("path").and_then(Value::as_str).unwrap_or("");
                                    let msg = i
                                        .get("message")
                                        .and_then(Value::as_str)
                                        .unwrap_or("invalid");
                                    if path.is_empty() {
                                        msg.to_string()
                                    } else {
                                        format!("{path}: {msg}")
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    format!(
                        "{} ({})",
                        label(item),
                        if issues.is_empty() {
                            "invalid".to_string()
                        } else {
                            issues.join("; ")
                        }
                    )
                })
                .collect();
            tracing::warn!(
                path,
                count = rejected.len(),
                "raindrop: ingest rejected {} item(s), which were not stored: {} (logged at most \
                 once per 30s)",
                rejected.len(),
                quoted.join(" | ")
            );
        }
    }
    if let Some(warnings) = map.get("warnings").and_then(Value::as_array) {
        let warnings: Vec<&Value> = warnings.iter().filter(|v| v.is_object()).collect();
        if !warnings.is_empty() && crate::helpers::should_log_rate_limited("ingest_warnings") {
            let quoted: Vec<String> = warnings
                .iter()
                .take(3)
                .map(|item| {
                    let message = item.get("message").and_then(Value::as_str).unwrap_or("");
                    if message.is_empty() {
                        let fields: Vec<&str> = item
                            .get("fields")
                            .and_then(Value::as_array)
                            .map(|f| f.iter().filter_map(Value::as_str).collect())
                            .unwrap_or_default();
                        format!("{}: ignored {}", label(item), fields.join(", "))
                    } else {
                        format!("{}: {message}", label(item))
                    }
                })
                .collect();
            tracing::warn!(
                path,
                count = warnings.len(),
                "raindrop: ingest accepted {} item(s) with changes: {} (logged at most once per 30s)",
                warnings.len(),
                quoted.join(" | ")
            );
        }
    }
}

/// Configuration for the retrying HTTP transport.
#[derive(Clone)]
pub(crate) struct TransportConfig {
    pub base_url: String,
    pub write_key: String,
    /// Validated project slug. When `Some`, every outbound request carries an
    /// `X-Raindrop-Project-Id` header so the backend routes telemetry to that
    /// project; when `None`, no header is sent and the backend uses the org's
    /// default project. Validated once at build time (see [`crate::project_id`]).
    pub project_id: Option<String>,
    pub local_workshop_url: Option<String>,
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub jitter_fraction: f64,
    /// Per-attempt bound applied to every cloud POST (connect through body),
    /// independent of how the underlying `reqwest::Client` was built — so a
    /// caller-injected client without timeouts can never hang a flush.
    pub request_timeout: Duration,
    pub debug: bool,
}

/// Why a deadline-bounded send did not deliver (see `post_json_until`).
#[derive(Debug)]
pub(crate) enum SendFailure {
    /// The deadline passed before the payload went out: before its first
    /// attempt, or while it waited on a retry or an overload pause.
    Unsent,
    /// The deadline ended a request mid-flight; it may have reached ingest.
    CutOff,
    /// Every attempt failed (retries ran out), or ingest rejected it.
    Failed(Error),
}

impl SendFailure {
    /// The failure as the public error type, for logging.
    pub(crate) fn into_error(self) -> Error {
        match self {
            SendFailure::Unsent => {
                Error::Http("deadline passed before the request was sent".into())
            }
            SendFailure::CutOff => Error::Http("request cut off at the deadline".into()),
            SendFailure::Failed(err) => err,
        }
    }

    /// Ingest received the payload and rejected it for good.
    pub(crate) fn is_permanent_rejection(&self) -> bool {
        matches!(self, SendFailure::Failed(err) if is_permanent_rejection(err))
    }
}

/// Hooks for tests to override sleep / jitter without monkey-patching.
type SleepFn =
    Arc<dyn Fn(Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync>;
type RandFn = Arc<dyn Fn() -> f64 + Send + Sync>;

#[derive(Clone)]
pub(crate) struct RetryingHttpClient {
    cfg: TransportConfig,
    http: ReqwestClient,
    sleep: Arc<Mutex<SleepFn>>,
    rand: Arc<Mutex<RandFn>>,
    mirror_tasks: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl RetryingHttpClient {
    pub(crate) fn new(cfg: TransportConfig, http: ReqwestClient) -> Self {
        Self {
            cfg,
            http,
            sleep: Arc::new(Mutex::new(Arc::new(|d| {
                Box::pin(async move { tokio::time::sleep(d).await })
            }))),
            rand: Arc::new(Mutex::new(Arc::new(|| rand::thread_rng().gen::<f64>()))),
            mirror_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Drain pending fire-and-forget local-mirror tasks. Used by `Client::flush`
    /// so tests (and explicit-flush callers) observe the mirror POST landing
    /// rather than racing against a background `tokio::spawn`.
    pub(crate) async fn await_pending_mirrors(&self) {
        let tasks: Vec<JoinHandle<()>> = {
            let mut guard = self.mirror_tasks.lock().expect("mirror task lock poisoned");
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }

    /// POST a JSON body with retries, bounded by a caller deadline (CONTRACT 1.2.1): no retry
    /// delay or overload pause runs past it, and each request's timeout is
    /// clamped to what is left of it. The failure says whether the payload
    /// was never sent (it can stay queued) or was cut off mid-request (it may
    /// already have reached ingest, so it must not be resent).
    pub(crate) async fn post_json_until<T: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &T,
        deadline: Option<Instant>,
    ) -> std::result::Result<(), SendFailure> {
        let remaining = || deadline.map(|d| d.saturating_duration_since(Instant::now()));
        if remaining() == Some(Duration::ZERO) {
            return Err(SendFailure::Unsent);
        }
        let payload = serde_json::to_vec(body).map_err(|e| SendFailure::Failed(e.into()))?;

        // No client-side size cap: whether a payload is too large is ingest's
        // decision, and a 413 is a permanent rejection (CONTRACT 15.1).
        if let Some(local_url) = &self.cfg.local_workshop_url {
            self.spawn_local_mirror(local_url.clone(), path.to_string(), payload.clone());
        }

        if self.cfg.write_key.is_empty() {
            return Ok(());
        }

        let url = format!(
            "{}{}",
            self.cfg.base_url,
            path.strip_prefix('/').unwrap_or(path)
        );

        let mut last_err: Option<Error> = None;

        for attempt in 1..=self.cfg.max_attempts {
            let mut delay = if attempt > 1 {
                self.retry_delay(attempt - 1, last_err.as_ref()).await
            } else {
                Duration::ZERO
            };
            if let Some(pause) = send_pause_remaining(&self.cfg.base_url) {
                delay = delay.max(pause);
            }
            if let Some(left) = remaining() {
                if delay >= left {
                    // Waiting on a retry or a Retry-After pause that outlasts
                    // the deadline: not sent, so it can stay queued.
                    return Err(SendFailure::Unsent);
                }
            }
            if delay > Duration::ZERO {
                let sleep = {
                    let guard = self.sleep.lock().await;
                    guard.clone()
                };
                sleep(delay).await;
            }
            // Telemetry must never wedge the host: bound every attempt even
            // when the reqwest client itself has no timeout configured (e.g.
            // caller-injected via `http_client`), and never past the deadline.
            let mut attempt_timeout = self.cfg.request_timeout;
            if let Some(left) = remaining() {
                if left.is_zero() {
                    return Err(SendFailure::Unsent);
                }
                attempt_timeout = attempt_timeout.min(left);
            }

            let mut req = self
                .http
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.cfg.write_key))
                .header("Content-Type", "application/json")
                .timeout(attempt_timeout)
                .body(payload.clone());

            if let Some(project_id) = &self.cfg.project_id {
                req = req.header(crate::project_id::PROJECT_ID_HEADER, project_id.as_str());
            }

            if self.cfg.debug {
                req = req.header("X-Raindrop-Sdk", "raindrop-rust");
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(err) => {
                    if remaining() == Some(Duration::ZERO) {
                        // The deadline ended this request mid-flight.
                        return Err(SendFailure::CutOff);
                    }
                    last_err = Some(Error::Http(err.to_string()));
                    continue;
                }
            };

            let status = resp.status();
            if status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                report_ingest_response(path, &body);
                return Ok(());
            }

            let retry_after = parse_retry_after(resp.headers()).map(|d| d.min(MAX_RETRY_AFTER));
            note_send_pause(&self.cfg.base_url, status.as_u16(), retry_after);
            let body = resp.text().await.unwrap_or_default();
            let truncated_body = if body.len() > 4096 {
                body[..4096].to_string()
            } else {
                body
            };

            let err = Error::HttpStatus {
                status: status.as_u16(),
                body: truncated_body,
            };

            // A permanent rejection is never retried; the caller drops the
            // payload with a warning (CONTRACT 3, 15.1).
            if !is_retryable_status(status.as_u16()) {
                return Err(SendFailure::Failed(err));
            }

            // Retryable: retain retry_after for the next iteration's backoff.
            last_err = Some(WithRetryAfter::wrap(err, retry_after));
        }

        Err(SendFailure::Failed(
            last_err.unwrap_or_else(|| Error::Http("unknown error".into())),
        ))
    }

    fn spawn_local_mirror(&self, base_url: String, path: String, payload: Vec<u8>) {
        let http = self.http.clone();
        let write_key = self.cfg.write_key.clone();
        let project_id = self.cfg.project_id.clone();
        let debug = self.cfg.debug;
        let task = tokio::spawn(async move {
            let url = format!("{}{}", base_url, path.strip_prefix('/').unwrap_or(&path));
            let mut req = http
                .post(&url)
                .header("Content-Type", "application/json")
                .body(payload)
                .timeout(LOCAL_MIRROR_TIMEOUT);
            if !write_key.is_empty() {
                req = req.header("Authorization", format!("Bearer {}", write_key));
            }
            if let Some(project_id) = &project_id {
                req = req.header(crate::project_id::PROJECT_ID_HEADER, project_id.as_str());
            }
            if debug {
                req = req.header("X-Raindrop-Sdk", "raindrop-rust");
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        tracing::debug!(%url, %status, "raindrop: local workshop mirror non-2xx");
                    }
                }
                Err(err) => {
                    tracing::debug!(%url, %err, "raindrop: local workshop mirror failed");
                }
            }
        });
        if let Ok(mut tasks) = self.mirror_tasks.lock() {
            // Prune completed handles so an app that never calls flush()
            // doesn't accumulate one JoinHandle per mirrored POST forever.
            tasks.retain(|t| !t.is_finished());
            tasks.push(task);
        }
    }

    async fn retry_delay(&self, retry_number: u32, previous: Option<&Error>) -> Duration {
        if let Some(retry_after) = previous.and_then(WithRetryAfter::extract) {
            return retry_after;
        }
        let base = self.cfg.base_delay;
        let mut delay = base.saturating_mul(1u32 << (retry_number.saturating_sub(1)));
        if self.cfg.jitter_fraction > 0.0 {
            let r = {
                let guard = self.rand.lock().await;
                guard()
            };
            let lo = 1.0 - self.cfg.jitter_fraction;
            let hi = 1.0 + self.cfg.jitter_fraction;
            let factor = lo + (hi - lo) * r;
            delay = Duration::from_secs_f64(delay.as_secs_f64() * factor);
        }
        delay
    }
}

impl std::fmt::Debug for RetryingHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryingHttpClient")
            .field("base_url", &self.cfg.base_url)
            .field("max_attempts", &self.cfg.max_attempts)
            .finish()
    }
}

/// Parse `Retry-After` header from either a number-of-seconds or HTTP-date format.
fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers
        .get("Retry-After")
        .or_else(|| headers.get("retry-after"))?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    if let Ok(parsed) = httpdate::parse_http_date(value) {
        let now = std::time::SystemTime::now();
        if let Ok(dur) = parsed.duration_since(now) {
            return Some(dur);
        }
        return Some(Duration::ZERO);
    }
    None
}

/// Adapt a `Retry-After` value through the `Error` chain so retries can honor it.
/// We piggyback on `Error::HttpStatus` by wrapping it with side-channel state in a thread-local
/// — but since we don't want global state, we instead use a tiny helper that augments a temporary
/// error type for the retry loop.
struct WithRetryAfter;

impl WithRetryAfter {
    fn wrap(err: Error, retry_after: Option<Duration>) -> Error {
        if let (Error::HttpStatus { status, body }, Some(d)) = (&err, retry_after) {
            // Encode retry-after into the body so the loop can extract it on the next iteration.
            return Error::HttpStatus {
                status: *status,
                body: format!("{}\n__retry_after_secs={}", body, d.as_secs_f64()),
            };
        }
        err
    }

    fn extract(err: &Error) -> Option<Duration> {
        if let Error::HttpStatus { body, .. } = err {
            for line in body.lines() {
                if let Some(rest) = line.strip_prefix("__retry_after_secs=") {
                    if let Ok(secs) = rest.parse::<f64>() {
                        return Some(Duration::from_secs_f64(secs));
                    }
                }
            }
        }
        None
    }
}

/// Normalize an endpoint to ensure it ends with `/`.
pub(crate) fn format_endpoint(endpoint: &str) -> String {
    if endpoint.is_empty() {
        return crate::DEFAULT_ENDPOINT.to_string();
    }
    if endpoint.ends_with('/') {
        endpoint.to_string()
    } else {
        format!("{}/", endpoint)
    }
}

// Re-export `httpdate` indirectly to avoid forcing it on consumers.
mod httpdate {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Parse a subset of HTTP-date formats (RFC 7231): IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`).
    /// Best-effort; if parsing fails returns Err.
    pub(crate) fn parse_http_date(value: &str) -> Result<SystemTime, ()> {
        // Use time crate to parse RFC 2822 / IMF-fixdate.
        let format = time::format_description::parse(
            "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT",
        )
        .map_err(|_| ())?;
        let dt = time::PrimitiveDateTime::parse(value, &format).map_err(|_| ())?;
        let secs = dt.assume_utc().unix_timestamp();
        if secs < 0 {
            return Err(());
        }
        Ok(UNIX_EPOCH + Duration::from_secs(secs as u64))
    }
}
