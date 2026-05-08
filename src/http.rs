use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use reqwest::header::HeaderMap;
use reqwest::Client as ReqwestClient;
use reqwest::StatusCode;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::error::{Error, Result};

/// Maximum payload size accepted by the ingestion gateway (1 MiB). Mirrors the JS SDK's
/// `MAX_INGEST_SIZE_BYTES` and the Python SDK's `max_ingest_size_bytes`. Oversized payloads
/// are silently dropped client-side; this matches the behavior of the other SDKs and avoids
/// a 413 on the gateway.
pub(crate) const MAX_INGEST_SIZE_BYTES: usize = 1024 * 1024;

/// Configuration for the retrying HTTP transport.
#[derive(Clone)]
pub(crate) struct TransportConfig {
    pub base_url: String,
    pub write_key: String,
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub jitter_fraction: f64,
    pub debug: bool,
}

/// Hooks for tests to override sleep / jitter without monkey-patching.
type SleepFn = Arc<dyn Fn(Duration) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;
type RandFn = Arc<dyn Fn() -> f64 + Send + Sync>;

#[derive(Clone)]
pub(crate) struct RetryingHttpClient {
    cfg: TransportConfig,
    http: ReqwestClient,
    sleep: Arc<Mutex<SleepFn>>,
    rand: Arc<Mutex<RandFn>>,
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
        }
    }

    pub(crate) async fn post_json<T: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<()> {
        let url = format!(
            "{}{}",
            self.cfg.base_url,
            path.strip_prefix('/').unwrap_or(path)
        );
        let payload = serde_json::to_vec(body)?;

        // Drop oversized payloads on the floor to match the Python and JS SDKs
        // (`MAX_INGEST_SIZE_BYTES` and Python's `max_ingest_size_bytes` of ~1 MiB).
        // The ingestion gateway enforces a similar cap and would return 413 otherwise.
        // Returning `Ok(())` here keeps SDK calls non-fatal — losing one payload is
        // strictly better than blocking the host application on a serialization disaster.
        // The warning is unconditional (NOT gated on `debug`) so production callers
        // without verbose logging still get a single line per drop and can detect
        // accidental oversize streams.
        if payload.len() > MAX_INGEST_SIZE_BYTES {
            tracing::warn!(
                path,
                bytes = payload.len(),
                max = MAX_INGEST_SIZE_BYTES,
                "raindrop: dropping oversized payload (> 1 MiB)"
            );
            return Ok(());
        }

        let mut last_err: Option<Error> = None;

        for attempt in 1..=self.cfg.max_attempts {
            if attempt > 1 {
                let delay = self.retry_delay(attempt - 1, last_err.as_ref()).await;
                if delay > Duration::ZERO {
                    let sleep = {
                        let guard = self.sleep.lock().await;
                        guard.clone()
                    };
                    sleep(delay).await;
                }
            }

            let mut req = self
                .http
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.cfg.write_key))
                .header("Content-Type", "application/json")
                .body(payload.clone());

            if self.cfg.debug {
                req = req.header("X-Raindrop-Sdk", "raindrop-rust");
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(err) => {
                    last_err = Some(Error::Http(err.to_string()));
                    continue;
                }
            };

            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }

            let retry_after = parse_retry_after(resp.headers());
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

            // Non-retryable client errors: don't retry, return immediately.
            if status.as_u16() < 500 && status != StatusCode::TOO_MANY_REQUESTS {
                return Err(err);
            }

            // Retryable: retain retry_after for the next iteration's backoff.
            last_err = Some(WithRetryAfter::wrap(err, retry_after));
        }

        Err(last_err.unwrap_or_else(|| Error::Http("unknown error".into())))
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
        format!("{endpoint}/")
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
