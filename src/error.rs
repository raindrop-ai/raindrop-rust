use thiserror::Error;

/// SDK error type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// `flush()` returned with telemetry enqueued before it not yet handed to
    /// ingest: its deadline passed, or retries ran out (CONTRACT 1.2.1).
    /// The SDK has already logged a warning; this is safe to ignore, and
    /// ignoring it changes nothing. Payloads that were never sent stay queued
    /// for a later send; `dropped` of them were lost (a request cut off
    /// mid-flight, which may already have reached ingest, is not resent).
    #[error("raindrop: {count} telemetry item(s) not delivered ({dropped} dropped)")]
    Undelivered {
        /// Items not handed to ingest when the call returned.
        count: usize,
        /// Of those, items dropped rather than kept queued.
        dropped: usize,
    },

    /// The client has been closed.
    #[error("raindrop: client closed")]
    Closed,

    /// HTTP transport error.
    #[error("raindrop: http error: {0}")]
    Http(String),

    /// Non-success HTTP response from the Raindrop API.
    #[error("raindrop: {status}: {body}")]
    HttpStatus {
        /// Status code.
        status: u16,
        /// Body of the response (truncated to a reasonable length by the SDK).
        body: String,
    },

    /// JSON serialization failed.
    #[error("raindrop: json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration error.
    #[error("raindrop: config error: {0}")]
    Config(String),
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Http(err.to_string())
    }
}

/// Convenience alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
