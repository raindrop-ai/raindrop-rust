use thiserror::Error;

/// SDK error type.
#[derive(Debug, Error)]
pub enum Error {
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
