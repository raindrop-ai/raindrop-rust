use std::fmt;

/// SDK error type.
#[derive(Debug)]
pub enum Error {
    /// The client has been closed.
    Closed,

    /// HTTP transport error.
    Http(String),

    /// Non-success HTTP response from the Raindrop API.
    HttpStatus {
        /// Status code.
        status: u16,
        /// Body of the response (truncated to a reasonable length by the SDK).
        body: String,
    },

    /// JSON serialization failed.
    Json(serde_json::Error),

    /// Configuration error.
    Config(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("raindrop: client closed"),
            Self::Http(message) => write!(f, "raindrop: http error: {message}"),
            Self::HttpStatus { status, body } => write!(f, "raindrop: {status}: {body}"),
            Self::Json(err) => write!(f, "raindrop: json error: {err}"),
            Self::Config(message) => write!(f, "raindrop: config error: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(err) => Some(err),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Http(err.to_string())
    }
}

/// Convenience alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    #[test]
    fn json_errors_keep_the_original_source() {
        let original = serde_json::from_str::<bool>("[").unwrap_err();
        let message = original.to_string();
        let error = super::Error::from(original);
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<serde_json::Error>()
            .unwrap();
        assert_eq!(source.to_string(), message);
        assert_eq!(
            error.to_string(),
            format!("raindrop: json error: {message}")
        );
    }
}
