//! Raindrop AI observability SDK for Rust.
//!
//! Track AI events, user signals, and OTLP-style traces.
//!
//! # Quick start
//!
//! ```no_run
//! use raindrop::{AiEvent, Client};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder()
//!     .write_key("rk_...")
//!     .endpoint("https://api.raindrop.ai/v1/")
//!     .build()?;
//!
//! client
//!     .track_ai(AiEvent {
//!         user_id: "user-123".into(),
//!         event: "chat_message".into(),
//!         input: "What is the capital of France?".into(),
//!         output: "Paris".into(),
//!         model: "gpt-4o".into(),
//!         convo_id: "conv-123".into(),
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! client.close().await?;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

mod buffer;
mod client;
mod error;
mod events;
mod helpers;
mod http;
mod otlp;
mod signals;
mod trace_buffer;
mod traces;
mod users;

pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use events::{
    AiEvent, Attachment, BeginOptions, Event, FinishOptions, Interaction, PatchOptions,
};
pub use otlp::{Attribute, AttributeValue, SpanStatusCode};
pub use signals::Signal;
pub use traces::{
    with_tool, with_tool_async, Span, SpanOptions, ToolOptions, ToolSpan, Tracer, TrackToolOptions,
};
pub use users::User;

/// Default Raindrop ingestion endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://api.raindrop.ai/v1/";

/// SDK version, exposed for telemetry context.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) const DEFAULT_LIBRARY_NAME: &str = "raindrop-rust";
pub(crate) const DEFAULT_SERVICE_NAME: &str = "raindrop.rust-sdk";
pub(crate) const DEFAULT_EVENT_NAME: &str = "ai_generation";
