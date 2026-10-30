//! Conformance driver for the raindrop-sdk-harness (DEV-1145).
//!
//! A thin CLI that maps the harness's language-neutral step vocabulary onto
//! the *public* Raindrop Rust SDK API — no internals, no test hooks. It speaks
//! driver protocol 1 (harness README, "Step vocabulary & driver protocol"):
//!
//! * `--describe` prints a JSON handshake object and exits 0.
//! * Otherwise it reads a JSON array of steps from stdin, executes them in
//!   order against a client configured *only* from the environment, prints one
//!   `{"step": <n>, "ms": <elapsed>}` timing line per completed step, and
//!   exits 0 on success.
//! * An unsupported step prints `unsupported:<step>` as the last stdout line
//!   and exits 3.
//!
//! Transport configuration comes only from the environment:
//!
//! * `RAINDROP_SINK_URL`   — ingest base URL (the driver appends `/v1/`).
//! * `RAINDROP_WRITE_KEY`  — bearer write key.
//! * `RAINDROP_PROJECT_ID` — optional project slug.
//!
//! The `init.app_git` step argument is mapped through the public
//! `ClientBuilder::app_git` / `disable_app_git` API.

use std::collections::BTreeMap;
use std::io::Read;
use std::process::ExitCode;
use std::time::Instant;

use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use raindrop::{
    AiEvent, AppGitConfig, Attachment, BeginOptions, Client, ClientBuilder, Event, FinishOptions,
    Interaction, PatchOptions, ResumeOptions, ResumeSubagentOptions, Signal, Span, SubagentOptions,
    ToolOptions, TrackToolOptions, User,
};

const DRIVER_VERSION: &str = "1.1.0";
const PROTOCOL: u64 = 1;
const SDK_NAME: &str = "raindrop-rust";

/// Canonical capability keys (harness `capabilities.yaml`) this SDK supports,
/// derived honestly from the public API:
///
/// * `events.track`         — `Client::track_event` (plain, non-AI event),
///   queued and batched to events/track.
/// * `events.track_ai`      — `Client::track_ai`, queued and batched to
///   events/track.
/// * `events.track_partial` — `Client::begin` / `Interaction::patch` /
///   `Interaction::finish`.
/// * `events.resume`        — `Client::resume_interaction_with`.
/// * `events.optional_user_id` — user ids are plain strings the caller may
///   leave empty; the SDK then does not send the event.
/// * `identify`             — `Client::identify`.
/// * `signal`               — `Client::track_signal` (DEV-1201): the step's
///   event_id/name land on signals/track as event_id/signal_name with
///   signal_type defaulting to "default".
/// * `logs.capture`         — SDK `tracing` records go to
///   `RAINDROP_HARNESS_LOG` (CONTRACT 14).
/// * `traces.retroactive_span` / `traces.span_parent` —
///   `Interaction::track_tool` with `Span::from_saved_ids` as the parent.
/// * `subagents.detached_dispatch` / `subagents.detached_child` —
///   `Interaction::subagent` and `Client::resume_subagent`.
const CAPABILITIES: &[&str] = &[
    "app_git.config",
    "app_git.environment",
    "app_git.contextual_auto_detection",
    "events.track",
    "events.track_ai",
    "events.track_partial",
    "events.resume",
    "events.optional_user_id",
    "identify",
    "signal",
    "logs.capture",
    "traces.retroactive_span",
    "traces.span_parent",
    "subagents.detached_dispatch",
    "subagents.detached_child",
];
const NOT_APPLICABLE: &[&str] = &["wrapper.capture", "events.track_ai_partial"];
// A not_applicable claim must argue "not fixable" (README policy).
const NOT_APPLICABLE_REASONS: &[(&str, &str)] = &[
    (
        "wrapper.capture",
        "core SDK driven by direct calls; a framework capture path cannot exist by design",
    ),
    (
        "events.track_ai_partial",
        "track_ai delivers via the ratified events/track batch route; the alternate partial-routing declaration cannot apply to it",
    ),
];

/// Reserved harness arg handled by the runner (per-step timing bound), never a
/// payload field, so the driver strips it before mapping args onto SDK calls.
const RESERVED_ARGS: &[&str] = &["max_ms"];

fn describe() -> Value {
    json!({
        "sdk_name": SDK_NAME,
        "sdk_version": raindrop::VERSION,
        "driver_version": DRIVER_VERSION,
        "protocol": PROTOCOL,
        "capabilities": CAPABILITIES,
        "not_applicable": NOT_APPLICABLE,
        "not_applicable_reasons": NOT_APPLICABLE_REASONS
            .iter()
            .cloned()
            .collect::<std::collections::BTreeMap<_, _>>(),
    })
}

/// A step the driver cannot execute — the exit-3 `unsupported:<step>` path.
struct Unsupported(String);

/// A driver/scenario malfunction — any other nonzero exit.
struct Failure(String);

enum StepError {
    Unsupported(Unsupported),
    Failure(Failure),
}

impl From<Unsupported> for StepError {
    fn from(u: Unsupported) -> Self {
        StepError::Unsupported(u)
    }
}

impl From<Failure> for StepError {
    fn from(f: Failure) -> Self {
        StepError::Failure(f)
    }
}

/// Drop reserved args and treat an explicit JSON `null` as omitted.
///
/// Per the driver protocol, `null` on an optional arg is equivalent to the
/// key being absent and must never be forwarded to an SDK call. Unknown keys
/// are kept (and simply never read): drivers MUST ignore unknown keys per the
/// harness parse-tolerance rule.
fn clean(args: &Value) -> Map<String, Value> {
    match args.as_object() {
        Some(map) => map
            .iter()
            .filter(|(k, v)| !RESERVED_ARGS.contains(&k.as_str()) && !v.is_null())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => Map::new(),
    }
}

fn required_str(args: &Map<String, Value>, key: &str, step: &str) -> Result<String, Failure> {
    match args.get(key).and_then(Value::as_str) {
        Some(s) => Ok(s.to_string()),
        None => Err(Failure(format!(
            "step {step}: missing or non-string required arg `{key}`"
        ))),
    }
}

fn optional_str(args: &Map<String, Value>, key: &str) -> String {
    args.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn properties(args: &Map<String, Value>, key: &str) -> BTreeMap<String, Value> {
    match args.get(key).and_then(Value::as_object) {
        Some(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        None => BTreeMap::new(),
    }
}

fn timestamp(args: &Map<String, Value>, step: &str) -> Result<Option<OffsetDateTime>, Failure> {
    match args.get("timestamp").and_then(Value::as_str) {
        Some(raw) => OffsetDateTime::parse(raw, &Rfc3339).map(Some).map_err(|e| {
            Failure(format!(
                "step {step}: cannot parse timestamp {raw:?} as RFC 3339: {e}"
            ))
        }),
        None => Ok(None),
    }
}

fn attachments(args: &Map<String, Value>, step: &str) -> Result<Vec<Attachment>, Failure> {
    let raw = match args.get("attachments") {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let items = raw
        .as_array()
        .ok_or_else(|| Failure(format!("step {step}: `attachments` is not an array")))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = item
            .as_object()
            .ok_or_else(|| Failure(format!("step {step}: attachment item is not an object")))?;
        let get = |key: &str| {
            obj.get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        // `type`, `role`, and `value` are required by the step schema; the
        // remaining keys are optional. Additional forward-compat keys have no
        // representation in the public `Attachment` struct and are dropped.
        out.push(Attachment {
            kind: get("type"),
            role: get("role"),
            value: get("value"),
            attachment_id: get("attachment_id"),
            name: get("name"),
            language: get("language"),
        });
    }
    Ok(out)
}

/// Owns the single SDK client and (at most one) open interaction binding.
#[derive(Default)]
struct Driver {
    client: Option<Client>,
    interaction: Option<Interaction>,
    /// Carriers from `dispatch_subagent`, by child event id.
    carriers: BTreeMap<String, BTreeMap<String, String>>,
}

impl Driver {
    fn client(&self) -> Result<&Client, Failure> {
        self.client
            .as_ref()
            .ok_or_else(|| Failure("step executed before init".into()))
    }

    async fn execute(&mut self, name: &str, args: &Value) -> Result<(), StepError> {
        let args = clean(args);
        match name {
            "init" => self.step_init(&args).map_err(StepError::from),
            "track" => self.step_track(&args).await.map_err(StepError::from),
            "track_ai" => self.step_track_ai(&args).await.map_err(StepError::from),
            "begin" => self.step_begin(&args).await.map_err(StepError::from),
            "patch" => self.step_patch(&args).await.map_err(StepError::from),
            "finish" => self.step_finish(&args).await.map_err(StepError::from),
            "resume" => self.step_resume(&args).map_err(StepError::from),
            "track_tool" => self.step_track_tool(&args).map_err(StepError::from),
            "dispatch_subagent" => self.step_dispatch_subagent(&args).map_err(StepError::from),
            "run_subagent" => self.step_run_subagent(&args).await.map_err(StepError::from),
            "identify" => self.step_identify(&args).await.map_err(StepError::from),
            "signal" => self.step_signal(&args).await.map_err(StepError::from),
            "flush" => self.step_flush().await.map_err(StepError::from),
            "close" => self.step_close().await.map_err(StepError::from),
            // Anything unknown takes the exit-3 unsupported path.
            other => Err(Unsupported(other.to_string()).into()),
        }
    }

    // -- lifecycle -------------------------------------------------------- //

    fn step_init(&mut self, args: &Map<String, Value>) -> Result<(), Failure> {
        let sink_url = std::env::var("RAINDROP_SINK_URL").unwrap_or_default();
        let sink_url = sink_url.trim().trim_end_matches('/').to_string();
        // A missing sink must never fall through to the SDK's production
        // default: a conformance run pointed at prod would ship test traffic
        // with a real-looking bearer key. Hard config error instead.
        if sink_url.is_empty() {
            return Err(Failure(
                "RAINDROP_SINK_URL is required: refusing to run against the SDK's default production endpoint".to_string(),
            ));
        }
        let write_key = std::env::var("RAINDROP_WRITE_KEY").unwrap_or_default();
        let write_key = write_key.trim().to_string();
        // An empty write key + disable_local_workshop() yields a disabled
        // client: every step "succeeds" while nothing ships — a silent
        // no-op run that would record zero requests. Hard config error.
        if write_key.is_empty() {
            return Err(Failure(
                "RAINDROP_WRITE_KEY is required: an empty key builds a disabled client and the run becomes a silent no-op".to_string(),
            ));
        }

        let mut builder = Client::builder()
            .write_key(write_key)
            // The driver must talk only to the harness-provided sink; never
            // mirror to a developer-machine Workshop daemon (env var or TCP
            // probe) during a conformance run.
            .disable_local_workshop();
        builder = builder.endpoint(format!("{sink_url}/v1/"));
        if let Ok(project_id) = std::env::var("RAINDROP_PROJECT_ID") {
            if !project_id.trim().is_empty() {
                builder = builder.project_id(project_id);
            }
        }
        builder = configure_app_git(builder, args)?;
        let client = builder
            .build()
            .map_err(|e| Failure(format!("init: cannot build client: {e}")))?;
        self.client = Some(client);
        Ok(())
    }

    async fn step_flush(&self) -> Result<(), Failure> {
        self.client()?
            .flush()
            .await
            .map_err(|e| Failure(format!("flush: {e}")))
    }

    async fn step_close(&self) -> Result<(), Failure> {
        self.client()?
            .close()
            .await
            .map_err(|e| Failure(format!("close: {e}")))
    }

    // -- events ----------------------------------------------------------- //

    async fn step_track(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let event = Event {
            event_id: optional_str(args, "event_id"),
            user_id: optional_str(args, "user_id"),
            event: required_str(args, "event", "track")?,
            timestamp: timestamp(args, "track")?,
            properties: properties(args, "properties"),
            attachments: attachments(args, "track")?,
        };
        self.client()?
            .track_event(event)
            .await
            .map_err(|e| Failure(format!("track: {e}")))
    }

    async fn step_track_ai(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let event = AiEvent {
            event_id: optional_str(args, "event_id"),
            user_id: optional_str(args, "user_id"),
            event: required_str(args, "event", "track_ai")?,
            timestamp: timestamp(args, "track_ai")?,
            input: optional_str(args, "input"),
            output: optional_str(args, "output"),
            model: optional_str(args, "model"),
            convo_id: optional_str(args, "convo_id"),
            properties: properties(args, "properties"),
            attachments: attachments(args, "track_ai")?,
        };
        self.client()?
            .track_ai(event)
            .await
            .map_err(|e| Failure(format!("track_ai: {e}")))
    }

    async fn step_identify(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let user = User {
            user_id: required_str(args, "user_id", "identify")?,
            traits: properties(args, "traits"),
        };
        self.client()?
            .identify(user)
            .await
            .map_err(|e| Failure(format!("identify: {e}")))
    }

    async fn step_signal(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let signal = Signal {
            event_id: required_str(args, "event_id", "signal")?,
            name: required_str(args, "name", "signal")?,
            kind: optional_str(args, "signal_type"),
            sentiment: optional_str(args, "sentiment"),
            timestamp: timestamp(args, "signal")?,
            properties: properties(args, "properties"),
            attachment_id: optional_str(args, "attachment_id"),
            comment: optional_str(args, "comment"),
            after: optional_str(args, "after"),
        };
        self.client()?
            .track_signal(signal)
            .await
            .map_err(|e| Failure(format!("signal: {e}")))
    }

    // -- partial (begin/patch/finish) lifecycle ---------------------------- //

    async fn step_begin(&mut self, args: &Map<String, Value>) -> Result<(), Failure> {
        if self.interaction.is_some() {
            return Err(Failure(
                "begin: an interaction is already open (single-binding rule)".into(),
            ));
        }
        let opts = BeginOptions {
            event_id: optional_str(args, "event_id"),
            user_id: optional_str(args, "user_id"),
            event: required_str(args, "event", "begin")?,
            timestamp: timestamp(args, "begin")?,
            input: optional_str(args, "input"),
            model: optional_str(args, "model"),
            convo_id: optional_str(args, "convo_id"),
            properties: properties(args, "properties"),
            attachments: attachments(args, "begin")?,
        };
        let interaction = self.client()?.begin(opts).await;
        self.interaction = Some(interaction);
        Ok(())
    }

    /// CONTRACT 9.2: continue an event begun elsewhere by id. Nothing is
    /// sent now; identity the step supplies rides on the next update.
    fn step_resume(&mut self, args: &Map<String, Value>) -> Result<(), Failure> {
        if self.interaction.is_some() {
            return Err(Failure(
                "resume: an interaction is already open (single-binding rule)".into(),
            ));
        }
        let event_id = required_str(args, "event_id", "resume")?;
        let identity = ResumeOptions {
            user_id: optional_str(args, "user_id"),
            convo_id: optional_str(args, "convo_id"),
            event: optional_str(args, "event"),
        };
        let interaction = self.client()?.resume_interaction_with(event_id, identity);
        self.interaction = Some(interaction);
        Ok(())
    }

    /// CONTRACT 12: a tool span recorded after the fact on the open event.
    fn step_track_tool(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let interaction = self
            .interaction
            .as_ref()
            .ok_or_else(|| Failure("track_tool: no open interaction".into()))?;
        let time = |key: &str| -> Result<Option<OffsetDateTime>, Failure> {
            match args.get(key).and_then(Value::as_str) {
                Some(raw) => OffsetDateTime::parse(raw, &Rfc3339)
                    .map(Some)
                    .map_err(|e| Failure(format!("track_tool: cannot parse {key} {raw:?}: {e}"))),
                None => Ok(None),
            }
        };
        let parent = match args.get("parent").and_then(Value::as_object) {
            Some(p) => {
                let trace_id = p
                    .get("trace_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let span_id = p.get("span_id").and_then(Value::as_str).unwrap_or_default();
                Some(Span::from_saved_ids(trace_id, span_id).ok_or_else(|| {
                    Failure("track_tool: parent ids are not valid lowercase hex".into())
                })?)
            }
            None => None,
        };
        let duration = args
            .get("duration_ms")
            .and_then(Value::as_u64)
            .map(std::time::Duration::from_millis);
        interaction.track_tool(TrackToolOptions {
            name: required_str(args, "name", "track_tool")?,
            parent,
            input: args.get("input").cloned(),
            output: args.get("output").cloned(),
            error: args
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_string),
            properties: BTreeMap::new(),
            start_time: time("start_time")?,
            end_time: time("end_time")?,
            duration,
        });
        Ok(())
    }

    /// CONTRACT 13.1: launch a detached sub-agent from the open event; the
    /// carrier is kept for a later `run_subagent`.
    fn step_dispatch_subagent(&mut self, args: &Map<String, Value>) -> Result<(), Failure> {
        let interaction = self
            .interaction
            .as_ref()
            .ok_or_else(|| Failure("dispatch_subagent: no open interaction".into()))?;
        let dispatch = interaction.subagent(SubagentOptions {
            name: required_str(args, "name", "dispatch_subagent")?,
            child_event_id: required_str(args, "child_event_id", "dispatch_subagent")?,
            input: args.get("input").cloned(),
            ..Default::default()
        });
        self.carriers
            .insert(dispatch.child_event_id, dispatch.headers);
        Ok(())
    }

    /// CONTRACT 13.2: run as a detached sub-agent from a carrier, reporting
    /// the child's own event.
    async fn step_run_subagent(&mut self, args: &Map<String, Value>) -> Result<(), Failure> {
        if self.interaction.is_some() {
            return Err(Failure(
                "run_subagent: an interaction is already open (single-binding rule)".into(),
            ));
        }
        let headers: BTreeMap<String, String> = match (
            args.get("carrier_from").and_then(Value::as_str),
            args.get("headers").and_then(Value::as_object),
        ) {
            (Some(child), None) => self.carriers.get(child).cloned().ok_or_else(|| {
                Failure(format!(
                    "run_subagent: no dispatch for child event {child:?}"
                ))
            })?,
            (None, Some(map)) => map
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect(),
            _ => {
                return Err(Failure(
                    "run_subagent: exactly one of `carrier_from` and `headers` is required".into(),
                ))
            }
        };
        let output = required_str(args, "output", "run_subagent")?;
        let client = self.client()?;
        let run = client
            .resume_subagent(
                &headers,
                ResumeSubagentOptions {
                    user_id: optional_str(args, "user_id"),
                    ..Default::default()
                },
            )
            .await;
        if let Some(tool_name) = args.get("tool_name").and_then(Value::as_str) {
            let tool = run.start_tool_span(tool_name, ToolOptions::default());
            tool.end();
        }
        run.finish(FinishOptions {
            output,
            ..Default::default()
        })
        .await
        .map_err(|e| Failure(format!("run_subagent: {e}")))
    }

    async fn step_patch(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let interaction = self
            .interaction
            .as_ref()
            .ok_or_else(|| Failure("patch: no open interaction".into()))?;
        let opts = PatchOptions {
            user_id: optional_str(args, "user_id"),
            event: optional_str(args, "event"),
            timestamp: timestamp(args, "patch")?,
            input: optional_str(args, "input"),
            output: optional_str(args, "output"),
            model: optional_str(args, "model"),
            convo_id: optional_str(args, "convo_id"),
            properties: properties(args, "properties"),
            attachments: attachments(args, "patch")?,
            is_pending: None,
        };
        interaction
            .patch(opts)
            .await
            .map_err(|e| Failure(format!("patch: {e}")))
    }

    async fn step_finish(&mut self, args: &Map<String, Value>) -> Result<(), Failure> {
        let interaction = self
            .interaction
            .take()
            .ok_or_else(|| Failure("finish: no open interaction".into()))?;
        let opts = FinishOptions {
            timestamp: timestamp(args, "finish")?,
            output: optional_str(args, "output"),
            model: optional_str(args, "model"),
            properties: properties(args, "properties"),
            attachments: attachments(args, "finish")?,
        };
        interaction
            .finish(opts)
            .await
            .map_err(|e| Failure(format!("finish: {e}")))
    }
}

fn configure_app_git(
    builder: ClientBuilder,
    args: &Map<String, Value>,
) -> Result<ClientBuilder, Failure> {
    let Some(value) = args.get("app_git") else {
        return Ok(builder);
    };
    if value == &Value::Bool(false) {
        return Ok(builder.disable_app_git());
    }
    let object = value
        .as_object()
        .ok_or_else(|| Failure("init: `app_git` must be false or an object".to_string()))?;
    const KNOWN: &[&str] = &[
        "commit_sha",
        "commit_dirty",
        "branch",
        "source_directory",
        "detect_branch",
        "auto_detect",
    ];
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !KNOWN.contains(key))
        .collect();
    if !unknown.is_empty() {
        return Err(Failure(format!(
            "init: unknown app_git options: {}",
            unknown.join(", ")
        )));
    }

    let mut config = AppGitConfig::new();
    for key in ["commit_sha", "branch", "source_directory"] {
        if let Some(value) = object.get(key) {
            let value = value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    Failure(format!("init: app_git.{key} must be a non-empty string"))
                })?;
            config = match key {
                "commit_sha" => config.commit_sha(value),
                "branch" => config.branch(value),
                "source_directory" => config.source_directory(value),
                _ => unreachable!(),
            };
        }
    }
    for key in ["commit_dirty", "detect_branch", "auto_detect"] {
        if let Some(value) = object.get(key) {
            let value = value
                .as_bool()
                .ok_or_else(|| Failure(format!("init: app_git.{key} must be a boolean")))?;
            config = match key {
                "commit_dirty" => config.commit_dirty(value),
                "detect_branch" => config.detect_branch(value),
                "auto_detect" => config.auto_detect(value),
                _ => unreachable!(),
            };
        }
    }
    Ok(builder.app_git(config))
}

/// SDK log records as JSON lines for the runner (CONTRACT 14): one
/// `{"level", "message"}` object per `tracing` event, appended to
/// `RAINDROP_HARNESS_LOG`.
struct HarnessLog {
    path: String,
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: Vec<String>,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields.push(format!("{}={value:?}", field.name()));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.push(format!("{}={value}", field.name()));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for HarnessLog {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let level = match *event.metadata().level() {
            tracing::Level::ERROR => "error",
            tracing::Level::WARN => "warn",
            tracing::Level::INFO => "info",
            _ => "debug",
        };
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let mut message = visitor.message;
        if !visitor.fields.is_empty() {
            message = format!("{message} ({})", visitor.fields.join(", "));
        }
        let line = json!({ "level": level, "message": message }).to_string();
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            use std::io::Write;
            let _ = writeln!(file, "{line}");
        }
    }
}

fn capture_sdk_logs() {
    use tracing_subscriber::layer::SubscriberExt;
    if let Ok(path) = std::env::var("RAINDROP_HARNESS_LOG") {
        if !path.trim().is_empty() {
            let subscriber = tracing_subscriber::registry().with(HarnessLog { path });
            let _ = tracing::subscriber::set_global_default(subscriber);
        }
    }
}

async fn run_steps(raw: &str) -> ExitCode {
    let steps: Vec<Value> = match serde_json::from_str(raw) {
        Ok(Value::Array(steps)) => steps,
        Ok(_) => {
            eprintln!("driver: stdin is valid JSON but not an array of steps");
            return ExitCode::from(1);
        }
        Err(e) => {
            eprintln!("driver: cannot parse steps from stdin: {e}");
            return ExitCode::from(1);
        }
    };

    let mut driver = Driver::default();
    for (index, step) in steps.iter().enumerate() {
        let (name, args) =
            match step
                .as_object()
                .and_then(|m| if m.len() == 1 { m.iter().next() } else { None })
            {
                Some((name, args)) => (name.clone(), args.clone()),
                None => {
                    eprintln!("driver: step {index} is not a single-key object");
                    return ExitCode::from(1);
                }
            };
        let start = Instant::now();
        match driver.execute(&name, &args).await {
            Ok(()) => {}
            Err(StepError::Unsupported(Unsupported(step_name))) => {
                println!("unsupported:{step_name}");
                return ExitCode::from(3);
            }
            Err(StepError::Failure(Failure(message))) => {
                eprintln!("driver: step {index} ({name}) failed: {message}");
                return ExitCode::from(1);
            }
        }
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        println!("{}", json!({ "step": index, "ms": ms }));
    }

    if driver.interaction.is_some() {
        eprintln!("driver: steps ended with an interaction still open (invalid scenario)");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.iter().any(|a| a == "--describe") {
        println!("{}", describe());
        return ExitCode::SUCCESS;
    }
    capture_sdk_logs();
    let mut raw = String::new();
    if let Err(e) = std::io::stdin().read_to_string(&mut raw) {
        eprintln!("driver: cannot read stdin: {e}");
        return ExitCode::from(1);
    }
    run_steps(&raw).await
}
