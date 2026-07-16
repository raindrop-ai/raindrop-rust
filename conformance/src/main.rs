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
//! Client configuration comes only from the environment:
//!
//! * `RAINDROP_SINK_URL`   — ingest base URL (the driver appends `/v1/`).
//! * `RAINDROP_WRITE_KEY`  — bearer write key.
//! * `RAINDROP_PROJECT_ID` — optional project slug.

use std::collections::BTreeMap;
use std::io::Read;
use std::process::ExitCode;
use std::time::Instant;

use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use raindrop::{
    AiEvent, Attachment, BeginOptions, Client, Event, FinishOptions, Interaction, PatchOptions,
    Signal, User,
};

const DRIVER_VERSION: &str = "1.0.0";
const PROTOCOL: u64 = 1;
const SDK_NAME: &str = "raindrop-rust";

/// Canonical capability keys (harness `capabilities.yaml`) this SDK supports,
/// derived honestly from the public API:
///
/// * `events.track`         — `Client::track_event` (plain, non-AI event).
/// * `events.track_ai`      — `Client::track_ai`.
/// * `events.track_partial` — `Client::begin` / `Interaction::patch` /
///                            `Interaction::finish`.
/// * `identify`             — `Client::identify`.
/// * `signal`               — `Client::track_signal` (DEV-1201): the step's
///                            event_id/name land on signals/track as
///                            event_id/signal_name with signal_type defaulting
///                            to "default".
const CAPABILITIES: &[&str] = &[
    "events.track",
    "events.track_ai",
    // Factually true delivery mode: track_event/track_ai ship begin-style to
    // events/track_partial (DEV-1149) — declaring it runs the wrap-*-partial
    // scenarios against the route this SDK actually uses.
    "events.track_ai_partial",
    "events.track_partial",
    // events.feature_flags (DEV-1214): the public event surfaces accept an
    // optional feature_flags map (string→string) that ships verbatim as the
    // top-level `feature_flags` wire key. NOTE: this SDK ships track_ai via
    // events/track_partial (not events/track), so the request-shape scenario —
    // which asserts the events/track route — is a route gap (DEV-1149),
    // ratcheted like every other events/track scenario; the wire shape itself
    // is proven by the SDK's unit tests.
    "events.feature_flags",
    "identify",
    "signal",
];
const NOT_APPLICABLE: &[&str] = &["wrapper.capture"];
// A not_applicable claim must argue "not fixable" (README policy).
// traces.otlp is deliberately neither declared nor not_applicable: the SDK
// ships OTLP spans, but the harness cannot drive trace emission yet — a
// visible gap tracked as DEV-1153.
const NOT_APPLICABLE_REASONS: &[(&str, &str)] = &[(
    "wrapper.capture",
    "core SDK driven by direct calls; a framework capture path cannot exist by design",
)];

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

/// Map the language-neutral `feature_flags` step arg onto the SDK's public
/// feature-flag surface (a string→string map). Non-string values are dropped:
/// the wire contract is `Record<string,string>`, so a driver must never
/// forward a non-string flag value.
fn feature_flags(args: &Map<String, Value>) -> BTreeMap<String, String> {
    match args.get("feature_flags").and_then(Value::as_object) {
        Some(map) => map
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
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
            "init" => self.step_init().map_err(StepError::from),
            "track" => self.step_track(&args).await.map_err(StepError::from),
            "track_ai" => self.step_track_ai(&args).await.map_err(StepError::from),
            "begin" => self.step_begin(&args).await.map_err(StepError::from),
            "patch" => self.step_patch(&args).await.map_err(StepError::from),
            "finish" => self.step_finish(&args).await.map_err(StepError::from),
            "identify" => self.step_identify(&args).await.map_err(StepError::from),
            "signal" => self.step_signal(&args).await.map_err(StepError::from),
            "flush" => self.step_flush().await.map_err(StepError::from),
            "close" => self.step_close().await.map_err(StepError::from),
            // Anything unknown takes the exit-3 unsupported path.
            other => Err(Unsupported(other.to_string()).into()),
        }
    }

    // -- lifecycle -------------------------------------------------------- //

    fn step_init(&mut self) -> Result<(), Failure> {
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
            user_id: required_str(args, "user_id", "track")?,
            event: required_str(args, "event", "track")?,
            timestamp: timestamp(args, "track")?,
            properties: properties(args, "properties"),
            attachments: attachments(args, "track")?,
            feature_flags: feature_flags(args),
        };
        self.client()?
            .track_event(event)
            .await
            .map_err(|e| Failure(format!("track: {e}")))
    }

    async fn step_track_ai(&self, args: &Map<String, Value>) -> Result<(), Failure> {
        let event = AiEvent {
            event_id: optional_str(args, "event_id"),
            user_id: required_str(args, "user_id", "track_ai")?,
            event: required_str(args, "event", "track_ai")?,
            timestamp: timestamp(args, "track_ai")?,
            input: optional_str(args, "input"),
            output: optional_str(args, "output"),
            model: optional_str(args, "model"),
            convo_id: optional_str(args, "convo_id"),
            properties: properties(args, "properties"),
            attachments: attachments(args, "track_ai")?,
            feature_flags: feature_flags(args),
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
            user_id: required_str(args, "user_id", "begin")?,
            event: required_str(args, "event", "begin")?,
            timestamp: timestamp(args, "begin")?,
            input: optional_str(args, "input"),
            model: optional_str(args, "model"),
            convo_id: optional_str(args, "convo_id"),
            properties: properties(args, "properties"),
            attachments: attachments(args, "begin")?,
            feature_flags: feature_flags(args),
        };
        let interaction = self.client()?.begin(opts).await;
        self.interaction = Some(interaction);
        Ok(())
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
            feature_flags: feature_flags(args),
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
            feature_flags: feature_flags(args),
        };
        interaction
            .finish(opts)
            .await
            .map_err(|e| Failure(format!("finish: {e}")))
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
    let mut raw = String::new();
    if let Err(e) = std::io::stdin().read_to_string(&mut raw) {
        eprintln!("driver: cannot read stdin: {e}");
        return ExitCode::from(1);
    }
    run_steps(&raw).await
}
