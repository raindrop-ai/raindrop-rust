use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use raindrop::{Client, ClientBuilder};

#[allow(dead_code)]
pub fn fast_client_builder(server: &MockServer) -> ClientBuilder {
    Client::builder()
        .write_key("rk_test")
        .endpoint(format!("{}/", server.uri()))
        .disable_local_workshop()
        .partial_flush_interval(Duration::ZERO)
        .trace_flush_interval(Duration::ZERO)
        .base_delay(Duration::from_millis(1))
        .jitter_fraction(0.0)
}

#[derive(Debug, Clone)]
pub struct Captured {
    #[allow(dead_code)]
    pub path: String,
    #[allow(dead_code)]
    pub body: Vec<u8>,
}

impl Captured {
    #[allow(dead_code)]
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("captured body is not valid json")
    }
}

#[derive(Default, Clone)]
pub struct Recorder {
    pub bodies: Arc<Mutex<Vec<Captured>>>,
    pub sequence: Arc<Mutex<Vec<ResponseTemplate>>>,
}

impl Recorder {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn push_response(self, response: ResponseTemplate) -> Self {
        self.sequence.lock().unwrap().push(response);
        self
    }

    #[allow(dead_code)]
    pub fn requests(&self) -> Vec<Captured> {
        self.bodies.lock().unwrap().clone()
    }

    #[allow(dead_code)]
    pub fn count(&self) -> usize {
        self.bodies.lock().unwrap().len()
    }
}

impl Respond for Recorder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let captured = Captured {
            path: request.url.path().to_string(),
            body: request.body.clone(),
        };
        self.bodies.lock().unwrap().push(captured);
        let mut seq = self.sequence.lock().unwrap();
        if !seq.is_empty() {
            seq.remove(0)
        } else {
            ResponseTemplate::new(204)
        }
    }
}

#[allow(dead_code)]
pub async fn mount_path(server: &MockServer, request_method: &str, request_path: &str) -> Recorder {
    let recorder = Recorder::new();
    let listener = recorder.clone();
    Mock::given(method(request_method))
        .and(path(request_path))
        .respond_with(listener)
        .mount(server)
        .await;
    recorder
}

#[allow(dead_code)]
pub async fn mount_any_post(server: &MockServer) -> Recorder {
    let recorder = Recorder::new();
    let listener = recorder.clone();
    Mock::given(method("POST"))
        .respond_with(listener)
        .mount(server)
        .await;
    recorder
}

#[allow(dead_code)]
pub fn json_get<'a>(value: &'a Value, p: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in p {
        match current.get(*key) {
            Some(next) => current = next,
            None => return None,
        }
    }
    Some(current)
}

/// Helper: extract a span attribute by key from an OTLP/JSON spans payload.
#[allow(dead_code)]
pub fn span_attr<'a>(span: &'a Value, key: &str) -> Option<&'a Value> {
    let attrs = span.get("attributes")?.as_array()?;
    for attr in attrs {
        if attr.get("key")?.as_str()? == key {
            return attr.get("value");
        }
    }
    None
}

/// Helper: extract the first span list out of an OTLP envelope JSON.
#[allow(dead_code)]
pub fn spans_of(payload: &Value) -> Vec<Value> {
    let resource_spans = payload
        .get("resourceSpans")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for rs in resource_spans {
        let scope_spans = rs
            .get("scopeSpans")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for ss in scope_spans {
            if let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) {
                out.extend(spans.iter().cloned());
            }
        }
    }
    out
}
