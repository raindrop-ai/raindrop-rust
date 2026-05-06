use serde::{Deserialize, Serialize};

use crate::helpers::{random_id_b64, stringify_value};

/// Span status code as defined by OpenTelemetry.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "u8", from = "u8")]
pub enum SpanStatusCode {
    /// Status not set.
    Unset = 0,
    /// Operation completed without error.
    Ok = 1,
    /// Operation completed with an error.
    Error = 2,
}

impl From<u8> for SpanStatusCode {
    fn from(value: u8) -> Self {
        match value {
            1 => SpanStatusCode::Ok,
            2 => SpanStatusCode::Error,
            _ => SpanStatusCode::Unset,
        }
    }
}

impl From<SpanStatusCode> for u8 {
    fn from(value: SpanStatusCode) -> Self {
        value as u8
    }
}

/// OTLP/JSON `AnyValue` representation. Matches the Go and JS SDK shape, where ints are
/// encoded as decimal strings.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct AttributeValue {
    /// String value.
    #[serde(skip_serializing_if = "Option::is_none", rename = "stringValue")]
    pub string_value: Option<String>,
    /// 64-bit integer encoded as a decimal string (per OTLP/JSON).
    #[serde(skip_serializing_if = "Option::is_none", rename = "intValue")]
    pub int_value: Option<String>,
    /// Double-precision float.
    #[serde(skip_serializing_if = "Option::is_none", rename = "doubleValue")]
    pub double_value: Option<f64>,
    /// Boolean.
    #[serde(skip_serializing_if = "Option::is_none", rename = "boolValue")]
    pub bool_value: Option<bool>,
    /// Array of values.
    #[serde(skip_serializing_if = "Option::is_none", rename = "arrayValue")]
    pub array_value: Option<OtlpArrayValue>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct OtlpArrayValue {
    pub values: Vec<AttributeValue>,
}

/// A typed key-value attribute on a span. Construct via the [`Attribute::string`],
/// [`Attribute::int`], [`Attribute::float`], [`Attribute::bool`], and
/// [`Attribute::string_array`] helpers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attribute {
    /// Attribute name.
    pub key: String,
    /// Attribute value.
    pub value: AttributeValue,
}

impl Attribute {
    /// Construct a string attribute.
    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue {
                string_value: Some(value.into()),
                ..Default::default()
            },
        }
    }

    /// Construct an integer attribute.
    pub fn int(key: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue {
                int_value: Some(value.to_string()),
                ..Default::default()
            },
        }
    }

    /// Construct a floating-point attribute.
    pub fn float(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue {
                double_value: Some(value),
                ..Default::default()
            },
        }
    }

    /// Construct a boolean attribute.
    pub fn bool(key: impl Into<String>, value: bool) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue {
                bool_value: Some(value),
                ..Default::default()
            },
        }
    }

    /// Construct a string array attribute.
    pub fn string_array(key: impl Into<String>, values: Vec<String>) -> Self {
        let inner = values
            .into_iter()
            .map(|v| AttributeValue {
                string_value: Some(v),
                ..Default::default()
            })
            .collect();
        Self {
            key: key.into(),
            value: AttributeValue {
                array_value: Some(OtlpArrayValue { values: inner }),
                ..Default::default()
            },
        }
    }

    /// Convert any JSON value to a string attribute.
    pub fn from_json(key: impl Into<String>, value: &serde_json::Value) -> Self {
        Self::string(key, stringify_value(value))
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpKeyValue {
    pub key: String,
    pub value: AttributeValue,
}

impl From<Attribute> for OtlpKeyValue {
    fn from(attr: Attribute) -> Self {
        OtlpKeyValue {
            key: attr.key,
            value: attr.value,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpStatus {
    pub code: u8,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub message: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpSpan {
    #[serde(rename = "traceId")]
    pub trace_id: String,
    #[serde(rename = "spanId")]
    pub span_id: String,
    #[serde(
        rename = "parentSpanId",
        skip_serializing_if = "String::is_empty",
        default
    )]
    pub parent_span_id: String,
    pub name: String,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "endTimeUnixNano")]
    pub end_time_unix_nano: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attributes: Vec<OtlpKeyValue>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub status: Option<OtlpStatus>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpScope {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpScopeSpans {
    pub scope: OtlpScope,
    pub spans: Vec<OtlpSpan>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpResource {
    pub attributes: Vec<OtlpKeyValue>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpResourceSpans {
    pub resource: OtlpResource,
    #[serde(rename = "scopeSpans")]
    pub scope_spans: Vec<OtlpScopeSpans>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct OtlpExportTraceServiceRequest {
    #[serde(rename = "resourceSpans")]
    pub resource_spans: Vec<OtlpResourceSpans>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct SpanIds {
    pub trace_id_b64: String,
    pub span_id_b64: String,
    pub parent_span_id_b64: Option<String>,
}

pub(crate) fn create_span_ids(parent: Option<&SpanIds>) -> SpanIds {
    let trace_id = match parent {
        Some(p) => p.trace_id_b64.clone(),
        None => random_id_b64(16),
    };
    let span_id = random_id_b64(8);
    SpanIds {
        trace_id_b64: trace_id,
        span_id_b64: span_id,
        parent_span_id_b64: parent.map(|p| p.span_id_b64.clone()),
    }
}

pub(crate) fn build_export_request(
    spans: Vec<OtlpSpan>,
    service_name: &str,
    service_version: &str,
) -> OtlpExportTraceServiceRequest {
    OtlpExportTraceServiceRequest {
        resource_spans: vec![OtlpResourceSpans {
            resource: OtlpResource {
                attributes: vec![
                    OtlpKeyValue {
                        key: "service.name".into(),
                        value: AttributeValue {
                            string_value: Some(service_name.to_string()),
                            ..Default::default()
                        },
                    },
                    OtlpKeyValue {
                        key: "service.version".into(),
                        value: AttributeValue {
                            string_value: Some(service_version.to_string()),
                            ..Default::default()
                        },
                    },
                ],
            },
            scope_spans: vec![OtlpScopeSpans {
                scope: OtlpScope {
                    name: service_name.to_string(),
                    version: service_version.to_string(),
                },
                spans,
            }],
        }],
    }
}
