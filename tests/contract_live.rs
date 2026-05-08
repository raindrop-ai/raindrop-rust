//! Tests for `contract::v1::live`.
//!
//! Locks down the `/v1/live` body shape and the validation rules Workshop
//! enforces (canonical event types + spanId on tool events).

use raindrop::contract::v1::live::{
    validate_live_event, LiveEvent, LiveEventType, LiveEventValidationError, LIVE_EVENT_TYPES,
};

#[test]
fn live_event_types_match_canonical_workshop_set() {
    assert_eq!(
        LIVE_EVENT_TYPES,
        &[
            "text_delta",
            "reasoning_delta",
            "tool_start",
            "tool_result",
            "status",
        ]
    );
}

#[test]
fn live_event_type_round_trips_to_wire_string() {
    assert_eq!(LiveEventType::TextDelta.as_wire_str(), "text_delta");
    assert_eq!(
        LiveEventType::ReasoningDelta.as_wire_str(),
        "reasoning_delta"
    );
    assert_eq!(LiveEventType::ToolStart.as_wire_str(), "tool_start");
    assert_eq!(LiveEventType::ToolResult.as_wire_str(), "tool_result");
    assert_eq!(LiveEventType::Status.as_wire_str(), "status");
}

#[test]
fn live_event_serializes_with_camel_case_trace_id_and_span_id() {
    let evt = LiveEvent {
        trace_id: "8351249bb0a5be11bd049b4a17a2bb64".into(),
        span_id: Some("94df3ca0dc0ae7da".into()),
        r#type: "tool_start".into(),
        content: Some("search_docs".into()),
        timestamp: Some(1778181165922),
        metadata: None,
    };
    let json = serde_json::to_value(&evt).unwrap();
    assert_eq!(json["traceId"], "8351249bb0a5be11bd049b4a17a2bb64");
    assert_eq!(json["spanId"], "94df3ca0dc0ae7da");
    assert_eq!(json["type"], "tool_start");
    assert_eq!(json["content"], "search_docs");
    assert_eq!(json["timestamp"], 1778181165922i64);
    // snake_case keys MUST NOT appear on the wire (live endpoint predates
    // snake_case track endpoints).
    assert!(json.get("trace_id").is_none());
    assert!(json.get("span_id").is_none());
}

#[test]
fn live_event_skips_serializing_optional_none_fields() {
    let evt = LiveEvent {
        trace_id: "a".into(),
        span_id: None,
        r#type: "status".into(),
        content: None,
        timestamp: None,
        metadata: None,
    };
    let json = serde_json::to_string(&evt).unwrap();
    assert!(
        !json.contains("spanId"),
        "spanId must be skipped when None, got {}",
        json
    );
    assert!(!json.contains("content"));
    assert!(!json.contains("timestamp"));
    assert!(!json.contains("metadata"));
}

#[test]
fn validate_live_event_rejects_tool_start_without_span_id() {
    let evt = LiveEvent {
        trace_id: "a".into(),
        span_id: None,
        r#type: "tool_start".into(),
        content: None,
        timestamp: None,
        metadata: None,
    };
    assert_eq!(
        validate_live_event(&evt),
        Err(LiveEventValidationError::MissingSpanIdOnToolEvent)
    );
}

#[test]
fn validate_live_event_rejects_tool_result_without_span_id() {
    let evt = LiveEvent {
        trace_id: "a".into(),
        span_id: None,
        r#type: "tool_result".into(),
        content: None,
        timestamp: None,
        metadata: None,
    };
    assert_eq!(
        validate_live_event(&evt),
        Err(LiveEventValidationError::MissingSpanIdOnToolEvent)
    );
}

#[test]
fn validate_live_event_accepts_tool_start_with_span_id() {
    let evt = LiveEvent {
        trace_id: "a".into(),
        span_id: Some("s".into()),
        r#type: "tool_start".into(),
        content: None,
        timestamp: None,
        metadata: None,
    };
    assert_eq!(validate_live_event(&evt), Ok(()));
}

#[test]
fn validate_live_event_accepts_text_delta_without_span_id() {
    let evt = LiveEvent {
        trace_id: "a".into(),
        span_id: None,
        r#type: "text_delta".into(),
        content: Some("hello".into()),
        timestamp: None,
        metadata: None,
    };
    assert_eq!(validate_live_event(&evt), Ok(()));
}

#[test]
fn validate_live_event_accepts_unknown_event_type_without_span_id() {
    // Forward-compat: Workshop will accept future types we haven't added yet.
    let evt = LiveEvent {
        trace_id: "a".into(),
        span_id: None,
        r#type: "future_event".into(),
        content: None,
        timestamp: None,
        metadata: None,
    };
    assert_eq!(validate_live_event(&evt), Ok(()));
}
