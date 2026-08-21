//! Schema conformance tests.
//!
//! Validates that:
//! - the canonical schema `docs/schemas/anp-task-v1.schema.json` accepts the
//!   example fixtures;
//! - Rust DTO serialization satisfies the schema (canonical contract);
//! - schema rejects unknown profile, negative/zero seq, terminal without
//!   final=true, resume without after_seq.

use std::path::Path;

use jsonschema::Validator;

use crate::dto::*;
use crate::PROFILE_ID;

fn schema() -> Validator {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("docs/schemas/anp-task-v1.schema.json");
    let raw = std::fs::read_to_string(&path).expect("schema file must exist");
    let schema: serde_json::Value = serde_json::from_str(&raw).expect("schema must be valid JSON");
    jsonschema::validator_for(&schema).expect("schema must compile")
}

fn errors(v: &Validator, instance: &serde_json::Value) -> Vec<String> {
    v.iter_errors(instance).map(|e| e.to_string()).collect()
}

fn assert_valid(v: &Validator, instance: &serde_json::Value) {
    let errs = errors(v, instance);
    assert!(errs.is_empty(), "expected valid, got: {errs:?}");
}

fn assert_invalid(v: &Validator, instance: &serde_json::Value) {
    assert!(
        !errors(v, instance).is_empty(),
        "expected invalid, but instance passed: {instance}"
    );
}

fn env() -> MessageEnvelope {
    MessageEnvelope::new(
        TaskId("t-0001".into()),
        OperationId("op-0001".into()),
        MessageId("m-0001".into()),
    )
}

#[test]
fn example_fixtures_are_valid() {
    let v = schema();
    for name in [
        "task-invoke.json",
        "task-progress.json",
        "task-completed.json",
        "task-resume.json",
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join(format!("examples/anp/{name}"));
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let value: serde_json::Value =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{name} json: {e}"));
        assert_valid(&v, &value);
    }
}

#[test]
fn dto_invoke_serialization_passes_schema() {
    let v = schema();
    let msg = TaskEvent::TaskInvoke(TaskInvoke {
        envelope: env(),
        agent: "assistant".into(),
        payload: serde_json::json!({"input": [{"type": "text", "text": "hello"}]}),
    });
    let value = serde_json::to_value(&msg).unwrap();
    assert_valid(&v, &value);
}

#[test]
fn dto_progress_serialization_passes_schema() {
    let v = schema();
    let msg = TaskEvent::TaskProgress(TaskProgress {
        envelope: env(),
        seq: 4,
        payload: serde_json::json!({"message": "reading", "percent": 40}),
    });
    let value = serde_json::to_value(&msg).unwrap();
    assert_valid(&v, &value);
}

#[test]
fn dto_completed_serialization_passes_schema() {
    let v = schema();
    let msg = TaskEvent::TaskCompleted(TaskCompleted {
        envelope: env(),
        seq: 7,
        is_final: true,
        artifacts: vec![ArtifactRef {
            uri: "https://peer.example/artifacts/summary.md".into(),
            mime_type: Some("text/markdown".into()),
        }],
        payload: serde_json::json!({"output": [{"type": "text", "text": "done"}]}),
    });
    let value = serde_json::to_value(&msg).unwrap();
    assert_valid(&v, &value);
}

#[test]
fn unknown_profile_rejected_by_schema() {
    let v = schema();
    let mut msg = TaskEvent::TaskProgress(TaskProgress {
        envelope: env(),
        seq: 4,
        payload: serde_json::json!({"message": "working"}),
    });
    let value = serde_json::to_value(&msg).unwrap();
    assert_valid(&v, &value);

    if let TaskEvent::TaskProgress(p) = &mut msg {
        p.envelope.profile = "agent-connector.other.v1".into();
    }
    let value = serde_json::to_value(&msg).unwrap();
    assert_invalid(&v, &value);
}

#[test]
fn zero_seq_rejected_by_schema() {
    let v = schema();
    let msg = TaskEvent::TaskProgress(TaskProgress {
        envelope: env(),
        seq: 0,
        payload: serde_json::json!({}),
    });
    let value = serde_json::to_value(&msg).unwrap();
    assert_invalid(&v, &value);
}

#[test]
fn terminal_without_final_rejected_by_schema() {
    let v = schema();
    let msg = TaskEvent::TaskCompleted(TaskCompleted {
        envelope: env(),
        seq: 7,
        is_final: false,
        artifacts: vec![],
        payload: serde_json::json!({}),
    });
    let value = serde_json::to_value(&msg).unwrap();
    assert_invalid(&v, &value);
}

#[test]
fn resume_without_after_seq_rejected_by_schema() {
    let v = schema();
    let mut value = serde_json::json!({
        "profile": PROFILE_ID,
        "version": 1,
        "message_type": "task.events",
        "task_id": "t-0001",
        "operation_id": "op-0001",
        "message_id": "m-0030",
        "payload": {}
    });
    assert_invalid(&v, &value);
    value["after_seq"] = serde_json::json!(4);
    assert_valid(&v, &value);
}

#[test]
fn missing_operation_id_rejected_by_schema() {
    let v = schema();
    let mut value = serde_json::json!({
        "profile": PROFILE_ID,
        "version": 1,
        "message_type": "task.invoke",
        "task_id": "t-0001",
        "message_id": "m-0001",
        "agent": "assistant",
        "payload": {"input": [{"type": "text", "text": "hello"}]}
    });
    assert_invalid(&v, &value);
    value["operation_id"] = serde_json::json!("op-0001");
    assert_valid(&v, &value);
}
