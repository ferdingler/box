//! Opt-in execution events on SDK queues separate from policy records.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::InstrumentationScope;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor;
use opentelemetry_sdk::logs::{LogBatch, LogProcessor, SdkLogRecord, SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::runtime::Tokio;
use serde_json::{Value, json};

use crate::export::TargetExporter;
use crate::record::Signal;

pub(crate) const SCOPE: &str = "strands-box.execution";
const VERSION_ATTRIBUTE: &str = "strands.box.execution.version";
const KIND_ATTRIBUTE: &str = "strands.box.execution.kind";
const QUEUE_CAPACITY: usize = 512;
const EVENT_CAPACITY: usize = QUEUE_CAPACITY - 4;
const MAXIMUM_EVENT_BYTES: usize = 32 * 1024;
const CLOSED: usize = 1 << (usize::BITS - 1);

#[derive(Debug, Default)]
struct Budget {
    pending: AtomicUsize,
    dropped: AtomicU64,
}

struct Exported<'a> {
    budget: &'a Budget,
    count: usize,
}

impl Drop for Exported<'_> {
    fn drop(&mut self) {
        self.budget.pending.fetch_sub(self.count, Ordering::Release);
    }
}

#[derive(Debug)]
struct ExecutionTarget {
    target: Arc<TargetExporter>,
    budget: Arc<Budget>,
}

impl opentelemetry_sdk::logs::LogExporter for ExecutionTarget {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let _exported = Exported {
            budget: &self.budget,
            count: batch.iter().count(),
        };
        self.target.export_logs(batch).await
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.target.set_resource_once(resource);
    }
}

#[derive(Debug)]
pub(crate) struct ExecutionLane {
    inner: BatchLogProcessor<Tokio>,
    budget: Arc<Budget>,
}

impl ExecutionLane {
    pub(crate) fn new(target: Arc<TargetExporter>) -> Self {
        let budget = Arc::new(Budget::default());
        Self {
            inner: BatchLogProcessor::builder(
                ExecutionTarget {
                    target,
                    budget: Arc::clone(&budget),
                },
                Tokio,
            )
            .with_batch_config(
                opentelemetry_sdk::logs::BatchConfigBuilder::default()
                    .with_max_queue_size(QUEUE_CAPACITY)
                    .with_max_export_batch_size(256)
                    .with_scheduled_delay(Duration::from_millis(200))
                    .with_max_export_timeout(Duration::from_secs(5))
                    .build(),
            )
            .build(),
            budget,
        }
    }
}

impl LogProcessor for ExecutionLane {
    fn emit(&self, data: &mut SdkLogRecord, instrumentation: &InstrumentationScope) {
        let footer = data.attributes_iter().any(|(key, value)| {
            key.as_str() == KIND_ATTRIBUTE
                && matches!(value, AnyValue::String(value) if value.as_str() == "capture_summary")
        });
        if footer {
            self.budget.pending.fetch_add(1, Ordering::AcqRel);
            let mut record = data.clone();
            if let Some(AnyValue::String(body)) = record.body()
                && let Ok(mut event) = serde_json::from_str::<Value>(body.as_str())
            {
                let dropped = self.budget.dropped.load(Ordering::Acquire);
                event["dropped_queue"] = json!(dropped);
                event["dropped_total"] =
                    json!(dropped + event["rejected_events"].as_u64().unwrap_or(0));
                record.set_body(AnyValue::String(event.to_string().into()));
            }
            self.inner.emit(&mut record, instrumentation);
            return;
        }
        if self
            .budget
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending < EVENT_CAPACITY).then_some(pending + 1)
            })
            .is_err()
        {
            self.budget.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.inner.emit(data, instrumentation);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

pub(crate) struct Execution {
    provider: SdkLoggerProvider,
    logger: SdkLogger,
    producers: AtomicUsize,
    sequence: AtomicU64,
    rejected: AtomicU64,
    clock: Instant,
    epoch_ms: f64,
}

struct Producer<'a>(&'a AtomicUsize);

impl Drop for Producer<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

impl Execution {
    pub(crate) fn new(provider: SdkLoggerProvider) -> Self {
        let execution = Self {
            logger: provider.logger(SCOPE),
            provider,
            producers: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            clock: Instant::now(),
            epoch_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
                * 1000.0,
        };
        execution.emit_record(
            json!({
                "kind": "capture_start", "capture_version": 3,
                "sequence": 0, "ts": execution.epoch_ms,
                "queue_capacity": QUEUE_CAPACITY, "maximum_event_bytes": MAXIMUM_EVENT_BYTES,
                "loss_detection": "sequence_gaps", "delivery": "best_effort",
                "coverage": {
                    "broker_requests": true, "parsed_gateway_exchanges": true,
                    "selected_mediated_filesystem_operations": true,
                    "native_filesystem_syscalls": false, "network_bypassing_gateway": false,
                    "uncorrelated_gateway_origin": "unknown"
                }
            }),
            "capture_start",
        );
        execution
    }

    pub(crate) fn emit(&self, mut event: Value) {
        let state = self.producers.fetch_add(1, Ordering::AcqRel);
        if state & CLOSED != 0 {
            self.producers.fetch_sub(1, Ordering::Release);
            return;
        }
        let _producer = Producer(&self.producers);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if !bounded_event(&event) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        event["ts"] = json!(self.now());
        event["sequence"] = json!(sequence);
        let body = event.to_string();
        if body.len() > MAXIMUM_EVENT_BYTES {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.emit_body(body, event["kind"].as_str().unwrap_or("event"));
    }

    pub(crate) fn close(&self) {
        self.producers.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub(crate) fn shutdown(&self) -> OTelSdkResult {
        self.close();
        while self.producers.load(Ordering::Acquire) != CLOSED {
            std::thread::sleep(Duration::from_millis(1));
        }
        let attempted = self.sequence.load(Ordering::Acquire);
        self.emit_record(
            json!({
                "kind": "capture_summary", "capture_version": 3,
                "ts": self.now(), "sequence": attempted + 1,
                "attempted": attempted, "expected_events": attempted, "last_sequence": attempted,
                "rejected_events": self.rejected.load(Ordering::Acquire),
                "outcome": "verify_sequence", "loss_detection": "sequence_gaps",
            }),
            "capture_summary",
        );
        self.provider.shutdown()
    }

    fn now(&self) -> f64 {
        self.epoch_ms + self.clock.elapsed().as_secs_f64() * 1000.0
    }

    fn emit_record(&self, event: Value, kind: &str) {
        self.emit_body(event.to_string(), kind);
    }

    fn emit_body(&self, body: String, kind: &str) {
        let mut record = self.logger.create_log_record();
        record.set_event_name("strands.box.execution.event");
        record.set_timestamp(SystemTime::now());
        record.set_severity_number(Signal::ExecutionTrace.severity());
        record.set_body(AnyValue::String(body.into()));
        record.add_attribute(VERSION_ATTRIBUTE, 3_i64);
        record.add_attribute(KIND_ATTRIBUTE, kind.to_string());
        self.logger.emit(record);
    }
}

fn bounded_event(event: &Value) -> bool {
    const KEYS: &[&str] = &[
        "kind",
        "id",
        "component",
        "from",
        "to",
        "parent",
        "label",
        "detail",
        "outcome",
        "exchange",
        "origin",
        "action_type",
        "provenance",
        "action",
        "verdict",
        "ts",
        "sequence",
    ];
    let Some(fields) = event.as_object() else {
        return false;
    };
    if !matches!(
        event["kind"].as_str(),
        Some("start" | "end" | "message" | "decision")
    ) {
        return false;
    }
    fields.len() <= KEYS.len()
        && fields.iter().all(|(key, value)| {
            KEYS.contains(&key.as_str())
                && match value {
                    Value::String(value) => value.len() <= 4096,
                    Value::Null | Value::Bool(_) | Value::Number(_) => true,
                    Value::Array(_) | Value::Object(_) => false,
                }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Collector, DecisionRecord, Target, TargetKind, TelemetryConfig};

    fn target(path: &std::path::Path, signals: Vec<Signal>) -> Target {
        Target::new(TargetKind::File, path.to_string_lossy().to_string()).receiving(signals)
    }

    fn logs(path: &std::path::Path, selected_scope: &str) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .flat_map(|batch| {
                batch["resourceLogs"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
            })
            .flat_map(|resource| {
                resource["scopeLogs"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
            })
            .filter(|scope| scope["scope"]["name"] == selected_scope)
            .flat_map(|scope| scope["logRecords"].as_array().cloned().unwrap_or_default())
            .collect()
    }

    fn events(path: &std::path::Path) -> Vec<Value> {
        logs(path, SCOPE)
            .into_iter()
            .map(|record| {
                serde_json::from_str(record["body"]["stringValue"].as_str().unwrap()).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn execution_is_absent_from_both_target_defaults() {
        let directory = tempfile::tempdir().unwrap();
        let named = directory.path().join("named.jsonl");
        let opened = directory.path().join("opened.jsonl");
        let config = TelemetryConfig::for_box("default")
            .with_target(Target::new(TargetKind::File, named.to_string_lossy()))
            .with_target(Target::opened_file(
                opened.to_string_lossy(),
                std::fs::File::create(&opened).unwrap(),
            ));
        for target in config.targets() {
            assert!(!target.signals().contains(&Signal::ExecutionTrace));
        }
        let collector = Collector::start(config).unwrap();
        assert!(!collector.execution_enabled());
        collector.execution(json!({"kind": "message", "id": "ignored"}));
        collector.record(DecisionRecord::permit("fs:read", "~/input", "permit"));
        collector.drained().await;
        assert!(events(&named).is_empty());
        assert!(events(&opened).is_empty());
        assert_eq!(logs(&named, crate::collector::SCOPE).len(), 1);
        assert_eq!(
            serde_json::to_string(&Signal::ExecutionTrace).unwrap(),
            "\"execution_trace\""
        );
    }

    #[tokio::test]
    async fn execution_payload_round_trips_with_its_run_and_completion_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("execution.jsonl");
        let collector = Collector::start(
            TelemetryConfig::for_box("execution")
                .with_target(target(&path, vec![Signal::ExecutionTrace])),
        )
        .unwrap();
        assert!(collector.execution_enabled());
        let original = json!({
            "kind": "start", "id": "filesystem-1", "component": "filesystem",
            "from": "shell", "to": "filesystem", "parent": "shell-1",
            "label": "Open for read", "detail": "/workspace/quoted \"input\" 🦀",
            "outcome": null, "origin": "shell", "action_type": "filesystem",
        });
        collector.execution(original.clone());
        collector.drained().await;
        assert!(!collector.execution_enabled());
        let events = events(&path);
        let start = events
            .iter()
            .find(|event| event["kind"] == "capture_start")
            .unwrap();
        assert_eq!(start["capture_version"], 3);
        assert_eq!(start["sequence"], 0);
        let mut event = events
            .iter()
            .find(|event| event["id"] == "filesystem-1")
            .unwrap()
            .clone();
        assert_eq!(event["sequence"], 1);
        assert!(event["ts"].as_f64().unwrap() > 0.0);
        event.as_object_mut().unwrap().remove("ts");
        event.as_object_mut().unwrap().remove("sequence");
        assert_eq!(event, original);
        let summary = events
            .iter()
            .find(|event| event["kind"] == "capture_summary")
            .unwrap();
        assert_eq!(summary["expected_events"], 1);
        assert_eq!(summary["last_sequence"], 1);
        assert_eq!(summary["dropped_queue"], 0);
        assert_eq!(summary["rejected_events"], 0);
        for record in logs(&path, SCOPE) {
            assert_eq!(record["eventName"], "strands.box.execution.event");
        }
        for line in std::fs::read_to_string(path).unwrap().lines() {
            let batch: Value = serde_json::from_str(line).unwrap();
            assert!(batch.get("resourceSpans").is_none());
            for resource in batch["resourceLogs"].as_array().unwrap() {
                let attributes = resource["resource"]["attributes"].as_array().unwrap();
                assert!(
                    attributes
                        .iter()
                        .any(|attribute| attribute["key"] == "strands.box.source"
                            && attribute["value"]["stringValue"] == "box")
                );
                assert!(attributes.iter().any(|attribute| {
                    attribute["key"] == "strands.box.run.id"
                        && !attribute["value"]["stringValue"]
                            .as_str()
                            .unwrap()
                            .is_empty()
                }));
            }
        }
    }

    #[tokio::test]
    async fn an_execution_flood_keeps_policy_records_and_its_own_loss_summary() {
        const ATTEMPTS: usize = 20_000;
        const DECISIONS: usize = 64;
        let directory = tempfile::tempdir().unwrap();
        let mixed = directory.path().join("mixed.jsonl");
        let policy = directory.path().join("policy.jsonl");
        let collector = Collector::start(
            TelemetryConfig::for_box("flood")
                .with_target(target(
                    &mixed,
                    vec![Signal::ExecutionTrace, Signal::PolicyPermitted],
                ))
                .with_target(target(&policy, vec![Signal::PolicyPermitted])),
        )
        .unwrap();
        for index in 0..ATTEMPTS {
            collector.execution(json!({"kind": "message", "id": format!("event-{index}")}));
        }
        for index in 0..DECISIONS {
            collector.record(DecisionRecord::permit(
                "fs:read",
                &format!("~/input-{index}"),
                "permit",
            ));
        }
        collector.drained().await;
        assert_eq!(logs(&mixed, crate::collector::SCOPE).len(), DECISIONS);
        assert_eq!(logs(&policy, crate::collector::SCOPE).len(), DECISIONS);
        assert!(events(&policy).is_empty());
        let events = events(&mixed);
        let summary = events
            .iter()
            .find(|event| event["kind"] == "capture_summary")
            .unwrap();
        let retained = events
            .iter()
            .filter(|event| event["kind"] == "message")
            .count();
        let dropped = summary["dropped_queue"].as_u64().unwrap() as usize;
        assert!(dropped > 0);
        assert_eq!(retained + dropped, ATTEMPTS);
        assert_eq!(summary["expected_events"], ATTEMPTS);
        assert_eq!(summary["rejected_events"], 0);
    }

    #[tokio::test]
    async fn unsupported_and_oversized_execution_fields_leave_counted_sequence_gaps() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.jsonl");
        let collector = Collector::start(
            TelemetryConfig::for_box("bounded")
                .with_target(target(&path, vec![Signal::ExecutionTrace])),
        )
        .unwrap();
        collector.execution(json!({"kind": "message", "trigger": "do-not-export-this-script"}));
        collector.execution(json!({"kind": "message", "detail": "x".repeat(4097)}));
        collector.execution(json!({"kind": "message", "id": "kept"}));
        collector.drained().await;
        let events = events(&path);
        let kept = events.iter().find(|event| event["id"] == "kept").unwrap();
        assert_eq!(kept["sequence"], 3);
        let summary = events
            .iter()
            .find(|event| event["kind"] == "capture_summary")
            .unwrap();
        assert_eq!(summary["expected_events"], 3);
        assert_eq!(summary["rejected_events"], 2);
        assert_eq!(summary["dropped_total"], 2);
        assert!(
            !std::fs::read_to_string(path)
                .unwrap()
                .contains("do-not-export-this-script")
        );
    }
}
