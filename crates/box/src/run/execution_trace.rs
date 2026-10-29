//! Execution events and causal context for optional telemetry.

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

static OUTPUT: OnceLock<Weak<telemetry::Collector>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);
type Context = (String, String, String);
type Bindings = HashMap<String, Vec<(u64, Context)>>;
static REQUESTS: OnceLock<Mutex<Bindings>> = OnceLock::new();

tokio::task_local! {
    static CONTEXT: Context;
}

fn context(correlation: &telemetry::Correlation) -> Option<Context> {
    CONTEXT.try_with(Clone::clone).ok().or_else(|| {
        let keys = correlation_keys(correlation);
        let requests = REQUESTS.get()?.lock().ok()?;
        keys.into_iter()
            .find_map(|key| active_context(requests.get(&key)?))
    })
}

fn correlation_keys(correlation: &telemetry::Correlation) -> Vec<String> {
    let Ok(value) = serde_json::to_value(correlation) else {
        return Vec::new();
    };
    let mut keys = Vec::new();
    if let Some(parent) = value.get("traceparent").and_then(Value::as_str) {
        keys.push(format!("trace:{parent}"));
    }
    let key = value.to_string();
    if key != "{}" {
        keys.push(key);
    }
    keys
}

fn active_context(entries: &[(u64, Context)]) -> Option<Context> {
    let candidate = &entries.last()?.1;
    entries
        .iter()
        .all(|(_, value)| value.2 == candidate.2)
        .then(|| candidate.clone())
}

fn remove_binding(requests: &mut Bindings, keys: &[String], token: u64) {
    for key in keys {
        if let Some(entries) = requests.get_mut(key) {
            entries.retain(|(id, _)| *id != token);
            if entries.is_empty() {
                requests.remove(key);
            }
        }
    }
}

struct RequestBinding {
    token: u64,
    keys: Vec<String>,
}

impl RequestBinding {
    fn enter(value: Context) -> Option<Self> {
        let keys = correlation_keys(&telemetry::Correlation::current());
        let token = NEXT.fetch_add(1, Ordering::Relaxed);
        let mut requests = REQUESTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .ok()?;
        for key in &keys {
            requests
                .entry(key.clone())
                .or_default()
                .push((token, value.clone()));
        }
        Some(Self { token, keys })
    }
}

impl Drop for RequestBinding {
    fn drop(&mut self) {
        if let Some(requests) = REQUESTS.get()
            && let Ok(mut requests) = requests.lock()
        {
            remove_binding(&mut requests, &self.keys, self.token);
        }
    }
}

pub(crate) fn enabled() -> bool {
    OUTPUT
        .get()
        .and_then(Weak::upgrade)
        .is_some_and(|collector| collector.execution_enabled())
}

fn bounded(text: &str) -> Cow<'_, str> {
    const LIMIT: usize = 4096;
    if text.len() <= LIMIT {
        return Cow::Borrowed(text);
    }
    let mut end = LIMIT - 20;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(format!("{}… [truncated]", &text[..end]))
}

pub(crate) fn initialize(collector: &Arc<telemetry::Collector>) {
    if collector.execution_enabled() {
        let _ = OUTPUT.set(Arc::downgrade(collector));
    }
}

pub(crate) fn correlate(correlation: telemetry::Correlation) -> telemetry::Correlation {
    if !enabled() {
        return correlation;
    }
    let mut value = serde_json::to_value(&correlation).unwrap_or_default();
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    if value.get("request_id").is_none() {
        value["request_id"] = json!(format!("execution-request-{sequence}"));
    }
    if value.get("traceparent").is_none() {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        value["traceparent"] = json!(format!("00-{epoch:032x}-{sequence:016x}-01"));
    }
    serde_json::from_value(value).unwrap_or(correlation)
}

fn emit(event: Value) {
    let Some(output) = OUTPUT.get().and_then(Weak::upgrade) else {
        return;
    };
    output.execution(event);
}

pub(crate) struct Activity {
    id: String,
    root: String,
    component: String,
    from: String,
    label: String,
    parent: Option<String>,
    detail: Option<String>,
    outcome: Option<String>,
    _binding: Option<RequestBinding>,
}

impl Activity {
    fn disabled() -> Self {
        Self {
            id: String::new(),
            root: String::new(),
            component: String::new(),
            from: String::new(),
            label: String::new(),
            parent: None,
            detail: None,
            outcome: None,
            _binding: None,
        }
    }
    pub(crate) fn start(component: &str, from: &str, label: &str) -> Self {
        Self::start_detail(component, from, label, None)
    }

    fn start_detail(component: &str, from: &str, label: &str, detail: Option<&str>) -> Self {
        if !enabled() {
            return Self::disabled();
        }
        let label = bounded(label);
        let detail = detail.map(bounded);
        let id = format!("{component}-{}", NEXT.fetch_add(1, Ordering::Relaxed));
        let parent_context = if component == "broker" && from == "agent" {
            None
        } else {
            context(&telemetry::Correlation::current())
        };
        let root = parent_context
            .as_ref()
            .map_or_else(|| id.clone(), |context| context.2.clone());
        let parent = parent_context.map(|context| context.1);
        let target = if component == "leaf" { &id } else { component };
        emit(json!({
            "kind": "start", "id": id, "component": component,
            "from": from, "to": target, "parent": parent, "label": label,
            "detail": detail,
        }));
        Self {
            id,
            root,
            component: component.to_string(),
            from: from.to_string(),
            label: label.into_owned(),
            parent,
            detail: detail.map(Cow::into_owned),
            outcome: None,
            _binding: None,
        }
    }

    pub(crate) fn ingress(correlation: &telemetry::Correlation, label: &str) -> Self {
        if !enabled() {
            return Self::disabled();
        }
        correlation.during(|| {
            let mut activity = Self::start_detail("broker", "agent", label, Some("box.sock"));
            activity._binding = RequestBinding::enter((
                "broker".to_string(),
                activity.id.clone(),
                activity.root.clone(),
            ));
            activity
        })
    }

    pub(crate) fn filesystem(from: &str, operation: &str, path: &str) -> Self {
        Self::start_detail("filesystem", from, operation, Some(path))
    }

    pub(crate) fn finish(mut self, completed: bool) {
        if self.id.is_empty() {
            return;
        }
        self.outcome = Some(if completed { "completed" } else { "failed" }.to_string());
    }

    pub(crate) async fn scope<T>(&self, future: impl Future<Output = T>) -> T {
        if self.id.is_empty() {
            return future.await;
        }
        CONTEXT
            .scope(
                (self.component.clone(), self.id.clone(), self.root.clone()),
                future,
            )
            .await
    }

    pub(crate) fn status(&self, status: i32) {
        if self.id.is_empty() {
            return;
        }
        let source = if self.component == "leaf" {
            &self.id
        } else {
            &self.component
        };
        emit(json!({
            "kind": "message", "id": self.id, "component": self.component,
            "from": source, "to": self.from, "label": "Exit status",
            "parent": self.parent,
            "detail": format!("exit status {status}"),
        }));
    }
}

impl Drop for Activity {
    fn drop(&mut self) {
        if self.id.is_empty() {
            return;
        }
        let source = if self.component == "leaf" {
            &self.id
        } else {
            &self.component
        };
        emit(json!({
            "kind": "end", "id": self.id, "component": self.component,
            "from": source, "to": self.from, "label": self.label,
            "parent": self.parent,
            "detail": self.detail,
            "outcome": self.outcome,
        }));
    }
}

pub(crate) async fn request<T>(component: &str, label: &str, future: impl Future<Output = T>) -> T {
    if !enabled() {
        return future.await;
    }
    let from = context(&telemetry::Correlation::current())
        .map(|context| context.0)
        .unwrap_or_else(|| "agent".to_string());
    let activity = Activity::start(component, &from, label);
    let _binding = RequestBinding::enter((
        component.to_string(),
        activity.id.clone(),
        activity.root.clone(),
    ));
    CONTEXT
        .scope(
            (
                component.to_string(),
                activity.id.clone(),
                activity.root.clone(),
            ),
            future,
        )
        .await
}

pub(crate) fn decision(
    action: &str,
    permit: bool,
    authority: &str,
    correlation: &telemetry::Correlation,
) {
    if !enabled() {
        return;
    }
    let context = context(correlation);
    let gateway = action.contains("net:") || action.contains("http:");
    let from = if gateway {
        "gateway"
    } else {
        context
            .as_ref()
            .map_or("unknown", |context| context.0.as_str())
    };
    let component = if authority == "policy" {
        "policy"
    } else {
        "box"
    };
    let exchange = serde_json::to_value(correlation)
        .ok()
        .and_then(|value| value.get("request_id").cloned());
    emit(json!({
        "kind": "decision",
        "id": format!("decision-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        "component": component, "from": from, "to": component,
        "parent": context.as_ref().map(|context| &context.1), "label": action,
        "action": action, "verdict": if permit { "permit" } else { "deny" },
        "detail": format!("Effective result · {authority}"),
        "exchange": exchange,
    }));
    if authority == "policy" {
        emit(json!({
            "kind": "message", "id": format!("policy-result-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
            "component": "policy", "from": "policy", "to": from,
            "parent": context.map(|context| context.1),
            "label": if permit { "Permit returned" } else { "Deny returned" },
            "action": action, "verdict": if permit { "permit" } else { "deny" },
            "exchange": exchange,
        }));
    }
}

pub(crate) fn transport(
    stage: &str,
    host: &str,
    port: u16,
    correlation: &egress_gateway::RequestId,
) {
    if !enabled() {
        return;
    }
    let context = context(&telemetry::Correlation::from_headers(
        correlation.context("traceparent"),
        correlation.context("tracestate"),
    ));
    let destination = format!("{host}:{port}");
    let destination = bounded(&destination);
    if matches!(stage, "request_received" | "exchange_handled") {
        let origin = context
            .as_ref()
            .map_or("unknown", |context| context.0.as_str());
        let received = stage == "request_received";
        emit(json!({
            "kind": if received { "start" } else { "end" },
            "id": format!("gateway-exchange:{}", correlation.as_str()),
            "component": "gateway",
            "from": if received { origin } else { "gateway" },
            "to": if received { "gateway" } else { origin },
            "parent": context.as_ref().map(|context| &context.1),
            "label": if received { "Gateway request received" } else { "Gateway exchange handled" },
            "detail": destination,
            "exchange": correlation.as_str(),
            "origin": origin,
            "action_type": "network",
            "provenance": "parsed_gateway_request",
        }));
        return;
    }
    let (from, to, label) = match stage {
        "connect_attempt" => ("gateway", "internet", "TCP connection attempted"),
        "connected" => ("internet", "gateway", "TCP connection established"),
        "request_attempt" => ("gateway", "internet", "HTTP request write attempted"),
        "response_received" => ("internet", "gateway", "HTTP response received"),
        _ => return,
    };
    emit(json!({
        "kind": "message", "id": format!("transport-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        "component": "gateway", "from": from, "to": to,
        "parent": context.map(|context| context.1), "label": label,
        "detail": destination,
        "exchange": correlation.as_str(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_bindings_leave_no_stale_context() {
        let keys = vec!["shared".to_string()];
        let a = ("broker".to_string(), "a".to_string(), "a".to_string());
        let b = ("broker".to_string(), "b".to_string(), "b".to_string());
        for (first, last, remaining) in [(1, 2, b.clone()), (2, 1, a.clone())] {
            let mut requests =
                Bindings::from([("shared".to_string(), vec![(1, a.clone()), (2, b.clone())])]);
            assert!(active_context(&requests["shared"]).is_none());
            remove_binding(&mut requests, &keys, first);
            assert_eq!(active_context(&requests["shared"]), Some(remaining));
            remove_binding(&mut requests, &keys, last);
            assert!(requests.is_empty());
        }
    }

    #[test]
    fn nested_bindings_keep_the_active_request() {
        let keys = vec!["shared".to_string()];
        let outer = ("broker".to_string(), "a".to_string(), "a".to_string());
        let inner = ("shell".to_string(), "shell-a".to_string(), "a".to_string());
        let mut requests = Bindings::from([(
            "shared".to_string(),
            vec![(1, outer.clone()), (2, inner.clone())],
        )]);
        assert_eq!(active_context(&requests["shared"]), Some(inner));
        remove_binding(&mut requests, &keys, 2);
        assert_eq!(active_context(&requests["shared"]), Some(outer));
        remove_binding(&mut requests, &keys, 1);
        assert!(requests.is_empty());
    }
}
