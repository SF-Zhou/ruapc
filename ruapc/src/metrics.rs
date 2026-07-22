//! Metric instrumentation via the [`metrics`] facade.
//!
//! RuaPC only *emits* metrics; it never renders or exports them. Install
//! any [`metrics::Recorder`] (e.g. `metrics-exporter-prometheus`,
//! `metrics-exporter-statsd`, ...) to collect them — without one, every
//! emission is a no-op.
//!
//! Per-method handles are interned in a [`dashmap::DashMap`] so the hot
//! path costs one lock-free lookup instead of label allocation. Handles
//! bind to the recorder installed at first use: install the recorder
//! before serving traffic.
//!
//! # Emitted metrics
//!
//! | name | type | labels |
//! |------|------|--------|
//! | `ruapc_server_requests_total` | counter | `method` |
//! | `ruapc_server_errors_total` | counter | `method` |
//! | `ruapc_server_inflight` | gauge | `method` |
//! | `ruapc_server_latency_seconds` | histogram | `method` |
//! | `ruapc_client_requests_total` | counter | `method` |
//! | `ruapc_client_errors_total` | counter | `method` |
//! | `ruapc_client_inflight` | gauge | `method` |
//! | `ruapc_client_latency_seconds` | histogram | `method` |
//! | `ruapc_connections` | gauge | `transport` |
//! | `ruapc_requests_rejected_total` | counter | |
//! | `ruapc_requests_expired_total` | counter | |
//! | `ruapc_waiter_pending` | gauge | |

use std::sync::atomic::AtomicI64;

use foldhash::fast::RandomState;
use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};

/// Per-method metric handles (facade handles; cheap to clone).
#[derive(Clone)]
pub(crate) struct MethodMetrics {
    /// Requests started.
    pub(crate) requests: Counter,
    /// Requests completed with an error.
    pub(crate) errors: Counter,
    /// Requests currently in flight.
    pub(crate) inflight: Gauge,
    /// Completion latency histogram (seconds).
    pub(crate) latency: Histogram,
}

impl MethodMetrics {
    fn new(side: &'static str, method: &str) -> Self {
        let method = method.to_string();
        Self {
            requests: counter!(format!("ruapc_{side}_requests_total"), "method" => method.clone()),
            errors: counter!(format!("ruapc_{side}_errors_total"), "method" => method.clone()),
            inflight: gauge!(format!("ruapc_{side}_inflight"), "method" => method.clone()),
            latency: histogram!(format!("ruapc_{side}_latency_seconds"), "method" => method),
        }
    }
}

/// Per-[`State`](crate::State) instrumentation helpers.
///
/// Only the interned handle caches and the load-shedding source of truth
/// live here; the actual metric storage belongs to the user-installed
/// [`metrics::Recorder`].
#[derive(Default)]
pub(crate) struct Metrics {
    /// Server-side requests currently in flight across all methods.
    ///
    /// This is dispatch state, not just a metric: load shedding reads it
    /// on every request, and the facade offers no read-back, so it is
    /// tracked locally (the `ruapc_server_inflight` gauges remain
    /// per-method, emitted through the facade).
    pub(crate) server_inflight: AtomicI64,
    server_methods: dashmap::DashMap<String, MethodMetrics, RandomState>,
    client_methods: dashmap::DashMap<String, MethodMetrics, RandomState>,
}

impl Metrics {
    /// Interned per-method server-side metrics.
    pub(crate) fn server_method(&self, method: &str) -> MethodMetrics {
        Self::intern(&self.server_methods, "server", method)
    }

    /// Interned per-method client-side metrics.
    pub(crate) fn client_method(&self, method: &str) -> MethodMetrics {
        Self::intern(&self.client_methods, "client", method)
    }

    fn intern(
        cache: &dashmap::DashMap<String, MethodMetrics, RandomState>,
        side: &'static str,
        method: &str,
    ) -> MethodMetrics {
        if let Some(m) = cache.get(method) {
            return m.clone();
        }
        cache
            .entry(method.to_string())
            .or_insert_with(|| MethodMetrics::new(side, method))
            .clone()
    }

    /// Records a new live connection for `transport`.
    pub(crate) fn connection_opened(&self, transport: &'static str) {
        gauge!("ruapc_connections", "transport" => transport).increment(1.0);
    }

    /// Records a closed connection for `transport`.
    pub(crate) fn connection_closed(&self, transport: &'static str) {
        gauge!("ruapc_connections", "transport" => transport).decrement(1.0);
    }

    /// Records a request rejected by the in-flight cap.
    pub(crate) fn request_rejected(&self) {
        counter!("ruapc_requests_rejected_total").increment(1);
    }

    /// Records a request dropped because its deadline expired before
    /// execution.
    pub(crate) fn request_expired(&self) {
        counter!("ruapc_requests_expired_total").increment(1);
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// `(name, labels, value)` triple captured from a snapshot.
    type Recorded = (String, Vec<(String, String)>, DebugValue);

    /// Runs `f` under a local debugging recorder and returns the snapshot
    /// as `(name, labels, value)` tuples.
    fn record<F: FnOnce(&Metrics)>(f: F) -> Vec<Recorded> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = Metrics::default();
        metrics::with_local_recorder(&recorder, || f(&metrics));
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let key = key.key();
                (
                    key.name().to_string(),
                    key.labels()
                        .map(|l| (l.key().to_string(), l.value().to_string()))
                        .collect(),
                    value,
                )
            })
            .collect()
    }

    fn find<'a>(
        snapshot: &'a [Recorded],
        name: &str,
        label: Option<(&str, &str)>,
    ) -> &'a DebugValue {
        &snapshot
            .iter()
            .find(|(n, labels, _)| {
                n == name
                    && label.is_none_or(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .unwrap_or_else(|| panic!("metric {name} not found"))
            .2
    }

    #[test]
    fn test_method_metrics_emission() {
        let snapshot = record(|metrics| {
            let m = metrics.server_method("Svc/echo");
            m.requests.increment(3);
            m.errors.increment(1);
            m.latency.record(0.000_05);
        });
        let label = Some(("method", "Svc/echo"));
        assert!(matches!(
            find(&snapshot, "ruapc_server_requests_total", label),
            DebugValue::Counter(3)
        ));
        assert!(matches!(
            find(&snapshot, "ruapc_server_errors_total", label),
            DebugValue::Counter(1)
        ));
        assert!(matches!(
            find(&snapshot, "ruapc_server_latency_seconds", label),
            DebugValue::Histogram(v) if v.len() == 1
        ));
    }

    #[test]
    fn test_connection_gauge_up_down() {
        let snapshot = record(|metrics| {
            metrics.connection_opened("HTTP");
            metrics.connection_opened("HTTP");
            metrics.connection_closed("HTTP");
        });
        let value = find(&snapshot, "ruapc_connections", Some(("transport", "HTTP")));
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 1.0));
    }

    #[test]
    fn test_method_interning_reuses_handles() {
        let recorder = DebuggingRecorder::new();
        let metrics = Metrics::default();
        metrics::with_local_recorder(&recorder, || {
            let a = metrics.client_method("Svc/x");
            let b = metrics.client_method("Svc/x");
            a.requests.increment(1);
            b.requests.increment(1);
        });
        let snapshot = recorder.snapshotter().snapshot().into_vec();
        // Both handles feed the same counter.
        let total: u64 = snapshot
            .iter()
            .filter(|(key, ..)| key.key().name() == "ruapc_client_requests_total")
            .map(|(.., value)| match value {
                DebugValue::Counter(c) => *c,
                _ => 0,
            })
            .sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_noop_without_recorder() {
        // Without a recorder every emission must be a silent no-op.
        let metrics = Metrics::default();
        let m = metrics.server_method("Svc/echo");
        m.requests.increment(1);
        m.latency.record(0.1);
        metrics.connection_opened("TCP");
        metrics.request_rejected();
    }
}
