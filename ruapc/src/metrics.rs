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
//! | `ruapc_data_frames_total` | counter | `direction` |
//! | `ruapc_data_bytes_total` | counter | `direction` |
//! | `ruapc_data_frames_discarded_total` | counter | |
//!
//! The `ruapc_data_*` metrics count the data frames that carry remote-memory
//! bytes on stream connections, with `direction` `sent` or `received`. A
//! frame is counted as sent once its connection accepted it, and as received
//! once it was consumed. A discarded frame is a received one whose bytes did
//! not all reach a write space: its request was gone or on another
//! connection, or the write space was sealed or refused the frame.

use std::sync::{OnceLock, atomic::AtomicI64};

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
    /// Starts accounting that settles on completion, cancellation or panic.
    #[inline]
    pub(crate) fn start(self) -> MethodCall {
        self.requests.increment(1);
        self.inflight.increment(1.0);
        MethodCall {
            metrics: self,
            start: std::time::Instant::now(),
        }
    }

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

/// Owns only existing facade handles; no allocation or timer registration.
pub(crate) struct MethodCall {
    metrics: MethodMetrics,
    start: std::time::Instant,
}

impl MethodCall {
    pub(crate) fn failed(&self) {
        self.metrics.errors.increment(1);
    }
}

impl Drop for MethodCall {
    fn drop(&mut self) {
        self.metrics
            .latency
            .record(self.start.elapsed().as_secs_f64());
        self.metrics.inflight.decrement(1.0);
    }
}

/// Data-frame metric handles, bound to the recorder at first use.
struct DataMetrics {
    sent_frames: Counter,
    sent_bytes: Counter,
    received_frames: Counter,
    received_bytes: Counter,
    discarded_frames: Counter,
}

impl DataMetrics {
    fn new() -> Self {
        Self {
            sent_frames: counter!("ruapc_data_frames_total", "direction" => "sent"),
            sent_bytes: counter!("ruapc_data_bytes_total", "direction" => "sent"),
            received_frames: counter!("ruapc_data_frames_total", "direction" => "received"),
            received_bytes: counter!("ruapc_data_bytes_total", "direction" => "received"),
            discarded_frames: counter!("ruapc_data_frames_discarded_total"),
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
    /// Server-side requests using the shared admission budget across all methods.
    ///
    /// This is dispatch state, not just a metric: load shedding reads it
    /// on every request, and the facade offers no read-back, so it is
    /// tracked locally (the `ruapc_server_inflight` gauges remain
    /// per-method, emitted through the facade).
    pub(crate) server_inflight: AtomicI64,
    server_methods: dashmap::DashMap<String, MethodMetrics, RandomState>,
    client_methods: dashmap::DashMap<String, MethodMetrics, RandomState>,
    data: OnceLock<DataMetrics>,
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

    /// Records a data frame of `bytes` raw bytes that a connection accepted.
    pub(crate) fn data_frame_sent(&self, bytes: usize) {
        let data = self.data.get_or_init(DataMetrics::new);
        data.sent_frames.increment(1);
        data.sent_bytes.increment(bytes as u64);
    }

    /// Records a consumed data frame of `bytes` raw bytes. `written` tells
    /// whether all of them reached a write space.
    pub(crate) fn data_frame_received(&self, bytes: usize, written: bool) {
        let data = self.data.get_or_init(DataMetrics::new);
        data.received_frames.increment(1);
        data.received_bytes.increment(bytes as u64);
        if !written {
            data.discarded_frames.increment(1);
        }
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
    fn data_frames_are_counted_by_direction_and_fate() {
        let snapshot = record(|metrics| {
            metrics.data_frame_sent(10);
            metrics.data_frame_sent(5);
            metrics.data_frame_received(7, true);
            metrics.data_frame_received(4, false);
            metrics.data_frame_received(0, false);
        });
        let counter = |name: &str, direction: Option<&str>| match find(
            &snapshot,
            name,
            direction.map(|value| ("direction", value)),
        ) {
            DebugValue::Counter(value) => *value,
            other => panic!("{name} is not a counter: {other:?}"),
        };
        assert_eq!(counter("ruapc_data_frames_total", Some("sent")), 2);
        assert_eq!(counter("ruapc_data_bytes_total", Some("sent")), 15);
        assert_eq!(counter("ruapc_data_frames_total", Some("received")), 3);
        assert_eq!(counter("ruapc_data_bytes_total", Some("received")), 11);
        assert_eq!(counter("ruapc_data_frames_discarded_total", None), 2);
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
        metrics.data_frame_sent(1);
        metrics.data_frame_received(1, false);
    }
}
