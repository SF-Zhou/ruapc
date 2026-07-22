//! Server-side handler dispatch: the glue between the router's method table
//! and user handler futures.
//!
//! [`spawn_handler`] wraps every handler execution with:
//!
//! - **load shedding** — when `max_inflight_requests` is reached the request
//!   is rejected immediately with [`ErrorKind::Overloaded`];
//! - **deadline enforcement** — requests whose client-provided budget
//!   already expired are dropped without execution (the client stopped
//!   waiting; a response would be wasted work);
//! - **metrics** — per-method request counters, in-flight gauges and
//!   latency histograms.
//!
//! Once a handler starts it always runs to completion: there is no
//! mid-flight cancellation. Aborting arbitrary user code at await points
//! risks breaking application invariants, and a cancel signal is inherently
//! unreliable — deadline checks (`Context::is_expired`) inside long-running
//! handlers are the supported way to stop wasted work.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::{Context, Error, ErrorKind, MethodMetrics, Payload, State};

/// Spawns a request handler with dispatch policies applied.
///
/// `f` receives the context and payload and must perform the complete
/// handling (deserialize, invoke, respond); it is produced by the
/// `#[ruapc::service]` macro.
///
/// Hidden from docs: this is macro plumbing, not a public API.
#[doc(hidden)]
pub fn spawn_handler<F, Fut>(mut ctx: Context, method: &'static str, payload: Payload, f: F)
where
    F: FnOnce(Context, Payload) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let state = ctx.state.clone();
    let metrics = &state.metrics;

    // Load shedding: reject before spawning any work.
    let cap = state.max_inflight_requests;
    if cap > 0 && metrics.server_inflight.load(Ordering::Relaxed) >= cap as i64 {
        metrics.request_rejected();
        tokio::spawn(async move {
            ctx.send_err_rsp(Error::new(
                ErrorKind::Overloaded,
                format!("server over capacity ({cap} requests in flight)"),
            ))
            .await;
        });
        return;
    }

    // Deadline enforcement: the client stopped waiting already.
    if ctx.is_expired() {
        metrics.request_expired();
        tracing::warn!(
            "dropping expired request {method} (msgid {})",
            ctx.msg_meta.msgid
        );
        return;
    }

    let method_metrics = metrics.server_method(method);
    method_metrics.requests.increment(1);
    method_metrics.inflight.increment(1.0);
    metrics.server_inflight.fetch_add(1, Ordering::Relaxed);

    let guard = InflightGuard {
        state,
        method_metrics,
        start: Instant::now(),
    };
    let fut = f(ctx, payload);
    tokio::spawn(async move {
        // The guard lives inside the task: normal completion and panic
        // both run its Drop.
        let _guard = guard;
        fut.await;
    });
}

/// Reverts the bookkeeping of one in-flight handler; runs on completion
/// and panic alike.
struct InflightGuard {
    state: std::sync::Arc<State>,
    method_metrics: MethodMetrics,
    start: Instant,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.method_metrics
            .latency
            .record(self.start.elapsed().as_secs_f64());
        self.method_metrics.inflight.decrement(1.0);
        self.state
            .metrics
            .server_inflight
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, MsgFlags, MsgMeta, SocketPoolConfig, SocketType};

    fn make_ctx(config: &SocketPoolConfig, timeout_ms: Option<u32>) -> Context {
        let base = Context::create(config).unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let socket = crate::Socket::TCP(crate::sockets::tcp::TcpSocket::new(tx));
        let meta = MsgMeta {
            method: "TestSvc/x".into(),
            flags: MsgFlags::IsReq,
            msgid: 1,
            buffer_info: None,
            timeout_ms,
        };
        Context::server_ctx(&base.state, socket, meta)
    }

    fn tcp_config() -> SocketPoolConfig {
        SocketPoolConfig {
            socket_type: SocketType::TCP,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_expired_request_is_dropped() {
        let ctx = make_ctx(&tcp_config(), Some(0));
        // The zero budget expired on arrival.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let state = ctx.state.clone();
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_clone = ran.clone();
        spawn_handler(ctx, "TestSvc/x", crate::Payload::Empty, move |_, _| {
            ran_clone.store(true, Ordering::SeqCst);
            async {}
        });
        tokio::task::yield_now().await;
        assert!(!ran.load(Ordering::SeqCst));
        assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_load_shedding_rejects_over_cap() {
        let config = SocketPoolConfig {
            socket_type: SocketType::TCP,
            max_inflight_requests: 1,
            ..Default::default()
        };
        let ctx = make_ctx(&config, None);
        let state = ctx.state.clone();
        // Simulate one request already in flight.
        state
            .metrics
            .server_inflight
            .fetch_add(1, Ordering::Relaxed);
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_clone = ran.clone();
        spawn_handler(ctx, "TestSvc/x", crate::Payload::Empty, move |_, _| {
            ran_clone.store(true, Ordering::SeqCst);
            async {}
        });
        tokio::task::yield_now().await;
        assert!(!ran.load(Ordering::SeqCst));
        // The rejected request must not consume in-flight capacity.
        assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_handler_runs_and_metrics_settle() {
        let ctx = make_ctx(&tcp_config(), Some(30_000));
        let state = ctx.state.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        spawn_handler(ctx, "TestSvc/x", crate::Payload::Empty, move |_, _| async {
            let _ = done_tx.send(());
        });
        done_rx.await.unwrap();
        // Give the guard drop a moment to run after the handler body.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 0);
    }
}
