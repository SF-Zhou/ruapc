//! Server-side handler dispatch: the glue between the router's method table
//! and user handler futures.
//!
//! [`spawn_handler_result`] wraps every handler execution with:
//!
//! - **load shedding** — when `max_inflight_requests` is reached the request
//!   is rejected immediately with [`ErrorKind::Overloaded`];
//! - **deadline enforcement** — requests whose client-provided budget
//!   already expired are dropped without execution (the client stopped
//!   waiting; a response would be wasted work);
//! - **metrics** — per-method request counters, in-flight gauges and
//!   latency histograms.
//!
//! Deadline expiry does not cancel a running handler: dropping user futures
//! at await points can break application invariants. Long-running handlers
//! should check [`Context::is_expired`] to stop cooperatively.

use std::sync::atomic::Ordering;

use super::catch_handler_panic;
use crate::{Context, Error, ErrorKind, Payload, Result, State};

/// Test helper for bodies that perform their own response delivery.
#[cfg(test)]
pub(crate) fn spawn_handler<F, Fut>(ctx: Context, method: &'static str, payload: Payload, f: F)
where
    F: FnOnce(Context, Payload) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    spawn_handler_result(ctx, method, payload, move |ctx, payload| {
        let future = f(ctx, payload);
        async move {
            future.await;
            Ok(())
        }
    });
}

/// Common execution boundary for generated and explicitly registered handlers.
pub(crate) fn spawn_handler_result<F, Fut>(
    mut ctx: Context,
    method: &'static str,
    payload: Payload,
    f: F,
) where
    F: FnOnce(Context, Payload) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let state = ctx.state.clone();
    let metrics = &state.metrics;

    let cap = state.max_inflight_requests;
    let inflight = metrics.server_inflight.fetch_add(1, Ordering::Relaxed);
    if cap > 0 && inflight >= cap as i64 {
        metrics.server_inflight.fetch_sub(1, Ordering::Relaxed);
        metrics.request_rejected();
        tokio::spawn(async move {
            ctx.send_err_rsp(Error::new(
                ErrorKind::Overloaded,
                format!("server over capacity ({cap} requests in flight) for {method}"),
            ))
            .await;
        });
        return;
    }

    let guard = InflightGuard { state };
    let metrics = &guard.state.metrics;

    // Deadline enforcement: the client stopped waiting already.
    if ctx.is_expired() {
        metrics.request_expired();
        tracing::warn!(
            "dropping expired request {method} (msgid {})",
            ctx.msg_meta.msgid
        );
        return;
    }

    tokio::spawn(async move {
        // The guard lives inside the task: normal completion and panic
        // both run its Drop.
        let execution_guard = guard;
        // Admission and execution can be separated by an arbitrarily long
        // scheduler delay. Do not construct the application's future if its
        // budget expired while this task was waiting to run.
        if ctx.is_expired() {
            execution_guard.state.metrics.request_expired();
            return;
        }
        let _method_call = execution_guard.state.metrics.server_method(method).start();
        let response_target = ctx.response_target();
        let result = catch_handler_panic(async move { f(ctx, payload).await })
            .await
            .unwrap_or_else(Err);
        if let Err(error) = result {
            response_target
                .send_err_rsp(&execution_guard.state, method, error)
                .await;
        }
    });
}

/// Reverts the bookkeeping of one in-flight handler; runs on completion
/// and panic alike.
struct InflightGuard {
    state: std::sync::Arc<State>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.state
            .metrics
            .server_inflight
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, MsgFlags, MsgMeta, Socket, SocketPoolConfig, sockets::tcp::TcpSocket};

    fn make_ctx(config: &SocketPoolConfig, timeout_ms: u32) -> Context {
        let base = Context::create(config).unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let socket = Socket::TCP(TcpSocket::new(tx));
        let meta = MsgMeta {
            method: "TestSvc/x".into(),
            flags: MsgFlags::IsReq,
            msgid: 1,
            read_regions: Vec::new(),
            write_regions: Vec::new(),
            timeout_ms,
        };
        Context::server_ctx(&base.state, socket, meta)
    }

    fn tcp_config() -> SocketPoolConfig {
        SocketPoolConfig::default()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simultaneous_arrivals_cannot_exceed_capacity() {
        const CAP: usize = 4;
        const THREADS: usize = 32;
        let ctx = make_ctx(
            &SocketPoolConfig {
                max_inflight_requests: CAP,
                ..Default::default()
            },
            30_000,
        );
        let admitted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = std::sync::Barrier::new(THREADS);
        let release = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let runtime = tokio::runtime::Handle::current();
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    let _entered = runtime.enter();
                    barrier.wait();
                    let release = release.clone();
                    let admitted = admitted.clone();
                    spawn_handler(ctx.clone(), "TestSvc/x", Payload::default(), move |_, _| {
                        admitted.fetch_add(1, Ordering::Relaxed);
                        async move {
                            let _permit = release.acquire().await.unwrap();
                        }
                    });
                });
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while admitted.load(Ordering::Relaxed) != CAP {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(admitted.load(Ordering::Relaxed), CAP);
        assert_eq!(
            ctx.state.metrics.server_inflight.load(Ordering::Relaxed),
            CAP as i64
        );
        release.add_permits(CAP);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while ctx.state.metrics.server_inflight.load(Ordering::Relaxed) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_expiring_in_scheduler_queue_never_constructs_handler() {
        let ctx = make_ctx(&tcp_config(), 10);
        let state = ctx.state.clone();
        let constructed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = constructed.clone();
        assert!(!ctx.is_expired());
        spawn_handler_result(ctx, "TestSvc/x", Payload::default(), move |_, _| {
            observed.store(true, Ordering::SeqCst);
            async { Ok(()) }
        });
        assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 1);
        // Deliberately keep this sole runtime thread occupied until the queued
        // request expires, then allow the task to receive its first poll.
        std::thread::sleep(std::time::Duration::from_millis(20));
        tokio::task::yield_now().await;
        assert!(!constructed.load(Ordering::SeqCst));
        assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_expired_request_is_dropped() {
        let ctx = make_ctx(&tcp_config(), 0);
        // The zero budget expired on arrival.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let state = ctx.state.clone();
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_clone = ran.clone();
        spawn_handler(ctx, "TestSvc/x", Payload::default(), move |_, _| {
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
            max_inflight_requests: 1,
            ..Default::default()
        };
        let ctx = make_ctx(&config, 30_000);
        let state = ctx.state.clone();
        // Simulate one request already in flight.
        state
            .metrics
            .server_inflight
            .fetch_add(1, Ordering::Relaxed);
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_clone = ran.clone();
        spawn_handler(ctx, "TestSvc/x", Payload::default(), move |_, _| {
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
        let ctx = make_ctx(&tcp_config(), 30_000);
        let state = ctx.state.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        spawn_handler(ctx, "TestSvc/x", Payload::default(), move |_, _| async {
            let _ = done_tx.send(());
        });
        done_rx.await.unwrap();
        // Give the guard drop a moment to run after the handler body.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(state.metrics.server_inflight.load(Ordering::Relaxed), 0);
    }
}
