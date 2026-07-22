//! Production-readiness tests: metrics emission, deadline propagation,
//! run-to-completion semantics, load shedding, and multi-address failover.

#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use ruapc::{ErrorKind, SocketPoolConfig, SocketType};

/// Installs a process-wide debugging recorder (once) and returns its
/// snapshotter. Must be called before the traffic whose metrics are
/// asserted: facade handles bind to the recorder installed at first use.
///
/// `Snapshotter::snapshot()` *drains* recorded values, so only one test in
/// this binary may take snapshots (`test_metrics_emission`) — concurrent
/// snapshots would steal each other's deltas.
fn global_snapshotter() -> &'static Snapshotter {
    static SNAPSHOTTER: std::sync::OnceLock<Snapshotter> = std::sync::OnceLock::new();
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        snapshotter
    })
}

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// Sums all values in `snapshot` for `name` whose labels contain `label`.
/// Histograms count samples. Returns `None` when the metric never appeared.
fn metric_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> Option<u64> {
    let mut found = false;
    let mut sum = 0u64;
    for (key, _, _, value) in snapshot {
        let key = key.key();
        if key.name() != name {
            continue;
        }
        if let Some((k, v)) = label
            && !key.labels().any(|l| l.key() == k && l.value() == v)
        {
            continue;
        }
        found = true;
        sum += match value {
            DebugValue::Counter(c) => *c,
            DebugValue::Gauge(g) => g.0 as u64,
            DebugValue::Histogram(h) => h.len() as u64,
        };
    }
    found.then_some(sum)
}

#[ruapc::service]
trait Prod {
    async fn echo(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
    /// Like `echo`, but only used by the metrics test so its counters are
    /// isolated from other tests sharing the process-wide recorder.
    async fn metered(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
    /// Returns the remaining request budget in milliseconds
    /// (`u64::MAX` when the request carries none).
    async fn budget(&self, _: &ruapc::Context, req: &()) -> ruapc::Result<u64>;
    async fn sleepy(&self, _: &ruapc::Context, req: &u64) -> ruapc::Result<()>;
}

#[derive(Default)]
struct ProdImpl {
    sleepy_started: AtomicU64,
    sleepy_finished: AtomicU64,
}

impl Prod for ProdImpl {
    async fn echo(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(req.clone())
    }

    async fn metered(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(req.clone())
    }

    async fn budget(&self, ctx: &ruapc::Context, (): &()) -> ruapc::Result<u64> {
        Ok(ctx
            .remaining_time()
            .map_or(u64::MAX, |d| u64::try_from(d.as_millis()).unwrap()))
    }

    async fn sleepy(&self, _: &ruapc::Context, req: &u64) -> ruapc::Result<()> {
        self.sleepy_started.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(*req)).await;
        self.sleepy_finished.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn unified_config() -> SocketPoolConfig {
    SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    }
}

fn make_server(service: Arc<ProdImpl>, config: &SocketPoolConfig) -> ruapc::Server {
    let mut router = ruapc::Router::default();
    service.ruapc_export(&mut router);
    ruapc::Server::create(router, config).unwrap()
}

async fn start(config: &SocketPoolConfig) -> (Arc<ProdImpl>, ruapc::Server, SocketAddr) {
    let service = Arc::new(ProdImpl::default());
    let server = make_server(service.clone(), config);
    let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
    let addr = server.listen(addr).await.unwrap();
    (service, server, addr)
}

/// RPC traffic must be observable through any user-installed
/// [`metrics::Recorder`] (here: `metrics-util`'s debugging recorder).
#[tokio::test]
async fn test_metrics_emission() {
    let _ = tracing_subscriber::fmt().try_init();
    // Install the recorder *before* any traffic.
    let snapshotter = global_snapshotter();

    let config = unified_config();
    let (_service, server, addr) = start(&config).await;

    let client = ruapc::Client::default();
    let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);
    for _ in 0..3 {
        client.metered(&ctx, &"x".to_string()).await.unwrap();
    }

    // Let the last handler's guard finish its bookkeeping, then take ONE
    // snapshot (snapshots drain the recorder).
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snapshot = snapshotter.snapshot().into_vec();

    let label = Some(("method", "Prod/metered"));
    // Server side: requests counted, none failed, latency samples recorded.
    assert_eq!(
        metric_sum(&snapshot, "ruapc_server_requests_total", label),
        Some(3)
    );
    assert_eq!(
        metric_sum(&snapshot, "ruapc_server_errors_total", label).unwrap_or(0),
        0
    );
    assert_eq!(
        metric_sum(&snapshot, "ruapc_server_latency_seconds", label),
        Some(3)
    );
    assert_eq!(
        metric_sum(&snapshot, "ruapc_server_inflight", label),
        Some(0)
    );
    // Client side mirrors it.
    assert_eq!(
        metric_sum(&snapshot, "ruapc_client_requests_total", label),
        Some(3)
    );
    assert_eq!(
        metric_sum(&snapshot, "ruapc_client_latency_seconds", label),
        Some(3)
    );
    // Connection gauges were emitted for the live transport.
    assert!(metric_sum(&snapshot, "ruapc_connections", None).is_some());

    server.stop();
    server.join().await;
}

/// The client's time budget must reach the server and be visible as a
/// deadline in the handler context.
#[tokio::test]
async fn test_deadline_propagation() {
    let _ = tracing_subscriber::fmt().try_init();
    let config = unified_config();
    let (_service, server, addr) = start(&config).await;

    let client = ruapc::Client {
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);
    let remaining_ms = client.budget(&ctx, &()).await.unwrap();
    assert!(
        remaining_ms > 0 && remaining_ms <= 5000,
        "server should see a deadline derived from the 5s client budget, got {remaining_ms}ms"
    );

    server.stop();
    server.join().await;
}

/// A client timeout must not affect the server-side handler: started
/// requests always run to completion (no mid-flight cancellation).
#[tokio::test]
async fn test_handler_runs_to_completion_after_client_timeout() {
    let _ = tracing_subscriber::fmt().try_init();
    let config = unified_config();
    let (service, server, addr) = start(&config).await;

    let client = ruapc::Client {
        timeout: Duration::from_millis(300),
        socket_type: Some(SocketType::TCP),
        ..Default::default()
    };
    let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);

    // 1s of work against a 300ms budget: the client times out...
    let err = client.sleepy(&ctx, &1_000).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Timeout);
    assert_eq!(service.sleepy_started.load(Ordering::SeqCst), 1);
    assert_eq!(service.sleepy_finished.load(Ordering::SeqCst), 0);

    // ...but the handler still finishes its work.
    let mut finished = false;
    for _ in 0..50 {
        if service.sleepy_finished.load(Ordering::SeqCst) == 1 {
            finished = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(finished, "handler should have run to completion");

    server.stop();
    server.join().await;
}

/// Requests beyond `max_inflight_requests` must be rejected immediately
/// with `Overloaded` instead of queueing up.
#[tokio::test]
async fn test_load_shedding() {
    let _ = tracing_subscriber::fmt().try_init();
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        max_inflight_requests: 2,
        ..Default::default()
    };
    let (_service, server, addr) = start(&config).await;

    let client_config = unified_config();
    let ctx = ruapc::Context::create(&client_config)
        .unwrap()
        .with_addr(addr);

    // Fill the two slots, then hit the cap.
    let occupants: Vec<_> = (0..2)
        .map(|_| {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let client = ruapc::Client {
                    timeout: Duration::from_secs(10),
                    ..Default::default()
                };
                client.sleepy(&ctx, &2_000).await
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ruapc::Client {
        timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let started = std::time::Instant::now();
    let err = client.echo(&ctx, &"nope".to_string()).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Overloaded, "err={err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "shedding must reject immediately"
    );

    for occupant in occupants {
        occupant.await.unwrap().unwrap();
    }
    // Capacity freed: requests pass again.
    client.echo(&ctx, &"ok".to_string()).await.unwrap();

    server.stop();
    server.join().await;
}

/// With a multi-address context, connect failures must fail over to the
/// next address transparently.
#[tokio::test]
async fn test_multi_address_failover() {
    let _ = tracing_subscriber::fmt().try_init();
    let config = unified_config();
    let (_service, server, addr) = start(&config).await;

    // A dead address (nothing listens there) plus the live server.
    let dead = SocketAddr::from_str("127.0.0.1:1").unwrap();
    let ctx = ruapc::Context::create(&config)
        .unwrap()
        .with_addrs(vec![dead, addr]);

    let client = ruapc::Client {
        socket_type: Some(SocketType::TCP),
        ..Default::default()
    };
    // Round-robin alternates the starting address; every request must
    // still succeed thanks to connect-phase failover.
    for i in 0..4 {
        let rsp = client.echo(&ctx, &format!("try{i}")).await.unwrap();
        assert_eq!(rsp, format!("try{i}"));
    }

    server.stop();
    server.join().await;
}
