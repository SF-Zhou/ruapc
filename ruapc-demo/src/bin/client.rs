use clap::Parser;
use ruapc::{Client, Context, Endpoint};
use ruapc_demo::{
    EchoService, GreetService, Request,
    app::{PoolOptions, RuntimeOptions, init_tracing},
    workload::{Rpc, Workload},
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// RPC endpoint.
    #[arg(default_value = "tcp://127.0.0.1:8000")]
    pub endpoint: Endpoint,

    /// Request value.
    #[arg(short, long, default_value = "alice")]
    pub value: String,

    /// Use MessagePack
    #[arg(long, default_value_t = false)]
    pub use_msgpack: bool,

    /// Enable stress testing.
    #[arg(long, default_value_t = false, conflicts_with = "bench")]
    pub stress: bool,

    /// Enable latency benchmark (reports throughput and latency percentiles).
    /// Use `--coroutines 1` for pure ping-pong latency.
    #[arg(long, default_value_t = false)]
    pub bench: bool,

    /// Stress/bench testing duration.
    #[arg(long, default_value = "60")]
    pub secs: u64,

    /// The number of coroutines.
    #[arg(long, default_value = "32")]
    pub coroutines: NonZeroUsize,

    /// Request payload size in bytes (bench mode).
    #[arg(long, default_value = "1024")]
    pub payload_size: usize,

    /// RPC to exercise: echo, read (server remote-reads our buffer and
    /// returns its CRC32C), write (server remote-writes our buffer and
    /// returns the CRC32C). read/write verify the checksum on every call.
    #[arg(long, value_enum, default_value_t = Rpc::Echo)]
    pub rpc: Rpc,

    /// Buffer length in bytes for --rpc read/write (per coroutine, max 64 MiB).
    #[arg(long, default_value = "1048576")]
    pub buffer_size: usize,

    /// Warmup duration in seconds before recording latency (bench mode).
    #[arg(long, default_value = "3")]
    pub warmup_secs: u64,

    #[command(flatten)]
    pub runtime: RuntimeOptions,

    #[command(flatten)]
    pub pool: PoolOptions,
}

impl Args {
    fn context(&self) -> Context {
        #[cfg(feature = "rdma")]
        let enable_rdma = self.endpoint.transport() == ruapc::Transport::RDMA;
        #[cfg(not(feature = "rdma"))]
        let enable_rdma = false;
        Context::create(&self.pool.config(enable_rdma))
            .expect("failed to create RPC context")
            .with_endpoint(self.endpoint)
    }

    async fn workload<'a>(
        &self,
        client: &'a Client,
        ctx: &Context,
        payload: Request,
        seed: usize,
    ) -> Workload<'a> {
        Workload::create(
            client,
            self.rpc,
            ctx,
            payload,
            self.buffer_size,
            seed as u64,
        )
        .await
        .expect("failed to prepare RPC workload")
    }
}

#[derive(Default)]
struct Counters {
    total: AtomicUsize,
    fails: AtomicUsize,
}

async fn stress_test(args: Args) {
    let state = Arc::new(Counters::default());
    let start_time = std::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    let ctx = args.context();
    for i in 0..args.coroutines.get() {
        let state = state.clone();
        let ctx = ctx.clone();
        let args = args.clone();
        tasks.spawn(async move {
            let client = Client {
                timeout: Duration::from_secs(5),
                use_msgpack: args.use_msgpack,
                ..Default::default()
            };
            let mut op = args
                .workload(&client, &ctx, Request(args.value.clone()), i)
                .await;
            while start_time.elapsed().as_secs() < args.secs {
                for _ in 0..256 {
                    let result = op.call(&ctx).await;
                    state.total.fetch_add(1, Ordering::Relaxed);
                    if result.is_err() {
                        state.fails.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    while !tasks.is_empty() {
        tokio::select! {
            Some(result) = tasks.join_next() => {
                result.expect("stress worker failed");
            }
            _ = interval.tick() => {
                let total = state.total.swap(0, Ordering::Relaxed);
                let fails = state.fails.swap(0, Ordering::Relaxed);
                tracing::info!("QPS: {total}/s, fails: {fails}/s");
            }
        }
    }
}

async fn bench_test(args: Args) {
    let ctx = args.context();

    let payload = Request("x".repeat(args.payload_size));
    let warmup = Duration::from_secs(args.warmup_secs);
    let total = Duration::from_secs(args.secs);
    assert!(total > warmup, "--secs must be larger than --warmup-secs");
    let ready = Arc::new(tokio::sync::Barrier::new(args.coroutines.get()));

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..args.coroutines.get() {
        let ctx = ctx.clone();
        let payload = payload.clone();
        let args = args.clone();
        let ready = ready.clone();
        tasks.spawn(async move {
            // Latency in nanoseconds, 1ns..60s, 3 significant digits.
            let mut hist = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                .expect("failed to create histogram");
            let client = Client {
                timeout: Duration::from_secs(5),
                use_msgpack: args.use_msgpack,
                ..Default::default()
            };
            let mut op = args.workload(&client, &ctx, payload, i).await;
            // Every worker has allocated and initialized its buffers before
            // warmup begins, so pool growth cannot consume measurement time.
            ready.wait().await;
            let start = std::time::Instant::now();
            let mut fails = 0u64;
            loop {
                let elapsed = start.elapsed();
                if elapsed >= total {
                    break;
                }
                let t = std::time::Instant::now();
                let result = op.call(&ctx).await;
                let nanos = t.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                if elapsed < warmup {
                    continue;
                }
                if result.is_err() {
                    fails += 1;
                } else {
                    let _ = hist.record(nanos.max(1));
                }
            }
            (hist, fails)
        });
    }

    let mut merged = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
        .expect("failed to create histogram");
    let mut fails = 0u64;
    while let Some(task) = tasks.join_next().await {
        let (hist, f) = task.expect("benchmark worker failed");
        merged.add(hist).unwrap();
        fails += f;
    }

    let measured_secs = (args.secs - args.warmup_secs) as f64;
    let p = |q: f64| merged.value_at_quantile(q) as f64 / 1_000.0;
    tracing::info!(
        "bench: endpoint={} rpc={:?} payload={}B buffer={}B coroutines={} duration={}s (warmup {}s)",
        args.endpoint,
        args.rpc,
        args.payload_size,
        args.buffer_size,
        args.coroutines,
        args.secs,
        args.warmup_secs,
    );
    let req_per_sec = merged.len() as f64 / measured_secs;
    match args.rpc {
        Rpc::Echo => tracing::info!(
            "requests: {} ok, {} fails, {:.0} req/s",
            merged.len(),
            fails,
            req_per_sec,
        ),
        Rpc::Read | Rpc::Write => tracing::info!(
            "requests: {} ok, {} fails, {:.0} req/s, {:.1} MiB/s",
            merged.len(),
            fails,
            req_per_sec,
            req_per_sec * args.buffer_size as f64 / (1024.0 * 1024.0),
        ),
    }
    tracing::info!(
        "latency(µs): mean={:.1} min={:.1} p50={:.1} p90={:.1} p99={:.1} p99.9={:.1} p99.99={:.1} max={:.1}",
        merged.mean() / 1_000.0,
        merged.min() as f64 / 1_000.0,
        p(0.50),
        p(0.90),
        p(0.99),
        p(0.999),
        p(0.9999),
        merged.max() as f64 / 1_000.0,
    );
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    init_tracing();
    let args = Args::parse();
    let runtime = args.runtime.build();
    runtime.block_on(async_main(args));
}

async fn async_main(args: Args) {
    if args.bench {
        bench_test(args).await;
    } else if args.stress {
        stress_test(args).await;
    } else {
        let ctx = args.context();
        let client = Client {
            use_msgpack: args.use_msgpack,
            ..Default::default()
        };
        match args.rpc {
            Rpc::Echo => {
                let rsp = client.echo(&ctx, &Request(args.value.clone())).await;
                tracing::info!("echo rsp: {:?}", rsp);

                let rsp = client.greet(&ctx, &Request(args.value.clone())).await;
                tracing::info!("greet rsp: {:?}", rsp);
            }
            Rpc::Read | Rpc::Write => {
                let mut op = args
                    .workload(&client, &ctx, Request(args.value.clone()), 0)
                    .await;
                match op.call(&ctx).await {
                    Ok(()) => tracing::info!(
                        "{:?} of {} bytes: crc32c verified",
                        args.rpc,
                        args.buffer_size
                    ),
                    Err(e) => tracing::error!("{:?} failed: {e:?}", args.rpc),
                }
            }
        }
    }
}
