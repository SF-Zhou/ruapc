use clap::Parser;
use hdrhistogram::Histogram;
use ruapc::{Buffer, Client, ClientWithBuffers, Context, Endpoint, Error, ErrorKind, Result};
use ruapc_demo::{
    EchoService, GreetService, MemBenchService, ReadCrcReq, Request, WriteCrcReq,
    app::{PoolOptions, RuntimeOptions, init_tracing},
    crc32c_of, fill_pattern,
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Parser, Debug, Clone)]
#[command(
    version,
    about = "Call the demo services or run a verified RPC workload"
)]
struct Args {
    /// RPC endpoint.
    #[arg(default_value = "tcp://127.0.0.1:8000")]
    endpoint: Endpoint,

    /// Request value (one-shot and stress echo).
    #[arg(short, long, default_value = "alice")]
    value: String,

    /// Use MessagePack instead of JSON.
    #[arg(long)]
    use_msgpack: bool,

    /// HTTP base path, matching the server (e.g. /api).
    #[arg(long, default_value = "")]
    http_base_path: String,

    /// Run a throughput workload with periodic progress.
    #[arg(long, conflicts_with = "bench")]
    stress: bool,

    /// Report throughput and latency percentiles; use --coroutines 1 for ping-pong.
    #[arg(long)]
    bench: bool,

    /// Load duration in seconds, including benchmark warmup; excludes setup.
    #[arg(long, default_value = "60", value_parser = clap::value_parser!(u64).range(1..))]
    secs: u64,

    /// Number of concurrent load workers.
    #[arg(long, default_value = "32")]
    coroutines: NonZeroUsize,

    /// Echo payload size in bytes (bench mode).
    #[arg(long, default_value = "1024")]
    payload_size: usize,

    /// RPC to exercise; read/write verify CRC32C on every call.
    #[arg(long, value_enum, default_value_t = Rpc::Echo)]
    rpc: Rpc,

    /// Read/write buffer length per worker, from 0 to 64 MiB.
    #[arg(long, default_value = "1048576")]
    buffer_size: usize,

    /// Initial seconds excluded from benchmark measurements.
    #[arg(long, default_value = "3")]
    warmup_secs: u64,

    #[command(flatten)]
    runtime: RuntimeOptions,

    #[command(flatten)]
    pool: PoolOptions,
}

impl Args {
    fn validate(&self) -> Result<()> {
        let message = if self.bench && self.secs <= self.warmup_secs {
            "--secs must be larger than --warmup-secs in bench mode"
        } else if self.buffer_size > 64 * 1024 * 1024 {
            "--buffer-size must not exceed 64 MiB"
        } else {
            return Ok(());
        };
        Err(Error::new(ErrorKind::InvalidArgument, message.into()))
    }

    fn context(&self) -> Result<Context> {
        #[cfg(feature = "rdma")]
        let enable_rdma = self.endpoint.transport() == ruapc::Transport::RDMA;
        #[cfg(not(feature = "rdma"))]
        let enable_rdma = false;
        let mut config = self.pool.config(enable_rdma);
        config.http_base_path.clone_from(&self.http_base_path);
        Ok(Context::create(&config)?.with_endpoint(self.endpoint))
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Rpc {
    /// Plain payload echo.
    Echo,
    /// Server reads the client's buffers and returns their CRC32C.
    Read,
    /// Server writes into the client's buffers and returns their CRC32C.
    Write,
}

// Each worker owns one operation; prepared buffers are reused across calls.
// Read sources remain immutable, and failed writes recover buffers only
// after RuaPC has released their ownership.
struct Workload<'a> {
    client: &'a Client,
    operation: Operation<'a>,
}

enum Operation<'a> {
    Echo(Request),
    Read {
        source: ClientWithBuffers<'a>,
        expected_crc: u32,
    },
    Write {
        buffers: Option<Vec<Buffer>>,
        len: usize,
    },
}

impl<'a> Workload<'a> {
    fn create(
        client: &'a Client,
        rpc: Rpc,
        ctx: &Context,
        payload: Request,
        buffer_size: usize,
        seed: u64,
    ) -> Result<Self> {
        let operation = match rpc {
            Rpc::Echo => Operation::Echo(payload),
            Rpc::Read => {
                let mut buffer = allocate(ctx, buffer_size)?;
                fill_pattern(&mut buffer, seed);
                let expected_crc = crc32c_of([&buffer]);
                Operation::Read {
                    source: client.with_read_buffer(buffer),
                    expected_crc,
                }
            }
            Rpc::Write => Operation::Write {
                buffers: Some(vec![allocate(ctx, buffer_size)?]),
                len: buffer_size,
            },
        };
        Ok(Self { client, operation })
    }

    async fn call(&mut self, ctx: &Context) -> Result<()> {
        match &mut self.operation {
            Operation::Echo(payload) => {
                let response = self.client.echo(ctx, payload).await?;
                if response != payload.0 {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        "echo response does not match the request".into(),
                    ));
                }
                Ok(())
            }
            Operation::Read {
                source,
                expected_crc,
            } => {
                let crc = source.read_crc(ctx, &ReadCrcReq {}).await?;
                verify_crc(crc, *expected_crc)
            }
            Operation::Write { buffers, len } => {
                let destination = match buffers.take() {
                    Some(buffers) => buffers,
                    None => vec![allocate(ctx, *len)?],
                };
                let call = self.client.with_write_buffers(destination);
                match call.write_crc(ctx, &WriteCrcReq { len: *len }).await {
                    Ok(response) => {
                        let (expected_crc, returned) = response.into_parts();
                        let received_len: usize = returned.iter().map(|buffer| buffer.len()).sum();
                        let crc = crc32c_of(&returned);
                        *buffers = Some(returned);
                        if received_len != *len {
                            return Err(Error::new(
                                ErrorKind::InvalidArgument,
                                format!("write returned {received_len} bytes, expected {len}"),
                            ));
                        }
                        verify_crc(crc, expected_crc)
                    }
                    Err(error) => {
                        // A failed transfer may leave the target empty even
                        // after DMA settles. Allocate replacements if needed.
                        *buffers = call.take_write_buffers();
                        Err(error)
                    }
                }
            }
        }
    }
}

fn allocate(ctx: &Context, len: usize) -> Result<Buffer> {
    // Fail immediately if the pool cannot hold all workers' buffers. Waiting
    // for capacity here can deadlock workers at the load-test start barrier.
    let mut buffer = ctx
        .state
        .buffer_pool
        .allocate(len.max(1))
        .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))?;
    buffer.set_len(len);
    Ok(buffer)
}

fn verify_crc(actual: u32, expected: u32) -> Result<()> {
    if actual != expected {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("crc32c mismatch: got {actual:#010x}, expect {expected:#010x}"),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct Counters {
    total: AtomicU64,
    fails: AtomicU64,
}

fn histogram() -> Histogram<u64> {
    Histogram::new(3).expect("three significant digits is a valid histogram configuration")
}

async fn load_test(args: Args, ctx: Context) -> Result<()> {
    let counters = Arc::new(Counters::default());
    let ready = Arc::new(tokio::sync::Barrier::new(args.coroutines.get()));
    let start = Arc::new(OnceLock::new());
    let warmup = Duration::from_secs(if args.bench { args.warmup_secs } else { 0 });
    let duration = Duration::from_secs(args.secs);
    let mut tasks = tokio::task::JoinSet::new();
    let mut finished = Instant::now();
    for seed in 0..args.coroutines.get() {
        let (ctx, args, counters, ready, start) = (
            ctx.clone(),
            args.clone(),
            counters.clone(),
            ready.clone(),
            start.clone(),
        );
        tasks.spawn(async move {
            let client = Client {
                timeout: Duration::from_secs(5),
                use_msgpack: args.use_msgpack,
                ..Default::default()
            };
            let payload = if args.bench && args.rpc == Rpc::Echo {
                Request("x".repeat(args.payload_size))
            } else {
                Request(args.value)
            };
            let mut op = Workload::create(
                &client,
                args.rpc,
                &ctx,
                payload,
                args.buffer_size,
                seed as u64,
            )?;
            let mut latencies = args.bench.then(histogram);
            let mut first_error = None;
            ready.wait().await;
            // Start together after workload and histogram preparation.
            let start = *start.get_or_init(Instant::now);
            loop {
                let sent = Instant::now();
                let elapsed = sent.duration_since(start);
                if elapsed >= duration {
                    break;
                }
                let result = op.call(&ctx).await;
                // Classify by request start time and include its full latency,
                // even when an in-flight request finishes after the stop time.
                if elapsed >= warmup {
                    counters.total.fetch_add(1, Ordering::Relaxed);
                    if result.is_err() {
                        counters.fails.fetch_add(1, Ordering::Relaxed);
                    } else if let Some(hist) = &mut latencies {
                        let nanos = sent.elapsed().as_nanos().clamp(1, u128::from(u64::MAX)) as u64;
                        hist.record(nanos).map_err(std::io::Error::other)?;
                    }
                }
                if let Err(error) = result {
                    first_error.get_or_insert(error);
                }
            }
            Ok::<_, Error>((latencies, first_error, Instant::now()))
        });
    }

    let mut merged = histogram();
    let mut first_error = None;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let (mut previous_total, mut previous_fails, mut last_tick) = (0, 0, Instant::now());
    while !tasks.is_empty() {
        tokio::select! {
            Some(result) = tasks.join_next() => {
                // On preparation failure, dropping JoinSet aborts workers
                // still waiting at the barrier.
                let (hist, error, end) = result.map_err(std::io::Error::other)??;
                finished = finished.max(end);
                if let Some(hist) = hist {
                    merged.add(hist).map_err(std::io::Error::other)?;
                }
                first_error = first_error.or(error);
            }
            _ = interval.tick(), if args.stress => {
                if let Some(start) = start.get() {
                    let now = Instant::now();
                    let seconds = now.duration_since(last_tick.max(*start)).as_secs_f64();
                    let total = counters.total.load(Ordering::Relaxed);
                    let fails = counters.fails.load(Ordering::Relaxed);
                    tracing::info!("QPS: {:.0}/s, fails: {}", (total - previous_total) as f64 / seconds, fails - previous_fails);
                    (previous_total, previous_fails, last_tick) = (total, fails, now);
                }
            }
        }
    }
    let total = counters.total.load(Ordering::Relaxed);
    let fails = counters.fails.load(Ordering::Relaxed);
    let seconds = (finished.duration_since(*start.get().expect("workers have started")) - warmup)
        .as_secs_f64();
    let rate = (total - fails) as f64 / seconds;
    tracing::info!(
        "load: endpoint={} rpc={:?} coroutines={} duration={}s warmup={}s measured={seconds:.3}s",
        args.endpoint,
        args.rpc,
        args.coroutines,
        args.secs,
        warmup.as_secs(),
    );
    tracing::info!(
        "requests: {} ok, {fails} fails, {rate:.0} req/s",
        total - fails
    );
    if args.rpc != Rpc::Echo {
        tracing::info!(
            "throughput: {:.1} MiB/s",
            rate * args.buffer_size as f64 / (1024.0 * 1024.0)
        );
    }
    if !merged.is_empty() {
        let p = |q: f64| merged.value_at_quantile(q) as f64 / 1_000.0;
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
    // Warmup failures also fail the command, although excluded from measurements.
    first_error.map_or(Ok(()), Err)
}

fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    init_tracing();
    args.runtime.build()?.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
    let ctx = args.context()?;
    if args.bench || args.stress {
        return load_test(args, ctx).await;
    }
    let client = Client {
        use_msgpack: args.use_msgpack,
        ..Default::default()
    };
    let request = Request(args.value);
    match args.rpc {
        Rpc::Echo => {
            let response = client.echo(&ctx, &request).await?;
            tracing::info!("echo rsp: {response:?}");
            let response = client.greet(&ctx, &request).await?;
            tracing::info!("greet rsp: {response:?}");
        }
        Rpc::Read | Rpc::Write => {
            Workload::create(&client, args.rpc, &ctx, request, args.buffer_size, 0)?
                .call(&ctx)
                .await?;
            tracing::info!(
                "{:?} of {} bytes: crc32c verified",
                args.rpc,
                args.buffer_size
            );
        }
    }
    Ok(())
}
