//! Source validation latency while the original borrowing request remains live.
//!
//! Run with `cargo bench -p ruapc-unsafe --features rdma --bench source_validation`.
//! `RUAPC_BENCH_TRANSPORT` selects TCP, WS, HTTP or RDMA; `RUAPC_BENCH_RDMA_DEVICE`
//! selects the NIC. `RUAPC_BENCH_SOURCE_MODE` selects `safe` or `unchecked`;
//! unset compares both paths in the same binary. `RUAPC_BENCH_SERIAL_ITERS` and
//! `RUAPC_BENCH_WARMUP_ITERS` control the per-case counts. Optional
//! `RUAPC_BENCH_SAMPLES_FILE` creates a CSV
//! of every measured latency; existing files are never overwritten.
//!
//! Four Tokio workers run both peers. The synthetic loaded case adds four CPU
//! tasks, each burning about 50us before yielding. The original source request
//! runs on a worker too. This is a scheduling stress case, not a production SLO.
//! Setup and warmup are excluded. READ timing includes allocation, transport and
//! source validation; complete-payload checks run after each timed READ.
//!
//! Both source modes use the same owned, empty destination vector. Safe sources
//! ask the original future to validate; unchecked sources use the caller's
//! lifetime contract instead. Source-scope creation is outside the timed loops.

use std::{
    fs::{File, OpenOptions},
    hint::black_box,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use ruapc::{
    Buffer, Client, Context, Error, ErrorKind, ListenMode, RawCall, Result, Router, Server,
    SocketPoolConfig, Transport,
};
use ruapc_unsafe::ClientBuffersExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const WORKERS: usize = 4;
const SOURCE_BYTES: usize = 64 * 1024;
const BURN: Duration = Duration::from_micros(50);
const CALL_BUDGET: Duration = Duration::from_secs(120);

struct Options {
    counts: Counts,
    transport: Option<Transport>,
    source_mode: Option<SourceMode>,
    device: Option<String>,
    samples: Option<String>,
}

impl Options {
    fn from_env() -> Self {
        let transport = optional("RUAPC_BENCH_TRANSPORT")
            .map(|value| value.parse().expect("invalid RUAPC_BENCH_TRANSPORT"));
        Self {
            counts: Counts {
                warmup: count("RUAPC_BENCH_WARMUP_ITERS", 200),
                iterations: count("RUAPC_BENCH_SERIAL_ITERS", 10_000),
            },
            transport,
            source_mode: optional("RUAPC_BENCH_SOURCE_MODE").map(|value| match value.as_str() {
                "safe" => SourceMode::Safe,
                "unchecked" => SourceMode::Unchecked,
                _ => panic!("RUAPC_BENCH_SOURCE_MODE must be safe or unchecked"),
            }),
            device: optional("RUAPC_BENCH_RDMA_DEVICE"),
            samples: optional("RUAPC_BENCH_SAMPLES_FILE"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceMode {
    Safe,
    Unchecked,
}

impl SourceMode {
    fn label(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Unchecked => "unchecked",
        }
    }
}

fn optional(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn count(name: &str, default: usize) -> usize {
    let result = optional(name).map_or(default, |value| {
        value.parse().unwrap_or_else(|_| panic!("invalid {name}"))
    });
    assert!(result > 0, "{name} must be positive");
    result
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
struct Counts {
    warmup: usize,
    iterations: usize,
}

// Matches the built-in wire request without exposing a new production API.
#[derive(Serialize, JsonSchema)]
struct RequestStatusRequest {
    request_id: u64,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct Timings {
    probe_ns: Vec<u64>,
    read_ns: Vec<u64>,
}

#[ruapc::service]
trait SourceValidationBench {
    async fn ready(&self, ctx: &Context, request: &()) -> Result<()>;
    async fn measure(&self, ctx: &Context, counts: &Counts) -> Result<Timings>;
}

struct Service {
    expected: Vec<u8>,
}

impl SourceValidationBench for Service {
    async fn ready(&self, _: &Context, _: &()) -> Result<()> {
        Ok(())
    }

    async fn measure(&self, ctx: &Context, counts: &Counts) -> Result<Timings> {
        if ctx.msg_meta.msgid == 0
            || ctx.msg_meta.read_regions.iter().map(|r| r.len).sum::<u64>() != SOURCE_BYTES as u64
        {
            return Err(invalid("request must advertise the complete source"));
        }
        let client = configured_client();
        let request = RequestStatusRequest {
            request_id: ctx.msg_meta.msgid,
        };
        let mut probe_ns = Vec::with_capacity(counts.iterations);
        let mut read_ns = Vec::with_capacity(counts.iterations);

        // All probes address the same original request. A positive response is
        // required; false or transport errors invalidate the measurement.
        for index in 0..counts.warmup + counts.iterations {
            let started = Instant::now();
            let pending: bool = client
                .ruapc_raw_call::<_, bool, Error>(ctx, &request, "MemoryService/request_is_pending")
                .await?;
            let elapsed = nanos(started.elapsed());
            if !pending {
                return Err(invalid("validation rejected the live source request"));
            }
            if index >= counts.warmup {
                probe_ns.push(elapsed);
            }
        }
        for index in 0..counts.warmup + counts.iterations {
            let started = Instant::now();
            let buffers = ctx.remote_read_allocated().await?;
            let elapsed = nanos(started.elapsed());
            verify(&buffers, &self.expected)?;
            if index >= counts.warmup {
                read_ns.push(elapsed);
            }
        }
        Ok(Timings { probe_ns, read_ns })
    }
}

fn configured_client() -> Client {
    Client {
        timeout: CALL_BUDGET,
        max_retries: 0,
        ..Default::default()
    }
}

fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).expect("sample exceeds u64 nanoseconds")
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidArgument, message.into())
}

fn verify(buffers: &[Buffer], expected: &[u8]) -> Result<()> {
    let mut offset = 0;
    for buffer in buffers {
        let end = offset + buffer.len();
        if end > expected.len() || buffer.as_slice() != &expected[offset..end] {
            return Err(invalid("READ payload changed"));
        }
        offset = end;
    }
    if offset != expected.len() {
        return Err(invalid("READ payload is incomplete"));
    }
    Ok(())
}

struct Load {
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Load {
    fn start(loaded: bool) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let tasks = (0..if loaded { WORKERS } else { 0 })
            .map(|index| {
                let stop = stop.clone();
                tokio::spawn(async move {
                    let mut value = index as u64;
                    while !stop.load(Ordering::Relaxed) {
                        let until = Instant::now() + BURN;
                        while Instant::now() < until {
                            for _ in 0..64 {
                                value = black_box(
                                    value.wrapping_mul(6364136223846793005).wrapping_add(1),
                                );
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                    black_box(value);
                })
            })
            .collect();
        Self { stop, tasks }
    }

    async fn finish(self) {
        self.stop.store(true, Ordering::Relaxed);
        for task in self.tasks {
            task.await.expect("synthetic load task panicked");
        }
    }
}

async fn measure(
    ctx: &Context,
    sources: Vec<Buffer>,
    counts: Counts,
    mode: SourceMode,
) -> (Result<Timings>, Vec<Buffer>) {
    let ctx = ctx.clone();
    // Run on a worker, not Runtime::block_on's otherwise uncontended main thread.
    tokio::spawn(async move {
        let client = configured_client();
        let pointer = sources[0].as_ptr() as usize;
        let attached = match mode {
            SourceMode::Safe => client.with_buffers(&sources, Vec::new()),
            SourceMode::Unchecked => {
                // SAFETY: sources remain alive and immutable. The original
                // request and its complete run scope are always awaited.
                unsafe { client.with_buffers_unchecked(&sources, Vec::new()) }
            }
        };
        let completed = attached
            .run(async |attached| attached.measure(&ctx, &counts).await)
            .await;
        assert!(completed.buffers.is_some_and(|buffers| buffers.is_empty()));
        assert_eq!(sources[0].as_ptr() as usize, pointer);
        (completed.result, sources)
    })
    .await
    .expect("source measurement task panicked")
}

#[allow(clippy::cast_precision_loss)]
fn report(
    transport: Transport,
    loaded: bool,
    mode: SourceMode,
    operation: &str,
    samples: &[u64],
    output: &mut Option<File>,
) {
    let label = if loaded { "synthetic_cpu" } else { "idle" };
    let mode = mode.label();
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean = samples.iter().map(|&v| v as f64).sum::<f64>() / samples.len() as f64 / 1000.0;
    let percentile = |p: usize| sorted[(p * sorted.len()).div_ceil(100) - 1] as f64 / 1000.0;
    println!(
        "  {transport:?}/{label}/{mode}/{operation} n={} mean_us={mean:.3} p50_us={:.3} p99_us={:.3} max_us={:.3} verified",
        samples.len(),
        percentile(50),
        percentile(99),
        *sorted.last().unwrap() as f64 / 1000.0
    );
    if let Some(output) = output {
        for (index, value) in samples.iter().enumerate() {
            writeln!(
                output,
                "{transport:?},{label},{mode},{operation},{index},{value}"
            )
            .unwrap();
        }
        output.flush().unwrap();
    }
}

async fn run(options: Options) {
    let expected = (0..SOURCE_BYTES)
        .map(|offset| ((offset.wrapping_mul(31) ^ (offset >> 8)) & 0xff) as u8)
        .collect::<Vec<_>>();
    let service = Arc::new(Service { expected });
    let mut router = Router::default();
    service.clone().ruapc_export(&mut router);
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 512 << 20,
        ..Default::default()
    };
    #[cfg(feature = "rdma")]
    let config = {
        let mut config = config;
        if let Some(device) = &options.device {
            config
                .rdma
                .get_or_insert_with(Default::default)
                .path
                .device_filter = vec![device.clone()];
        }
        config
    };
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = Context::create(&config).unwrap();
    let mut source = base.state.buffer_pool.allocate(SOURCE_BYTES).unwrap();
    source.set_len(SOURCE_BYTES);
    source.copy_from_slice(&service.expected);
    let mut sources = vec![source];
    let mut output = options.samples.as_ref().map(|path| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        writeln!(
            file,
            "transport,load,source_mode,operation,sample,latency_ns"
        )
        .unwrap();
        file
    });
    for (transport, scheme) in [
        (Transport::TCP, "tcp"),
        (Transport::WS, "ws"),
        (Transport::HTTP, "http"),
        #[cfg(feature = "rdma")]
        (Transport::RDMA, "tcp+rdma"),
    ] {
        if options
            .transport
            .is_some_and(|selected| selected != transport)
        {
            continue;
        }
        let ctx = base.with_endpoint(format!("{scheme}://{address}").parse().unwrap());
        configured_client()
            .ready(&ctx, &())
            .await
            .expect("requested transport must be available");
        for loaded in [false, true] {
            for mode in [SourceMode::Safe, SourceMode::Unchecked] {
                if options.source_mode.is_some_and(|selected| selected != mode) {
                    continue;
                }
                let load = Load::start(loaded);
                let (timings, original) =
                    measure(&ctx, sources, options.counts.clone(), mode).await;
                load.finish().await;
                let timings = timings.expect("source measurement failed");
                sources = original;
                verify(&sources, &service.expected).expect("original source changed");
                report(
                    transport,
                    loaded,
                    mode,
                    "probe",
                    &timings.probe_ns,
                    &mut output,
                );
                report(
                    transport,
                    loaded,
                    mode,
                    "read64KiB",
                    &timings.read_ns,
                    &mut output,
                );
            }
        }
    }
    server.stop();
    server.join().await;
}

fn main() {
    let options = Options::from_env();
    #[cfg(not(feature = "rdma"))]
    assert!(
        options.transport != Some(Transport::RDMA),
        "RDMA requires --features rdma"
    );
    println!(
        "source validation: {WORKERS} workers; original source future runs on worker; warmup={} iterations={} source_bytes={SOURCE_BYTES}; transport={:?} source_mode={:?} device={:?}",
        options.counts.warmup,
        options.counts.iterations,
        options.transport,
        options.source_mode,
        options.device
    );
    println!(
        "synthetic CPU case: {WORKERS} local tasks, ~50us burn then yield; not a production SLO; READ includes allocation/validation, excludes payload verification"
    );
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(options));
}
