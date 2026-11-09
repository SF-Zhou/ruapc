//! Compare owned and borrowed destinations over the same verified roundtrip.

use std::{sync::Arc, time::Instant};

use ruapc::{
    Buffer, Client, Completed, Context, ListenMode, Result, Router, Server, SocketPoolConfig,
    Transport,
};
use ruapc_unsafe::ClientBuffersExt;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SIZES: [usize; 2] = [64 * 1024, 1024 * 1024];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Owned,
    Borrowed,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::Borrowed => "borrowed",
        }
    }
}

struct Options {
    iterations: usize,
    warmup: usize,
    transport: Option<Transport>,
    device: Option<String>,
    mode: Option<Mode>,
}

impl Options {
    fn from_env() -> Self {
        Self {
            iterations: count("RUAPC_BENCH_SERIAL_ITERS", 1000),
            warmup: count("RUAPC_BENCH_WARMUP_ITERS", 100),
            transport: optional("RUAPC_BENCH_TRANSPORT")
                .map(|value| value.parse().expect("invalid RUAPC_BENCH_TRANSPORT")),
            device: optional("RUAPC_BENCH_RDMA_DEVICE"),
            mode: optional("RUAPC_BENCH_BUFFER_MODE").map(|value| match value.as_str() {
                "owned" => Mode::Owned,
                "borrowed" => Mode::Borrowed,
                _ => panic!("RUAPC_BENCH_BUFFER_MODE must be owned or borrowed"),
            }),
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
    let count = optional(name).map_or(default, |value| {
        value.parse().unwrap_or_else(|_| panic!("invalid {name}"))
    });
    assert!(count > 0, "{name} must be positive");
    count
}

#[ruapc::service]
trait BufferModeBench {
    async fn probe(&self, ctx: &Context, req: &()) -> Result<()>;
    async fn roundtrip(&self, ctx: &Context, case: &usize) -> Result<usize>;
}

struct Service {
    expected: [Vec<u8>; SIZES.len()],
}

impl Service {
    fn new() -> Self {
        Self {
            expected: SIZES.map(|size| {
                (0..size)
                    .map(|offset| ((offset.wrapping_mul(31) ^ (offset >> 8)) & 0xff) as u8)
                    .collect()
            }),
        }
    }
}

impl BufferModeBench for Service {
    async fn probe(&self, _: &Context, _: &()) -> Result<()> {
        Ok(())
    }

    async fn roundtrip(&self, ctx: &Context, case: &usize) -> Result<usize> {
        let buffers = ctx.remote_read_allocated().await?;
        let expected = &self.expected[*case];
        verify(&buffers, expected);
        ctx.remote_write_all(&buffers).await?;
        Ok(expected.len())
    }
}

fn verify(buffers: &[Buffer], expected: &[u8]) {
    let mut offset = 0;
    for buffer in buffers {
        let end = offset + buffer.len();
        assert!(
            end <= expected.len(),
            "result exceeds the original destination"
        );
        assert!(
            buffer.as_slice() == &expected[offset..end],
            "roundtrip differs from the complete expected pattern"
        );
        offset = end;
    }
    assert_eq!(offset, expected.len(), "incomplete roundtrip");
}

fn make_buffer(ctx: &Context, bytes: &[u8]) -> Vec<Buffer> {
    let mut buffer = ctx.state.buffer_pool.allocate(bytes.len()).unwrap();
    buffer.set_len(bytes.len());
    buffer.copy_from_slice(bytes);
    vec![buffer]
}

async fn call(
    mode: Mode,
    client: &Client,
    ctx: &Context,
    case: usize,
    read: &[Buffer],
    write: &mut Vec<Buffer>,
) -> usize {
    let result = match mode {
        Mode::Owned => {
            let Completed { result, buffers } = client
                .with_buffers(read, std::mem::take(write))
                .run(async |attached| attached.roundtrip(ctx, &case).await)
                .await;
            *write = buffers.expect("benchmark destinations must be recovered");
            result
        }
        Mode::Borrowed => {
            // SAFETY: this serial workload awaits every call and keeps the
            // destinations alive and exclusive until the call returns.
            unsafe { client.borrow_buffers(read, write) }
                .run(async |attached| attached.roundtrip(ctx, &case).await)
                .await
        }
    };
    result.expect("roundtrip must complete")
}

async fn measure(mode: Mode, ctx: &Context, case: usize, expected: &[u8], options: &Options) {
    let client = Client::default();
    let read = make_buffer(ctx, expected);
    let mut write = make_buffer(ctx, &vec![0; expected.len()]);
    let pointers = [read[0].as_ptr(), write[0].as_ptr()];
    for _ in 0..options.warmup {
        assert_eq!(
            call(mode, &client, ctx, case, &read, &mut write).await,
            expected.len()
        );
        verify(&write, expected);
    }
    let started = Instant::now();
    for _ in 0..options.iterations {
        assert_eq!(
            call(mode, &client, ctx, case, &read, &mut write).await,
            expected.len()
        );
        verify(&write, expected);
    }
    let elapsed = started.elapsed();
    assert_eq!([read[0].as_ptr(), write[0].as_ptr()], pointers);
    verify(&read, expected);
    #[allow(clippy::cast_precision_loss)]
    let us_per_op = elapsed.as_secs_f64() * 1e6 / options.iterations as f64;
    println!(
        "  roundtrip/{:<8} {:>4} KiB | {} iters | {:.2} us/op | elapsed_ns={} | verified",
        mode.label(),
        expected.len() / 1024,
        options.iterations,
        us_per_op,
        elapsed.as_nanos(),
    );
}

async fn run(options: Options) {
    let service = Arc::new(Service::new());
    let mut router = Router::default();
    service.clone().ruapc_export(&mut router);
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 512 * 1024 * 1024,
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
        println!("{transport:?}");
        let ctx = base.with_endpoint(format!("{scheme}://{address}").parse().unwrap());
        if let Err(error) = Client::default().probe(&ctx, &()).await {
            #[cfg(feature = "rdma")]
            if transport == Transport::RDMA {
                println!("  skipped: {error}");
                continue;
            }
            panic!("{transport:?} setup failed: {error}");
        }
        for (case, expected) in service.expected.iter().enumerate() {
            for mode in [Mode::Owned, Mode::Borrowed] {
                if options.mode.is_none_or(|selected| selected == mode) {
                    measure(mode, &ctx, case, expected, &options).await;
                }
            }
        }
    }
    server.stop();
    server.join().await;
}

fn main() {
    let options = Options::from_env();
    println!("ruapc: owned/borrowed verified remote-memory roundtrip");
    println!(
        "4 runtime workers; warmup={}; iterations={}; transport={:?}; device={:?}; mode={:?}",
        options.warmup, options.iterations, options.transport, options.device, options.mode,
    );
    println!("setup excluded; source/destination allocations reused; both copies verified");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(options));
}
