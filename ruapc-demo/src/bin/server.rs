use clap::Parser;
#[cfg(feature = "rdma")]
use ruapc::RdmaSocketPoolConfig;
use ruapc::{
    Context, Error, ErrorKind, ListenMode, Result, Router, Server, SocketPoolConfig, WithBuffers,
};
use ruapc_demo::{
    EchoService, GreetService, MemBenchService, ReadCrcReq, Request, WriteCrcReq, crc32c_of,
    fill_pattern,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Listen address.
    #[arg(default_value = "0.0.0.0:8000")]
    pub addr: std::net::SocketAddr,

    /// Listener protocol mode.
    #[arg(long, default_value = "unified")]
    pub listen_mode: ListenMode,

    /// Base path for HTTP RPC and documentation endpoints.
    #[arg(long, default_value = "")]
    pub http_base_path: String,

    /// RDMA: number of (CQ + poll thread) shards per device.
    #[arg(long, default_value = "1")]
    pub poll_threads: u32,

    /// RDMA: comma-separated device allowlist (e.g. "mlx5_0").
    #[arg(long, value_delimiter = ',')]
    pub rdma_devices: Vec<String>,

    /// Buffer pool memory limit in MiB (0 = library default).
    #[arg(long, default_value = "0")]
    pub pool_mem_mb: usize,

    /// Tokio worker threads (0 = number of CPUs).
    #[arg(long, default_value = "0")]
    pub worker_threads: usize,

    /// RDMA: poll-thread busy-poll window in microseconds.
    #[arg(long, default_value = "50")]
    pub poll_spin_us: u64,

    /// RDMA: number of dispatch worker tasks shared by all poll threads.
    #[arg(long, default_value = "32")]
    pub dispatch_workers: u32,

    /// RDMA: receive ring depth per connection (negotiated to the minimum
    /// of both sides); the send window is half of it. Small values force
    /// aggregation under load; raise for large-message pipelines.
    #[arg(long, default_value = "8")]
    pub recv_queue_len: u32,
}

#[derive(Default)]
struct DemoImpl {
    idx: AtomicU64,
}

impl EchoService for DemoImpl {
    async fn echo(&self, _c: &Context, r: &Request) -> Result<String> {
        Ok(r.0.clone())
    }
}

impl GreetService for DemoImpl {
    async fn greet(&self, _c: &Context, r: &Request) -> Result<String> {
        let val = self.idx.fetch_add(1, Ordering::AcqRel);
        Ok(format!("hello {}({})!", r.0, val))
    }
}

impl MemBenchService for DemoImpl {
    async fn read_crc(&self, ctx: &Context, _r: &ReadCrcReq) -> Result<u32> {
        let data = ctx.remote_read_all().await?;
        Ok(crc32c_of(&data))
    }

    async fn write_crc(&self, ctx: &Context, r: &WriteCrcReq) -> Result<WithBuffers<u32>> {
        if r.len == 0 {
            return Ok(ctx.sent_nothing().reply(crc32c_of([b"".as_slice()])));
        }
        let mut buf = ctx
            .state
            .buffer_pool
            .async_allocate(r.len)
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        let seed = self.idx.fetch_add(1, Ordering::AcqRel);
        fill_pattern(&mut buf[..r.len], seed);
        buf.set_len(r.len);
        let crc = crc32c_of([&buf]);
        let sent = ctx.remote_write_all(vec![buf]).await?;
        Ok(sent.reply(crc))
    }
}

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if args.worker_threads > 0 {
        builder.worker_threads(args.worker_threads);
    }
    let runtime = builder.build().expect("failed to build tokio runtime");
    runtime.block_on(async_main(args));
}

async fn async_main(args: Args) {
    let demo = Arc::new(DemoImpl::default());
    let mut router = Router::default();
    EchoService::ruapc_export(demo.clone(), &mut router);
    GreetService::ruapc_export(demo.clone(), &mut router);
    MemBenchService::ruapc_export(demo.clone(), &mut router);
    #[allow(unused_mut)]
    let mut config = SocketPoolConfig {
        listen_mode: args.listen_mode,
        buffer_pool_memory: args.pool_mem_mb * 1024 * 1024,
        http_base_path: args.http_base_path,
        ..Default::default()
    };
    #[cfg(feature = "rdma")]
    {
        config.rdma = Some(RdmaSocketPoolConfig {
            poll_threads_per_device: args.poll_threads,
            device_filter: args.rdma_devices.clone(),
            poll_spin_us: args.poll_spin_us,
            dispatch_workers: args.dispatch_workers,
            recv_queue_len: args.recv_queue_len,
            ..Default::default()
        });
    }
    let server = Server::create(router, &config).unwrap();

    let server = Arc::new(server);
    let addr = server.listen(args.addr).await.unwrap();
    tracing::info!(
        "Serving {:?} on {}...",
        [
            <DemoImpl as EchoService>::NAME,
            <DemoImpl as GreetService>::NAME,
            <DemoImpl as MemBenchService>::NAME
        ],
        addr.to_string()
    );

    server.join().await
}
