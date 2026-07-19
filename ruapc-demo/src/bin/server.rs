use clap::Parser;
use ruapc::{Context, Result, Router, Server, SocketPoolConfig, SocketType};
use ruapc_demo::{EchoService, GreetService, Request};
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

    /// Socket type.
    #[arg(long, default_value = "unified")]
    pub socket_type: SocketType,

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
    #[allow(unused_mut)]
    let mut config = SocketPoolConfig {
        socket_type: args.socket_type,
        buffer_pool_memory: args.pool_mem_mb * 1024 * 1024,
        ..Default::default()
    };
    #[cfg(feature = "rdma")]
    {
        config.rdma.poll_threads_per_device = args.poll_threads;
        config.rdma.device_filter = args.rdma_devices.clone();
        config.rdma.poll_spin_us = args.poll_spin_us;
    }
    let server = Server::create(router, &config).unwrap();

    let server = Arc::new(server);
    let addr = server.listen(args.addr).await.unwrap();
    tracing::info!(
        "Serving {:?} on {}...",
        [
            <DemoImpl as EchoService>::NAME,
            <DemoImpl as GreetService>::NAME
        ],
        addr.to_string()
    );

    server.join().await
}
