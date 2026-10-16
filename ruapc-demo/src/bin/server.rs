//! Serve the echo, greeting and verified remote-memory examples.

use clap::Parser;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use ruapc::{Context, Error, ErrorKind, ListenMode, Result, Router, Server, WithBuffers};
use ruapc_demo::{
    EchoService, GreetService, MemBenchService, ReadCrcReq, Request, WriteCrcReq,
    app::{PoolOptions, RuntimeOptions, init_tracing},
    crc32c_of, fill_pattern,
};

#[derive(Parser, Debug)]
#[command(version, about = "Serve the RuaPC example services")]
struct Args {
    /// Listen address.
    #[arg(default_value = "0.0.0.0:8000")]
    addr: std::net::SocketAddr,

    /// Listener protocol mode.
    #[arg(long, default_value = "unified")]
    listen_mode: ListenMode,

    /// Base path for HTTP RPC and documentation endpoints.
    #[arg(long, default_value = "")]
    http_base_path: String,

    /// Disable RDMA initialization and serve only stream transports.
    #[cfg(feature = "rdma")]
    #[arg(long)]
    no_rdma: bool,

    #[command(flatten)]
    runtime: RuntimeOptions,

    #[command(flatten)]
    pool: PoolOptions,
}

/// Register all demo services on one shared, stateful implementation.
fn router() -> Router {
    let demo = Arc::new(DemoImpl::default());
    let mut router = Router::default();
    EchoService::ruapc_export(demo.clone(), &mut router);
    GreetService::ruapc_export(demo.clone(), &mut router);
    MemBenchService::ruapc_export(demo, &mut router);
    router
}

#[derive(Default)]
struct DemoImpl {
    sequence: AtomicU64,
}

impl EchoService for DemoImpl {
    async fn echo(&self, _ctx: &Context, request: &Request) -> Result<String> {
        Ok(request.0.clone())
    }
}

impl GreetService for DemoImpl {
    async fn greet(&self, _ctx: &Context, request: &Request) -> Result<String> {
        let val = self.sequence.fetch_add(1, Ordering::Relaxed);
        Ok(format!("hello {}({})!", request.0, val))
    }
}

impl MemBenchService for DemoImpl {
    async fn read_crc(&self, ctx: &Context, _request: &ReadCrcReq) -> Result<u32> {
        let data = ctx.remote_read_all().await?;
        Ok(crc32c_of(&data))
    }

    async fn write_crc(&self, ctx: &Context, request: &WriteCrcReq) -> Result<WithBuffers<u32>> {
        if request.len == 0 {
            return Ok(ctx.sent_nothing().reply(crc32c_of([b"".as_slice()])));
        }
        let mut buf = ctx
            .state
            .buffer_pool
            .allocate(request.len)
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        let seed = self.sequence.fetch_add(1, Ordering::Relaxed);
        fill_pattern(&mut buf[..request.len], seed);
        buf.set_len(request.len);
        let crc = crc32c_of([&buf]);
        let sent = ctx.remote_write_all(vec![buf]).await?;
        Ok(sent.reply(crc))
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing();
    args.runtime.build()?.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    #[cfg(feature = "rdma")]
    let enable_rdma = !args.no_rdma;
    #[cfg(not(feature = "rdma"))]
    let enable_rdma = false;
    let mut config = args.pool.config(enable_rdma);
    config.listen_mode = args.listen_mode;
    config.http_base_path = args.http_base_path;
    let server = Server::create(router(), &config)?;
    let addr = server.listen(args.addr).await?;
    tracing::info!("Serving EchoService, GreetService and MemBenchService on {addr}");

    let result = tokio::signal::ctrl_c().await;
    server.stop();
    server.join().await;
    result.map_err(Into::into)
}
