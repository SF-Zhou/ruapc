use clap::Parser;
use ruapc::{Context, Error, ErrorKind, ListenMode, Result, Router, Server, WithBuffers};
use ruapc_demo::{
    EchoService, GreetService, MemBenchService, ReadCrcReq, Request, WriteCrcReq,
    app::{PoolOptions, RuntimeOptions, init_tracing},
    crc32c_of, fill_pattern,
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

    #[command(flatten)]
    pub runtime: RuntimeOptions,

    #[command(flatten)]
    pub pool: PoolOptions,
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
            .async_allocate(request.len)
            .await
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        let seed = self.sequence.fetch_add(1, Ordering::Relaxed);
        fill_pattern(&mut buf[..request.len], seed);
        buf.set_len(request.len);
        let crc = crc32c_of([&buf]);
        let sent = ctx.remote_write_all(vec![buf]).await?;
        Ok(sent.reply(crc))
    }
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
    let demo = Arc::new(DemoImpl::default());
    let mut router = Router::default();
    EchoService::ruapc_export(demo.clone(), &mut router);
    GreetService::ruapc_export(demo.clone(), &mut router);
    MemBenchService::ruapc_export(demo.clone(), &mut router);
    let mut config = args.pool.config(true);
    config.listen_mode = args.listen_mode;
    config.http_base_path = args.http_base_path;
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
