use clap::Parser;
use ruapc::{Context, Result, Router, Server};
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    let demo = Arc::new(DemoImpl::default());
    let mut router = Router::default();
    router.add_methods(EchoService::ruapc_export(demo.clone()));
    router.add_methods(GreetService::ruapc_export(demo.clone()));
    let server = Server::create(router);

    let server = Arc::new(server);
    let (addr, listen_handle) = server.listen(args.addr).await.unwrap();
    tracing::info!(
        "Serving {:?} on {}...",
        [
            <DemoImpl as EchoService>::NAME,
            <DemoImpl as GreetService>::NAME
        ],
        addr.to_string()
    );
    listen_handle.await.unwrap();
}
