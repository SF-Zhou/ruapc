use clap::Parser;
use ruapc::*;
use ruapc_demo::{EchoService, GreetService, Request};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Listen address.
    #[arg(default_value = "127.0.0.1:8000")]
    pub addr: std::net::SocketAddr,

    /// Socket type.
    #[arg(long, default_value = "tcp")]
    pub socket_type: SocketType,

    /// Request value.
    #[arg(short, long, default_value = "alice")]
    pub value: String,

    /// Use MessagePack
    #[arg(long, default_value_t = false)]
    pub use_msgpack: bool,

    /// Enable stress testing.
    #[arg(long, default_value_t = false)]
    pub stress: bool,

    /// Stress testing duration.
    #[arg(long, default_value = "60")]
    pub secs: u64,

    /// The number of coroutines.
    #[arg(long, default_value = "32")]
    pub coroutines: usize,
}

#[derive(Default)]
struct State {
    total: AtomicUsize,
    fails: AtomicUsize,
}

async fn stress_test(args: Args) {
    let state = Arc::new(State::default());
    let start_time = std::time::Instant::now();
    let mut tasks = vec![];
    let ctx = Context::create(&SocketPoolConfig {
        socket_type: args.socket_type,
    })
    .with_addr(args.addr);
    for _ in 0..args.coroutines {
        let value = Request(args.value.clone());
        let state = state.clone();
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            while start_time.elapsed().as_secs() < args.secs {
                let client = Client {
                    config: ClientConfig {
                        timeout: Duration::from_secs(5),
                        use_msgpack: args.use_msgpack,
                    },
                };
                for _ in 0..256 {
                    let result = client.echo(&ctx, &value).await;
                    state.total.fetch_add(1, Ordering::AcqRel);
                    if result.is_err() {
                        state.fails.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
        }));
    }
    tokio::select! {
        _ = async {
            for task in tasks {
                task.await.unwrap();
            }
        } => {
        }
        _ = async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let total = state.total.swap(0, Ordering::AcqRel);
                let fails = state.fails.swap(0, Ordering::AcqRel);
                tracing::info!("QPS: {total}/s, fails: {fails}/s");
            }
        } => {
        }
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

    if args.stress {
        stress_test(args).await;
    } else {
        let ctx = Context::create(&SocketPoolConfig {
            socket_type: args.socket_type,
        })
        .with_addr(args.addr);
        let client = Client {
            config: ClientConfig {
                use_msgpack: args.use_msgpack,
                ..Default::default()
            },
        };
        let rsp = client.echo(&ctx, &Request(args.value.clone())).await;
        tracing::info!("echo rsp: {:?}", rsp);

        let rsp = client.greet(&ctx, &Request(args.value.clone())).await;
        tracing::info!("greet rsp: {:?}", rsp);
    }
}
