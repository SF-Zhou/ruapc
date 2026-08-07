use clap::Parser;
use ruapc::{Client, Context, Endpoint, SocketPoolConfig, services::MetaService};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// RPC endpoint.
    #[arg(default_value = "tcp://127.0.0.1:8000")]
    pub endpoint: Endpoint,

    /// Use `MessagePack`.
    #[arg(long, default_value_t = false)]
    pub use_msgpack: bool,

    /// Get metadata.
    #[arg(long, default_value_t = false)]
    pub openapi: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    #[allow(unused_mut)]
    let mut config = SocketPoolConfig::default();
    #[cfg(feature = "rdma")]
    if args.endpoint.transport() == ruapc::Transport::RDMA {
        config.rdma = Some(Default::default());
    }
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(args.endpoint);
    let client = Client {
        use_msgpack: args.use_msgpack,
        ..Default::default()
    };

    if args.openapi {
        match client.openapi(&ctx, &()).await {
            Ok(rsp) => println!("{}", serde_json::to_string_pretty(&rsp).unwrap()),
            Err(err) => eprintln!("request failed: {err}"),
        }
    } else {
        match client.list_methods(&ctx, &()).await {
            Ok(rsp) => println!("{}", serde_json::to_string_pretty(&rsp).unwrap()),
            Err(err) => eprintln!("request failed: {err}"),
        }
    }
}
