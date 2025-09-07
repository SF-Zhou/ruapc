use clap::Parser;
use ruapc::{Client, Context, SocketPoolConfig, SocketType, services::MetaService};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Listen address.
    #[arg(default_value = "127.0.0.1:8000")]
    pub addr: std::net::SocketAddr,

    /// Socket type.
    #[arg(long, default_value = "tcp")]
    pub socket_type: SocketType,

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

    let ctx = Context::create(&SocketPoolConfig {
        socket_type: args.socket_type,
    })
    .unwrap()
    .with_addr(args.addr);
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
