use clap::Parser;
use ruapc::{Client, ClientConfig, Context, SocketPoolConfig, SocketType, services::MetaService};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Listen address.
    #[arg(default_value = "127.0.0.1:8000")]
    pub addr: std::net::SocketAddr,

    /// Socket type.
    #[arg(long, default_value = "tcp")]
    pub socket_type: SocketType,

    /// Use `MessagePack`
    #[arg(long, default_value_t = false)]
    pub use_msgpack: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

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
    match client.get_metadata(&ctx, &()).await {
        Ok(rsp) => println!("{}", serde_json::to_string_pretty(&rsp).unwrap()),
        Err(err) => eprintln!("request failed: {err}"),
    }
}
