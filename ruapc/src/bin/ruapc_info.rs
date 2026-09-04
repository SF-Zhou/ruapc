use clap::Parser;
use ruapc::{
    Client, Context, Endpoint, SocketPoolConfig,
    services::{DescribeRequest, ReflectionService},
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// RPC endpoint.
    #[arg(default_value = "tcp://127.0.0.1:8000")]
    pub endpoint: Endpoint,

    /// Use `MessagePack`.
    #[arg(long, default_value_t = false)]
    pub use_msgpack: bool,

    /// Print the full OpenAPI document instead of the structured service description.
    #[arg(long, default_value_t = false)]
    pub openapi: bool,

    /// Describe one exact wire service name.
    #[arg(long, conflicts_with = "openapi")]
    pub service: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = SocketPoolConfig::default();
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
        match client
            .describe(
                &ctx,
                &DescribeRequest {
                    service: args.service,
                },
            )
            .await
        {
            Ok(rsp) => println!("{}", serde_json::to_string_pretty(&rsp).unwrap()),
            Err(err) => eprintln!("request failed: {err}"),
        }
    }
}
