#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{str::FromStr, sync::Arc};

use ruapc::{self, SocketPoolConfig, SocketType};

#[ruapc::service]
trait Foo {
    async fn hello(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

struct FooImpl;

impl Foo for FooImpl {
    async fn hello(&self, _: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(format!("hello {}!", req))
    }
}

#[tokio::test]
async fn test_hello() {
    tracing_subscriber::fmt().init();

    for socket_type in [
        SocketType::TCP,
        SocketType::WS,
        SocketType::HTTP,
        SocketType::UNIFIED,
    ] {
        let foo = Arc::new(FooImpl);
        let mut router = ruapc::Router::default();
        router.add_methods(foo.ruapc_export());

        let config = SocketPoolConfig { socket_type };
        let server = ruapc::Server::create(router, &config);
        let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
        let addr = server.listen(addr).await.unwrap();

        let client = ruapc::Client::default();
        let ctx = ruapc::Context::create(&config).with_addr(addr);
        let rsp = client.hello(&ctx, &"ruapc".to_string()).await.unwrap();
        assert_eq!(rsp, "hello ruapc!");

        server.stop();
        server.join().await;

        client.hello(&ctx, &"ruapc".to_string()).await.unwrap_err();
    }
}
