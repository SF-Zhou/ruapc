#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{str::FromStr, sync::Arc};

use ruapc;

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

    let foo = Arc::new(FooImpl);
    let mut router = ruapc::Router::default();
    router.add_methods(foo.ruapc_export());

    let server = ruapc::Server::create(router);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let (addr, listen_handle) = server.listen(addr).await.unwrap();

    let client = ruapc::Client::default();
    let ctx = ruapc::Context::create_for_client(addr);
    let rsp = client.hello(&ctx, &"ruapc".to_string()).await.unwrap();
    assert_eq!(rsp, "hello ruapc!");

    server.stop();
    let _ = listen_handle.await;

    client.hello(&ctx, &"ruapc".to_string()).await.unwrap_err();
}
