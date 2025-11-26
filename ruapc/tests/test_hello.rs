#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{str::FromStr, sync::Arc};

use ruapc::{SocketPoolConfig, SocketType};

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
        #[cfg(feature = "rdma")]
        SocketType::RDMA,
    ] {
        let foo = Arc::new(FooImpl);
        let mut router = ruapc::Router::default();
        foo.ruapc_export(&mut router);

        let config = SocketPoolConfig {
            socket_type: SocketType::UNIFIED,
        };
        let server = ruapc::Server::create(router, &config).unwrap();
        let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
        let addr = server.listen(addr).await.unwrap();

        let client = ruapc::Client {
            socket_type: Some(socket_type),
            ..Default::default()
        };
        let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);
        let rsp = client.hello(&ctx, &"ruapc".to_string()).await.unwrap();
        assert_eq!(rsp, "hello ruapc!");

        server.stop();
        tokio::time::timeout(std::time::Duration::from_secs(30), server.join())
            .await
            .unwrap();

        client.hello(&ctx, &"ruapc".to_string()).await.unwrap_err();
    }
}

#[tokio::test]
async fn test_http() {
    let foo = Arc::new(FooImpl);
    let mut router = ruapc::Router::default();
    foo.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = reqwest::Client::new();
    let req = client
        .post(format!("http://{}/MetaService/list_methods", addr))
        .build()
        .unwrap();
    let rsp = client
        .execute(req)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<ruapc::Result<Vec<String>>>()
        .await
        .unwrap()
        .unwrap();
    assert!(rsp.len() > 0);

    server.stop();
    server.join().await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn test_rdma() {
    use ruapc::Result;

    let foo = Arc::new(FooImpl);
    let mut router = ruapc::Router::default();
    foo.ruapc_export(&mut router);

    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
    };
    let server = ruapc::Server::create(router, &config).unwrap();
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.listen(addr).await.unwrap();

    let client = ruapc::Client {
        socket_type: Some(SocketType::RDMA),
        ..Default::default()
    };

    let mut ctx = ruapc::Context::create(&config).unwrap();
    client.hello(&ctx, &"ruapc".to_string()).await.unwrap_err();
    ctx.send_rsp(Result::Ok(())).await;

    let ctx = ctx.with_addr(addr);
    for _ in 0..256 {
        let rsp = client.hello(&ctx, &"ruapc".to_string()).await.unwrap();
        assert_eq!(rsp, "hello ruapc!");
    }

    server.stop();
    tokio::time::timeout(std::time::Duration::from_secs(30), server.join())
        .await
        .unwrap();

    client.hello(&ctx, &"ruapc".to_string()).await.unwrap_err();
}
