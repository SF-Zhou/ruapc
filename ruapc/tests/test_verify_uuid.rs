#![forbid(unsafe_code)]
#![feature(return_type_notation)]

use std::{str::FromStr, sync::Arc, time::Duration};

use ruapc::{Client, SocketPoolConfig, SocketType, services::MetaService};

const CLIENT_TIMEOUT: Duration = Duration::from_millis(200);

#[ruapc::service]
trait Foo {
    async fn hello(&self, _: &ruapc::Context, req: &Duration) -> ruapc::Result<()>;
}

struct FooImpl;

impl Foo for FooImpl {
    async fn hello(&self, ctx: &ruapc::Context, req: &Duration) -> ruapc::Result<()> {
        tokio::time::sleep(*req).await;

        let client = Client::default();
        let in_waiting = client.verify_uuid(ctx, &ctx.msg_meta.msgid).await?;
        if *req < CLIENT_TIMEOUT {
            assert!(in_waiting);
        } else {
            assert!(!in_waiting);
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_verify_uuid() {
    tracing_subscriber::fmt().init();

    for socket_type in [
        SocketType::TCP,
        SocketType::WS,
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
        let ctx = ruapc::Context::create(&config).unwrap().with_addr(addr);

        let client = ruapc::Client {
            socket_type: Some(socket_type),
            timeout: CLIENT_TIMEOUT,
            ..Default::default()
        };
        client
            .hello(&ctx, &Duration::from_millis(10))
            .await
            .unwrap();

        client
            .hello(&ctx, &Duration::from_millis(250))
            .await
            .unwrap_err();

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
