#![forbid(unsafe_code)]

use std::{str::FromStr, sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruapc::{Client, Endpoint, ListenMode, SocketPoolConfig, Transport};

const CLIENT_TIMEOUT: Duration = Duration::from_millis(200);

// A wire-only test proxy: the built-in request-status operation is
// intentionally not part of RuaPC's public Rust API or reflection surface.
#[derive(Serialize, Deserialize, JsonSchema)]
struct RequestStatusRequest {
    request_id: u64,
}

#[ruapc::service(name = "_ruapc.memory")]
trait RequestStatusClient {
    async fn request_is_pending(
        &self,
        ctx: &ruapc::Context,
        req: &RequestStatusRequest,
    ) -> ruapc::Result<bool>;
}

#[ruapc::service]
trait Foo {
    async fn hello(&self, _: &ruapc::Context, req: &Duration) -> ruapc::Result<()>;
}

struct FooImpl;

impl Foo for FooImpl {
    async fn hello(&self, ctx: &ruapc::Context, req: &Duration) -> ruapc::Result<()> {
        tokio::time::sleep(*req).await;

        let client = Client::default();
        let in_waiting = client
            .request_is_pending(
                ctx,
                &RequestStatusRequest {
                    request_id: ctx.msg_meta.msgid,
                },
            )
            .await?;
        if *req < CLIENT_TIMEOUT {
            assert!(in_waiting);
        } else {
            assert!(!in_waiting);
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_verify_message_id() {
    tracing_subscriber::fmt().init();

    for transport in [
        Transport::TCP,
        Transport::WS,
        #[cfg(feature = "rdma")]
        Transport::RDMA,
    ] {
        let foo = Arc::new(FooImpl);
        let mut router = ruapc::Router::default();
        foo.ruapc_export(&mut router);

        let config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            ..Default::default()
        };
        let server = ruapc::Server::create(router, &config).unwrap();
        let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
        let addr = server.listen(addr).await.unwrap();
        let ctx = ruapc::Context::create(&config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, addr));

        let client = ruapc::Client {
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

        drop(server);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
