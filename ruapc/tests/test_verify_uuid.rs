#![forbid(unsafe_code)]

use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

use ruapc::{Client, Context, ErrorKind, ListenMode, Result, Router, Server, SocketPoolConfig};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_millis(200);

// A wire-only test proxy: the built-in source-validation operation is
// not part of RuaPC's public Rust API.
#[derive(Serialize, Deserialize, JsonSchema)]
struct ValidateSourceRequest {
    request_id: u64,
}

#[ruapc::service(name = "MemoryService")]
trait ValidateSourceClient {
    async fn validate_read_source(&self, ctx: &Context, req: &ValidateSourceRequest) -> Result<()>;
}

#[ruapc::service]
trait Foo {
    async fn hello(&self, ctx: &Context, gated: &bool) -> Result<bool>;
}

struct FooImpl {
    arrived: mpsc::UnboundedSender<()>,
    resume: Notify,
    completed: mpsc::UnboundedSender<Result<()>>,
}

impl Foo for FooImpl {
    async fn hello(&self, ctx: &Context, gated: &bool) -> Result<bool> {
        if *gated {
            self.arrived.send(()).unwrap();
            self.resume.notified().await;
        }
        let outcome = Client::default()
            .validate_read_source(
                ctx,
                &ValidateSourceRequest {
                    request_id: ctx.msg_meta.msgid,
                },
            )
            .await;
        // Observe the handler even when its original caller timed out or was
        // cancelled: the call's own timeout alone cannot test this probe.
        self.completed.send(outcome.clone()).unwrap();
        Ok(outcome.is_ok())
    }
}

#[tokio::test]
async fn test_verify_message_id() {
    for scheme in [
        "tcp",
        "ws",
        "http",
        #[cfg(feature = "rdma")]
        "tcp+rdma",
    ] {
        let (arrived, mut arrival) = mpsc::unbounded_channel();
        let (completed, mut completion) = mpsc::unbounded_channel();
        let foo = Arc::new(FooImpl {
            arrived,
            resume: Notify::new(),
            completed,
        });
        let mut router = Router::default();
        foo.clone().ruapc_export(&mut router);
        let config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            ..Default::default()
        };
        let server = Server::create(router, &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(format!("{scheme}://{address}").parse().unwrap());
        let client = Client {
            timeout: Duration::from_secs(3),
            max_retries: 0,
            ..Default::default()
        };
        let mut source = ctx.state.buffer_pool.allocate(4).unwrap();
        source.set_len(4);
        source.copy_from_slice(b"live");

        assert!(
            client
                .with_read_buffer(&source)
                .hello(&ctx, &false)
                .await
                .unwrap(),
            "{scheme}: the actively polled borrowing future must answer"
        );
        tokio::time::timeout(TEST_TIMEOUT, completion.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            !client.hello(&ctx, &false).await.unwrap(),
            "{scheme}: a waiter without a read attachment cannot validate a source"
        );
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, completion.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err()
                .kind,
            ErrorKind::Timeout,
            "{scheme}"
        );

        for expires in [false, true] {
            let client = Client {
                timeout: if expires {
                    CLIENT_TIMEOUT
                } else {
                    Duration::from_secs(3)
                },
                max_retries: 0,
                ..Default::default()
            };
            let attached = client.with_read_buffer(&source);
            let mut call = Box::pin(attached.hello(&ctx, &true));
            tokio::time::timeout(TEST_TIMEOUT, async {
                tokio::select! {
                    outcome = &mut call => panic!("{scheme}: gated call ended early: {outcome:?}"),
                    arrived = arrival.recv() => arrived.expect("handler must reach the gate"),
                }
            })
            .await
            .expect("handler must start before cancellation or expiry");
            if expires {
                let error = tokio::time::timeout(TEST_TIMEOUT, call)
                    .await
                    .expect("the gated original call must time out")
                    .unwrap_err();
                assert_eq!(error.kind, ErrorKind::Timeout, "{scheme}");
            } else {
                drop(call);
            }
            drop(attached);
            source.fill(if expires { 0xa5 } else { 0x5a });
            foo.resume.notify_one();
            let outcome = tokio::time::timeout(TEST_TIMEOUT, completion.recv())
                .await
                .expect("the handler's validation attempt must finish independently")
                .expect("the handler must report its validation outcome");
            let error = outcome.expect_err("an ended source borrow must not be accepted");
            assert_eq!(error.kind, ErrorKind::Timeout, "{scheme}");
        }

        drop(source);
        drop(ctx);
        server.stop();
        tokio::time::timeout(TEST_TIMEOUT, server.join())
            .await
            .expect("server must stop after all validation attempts finish");
    }
}
