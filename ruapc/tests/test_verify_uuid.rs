#![forbid(unsafe_code)]

use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

use ruapc::{Client, Context, ErrorKind, ListenMode, Result, Router, Server, SocketPoolConfig};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_millis(200);

// A wire-only test proxy: the built-in request-status operation is
// not part of RuaPC's public Rust API.
#[derive(Serialize, Deserialize, JsonSchema)]
struct RequestStatusRequest {
    request_id: u64,
}

#[ruapc::service(name = "MemoryService")]
trait RequestStatusClient {
    async fn request_is_pending(&self, ctx: &Context, req: &RequestStatusRequest) -> Result<bool>;
}

#[ruapc::service]
trait Foo {
    async fn hello(&self, ctx: &Context, gated: &bool) -> Result<bool>;
}

struct FooImpl {
    arrived: mpsc::UnboundedSender<()>,
    resume: Notify,
    completed: mpsc::UnboundedSender<Result<bool>>,
}

impl Foo for FooImpl {
    async fn hello(&self, ctx: &Context, gated: &bool) -> Result<bool> {
        if *gated {
            self.arrived.send(()).unwrap();
            self.resume.notified().await;
        }
        let outcome = Client::default()
            .request_is_pending(
                ctx,
                &RequestStatusRequest {
                    request_id: ctx.msg_meta.msgid,
                },
            )
            .await;
        // Observe the handler even when its original caller timed out or was
        // cancelled: the call's own timeout alone cannot test this probe.
        self.completed.send(outcome.clone()).unwrap();
        outcome
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
                .unwrap()
                .hello(&ctx, &false)
                .await
                .unwrap(),
            "{scheme}: the actively polled borrowing future must answer"
        );
        assert!(
            tokio::time::timeout(TEST_TIMEOUT, completion.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
        assert!(
            !client.hello(&ctx, &false).await.unwrap(),
            "{scheme}: a waiter without a read attachment cannot validate a source"
        );
        assert!(
            !tokio::time::timeout(TEST_TIMEOUT, completion.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
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
            let attached = client.with_read_buffer(&source).unwrap();
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
            match outcome {
                Ok(false) => {}
                Err(error) if expires => assert_eq!(error.kind, ErrorKind::Timeout, "{scheme}"),
                other => panic!("{scheme}: ended source borrow was accepted: {other:?}"),
            }
        }

        drop(source);
        drop(ctx);
        server.stop();
        tokio::time::timeout(TEST_TIMEOUT, server.join())
            .await
            .expect("server must stop after all validation attempts finish");
    }
}
