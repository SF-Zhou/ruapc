//! Source verification modes differ only in the confirmation handshake.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use ruapc::{
    Client, Context, CopyOp, Endpoint, Error, ErrorKind, ListenMode, RawCall, Result, Router,
    Server, SocketPoolConfig, Transport,
};
use ruapc_unsafe::ClientBuffersExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_millis(100);

#[ruapc::service]
trait SourceValidationTest {
    async fn hold(&self, ctx: &Context, request: &()) -> Result<()>;
}

struct Service {
    entered: mpsc::UnboundedSender<Context>,
    release: Notify,
    finished: Notify,
}

impl SourceValidationTest for Service {
    async fn hold(&self, ctx: &Context, _: &()) -> Result<()> {
        self.entered
            .send(ctx.clone())
            .expect("test is still running");
        self.release.notified().await;
        self.finished.notify_one();
        Ok(())
    }
}

// Wire-only proxies for the internal reverse-RPC methods. Using JSON also
// keeps the byte-vector proxy independent of MessagePack's serde_bytes adapter.
#[derive(Serialize, JsonSchema)]
struct RequestStatusRequest {
    request_id: u64,
}

#[derive(Serialize, JsonSchema)]
struct ReadInlineRequest {
    request_id: u64,
    ops: Vec<CopyOp>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ReadInlineResponse {
    bytes: Vec<u8>,
}

fn probe_client() -> Client {
    Client {
        timeout: PROBE_TIMEOUT,
        max_retries: 0,
        use_msgpack: false,
        ..Default::default()
    }
}

async fn pending(ctx: &Context) -> Result<bool> {
    probe_client()
        .ruapc_raw_call::<_, bool, Error>(
            ctx,
            &RequestStatusRequest {
                request_id: ctx.msg_meta.msgid,
            },
            "MemoryService/request_is_pending",
        )
        .await
}

async fn gather(ctx: &Context) -> Result<ReadInlineResponse> {
    probe_client()
        .ruapc_raw_call::<_, ReadInlineResponse, Error>(
            ctx,
            &ReadInlineRequest {
                request_id: ctx.msg_meta.msgid,
                ops: vec![CopyOp::new(0, 0, 6)],
            },
            "MemoryService/read_inline",
        )
        .await
}

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .expect("test operation did not finish")
}

async fn with_owner<F: Future, O: Future>(owner: Pin<&mut F>, operation: O) -> O::Output {
    tokio::select! {
        result = bounded(operation) => result,
        _ = owner => panic!("original request returned before the handler was released"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceMode {
    Safe,
    Unchecked,
    BorrowedDestinations,
}

async fn exercise(transport: Transport) {
    let (entered, mut requests) = mpsc::unbounded_channel();
    let service = Arc::new(Service {
        entered,
        release: Notify::new(),
        finished: Notify::new(),
    });
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        #[cfg(feature = "rdma")]
        rdma: (transport == Transport::RDMA).then(Default::default),
        ..Default::default()
    };
    let mut router = Router::default();
    service.clone().ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let endpoint = if transport == Transport::RDMA {
        Endpoint::rdma(Transport::TCP, [address]).unwrap()
    } else {
        Endpoint::new(transport, [address]).unwrap()
    };
    let ctx = Context::create(&config).unwrap().with_endpoint(endpoint);
    let client = Client {
        timeout: Duration::from_secs(30),
        max_retries: 0,
        ..Default::default()
    };

    for mode in [
        SourceMode::Safe,
        SourceMode::Unchecked,
        SourceMode::BorrowedDestinations,
    ] {
        let mut source = ctx.state.buffer_pool.allocate(6).unwrap();
        source.set_len(6);
        source.copy_from_slice(b"source");
        let mut sources = vec![source];
        let mut destinations = Vec::new();
        let mut owner = Box::pin(async {
            match mode {
                SourceMode::Safe => {
                    client
                        .with_buffers(&sources, Vec::new())
                        .run(async |attached| attached.hold(&ctx, &()).await)
                        .await
                        .result
                }
                SourceMode::Unchecked => {
                    // SAFETY: the source stays alive and immutable until this
                    // request is synchronously cancelled below, never forgotten.
                    unsafe { client.with_buffers_unchecked(&sources, Vec::new()) }
                        .run(async |attached| attached.hold(&ctx, &()).await)
                        .await
                        .result
                }
                SourceMode::BorrowedDestinations => {
                    // SAFETY: no source or destination access conflicts with
                    // the call, which is driven to completion before reuse.
                    unsafe { client.borrow_buffers(&sources, &mut destinations) }
                        .run(async |attached| attached.hold(&ctx, &()).await)
                        .await
                }
            }
        });
        let reverse = with_owner(owner.as_mut(), requests.recv())
            .await
            .expect("hold handler published its connected context");

        // The original future remains owned here but is deliberately not
        // polled while the reverse query runs. It is never leaked or forgotten.
        let paused_validation = bounded(pending(&reverse)).await;
        let active_validation = with_owner(owner.as_mut(), pending(&reverse)).await;

        // Even the caller-validated mode must route CPU copies through the
        // original future. Exercise the same builtin over every transport.
        let paused_copy = bounded(gather(&reverse)).await;
        let active_copy = with_owner(owner.as_mut(), gather(&reverse)).await;
        let transferred = with_owner(owner.as_mut(), reverse.remote_read_allocated()).await;

        if mode == SourceMode::BorrowedDestinations {
            // Complete before checking results so assertion unwinding cannot
            // cancel a started borrowed-destination scope.
            service.release.notify_one();
            bounded(owner.as_mut()).await.unwrap();
        }
        drop(owner);
        // Both safe and unchecked owning calls revoke their original waiter
        // on ordinary cancellation before releasing the source borrow.
        sources[0].copy_from_slice(b"reused");
        let revoked_validation = bounded(pending(&reverse)).await;
        let revoked_copy = bounded(gather(&reverse)).await;
        if mode != SourceMode::BorrowedDestinations {
            service.release.notify_one();
        }
        bounded(service.finished.notified()).await;

        match mode {
            SourceMode::Safe => {
                assert_eq!(paused_validation.unwrap_err().kind, ErrorKind::Timeout);
            }
            SourceMode::Unchecked | SourceMode::BorrowedDestinations => {
                assert!(paused_validation.unwrap(), "{transport:?}/{mode:?}");
            }
        }
        assert!(active_validation.unwrap(), "{transport:?}/{mode:?}");
        assert_eq!(paused_copy.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(active_copy.unwrap().bytes, b"source");
        let transferred: Vec<u8> = transferred
            .unwrap()
            .iter()
            .flat_map(|buffer| buffer.iter().copied())
            .collect();
        assert_eq!(transferred, b"source");
        assert!(!revoked_validation.unwrap(), "{transport:?}/{mode:?}");
        assert_eq!(revoked_copy.unwrap_err().kind, ErrorKind::Timeout);
        assert_eq!(sources[0].as_slice(), b"reused");
    }

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn source_confirmation_and_cpu_copies_across_stream_transports() {
    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        exercise(transport).await;
    }
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn source_confirmation_and_cpu_copies_over_rdma() {
    exercise(Transport::RDMA).await;
}
