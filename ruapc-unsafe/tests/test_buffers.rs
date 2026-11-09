//! Owned and borrowed destinations share typed calls and preserve original buffers.

use std::{fmt, sync::Arc, time::Duration};

use ruapc::{
    Buffer, Client, Completed, Context, CopyOp, Endpoint, Error, ErrorKind, ListenMode,
    MAX_REGIONS, Router, Server, SocketPoolConfig, Transport,
};
use ruapc_bufpool::BufferPoolBuilder;
use ruapc_unsafe::{ClientBuffersExt, ContextBuffersExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
enum AppError {
    Framework(Error),
    Rejected,
}

impl From<Error> for AppError {
    fn from(error: Error) -> Self {
        Self::Framework(error)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for AppError {}

type AppResult = Result<usize, AppError>;
type BufferedResult = Result<usize, AppError>;

#[ruapc::service]
trait BufferModes {
    async fn exchange(&self, ctx: &Context, reject: &bool) -> AppResult;
    async fn buffered(&self, ctx: &Context, req: &()) -> BufferedResult;
    async fn hold(&self, ctx: &Context, req: &()) -> AppResult;
}

#[derive(Default)]
struct Service {
    entered: Notify,
    release: Notify,
    finished: Notify,
}

async fn copy_attachments(ctx: &Context) -> AppResult {
    let len = ctx.remote_read_space()?.total_len() as usize;
    let mut sources = vec![buffer(ctx, &vec![0; len])];
    let original = identity(&sources);
    let invalid = [CopyOp::new(len as u64, 0, 1)];
    // SAFETY: immediate awaiting keeps the live destinations inaccessible until return.
    let error = unsafe { ctx.remote_read(&invalid, &mut sources).await }.unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
    assert_eq!(identity(&sources), original);
    // SAFETY: the buffers remain alive and exclusively accessible until this awaited call returns.
    unsafe { ctx.remote_read_all(&mut sources).await? };
    assert_eq!(identity(&sources), original);
    let error = ctx.remote_write(&invalid, &sources).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidCopyOp);
    assert_eq!(identity(&sources), original);
    ctx.remote_write_all(&sources).await?;
    assert_eq!(identity(&sources), original);
    Ok(len)
}

impl BufferModes for Service {
    async fn exchange(&self, ctx: &Context, reject: &bool) -> AppResult {
        let len = copy_attachments(ctx).await?;
        if *reject {
            Err(AppError::Rejected)
        } else {
            Ok(len)
        }
    }

    async fn buffered(&self, ctx: &Context, _: &()) -> BufferedResult {
        copy_attachments(ctx).await
    }

    async fn hold(&self, _: &Context, _: &()) -> AppResult {
        self.entered.notify_one();
        self.release.notified().await;
        self.finished.notify_one();
        Ok(0)
    }
}

fn config() -> SocketPoolConfig {
    SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    }
}

async fn start(service: Arc<Service>) -> (Server, Context) {
    let config = config();
    let mut router = Router::default();
    service.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::tcp([address]).unwrap());
    (server, ctx)
}

fn buffer(ctx: &Context, bytes: &[u8]) -> Buffer {
    let mut buffer = ctx.state.buffer_pool.allocate(bytes.len()).unwrap();
    buffer.set_len(bytes.len());
    buffer.copy_from_slice(bytes);
    buffer
}

fn identity(buffers: &[Buffer]) -> Vec<(usize, usize, usize)> {
    buffers
        .iter()
        .map(|buffer| (buffer.as_ptr() as usize, buffer.len(), buffer.capacity()))
        .collect()
}

fn originals(ctx: &Context) -> (Vec<Buffer>, Vec<Buffer>) {
    (
        vec![buffer(ctx, b"sou"), buffer(ctx, b"rce")],
        vec![buffer(ctx, b"be"), buffer(ctx, b"fore")],
    )
}

fn bytes(buffers: &[Buffer]) -> Vec<u8> {
    buffers
        .iter()
        .flat_map(|buffer| buffer.iter().copied())
        .collect()
}

fn require_send<T: Send>(value: T) -> T {
    value
}

fn assert_originals(
    buffers: Option<Vec<Buffer>>,
    read: &[Buffer],
    reads: &[(usize, usize, usize)],
    writes: &[(usize, usize, usize)],
    expected_write: &[u8],
) {
    let write = buffers.unwrap();
    assert_eq!(identity(read), reads);
    assert_eq!(identity(&write), writes);
    assert_eq!(bytes(read), b"source");
    assert_eq!(bytes(&write), expected_write);
}

#[tokio::test]
async fn both_modes_preserve_custom_results_and_original_buffers_across_stream_transports() {
    let service = Arc::new(Service::default());
    let config = config();
    let mut router = Router::default();
    service.ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = Context::create(&config).unwrap();

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        let ctx = base.with_endpoint(Endpoint::new(transport, [address]).unwrap());
        for use_msgpack in [true, false] {
            let client = Client {
                use_msgpack,
                ..Default::default()
            };
            for reject in [false, true] {
                let (read, write) = originals(&ctx);
                let read_identity = identity(&read);
                let write_identity = identity(&write);
                let completed: Completed<AppResult> = require_send(
                    client
                        .with_buffers(&read, write)
                        .run(async |attached| attached.exchange(&ctx, &reject).await),
                )
                .await;
                let (result, recovered) = completed.into_parts();
                assert_eq!(
                    result,
                    if reject {
                        Err(AppError::Rejected)
                    } else {
                        Ok(6)
                    }
                );
                assert_originals(recovered, &read, &read_identity, &write_identity, b"source");

                let (read, mut write) = originals(&ctx);
                let read_identity = identity(&read);
                let write_identity = identity(&write);
                // SAFETY: no cancellation; these original allocations outlive the awaited call.
                let result: AppResult = require_send(
                    unsafe { client.borrow_buffers(&read, &mut write) }
                        .run(async |attached| attached.exchange(&ctx, &reject).await),
                )
                .await;
                assert_eq!(
                    result,
                    if reject {
                        Err(AppError::Rejected)
                    } else {
                        Ok(6)
                    }
                );
                assert_eq!(identity(&read), read_identity);
                assert_eq!(identity(&write), write_identity);
                assert_eq!(bytes(&read), b"source");
                assert_eq!(bytes(&write), b"source");
            }
        }
    }

    server.stop();
    server.join().await;
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn both_modes_restore_original_buffers_after_rdma_success_and_application_error() {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(Service::default()).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(Endpoint::rdma(Transport::TCP, [address]).unwrap());
    let client = Client {
        timeout: Duration::from_secs(5),
        ..Default::default()
    };

    for reject in [false, true] {
        let (read, write) = originals(&ctx);
        let read_identity = identity(&read);
        let write_identity = identity(&write);
        let Completed { result, buffers } = client
            .with_buffers(&read, write)
            .run(async |attached| attached.exchange(&ctx, &reject).await)
            .await;
        assert_eq!(
            result,
            if reject {
                Err(AppError::Rejected)
            } else {
                Ok(6)
            }
        );
        assert_originals(buffers, &read, &read_identity, &write_identity, b"source");

        let (read, mut write) = originals(&ctx);
        let read_identity = identity(&read);
        let write_identity = identity(&write);
        // SAFETY: the call is awaited with both allocations alive and unchanged.
        let result: AppResult = unsafe { client.borrow_buffers(&read, &mut write) }
            .run(async |attached| attached.exchange(&ctx, &reject).await)
            .await;
        assert_eq!(
            result,
            if reject {
                Err(AppError::Rejected)
            } else {
                Ok(6)
            }
        );
        assert_eq!(identity(&read), read_identity);
        assert_eq!(identity(&write), write_identity);
        assert_eq!(bytes(&read), b"source");
        assert_eq!(bytes(&write), b"source");
    }

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn scoped_borrowed_calls_preserve_the_declared_alias_and_original_buffers() {
    let (server, ctx) = start(Arc::new(Service::default())).await;
    let client = Client::default();
    let (read, mut write) = originals(&ctx);
    let read_identity = identity(&read);
    let write_identity = identity(&write);
    // SAFETY: the call is awaited with both allocations alive and unchanged.
    let reply: BufferedResult = unsafe { client.borrow_buffers(&read, &mut write) }
        .run(async |attached| attached.buffered(&ctx, &()).await)
        .await;
    assert_eq!(reply.unwrap(), 6);
    assert_eq!(identity(&read), read_identity);
    assert_eq!(identity(&write), write_identity);
    assert_eq!(bytes(&read), b"source");
    assert_eq!(bytes(&write), b"source");
    server.stop();
    server.join().await;
}

#[tokio::test]
async fn unchecked_sources_preserve_owned_destinations_and_application_errors() {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(Service::default()).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = Context::create(&config).unwrap();
    let client = Client {
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let transports = [
        Transport::TCP,
        Transport::WS,
        Transport::HTTP,
        #[cfg(feature = "rdma")]
        Transport::RDMA,
    ];
    for transport in transports {
        let endpoint = if transport == Transport::RDMA {
            Endpoint::rdma(Transport::TCP, [address]).unwrap()
        } else {
            Endpoint::new(transport, [address]).unwrap()
        };
        let ctx = base.with_endpoint(endpoint);
        for reject in [false, true] {
            let (read, write) = originals(&ctx);
            let read_identity = identity(&read);
            let write_identity = identity(&write);
            // SAFETY: all calls complete while the original sources remain
            // alive and immutable; only the destinations move into the scope.
            let scope = unsafe { client.with_buffers_unchecked(&read, write) };
            let completed: Completed<AppResult> =
                require_send(scope.run(async |attached| attached.exchange(&ctx, &reject).await))
                    .await;
            assert_eq!(
                completed.result,
                if reject {
                    Err(AppError::Rejected)
                } else {
                    Ok(6)
                }
            );
            assert_originals(
                completed.buffers,
                &read,
                &read_identity,
                &write_identity,
                b"source",
            );
        }
    }
    server.stop();
    server.join().await;
}

#[tokio::test]
async fn preflight_failure_restores_owned_and_borrowed_attachments() {
    let ctx = Context::create(&config()).unwrap();
    let client = Client::default();
    let (read, write) = originals(&ctx);
    let read_identity = identity(&read);
    let write_identity = identity(&write);
    let Completed { result, buffers } = client
        .with_buffers(&read, write)
        .run(async |attached| attached.exchange(&ctx, &false).await)
        .await;
    assert!(matches!(
        result,
        Err(AppError::Framework(Error {
            kind: ErrorKind::InvalidArgument,
            ..
        }))
    ));
    assert_originals(buffers, &read, &read_identity, &write_identity, b"before");

    let (read, mut write) = originals(&ctx);
    let read_identity = identity(&read);
    let write_identity = identity(&write);
    // SAFETY: the call is awaited with both allocations alive and unchanged.
    let result: AppResult = unsafe { client.borrow_buffers(&read, &mut write) }
        .run(async |attached| attached.exchange(&ctx, &false).await)
        .await;
    assert!(matches!(result, Err(AppError::Framework(_))));
    assert_eq!(identity(&read), read_identity);
    assert_eq!(identity(&write), write_identity);
    assert_eq!(bytes(&read), b"source");
    assert_eq!(bytes(&write), b"before");
}

#[tokio::test]
async fn invalid_source_layout_preserves_sources_and_returns_all_destinations() {
    let ctx = Context::create(&config()).unwrap();
    let client = Client::default();
    let read: Vec<_> = (0..=MAX_REGIONS).map(|_| buffer(&ctx, b"r")).collect();
    let write = vec![buffer(&ctx, b"write")];
    let read_identity = identity(&read);
    let write_identity = identity(&write);
    let Completed { result, buffers } = client
        .with_buffers(&read, write)
        .run(async |attached| attached.exchange(&ctx, &false).await)
        .await;
    let buffers = buffers.unwrap();
    assert!(matches!(
        result,
        Err(AppError::Framework(Error {
            kind: ErrorKind::InvalidCopyOp,
            ..
        }))
    ));
    assert_eq!(identity(&read), read_identity);
    assert_eq!(identity(&buffers), write_identity);
}

#[tokio::test]
async fn completed_buffers_return_empty_destinations_without_a_call() {
    let client = Client::default();
    let Completed { result, buffers } = client
        .with_buffers(&[], Vec::new())
        .run(async |_| Err::<(), _>(AppError::Rejected))
        .await;
    let buffers = buffers.unwrap();
    assert_eq!(result, Err(AppError::Rejected));
    assert!(buffers.is_empty());
}

#[tokio::test]
async fn borrowed_source_remains_shared_and_unchanged_while_the_call_is_pending() {
    let service = Arc::new(Service::default());
    let (server, ctx) = start(service.clone()).await;
    let client = Client::default();
    let (read, mut write) = originals(&ctx);
    let read_identity = identity(&read);
    let write_identity = identity(&write);
    // SAFETY: join drives the call to completion, and the parallel branch only
    // reads the immutable source. Neither original allocation is moved or changed.
    let result = unsafe { client.borrow_buffers(&read, &mut write) }
        .run(async |attached| {
            let (result, ()) = tokio::join!(attached.hold(&ctx, &()), async {
                service.entered.notified().await;
                assert_eq!(identity(&read), read_identity);
                assert_eq!(bytes(&read), b"source");
                service.release.notify_one();
            });
            result
        })
        .await;
    assert_eq!(result, Ok(0));
    assert_eq!(identity(&write), write_identity);
    assert_eq!(bytes(&write), b"before");
    server.stop();
    server.join().await;
}

#[tokio::test]
async fn returning_after_cancelling_an_inner_call_still_restores_owned_buffers() {
    let service = Arc::new(Service::default());
    let (server, ctx) = start(service.clone()).await;
    let client = Client::default();
    let (read, write) = originals(&ctx);
    let read_identity = identity(&read);
    let write_identity = identity(&write);
    let completed = client
        .with_buffers(&read, write)
        .run(async |attached| {
            let mut call = Box::pin(attached.hold(&ctx, &()));
            tokio::select! {
                result = &mut call => panic!("held request unexpectedly returned: {result:?}"),
                () = service.entered.notified() => {}
                () = tokio::time::sleep(Duration::from_secs(5)) => panic!("handler did not start"),
            }
            drop(call);
            Err::<usize, _>(AppError::Rejected)
        })
        .await;
    assert_eq!(completed.result, Err(AppError::Rejected));
    assert_originals(
        completed.buffers,
        &read,
        &read_identity,
        &write_identity,
        b"before",
    );
    service.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), service.finished.notified())
        .await
        .unwrap();
    server.stop();
    server.join().await;
}

#[tokio::test]
async fn cancelled_calls_release_source_borrows_and_retain_owned_destinations_as_needed() {
    let service = Arc::new(Service::default());
    let (server, ctx) = start(service.clone()).await;
    let client = Client::default();

    for forget in [false, true] {
        // Keep every other block occupied so the allocation check observes
        // exactly the destination held by this call.
        let pool = BufferPoolBuilder::new(vec![]).max_memory(64 << 20).build();
        let _blockers = [
            pool.allocate(16 << 20).unwrap(),
            pool.allocate(16 << 20).unwrap(),
        ];
        let mut read = pool.allocate(16 << 20).unwrap();
        let mut write = pool.allocate(16 << 20).unwrap();
        read.set_len(6);
        write.set_len(6);
        read.copy_from_slice(b"source");
        write.copy_from_slice(b"before");
        let mut read = vec![read];
        let mut future = Box::pin(
            client
                .with_buffers(&read, vec![write])
                .run(async |attached| attached.hold(&ctx, &()).await),
        );
        tokio::select! {
            result = &mut future => panic!("held request unexpectedly returned: {result:?}"),
            () = service.entered.notified() => {}
            () = tokio::time::sleep(Duration::from_secs(5)) => panic!("handler did not start"),
        }
        assert!(pool.allocate(16 << 20).is_err());
        if forget {
            std::mem::forget(future);
        } else {
            drop(future);
        }
        // Sources are ordinary references, even when the request is forgotten.
        // The unpolled source service cannot read these reused bytes.
        read[0].copy_from_slice(b"reused");
        assert_eq!(bytes(&read), b"reused");
        service.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), service.finished.notified())
            .await
            .unwrap();
        if forget {
            assert!(pool.allocate(16 << 20).is_err());
        } else {
            let recovered =
                tokio::time::timeout(Duration::from_secs(5), pool.async_allocate(16 << 20))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(recovered.capacity(), 16 << 20);
        }
    }
    server.stop();
    server.join().await;
}
