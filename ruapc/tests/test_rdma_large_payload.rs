#![forbid(unsafe_code)]
#![cfg(feature = "rdma")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use ruapc::{
    Buffer, Client, Context, Endpoint, ErrorKind, ListenMode, Result, ResultWithBuffers, Router,
    Server, SocketPoolConfig, Transport,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

const INLINE_LIMIT: u32 = 16 * 1024;
const LARGE_SIZE: usize = 1024 * 1024;
const ATTACHMENT_SIZE: usize = 256 * 1024;
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
struct Blob {
    data: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
struct Digest {
    len: usize,
    checksum: u64,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct Download {
    len: usize,
    seed: u8,
}

fn blob(len: usize, seed: u8) -> Blob {
    // Include JSON escapes so its serialized length differs from the logical
    // string length, while keeping the data identical across both codecs.
    let data = (0..len)
        .map(|index| char::from(b' ' + ((index * 17 + usize::from(seed)) % 95) as u8))
        .collect();
    Blob { data }
}

fn digest(data: &[u8]) -> Digest {
    let checksum = data.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    Digest {
        len: data.len(),
        checksum,
    }
}

fn flatten(buffers: &[Buffer]) -> Vec<u8> {
    buffers
        .iter()
        .flat_map(|buffer| buffer.iter().copied())
        .collect()
}

fn assert_no_attachments(ctx: &Context) {
    assert!(ctx.msg_meta.read_regions.is_empty());
    assert!(ctx.msg_meta.write_regions.is_empty());
}

#[ruapc::service]
trait LargePayloadService {
    async fn upload(&self, ctx: &Context, req: &Blob) -> Result<Digest>;
    async fn download(&self, ctx: &Context, req: &Download) -> Result<Blob>;
    async fn echo(&self, ctx: &Context, req: &Blob) -> Result<Blob>;
    async fn exchange_attached(&self, ctx: &Context, req: &Blob) -> ResultWithBuffers<Blob>;
    async fn callback(&self, ctx: &Context, req: &Blob) -> Result<Blob>;
    async fn hold(&self, ctx: &Context, req: &Blob) -> Result<Blob>;
}

#[derive(Default)]
struct LargePayloadServiceImpl {
    hold_entered: Notify,
    hold_release: Notify,
    hold_finished: Notify,
    hold_calls: AtomicUsize,
}

impl LargePayloadService for LargePayloadServiceImpl {
    async fn upload(&self, ctx: &Context, req: &Blob) -> Result<Digest> {
        assert_no_attachments(ctx);
        Ok(digest(req.data.as_bytes()))
    }

    async fn download(&self, ctx: &Context, req: &Download) -> Result<Blob> {
        assert_no_attachments(ctx);
        Ok(blob(req.len, req.seed))
    }

    async fn echo(&self, ctx: &Context, req: &Blob) -> Result<Blob> {
        assert_no_attachments(ctx);
        Ok(req.clone())
    }

    async fn exchange_attached(&self, ctx: &Context, req: &Blob) -> ResultWithBuffers<Blob> {
        // Transport payload regions must never replace application regions.
        assert_eq!(ctx.msg_meta.read_regions.len(), 2);
        assert_eq!(ctx.msg_meta.write_regions.len(), 2);
        let buffers = ctx.remote_read_all().await?;
        let mut data = flatten(&buffers);
        drop(buffers);
        data.reverse();
        let mut source = ctx.state.buffer_pool.allocate(data.len()).unwrap();
        source[..data.len()].copy_from_slice(&data);
        source.set_len(data.len());
        let sent = ctx.remote_write_all(vec![source]).await?;
        Ok(sent.reply(req.clone()))
    }

    async fn callback(&self, ctx: &Context, req: &Blob) -> Result<Blob> {
        assert_no_attachments(ctx);
        client(true).bounce(ctx, req).await
    }

    async fn hold(&self, ctx: &Context, req: &Blob) -> Result<Blob> {
        assert_no_attachments(ctx);
        self.hold_calls.fetch_add(1, Ordering::SeqCst);
        self.hold_entered.notify_one();
        self.hold_release.notified().await;
        self.hold_finished.notify_one();
        Ok(req.clone())
    }
}

#[ruapc::service]
trait LargePayloadCallback {
    async fn bounce(&self, ctx: &Context, req: &Blob) -> Result<Blob>;
}

struct LargePayloadCallbackImpl;

impl LargePayloadCallback for LargePayloadCallbackImpl {
    async fn bounce(&self, ctx: &Context, req: &Blob) -> Result<Blob> {
        assert_no_attachments(ctx);
        Ok(req.clone())
    }
}

fn config() -> SocketPoolConfig {
    let mut config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        ..Default::default()
    };
    let rdma = config.rdma.as_mut().unwrap();
    rdma.connection.max_msg_size = INLINE_LIMIT;
    rdma.connection.recv_queue_len = 4;
    rdma.polling.dispatch_workers = 1;
    rdma.remote_memory.bandwidth_limit_ratio = 0.0;
    rdma.maintenance.interval_ms = 0;
    config
}

fn client(use_msgpack: bool) -> Client {
    Client {
        use_msgpack,
        timeout: TEST_TIMEOUT,
        max_retries: 0,
        ..Default::default()
    }
}

async fn setup(config: &SocketPoolConfig) -> (Arc<LargePayloadServiceImpl>, Server, Context) {
    let service = Arc::new(LargePayloadServiceImpl::default());
    let mut router = Router::default();
    service.clone().ruapc_export(&mut router);
    let server = Server::create(router, config).unwrap();
    let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let mut router = Router::default();
    Arc::new(LargePayloadCallbackImpl).ruapc_export(&mut router);
    let ctx = Context::create_with_router(router, config)
        .unwrap()
        .with_endpoint(Endpoint::rdma(Transport::TCP, [addr]).unwrap());
    (service, server, ctx)
}

async fn stop(server: Server, ctx: Context) {
    drop(ctx);
    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .expect("RDMA server must stop promptly after large transfers");
}

async fn request_response_and_echo(use_msgpack: bool) {
    let (_, server, ctx) = setup(&config()).await;
    let client = client(use_msgpack);
    let request = blob(LARGE_SIZE, 7);

    // Each direction is exercised independently, followed by a call with
    // large payloads in both directions and an inline call on the same QP.
    assert_eq!(
        client.upload(&ctx, &request).await.unwrap(),
        digest(request.data.as_bytes())
    );
    assert_eq!(
        client
            .download(
                &ctx,
                &Download {
                    len: LARGE_SIZE,
                    seed: 23
                }
            )
            .await
            .unwrap(),
        blob(LARGE_SIZE, 23)
    );
    assert_eq!(client.echo(&ctx, &request).await.unwrap(), request);
    let inline = blob(31, 99);
    assert_eq!(client.echo(&ctx, &inline).await.unwrap(), inline);
    stop(server, ctx).await;
}

#[tokio::test]
async fn msgpack_large_request_response_and_echo() {
    request_response_and_echo(true).await;
}

#[tokio::test]
async fn json_large_request_response_and_echo() {
    request_response_and_echo(false).await;
}

async fn payload_with_attachments(use_msgpack: bool) {
    let (_, server, ctx) = setup(&config()).await;
    let client = client(use_msgpack);
    let request = blob(ATTACHMENT_SIZE, 19);
    let attachment = blob(ATTACHMENT_SIZE, 61).data.into_bytes();

    let source_split = attachment.len() / 3;
    let sources = [&attachment[..source_split], &attachment[source_split..]]
        .into_iter()
        .map(|bytes| {
            let mut buffer = ctx.state.buffer_pool.allocate(bytes.len()).unwrap();
            buffer[..bytes.len()].copy_from_slice(bytes);
            buffer.set_len(bytes.len());
            buffer
        })
        .collect();
    let destination_split = attachment.len() / 2 + 17;
    let destinations = [destination_split, attachment.len() - destination_split]
        .into_iter()
        .map(|len| {
            let mut buffer = ctx.state.buffer_pool.allocate(len).unwrap();
            buffer.set_len(len);
            buffer
        })
        .collect();
    let mut wrapper = client
        .with_read_buffers(sources)
        .with_write_buffers(destinations);
    let (response, buffers) = wrapper
        .exchange_attached(&ctx, &request)
        .await
        .unwrap()
        .into_parts();
    assert_eq!(response, request);
    assert_eq!(buffers.len(), 2);
    assert_eq!(
        flatten(&buffers),
        attachment.iter().rev().copied().collect::<Vec<_>>()
    );
    let recovered = wrapper
        .take_read_buffers()
        .expect("completed application READs release the attachment owner");
    assert_eq!(flatten(&recovered), attachment);
    drop(wrapper);
    drop(buffers);
    drop(recovered);
    stop(server, ctx).await;
}

#[tokio::test]
async fn msgpack_large_payload_preserves_read_and_write_attachments() {
    payload_with_attachments(true).await;
}

#[tokio::test]
async fn json_large_payload_preserves_read_and_write_attachments() {
    payload_with_attachments(false).await;
}

#[tokio::test]
async fn large_callback_confirmation_progresses_with_full_handler_capacity() {
    let mut config = config();
    config.max_inflight_requests = 1;
    let (_, server, ctx) = setup(&config).await;
    let request = blob(ATTACHMENT_SIZE, 37);

    // The server's only handler waits for a large callback. Its payload
    // confirmation must progress while that handler still owns admission.
    assert_eq!(
        client(true).callback(&ctx, &request).await.unwrap(),
        request
    );
    stop(server, ctx).await;
}

#[tokio::test]
async fn expired_and_cancelled_large_calls_leave_connection_usable() {
    let (service, server, ctx) = setup(&config()).await;
    let client = client(true);
    let request = blob(ATTACHMENT_SIZE, 47);
    assert_eq!(client.echo(&ctx, &request).await.unwrap(), request);

    let mut timeout_client = client.clone();
    timeout_client.timeout = Duration::from_secs(2);
    let timeout_ctx = ctx.clone();
    let timeout_request = request.clone();
    let timed_call =
        tokio::spawn(async move { timeout_client.hold(&timeout_ctx, &timeout_request).await });
    tokio::time::timeout(TEST_TIMEOUT, service.hold_entered.notified())
        .await
        .expect("large request must reach the handler before expiring");
    let err = timed_call.await.unwrap().unwrap_err();
    assert_eq!(err.kind, ErrorKind::Timeout);
    service.hold_release.notify_one();
    tokio::time::timeout(TEST_TIMEOUT, service.hold_finished.notified())
        .await
        .unwrap();
    assert_eq!(client.echo(&ctx, &request).await.unwrap(), request);

    let cancel_client = client.clone();
    let cancel_ctx = ctx.clone();
    let cancel_request = request.clone();
    let cancelled_call =
        tokio::spawn(async move { cancel_client.hold(&cancel_ctx, &cancel_request).await });
    tokio::time::timeout(TEST_TIMEOUT, service.hold_entered.notified())
        .await
        .expect("large request must reach the handler before cancellation");
    cancelled_call.abort();
    assert!(cancelled_call.await.unwrap_err().is_cancelled());
    service.hold_release.notify_one();
    tokio::time::timeout(TEST_TIMEOUT, service.hold_finished.notified())
        .await
        .unwrap();
    assert_eq!(client.echo(&ctx, &request).await.unwrap(), request);
    assert_eq!(service.hold_calls.load(Ordering::SeqCst), 2);
    stop(server, ctx).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_large_calls_and_inline_traffic_share_a_small_send_window() {
    let mut config = config();
    let rdma = config.rdma.as_mut().unwrap();
    rdma.connection.recv_queue_len = 2;
    rdma.polling.dispatch_workers = 4;
    let (_, server, ctx) = setup(&config).await;
    let mut tasks = tokio::task::JoinSet::new();
    for seed in 0..24 {
        let ctx = ctx.clone();
        tasks.spawn(async move {
            let client = client(seed % 2 == 0);
            let request = blob(if seed % 3 == 0 { 32 } else { 192 * 1024 }, seed);
            for _ in 0..8 {
                assert_eq!(client.echo(&ctx, &request).await.unwrap(), request);
            }
        });
    }
    tokio::time::timeout(TEST_TIMEOUT, async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    stop(server, ctx).await;
}

#[tokio::test]
async fn negotiated_large_limit_rejects_oversize_before_transport_acceptance() {
    let mut config = config();
    config.rdma.as_mut().unwrap().connection.max_large_msg_size = 128 * 1024;
    let (_, server, ctx) = setup(&config).await;
    let client = client(true);
    let accepted = blob(96 * 1024, 5);
    assert_eq!(client.echo(&ctx, &accepted).await.unwrap(), accepted);
    let error = client.upload(&ctx, &blob(256 * 1024, 6)).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::SerializeFailed);
    assert_eq!(client.echo(&ctx, &accepted).await.unwrap(), accepted);
    stop(server, ctx).await;
}

#[tokio::test]
async fn disabled_peer_negotiates_legacy_inline_messages_only() {
    let mut server_config = config();
    server_config
        .rdma
        .as_mut()
        .unwrap()
        .connection
        .max_large_msg_size = 0;
    let mut router = Router::default();
    Arc::new(LargePayloadServiceImpl::default()).ruapc_export(&mut router);
    let server = Server::create(router, &server_config).unwrap();
    let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let ctx = Context::create(&config())
        .unwrap()
        .with_endpoint(Endpoint::rdma(Transport::TCP, [addr]).unwrap());
    let client = client(true);
    let inline = blob(100, 3);
    assert_eq!(client.echo(&ctx, &inline).await.unwrap(), inline);
    let error = client.upload(&ctx, &blob(128 * 1024, 4)).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::SerializeFailed);
    assert_eq!(client.echo(&ctx, &inline).await.unwrap(), inline);
    stop(server, ctx).await;
}
