//! Self-contained demo of the remote read/write API.
//!
//! Starts a server in-process, then demonstrates:
//! - **Upload**: the client moves registered buffers into a source wrapper via
//!   `with_read_buffers`; the server pulls them with `remote_read_all`.
//! - **Download**: the client pre-provides pinned destination buffers via
//!   `with_write_buffers`; the service method returns
//!   `ResultWithBuffers<T>`, the server writes into the client's buffers
//!   with `remote_write_all`, and every buffer comes back through the
//!   method's return value.
//!
//! Both directions treat multiple buffers as one logical contiguous
//! space; servers can also issue vectored transfers with explicit
//! offsets (`Context::remote_read` / `remote_write` with `CopyOp`s).
//!
//! Works identically over TCP / WS / HTTP / RDMA:
//!
//! ```sh
//! cargo run --bin remote_memory -- --transport tcp
//! cargo run --bin remote_memory --features rdma -- --transport rdma
//! ```

use std::sync::Arc;

use clap::Parser;
use ruapc::{
    Client, Context, Endpoint, Error, ErrorKind, ListenMode, Result, Router, Server,
    SocketPoolConfig, Transport, WithBuffers,
};
use ruapc_demo::app::init_tracing;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Transport used by the in-process client.
    #[arg(long, default_value = "tcp")]
    pub transport: Transport,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct UploadReq {
    name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DownloadReq {
    len: usize,
}

#[ruapc::service]
trait BlobService {
    /// Client attaches buffers; server reads them and reports their size.
    async fn upload(&self, ctx: &Context, req: &UploadReq) -> Result<usize>;

    /// Server fills the client's pinned write buffers and reports the
    /// write latency (in microseconds) as the response — computed *after*
    /// the transfer, which the `remote_write` + `SentBuffers::reply`
    /// two-step allows.
    async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<u64>>;
}

struct BlobServiceImpl;

impl BlobService for BlobServiceImpl {
    async fn upload(&self, ctx: &Context, req: &UploadReq) -> Result<usize> {
        // One call: allocates right-sized local buffers and transfers
        // exactly the client's logical data (TCP: reverse RPC copy,
        // RDMA: batched one-sided RDMA READs).
        let data = ctx.remote_read_all().await?;
        let total: usize = data.iter().map(|b| b.len()).sum();
        let preview = data
            .first()
            .map(|buffer| &buffer[..buffer.len().min(16)])
            .unwrap_or_default();
        tracing::info!(
            "server: received upload '{}' ({total} bytes in {} buffer(s)): {:?}...",
            req.name,
            data.len(),
            preview
        );
        Ok(total)
    }

    async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<u64>> {
        if req.len == 0 {
            return Ok(ctx.sent_nothing().reply(0));
        }
        // Fill a pool buffer and set its logical length.
        let mut buf = ctx
            .state
            .buffer_pool
            .allocate(req.len.max(1))
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        for (i, b) in buf[..req.len].iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        buf.set_len(req.len);

        // Transfer first: the write happens right here in the handler,
        // into the buffers the client pinned for this request.
        let t0 = std::time::Instant::now();
        let sent = ctx.remote_write_all(vec![buf]).await?;
        // The response value is decided after the transfer completed —
        // here it carries the observed write latency back to the client.
        let push_micros = t0.elapsed().as_micros() as u64;
        Ok(sent.reply(push_micros))
    }
}

/// Start the example server, run both transfer directions, then shut it down.
#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        #[cfg(feature = "rdma")]
        rdma: (args.transport == Transport::RDMA).then(Default::default),
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(BlobServiceImpl).ruapc_export(&mut router);
    let server = Arc::new(Server::create(router, &config)?);
    let addr = server.clone().listen(([127, 0, 0, 1], 0).into()).await?;
    tracing::info!("server listening on {addr}");

    let result = async {
        let ctx = Context::create(&config)?.with_endpoint(Endpoint::new(args.transport, addr));
        let client = Client::default();
        demonstrate_upload(&client, &ctx).await?;
        demonstrate_download(&client, &ctx).await?;
        tracing::info!("remote read/write demo finished successfully");
        Ok(())
    }
    .await;

    server.stop();
    server.join().await;
    result
}

async fn demonstrate_upload(client: &Client, ctx: &Context) -> Result<()> {
    // Two owned buffers form one immutable logical read space. Capacity is
    // allocation space; set_len marks the bytes the server may read.
    let payload = b"hello remote memory!";
    let (first, second) = payload.split_at(8);
    let mut buffers = Vec::with_capacity(2);
    for data in [first, second] {
        let mut buffer = ctx
            .state
            .buffer_pool
            .allocate(data.len())
            .expect("failed to allocate upload buffer");
        buffer[..data.len()].copy_from_slice(data);
        buffer.set_len(data.len());
        buffers.push(buffer);
    }

    let source = client.with_read_buffers(buffers);
    let uploaded = source
        .upload(
            ctx,
            &UploadReq {
                name: "greeting".into(),
            },
        )
        .await?;
    tracing::info!("client: server read {uploaded} bytes from our buffers");
    assert_eq!(uploaded, payload.len());
    // The owning wrapper can upload again without rebuilding its source.
    // It exposes shared views while reads may still be running.
    assert_eq!(
        source
            .read_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>(),
        payload.len()
    );
    Ok(())
}

async fn demonstrate_download(client: &Client, ctx: &Context) -> Result<()> {
    // The server writes one contiguous source into two pinned destination
    // buffers. Their ownership returns together with the response value.
    let want = 4096;
    let mut destinations = Vec::with_capacity(2);
    for len in [1024, want - 1024] {
        let mut buffer = ctx
            .state
            .buffer_pool
            .allocate(len)
            .expect("failed to allocate download buffer");
        buffer.set_len(len);
        destinations.push(buffer);
    }
    let (push_micros, received) = client
        .with_write_buffers(destinations)
        .download(ctx, &DownloadReq { len: want })
        .await?
        .into_parts();
    let received_len: usize = received.iter().map(|buffer| buffer.len()).sum();
    tracing::info!(
        "client: received {received_len} bytes from server \
         (server-side write took {push_micros}µs)"
    );
    assert_eq!(received_len, want);
    assert!(
        received
            .iter()
            .flat_map(|buffer| buffer.iter())
            .enumerate()
            .all(|(i, &byte)| byte == (i % 251) as u8)
    );
    Ok(())
}
