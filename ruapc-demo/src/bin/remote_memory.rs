//! Self-contained demo of the remote read/write API.
//!
//! Starts a server in-process, then demonstrates:
//! - **Upload**: the client attaches registered buffers via
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
//! cargo run --bin remote_memory -- --socket-type tcp
//! cargo run --bin remote_memory --features rdma -- --socket-type rdma
//! ```

use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use ruapc::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Socket type used by the client.
    #[arg(long, default_value = "tcp")]
    pub socket_type: SocketType,
}

// ==========================================================================
// Service definition
// ==========================================================================

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
        tracing::info!(
            "server: received upload '{}' ({total} bytes in {} buffer(s)): {:?}...",
            req.name,
            data.len(),
            &data[0][..data[0].len().min(16)]
        );
        Ok(total)
    }

    async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<u64>> {
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

// ==========================================================================
// Main
// ==========================================================================

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    // ---- Server ----------------------------------------------------------
    let config = SocketPoolConfig {
        socket_type: SocketType::UNIFIED,
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(BlobServiceImpl).ruapc_export(&mut router);
    let server = Arc::new(Server::create(router, &config).unwrap());
    let addr = std::net::SocketAddr::from_str("127.0.0.1:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();
    tracing::info!("server listening on {addr}");

    // ---- Client ----------------------------------------------------------
    let ctx = Context::create(&config).unwrap().with_addr(addr);
    let client = Client {
        socket_type: Some(args.socket_type),
        ..Default::default()
    };

    // Upload: fill two registered buffers, mark the valid lengths, attach
    // them — together they form one logical read space.
    let payload = b"hello remote memory!";
    let (a, b) = payload.split_at(8);
    let mut buf_a = ctx.state.buffer_pool.allocate(1 << 20).unwrap();
    buf_a[..a.len()].copy_from_slice(a);
    buf_a.set_len(a.len());
    let mut buf_b = ctx.state.buffer_pool.allocate(1 << 20).unwrap();
    buf_b[..b.len()].copy_from_slice(b);
    buf_b.set_len(b.len());
    let read_bufs = [buf_a, buf_b];

    let req = UploadReq {
        name: "greeting".into(),
    };
    let uploaded = client
        .with_read_buffers(&read_bufs)
        .upload(&ctx, &req)
        .await
        .unwrap();
    tracing::info!("client: server read {uploaded} bytes from our buffers");
    assert_eq!(uploaded, payload.len());

    // Download: pre-provide the pinned destination buffer; the
    // ResultWithBuffers return type means every attached buffer comes
    // back through the method's return value — together with a response
    // computed after the transfer (the server-side write latency).
    let want = 4096;
    let mut dst = ctx.state.buffer_pool.allocate(want).unwrap();
    dst.set_len(want);
    let (push_micros, received) = client
        .with_write_buffers(vec![dst])
        .download(&ctx, &DownloadReq { len: want })
        .await
        .unwrap()
        .into_parts();
    let received_len: usize = received.iter().map(|b| b.len()).sum();
    tracing::info!(
        "client: received {received_len} bytes from server \
         (server-side write took {push_micros}µs)"
    );
    assert_eq!(received_len, want);
    assert!(
        received[0]
            .iter()
            .enumerate()
            .all(|(i, &b)| b == (i % 251) as u8)
    );

    tracing::info!("remote read/write demo finished successfully");
    server.stop();
    server.join().await;
}
