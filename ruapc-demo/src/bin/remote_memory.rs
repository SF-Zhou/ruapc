//! Self-contained demo of the remote read/write API.
//!
//! Starts a server in-process, then demonstrates:
//! - **Upload**: the client attaches a registered buffer via
//!   `with_read_buffer`; the server pulls it with `remote_read_request`.
//! - **Download**: the service method returns `ResultWithBuffer<T>`; the
//!   server hands the buffer over in its return value and the client
//!   receives it as `WithBuffer<T>` from the same method call.
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
    /// Client attaches a buffer; server reads it and reports its size.
    async fn upload(&self, ctx: &Context, req: &UploadReq) -> Result<usize>;

    /// Server pushes a buffer to the client and reports the push latency
    /// (in microseconds) as the response — computed *after* the transfer,
    /// which the `remote_write` + `SentBuffer::reply` two-step allows.
    async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffer<u64>>;
}

struct BlobServiceImpl;

impl BlobService for BlobServiceImpl {
    async fn upload(&self, ctx: &Context, req: &UploadReq) -> Result<usize> {
        // One call: allocates a right-sized local buffer and transfers
        // exactly the client's logical data length (TCP: reverse RPC copy,
        // RDMA: one-sided RDMA READ).
        let data = ctx.remote_read_request().await?;
        tracing::info!(
            "server: received upload '{}' ({} bytes): {:?}...",
            req.name,
            data.len(),
            &data[..data.len().min(16)]
        );
        Ok(data.len())
    }

    async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffer<u64>> {
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

        // Transfer first: the push happens right here in the handler.
        let t0 = std::time::Instant::now();
        let sent = ctx.remote_write(buf).await?;
        // The response value is decided after the transfer completed —
        // here it carries the observed push latency back to the client.
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

    // Upload: fill a registered buffer, mark the valid length, attach it.
    let payload = b"hello remote memory!";
    let mut buf = ctx.state.buffer_pool.allocate(1 << 20).unwrap();
    buf[..payload.len()].copy_from_slice(payload);
    buf.set_len(payload.len());

    let req = UploadReq {
        name: "greeting".into(),
    };
    let uploaded = client
        .with_read_buffer(&buf)
        .upload(&ctx, &req)
        .await
        .unwrap();
    tracing::info!("client: server read {uploaded} bytes from our buffer");
    assert_eq!(uploaded, payload.len());

    // Download: the ResultWithBuffer return type means the buffer arrives
    // as part of the method's return value — together with a response
    // computed after the transfer (the server-side push latency).
    let want = 4096;
    let (push_micros, received) = client
        .download(&ctx, &DownloadReq { len: want })
        .await
        .unwrap()
        .into_parts();
    tracing::info!(
        "client: received {} bytes from server (server-side push took {push_micros}µs)",
        received.len()
    );
    assert_eq!(received.len(), want);
    assert!(
        received
            .iter()
            .enumerate()
            .all(|(i, &b)| b == (i % 251) as u8)
    );

    tracing::info!("remote read/write demo finished successfully");
    server.stop();
    server.join().await;
}
