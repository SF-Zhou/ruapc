#![forbid(unsafe_code)]

//! A request's attached memory is reachable only from the connection that
//! carries the request. Request IDs are sequential, so a peer on another
//! connection of the same process can guess them.

use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

use ruapc::{
    BufferAvailability, BufferReply, Client, Context, CopyOp, ListenMode, Result, Router, Server,
    SocketPoolConfig,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const SECRET: &[u8] = b"meant for the holder only";
/// Covers every request ID the victim can have issued by the time of the theft.
const GUESSES: u64 = 256;

// Wire-only proxies: the built-in remote-memory operations are not part of
// RuaPC's public Rust API, but any peer can put their names on the wire.
#[derive(Serialize, Deserialize, JsonSchema)]
struct ReadInline {
    ops: Vec<CopyOp>,
    request_id: u64,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct InlineBytes {
    bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct WriteInline {
    request_id: u64,
    ops: Vec<CopyOp>,
    bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct ValidateSource {
    request_id: u64,
}

#[ruapc::service(name = "MemoryService")]
trait RawMemory {
    async fn read_inline(&self, ctx: &Context, req: &ReadInline) -> Result<InlineBytes>;
    async fn write_inline(&self, ctx: &Context, req: &WriteInline) -> Result<()>;
    async fn validate_read_source(&self, ctx: &Context, req: &ValidateSource) -> Result<()>;
}

#[ruapc::service]
trait Holder {
    async fn hold(&self, ctx: &Context, req: &()) -> Result<BufferReply<u64>>;
}

struct HolderImpl {
    arrived: mpsc::UnboundedSender<()>,
    resume: Notify,
}

impl Holder for HolderImpl {
    async fn hold(&self, ctx: &Context, _req: &()) -> Result<BufferReply<u64>> {
        // Keep the request pending, with both attachments, during the theft.
        self.arrived.send(()).unwrap();
        self.resume.notified().await;
        // The request's own connection still reaches both attachments.
        let data = ctx.remote_read_all().await?;
        let total = data.iter().map(|buffer| buffer.len() as u64).sum();
        let receipt = ctx.remote_write_all(data).await?;
        Ok(receipt.into_reply(total))
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
struct Loot {
    read: Vec<u8>,
    writes: u32,
    validations: u32,
}

#[ruapc::service]
trait Thief {
    async fn steal(&self, ctx: &Context, len: &u64) -> Result<Loot>;
}

struct ThiefImpl;

impl Thief for ThiefImpl {
    async fn steal(&self, ctx: &Context, len: &u64) -> Result<Loot> {
        // JSON keeps the proxies free of byte-string codecs.
        let client = Client {
            use_msgpack: false,
            ..Default::default()
        };
        let ops = vec![CopyOp::new(0, 0, *len)];
        let mut loot = Loot::default();
        for request_id in 0..GUESSES {
            let read = ReadInline {
                ops: ops.clone(),
                request_id,
            };
            if let Ok(reply) = client.read_inline(ctx, &read).await {
                loot.read.extend(reply.bytes);
            }
            let write = WriteInline {
                request_id,
                ops: ops.clone(),
                bytes: vec![b'!'; usize::try_from(*len).unwrap()],
            };
            loot.writes += u32::from(client.write_inline(ctx, &write).await.is_ok());
            let validate = ValidateSource { request_id };
            loot.validations +=
                u32::from(client.validate_read_source(ctx, &validate).await.is_ok());
        }
        Ok(loot)
    }
}

async fn serve(router: Router, config: &SocketPoolConfig) -> (Server, std::net::SocketAddr) {
    let server = Server::create(router, config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    (server, address)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_connection_cannot_reach_a_pending_requests_memory() {
    for scheme in [
        "tcp",
        "ws",
        "http",
        #[cfg(feature = "rdma")]
        "tcp+rdma",
    ] {
        let config = SocketPoolConfig {
            listen_mode: ListenMode::UNIFIED,
            ..Default::default()
        };
        let (arrived, mut arrival) = mpsc::unbounded_channel();
        let holder = Arc::new(HolderImpl {
            arrived,
            resume: Notify::new(),
        });
        let mut router = Router::default();
        holder.clone().ruapc_export(&mut router);
        let (holder_server, holder_address) = serve(router, &config).await;
        let mut router = Router::default();
        Arc::new(ThiefImpl).ruapc_export(&mut router);
        let (thief_server, thief_address) = serve(router, &config).await;

        // One client process talks to both servers.
        let base = Context::create(&config).unwrap();
        let to_holder = base.with_endpoint(format!("{scheme}://{holder_address}").parse().unwrap());
        let to_thief = base.with_endpoint(format!("{scheme}://{thief_address}").parse().unwrap());
        let client = Client {
            timeout: TEST_TIMEOUT,
            max_retries: 0,
            ..Default::default()
        };

        let mut source = base.state.buffer_pool.allocate(SECRET.len()).unwrap();
        source.set_len(SECRET.len());
        source.copy_from_slice(SECRET);
        let mut destination = base.state.buffer_pool.allocate(SECRET.len()).unwrap();
        destination.set_len(SECRET.len());
        destination.fill(b'.');

        let attached = client
            .with_read_buffer(&source)
            .with_write_buffers(vec![destination]);
        // The borrowing call is polled throughout, as it would be when
        // answering its legitimate peer.
        let (reply, loot) = tokio::time::timeout(TEST_TIMEOUT * 2, async {
            tokio::join!(attached.hold(&to_holder, &()), async {
                arrival
                    .recv()
                    .await
                    .expect("the holder must receive the call");
                let loot = client.steal(&to_thief, &(SECRET.len() as u64)).await;
                holder.resume.notify_one();
                loot
            })
        })
        .await
        .unwrap_or_else(|_| panic!("{scheme}: calls must finish"));

        assert_eq!(
            loot.unwrap(),
            Loot::default(),
            "{scheme}: another connection reached a pending request's memory"
        );
        let (total, destinations) = reply.unwrap().into_parts();
        assert_eq!(total, SECRET.len() as u64, "{scheme}");
        let BufferAvailability::Available(destinations) = destinations else {
            panic!("{scheme}: destinations must return with the reply");
        };
        assert_eq!(&destinations[0][..], SECRET, "{scheme}");

        drop(attached);
        holder_server.stop();
        thief_server.stop();
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(holder_server.join(), thief_server.join());
        })
        .await
        .unwrap_or_else(|_| panic!("{scheme}: servers must stop"));
    }
}
