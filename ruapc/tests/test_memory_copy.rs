use std::{net::SocketAddr, sync::Arc, time::Duration};

use ruapc::{
    Buffer, BufferPool, Client, Context, CopyOp, Endpoint, ListenMode, ResultBufferReply, Router,
    Server, SocketPoolConfig, Transport, service,
};
use ruapc_bufpool::MemoryKey;

const OPS: [CopyOp; 3] = [
    CopyOp::new(1, 7, 4),
    CopyOp::new(u64::MAX, u64::MAX, 0),
    CopyOp::new(0, 1, 3),
];

fn buffers(pool: &Arc<BufferPool>, bytes: &[u8], lengths: &[usize]) -> Vec<Buffer> {
    assert_eq!(lengths.iter().sum::<usize>(), bytes.len());
    let mut offset = 0;
    lengths
        .iter()
        .map(|&len| {
            if len == 0 {
                return Buffer::empty(pool);
            }
            let mut buffer = pool.allocate(len).unwrap();
            buffer.set_len(len);
            buffer.copy_from_slice(&bytes[offset..offset + len]);
            offset += len;
            buffer
        })
        .collect()
}

fn flatten(buffers: &[Buffer]) -> Vec<u8> {
    buffers
        .iter()
        .flat_map(|buffer| buffer.iter().copied())
        .collect()
}

#[service]
trait PackedCopy {
    async fn exchange(&self, ctx: &Context, req: &()) -> ResultBufferReply<Vec<u8>>;
}

struct PackedCopyService;

impl PackedCopy for PackedCopyService {
    async fn exchange(&self, ctx: &Context, _: &()) -> ResultBufferReply<Vec<u8>> {
        #[cfg(feature = "rdma")]
        assert!(ctx.state.rdma_devices.is_empty());
        for (regions, lengths) in [
            (&ctx.msg_meta.read_regions, [2, 0, 3, 3]),
            (&ctx.msg_meta.write_regions, [0, 5, 2, 5]),
        ] {
            assert_eq!(
                regions.iter().map(|region| region.len).collect::<Vec<_>>(),
                lengths
            );
            assert!(
                regions
                    .iter()
                    .all(|region| { region.addr == 0 && region.key == MemoryKey::default() })
            );
        }
        let destination = buffers(&ctx.state.buffer_pool, b"............", &[3, 0, 4, 5]);
        let read = ctx.remote_read(&OPS, destination).await?;
        let source = buffers(&ctx.state.buffer_pool, b"ABCDEFGH", &[1, 0, 4, 3]);
        let sent = ctx.remote_write(&OPS, source).await?;
        Ok(sent.into_reply(flatten(&read)))
    }
}

#[tokio::test]
async fn packed_copies_keep_operation_order_holes_and_empty_segments_on_stream_transports() {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 128 << 20,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(PackedCopyService).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    // Stream attachments need neither a registration nor allocation from the
    // Context's own pool. Zero-length segments can have no backing allocation.
    let pool = BufferPool::new(Vec::new());

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [address]).unwrap());
        #[cfg(feature = "rdma")]
        assert!(ctx.state.rdma_devices.is_empty());
        let client = Client {
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let source = buffers(&pool, b"abcdefgh", &[2, 0, 3, 3]);
        let destination = buffers(&pool, b"............", &[0, 5, 2, 5]);
        let destination_pointers: Vec<_> =
            destination.iter().map(|buffer| buffer.as_ptr()).collect();
        let attached = client
            .with_read_buffers(&source)
            .with_write_buffers(destination);
        let (read, written) = attached.exchange(&ctx, &()).await.unwrap().into_parts();
        let written = written.into_available().expect("destinations available");

        // Operation order differs from destination order, the sources overlap,
        // and neither operation covers the holes in either destination.
        assert_eq!(read, b".abc...bcde.", "{transport:?} remote_read");
        assert_eq!(
            flatten(&written),
            b".ABC...BCDE.",
            "{transport:?} remote_write"
        );
        assert_eq!(
            written
                .iter()
                .map(|buffer| buffer.len())
                .collect::<Vec<_>>(),
            [0, 5, 2, 5]
        );
        assert_eq!(
            written
                .iter()
                .map(|buffer| buffer.as_ptr())
                .collect::<Vec<_>>(),
            destination_pointers,
        );
        assert_eq!(written[0].capacity(), 0);
        drop(attached);
        assert_eq!(flatten(&source), b"abcdefgh");
        assert_eq!(source[1].capacity(), 0);
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}

#[service]
trait LargeCopy {
    /// Reads the whole read space, then writes the two specified ranges back.
    async fn copy_back(&self, ctx: &Context, ops: &[CopyOp; 2]) -> ResultBufferReply<u64>;
}

struct LargeCopyService;

impl LargeCopy for LargeCopyService {
    async fn copy_back(&self, ctx: &Context, ops: &[CopyOp; 2]) -> ResultBufferReply<u64> {
        let read = ctx.remote_read_all().await?;
        let total: u64 = read.iter().map(|buffer| buffer.len() as u64).sum();
        let sent = ctx.remote_write(ops, read).await?;
        Ok(sent.into_reply(total))
    }
}

async fn large_copy_server() -> (Server, SocketAddr, SocketPoolConfig) {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 256 << 20,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    };
    let mut router = Router::default();
    Arc::new(LargeCopyService).ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    (server, address, config)
}

/// Transfers of several data frames each, from two requests in flight on the
/// same connection at once, keep every byte in place in both directions.
#[tokio::test]
async fn multi_frame_transfers_interleave_on_stream_transports() {
    let (server, address, config) = large_copy_server().await;
    let pool = BufferPool::new(Vec::new());
    // Uneven buffers put frame and buffer boundaries in different places.
    const LENGTHS: [usize; 3] = [(1 << 20) + 3, 3 << 20, (1 << 20) + 14];
    let total: usize = LENGTHS.iter().sum();

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [address]).unwrap());
        let client = Client {
            timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let transfer = |seed: u8| {
            let pattern: Vec<u8> = (0..total).map(|index| (index % 251) as u8 ^ seed).collect();
            let source = buffers(&pool, &pattern, &LENGTHS);
            let destination = buffers(&pool, &vec![0; total], &[total / 2, total - total / 2]);
            let ctx = &ctx;
            let client = &client;
            async move {
                let attached = client
                    .with_read_buffers(&source)
                    .with_write_buffers(destination);
                let half = total / 2;
                let rest = (total - half) as u64;
                let ops = [
                    CopyOp::new(half as u64, 0, rest),
                    CopyOp::new(0, rest, half as u64),
                ];
                let (len, written) = attached.copy_back(ctx, &ops).await.unwrap().into_parts();
                assert_eq!(len, total as u64);
                let written = flatten(&written.into_available().expect("destinations"));
                let mut expected = pattern[half..].to_vec();
                expected.extend_from_slice(&pattern[..half]);
                assert!(written == expected, "{transport:?} seed {seed}");
            }
        };
        tokio::join!(transfer(0), transfer(0xa5));
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}

#[tokio::test]
async fn fragmented_writes_preserve_large_spans_and_untouched_gaps() {
    let (server, address, config) = large_copy_server().await;
    let pool = BufferPool::new(Vec::new());

    // remote_read_all mirrors all 34 buffers on the server. This single 40 KiB
    // frame exceeds the small HTTP/2 frame's packing threshold and fits TCP's
    // scatter limit, with a run of small slices between two large spans.
    let mut lengths = [512; 34];
    lengths[0] = 16 << 10;
    lengths[33] = 8 << 10;
    let total = lengths.iter().sum::<usize>();
    let pattern: Vec<_> = (0..total).map(|offset| (offset % 251) as u8).collect();
    let source = buffers(&pool, &pattern, &lengths);
    const GAP: usize = 7;
    let initial = vec![0xff; total + 3 * GAP];
    let mut expected = initial.clone();
    let half = total / 2;
    expected[GAP..GAP + half].copy_from_slice(&pattern[half..]);
    expected[2 * GAP + half..2 * GAP + total].copy_from_slice(&pattern[..half]);
    // Write the upper destination first; leave a sentinel before, between and
    // after the two nonoverlapping ranges.
    let ops = [
        CopyOp::new(0, (2 * GAP + half) as u64, half as u64),
        CopyOp::new(half as u64, GAP as u64, half as u64),
    ];

    for transport in [Transport::TCP, Transport::HTTP] {
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [address]).unwrap());
        // Neither destination boundary coincides with the source segments or
        // the halves of the copy batch.
        let destination = buffers(&pool, &initial, &[11, half, initial.len() - half - 11]);
        let client = Client::default();
        let attached = client
            .with_read_buffers(&source)
            .with_write_buffers(destination);
        let (count, written) = attached.copy_back(&ctx, &ops).await.unwrap().into_parts();
        assert_eq!(count, total as u64);
        let actual = flatten(&written.into_available().expect("destinations available"));
        assert!(
            actual == expected,
            "{transport:?} mixed spans or untouched gaps"
        );
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}
