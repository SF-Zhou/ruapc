use std::{sync::Arc, time::Duration};

use ruapc::{
    Buffer, BufferAvailability, Client, Context, CopyOp, Endpoint, ErrorKind, ListenMode, Result,
    ResultBufferReply, Router, Server, SocketPoolConfig, Transport, service,
};
use tokio::sync::{Notify, mpsc};

const OPS: [CopyOp; 3] = [
    CopyOp::new(1, 7, 4),
    CopyOp::new(u64::MAX, u64::MAX, 0),
    CopyOp::new(0, 1, 3),
];

fn buffers(ctx: &Context, bytes: &[u8], lengths: &[usize]) -> Vec<Buffer> {
    assert_eq!(lengths.iter().sum::<usize>(), bytes.len());
    let mut offset = 0;
    lengths
        .iter()
        .map(|&len| {
            let mut buffer = ctx.state.buffer_pool.allocate(len.max(1)).unwrap();
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

    async fn exchange_fragmented(&self, ctx: &Context, req: &()) -> ResultBufferReply<()>;
}

struct PackedCopyService;

impl PackedCopy for PackedCopyService {
    async fn exchange(&self, ctx: &Context, _: &()) -> ResultBufferReply<Vec<u8>> {
        let destination = buffers(ctx, b"............", &[3, 0, 4, 5]);
        let read = ctx.remote_read(&OPS, destination).await?;
        let source = buffers(ctx, b"ABCDEFGH", &[1, 0, 4, 3]);
        let sent = ctx.remote_write(&OPS, source).await?;
        Ok(sent.into_reply(flatten(&read)))
    }

    async fn exchange_fragmented(&self, ctx: &Context, _: &()) -> ResultBufferReply<()> {
        let source = large_source();
        let expected = large_destination(&source);
        let destination = fragmented(ctx, &vec![0xa5; expected.len()], 19001);
        let read = ctx.remote_read(&large_operations(), destination).await?;
        assert_eq!(flatten(&read), expected);

        let source = fragmented(ctx, &source, 20011);
        let sent = ctx.remote_write(&large_operations(), source).await?;
        Ok(sent.into_reply(()))
    }
}

fn large_source() -> Vec<u8> {
    (0..1024 * 1024 + 31)
        .map(|offset: usize| (offset.wrapping_mul(37) ^ (offset >> 8)) as u8)
        .collect()
}

fn large_operations() -> Vec<CopyOp> {
    (0..64)
        .map(|index| CopyOp::new(((index * 37) % 64) * 16384 + 7, index * 16401 + 11, 16384))
        .collect()
}

fn large_destination(source: &[u8]) -> Vec<u8> {
    let mut expected = vec![0xa5; 64 * 16401 + 23];
    for op in large_operations() {
        let src = op.src_offset as usize;
        let dst = op.dst_offset as usize;
        let len = op.len as usize;
        expected[dst..dst + len].copy_from_slice(&source[src..src + len]);
    }
    expected
}

fn fragmented(ctx: &Context, data: &[u8], chunk_size: usize) -> Vec<Buffer> {
    let lengths: Vec<_> = data.chunks(chunk_size).map(<[u8]>::len).collect();
    buffers(ctx, data, &lengths)
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

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [address]).unwrap());
        let client = Client {
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let source = buffers(&ctx, b"abcdefgh", &[2, 0, 3, 3]);
        let destination = buffers(&ctx, b"............", &[0, 5, 2, 5]);
        let destination_pointers: Vec<_> =
            destination.iter().map(|buffer| buffer.as_ptr()).collect();
        let mut attached = client
            .with_read_buffers(source)
            .unwrap()
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
        assert_eq!(
            flatten(&attached.take_read_buffers().into_available().unwrap()),
            b"abcdefgh"
        );

        // More than 100 source slices cross TCP's bounded writev batches and
        // HTTP DATA frames. Consecutive calls also detect frame interleaving
        // or an unconsumed tail from the previous operation.
        let source = large_source();
        let expected = large_destination(&source);
        for use_msgpack in [true, false] {
            let client = Client {
                use_msgpack,
                ..client.clone()
            };
            let source = fragmented(&ctx, &source, 17011);
            let destination = fragmented(&ctx, &vec![0xa5; expected.len()], 18013);
            let attached = client
                .with_read_buffers(source)
                .unwrap()
                .with_write_buffers(destination);
            let ((), written) = attached
                .exchange_fragmented(&ctx, &())
                .await
                .unwrap()
                .into_parts();
            assert_eq!(
                flatten(&written.into_available().expect("destinations available")),
                expected,
                "{transport:?} fragmented remote_write",
            );
        }
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}

#[service]
trait DelayedRead {
    async fn read_after_release(&self, ctx: &Context, req: &()) -> Result<()>;
}

struct DelayedReadService {
    started: Notify,
    release: Notify,
    result: mpsc::UnboundedSender<(ErrorKind, Vec<u8>)>,
}

impl DelayedRead for DelayedReadService {
    async fn read_after_release(&self, ctx: &Context, _: &()) -> Result<()> {
        self.started.notify_one();
        self.release.notified().await;
        let destination = buffers(ctx, b"untouched", &[4, 5]);
        let error = ctx
            .remote_read(&[CopyOp::new(0, 0, 9)], destination)
            .await
            .expect_err("a removed original request cannot authorize a late read");
        let (error, recovered) = error.into_parts();
        self.result
            .send((
                error.kind.clone(),
                flatten(&recovered.expect("local buffers recoverable")),
            ))
            .unwrap();
        Err(error)
    }
}

#[tokio::test]
async fn cancelled_and_forgotten_stream_calls_do_not_authorize_late_source_reads() {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 128 << 20,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    };
    let (result, mut results) = mpsc::unbounded_channel();
    let service = Arc::new(DelayedReadService {
        started: Notify::new(),
        release: Notify::new(),
        result,
    });
    let mut router = Router::default();
    service.clone().ruapc_export(&mut router);
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [address]).unwrap());
        for forget in [false, true] {
            let client = Client {
                timeout: if forget {
                    Duration::from_millis(200)
                } else {
                    Duration::from_secs(5)
                },
                ..Default::default()
            };
            let mut attached = client
                .with_read_buffers(buffers(&ctx, b"original!", &[3, 0, 6]))
                .unwrap();
            let mut request = Box::pin(attached.read_after_release(&ctx, &()));
            tokio::select! {
                () = service.started.notified() => {}
                result = &mut request => panic!("request completed before release: {result:?}"),
                () = tokio::time::sleep(Duration::from_secs(5)) => panic!("handler did not start"),
            }
            if forget {
                std::mem::forget(request);
                assert!(matches!(
                    attached.take_read_buffers(),
                    BufferAvailability::Unavailable
                ));
            } else {
                drop(request);
            }

            // Cancellation removes the waiter immediately. A forgotten future
            // relies on expiry; failed uniqueness checks must preserve its
            // source so a later attempt can still recover the same buffers.
            let mut recovered = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match attached.take_read_buffers() {
                        BufferAvailability::Available(buffers) => break buffers,
                        BufferAvailability::Unavailable => tokio::task::yield_now().await,
                        BufferAvailability::NotAttached => panic!("source lost on failed recovery"),
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(flatten(&recovered), b"original!");
            for buffer in &mut recovered {
                buffer.fill(0xff);
            }
            service.release.notify_one();
            let (kind, unchanged) = tokio::time::timeout(Duration::from_secs(5), results.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                kind,
                ErrorKind::Timeout,
                "{transport:?}, forgotten={forget}"
            );
            assert_eq!(unchanged, b"untouched");
        }
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}
