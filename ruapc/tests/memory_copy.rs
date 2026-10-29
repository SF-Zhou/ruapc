use std::{sync::Arc, time::Duration};

use ruapc::{
    Buffer, Client, Context, CopyOp, Endpoint, ListenMode, ResultBufferReply, Router, Server,
    SocketPoolConfig, Transport, service,
};

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
    }

    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}
