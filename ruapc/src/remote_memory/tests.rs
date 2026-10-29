use super::*;
#[cfg(feature = "rdma")]
use ruapc_memory::{BufferSet, CopyBatch};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ruapc_bufpool::RemoteBufferInfo;
use serde::Serialize;

#[cfg(feature = "rdma")]
use crate::remote_memory::{
    WriteTarget,
    rpc_transfer::{MemoryService, WriteInlineRequest},
};
use crate::{
    BufferReply, Error, ErrorKind, MAX_COPY_OPS, Message, MsgFlags, MsgMeta, Payload, Socket,
    SocketPoolConfig, msg::frame, tcp::TcpSocket,
};

#[derive(Serialize)]
struct PackedReadReply {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

#[tokio::test]
async fn test_send_rsp_invalid_endpoint_logs_and_does_not_panic() {
    // Context starts without a destination by default.
    let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    // This should log an error and silently return (no panic).
    ctx.send_err_rsp(Error::kind(ErrorKind::Timeout)).await;
}

fn ctx_with_read_regions(regions: Vec<RemoteBufferInfo>) -> Context {
    let (ctx, _) = connected_context(MsgMeta {
        read_regions: regions,
        ..Default::default()
    });
    ctx
}

fn connected_context(meta: MsgMeta) -> (Context, tokio::sync::mpsc::Receiver<Bytes>) {
    let config = SocketPoolConfig {
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    };
    let base = Context::create(&config).unwrap();
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    (
        Context::server_ctx(&base.state, Socket::TCP(TcpSocket::new(sender)), meta),
        receiver,
    )
}

fn region(len: u64) -> RemoteBufferInfo {
    RemoteBufferInfo {
        key: ruapc_bufpool::MemoryKey { lkey: 0, rkey: 0 },
        addr: 0x1000,
        len,
    }
}

#[tokio::test]
async fn test_remote_read_missing_regions_recovers_buffers() {
    let ctx = ctx_with_read_regions(vec![]);
    let local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
    let result = ctx.remote_read(&[CopyOp::new(0, 0, 1)], local).await;
    let mut err = result.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::MissingBufferInfo);
    let recovered = err
        .take_recovered_buffers()
        .expect("buffers should be recovered");
    assert_eq!(recovered.len(), 1);
}

#[tokio::test]
async fn test_remote_read_invalid_endpoint_returns_err() {
    let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    ctx.msg_meta.read_regions = vec![region(8)];
    let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    local.set_len(8);
    let result = ctx.remote_read(&[CopyOp::new(0, 0, 8)], vec![local]).await;
    let mut err = result.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::NotConnected);
    // The consumed buffers are recoverable from the error.
    let recovered = err
        .take_recovered_buffers()
        .expect("buffers should be recovered");
    assert_eq!(recovered[0].capacity(), 1024 * 1024);
}

#[tokio::test]
async fn test_remote_read_rejects_out_of_bounds_ops() {
    let ctx = ctx_with_read_regions(vec![region(8), region(8)]);
    let space = ctx.remote_read_space().unwrap();
    assert_eq!(space.total_len(), 16);
    assert_eq!(space.region_count(), 2);

    let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    local.set_len(16);
    // Source range exceeds the 16-byte remote space.
    let result = ctx.remote_read(&[CopyOp::new(9, 0, 8)], vec![local]).await;
    let mut err = result.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
    let local = err.take_recovered_buffers().unwrap();

    // Overlapping destination ranges are rejected.
    let ops = [CopyOp::new(0, 0, 8), CopyOp::new(8, 4, 8)];
    let err = ctx.remote_read(&ops, local).await.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
}

#[tokio::test]
async fn zero_byte_transfers_validate_context_and_batch_without_sending() {
    let disconnected = Context::create(&SocketPoolConfig::default()).unwrap();
    let (missing, mut missing_queue) = connected_context(MsgMeta::default());
    let (connected, mut queue) = connected_context(MsgMeta {
        read_regions: vec![region(8)],
        write_regions: vec![region(8)],
        ..Default::default()
    });
    for ops in [vec![], vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
        for (ctx, expected) in [
            (&disconnected, ErrorKind::NotConnected),
            (&missing, ErrorKind::MissingBufferInfo),
        ] {
            assert_eq!(
                ctx.remote_read(&ops, Vec::new())
                    .await
                    .unwrap_err()
                    .error
                    .kind,
                expected
            );
            assert_eq!(
                ctx.remote_write(&ops, Vec::new())
                    .await
                    .unwrap_err()
                    .error
                    .kind,
                expected
            );
        }
        assert!(
            connected
                .remote_read(&ops, Vec::new())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            connected
                .remote_write(&ops, Vec::new())
                .await
                .unwrap()
                .source_buffers()
                .available()
                .unwrap()
                .is_empty()
        );
    }
    let ops = vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1];
    for write in [false, true] {
        let mut buffer = connected.state.buffer_pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"keep");
        let pointer = buffer.as_ptr();
        let mut error = if write {
            connected
                .remote_write(&ops, vec![buffer])
                .await
                .unwrap_err()
        } else {
            connected.remote_read(&ops, vec![buffer]).await.unwrap_err()
        };
        assert_eq!(error.error.kind, ErrorKind::InvalidCopyOp);
        let recovered = error.take_recovered_buffers().unwrap();
        assert_eq!(recovered[0].as_ptr(), pointer);
        assert_eq!(recovered[0].as_slice(), b"keep");
    }
    assert!(queue.try_recv().is_err());
    assert!(missing_queue.try_recv().is_err());
}

#[tokio::test]
async fn test_remote_write_invalid_endpoint_returns_err() {
    let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    ctx.msg_meta.write_regions = vec![region(1024)];
    let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    local.set_len(1024);
    let result = ctx
        .remote_write(&[CopyOp::new(0, 0, 1024)], vec![local])
        .await;
    let mut err = result.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::NotConnected);
    assert!(err.take_recovered_buffers().is_some());
    // Once taken, the buffers are gone.
    assert!(err.take_recovered_buffers().is_none());
    // Converting to Error drops any remaining buffers and keeps the kind.
    let plain: Error = err.into();
    assert_eq!(plain.kind, ErrorKind::NotConnected);
}

#[tokio::test]
async fn test_remote_write_zero_total_short_circuits() {
    let (ctx, mut queue) = connected_context(MsgMeta {
        write_regions: vec![region(0)],
        ..Default::default()
    });
    let sent = ctx.remote_write_all(vec![]).await.unwrap();
    assert!(queue.try_recv().is_err());
    assert!(sent.source_buffers().available().unwrap().is_empty());
    let _rsp: BufferReply<u32> = sent.into_reply(7);
    // The explicit no-op witness works the same way.
    let _rsp: BufferReply<u32> = ctx.sent_nothing().into_reply(7);
}

#[tokio::test]
async fn test_remote_spaces_missing_return_err() {
    let ctx = ctx_with_read_regions(vec![]);
    assert_eq!(
        ctx.remote_read_space().unwrap_err().kind,
        ErrorKind::MissingBufferInfo
    );
    assert_eq!(
        ctx.remote_write_space().unwrap_err().kind,
        ErrorKind::MissingBufferInfo
    );
    // remote_read_all surfaces the same error.
    let err = ctx.remote_read_all().await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::MissingBufferInfo);
}

#[tokio::test]
async fn inline_read_bad_packed_replies_and_rpc_errors_preserve_destinations() {
    for (reply, expected) in [
        (Ok(vec![0x5a; 3]), ErrorKind::InvalidCopyOp),
        (Ok(vec![0x5a; 5]), ErrorKind::InvalidCopyOp),
        (
            Err(Error::kind(ErrorKind::Overloaded)),
            ErrorKind::Overloaded,
        ),
    ] {
        let base = Context::create(&SocketPoolConfig::default()).unwrap();
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        let ctx = Context::server_ctx(
            &base.state,
            Socket::TCP(TcpSocket::new(sender)),
            MsgMeta {
                msgid: 123,
                read_regions: vec![region(4)],
                timeout_ms: 5_000,
                ..Default::default()
            },
        );
        let mut local = ctx.state.buffer_pool.allocate(8).unwrap();
        local.set_len(8);
        local.copy_from_slice(b"........");
        let original_pointer = local.as_ptr();
        let ops = [CopyOp::new(2, 4, 2), CopyOp::new(0, 0, 2)];
        let peer = async {
            let encoded = queue.recv().await.unwrap();
            let mut encoded = BytesMut::from(encoded.as_ref());
            let request =
                Message::parse(frame::parse_message(&mut encoded).unwrap().unwrap()).unwrap();
            assert_eq!(request.meta.method, "MemoryService/read_inline");
            let response = reply.map(|bytes| PackedReadReply { bytes });
            ctx.state.waiter.post(
                request.meta.msgid,
                Message::new(
                    MsgMeta {
                        msgid: request.meta.msgid,
                        flags: MsgFlags::UseMessagePack | MsgFlags::IsRsp,
                        ..Default::default()
                    },
                    Payload::from(Bytes::from(rmp_serde::to_vec_named(&response).unwrap())),
                ),
            );
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(ctx.remote_read(&ops, vec![local]), peer)
        })
        .await
        .unwrap();
        let mut error = result.unwrap_err();
        assert_eq!(error.error.kind, expected);
        let recovered = error.take_recovered_buffers().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].as_ptr(), original_pointer);
        assert_eq!(recovered[0].as_slice(), b"........");
    }
}

#[cfg(feature = "rdma")]
#[tokio::test]
async fn inline_write_checks_request_before_busy_and_keeps_empty_batch_semantics() {
    let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
    buffer.set_len(4);
    buffer.copy_from_slice(b"keep");
    let target = WriteTarget::new(BufferSet::try_new(vec![buffer]).unwrap());
    let (request_id, _receiver) = ctx.state.waiter.alloc(Duration::from_secs(5));
    ctx.state
        .waiter
        .bind_write_target(request_id, target.clone());
    let ((), checked_out) = target.checkout(|_| Ok(())).unwrap();

    let empty = CopyBatch::new(&[]).unwrap();
    target.copy_inline(&empty, &[]).unwrap();
    let zero_ops = [CopyOp::new(u64::MAX, u64::MAX, 0)];
    let zeros = CopyBatch::new(&zero_ops).unwrap();
    target.copy_inline(&zeros, &[]).unwrap();

    for (ops, bytes, expected) in [
        (
            vec![CopyOp::new(0, 0, 1)],
            vec![b'x'],
            ErrorKind::BuffersInUse,
        ),
        (vec![CopyOp::new(0, 0, 1)], vec![], ErrorKind::InvalidCopyOp),
        (
            vec![CopyOp::new(0, 4, 1)],
            vec![b'x'],
            ErrorKind::InvalidCopyOp,
        ),
    ] {
        let request = WriteInlineRequest {
            request_id,
            ops,
            bytes,
        };
        assert_eq!(
            ().write_inline(&ctx, &request).await.unwrap_err().kind,
            expected
        );
    }
    assert_eq!(checked_out.buffers()[0].as_slice(), b"keep");
    target.restore(checked_out);
    target.copy_inline(&zeros, &[]).unwrap();
}
