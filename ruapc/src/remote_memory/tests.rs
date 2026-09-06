use super::*;
use crate::{Error, ErrorKind, SocketPoolConfig};

#[tokio::test]
async fn test_send_rsp_invalid_endpoint_logs_and_does_not_panic() {
    // Context starts without a destination by default.
    let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    // This should log an error and silently return (no panic).
    ctx.send_err_rsp(Error::kind(ErrorKind::Timeout)).await;
}

fn ctx_with_read_regions(regions: Vec<RemoteBufferInfo>) -> Context {
    let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    ctx.msg_meta.read_regions = regions;
    ctx
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
    let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    let local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
    let result = ctx.remote_read(&[CopyOp::new(0, 0, 1)], local).await;
    let mut err = result.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::MissingBufferInfo);
    let recovered = err.take_buffers().expect("buffers should be recovered");
    assert_eq!(recovered.len(), 1);
}

#[tokio::test]
async fn test_remote_read_invalid_endpoint_returns_err() {
    let ctx = ctx_with_read_regions(vec![region(8)]);
    let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
    local.set_len(8);
    let result = ctx.remote_read(&[CopyOp::new(0, 0, 8)], vec![local]).await;
    let mut err = result.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::NotConnected);
    // The consumed buffers are recoverable from the error.
    let recovered = err.take_buffers().expect("buffers should be recovered");
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
    let local = err.take_buffers().unwrap();

    // Overlapping destination ranges are rejected.
    let ops = [CopyOp::new(0, 0, 8), CopyOp::new(8, 4, 8)];
    let err = ctx.remote_read(&ops, local).await.unwrap_err();
    assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
}

#[tokio::test]
async fn test_remote_read_zero_len_ops_short_circuit() {
    // All-zero ops complete without a connection or regions.
    let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    let local = ctx.remote_read(&[CopyOp::new(0, 0, 0)], vec![]).await;
    assert!(local.unwrap().is_empty());
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
    assert!(err.take_buffers().is_some());
    // Once taken, the buffers are gone.
    assert!(err.take_buffers().is_none());
    // Converting to Error drops any remaining buffers and keeps the kind.
    let plain: Error = err.into();
    assert_eq!(plain.kind, ErrorKind::NotConnected);
}

#[tokio::test]
async fn test_remote_write_zero_total_short_circuits() {
    let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    // No write regions attached, but nothing to transfer either.
    let sent = ctx.remote_write_all(vec![]).await.unwrap();
    assert!(sent.buffers().is_empty());
    let _rsp: crate::WithBuffers<u32> = sent.reply(7);
    // The explicit no-op witness works the same way.
    let _rsp: crate::WithBuffers<u32> = ctx.sent_nothing().reply(7);
}

#[tokio::test]
async fn test_remote_spaces_missing_return_err() {
    let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
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
