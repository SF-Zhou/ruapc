//! Reverse-RPC implementation used by byte-stream transports.

use super::scatter::SpaceLayout;
use crate::services::{MemoryService, ReadInlineRequest, WriteInlineRequest};
use crate::{Buffer, Context, CopyOp, RemoteIoError, RemoteSpace};

/// Executes a validated batch of reads from the peer's read space into
/// the `local` buffers (see [`Context::remote_read`] for the space and
/// op semantics; validation already happened there).
///
/// Default implementation (TCP/WS/HTTP): a reverse
/// `_ruapc.memory/read_inline`
/// RPC returns the requested byte ranges inline, which are then
/// scattered into `local` according to the ops.
pub(crate) async fn read(
    ctx: &Context,
    ops: &[CopyOp],
    mut local: Vec<Buffer>,
    _remote: &RemoteSpace<'_>,
) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
    // The request ID selects the peer's owned source; offsets address its
    // original logical space without accepting peer-provided memory addresses.
    let req = ReadInlineRequest {
        ops: ops.to_vec(),
        request_id: ctx.msg_meta.msgid,
    };
    let client = crate::Client::default();
    let bytes: Vec<u8> = match client.read_inline(ctx, &req).await {
        Ok(rsp) => rsp.bytes,
        Err(e) => return Err(RemoteIoError::new(e, Some(local))),
    };
    let expected: u64 = ops.iter().map(|op| op.len).sum();
    if bytes.len() as u64 != expected {
        return Err(RemoteIoError::new(
            crate::Error::new(
                crate::ErrorKind::InvalidCopyOp,
                format!(
                    "remote read returned {} bytes but the ops requested {expected}",
                    bytes.len()
                ),
            ),
            Some(local),
        ));
    }
    // Scatter the inline blob (op payloads concatenated in op order)
    // into the local space.
    let layout = match SpaceLayout::from_lens(local.iter().map(|b| b.len() as u64)) {
        Ok(layout) => layout,
        Err(e) => return Err(RemoteIoError::new(e, Some(local))),
    };
    let mut cursor = 0usize;
    for op in ops {
        let _ = layout.for_each_slice::<std::convert::Infallible>(
            op.dst_offset,
            op.len,
            |seg, off, len| {
                let (off, len) = (off as usize, len as usize);
                local[seg][off..off + len].copy_from_slice(&bytes[cursor..cursor + len]);
                cursor += len;
                Ok(())
            },
        );
    }
    Ok(local)
}

/// Executes a validated batch of writes from the `local` buffers into
/// the peer's write space (see [`Context::remote_write`]; validation
/// already happened there).
///
/// Default implementation (TCP/WS/HTTP): the op payloads travel inline
/// in a reverse `_ruapc.memory/write_inline` RPC and the client copies them
/// into its pinned write buffers.
pub(crate) async fn write(
    ctx: &Context,
    ops: &[CopyOp],
    local: Vec<Buffer>,
) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
    // Gather the op payloads (in op order) into one inline blob.
    let layout = match SpaceLayout::from_lens(local.iter().map(|b| b.len() as u64)) {
        Ok(layout) => layout,
        Err(e) => return Err(RemoteIoError::new(e, Some(local))),
    };
    let total: u64 = ops.iter().map(|op| op.len).sum();
    let mut bytes = Vec::with_capacity(total as usize);
    for op in ops {
        let _ = layout.for_each_slice::<std::convert::Infallible>(
            op.src_offset,
            op.len,
            |seg, off, len| {
                let (off, len) = (off as usize, len as usize);
                bytes.extend_from_slice(&local[seg][off..off + len]);
                Ok(())
            },
        );
    }
    let req = WriteInlineRequest {
        request_id: ctx.msg_meta.msgid,
        ops: ops.to_vec(),
        bytes,
    };
    let client = crate::Client::default();
    match client.write_inline(ctx, &req).await {
        Ok(()) => Ok(local),
        Err(e) => Err(RemoteIoError::new(e, Some(local))),
    }
}
