//! RDMA READ planning and batch ownership. Memory remains held until every
//! posted work request has completed, including error and flush completions.

mod batch;

pub(super) use batch::{ReadBatch, ReadHold};

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::ReadSge;

use super::RdmaSocket;
use crate::{
    Buffer, Context, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result,
    remote_memory::{
        WriteTarget,
        scatter::{self, SpaceLayout},
    },
    services::{MemoryService, ReadIntoTargetRequest, RequestStatusRequest},
};

/// One planned RDMA READ work request: a contiguous remote range
/// scattered into up to `max_send_sge` local segments.
#[derive(Debug)]
struct PlannedRead {
    remote_addr: u64,
    rkey: u32,
    sges: Vec<ReadSge>,
}

impl PlannedRead {
    fn len(&self) -> u64 {
        self.sges.iter().map(|sge| u64::from(sge.len)).sum()
    }
}

/// Translates the chunk plan of a validated op batch into concrete work
/// requests, resolving remote regions to `(addr, rkey)` and local
/// segments (given as per-segment `(base address, lkey)`) to scatter
/// entries.
fn build_planned_reads(
    regions: &[RemoteBufferInfo],
    src_layout: &SpaceLayout,
    dst_layout: &SpaceLayout,
    dst_bases: &[(u64, u32)],
    ops: &[CopyOp],
    max_sge: usize,
) -> Result<Vec<PlannedRead>> {
    scatter::plan_chunks(src_layout, dst_layout, ops, max_sge.max(1))
        .into_iter()
        .map(|chunk| {
            let region = &regions[chunk.seg];
            let sges = chunk
                .dst
                .iter()
                .map(|slice| {
                    let (base, lkey) = dst_bases[slice.seg];
                    Ok(ReadSge {
                        addr: base + slice.off,
                        len: u32::try_from(slice.len).map_err(|_| {
                            Error::new(
                                ErrorKind::InvalidCopyOp,
                                "scatter slice exceeds u32::MAX bytes".into(),
                            )
                        })?,
                        lkey,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(PlannedRead {
                remote_addr: region.addr + chunk.off,
                rkey: region.key.rkey,
                sges,
            })
        })
        .collect()
}

impl RdmaSocket {
    /// Resolves every outstanding read batch of a socket with
    /// `ConnectionClosed` (without releasing their memory holds).
    pub(crate) fn fail_read_batches(&self) {
        for entry in self.rdma_completions.iter() {
            entry.value().fail(Error::new(
                ErrorKind::ConnectionClosed,
                "rdma poll thread shut down with reads in flight".into(),
            ));
        }
    }

    /// Posts the planned reads and waits for the batch to complete.
    ///
    /// On success the hold is handed back. On failure the second element
    /// carries the hold only when *nothing* reached the hardware; once a
    /// work request is in flight the memory stays parked in the batch
    /// until its (possibly flush) completion arrives.
    async fn execute_reads(
        &self,
        reads: &[PlannedRead],
        hold: ReadHold,
        request_remaining: Option<Duration>,
    ) -> std::result::Result<ReadHold, (Error, Option<ReadHold>)> {
        debug_assert!(!reads.is_empty());
        let bytes = reads.iter().map(PlannedRead::len).sum();
        if let Err(error) = self
            .bandwidth_limiter
            .reserve_recv(bytes, request_remaining)
            .await
        {
            return Err((error, Some(hold)));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let deadline = self.read_timeout.map(|timeout| Instant::now() + timeout);
        let batch = ReadBatch::new(reads.len(), hold, tx, deadline);

        for (posted, read) in reads.iter().enumerate() {
            if let Err(err) = self.post_read(read, &batch).await {
                if posted == 0 {
                    // Nothing reached the hardware: recover the hold now.
                    return Err((err, batch.cancel()));
                }
                // Some reads are in flight. Account the unposted suffix before
                // failing the connection, then let flush completions settle it.
                batch.abort_unposted(reads.len() - posted);
                self.set_error();
                let _ = rx.await;
                return Err((err, None));
            }
        }

        match rx.await {
            Ok(Ok(hold)) => Ok(hold),
            Ok(Err(e)) => Err((e, None)),
            Err(_) => Err((
                Error::new(
                    ErrorKind::RdmaSendFailed,
                    "RDMA read batch abandoned (connection torn down)".into(),
                ),
                None,
            )),
        }
    }

    /// Acquire device/SQ capacity and publish the memory hold as one post
    /// transaction. Failed posts return permits through RAII; successful posts
    /// transfer permit ownership to the completion poller.
    #[allow(unsafe_code)] // Verbs memory ownership contract; see each SAFETY comment.
    async fn post_read(&self, read: &PlannedRead, batch: &Arc<ReadBatch>) -> Result<()> {
        let permits_closed =
            |_| Error::new(ErrorKind::RdmaSendFailed, "RDMA read permits closed".into());
        // Take the per-connection guard first: a congested SQ must not hoard
        // the shared device's permits while waiting for its own capacity.
        let sq_permit = self
            .sq_read_permits
            .acquire()
            .await
            .map_err(permits_closed)?;
        let device_permit = self.read_permits.acquire().await.map_err(permits_closed)?;
        // SAFETY: the validated plan resolves registered destination memory.
        // Register the batch hold before posting; completion or QP destruction
        // releases it only after the NIC can no longer access that memory.
        unsafe {
            self.queue_pair.read_sges(
                &read.sges,
                read.remote_addr,
                read.rkey,
                |wr_id| {
                    self.rdma_completions.insert(wr_id, batch.clone());
                },
                |wr_id| {
                    self.rdma_completions.remove(&wr_id);
                },
            )
        }?;
        sq_permit.forget();
        device_permit.forget();
        Ok(())
    }

    /// Planning only borrows destination buffers, so all validation failures
    /// reach the caller before their ownership moves into a DMA hold.
    fn plan_remote_read(
        &self,
        ops: &[CopyOp],
        local: &[Buffer],
        remote: &RemoteSpace<'_>,
    ) -> Result<Vec<PlannedRead>> {
        let device = &self.queue_pair.device_index;
        let bases = local
            .iter()
            .map(|buf| {
                let key = buf
                    .memory_key(device)
                    .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
                Ok((buf.as_ptr() as u64, key.lkey))
            })
            .collect::<Result<Vec<_>>>()?;
        let layout = SpaceLayout::from_lens(local.iter().map(|b| b.len() as u64))?;
        build_planned_reads(
            remote.regions(),
            remote.layout(),
            &layout,
            &bases,
            ops,
            self.queue_pair.gather_limit(),
        )
    }

    /// Executes the client side of `_ruapc.memory/read_into_target`: RDMA READs from
    /// the peer's regions into the pinned write target. The target `Arc`
    /// keeps the destination memory alive for as long as any read is in
    /// flight, so no post-transfer liveness verification is needed.
    pub(crate) async fn read_into_target(
        &self,
        regions: &[RemoteBufferInfo],
        src_layout: &SpaceLayout,
        ops: &[CopyOp],
        target: Arc<WriteTarget>,
        request_remaining: Option<Duration>,
    ) -> Result<()> {
        let device = &self.queue_pair.device_index;
        let bases = target.export_sge_bases(device)?;
        let planned = build_planned_reads(
            regions,
            src_layout,
            target.layout(),
            &bases,
            ops,
            self.queue_pair.gather_limit(),
        )?;
        if planned.is_empty() {
            return Ok(());
        }
        match self
            .execute_reads(&planned, ReadHold::Target(target), request_remaining)
            .await
        {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(e),
        }
    }

    pub(super) async fn read_remote(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        let planned = match self.plan_remote_read(ops, &local, remote) {
            Ok(planned) => planned,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if planned.is_empty() {
            return Ok(local);
        }

        let local = match self
            .execute_reads(&planned, ReadHold::Buffers(local), ctx.remaining_time())
            .await
        {
            Ok(ReadHold::Buffers(local)) => local,
            Ok(ReadHold::Target(_)) => unreachable!("remote_read holds buffers"),
            Err((e, hold)) => {
                let buffers = match hold {
                    Some(ReadHold::Buffers(buffers)) => Some(buffers),
                    _ => None,
                };
                return Err(RemoteIoError::new(e, buffers));
            }
        };

        // After the RDMA READs complete, verify the client's original
        // request is still alive. RDMA READ is one-sided — the client
        // cannot know its memory was read — and once its request times
        // out, the read buffers may have been reclaimed and refilled, so
        // the data would be garbage.
        let request = RequestStatusRequest {
            request_id: ctx.msg_meta.msgid,
        };
        let client = crate::Client::default();
        let still_waiting: bool = match client.request_is_pending(ctx, &request).await {
            Ok(w) => w,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if !still_waiting {
            return Err(RemoteIoError::new(
                Error::new(
                    ErrorKind::Timeout,
                    "RDMA read completed but client request has already timed out".into(),
                ),
                Some(local),
            ));
        }

        Ok(local)
    }

    pub(super) async fn write_remote(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        // No one-sided RDMA WRITE (unsafe against client buffer lifetime):
        // send a reverse `read_into_target` RPC advertising our source buffers as read
        // regions; the client executes RDMA READs into its pinned write
        // target. The request owns the source buffers through ReadSource;
        // they are recovered only after local readers release their holds.
        let req = ReadIntoTargetRequest {
            request_id: ctx.msg_meta.msgid,
            ops: ops.to_vec(),
        };
        let bytes = ops.iter().map(|op| op.len).sum();
        let client = crate::Client::default();
        let mut source = client
            .with_read_buffers(local)
            .with_read_charge_bytes(bytes);
        let result = source.read_into_target(ctx, &req).await;
        let buffers = source.take_read_buffers();
        match result {
            Ok(()) => buffers.ok_or_else(|| {
                RemoteIoError::new(
                    Error::new(
                        ErrorKind::BuffersInUse,
                        "RDMA write completed while its source buffers still have active readers"
                            .into(),
                    ),
                    None,
                )
            }),
            Err(e) => Err(RemoteIoError::new(e, buffers)),
        }
    }
}
