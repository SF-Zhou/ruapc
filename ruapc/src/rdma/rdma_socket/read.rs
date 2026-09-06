//! READ planning in logical buffer coordinates. The verbs crate owns posted
//! destinations and validates their local ranges before exposing them to DMA.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{ReadFailure, ReadPosting, ReadRequest, ReadSegment};

use super::RdmaSocket;
use crate::{
    Buffer, Context, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result,
    remote_memory::{
        WriteTarget,
        scatter::{self, SpaceLayout},
    },
    services::{MemoryService, ReadIntoTargetRequest, RequestStatusRequest},
};

fn build_planned_reads(
    regions: &[RemoteBufferInfo],
    src_layout: &SpaceLayout,
    dst_layout: &SpaceLayout,
    ops: &[CopyOp],
    max_sge: usize,
) -> Result<Vec<ReadRequest>> {
    scatter::plan_chunks(src_layout, dst_layout, ops, max_sge.max(1))
        .into_iter()
        .map(|chunk| {
            let region = &regions[chunk.seg];
            let segments = chunk
                .dst
                .into_iter()
                .map(|slice| {
                    Ok(ReadSegment {
                        buffer: slice.seg,
                        offset: usize::try_from(slice.off)
                            .map_err(|_| Error::kind(ErrorKind::InvalidCopyOp))?,
                        len: usize::try_from(slice.len)
                            .map_err(|_| Error::kind(ErrorKind::InvalidCopyOp))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ReadRequest {
                remote_addr: region
                    .addr
                    .checked_add(chunk.off)
                    .ok_or_else(|| Error::kind(ErrorKind::InvalidCopyOp))?,
                rkey: region.key.rkey,
                segments,
            })
        })
        .collect()
}

fn plan_error(error: ruapc_rdma::Error) -> Error {
    if error.kind == ruapc_rdma::ErrorKind::InvalidReadPlan {
        Error::new(ErrorKind::InvalidCopyOp, error.to_string())
    } else {
        error.into()
    }
}

fn completion_error(reason: ReadFailure) -> Error {
    match reason {
        ReadFailure::Timeout => Error::new(
            ErrorKind::RdmaReadTimeout,
            "RDMA READ exceeded rdma.remote_memory.read_timeout_ms".into(),
        ),
        ReadFailure::ConnectionClosed => Error::new(
            ErrorKind::ConnectionClosed,
            "RDMA poller shut down with reads in flight".into(),
        ),
        ReadFailure::Completion | ReadFailure::Cancelled => Error::new(
            ErrorKind::RdmaSendFailed,
            "RDMA READ batch failed or was abandoned".into(),
        ),
    }
}

impl RdmaSocket {
    pub(crate) fn fail_read_batches(&self) {
        self.queue_pair.fail_pending_reads();
    }

    /// Preparation takes real buffer ownership; local addresses and keys never
    /// cross this layer's interface. Failed/cancelled batches stay in the QP
    /// until all posted requests complete or it is successfully destroyed.
    async fn execute_reads(
        &self,
        reads: Vec<ReadRequest>,
        local: Vec<Buffer>,
        request_remaining: Option<Duration>,
    ) -> std::result::Result<Vec<Buffer>, (Error, Option<Vec<Buffer>>)> {
        let bytes = reads
            .iter()
            .flat_map(|read| &read.segments)
            .map(|segment| segment.len as u64)
            .sum();
        if let Err(error) = self
            .bandwidth_limiter
            .reserve_recv(bytes, request_remaining)
            .await
        {
            return Err((error, Some(local)));
        }
        let deadline = self.read_timeout.map(|timeout| Instant::now() + timeout);
        let (mut posting, receiver) = self
            .queue_pair
            .prepare_reads(local, reads, deadline)
            .map_err(|(error, buffers)| (plan_error(error), Some(buffers)))?;
        for posted in 0..posting.remaining() {
            if let Err(error) = self.post_read(&mut posting).await {
                if posted == 0 {
                    return Err((error, posting.cancel()));
                }
                // Drop accounts the unposted suffix before flushing the QP.
                drop(posting);
                self.set_error();
                let _ = receiver.await;
                return Err((error, None));
            }
        }
        let result = receiver.await;
        // Retain the posting owner's Arc until notification so the runtime
        // task, rather than the CQ poller, normally destroys the batch.
        drop(posting);
        match result {
            Ok(Ok(buffers)) => Ok(buffers),
            Ok(Err(reason)) => Err((completion_error(reason), None)),
            Err(_) => Err((completion_error(ReadFailure::Cancelled), None)),
        }
    }

    async fn post_read(&self, posting: &mut ReadPosting) -> Result<()> {
        let permits_closed =
            |_| Error::new(ErrorKind::RdmaSendFailed, "RDMA read permits closed".into());
        // A congested connection must not reserve the whole device's capacity.
        let sq_permit = self
            .sq_read_permits
            .acquire()
            .await
            .map_err(permits_closed)?;
        let device_permit = self.read_permits.acquire().await.map_err(permits_closed)?;
        self.queue_pair.post_read(posting).map_err(plan_error)?;
        sq_permit.forget();
        device_permit.forget();
        Ok(())
    }

    pub(crate) async fn read_into_target(
        &self,
        regions: &[RemoteBufferInfo],
        src_layout: &SpaceLayout,
        ops: &[CopyOp],
        target: Arc<WriteTarget>,
        request_remaining: Option<Duration>,
    ) -> Result<()> {
        let planned = build_planned_reads(
            regions,
            src_layout,
            target.layout(),
            ops,
            self.queue_pair.gather_limit(),
        )?;
        if planned.is_empty() {
            return Ok(());
        }
        let buffers = target.take_for_read()?;
        match self
            .execute_reads(planned, buffers, request_remaining)
            .await
        {
            Ok(buffers) => {
                target.restore_after_read(buffers);
                Ok(())
            }
            Err((error, buffers)) => {
                if let Some(buffers) = buffers {
                    target.restore_after_read(buffers);
                }
                Err(error)
            }
        }
    }

    pub(super) async fn read_remote(
        &self,
        ctx: &Context,
        ops: &[CopyOp],
        local: Vec<Buffer>,
        remote: &RemoteSpace<'_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        let layout = SpaceLayout::from_lens(local.iter().map(|buffer| buffer.len() as u64));
        let planned = layout.and_then(|layout| {
            build_planned_reads(
                remote.regions(),
                remote.layout(),
                &layout,
                ops,
                self.queue_pair.gather_limit(),
            )
        });
        let planned = match planned {
            Ok(planned) => planned,
            Err(error) => return Err(RemoteIoError::new(error, Some(local))),
        };
        if planned.is_empty() {
            return Ok(local);
        }
        let local = self
            .execute_reads(planned, local, ctx.remaining_time())
            .await
            .map_err(|(error, buffers)| RemoteIoError::new(error, buffers))?;

        // A one-sided READ cannot acknowledge source lifetime to the peer.
        // Reject data from a request that expired before the read completed.
        let request = RequestStatusRequest {
            request_id: ctx.msg_meta.msgid,
        };
        let client = crate::Client::default();
        let pending = match client.request_is_pending(ctx, &request).await {
            Ok(pending) => pending,
            Err(error) => return Err(RemoteIoError::new(error, Some(local))),
        };
        if !pending {
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
        // The reverse RPC lets the client own the destination through completion:
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
