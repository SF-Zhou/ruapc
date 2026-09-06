//! READ planning in logical buffer coordinates. The verbs crate owns posted
//! destinations and validates their local ranges before exposing them to DMA.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{ReadFailure, ReadPosting, ReadRequest, ReadSegment};
use tokio::sync::{Notify, Semaphore, SemaphorePermit};

use super::RdmaSocket;
use crate::{
    Buffer, Context, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result,
    rdma::RdmaState,
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

fn read_closed_error() -> Error {
    Error::new(
        ErrorKind::ConnectionClosed,
        "RDMA connection closed before READ posting".into(),
    )
}

async fn acquire_read_permits<'a>(
    state: &RdmaState,
    sq: &'a Semaphore,
    device: &'a Semaphore,
    closed: &Notify,
) -> Result<(SemaphorePermit<'a>, SemaphorePermit<'a>)> {
    if !state.is_ok() {
        return Err(read_closed_error());
    }
    // Uncontended reads need only the existing semaphore operations. A
    // closure waiter is registered only when either resource is exhausted.
    if let Ok(sq_permit) = sq.try_acquire()
        && let Ok(device_permit) = device.try_acquire()
    {
        return Ok((sq_permit, device_permit));
    }
    let notified = closed.notified();
    tokio::pin!(notified);
    // Register before checking state so closure cannot fall between that
    // check and the first poll of a blocked semaphore acquisition.
    notified.as_mut().enable();
    if !state.is_ok() {
        return Err(read_closed_error());
    }
    tokio::select! {
        biased;
        _ = &mut notified => Err(read_closed_error()),
        permits = async {
            // A congested connection must not reserve the whole NIC's capacity.
            let sq_permit = sq.acquire().await.map_err(|_| read_closed_error())?;
            let device_permit = device.acquire().await.map_err(|_| read_closed_error())?;
            Ok((sq_permit, device_permit))
        } => permits,
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
        if !self.state.is_ok() {
            return Err((read_closed_error(), Some(local)));
        }
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
        let (sq_permit, device_permit) = acquire_read_permits(
            &self.state,
            &self.sq_read_permits,
            &self.read_permits,
            &self.read_closed,
        )
        .await?;
        if !self.state.is_ok() {
            return Err(read_closed_error());
        }
        // The held SQ permit keeps the completion route registered even if
        // closing the connection races this final check and the actual post.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closing_read_admission_wakes_a_task_waiting_for_sq_capacity() {
        let state = RdmaState::new(4);
        let sq = Semaphore::new(1);
        let device = Semaphore::new(2);
        let closed = Notify::new();
        let outstanding = sq.acquire().await.unwrap();
        let acquiring = acquire_read_permits(&state, &sq, &device, &closed);
        tokio::pin!(acquiring);
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(device.available_permits(), 2);

        state.set_error();
        sq.close();
        closed.notify_waiters();
        assert_eq!(
            acquiring.await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(sq.available_permits(), 0);
        drop(outstanding);
        assert_eq!(sq.available_permits(), 1);
        assert_eq!(device.available_permits(), 2);
    }

    #[tokio::test]
    async fn closing_read_admission_releases_sq_capacity_while_device_is_full() {
        let state = RdmaState::new(4);
        let sq = Semaphore::new(2);
        let device = Semaphore::new(1);
        let closed = Notify::new();
        let unrelated_read = device.acquire().await.unwrap();
        let acquiring = acquire_read_permits(&state, &sq, &device, &closed);
        tokio::pin!(acquiring);
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(sq.available_permits(), 1);

        state.set_error();
        sq.close();
        closed.notify_waiters();
        assert_eq!(
            acquiring.await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(sq.available_permits(), 2);
        assert_eq!(device.available_permits(), 0);
        assert!(!device.is_closed());
        drop(unrelated_read);
        assert_eq!(device.available_permits(), 1);
    }

    #[tokio::test]
    async fn closure_before_registering_a_waiter_rejects_without_taking_permits() {
        let state = RdmaState::new(4);
        let sq = Semaphore::new(2);
        let device = Semaphore::new(2);
        let closed = Notify::new();
        state.set_error();
        sq.close();
        closed.notify_waiters();

        assert_eq!(
            acquire_read_permits(&state, &sq, &device, &closed)
                .await
                .unwrap_err()
                .kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(sq.available_permits(), 2);
        assert_eq!(device.available_permits(), 2);
    }
}
