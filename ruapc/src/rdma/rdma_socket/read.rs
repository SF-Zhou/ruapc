//! READ planning in logical buffer coordinates. The verbs crate owns posted
//! destinations and validates their local ranges before exposing them to DMA.

use std::{sync::Arc, time::Instant};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_memory::{BufferSet, CopyBatch, SpaceLayout};
use ruapc_rdma::{ReadCompletion, ReadFailure, ReadPosting, ReadRequest, ReadSegment};

mod credits;
mod pending;
pub(crate) use credits::ReadCredits;
pub(super) use pending::PendingReads;

use super::RdmaSocket;
use crate::{
    Buffer, Client, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result,
    remote_memory::{WriteTarget, transfer::PreparedRead},
    services::{MemoryService, RequestStatusRequest},
};

fn build_planned_reads(
    source: &RemoteSpace<'_>,
    dst_layout: &SpaceLayout,
    ops: &CopyBatch<'_>,
    max_sge: usize,
) -> Result<Vec<ReadRequest>> {
    ops.plan_chunks(source.layout(), dst_layout, max_sge.max(1))?
        .into_iter()
        .map(|chunk| {
            let region = &source.regions()[chunk.seg];
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

fn read_failure_error(reason: ReadFailure) -> Error {
    match reason {
        ReadFailure::Timeout => Error::new(
            ErrorKind::RdmaReadTimeout,
            "RDMA READ exceeded rdma.remote_memory.read_timeout_ms".into(),
        ),
        ReadFailure::RequestTimeout => Error::new(
            ErrorKind::Timeout,
            "request deadline expired during RDMA READ".into(),
        ),
        ReadFailure::ConnectionClosed => Error::new(
            ErrorKind::ConnectionClosed,
            "RDMA connection closed with reads in flight".into(),
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

#[derive(Clone, Copy)]
struct ReadDeadline {
    at: Instant,
    reason: ReadFailure,
}

impl ReadDeadline {
    fn earliest(read: Option<Instant>, request: Option<Instant>) -> Option<Self> {
        let (at, reason) = match (read, request) {
            (Some(read), Some(request)) if read < request => (read, ReadFailure::Timeout),
            (_, Some(request)) => (request, ReadFailure::RequestTimeout),
            (Some(read), None) => (read, ReadFailure::Timeout),
            (None, None) => return None,
        };
        Some(Self { at, reason })
    }

    fn check(deadline: Option<Self>) -> Result<()> {
        if let Some(deadline) = deadline
            && Instant::now() >= deadline.at
        {
            return Err(read_failure_error(deadline.reason));
        }
        Ok(())
    }
}

impl RdmaSocket {
    /// Transport payload READs use the same owned plans and completion evidence
    /// as remote memory, with a separate payload finish RPC for the source.
    pub(super) async fn read_payload(
        &self,
        regions: &[RemoteBufferInfo],
        buffer: Buffer,
        request_deadline: Option<Instant>,
    ) -> Result<Buffer> {
        let src = RemoteSpace::new(regions)?;
        let dst = SpaceLayout::from_lens([buffer.len() as u64])?;
        let ops = [CopyOp::new(0, 0, buffer.len() as u64)];
        let batch = CopyBatch::new(&ops)?;
        let reads = build_planned_reads(&src, &dst, &batch, self.queue_pair.gather_limit())?;
        let mut buffers = self
            .execute_reads(reads, vec![buffer], request_deadline)
            .await?;
        Ok(buffers.pop().expect("one owned payload destination"))
    }

    pub(crate) fn fail_read_batches(&self) {
        self.pending_reads.fail();
    }

    pub(crate) fn expire_read_batches(&self, now: Instant) -> bool {
        self.pending_reads.expire(now)
    }

    pub(crate) fn register_read_batch(
        &self,
        posting: &ReadPosting,
        read_deadline: Option<Instant>,
        request_deadline: Option<Instant>,
    ) {
        self.pending_reads
            .register(posting.handle(), read_deadline, request_deadline);
    }

    /// Preparation takes real buffer ownership; local addresses and keys never
    /// cross this layer's interface. Failed/cancelled batches stay in the QP
    /// until all posted requests complete or it is successfully destroyed.
    async fn execute_reads(
        &self,
        reads: Vec<ReadRequest>,
        local: Vec<Buffer>,
        request_deadline: Option<Instant>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        if !self.state.is_ok() {
            return Err(RemoteIoError::new(read_closed_error(), Some(local)));
        }
        // Admission consumes the same absolute budgets as completion. Keep
        // both: request expiry must not disable the connection's READ watchdog.
        let now = Instant::now();
        let read_deadline = self.read_timeout.map(|timeout| now + timeout);
        let deadline = ReadDeadline::earliest(read_deadline, request_deadline);
        let bytes = reads
            .iter()
            .flat_map(|read| &read.segments)
            .map(|segment| segment.len as u64)
            .sum();
        if let Err(error) = self
            .bandwidth_limiter
            .reserve_recv(bytes, deadline.map(|deadline| deadline.at))
            .await
        {
            // The shaper rejects a wait beyond the supplied budget as Timeout,
            // including when the tighter budget came from the READ watchdog.
            let error = if error.kind == ErrorKind::Timeout {
                deadline.map_or(error, |deadline| read_failure_error(deadline.reason))
            } else {
                error
            };
            return Err(RemoteIoError::new(error, Some(local)));
        }
        if let Err(error) = ReadDeadline::check(deadline) {
            return Err(RemoteIoError::new(error, Some(local)));
        }
        let (mut posting, receiver) = self
            .queue_pair
            .prepare_reads(local, reads)
            .map_err(|(error, buffers)| RemoteIoError::new(plan_error(error), Some(buffers)))?;
        let mut registered = false;
        while posting.remaining() != 0 {
            if let Err(error) = self.post_read(&mut posting, deadline).await {
                // The cursor returns buffers only before the first post;
                // otherwise it accounts the suffix and preserves posted DMA.
                let buffers = posting.cancel();
                // Waiting for admission does not prove a QP failure. The
                // poller still applies the independent READ watchdog to any
                // outstanding work after this caller's deadline has expired.
                if buffers.is_none()
                    && !matches!(error.kind, ErrorKind::Timeout | ErrorKind::RdmaReadTimeout)
                {
                    self.set_error();
                }
                return Err(RemoteIoError::new(error, buffers));
            }
            if !registered {
                // Publish after the first provider-accepted WR and before the
                // next await, including admission for a later WR. Cancellation
                // cannot skip registration while leaving posted DMA behind.
                self.register_read_batch(&posting, read_deadline, request_deadline);
                registered = true;
            }
        }
        let result = self.await_read_batch(receiver).await;
        // Retain the posting owner's Arc until notification so the runtime
        // task, rather than the CQ poller, normally destroys the batch.
        drop(posting);
        result.map_err(Into::into)
    }

    /// Waits after posting finishes or its unposted suffix has been cancelled.
    pub(crate) async fn await_read_batch(&self, receiver: ReadCompletion) -> Result<Vec<Buffer>> {
        // A task may pass its final health check, then record its READ only
        // after shutdown has scanned the pending list. Recheck only once per
        // batch, after posting, so that late work receives a failure even when
        // no poller remains. If shutdown scans after publication instead, the
        // registered batch fails there. Registration follows the first
        // provider-accepted post and precedes this recheck, so one of the two
        // observers always sees the batch. Failure notification leaves the DMA
        // holds in the QP.
        if !self.state.is_ok() {
            self.fail_read_batches();
        }
        receiver.await.map_err(read_failure_error)
    }

    async fn post_read(
        &self,
        posting: &mut ReadPosting,
        deadline: Option<ReadDeadline>,
    ) -> Result<()> {
        let (sq_permit, device_permit) = self.read_credits.acquire(deadline).await?;
        if !self.state.is_ok() {
            return Err(read_closed_error());
        }
        // Covers uncontended admission and credits becoming ready together
        // with expiry, without creating a timer for the fast path.
        ReadDeadline::check(deadline)?;
        // The held SQ permit keeps the completion route registered even if
        // closing the connection races this final check and the actual post.
        self.queue_pair.post_read(posting).map_err(plan_error)?;
        sq_permit.forget();
        device_permit.forget();
        Ok(())
    }

    pub(crate) async fn read_into_target(
        &self,
        source: &RemoteSpace<'_>,
        ops: &CopyBatch<'_>,
        target: Arc<WriteTarget>,
        request_deadline: Option<Instant>,
    ) -> Result<()> {
        ops.check_bounds(source.total_len(), target.total_len())?;
        if ops.byte_count() == 0 {
            return Ok(());
        }
        let (planned, buffers) = target.checkout(|layout| {
            build_planned_reads(source, layout, ops, self.queue_pair.gather_limit())
        })?;
        match self
            .execute_reads(planned, buffers.into_buffers(), request_deadline)
            .await
        {
            Ok(buffers) => {
                target.restore(BufferSet::try_new(buffers).expect("READ preserves buffer layout"));
                Ok(())
            }
            Err(mut error) => {
                if let Some(buffers) = error.take_recovered_buffers() {
                    target.restore(
                        BufferSet::try_new(buffers).expect("READ preserves buffer layout"),
                    );
                }
                Err(error.into())
            }
        }
    }

    pub(crate) async fn read_remote(
        &self,
        prepared: PreparedRead<'_, '_>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        let transfer = prepared.into_parts();
        let planned = match build_planned_reads(
            &transfer.remote,
            transfer.local.layout(),
            &transfer.ops,
            self.queue_pair.gather_limit(),
        ) {
            Ok(planned) => planned,
            Err(error) => {
                return Err(RemoteIoError::new(
                    error,
                    Some(transfer.local.into_buffers()),
                ));
            }
        };
        let ctx = transfer.ctx;
        let local = self
            .execute_reads(planned, transfer.local.into_buffers(), ctx.deadline)
            .await?;

        // Validate after READ completion; see Context::remote_read for the
        // immutable-source and request-identity assumptions. Error destinations
        // below are recoverable memory, not validated transfer results.
        let request = RequestStatusRequest {
            request_id: ctx.msg_meta.msgid,
        };
        let client = Client::default();
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
}

#[cfg(test)]
mod timeout_tests;
