//! Validated, request-bound transfers sharing one executor across input modes.

use super::{Recovery, rpc_transfer};
use crate::{
    Buffer, Completed, Context, CopyOp, Error, ErrorKind, RemoteSpace, Result, Socket,
    core::ContextEndpoint,
};
use ruapc_bufpool::WriteBuffer;
use ruapc_memory::{BorrowedSource, BufferSet, CopyBatch};

pub(crate) struct PreparedRead<'ctx, 'ops> {
    pub(crate) ctx: &'ctx Context,
    socket: &'ctx Socket,
    #[cfg(feature = "rdma")]
    pub(crate) remote: RemoteSpace<'ctx>,
    pub(crate) ops: CopyBatch<'ops>,
    pub(crate) local: BufferSet<WriteBuffer>,
    #[cfg(feature = "rdma")]
    pub(crate) recovery: Recovery,
}

fn connection(ctx: &Context) -> Result<&Socket> {
    match &ctx.endpoint {
        ContextEndpoint::Connected(socket) => Ok(socket),
        _ => Err(Error::new(
            ErrorKind::NotConnected,
            "remote memory transfers require a connected handler context".into(),
        )),
    }
}

pub(crate) type TransferCompletion<B> = Completed<Result<()>, Vec<B>>;

impl<'ctx, 'ops> PreparedRead<'ctx, 'ops> {
    fn prepare(
        ctx: &'ctx Context,
        remote: Result<RemoteSpace<'ctx>>,
        ops: &'ops [CopyOp],
        local: BufferSet<WriteBuffer>,
        _recovery: Recovery,
    ) -> std::result::Result<Self, TransferCompletion<WriteBuffer>> {
        let checked = (|| {
            let socket = connection(ctx)?;
            let remote = remote?;
            let ops = CopyBatch::new(ops)?;
            ops.check_bounds(remote.total_len(), local.layout().total())?;
            Ok::<_, Error>((socket, remote, ops))
        })();
        match checked {
            Ok((socket, _remote, ops)) => Ok(Self {
                ctx,
                socket,
                #[cfg(feature = "rdma")]
                remote: _remote,
                ops,
                local,
                #[cfg(feature = "rdma")]
                recovery: _recovery,
            }),
            Err(error) => Err(Completed {
                result: Err(error),
                buffers: Some(local.into_buffers()),
            }),
        }
    }
}

impl PreparedRead<'_, '_> {
    async fn execute(self) -> TransferCompletion<WriteBuffer> {
        if self.ops.byte_count() == 0 {
            return Completed {
                result: Ok(()),
                buffers: Some(self.local.into_buffers()),
            };
        }
        match self.socket {
            Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => rpc_transfer::read(self).await,
            #[cfg(feature = "rdma")]
            Socket::RDMA(socket) => socket.read_remote(self).await,
        }
    }
}

fn checked_buffers(
    buffers: Vec<WriteBuffer>,
) -> std::result::Result<BufferSet<WriteBuffer>, TransferCompletion<WriteBuffer>> {
    BufferSet::try_new(buffers).map_err(|e| {
        let (error, buffers) = e.into_parts();
        Completed {
            result: Err(error.into()),
            buffers: Some(buffers),
        }
    })
}

pub(super) async fn read(
    ctx: &Context,
    ops: Option<&[CopyOp]>,
    local: Vec<WriteBuffer>,
    recovery: Recovery,
) -> TransferCompletion<WriteBuffer> {
    read_from(ctx, ctx.remote_read_space(), ops, local, recovery).await
}

async fn read_from<'ctx>(
    ctx: &'ctx Context,
    remote: Result<RemoteSpace<'ctx>>,
    ops: Option<&[CopyOp]>,
    local: Vec<WriteBuffer>,
    recovery: Recovery,
) -> TransferCompletion<WriteBuffer> {
    let all = [CopyOp::new(
        0,
        0,
        remote.as_ref().map_or(0, RemoteSpace::total_len),
    )];
    let ops = ops.unwrap_or(&all);
    match checked_buffers(local)
        .and_then(|local| PreparedRead::prepare(ctx, remote, ops, local, recovery))
    {
        Ok(prepared) => prepared.execute().await,
        Err(completed) => completed,
    }
}
/// Sources stay borrowed by this future only. Stream requests carry copied
/// payloads; RDMA requests carry metadata and never publish local CPU access.
pub(super) async fn write(ctx: &Context, ops: Option<&[CopyOp]>, local: &[Buffer]) -> Result<()> {
    let socket = connection(ctx)?;
    let remote = ctx.remote_write_space()?;
    let source = BorrowedSource::new(local)?;
    let all = [CopyOp::new(0, 0, source.layout().total())];
    let batch = CopyBatch::new(ops.unwrap_or(&all))?;
    batch.check_bounds(source.layout().total(), remote.total_len())?;
    if batch.byte_count() == 0 {
        return Ok(());
    }
    match socket {
        Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => {
            // Every CPU read finishes synchronously before the request can be
            // queued or awaited. Dropping or forgetting it leaves owned bytes.
            let bytes = source.gather(&batch)?;
            rpc_transfer::write(ctx, batch, bytes).await
        }
        #[cfg(feature = "rdma")]
        Socket::RDMA(_) => rpc_transfer::write_via_read(ctx, batch, local).await,
    }
}

pub(super) async fn read_allocated(ctx: &Context) -> Result<Vec<Buffer>> {
    connection(ctx)?;
    let remote = ctx.remote_read_space()?;
    let mut local = Vec::with_capacity(remote.regions().len());
    for region in remote.regions().iter().filter(|region| region.len != 0) {
        let len = usize::try_from(region.len)?;
        let mut buffer = ctx
            .state
            .buffer_pool
            .allocate(len)
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;
        buffer.set_len(len);
        local.push(WriteBuffer::from(buffer));
    }
    let completed = read_from(ctx, Ok(remote), None, local, Recovery::DetachOwned).await;
    completed.result?;
    Ok(completed
        .buffers
        .expect("successful reads return their destinations")
        .into_iter()
        .map(|buffer| buffer.into_owned().expect("allocated read destinations"))
        .collect())
}
