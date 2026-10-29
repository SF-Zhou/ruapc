//! A transfer binds validated memory to the request and connection that own it.

use ruapc_memory::{BufferSet, CopyBatch};

use super::{BufferAvailability, WriteReceipt, rpc_transfer};
use crate::{
    Buffer, Context, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result, Socket,
    core::ContextEndpoint,
};

enum Direction {
    Read,
    Write,
}

/// Only preparation in this module constructs a transfer. Executors consume it,
/// so they cannot select a different request, connection, or memory space.
pub(crate) struct Transfer<'ctx, 'ops> {
    pub(crate) ctx: &'ctx Context,
    socket: &'ctx Socket,
    #[cfg(feature = "rdma")]
    pub(crate) remote: RemoteSpace<'ctx>,
    pub(crate) ops: CopyBatch<'ops>,
    pub(crate) local: BufferSet,
}

fn buffer_set(local: Vec<Buffer>) -> std::result::Result<BufferSet, RemoteIoError> {
    BufferSet::try_new(local).map_err(|error| {
        let (error, buffers) = error.into_parts();
        RemoteIoError::new(error.into(), Some(buffers))
    })
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

impl<'ctx, 'ops> Transfer<'ctx, 'ops> {
    fn prepare(
        ctx: &'ctx Context,
        remote: Result<RemoteSpace<'ctx>>,
        ops: &'ops [CopyOp],
        local: BufferSet,
        direction: Direction,
    ) -> std::result::Result<Self, RemoteIoError> {
        let validation = (|| {
            let socket = connection(ctx)?;
            let remote = remote?;
            let ops = CopyBatch::new(ops)?;
            let (source, destination) = match direction {
                Direction::Read => (remote.total_len(), local.layout().total()),
                Direction::Write => (local.layout().total(), remote.total_len()),
            };
            ops.check_bounds(source, destination)?;
            Ok::<_, Error>((socket, remote, ops))
        })();
        match validation {
            Ok((socket, _remote, ops)) => Ok(Self {
                ctx,
                socket,
                #[cfg(feature = "rdma")]
                remote: _remote,
                ops,
                local,
            }),
            Err(error) => Err(RemoteIoError::new(error, Some(local.into_buffers()))),
        }
    }
}

pub(crate) struct PreparedRead<'ctx, 'ops>(Transfer<'ctx, 'ops>);
pub(crate) struct PreparedWrite<'ctx, 'ops>(Transfer<'ctx, 'ops>);

impl<'ctx, 'ops> PreparedRead<'ctx, 'ops> {
    pub(crate) fn into_parts(self) -> Transfer<'ctx, 'ops> {
        self.0
    }

    async fn execute(self) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        if self.0.ops.byte_count() == 0 {
            return Ok(self.0.local.into_buffers());
        }
        match self.0.socket {
            Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => rpc_transfer::read(self).await,
            #[cfg(feature = "rdma")]
            Socket::RDMA(socket) => socket.read_remote(self).await,
        }
    }
}

impl<'ctx, 'ops> PreparedWrite<'ctx, 'ops> {
    pub(crate) fn into_parts(self) -> Transfer<'ctx, 'ops> {
        self.0
    }

    async fn execute(self) -> std::result::Result<WriteReceipt, RemoteIoError> {
        let sources = if self.0.ops.byte_count() == 0 {
            BufferAvailability::Available(self.0.local.into_buffers())
        } else {
            match self.0.socket {
                Socket::TCP(_) | Socket::WS(_) | Socket::HTTP(_) => {
                    rpc_transfer::write(self).await?
                }
                #[cfg(feature = "rdma")]
                Socket::RDMA(_) => rpc_transfer::write_via_read(self).await?,
            }
        };
        Ok(WriteReceipt::new(sources))
    }
}

pub(super) async fn read(
    ctx: &Context,
    ops: &[CopyOp],
    local: Vec<Buffer>,
) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
    PreparedRead(Transfer::prepare(
        ctx,
        ctx.remote_read_space(),
        ops,
        buffer_set(local)?,
        Direction::Read,
    )?)
    .execute()
    .await
}

pub(super) async fn write(
    ctx: &Context,
    ops: &[CopyOp],
    local: Vec<Buffer>,
) -> std::result::Result<WriteReceipt, RemoteIoError> {
    PreparedWrite(Transfer::prepare(
        ctx,
        ctx.remote_write_space(),
        ops,
        buffer_set(local)?,
        Direction::Write,
    )?)
    .execute()
    .await
}

pub(super) async fn read_all(ctx: &Context) -> Result<Vec<Buffer>> {
    connection(ctx)?;
    let remote = ctx.remote_read_space()?;
    let mut local = Vec::with_capacity(remote.regions().len());
    for region in remote.regions().iter().filter(|region| region.len != 0) {
        let len = usize::try_from(region.len)
            .map_err(|_| Error::new(ErrorKind::InvalidArgument, "region too large".into()))?;
        let mut buffer = ctx
            .state
            .buffer_pool
            .allocate(len)
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))?;
        buffer.set_len(len);
        local.push(buffer);
    }
    let ops = [CopyOp::new(0, 0, remote.total_len())];
    // Internally allocated destinations have no caller that needs recovery.
    Ok(PreparedRead(Transfer::prepare(
        ctx,
        Ok(remote),
        &ops,
        buffer_set(local)?,
        Direction::Read,
    )?)
    .execute()
    .await?)
}

pub(super) async fn write_all(
    ctx: &Context,
    local: Vec<Buffer>,
) -> std::result::Result<WriteReceipt, RemoteIoError> {
    let local = buffer_set(local)?;
    let ops = [CopyOp::new(0, 0, local.layout().total())];
    PreparedWrite(Transfer::prepare(
        ctx,
        ctx.remote_write_space(),
        &ops,
        local,
        Direction::Write,
    )?)
    .execute()
    .await
}
