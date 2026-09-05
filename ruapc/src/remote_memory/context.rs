//! Request-attached memory spaces and transfer validation.

use super::scatter::{self, SpaceLayout};
use crate::core::ContextEndpoint;
use crate::{Buffer, Context, CopyOp, Error, RemoteIoError, Result, Socket, SocketTrait};
use ruapc_bufpool::RemoteBufferInfo;

/// A read or write view of the remote peer's registered memory, built from
/// the regions attached to the current request.
///
/// The regions form one logical contiguous space (in region order, each
/// contributing its advertised length); [`CopyOp`] offsets address this
/// space. Obtained via [`Context::remote_read_space`] /
/// [`Context::remote_write_space`].
#[derive(Debug)]
pub struct RemoteSpace<'a> {
    regions: &'a [RemoteBufferInfo],
    layout: SpaceLayout,
}

impl<'a> RemoteSpace<'a> {
    pub(crate) fn new(regions: &'a [RemoteBufferInfo]) -> Result<Self> {
        for region in regions {
            if region.addr.checked_add(region.len).is_none() {
                return Err(Error::new(
                    crate::ErrorKind::InvalidCopyOp,
                    "remote region addr + len overflows u64".into(),
                ));
            }
        }
        let layout = SpaceLayout::from_lens(regions.iter().map(|r| r.len))?;
        Ok(Self { regions, layout })
    }

    /// Total length of the logical space in bytes.
    #[must_use]
    pub fn total_len(&self) -> u64 {
        self.layout.total()
    }

    /// The raw regions composing the space, in order.
    #[must_use]
    pub fn regions(&self) -> &'a [RemoteBufferInfo] {
        self.regions
    }

    /// Number of regions.
    #[must_use]
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    /// Only the RDMA read planner consumes the layout directly.
    #[cfg_attr(not(feature = "rdma"), allow(dead_code))]
    pub(crate) fn layout(&self) -> &SpaceLayout {
        &self.layout
    }
}

/// Builds the logical-space layout of a set of local buffers (each
/// contributing its logical length).
fn local_layout(buffers: &[Buffer]) -> Result<SpaceLayout> {
    SpaceLayout::from_lens(buffers.iter().map(|b| b.len() as u64))
}

impl Context {
    /// Returns the *read space* the client attached to the current request
    /// via [`Client::with_read_buffers`](crate::Client::with_read_buffers):
    /// the logical concatenation of the advertised regions, readable with
    /// [`remote_read`](Self::remote_read).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no read regions.
    pub fn remote_read_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.read_regions.is_empty() {
            return Err(Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "request carries no read regions; client must attach buffers \
                 via with_read_buffers()"
                    .into(),
            ));
        }
        RemoteSpace::new(&self.msg_meta.read_regions)
    }

    /// Returns the *write space* the client attached to the current request
    /// via [`Client::with_write_buffers`](crate::Client::with_write_buffers):
    /// the logical concatenation of the pinned destination regions,
    /// writable with [`remote_write`](Self::remote_write).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no write regions.
    pub fn remote_write_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.write_regions.is_empty() {
            return Err(Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "request carries no write regions; client must attach buffers \
                 via with_write_buffers()"
                    .into(),
            ));
        }
        RemoteSpace::new(&self.msg_meta.write_regions)
    }

    /// Executes a batch of reads from the client's read space into `local`.
    ///
    /// Both sides are logical contiguous spaces: the client's attached
    /// read buffers (source, see [`remote_read_space`](Self::remote_read_space))
    /// and the concatenation of the `local` buffers' logical lengths
    /// (destination). Each [`CopyOp`] copies `len` bytes from
    /// `src_offset` (client space) to `dst_offset` (local space).
    ///
    /// The batch is validated before anything is transferred: bounds,
    /// overflow, op count, and non-overlapping destination ranges. On RDMA
    /// the ops are fragmented into one-sided RDMA READ work requests
    /// (contiguous remote range + local scatter-gather list) executed
    /// concurrently; on TCP/WS/HTTP a reverse
    /// `MemoryService::read_inline` RPC
    /// moves the bytes inline.
    ///
    /// Returns the same buffers, now filled at the ops' destination
    /// ranges. On failure they are handed back inside [`RemoteIoError`]
    /// whenever they survived the operation; propagating with `?` converts
    /// to [`Error`] and drops them back to the pool.
    pub async fn remote_read(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        if ops.iter().all(|op| op.len == 0) {
            return Ok(local);
        }
        let (socket, space) = match self.prepare_transfer(ops, &local, Direction::Read) {
            Ok(transfer) => transfer,
            Err(error) => return Err(RemoteIoError::new(error, Some(local))),
        };
        socket.remote_read(self, ops, local, &space).await
    }

    /// Reads the client's entire read space into freshly allocated
    /// buffers, one per region (mirroring the client's segmentation).
    ///
    /// Convenience wrapper around
    /// [`remote_read_space`](Self::remote_read_space) and
    /// [`remote_read`](Self::remote_read). The returned buffers' logical
    /// lengths equal the transferred sizes; zero-length regions are
    /// skipped.
    pub async fn remote_read_all(&self) -> Result<Vec<Buffer>> {
        let space = self.remote_read_space()?;
        let total = space.total_len();
        let mut local = Vec::new();
        for region in space.regions() {
            if region.len == 0 {
                continue;
            }
            let mut buf = self
                .state
                .buffer_pool
                .allocate(usize::try_from(region.len).map_err(|_| {
                    Error::new(crate::ErrorKind::InvalidArgument, "region too large".into())
                })?)
                .map_err(|e| Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
            buf.set_len(region.len as usize);
            local.push(buf);
        }
        if total == 0 {
            return Ok(local);
        }
        // The buffers are pool-allocated internally, so there is nothing
        // for the caller to recover on failure: flatten to a plain Error.
        Ok(self.remote_read(&[CopyOp::new(0, 0, total)], local).await?)
    }

    /// Executes a batch of writes from `local` into the client's write
    /// space and returns a [`SentBuffers`](crate::SentBuffers) witness of
    /// the completed transfer.
    ///
    /// Both sides are logical contiguous spaces: the concatenation of the
    /// `local` buffers' logical lengths (source) and the client's pinned
    /// write buffers (destination, see
    /// [`remote_write_space`](Self::remote_write_space)). Each [`CopyOp`]
    /// copies `len` bytes from `src_offset` (local space) to `dst_offset`
    /// (client space).
    ///
    /// The batch is validated before anything is transferred (bounds,
    /// overflow, op count, non-overlapping destination ranges; overlap
    /// across *separate* `remote_write` calls is the caller's
    /// responsibility). No one-sided RDMA WRITE is used: on RDMA the
    /// server sends a reverse `MemoryService::read_into_target` RPC
    /// advertising `local`
    /// as readable regions, and the *client* executes the RDMA READs into
    /// its pinned buffers — their lifetime is anchored client-side, which
    /// makes the transfer safe against client timeouts. On TCP the data
    /// travels inline via `MemoryService::write_inline`.
    ///
    /// The transfer happens *here*, inside the handler, so its latency and
    /// errors are directly observable. Pair the witness with a response
    /// value afterwards; several writes combine via
    /// [`SentBuffers::merge`](crate::SentBuffers::merge):
    ///
    /// ```rust,ignore
    /// let t0 = std::time::Instant::now();
    /// let sent = ctx.remote_write_all(bufs).await?;
    /// Ok(sent.reply(Stats { push_micros: t0.elapsed().as_micros() as u64 }))
    /// ```
    ///
    /// A batch moving zero bytes short-circuits without touching the
    /// network. On failure the local buffers are handed back inside
    /// [`RemoteIoError`] whenever they survived the operation.
    pub async fn remote_write(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<crate::SentBuffers, RemoteIoError> {
        if ops.iter().all(|op| op.len == 0) {
            return Ok(crate::SentBuffers::new(local));
        }
        let (socket, _) = match self.prepare_transfer(ops, &local, Direction::Write) {
            Ok(transfer) => transfer,
            Err(error) => return Err(RemoteIoError::new(error, Some(local))),
        };
        let buffers = socket.remote_write(self, ops, local).await?;
        Ok(crate::SentBuffers::new(buffers))
    }

    /// Writes the entire logical content of `local` to the beginning of
    /// the client's write space (a single 1:1 [`CopyOp`]).
    ///
    /// Zero total length (including an empty `local`) short-circuits
    /// without touching the network.
    pub async fn remote_write_all(
        &self,
        local: Vec<Buffer>,
    ) -> std::result::Result<crate::SentBuffers, RemoteIoError> {
        let total = match local_layout(&local) {
            Ok(layout) => layout.total(),
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if total == 0 {
            return Ok(crate::SentBuffers::new(local));
        }
        self.remote_write(&[CopyOp::new(0, 0, total)], local).await
    }

    /// All validation happens while the caller still owns `local`. Only a
    /// prepared transfer may move those buffers into transport execution.
    fn prepare_transfer(
        &self,
        ops: &[CopyOp],
        local: &[Buffer],
        direction: Direction,
    ) -> Result<(&Socket, RemoteSpace<'_>)> {
        let remote = match direction {
            Direction::Read => self.remote_read_space()?,
            Direction::Write => self.remote_write_space()?,
        };
        let local_len = local_layout(local)?.total();
        let (src_len, dst_len) = match direction {
            Direction::Read => (remote.total_len(), local_len),
            Direction::Write => (local_len, remote.total_len()),
        };
        scatter::validate_ops(ops, src_len, dst_len)?;
        let ContextEndpoint::Connected(socket) = &self.endpoint else {
            return Err(Error::new(
                crate::ErrorKind::NotConnected,
                "remote memory transfers require a connected handler context".into(),
            ));
        };
        Ok((socket, remote))
    }

    /// Produces an empty [`SentBuffers`](crate::SentBuffers) witness for
    /// handler paths that have nothing to transfer, fulfilling a
    /// `Result<WithBuffers<T>, E>` contract without touching the network.
    #[must_use]
    pub fn sent_nothing(&self) -> crate::SentBuffers {
        crate::SentBuffers::new(Vec::new())
    }
}

enum Direction {
    Read,
    Write,
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
