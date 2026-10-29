//! Request-attached memory spaces and transfer validation.

use super::{BufferAvailability, WriteReceipt, transfer};
use crate::{Buffer, Context, CopyOp, Error, ErrorKind, RemoteIoError, RemoteSpace, Result};

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
                ErrorKind::MissingBufferInfo,
                "request carries no read regions; client must attach buffers \
                 via with_read_buffers()"
                    .into(),
            ));
        }
        Ok(RemoteSpace::new(&self.msg_meta.read_regions)?)
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
                ErrorKind::MissingBufferInfo,
                "request carries no write regions; client must attach buffers \
                 via with_write_buffers()"
                    .into(),
            ));
        }
        Ok(RemoteSpace::new(&self.msg_meta.write_regions)?)
    }

    /// Executes a batch of reads from the client's read space into `local`.
    ///
    /// Both sides are logical contiguous spaces: the client's attached
    /// read buffers (source, see [`remote_read_space`](Self::remote_read_space))
    /// and the concatenation of the `local` buffers' logical lengths
    /// (destination). Each [`CopyOp`] copies `len` bytes from
    /// `src_offset` (client space) to `dst_offset` (local space).
    ///
    /// All transfers validate the connection, attached space, bounds, overflow,
    /// operation count and destination overlap. An empty or zero-byte batch
    /// returns `local` after validation without I/O.
    /// RDMA uses one-sided READs; TCP/WS/HTTP use a reverse inline-copy RPC.
    ///
    /// Returns the same buffers with the requested ranges filled. Failures
    /// return recoverable buffers in [`RemoteIoError`]; in-flight RDMA can
    /// retain them beyond the call. Converting the error with `?` to [`Error`]
    /// drops any returned buffers.
    ///
    /// For transfers that copy bytes, RDMA validates the completed READs with
    /// an original-request waiter membership probe. With the original immutable
    /// source still held and no request-ID reuse, a positive probe validates
    /// the copy. It is not a precise deadline check.
    ///
    /// Source recovery after cancellation does not wait for remote DMA.
    /// Failed validation can return readable destinations in [`RemoteIoError`];
    /// those bytes are not a valid transfer result. See the
    /// [source contract](https://github.com/SF-Zhou/ruapc/blob/main/docs/safe-boundaries.md#application-read-source-validation-and-recovery)
    /// for the success argument and failure behavior.
    pub async fn remote_read(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        transfer::read(self, ops, local).await
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
        transfer::read_all(self).await
    }

    /// Executes a batch of writes from `local` into the client's write
    /// space and returns a [`WriteReceipt`](crate::WriteReceipt) witness of
    /// the completed transfer.
    ///
    /// Both sides are logical contiguous spaces: the concatenation of the
    /// `local` buffers' logical lengths (source) and the client's pinned
    /// write buffers (destination, see
    /// [`remote_write_space`](Self::remote_write_space)). Each [`CopyOp`]
    /// copies `len` bytes from `src_offset` (local space) to `dst_offset`
    /// (client space).
    ///
    /// All transfers validate the connection, attached space, bounds, overflow, operation count, and
    /// destination overlap within the batch. Callers coordinate writes across
    /// separate calls. RDMA uses a reverse RPC: the client READs `local` into
    /// owned destinations and replies after completion. The source is retained
    /// while awaiting that response; this path does not use the pending probe
    /// described in [`remote_read`](Self::remote_read). Failure or cancellation
    /// can still recover the source before remote DMA finishes. TCP/WS/HTTP
    /// instead send the bytes in an inline-copy RPC.
    ///
    /// The transfer happens *here*, inside the handler, so its latency and
    /// errors are directly observable. Pair the witness with a response
    /// value afterwards:
    ///
    /// ```rust,ignore
    /// let t0 = std::time::Instant::now();
    /// let sent = ctx.remote_write_all(bufs).await?;
    /// Ok(sent.into_reply(Stats { push_micros: t0.elapsed().as_micros() as u64 }))
    /// ```
    ///
    /// An empty or zero-byte batch returns a receipt after validation without I/O.
    /// A successful receipt records completion even if its source buffers cannot
    /// currently be recovered. Failures return recoverable local buffers in
    /// [`RemoteIoError`].
    pub async fn remote_write(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<WriteReceipt, RemoteIoError> {
        transfer::write(self, ops, local).await
    }

    /// Writes the entire logical content of `local` to the beginning of
    /// the client's write space (a single 1:1 [`CopyOp`]).
    ///
    /// Zero total length (including an empty `local`) skips I/O after validating
    /// the connection and both memory spaces.
    pub async fn remote_write_all(
        &self,
        local: Vec<Buffer>,
    ) -> std::result::Result<WriteReceipt, RemoteIoError> {
        transfer::write_all(self, local).await
    }

    /// Produces an empty [`WriteReceipt`](crate::WriteReceipt) witness for
    /// handler paths that have nothing to transfer, fulfilling a
    /// `Result<BufferReply<T>, E>` contract without touching the network.
    #[must_use]
    pub fn sent_nothing(&self) -> WriteReceipt {
        WriteReceipt::new(BufferAvailability::Available(Vec::new()))
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
