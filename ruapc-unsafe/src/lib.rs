//! Explicit unsafe source validation and borrowed destinations for RuaPC callers.
//!
//! Import [`ClientBuffersExt`] and [`ContextBuffersExt`] to use real references.
//! The caller, rather than reference counting or access locks, keeps destination
//! allocations valid and exclusive through operation completion. Client sources
//! skip the borrowing future's validation reply under the caller's immutability
//! contract; CPU copies still run in that future. Safe source validation and
//! cancellation-safe owned destinations remain available in `ruapc`.

#![deny(unsafe_op_in_unsafe_fn)]
#![allow(async_fn_in_trait)]

use ruapc::{Buffer, Client, Context, CopyOp, OwnedBuffers, Result};
use ruapc_bufpool::{ReadSources, WriteBuffer};

/// Borrowed attachments whose generated RPC methods are available only inside
/// [`run`](BorrowedBuffers::run). Completing that scope releases its descriptors
/// before the original buffers can be used again.
#[doc(inline)]
pub use ruapc::BufferScope as BorrowedBuffers;

/// Advanced source validation and destination borrowing for client calls.
pub trait ClientBuffersExt {
    /// Borrows sources with direct request-ID validation and owns destinations.
    ///
    /// This has the same consuming `run` and `Completed` result as
    /// [`Client::with_buffers`]. It skips the source future's validation reply;
    /// stream copies still execute under that future's actual source borrow.
    ///
    /// # Safety
    /// Keep sources and their registrations alive and their bytes immutable
    /// until each started call finishes or is cancelled. Do not forget or leak
    /// any started, unfinished method future or its enclosing `run` future:
    /// that can leave its request ID pending after Rust releases the borrow.
    /// This applies to every typed or raw call in the scope, not only `run`.
    ///
    /// Ordinary drop, task abortion, cancelling timeouts and unwinding are
    /// allowed: they synchronously revoke the original request ID. Owned
    /// destinations still follow normal DMA retention and recovery rules.
    /// Source cancellation never waits for remote DMA. An unused builder or
    /// unpolled future has published nothing and may be dropped or forgotten.
    ///
    /// ```compile_fail,E0133
    /// use ruapc::{Buffer, Client};
    /// use ruapc_unsafe::ClientBuffersExt;
    /// fn without_contract(client: &Client, read: &[Buffer]) {
    ///     let _ = client.with_buffers_unchecked(read, Vec::new());
    /// }
    /// ```
    /// Sources still carry their ordinary reference lifetime:
    /// ```compile_fail
    /// use ruapc::{Buffer, Client, OwnedBuffers};
    /// use ruapc_unsafe::ClientBuffersExt;
    /// unsafe fn escape<'a>(client: &'static Client, read: &'a [Buffer])
    ///     -> OwnedBuffers<'static> {
    ///     unsafe { client.with_buffers_unchecked(read, Vec::new()) }
    /// }
    /// ```
    unsafe fn with_buffers_unchecked<'a>(
        &'a self,
        read: &'a [Buffer],
        write: Vec<Buffer>,
    ) -> OwnedBuffers<'a>;

    /// Borrows allocations for generated RPC methods inside [`BorrowedBuffers::run`].
    ///
    /// # Safety
    /// Keep destination buffers and their registrations alive at their original
    /// addresses and exclude all other access until `run` and every started
    /// method completes. With borrowed destinations, once polled, calls must be
    /// driven to completion, including error cleanup: do not drop, abort, forget,
    /// timeout, or unwind out of their futures. An unused builder or an unpolled
    /// `run` future can be dropped without starting work. Sources use direct
    /// request-ID validation under the same completion contract: keep their
    /// allocations and registrations alive and their bytes immutable. For
    /// cancellation-safe owned destinations with this source fast path, use
    /// [`Self::with_buffers_unchecked`]. For safe sources use [`Client::with_buffers`].
    /// Completing `run` settles local DMA and revokes old request access before
    /// releasing the borrowed descriptors. Source reuse after failure does not
    /// prove remote DMA completion.
    ///
    /// Calling this constructor requires an explicit unsafe boundary:
    /// ```compile_fail,E0133
    /// use ruapc::{Buffer, Client};
    /// use ruapc_unsafe::{BorrowedBuffers, ClientBuffersExt};
    /// fn without_contract<'a>(client: &'a Client, read: &'a [Buffer], write: &'a mut [Buffer])
    ///     -> BorrowedBuffers<'a> {
    ///     client.borrow_buffers(read, write)
    /// }
    /// ```
    /// The returned builder also retains the reference lifetime in its type:
    /// ```compile_fail
    /// use ruapc::{Buffer, Client};
    /// use ruapc_unsafe::{BorrowedBuffers, ClientBuffersExt};
    /// unsafe fn escape<'a>(client: &'static Client, read: &'a [Buffer], write: &'a mut [Buffer])
    ///     -> BorrowedBuffers<'static> {
    ///     unsafe { client.borrow_buffers(read, write) }
    /// }
    /// ```
    unsafe fn borrow_buffers<'a>(
        &'a self,
        read: &'a [Buffer],
        write: &'a mut [Buffer],
    ) -> BorrowedBuffers<'a>;
}

impl ClientBuffersExt for Client {
    unsafe fn with_buffers_unchecked<'a>(
        &'a self,
        read: &'a [Buffer],
        write: Vec<Buffer>,
    ) -> OwnedBuffers<'a> {
        // SAFETY: the caller retains immutable sources until every request is
        // completed or cancelled, and cannot forget a started call.
        let read = unsafe { ReadSources::caller_validated(read) };
        self.with_read_sources(read, write)
    }

    unsafe fn borrow_buffers<'a>(
        &'a self,
        read: &'a [Buffer],
        write: &'a mut [Buffer],
    ) -> BorrowedBuffers<'a> {
        // SAFETY: the caller keeps sources immutable and drives every started
        // call to completion; the destination contract excludes cancellation.
        let read = unsafe { ReadSources::caller_validated(read) };
        let write = unsafe { write_capabilities(write) };
        self.with_buffer_storage(read, write)
    }
}

// These snapshots carry no allocation lifetime. Every caller delegates their
// construction contract to its own unsafe reference API and completion scope.
unsafe fn write_capabilities(local: &mut [Buffer]) -> Vec<WriteBuffer> {
    local
        .iter_mut()
        .map(|b| unsafe { WriteBuffer::borrowed(b) })
        .collect()
}

/// Advanced borrowed destinations for the same remote-memory executor.
/// Source writes use the safe inherent methods on [`Context`].
pub trait ContextBuffersExt {
    /// Reads into the caller's original destinations.
    ///
    /// # Safety
    /// Once polled, drive the returned future to completion without dropping,
    /// forgetting, task abortion or unwinding. Keep destinations and registrations
    /// alive, fixed in memory and inaccessible to other CPU/DMA users until then.
    async unsafe fn remote_read(&self, ops: &[CopyOp], local: &mut [Buffer]) -> Result<()>;

    /// Reads the entire remote source space into supplied destinations.
    ///
    /// # Safety
    /// The same obligations as [`Self::remote_read`] apply.
    async unsafe fn remote_read_all(&self, local: &mut [Buffer]) -> Result<()>;
}

impl ContextBuffersExt for Context {
    async unsafe fn remote_read(&self, ops: &[CopyOp], local: &mut [Buffer]) -> Result<()> {
        // SAFETY: delegated caller obligations cover the complete awaited operation.
        let buffers = unsafe { write_capabilities(local) };
        self.read_buffer_storage(Some(ops), buffers).await.result
    }
    async unsafe fn remote_read_all(&self, local: &mut [Buffer]) -> Result<()> {
        // SAFETY: this method carries the same lifetime and exclusion obligations.
        let buffers = unsafe { write_capabilities(local) };
        self.read_buffer_storage(None, buffers).await.result
    }
}
