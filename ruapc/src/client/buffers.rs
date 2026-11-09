//! Borrowed immutable sources and owned destinations around one scoped executor.

use super::{Client, ClientWithBuffers};
use crate::Buffer;
use ruapc_bufpool::{ReadSources, WriteBuffer};

/// A call's unchanged application result and its recovered destination buffers.
/// Error bytes are not valid transfer results.
#[derive(Debug)]
#[must_use]
pub struct Completed<R, B = Vec<Buffer>> {
    /// The original application result, including custom error types.
    pub result: R,
    /// Original destinations after local settlement. `None` means the owning
    /// call exhausted its recovery grace; the runtime retains outstanding
    /// destinations and reclaims them after settlement.
    pub buffers: Option<B>,
}

impl<R, B> Completed<R, B> {
    /// Separates the application result and recovered destinations.
    pub fn into_parts(self) -> (R, Option<B>) {
        (self.result, self.buffers)
    }
}

/// Borrows immutable sources and owns destinations for a consuming `run` scope.
#[must_use]
pub struct OwnedBuffers<'a> {
    client: &'a Client,
    read: ReadSources<'a>,
    write: Vec<Buffer>,
}

/// Storage scope used by the separate unsafe destination-borrowing facade.
/// Only `run` exposes the temporary RPC execution handle.
#[doc(hidden)]
#[must_use]
pub struct BufferScope<'a> {
    attached: ClientWithBuffers<'a>,
}

impl BufferScope<'_> {
    /// Executes typed calls, then recovers destination storage before returning.
    /// Borrowed destination capabilities retain their unsafe completion contract.
    pub async fn run<R>(mut self, call: impl AsyncFnOnce(&ClientWithBuffers<'_>) -> R) -> R {
        let result = call(&self.attached).await;
        self.attached.recover().await;
        result
    }
}

impl Client {
    /// Established destination-capability boundary for the unsafe facade.
    /// Sources retain real references and their established validation policy.
    /// Only the borrowing future can perform CPU copies from them.
    /// Borrowed destinations still require their unsafe constructor's lifetime,
    /// exclusive access and completion obligations.
    #[doc(hidden)]
    pub fn with_buffer_storage<'a>(
        &'a self,
        read: ReadSources<'a>,
        write: Vec<WriteBuffer>,
    ) -> BufferScope<'a> {
        BufferScope {
            attached: ClientWithBuffers::new(self, read, write),
        }
    }

    /// Borrows immutable sources and owns destinations during a buffered call.
    /// The destination vector is returned separately from the service result.
    /// Cancelling or forgetting a started future is memory-safe: source commands
    /// can only be answered while it is polled, and posted destinations remain
    /// owned by the runtime through local DMA settlement.
    pub fn with_buffers<'a>(&'a self, read: &'a [Buffer], write: Vec<Buffer>) -> OwnedBuffers<'a> {
        self.with_read_sources(ReadSources::new(read), write)
    }

    /// Established source capability boundary for the unsafe facade.
    /// Destination ownership and recovery are identical to `with_buffers`.
    #[doc(hidden)]
    pub fn with_read_sources<'a>(
        &'a self,
        read: ReadSources<'a>,
        write: Vec<Buffer>,
    ) -> OwnedBuffers<'a> {
        OwnedBuffers {
            client: self,
            read,
            write,
        }
    }
}

impl OwnedBuffers<'_> {
    /// Runs typed calls and returns all original destinations when settled
    /// within the recovery grace. Otherwise `buffers` is `None` and reclamation
    /// continues in the background. That scope cannot issue further calls.
    /// Source handles remain in the caller throughout and are never returned.
    pub async fn run<R>(self, call: impl AsyncFnOnce(&ClientWithBuffers<'_>) -> R) -> Completed<R> {
        let attached = ClientWithBuffers::new_owned(self.client, self.read, self.write);
        let result = call(&attached).await;
        let buffers = attached.into_owned().await;
        Completed { result, buffers }
    }
}
