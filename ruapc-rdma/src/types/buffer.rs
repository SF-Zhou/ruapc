//! RDMA buffer trait.
//!
//! Types implementing this trait can be passed directly to
//! [`QueuePair::send`](crate::QueuePair), [`QueuePair::recv`](crate::QueuePair),
//! and [`QueuePair::read`](crate::QueuePair).

/// A buffer registered for RDMA access.
///
/// Provides the registered address, length, local key, and remote key
/// needed for posting work requests. The implementing type owns the
/// backing memory and its associated [`MemoryRegion`](crate::MemoryRegion),
/// ensuring proper deregistration on drop.
pub trait RdmaBuffer: Send + 'static {
    /// Registered virtual address of the buffer.
    fn addr(&self) -> *mut std::ffi::c_void;

    /// Length of the buffer in bytes.
    fn len(&self) -> usize;

    /// Local key of the registered memory region.
    fn lkey(&self) -> u32;

    /// Remote key of the registered memory region.
    fn rkey(&self) -> u32;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
