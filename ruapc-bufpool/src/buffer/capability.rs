//! Owned destinations and explicitly unsafe, untracked borrowed destinations.

use std::{
    io::{Error, ErrorKind, Result},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use super::Buffer;
use crate::{MemoryKey, RemoteBufferInfo, buddy::BuddyBlock};

/// A snapshot contains no ownership, lifetime tracking or access counter.
/// Its unsafe constructor's caller retains the original allocation and pool.
#[derive(Debug)]
struct BorrowedBuffer {
    ptr: NonNull<u8>,
    block: NonNull<BuddyBlock>,
    len: usize,
    capacity: usize,
}

// SAFETY: unsafe borrowed constructors require the caller to keep the allocation
// and registrations alive, and uphold the capability's access permission on
// every thread until all local accesses have finished. The snapshot itself is
// immutable; byte access is exposed only by the enclosing WriteBuffer.
unsafe impl Send for BorrowedBuffer {}
// SAFETY: shared access through the wrapper is immutable. Mutable access is
// available only through an exclusive borrow of WriteBuffer, and its constructor
// requires exclusion of all accesses through the original handle or other views.
unsafe impl Sync for BorrowedBuffer {}

impl BorrowedBuffer {
    fn snapshot(buffer: &Buffer) -> Self {
        Self {
            ptr: buffer.ptr,
            block: buffer.block,
            len: buffer.len(),
            capacity: buffer.capacity(),
        }
    }

    fn memory_key(&self, registrar_index: usize) -> Result<MemoryKey> {
        if self.capacity == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "memory not registered: empty buffer has no backing memory",
            ));
        }
        // SAFETY: the constructor's caller retains the allocation and its pool.
        // Registration metadata is immutable for that pool's lifetime. Borrow
        // only this field while allocator metadata changes elsewhere.
        unsafe { &(*self.block.as_ptr()).registrations }
            .get(registrar_index)
            .map(|registration| registration.memory_key())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("memory not registered at registrar index {registrar_index}"),
                )
            })
    }
}

#[derive(Debug)]
enum Storage {
    Owned(Buffer),
    Borrowed(BorrowedBuffer),
}

impl Storage {
    fn len(&self) -> usize {
        match self {
            Self::Owned(buffer) => buffer.len(),
            Self::Borrowed(buffer) => buffer.len,
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Owned(buffer) => buffer.capacity(),
            Self::Borrowed(buffer) => buffer.capacity,
        }
    }

    fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Owned(buffer) => buffer.as_ptr(),
            Self::Borrowed(buffer) => buffer.ptr.as_ptr(),
        }
    }

    fn memory_key(&self, registrar_index: usize) -> Result<MemoryKey> {
        match self {
            Self::Owned(buffer) => buffer.memory_key(registrar_index),
            Self::Borrowed(buffer) => buffer.memory_key(registrar_index),
        }
    }
}

/// Exclusive access to an owned buffer or an explicitly unsafe borrowed view.
///
/// Safe construction via `From<Buffer>` transfers real ownership. The borrowed
/// form stores only metadata and relies on the unsafe constructor's caller for
/// lifetime and exclusivity. Dropping a borrowed view does not free memory or
/// establish that device work has stopped.
#[derive(Debug)]
pub struct WriteBuffer(Storage);

impl WriteBuffer {
    /// Creates an untracked exclusive snapshot for low-level integration.
    ///
    /// # Safety
    ///
    /// The caller must keep the original allocation, initialized byte range and
    /// registrations alive and unchanged until this view and all local copies
    /// or posted device operations using it have finished. Throughout that time,
    /// no access to its bytes is permitted through the original handle or any
    /// other alias. Access through this capability must obey normal shared and
    /// exclusive borrowing rules, including while raw pointers are in use.
    ///
    /// The reference passed here does not encode this obligation. Dropping,
    /// cancelling or forgetting a future, cursor, capability or result receiver
    /// does not prove device completion and must not permit reuse or reclamation.
    /// Retain the original until all posted operations settle or successful
    /// provider destruction proves they can no longer access the allocation.
    ///
    /// ```compile_fail,E0133
    /// # use ruapc_bufpool::{Buffer, WriteBuffer};
    /// fn untracked(buffer: &mut Buffer) {
    ///     let _view = WriteBuffer::borrowed(buffer);
    /// }
    /// ```
    #[doc(hidden)]
    pub unsafe fn borrowed(buffer: &mut Buffer) -> Self {
        Self(Storage::Borrowed(BorrowedBuffer::snapshot(buffer)))
    }

    /// Returns the allocation address. Raw-pointer users must honor this
    /// capability's exclusive access and lifetime requirements.
    #[must_use]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_ptr().cast_mut()
    }

    /// Borrows the initialized bytes exclusively.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: Owned storage exclusively owns the allocation. Borrowed
        // storage's constructor requires the same exclusivity for its use.
        // The unique wrapper borrow excludes simultaneous wrapper slices.
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    /// The logical length, captured when a borrowed view was constructed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The original allocation capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Whether the logical space is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The allocation address. A raw pointer grants no independent
    /// lifetime, access permission or completion evidence.
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    /// Borrows initialized bytes under this capability's permission.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: owned storage is alive and initialized; the unsafe
        // borrowed constructor imposes those same requirements on its
        // caller. Shared borrowing excludes this wrapper's mutation.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Returns the registration key for a pool-local registrar index.
    pub fn memory_key(&self, registrar_index: usize) -> Result<MemoryKey> {
        self.0.memory_key(registrar_index)
    }

    /// Exports registered metadata with this capability's logical length.
    pub fn remote_buffer_info(&self, registrar_index: usize) -> Result<RemoteBufferInfo> {
        Ok(RemoteBufferInfo {
            key: self.memory_key(registrar_index)?,
            addr: self.as_ptr() as u64,
            len: self.len() as u64,
        })
    }

    /// Recovers an owned buffer. A borrowed view is returned unchanged
    /// as `Err`; this never creates ownership from raw metadata.
    pub fn into_owned(self) -> std::result::Result<Buffer, Self> {
        match self.0 {
            Storage::Owned(buffer) => Ok(buffer),
            storage @ Storage::Borrowed(_) => Err(Self(storage)),
        }
    }
}

impl From<Buffer> for WriteBuffer {
    fn from(buffer: Buffer) -> Self {
        Self(Storage::Owned(buffer))
    }
}

impl Deref for WriteBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for WriteBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl DerefMut for WriteBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl AsMut<[u8]> for WriteBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferPoolBuilder;
    use std::sync::Arc;

    #[test]
    fn owned_destination_roundtrips_the_exact_buffer() {
        let pool = BufferPoolBuilder::new(Vec::new()).build();
        let mut buffer = pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        let original = (buffer.as_ptr(), buffer.len(), buffer.capacity());
        let mut write = WriteBuffer::from(buffer);
        assert_eq!(write.as_ref(), b"read");
        write.as_mut().copy_from_slice(b"done");
        let buffer = write.into_owned().unwrap();
        assert_eq!((buffer.as_ptr(), buffer.len(), buffer.capacity()), original);
        assert_eq!(buffer.as_slice(), b"done");
    }

    #[test]
    fn borrowed_destination_preserves_pointer_and_does_not_retain_or_release_the_pool() {
        let pool = BufferPoolBuilder::new(Vec::new())
            .thread_cache(false)
            .build();
        let mut buffer = pool.allocate(4).unwrap();
        buffer.set_len(4);
        buffer.copy_from_slice(b"read");
        let original = (buffer.as_ptr(), buffer.len(), buffer.capacity());
        let references = Arc::strong_count(&pool);
        let free = pool.slab_free_counts();
        {
            // SAFETY: all byte access uses this sole view, which is dropped
            // before the original handle is accessed or released again.
            let mut write = unsafe { WriteBuffer::borrowed(&mut buffer) };
            assert_eq!(Arc::strong_count(&pool), references);
            assert_eq!((write.as_ptr(), write.len(), write.capacity()), original);
            assert_eq!(write.as_slice(), b"read");
            write.copy_from_slice(b"done");
            let write = write.into_owned().unwrap_err();
            assert_eq!(write.as_slice(), b"done");
        }
        assert_eq!(pool.slab_free_counts(), free);
        assert_eq!(Arc::strong_count(&pool), references);
        assert_eq!(buffer.as_slice(), b"done");
        assert_eq!((buffer.as_ptr(), buffer.len(), buffer.capacity()), original);
    }

    #[test]
    fn borrowed_write_can_finish_on_another_thread_with_the_original_alive() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<WriteBuffer>();
        let pool = BufferPoolBuilder::new(Vec::new()).build();
        let mut buffer = pool.allocate(4).unwrap();
        buffer.set_len(4);
        // SAFETY: the original and pool remain alive, and the scoped worker
        // finishes and drops the view before any subsequent original access.
        let mut view = unsafe { WriteBuffer::borrowed(&mut buffer) };
        std::thread::scope(|scope| {
            scope.spawn(move || view.copy_from_slice(b"done"));
        });
        assert_eq!(buffer.as_slice(), b"done");
    }

    #[test]
    fn borrowed_metadata_preserves_registration_order_and_logical_length() {
        use crate::{AlignedMemory, MemoryRegistrar, Registration};

        #[derive(Debug)]
        struct Keys(u32);
        impl Registration for Keys {
            fn memory_key(&self) -> MemoryKey {
                MemoryKey {
                    lkey: self.0,
                    rkey: self.0 + 1,
                }
            }
        }
        // SAFETY: this registrar produces constant keys without retaining or
        // accessing any memory. No actual device operation uses these keys.
        unsafe impl MemoryRegistrar for Keys {
            fn register(&self, _: &Arc<AlignedMemory>) -> Result<Box<dyn Registration>> {
                Ok(Box::new(Keys(self.0)))
            }
        }

        let pool = BufferPoolBuilder::new(vec![Arc::new(Keys(10)), Arc::new(Keys(20))]).build();
        let mut buffer = pool.allocate(17).unwrap();
        buffer.set_len(17);
        let expected = [
            buffer.remote_buffer_info(0).unwrap(),
            buffer.remote_buffer_info(1).unwrap(),
        ];
        // SAFETY: the original and its registrations outlive this sole view.
        let write = unsafe { WriteBuffer::borrowed(&mut buffer) };
        for (index, metadata) in expected.iter().enumerate() {
            assert_eq!(&write.remote_buffer_info(index).unwrap(), metadata);
            assert_eq!(metadata.len, 17);
        }
        assert!(write.memory_key(2).is_err());
    }

    #[test]
    fn empty_borrowed_views_have_no_registration_or_ownership() {
        let pool = BufferPoolBuilder::new(Vec::new()).build();
        let mut buffer = Buffer::empty(&pool);
        // SAFETY: this view is the only byte accessor and outlived by buffer.
        let mut write = unsafe { WriteBuffer::borrowed(&mut buffer) };
        assert!(write.is_empty());
        assert_eq!(write.capacity(), 0);
        assert!(write.memory_key(0).is_err());
        assert!(write.remote_buffer_info(0).is_err());
        assert!(write.as_mut_slice().is_empty());
        assert!(write.into_owned().is_err());
        assert!(buffer.is_empty());
    }
}
