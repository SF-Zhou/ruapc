//! Source references and the authority for validating their published contents.

use super::Buffer;

/// Established source-validation capability for RPC integration.
///
/// This always holds real references. It neither snapshots raw pointers nor
/// adds ownership, reference counts or access state to the original buffers.
/// Safe construction requires the borrowing future to confirm source validity.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ReadSources<'a> {
    buffers: &'a [Buffer],
    confirm: bool,
}

impl<'a> ReadSources<'a> {
    /// Uses the borrowing future to confirm each published read result.
    pub fn new(buffers: &'a [Buffer]) -> Self {
        Self {
            buffers,
            confirm: true,
        }
    }

    /// Delegates source validity to the caller instead of the borrowing future.
    ///
    /// # Safety
    /// Keep these allocations, registrations and contents valid and immutable
    /// while any request publishing them remains pending. Every started call
    /// must either finish or be cancelled through a path that revokes its
    /// request ID before the sources are modified or released. This includes
    /// calls made through copies of this capability.
    ///
    /// Forgetting or leaking a started call does not revoke its request ID.
    /// Do not use the end of its Rust borrow as evidence of revocation. An
    /// unused capability or an unpolled call has published nothing and can be
    /// dropped or forgotten. This capability grants no independent CPU access;
    /// synchronous source copies must still execute under the actual borrow.
    ///
    /// ```compile_fail,E0133
    /// use ruapc_bufpool::{Buffer, ReadSources};
    /// fn bypass(buffers: &[Buffer]) {
    ///     let _ = ReadSources::caller_validated(buffers);
    /// }
    /// ```
    pub unsafe fn caller_validated(buffers: &'a [Buffer]) -> Self {
        Self {
            buffers,
            confirm: false,
        }
    }

    /// The original buffers, with their ordinary Rust borrow lifetime.
    pub fn buffers(self) -> &'a [Buffer] {
        self.buffers
    }

    /// Whether positive source validation needs the borrowing future's reply.
    pub fn requires_confirmation(self) -> bool {
        self.confirm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferPoolBuilder;

    #[test]
    fn validation_modes_keep_the_same_references() {
        let pool = BufferPoolBuilder::new(Vec::new()).build();
        let buffers = [pool.allocate(4).unwrap()];
        let safe = ReadSources::new(&buffers);
        // SAFETY: nothing is published and the originals remain immutable.
        let unchecked = unsafe { ReadSources::caller_validated(&buffers) };
        assert!(safe.requires_confirmation());
        assert!(!unchecked.requires_confirmation());
        assert_eq!(safe.buffers().as_ptr(), buffers.as_ptr());
        assert_eq!(unchecked.buffers().as_ptr(), buffers.as_ptr());
    }
}
