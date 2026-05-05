//! [`CompChannel`]: RAII wrapper for `ibv_comp_channel`.

use std::{ptr, sync::Arc};

use super::context::Context;
use crate::{ErrorKind, Result};

/// A completion channel for event-driven CQ notifications.
///
/// Holds a shared reference to the [`Context`] to guarantee the device
/// context remains valid.
pub struct CompChannel {
    ptr: *mut crate::ibv_comp_channel,
    /// Prevents the context from being closed while this channel exists.
    _context: Arc<Context>,
}

impl CompChannel {
    /// Creates a new completion channel on the given context.
    pub fn create(context: &Arc<Context>) -> Result<Arc<Self>> {
        let ptr = unsafe { crate::ibv_create_comp_channel(context.as_ptr()) };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateCompChannelFail.with_errno());
        }
        Ok(Arc::new(Self {
            ptr,
            _context: Arc::clone(context),
        }))
    }

    /// Returns the raw completion channel pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_comp_channel {
        self.ptr
    }

    /// Sets the completion channel to non-blocking mode.
    pub fn set_nonblock(&self) -> Result<()> {
        let fd = unsafe { (*self.ptr).fd };
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(ErrorKind::IBSetNonBlockFail.with_errno());
        }
        let ret = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret < 0 {
            return Err(ErrorKind::IBSetNonBlockFail.with_errno());
        }
        Ok(())
    }

    /// Returns the file descriptor for async event notification.
    ///
    /// This can be used with `AsyncFd` (tokio) to poll for completion events
    /// without busy-waiting.
    pub fn fd(&self) -> std::os::unix::io::BorrowedFd<'_> {
        let fd = unsafe { (*self.ptr).fd };
        unsafe { std::os::unix::io::BorrowedFd::borrow_raw(fd) }
    }

    /// Waits for a completion event on this channel.
    ///
    /// Returns the raw CQ pointer that received the event. The caller must
    /// acknowledge the event via [`super::CompletionQueue::ack_events`].
    pub fn get_event(&self) -> Result<*mut crate::ibv_cq> {
        let mut cq_ptr: *mut crate::ibv_cq = ptr::null_mut();
        let mut cq_context: *mut std::ffi::c_void = ptr::null_mut();
        let ret = unsafe { crate::ibv_get_cq_event(self.ptr, &mut cq_ptr, &mut cq_context) };
        if ret != 0 {
            return Err(ErrorKind::IBGetCompQueueEventFail.with_errno());
        }
        Ok(cq_ptr)
    }
}

impl Drop for CompChannel {
    fn drop(&mut self) {
        let _ = unsafe { crate::ibv_destroy_comp_channel(self.ptr) };
    }
}

impl std::fmt::Debug for CompChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompChannel")
            .field("ptr", &self.ptr)
            .finish()
    }
}

unsafe impl Send for CompChannel {}
unsafe impl Sync for CompChannel {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;
    use crate::*;

    #[test]
    fn test_comp_channel_create() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cc = CompChannel::create(&ctx).unwrap();
        assert!(!cc.as_ptr().is_null());
    }

    #[test]
    fn test_comp_channel_set_nonblock() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cc = CompChannel::create(&ctx).unwrap();
        cc.set_nonblock().unwrap();
    }
}
