//! [`CompletionQueue`]: RAII wrapper for `ibv_cq`.

use std::{os::raw::c_int, ptr, sync::Arc};

use super::{comp_channel::CompChannel, context::Context};
use crate::{ErrorKind, Result, ibv_wc};

/// A completion queue (CQ).
///
/// Holds work completions from send/recv operations. Maintains shared
/// references to the [`Context`] and optional [`CompChannel`] to ensure
/// they outlive this CQ.
pub struct CompletionQueue {
    ptr: *mut crate::ibv_cq,
    /// Prevents the context from being closed while this CQ exists.
    _context: Arc<Context>,
    /// Prevents the completion channel from being destroyed while this CQ exists.
    _channel: Option<Arc<CompChannel>>,
}

impl CompletionQueue {
    /// Creates a new completion queue.
    ///
    /// # Arguments
    ///
    /// * `context` - The device context
    /// * `cq_size` - Minimum number of CQ entries
    /// * `channel` - Optional completion channel for event notifications
    pub fn create(
        context: &Arc<Context>,
        cq_size: c_int,
        channel: Option<&Arc<CompChannel>>,
    ) -> Result<Arc<Self>> {
        let channel_ptr = channel.map(|c| c.as_ptr()).unwrap_or(ptr::null_mut());
        let ptr = unsafe {
            crate::ibv_create_cq(
                context.as_ptr(),
                cq_size,
                ptr::null_mut(), // cq_context
                channel_ptr,
                0, // comp_vector
            )
        };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateCompQueueFail.with_errno());
        }
        Ok(Arc::new(Self {
            ptr,
            _context: Arc::clone(context),
            _channel: channel.cloned(),
        }))
    }

    /// Returns the raw CQ pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_cq {
        self.ptr
    }

    /// Requests notification for the next completion event.
    pub fn req_notify(&self, solicited_only: bool) -> Result<()> {
        let ret = unsafe {
            (*(*self.ptr).context).ops.req_notify_cq.unwrap_unchecked()(
                self.ptr,
                solicited_only as c_int,
            )
        };
        if ret != 0 {
            return Err(ErrorKind::IBReqNotifyCompQueueFail.with_errno());
        }
        Ok(())
    }

    /// Polls the completion queue for work completions.
    ///
    /// Returns the number of completions written to the `wc` slice.
    pub fn poll(&self, wc: &mut [ibv_wc]) -> Result<usize> {
        let ret = unsafe {
            (*(*self.ptr).context).ops.poll_cq.unwrap_unchecked()(
                self.ptr,
                wc.len() as c_int,
                wc.as_mut_ptr(),
            )
        };
        if ret < 0 {
            return Err(ErrorKind::IBPollCompQueueFail.with_errno());
        }
        Ok(ret as usize)
    }

    /// Acknowledges CQ events received via [`CompChannel::get_event`].
    pub fn ack_events(&self, nevents: u32) {
        unsafe { crate::ibv_ack_cq_events(self.ptr, nevents) };
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let _ = unsafe { crate::ibv_destroy_cq(self.ptr) };
    }
}

impl std::fmt::Debug for CompletionQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionQueue")
            .field("ptr", &self.ptr)
            .finish()
    }
}

unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;
    use crate::*;

    #[test]
    fn test_cq_create_without_channel() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        assert!(!cq.as_ptr().is_null());
    }

    #[test]
    fn test_cq_create_with_channel() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cc = CompChannel::create(&ctx).unwrap();
        let cq = CompletionQueue::create(&ctx, 16, Some(&cc)).unwrap();
        assert!(!cq.as_ptr().is_null());
    }

    #[test]
    fn test_cq_poll_empty() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        let mut wc = [ibv_wc::default(); 4];
        let n = cq.poll(&mut wc).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_cq_req_notify() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cc = CompChannel::create(&ctx).unwrap();
        let cq = CompletionQueue::create(&ctx, 16, Some(&cc)).unwrap();
        cq.req_notify(false).unwrap();
    }
}
