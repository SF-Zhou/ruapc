use crate::{CompChannel, Device, ErrorKind, Result, verbs};
use std::sync::Arc;

/// Raw completion queue wrapper with automatic cleanup.
///
/// This type wraps a raw InfiniBand completion queue pointer
/// and ensures proper cleanup on drop.
struct RawCompQueue(*mut verbs::ibv_cq);
impl std::ops::Deref for RawCompQueue {
    type Target = verbs::ibv_cq;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}
impl Drop for RawCompQueue {
    fn drop(&mut self) {
        let _ = unsafe { verbs::ibv_destroy_cq(self.0) };
    }
}
unsafe impl Send for RawCompQueue {}
unsafe impl Sync for RawCompQueue {}

/// Completion queue for RDMA work request completions.
///
/// A completion queue (CQ) is used to report the completion status
/// of work requests posted to send and receive queues. Applications
/// poll the CQ or use event notification to determine when operations
/// have completed.
pub struct CompQueue {
    comp_queue: RawCompQueue,
    /// Maximum number of completion queue entries.
    pub cqe: usize,
    /// Associated completion channel for event notification.
    pub(crate) comp_channel: CompChannel,
    _device: Arc<Device>,
}

impl CompQueue {
    /// Creates a new completion queue for the given device.
    ///
    /// # Arguments
    ///
    /// * `device` - The RDMA device to create the completion queue for
    /// * `max_cqe` - Maximum number of completion queue entries
    ///
    /// # Errors
    ///
    /// Returns an error if completion queue creation fails.
    pub fn create(device: &Arc<Device>, max_cqe: u32) -> Result<Self> {
        let comp_channel = CompChannel::create(device.clone())?;

        let ptr = unsafe {
            verbs::ibv_create_cq(
                device.context_ptr(),
                max_cqe as _,
                std::ptr::null_mut(),
                comp_channel.comp_channel_ptr(),
                0,
            )
        };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateCompQueueFail.with_errno());
        }
        let comp_queue = RawCompQueue(ptr);
        let cqe = comp_queue.cqe as usize;

        Ok(Self {
            comp_queue,
            cqe,
            comp_channel,
            _device: device.clone(),
        })
    }

    /// Returns the raw completion queue pointer.
    ///
    /// # Safety
    ///
    /// The returned pointer is only valid as long as this `CompQueue` exists.
    pub(crate) fn comp_queue_ptr(&self) -> *mut verbs::ibv_cq {
        self.comp_queue.0
    }

    /// Requests event notification for the next completion.
    ///
    /// After calling this, an event will be generated on the associated
    /// completion channel when the next completion is added to the queue.
    ///
    /// # Errors
    ///
    /// Returns an error if the notification request fails.
    pub fn req_notify(&self) -> Result<()> {
        let ret = unsafe { verbs::ibv_req_notify_cq(self.comp_queue_ptr(), 0) };
        if ret == 0 {
            Ok(())
        } else {
            Err(ErrorKind::IBReqNotifyCompQueueFail.with_errno())
        }
    }

    /// Polls the completion queue for work completions.
    ///
    /// Retrieves completed work requests from the completion queue without blocking.
    ///
    /// # Arguments
    ///
    /// * `wcs` - Buffer to store work completion entries
    ///
    /// # Returns
    ///
    /// Returns a slice of the input buffer containing the completed entries.
    ///
    /// # Errors
    ///
    /// Returns an error if polling fails.
    pub fn poll_cq<'a>(&self, wcs: &'a mut [verbs::ibv_wc]) -> Result<&'a mut [verbs::ibv_wc]> {
        let num = unsafe {
            verbs::ibv_poll_cq(
                self.comp_queue_ptr(),
                std::cmp::min(self.cqe, wcs.len()) as _,
                wcs.as_mut_ptr() as _,
            )
        };
        if num >= 0 {
            Ok(&mut wcs[..num as usize])
        } else {
            Err(ErrorKind::IBPollCompQueueFail.with_errno())
        }
    }

    /// Gets completion queue events from the completion channel.
    ///
    /// This is a non-blocking operation that returns 0 if no events
    /// are available.
    ///
    /// # Returns
    ///
    /// Returns the number of events retrieved (0 or 1).
    ///
    /// # Errors
    ///
    /// Returns an error if getting events fails (other than would-block).
    pub fn get_cq_events(&self) -> Result<usize> {
        let mut cq = std::ptr::null_mut();
        let mut cq_ctx = std::ptr::null_mut();
        let ret = unsafe {
            verbs::ibv_get_cq_event(self.comp_channel.comp_channel_ptr(), &mut cq, &mut cq_ctx)
        };
        if ret == 0 {
            Ok(1)
        } else if std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock {
            Ok(0)
        } else {
            Err(ErrorKind::IBGetCompQueueEventFail.with_errno())
        }
    }

    /// Acknowledges completion queue events.
    ///
    /// Must be called after `get_cq_events` to properly manage event counts.
    ///
    /// # Arguments
    ///
    /// * `nevents` - Number of events to acknowledge
    pub fn ack_cq_events(&self, nevents: usize) {
        unsafe { verbs::ibv_ack_cq_events(self.comp_queue_ptr(), nevents as u32) };
    }
}

impl std::fmt::Debug for CompQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompQueue").field("cqe", &self.cqe).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Devices;

    #[test]
    fn test_comp_queue() {
        let max_cqe = 1024;
        let device = Devices::availables().unwrap().first().unwrap().clone();
        let comp_queue = CompQueue::create(&device, max_cqe).unwrap();
        let mut wcs = [verbs::ibv_wc::default(); 16];
        let result = comp_queue.poll_cq(&mut wcs).unwrap();
        assert_eq!(result.len(), 0);
        println!("{comp_queue:?}");
    }
}
