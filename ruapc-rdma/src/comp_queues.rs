use crate::{CompChannel, Device, ErrorKind, Result, verbs};
use std::sync::Arc;

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

pub struct CompQueue {
    comp_queue: RawCompQueue,
    pub cqe: usize,
    pub(crate) comp_channel: CompChannel,
    _device: Arc<Device>,
}

impl CompQueue {
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

    pub(crate) fn comp_queue_ptr(&self) -> *mut verbs::ibv_cq {
        self.comp_queue.0
    }

    pub fn req_notify(&self) -> Result<()> {
        let ret = unsafe { verbs::ibv_req_notify_cq(self.comp_queue_ptr(), 0) };
        if ret == 0 {
            Ok(())
        } else {
            Err(ErrorKind::IBReqNotifyCompQueueFail.with_errno())
        }
    }

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
