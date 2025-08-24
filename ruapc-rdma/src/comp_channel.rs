use crate::{Device, ErrorKind, Result, verbs};
use std::{os::fd::BorrowedFd, sync::Arc};

struct RawCompChannel(*mut verbs::ibv_comp_channel);
impl std::ops::Deref for RawCompChannel {
    type Target = verbs::ibv_comp_channel;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}
impl Drop for RawCompChannel {
    fn drop(&mut self) {
        let _ = unsafe { verbs::ibv_destroy_comp_channel(self.0) };
    }
}
unsafe impl Send for RawCompChannel {}
unsafe impl Sync for RawCompChannel {}

pub struct CompChannel {
    channel: RawCompChannel,
    _device: Arc<Device>,
}

impl CompChannel {
    pub fn create(device: Arc<Device>) -> Result<Self> {
        let ptr = unsafe { verbs::ibv_create_comp_channel(device.context_ptr()) };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateCompChannelFail.with_errno());
        }

        let this = Self {
            channel: RawCompChannel(ptr),
            _device: device,
        };

        // set fd non-blocking.
        let flags = unsafe { libc::fcntl(this.channel.fd, libc::F_GETFL, 0) };
        if flags == -1 {
            return Err(ErrorKind::IBSetNonBlockFailed.with_errno());
        }
        let ret = unsafe { libc::fcntl(this.channel.fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if ret == -1 {
            return Err(ErrorKind::IBSetNonBlockFailed.with_errno());
        }

        Ok(this)
    }

    pub fn comp_channel_ptr(&self) -> *mut verbs::ibv_comp_channel {
        self.channel.0
    }

    pub fn notify_fd<'a>(&'a self) -> BorrowedFd<'a> {
        unsafe { BorrowedFd::borrow_raw(self.channel.fd) }
    }
}

impl std::fmt::Debug for CompChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.channel.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Devices;

    #[test]
    fn test_create_comp_channel() {
        let devices = Devices::availables().unwrap();
        assert!(!devices.is_empty());
        for device in &devices {
            let comp_channel = CompChannel::create(device.clone()).unwrap();
            println!("{:?}", comp_channel);
        }
    }
}
