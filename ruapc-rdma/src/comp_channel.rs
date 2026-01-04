use crate::{Device, ErrorKind, Result, verbs};
use std::{os::fd::BorrowedFd, sync::Arc};

/// Raw completion channel wrapper with automatic cleanup.
///
/// This type wraps a raw InfiniBand completion channel pointer
/// and ensures proper cleanup on drop.
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

/// Completion channel for asynchronous RDMA event notification.
///
/// A completion channel allows applications to receive notifications
/// when completion queue events occur, enabling efficient event-driven
/// RDMA programming without polling.
///
/// The channel is automatically configured in non-blocking mode.
pub struct CompChannel {
    channel: RawCompChannel,
    _device: Arc<Device>,
}

impl CompChannel {
    /// Creates a new completion channel for the given device.
    ///
    /// The channel is automatically configured in non-blocking mode
    /// to allow for efficient asynchronous operations.
    ///
    /// # Arguments
    ///
    /// * `device` - The RDMA device to create the channel for
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Channel creation fails
    /// - Setting non-blocking mode fails
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

    /// Returns the raw completion channel pointer.
    ///
    /// # Safety
    ///
    /// The returned pointer is only valid as long as this `CompChannel` exists.
    pub fn comp_channel_ptr(&self) -> *mut verbs::ibv_comp_channel {
        self.channel.0
    }

    /// Returns a borrowed file descriptor for event notification.
    ///
    /// This file descriptor can be used with async runtimes like tokio
    /// to efficiently wait for completion events.
    ///
    /// # Returns
    ///
    /// Returns a borrowed file descriptor.
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
