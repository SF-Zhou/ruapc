//! Minimal `poll(2)` wrapper for the RDMA poll thread.
//!
//! The dedicated completion poll thread sleeps on two file descriptors when
//! idle: the completion channel fd (CQ interrupts) and a wake pipe (pending
//! sends, connection registration, shutdown). This module wraps the `poll(2)`
//! syscall so higher-level crates can stay free of `unsafe` code.

use std::os::unix::io::RawFd;

use crate::{ErrorKind, Result};

/// Waits until one of the two file descriptors becomes readable.
///
/// Returns `(a_readable, b_readable)`. A negative `timeout_ms` blocks
/// indefinitely; `0` returns immediately. `EINTR` is reported as a timeout.
/// Error conditions on the descriptors (`POLLERR`/`POLLHUP`/`POLLNVAL`) are
/// reported as readable so callers observe the failure on the next read.
pub fn poll_readable2(fd_a: RawFd, fd_b: RawFd, timeout_ms: i32) -> Result<(bool, bool)> {
    const READABLE: libc::c_short = libc::POLLIN | libc::POLLERR | libc::POLLHUP | libc::POLLNVAL;

    let mut fds = [
        libc::pollfd {
            fd: fd_a,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: fd_b,
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let ret = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout_ms) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::Interrupted {
            return Ok((false, false));
        }
        return Err(ErrorKind::PollFdFail.with_errno());
    }
    Ok((
        fds[0].revents & READABLE != 0,
        fds[1].revents & READABLE != 0,
    ))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::os::unix::io::AsRawFd as _;

    use super::*;

    #[test]
    fn test_poll_timeout() {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        let (ra, rb) = poll_readable2(a.as_raw_fd(), b.as_raw_fd(), 10).unwrap();
        assert!(!ra);
        assert!(!rb);
    }

    #[test]
    fn test_poll_readable() {
        let (mut a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.write_all(&[1]).unwrap();
        let (ra, rb) = poll_readable2(a.as_raw_fd(), b.as_raw_fd(), 1000).unwrap();
        assert!(!ra);
        assert!(rb);
    }
}
