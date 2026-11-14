//! Poll-thread wakeups: a sleep-gated pipe plus a per-connection queue.

use std::{
    io::Write as _,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// Wake protocol shared by the poll thread and every waker.
///
/// Wakers publish `pending` and only write the pipe when the poll thread has
/// announced `sleeping`; the poll thread announces sleep and then rechecks
/// `pending`. Both sides use `SeqCst` (a Dekker handshake), so at least one
/// observes the other: either the poll thread skips `poll(2)`, or the waker
/// writes the pipe. A busy poll thread therefore costs wakers no syscall.
#[derive(Debug)]
pub(super) struct WakeShared {
    pipe: UnixStream,
    /// Set by every wake; consumed at the top of each poll-loop iteration.
    pub(super) pending: AtomicBool,
    /// Set by the poll thread around its `poll(2)` sleep.
    pub(super) sleeping: AtomicBool,
    /// Connections requesting maintenance. Each socket appears at most once
    /// until the poll thread clears its `maintenance_queued` flag.
    pub(super) conns: Mutex<Vec<u32>>,
}

/// Wakes the poll thread, optionally scheduling one connection's maintenance.
#[derive(Clone, Debug)]
pub struct PollerWaker(pub(super) Arc<WakeShared>);

impl PollerWaker {
    pub(super) fn new(pipe: UnixStream) -> Self {
        Self(Arc::new(WakeShared {
            pipe,
            pending: AtomicBool::new(false),
            sleeping: AtomicBool::new(false),
            conns: Mutex::new(Vec::new()),
        }))
    }

    /// Breaks an idle sleep so the poll thread observes inbox, budget or
    /// shutdown changes. Best-effort pipe write: a full pipe already wakes it.
    pub fn wake(&self) {
        self.0.pending.store(true, Ordering::SeqCst);
        if self.0.sleeping.load(Ordering::SeqCst) {
            let _ = (&self.0.pipe).write(&[1u8]);
        }
    }

    /// Schedules maintenance of the connection on `qp_num` (pending sends,
    /// activation, error teardown), then wakes the poll thread. `queued` is
    /// the socket's flag; the poll thread clears it before maintenance, so a
    /// change published before this call is observed by that maintenance.
    pub(crate) fn wake_conn(&self, qp_num: u32, queued: &AtomicBool) {
        if !queued.swap(true, Ordering::AcqRel) {
            self.0.conns.lock().unwrap().push(qp_num);
        }
        self.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;

    #[test]
    fn wakes_write_the_pipe_only_while_the_poll_thread_sleeps() {
        let (writer, mut reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        reader.set_nonblocking(true).unwrap();
        let waker = PollerWaker::new(writer);
        let shared = &waker.0;
        // A busy poll thread only needs the hint: no pipe byte is written.
        waker.wake();
        assert!(shared.pending.swap(false, Ordering::AcqRel));
        assert!(reader.read(&mut [0; 1]).is_err());
        // A sleeping poll thread is woken through the pipe.
        shared.sleeping.store(true, Ordering::SeqCst);
        waker.clone().wake();
        reader.read_exact(&mut [0; 1]).unwrap();
        assert!(shared.pending.swap(false, Ordering::AcqRel));
    }

    #[test]
    fn connection_wakes_queue_each_socket_once_until_cleared() {
        let (writer, _reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        let waker = PollerWaker::new(writer);
        let queued = AtomicBool::new(false);
        waker.wake_conn(7, &queued);
        waker.wake_conn(7, &queued);
        assert_eq!(*waker.0.conns.lock().unwrap(), [7]);
        assert!(waker.0.pending.load(Ordering::Acquire));
        // The poll thread clears the flag before maintenance; a later change
        // queues the connection again.
        waker.0.conns.lock().unwrap().clear();
        assert!(queued.swap(false, Ordering::AcqRel));
        waker.wake_conn(7, &queued);
        assert_eq!(*waker.0.conns.lock().unwrap(), [7]);
    }
}
