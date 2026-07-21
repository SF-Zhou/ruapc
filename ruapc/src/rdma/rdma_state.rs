use std::sync::atomic::{AtomicU64, Ordering};

const ERROR: u64 = 1 << 63;

/// Result of trying to acquire one send credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendPermit {
    /// Credit acquired. `window_tail` is true when this acquisition
    /// consumed the last free credit: the posted work request must then be
    /// signaled, otherwise (with selective signaling) no completion sweeps
    /// the exhausted window and the connection stalls until an unrelated
    /// signaled WR (e.g. the 5s keepalive ACK) happens to do so.
    Granted { window_tail: bool },
    /// Send window exhausted; the message must queue as a pending send.
    Full,
    /// Connection is in the error state.
    Error,
}

/// Per-connection send window accounted in *work requests* (credits).
///
/// One credit corresponds to exactly one data WR — an aggregate send
/// carrying many messages still consumes a single credit — and, on the
/// peer, to one receive-ring buffer. This matches the resource the flow
/// control actually protects (the peer's posted receive WRs; running out
/// means RNR NAKs), so packing messages into fewer WRs directly frees
/// window capacity.
///
/// Credits are returned by the poll thread as
/// `min(data_completed, data_confirmed)`: the WR completed locally (its
/// buffer was reclaimed) *and* the peer acknowledged the matching receive
/// completion.
#[derive(Debug)]
pub struct RdmaState {
    /// Maximum number of in-flight data work requests.
    limit: u64,
    /// Credits acquired so far; the most significant bit is the error flag.
    submitted: AtomicU64,
    /// Credits returned so far (published by the poll thread).
    finished: AtomicU64,
}

impl RdmaState {
    /// Creates a new send window allowing `limit` in-flight data WRs.
    pub fn new(limit: u32) -> Self {
        Self {
            limit: u64::from(limit.max(1)),
            submitted: AtomicU64::new(0),
            finished: AtomicU64::new(0),
        }
    }

    /// Tries to acquire one send credit against the published window.
    pub fn try_acquire(&self) -> SendPermit {
        self.try_acquire_at(self.finished.load(Ordering::Acquire))
    }

    /// Tries to acquire one send credit against a caller-provided
    /// `finished` value.
    ///
    /// The poll thread flushes pending sends against its *not yet
    /// published* finished count: freshly freed credits are handed to the
    /// backlog before [`update_send_finished`](Self::update_send_finished)
    /// makes them visible to direct senders, so pending traffic cannot be
    /// starved by new sends racing for the same credits.
    pub fn try_acquire_at(&self, finished: u64) -> SendPermit {
        let bound = finished.saturating_add(self.limit);
        let mut bits = self.submitted.load(Ordering::Relaxed);
        loop {
            if bits & ERROR != 0 {
                return SendPermit::Error;
            }
            if bits >= bound {
                return SendPermit::Full;
            }
            match self.submitted.compare_exchange_weak(
                bits,
                bits + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return SendPermit::Granted {
                        window_tail: bits + 1 >= bound,
                    };
                }
                Err(current) => bits = current,
            }
        }
    }

    /// Publishes the count of returned credits.
    /// This should only be called by the poll thread.
    pub fn update_send_finished(&self, finished: u64) {
        if self.finished.load(Ordering::Acquire) != finished {
            self.finished.store(finished, Ordering::Release);
        }
    }

    /// Sets the error state.
    ///
    /// # Returns
    /// * `true` if this call was the first to set the error state
    /// * `false` if the error state was already set
    pub fn set_error(&self) -> bool {
        let bits = self.submitted.fetch_or(ERROR, Ordering::AcqRel);
        bits & ERROR == 0
    }

    /// Checks whether the connection is in a normal (non-error) state.
    pub fn is_ok(&self) -> bool {
        self.submitted.load(Ordering::Acquire) & ERROR == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credit_window() {
        let state = RdmaState::new(16);

        for i in 0..16 {
            match state.try_acquire() {
                SendPermit::Granted { window_tail } => {
                    assert_eq!(window_tail, i == 15, "tail flag at credit {i}");
                }
                other => panic!("expected credit {i} granted, got {other:?}"),
            }
        }
        assert_eq!(state.try_acquire(), SendPermit::Full);

        // Returning one credit frees exactly one acquisition.
        state.update_send_finished(1);
        assert_eq!(
            state.try_acquire(),
            SendPermit::Granted { window_tail: true }
        );
        assert_eq!(state.try_acquire(), SendPermit::Full);
        assert!(state.is_ok());
    }

    #[test]
    fn test_acquire_at_unpublished_bound() {
        let state = RdmaState::new(4);
        for _ in 0..4 {
            assert!(matches!(state.try_acquire(), SendPermit::Granted { .. }));
        }
        assert_eq!(state.try_acquire(), SendPermit::Full);

        // The poll thread can spend credits against a newer finished value
        // before publishing it; direct senders still see the window full.
        assert!(matches!(
            state.try_acquire_at(2),
            SendPermit::Granted { .. }
        ));
        assert_eq!(state.try_acquire(), SendPermit::Full);
        state.update_send_finished(2);
        // 5 acquired, bound is 2 + 4 = 6: one credit left.
        assert_eq!(
            state.try_acquire(),
            SendPermit::Granted { window_tail: true }
        );
        assert_eq!(state.try_acquire(), SendPermit::Full);
    }

    #[test]
    fn test_error_state() {
        let state = RdmaState::new(2);
        assert!(state.is_ok());
        assert!(state.set_error());
        assert!(!state.set_error());
        assert!(!state.is_ok());
        assert_eq!(state.try_acquire(), SendPermit::Error);
        assert_eq!(state.try_acquire_at(100), SendPermit::Error);
    }
}
