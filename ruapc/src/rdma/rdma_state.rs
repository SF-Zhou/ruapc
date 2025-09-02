use std::sync::atomic::{AtomicU64, Ordering};

const ERROR: u64 = 1 << 63;

/// An atomic index type that can track both a numeric value and an error state.
/// The most significant bit is used as an error flag, while the remaining bits
/// store the actual index value.
#[derive(Default)]
struct AtomicIndex(AtomicU64);

impl AtomicIndex {
    fn is_ok(&self) -> bool {
        TaskIndex(self.load()).is_ok()
    }

    fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    fn store(&self, val: u64) {
        self.0.store(val, Ordering::Release);
    }

    fn fetch_add(&self, cnt: u64) -> u64 {
        self.0.fetch_add(cnt, Ordering::AcqRel)
    }

    fn set_error(&self) -> u64 {
        self.0.fetch_or(ERROR, Ordering::AcqRel)
    }
}

impl std::fmt::Debug for AtomicIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bits = self.0.load(Ordering::Acquire);
        let is_ok = bits & ERROR == 0;
        let submitted = bits & !ERROR;
        write!(f, "{}({submitted})", if is_ok { "ok" } else { "error" })
    }
}

/// Represents a task index that can be in either a normal or error state.
/// The value is derived from `AtomicIndex` and maintains the same error bit convention.
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskIndex(u64);

impl TaskIndex {
    pub fn is_ok(self) -> bool {
        self.0 & ERROR == 0
    }
}

/// Manages the state of RDMA (Remote Direct Memory Access) operations.
/// This structure tracks send operations, enforces send limits, and handles error states.
///
/// The state maintains:
/// * A maximum send limit to control concurrent operations
/// * Tracking of submitted and finished send operations
/// * Error state management
#[derive(Debug)]
pub struct RdmaState {
    max_send_limit: u64,
    send_submitted: AtomicIndex,
    send_finished: AtomicU64,
    last_send_index: AtomicIndex,
}

impl RdmaState {
    /// Creates a new RDMA state with the specified maximum send limit.
    ///
    /// # Arguments
    /// * `max_send_limit` - The maximum number of concurrent send operations allowed
    pub fn new(max_send_limit: u32) -> Self {
        Self {
            max_send_limit: u64::from(max_send_limit),
            send_submitted: AtomicIndex::default(),
            send_finished: AtomicU64::default(),
            last_send_index: AtomicIndex::default(),
        }
    }

    /// Acquires a new send index for a task.
    /// Returns a `TaskIndex` that may be in error state if the system is in error state.
    pub fn apply_send_index(&self) -> TaskIndex {
        let bits = self.send_submitted.fetch_add(1);
        TaskIndex(bits)
    }

    /// Returns the upper bound of sendable operations.
    /// This is calculated as the number of finished operations plus the maximum send limit.
    pub fn sendable_bound(&self) -> u64 {
        self.send_finished.load(Ordering::Acquire) + self.max_send_limit
    }

    /// Checks if a task with the given index is ready to send.
    /// Returns true if the index is within the current sendable bound.
    ///
    /// # Arguments
    /// * `index` - The task index to check
    pub fn ready_to_send(&self, index: TaskIndex) -> bool {
        index.0 < self.sendable_bound()
    }

    /// Updates the count of finished send operations.
    /// This should only be called by the event loop.
    ///
    /// # Arguments
    /// * `index` - The index of the last completed send operation
    ///
    /// # Returns
    /// Returns the new sendable bound after the update
    pub fn update_send_finished(&self, index: u64) -> u64 {
        if self.send_finished.load(Ordering::Acquire) != index {
            self.send_finished.store(index, Ordering::Release);
        }
        index + self.max_send_limit
    }

    /// Sets the error state for this RDMA state.
    ///
    /// # Returns
    /// * `true` if this call was the first to set the error state
    /// * `false` if the error state was already set
    pub fn set_error(&self) -> bool {
        let bits = self.send_submitted.set_error();

        if bits & ERROR == 0 {
            // first to set error.
            self.last_send_index.store(bits | ERROR);
            true
        } else {
            false
        }
    }

    /// Checks if the RDMA state is in a normal (non-error) state.
    ///
    /// # Returns
    /// * `true` if the state is normal
    /// * `false` if in error state
    pub fn is_ok(&self) -> bool {
        self.send_submitted.is_ok()
    }

    /// Checks if the RDMA connection is ready to be removed based on completed sends.
    ///
    /// # Arguments
    /// * `send_completed` - The index of the last completed send operation
    ///
    /// # Returns
    /// * `true` if the connection can be safely removed
    /// * `false` if there are still pending operations
    pub fn ready_to_remove(&self, send_completed: u64) -> bool {
        send_completed | ERROR == self.last_send_index.load()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_normal() {
        let state = RdmaState::new(16);
        println!("{:#?}", state);

        for _ in 0..16 {
            let index = state.apply_send_index();
            assert!(index.is_ok());
            assert!(state.ready_to_send(index));
        }
        let index = state.apply_send_index();
        assert!(index.is_ok());
        assert!(!state.ready_to_send(index));

        state.update_send_finished(1);
        assert!(state.ready_to_send(index));
        assert!(state.is_ok());

        state.update_send_finished(1);
        state.set_error();
        assert!(!state.is_ok());
        assert!(!state.apply_send_index().is_ok());
        println!("{:#?}", state);

        assert!(!state.ready_to_remove(16));
        assert!(state.ready_to_remove(17));
    }
}
