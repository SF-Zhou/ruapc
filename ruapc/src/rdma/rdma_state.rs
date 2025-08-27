use std::sync::atomic::{AtomicU64, Ordering};

const ERROR: u64 = 1 << 63;

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

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskIndex(u64);

impl TaskIndex {
    pub fn is_ok(self) -> bool {
        self.0 & ERROR == 0
    }
}

#[derive(Debug)]
pub struct RdmaState {
    max_send_limit: u64,
    send_submitted: AtomicIndex,
    send_finished: AtomicU64,
    last_send_index: AtomicIndex,
}

impl RdmaState {
    pub fn new(max_send_limit: u32) -> Self {
        Self {
            max_send_limit: u64::from(max_send_limit),
            send_submitted: AtomicIndex::default(),
            send_finished: AtomicU64::default(),
            last_send_index: AtomicIndex::default(),
        }
    }

    pub fn apply_send_index(&self) -> TaskIndex {
        let bits = self.send_submitted.fetch_add(1);
        TaskIndex(bits)
    }

    pub fn sendable_bound(&self) -> u64 {
        self.send_finished.load(Ordering::Acquire) + self.max_send_limit
    }

    pub fn ready_to_send(&self, index: TaskIndex) -> bool {
        index.0 < self.sendable_bound()
    }

    /// only update by event loop.
    pub fn update_send_finished(&self, index: u64) -> u64 {
        if self.send_finished.load(Ordering::Acquire) != index {
            self.send_finished.store(index, Ordering::Release);
        }
        index + self.max_send_limit
    }

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

    pub fn is_ok(&self) -> bool {
        self.send_submitted.is_ok()
    }

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
