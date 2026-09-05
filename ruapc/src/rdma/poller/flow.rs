//! Receive-ring and SEND credit accounting, owned only by the poll thread.
//!
//! A data SEND releases its window credit only after both local completion
//! and peer acknowledgment. One received WR consumes one credit, regardless of
//! the number of frames it carries. READs do not participate in this ledger.

use std::time::{Duration, Instant};

const ACK_KEEPALIVE: Duration = Duration::from_secs(5);
const STALL_DELAY: Duration = Duration::from_secs(2);
const ACK_COUNTER_MAX: u64 = u16::MAX as u64;

#[derive(Debug, Default)]
struct RecvStats {
    submitted: u64,
    completed: u64,
    ack_received: u64,
    ack_acked: u64,
    data_received: u64,
    data_acked: u64,
}

#[derive(Debug, Default)]
struct SendStats {
    data_completed: u64,
    data_confirmed: u64,
    ack_submitted: u64,
    ack_completed: u64,
    ack_confirmed: u64,
}

#[derive(Debug)]
pub(super) struct FlowControl {
    recv: RecvStats,
    send: SendStats,
    last_ack: Instant,
    /// Half the peer's send window keeps two ACK batches worth of headroom.
    /// The cadence must fit the two 16-bit counters in immediate data.
    ack_threshold: u64,
    /// Outstanding standalone ACKs use the non-window half of the receive ring.
    ack_limit: u64,
}

impl FlowControl {
    pub(super) fn new(send_window: u32, recv_submitted: u64, now: Instant) -> Self {
        Self {
            recv: RecvStats {
                submitted: recv_submitted,
                ..Default::default()
            },
            send: SendStats::default(),
            last_ack: now,
            ack_threshold: u64::from((send_window / 2).max(1)).min(ACK_COUNTER_MAX),
            ack_limit: u64::from(send_window.max(2)),
        }
    }

    pub(super) fn receive_completed(&mut self) {
        self.recv.completed += 1;
    }

    pub(super) fn receive_posted(&mut self) {
        self.recv.submitted += 1;
    }

    pub(super) fn received_data(&mut self) {
        self.recv.data_received += 1;
    }

    pub(super) fn received_ack(&mut self) {
        self.recv.ack_received += 1;
    }

    pub(super) fn data_completed(&mut self) {
        self.send.data_completed += 1;
    }

    pub(super) fn ack_completed(&mut self) {
        self.send.ack_completed += 1;
    }

    pub(super) fn peer_ack(&mut self, imm: u32) {
        self.send.data_confirmed += u64::from(imm & 0xFFFF);
        self.send.ack_confirmed += u64::from(imm >> 16);
    }

    /// Completed locally AND acknowledged by the peer; publishing only one
    /// side would reuse registered buffers or peer receive slots too early.
    pub(super) fn finished_data(&self) -> u64 {
        self.send.data_completed.min(self.send.data_confirmed)
    }

    pub(super) fn stalled(&self, now: Instant) -> bool {
        now.duration_since(self.last_ack) >= STALL_DELAY
    }

    pub(super) fn ack_starved(&self, now: Instant) -> bool {
        self.pending_data() >= self.ack_threshold && self.stalled(now)
    }

    fn pending_data(&self) -> u64 {
        self.recv.data_received - self.recv.data_acked
    }

    /// ACK deltas are encoded in two 16-bit immediate-data fields. A large
    /// window may need multiple ACKs; unencoded credits stay in the ledger.
    pub(super) fn due_ack(&self, now: Instant) -> Option<u32> {
        let data = self.pending_data();
        let acks = self.recv.ack_received - self.recv.ack_acked;
        if data >= self.ack_threshold
            || acks >= (self.ack_limit / 2).min(ACK_COUNTER_MAX)
            || now.duration_since(self.last_ack) >= ACK_KEEPALIVE
        {
            Some(((acks.min(ACK_COUNTER_MAX) as u32) << 16) | data.min(ACK_COUNTER_MAX) as u32)
        } else {
            None
        }
    }

    /// Commit only the credits successfully posted, including piggybacked ACKs.
    pub(super) fn mark_acked(&mut self, imm: u32, now: Instant) {
        self.recv.data_acked += u64::from(imm & 0xFFFF);
        self.recv.ack_acked += u64::from(imm >> 16);
        self.last_ack = now;
    }

    pub(super) fn can_submit_ack(&self) -> bool {
        let done = self.send.ack_completed.min(self.send.ack_confirmed);
        self.send.ack_submitted < done + self.ack_limit
    }

    /// A rejected post produces neither CQE nor peer ACK. Settle both sides
    /// immediately so it cannot keep teardown waiting for nonexistent work.
    pub(super) fn ack_posted(&mut self, success: bool) {
        self.send.ack_submitted += 1;
        if !success {
            self.send.ack_completed += 1;
            self.send.ack_confirmed += 1;
        }
    }

    /// Data sends are reclaimed separately after the QP has finished flushing:
    /// successful unsignaled SENDs do not have individual CQEs to count here.
    pub(super) fn flushed(&self) -> bool {
        self.send.ack_submitted == self.send.ack_completed
            && self.recv.submitted == self.recv.completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credits_wait_for_both_local_completion_and_peer_ack() {
        let mut flow = FlowControl::new(4, 0, Instant::now());
        flow.data_completed();
        assert_eq!(flow.finished_data(), 0);
        flow.peer_ack(2);
        assert_eq!(flow.finished_data(), 1);
        flow.data_completed();
        assert_eq!(flow.finished_data(), 2);
    }

    #[test]
    fn ack_cadence_scales_with_the_negotiated_window() {
        let now = Instant::now();
        for window in [1, 2, 4, 8, 32, 128] {
            let mut flow = FlowControl::new(window, 0, now);
            for _ in 0..(window / 2).max(1) - 1 {
                flow.received_data();
                assert_eq!(flow.due_ack(now), None);
            }
            flow.received_data();
            let ack = flow
                .due_ack(now)
                .expect("ACK must precede window exhaustion");
            flow.mark_acked(ack, now);
            assert_eq!(flow.due_ack(now), None);
            assert_eq!(flow.due_ack(now + ACK_KEEPALIVE), Some(0));
        }
    }

    #[test]
    fn standalone_acks_need_both_completion_and_confirmation_to_reuse_capacity() {
        let mut flow = FlowControl::new(2, 0, Instant::now());
        flow.ack_posted(true);
        flow.ack_posted(true);
        assert!(!flow.can_submit_ack());
        flow.ack_completed();
        assert!(!flow.can_submit_ack());
        flow.peer_ack(1 << 16);
        assert!(flow.can_submit_ack());
    }

    #[test]
    fn failed_ack_posts_and_flushed_receives_do_not_block_teardown() {
        let mut flow = FlowControl::new(4, 2, Instant::now());
        flow.ack_posted(false);
        flow.receive_completed();
        assert!(!flow.flushed());
        flow.receive_completed();
        assert!(flow.flushed());
    }

    #[test]
    fn large_ack_deltas_preserve_credits_across_multiple_wire_messages() {
        let now = Instant::now();
        let mut flow = FlowControl::new(u32::MAX, 0, now);
        flow.recv.data_received = ACK_COUNTER_MAX + 7;
        flow.recv.ack_received = ACK_COUNTER_MAX + 11;
        let first = flow.due_ack(now).unwrap();
        assert_eq!(first, u32::MAX);
        flow.mark_acked(first, now);
        let second = flow.due_ack(now + ACK_KEEPALIVE).unwrap();
        assert_eq!(second, (11 << 16) | 7);
        flow.mark_acked(second, now + ACK_KEEPALIVE);
        assert_eq!(flow.pending_data(), 0);
        assert_eq!(flow.recv.ack_received, flow.recv.ack_acked);
    }
}
