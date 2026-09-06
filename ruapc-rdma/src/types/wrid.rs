//! CQ-specific work request identities with two fixed work-type bits.
//!
//! ```text
//! | type: 2 bits | CQ route slot: s bits | sequence: 62 - s bits |
//! ```
//!
//! Each CQ fixes `s` from its actual capacity at creation. A WRID therefore
//! cannot decode its own slot or sequence: use the originating CQ's
//! [`crate::Completion::slot`] and [`crate::Completion::sequence`] instead.
//! CQ-owned route leases preserve sequence watermarks across QP lifetimes.

/// A work request identity. Only its type is independent of its originating CQ.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WRID(u64);

/// Work request type encoded in a [`WRID`].
///
/// The type remains available even for error CQEs, whose provider opcode need
/// not be valid. SEND variants and READ share one SQ sequence stream.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WRType {
    /// Receive work request
    Recv = 0,
    /// Send work request
    SendData = 1,
    /// Send-with-immediate work request
    SendImm = 2,
    /// RDMA read work request
    Read = 3,
}

impl WRID {
    /// Bit position of the type field, independent of CQ layout.
    pub const TYPE_SHIFT: u32 = 62;
    pub(crate) const PAYLOAD_MASK: u64 = (1 << Self::TYPE_SHIFT) - 1;

    /// Called with the prefix and sequence validated by the CQ route allocator.
    #[inline]
    pub(crate) fn new(wr_type: WRType, payload: u64) -> Self {
        debug_assert!(payload <= Self::PAYLOAD_MASK);
        Self(((wr_type as u64) << Self::TYPE_SHIFT) | payload)
    }

    /// Returns the type of the work request.
    #[inline]
    pub fn get_type(&self) -> WRType {
        match self.0 >> Self::TYPE_SHIFT {
            0 => WRType::Recv,
            1 => WRType::SendData,
            2 => WRType::SendImm,
            3 => WRType::Read,
            _ => unreachable!(),
        }
    }

    /// Returns the raw underlying `u64` value.
    #[inline]
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for WRID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Slot/sequence formatting would require the originating CQ's layout.
        write!(f, "{:?}({:#018x})", self.get_type(), self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_and_payload_roundtrip_without_assuming_a_cq_layout() {
        for wr_type in [
            WRType::Recv,
            WRType::SendData,
            WRType::SendImm,
            WRType::Read,
        ] {
            for payload in [0, 1, WRID::PAYLOAD_MASK] {
                let wrid = WRID::new(wr_type, payload);
                assert_eq!(wrid.get_type(), wr_type);
                assert_eq!(wrid.raw() & WRID::PAYLOAD_MASK, payload);
                assert_eq!(wrid.raw(), ((wr_type as u64) << WRID::TYPE_SHIFT) | payload);
            }
        }
    }

    #[test]
    fn debug_prints_raw_identity_without_guessing_the_layout() {
        assert_eq!(
            format!("{:?}", WRID::new(WRType::Recv, 123)),
            "Recv(0x000000000000007b)"
        );
    }
}
