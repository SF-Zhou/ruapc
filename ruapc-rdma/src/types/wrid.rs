//! Fixed-width work request identities, scoped by the CQ and hardware QPN.
//!
//! ```text
//! | type: 2 bits | per-direction QP sequence: 62 bits |
//! ```
//!
//! A WRID need not be unique across QPs: CQ-issued completions also identify
//! their hardware QPN. CQ-owned leases preserve sequence floors across QPN
//! reuse without reserving any WRID bits for connection routing.

/// A work request identity within one hardware QP number and originating CQ.
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
    /// Sequence width, independent of CQ capacity and connection count.
    pub const SEQUENCE_BITS: u32 = 62;
    /// Largest sequence that can be allocated without repeating an identity.
    pub const MAX_SEQUENCE: u64 = (1 << Self::SEQUENCE_BITS) - 1;
    pub const TYPE_SHIFT: u32 = Self::SEQUENCE_BITS;

    /// Called with a sequence reserved by the QP's CQ-owned identity lease.
    #[inline]
    pub(crate) fn new(wr_type: WRType, payload: u64) -> Self {
        debug_assert!(payload <= Self::MAX_SEQUENCE);
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

    /// Returns the sequence in its QP's send or receive stream.
    #[inline]
    pub fn sequence(self) -> u64 {
        self.0 & Self::MAX_SEQUENCE
    }
}

impl std::fmt::Debug for WRID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({:#018x})", self.get_type(), self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_and_sequence_roundtrip_at_fixed_width_boundaries() {
        for wr_type in [
            WRType::Recv,
            WRType::SendData,
            WRType::SendImm,
            WRType::Read,
        ] {
            for payload in [0, 1, (1 << 42) - 1, 1 << 42, WRID::MAX_SEQUENCE] {
                let wrid = WRID::new(wr_type, payload);
                assert_eq!(wrid.get_type(), wr_type);
                assert_eq!(wrid.sequence(), payload);
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
