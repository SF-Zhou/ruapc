//! Fixed-width work request identities, scoped by the CQ and hardware QPN.
//!
//! ```text
//! 63                                           2 1             0
//! | per-direction QP sequence: 62 bits           | type: 2 bits |
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
    /// Type width in the least significant bits.
    pub const TYPE_BITS: u32 = 2;
    /// Sequence width, independent of CQ capacity and connection count.
    pub const SEQUENCE_BITS: u32 = u64::BITS - Self::TYPE_BITS;
    /// Largest sequence that can be allocated without repeating an identity.
    pub const MAX_SEQUENCE: u64 = (1 << Self::SEQUENCE_BITS) - 1;
    const TYPE_MASK: u64 = (1 << Self::TYPE_BITS) - 1;

    /// Called with a sequence reserved by the QP's CQ-owned identity lease.
    #[inline]
    pub(crate) fn new(wr_type: WRType, sequence: u64) -> Self {
        debug_assert!(sequence <= Self::MAX_SEQUENCE);
        Self((sequence << Self::TYPE_BITS) | wr_type as u64)
    }

    /// Returns the type of the work request.
    #[inline]
    pub fn get_type(&self) -> WRType {
        match self.0 & Self::TYPE_MASK {
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
        self.0 >> Self::TYPE_BITS
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
            for sequence in [
                0,
                1,
                (1 << 42) - 1,
                1 << 42,
                (1 << 61) - 1,
                1 << 61,
                WRID::MAX_SEQUENCE - 1,
                WRID::MAX_SEQUENCE,
            ] {
                let wrid = WRID::new(wr_type, sequence);
                assert_eq!(wrid.get_type(), wr_type);
                assert_eq!(wrid.sequence(), sequence);
            }
        }
    }

    #[test]
    fn raw_identity_places_type_below_the_full_sequence() {
        assert_eq!(WRID::TYPE_BITS, 2);
        assert_eq!(WRID::SEQUENCE_BITS, 62);
        assert_eq!(WRID::MAX_SEQUENCE, 0x3fff_ffff_ffff_ffff);
        for (wr_type, zero_raw, one_raw, max_raw) in [
            (WRType::Recv, 0, 4, 0xffff_ffff_ffff_fffc),
            (WRType::SendData, 1, 5, 0xffff_ffff_ffff_fffd),
            (WRType::SendImm, 2, 6, 0xffff_ffff_ffff_fffe),
            (WRType::Read, 3, 7, 0xffff_ffff_ffff_ffff),
        ] {
            for (sequence, raw) in [(0, zero_raw), (1, one_raw), (WRID::MAX_SEQUENCE, max_raw)] {
                assert_eq!(WRID::new(wr_type, sequence).raw(), raw);
                let decoded = WRID(raw);
                assert_eq!(decoded.get_type(), wr_type);
                assert_eq!(decoded.sequence(), sequence);
            }
        }
    }

    #[test]
    fn debug_prints_raw_identity_without_guessing_the_layout() {
        for (wr_type, sequence, expected) in [
            (WRType::Recv, 123, "Recv(0x00000000000001ec)"),
            (WRType::SendData, 0, "SendData(0x0000000000000001)"),
            (WRType::SendImm, 1, "SendImm(0x0000000000000006)"),
            (WRType::Read, WRID::MAX_SEQUENCE, "Read(0xffffffffffffffff)"),
        ] {
            assert_eq!(format!("{:?}", WRID::new(wr_type, sequence)), expected);
        }
    }
}
