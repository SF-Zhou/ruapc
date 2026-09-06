//! Work request ID with type, CQ route slot and per-direction sequence.
//!
//! The WRID (Work Request ID) encodes a [`WRType`], a CQ-owned route slot
//! and a monotonic per-direction sequence into a single 64-bit value:
//!
//! ```text
//! | 2 bits | 14 bits | 48 bits  |
//! | type   | slot    | sequence |
//! ```
//!
//! Each CQ leases slots to QPs and preserves their sequence watermark when a
//! slot is reused. A completion maps to its QP with a plain array index; its
//! sequence also distinguishes that QP from previous occupants of the slot.

/// Work request ID with encoded type, CQ route slot and sequence.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WRID(u64);

/// Work request type encoded in a [`WRID`]
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
    /// Bit position of the type field.
    pub const TYPE_SHIFT: u32 = 62;
    /// Bit position of the CQ route slot field.
    pub const SLOT_SHIFT: u32 = 48;
    /// Width of the CQ route slot field.
    pub const SLOT_BITS: u32 = Self::TYPE_SHIFT - Self::SLOT_SHIFT;
    /// Maximum CQ route slot value.
    pub const SLOT_MAX: u16 = (1 << Self::SLOT_BITS) - 1;
    /// Mask extracting the ID field.
    pub const ID_MASK: u64 = (1 << Self::SLOT_SHIFT) - 1;

    /// Creates a new WRID with the specified type, CQ route slot and sequence.
    #[inline]
    pub fn new(wr_type: WRType, slot: u16, id: u64) -> Self {
        assert!(slot <= Self::SLOT_MAX, "slot too large");
        assert!(id <= Self::ID_MASK, "ID too large");
        Self(((wr_type as u64) << Self::TYPE_SHIFT) | (u64::from(slot) << Self::SLOT_SHIFT) | id)
    }

    /// Creates a WRID for a receive operation
    #[inline]
    pub fn recv(slot: u16, id: u64) -> Self {
        Self::new(WRType::Recv, slot, id)
    }

    /// Creates a WRID for a send data operation
    #[inline]
    pub fn send_data(slot: u16, id: u64) -> Self {
        Self::new(WRType::SendData, slot, id)
    }

    /// Creates a WRID for a send with immediate data operation
    #[inline]
    pub fn send_imm(slot: u16, id: u64) -> Self {
        Self::new(WRType::SendImm, slot, id)
    }

    /// Creates a WRID for an RDMA read operation
    #[inline]
    pub fn read(slot: u16, id: u64) -> Self {
        Self::new(WRType::Read, slot, id)
    }

    /// Returns the type of the work request
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

    /// Returns the CQ route slot as an array index.
    #[inline]
    pub fn get_slot(&self) -> usize {
        ((self.0 >> Self::SLOT_SHIFT) as usize) & Self::SLOT_MAX as usize
    }

    /// Returns the ID portion of the WRID
    #[inline]
    pub fn get_id(&self) -> u64 {
        self.0 & Self::ID_MASK
    }

    /// Returns the raw underlying `u64` value.
    #[inline]
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for WRID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self.get_type() {
            WRType::Recv => "Recv",
            WRType::SendData => "SendData",
            WRType::SendImm => "SendImm",
            WRType::Read => "Read",
        };
        write!(f, "{name}({}:{})", self.get_slot(), self.get_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrid_roundtrip_all_types() {
        for (wr_type, slot, id) in [
            (WRType::Recv, 0u16, 0u64),
            (WRType::SendData, 1, 2000),
            (WRType::SendImm, WRID::SLOT_MAX, 3000),
            (WRType::Read, 0x3F_0F, WRID::ID_MASK),
        ] {
            let wrid = WRID::new(wr_type, slot, id);
            assert_eq!(wrid.get_type(), wr_type);
            assert_eq!(wrid.get_slot(), usize::from(slot));
            assert_eq!(wrid.get_id(), id);
        }
    }

    #[test]
    fn test_wrid_constructors() {
        assert_eq!(WRID::recv(7, 1).get_type(), WRType::Recv);
        assert_eq!(WRID::send_data(7, 2).get_type(), WRType::SendData);
        assert_eq!(WRID::send_imm(7, 3).get_type(), WRType::SendImm);
        assert_eq!(WRID::read(7, 4).get_type(), WRType::Read);
        assert_eq!(WRID::read(7, 4).get_slot(), 7);
        assert_eq!(WRID::read(7, 4).get_id(), 4);
    }

    #[test]
    #[should_panic(expected = "ID too large")]
    fn test_wrid_rejects_large_id() {
        let _ = WRID::recv(0, WRID::ID_MASK + 1);
    }

    #[test]
    #[should_panic(expected = "slot too large")]
    fn test_wrid_rejects_large_slot() {
        let _ = WRID::recv(WRID::SLOT_MAX + 1, 0);
    }

    #[test]
    fn test_wrid_debug_format() {
        let wrid = WRID::recv(5, 0x1234);
        assert_eq!(format!("{wrid:?}"), "Recv(5:4660)");
        let wrid = WRID::send_data(0, 0x5678);
        assert_eq!(format!("{wrid:?}"), "SendData(0:22136)");
    }

    #[test]
    fn test_wrid_raw_is_stable() {
        let wrid = WRID::new(WRType::SendImm, 3, 9);
        assert_eq!(
            wrid.raw(),
            (2u64 << WRID::TYPE_SHIFT) | (3u64 << WRID::SLOT_SHIFT) | 9
        );
    }
}
