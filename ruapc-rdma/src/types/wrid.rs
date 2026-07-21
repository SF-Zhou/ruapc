//! Work request ID with type, connection tag and per-direction ID
//!
//! The WRID (Work Request ID) encodes a [`WRType`], an opaque connection
//! tag and a monotonic per-direction ID into a single 64-bit value:
//!
//! ```text
//! | 2 bits | 22 bits | 40 bits |
//! | type   | tag     | id      |
//! ```
//!
//! The tag is opaque to this crate; `ruapc` packs a poller slot index and
//! a generation counter into it so a completion maps back to its
//! connection with a plain array index — no `qp_num` hash lookup.

/// Work request ID with encoded type, connection tag and ID
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
    /// Bit position of the connection tag field.
    pub const TAG_SHIFT: u32 = 40;
    /// Width of the connection tag field.
    pub const TAG_BITS: u32 = Self::TYPE_SHIFT - Self::TAG_SHIFT;
    /// Maximum connection tag value.
    pub const TAG_MAX: u32 = (1 << Self::TAG_BITS) - 1;
    /// Mask extracting the ID field.
    pub const ID_MASK: u64 = (1 << Self::TAG_SHIFT) - 1;

    /// Creates a new WRID with the specified type, connection tag and ID
    pub fn new(wr_type: WRType, tag: u32, id: u64) -> Self {
        assert!(tag <= Self::TAG_MAX, "tag too large");
        assert!(id <= Self::ID_MASK, "ID too large");
        Self(((wr_type as u64) << Self::TYPE_SHIFT) | (u64::from(tag) << Self::TAG_SHIFT) | id)
    }

    /// Creates a WRID for a receive operation
    pub fn recv(tag: u32, id: u64) -> Self {
        Self::new(WRType::Recv, tag, id)
    }

    /// Creates a WRID for a send data operation
    pub fn send_data(tag: u32, id: u64) -> Self {
        Self::new(WRType::SendData, tag, id)
    }

    /// Creates a WRID for a send with immediate data operation
    pub fn send_imm(tag: u32, id: u64) -> Self {
        Self::new(WRType::SendImm, tag, id)
    }

    /// Creates a WRID for an RDMA read operation
    pub fn read(tag: u32, id: u64) -> Self {
        Self::new(WRType::Read, tag, id)
    }

    /// Returns the type of the work request
    pub fn get_type(&self) -> WRType {
        match self.0 >> Self::TYPE_SHIFT {
            0 => WRType::Recv,
            1 => WRType::SendData,
            2 => WRType::SendImm,
            3 => WRType::Read,
            _ => unreachable!(),
        }
    }

    /// Returns the connection tag portion of the WRID
    pub fn get_tag(&self) -> u32 {
        ((self.0 >> Self::TAG_SHIFT) as u32) & Self::TAG_MAX
    }

    /// Returns the ID portion of the WRID
    pub fn get_id(&self) -> u64 {
        self.0 & Self::ID_MASK
    }

    /// Returns the raw underlying `u64` value.
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
        write!(f, "{name}({}:{})", self.get_tag(), self.get_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrid_roundtrip_all_types() {
        for (wr_type, tag, id) in [
            (WRType::Recv, 0u32, 0u64),
            (WRType::SendData, 1, 2000),
            (WRType::SendImm, WRID::TAG_MAX, 3000),
            (WRType::Read, 0x3F_0F, WRID::ID_MASK),
        ] {
            let wrid = WRID::new(wr_type, tag, id);
            assert_eq!(wrid.get_type(), wr_type);
            assert_eq!(wrid.get_tag(), tag);
            assert_eq!(wrid.get_id(), id);
        }
    }

    #[test]
    fn test_wrid_constructors() {
        assert_eq!(WRID::recv(7, 1).get_type(), WRType::Recv);
        assert_eq!(WRID::send_data(7, 2).get_type(), WRType::SendData);
        assert_eq!(WRID::send_imm(7, 3).get_type(), WRType::SendImm);
        assert_eq!(WRID::read(7, 4).get_type(), WRType::Read);
        assert_eq!(WRID::read(7, 4).get_tag(), 7);
        assert_eq!(WRID::read(7, 4).get_id(), 4);
    }

    #[test]
    #[should_panic(expected = "ID too large")]
    fn test_wrid_rejects_large_id() {
        let _ = WRID::recv(0, WRID::ID_MASK + 1);
    }

    #[test]
    #[should_panic(expected = "tag too large")]
    fn test_wrid_rejects_large_tag() {
        let _ = WRID::recv(WRID::TAG_MAX + 1, 0);
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
            (2u64 << WRID::TYPE_SHIFT) | (3u64 << WRID::TAG_SHIFT) | 9
        );
    }
}
