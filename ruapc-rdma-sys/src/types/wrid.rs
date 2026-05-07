//! Work request ID with type information
//!
//! The WRID (Work Request ID) encodes a [`WRType`] and an opaque ID into a
//! single 64-bit value for matching work requests to their completions.

/// Work request ID with encoded type information
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
    /// Number of bits used for type information
    pub const TYPE_BITS: u32 = 62;
    /// Mask to extract type bits from WRID
    pub const TYPE_MASK: u64 = ((1 << (u64::BITS - Self::TYPE_BITS)) - 1) << Self::TYPE_BITS;

    /// Creates a new WRID with the specified type and ID
    pub fn new(wr_type: WRType, id: u64) -> Self {
        assert!(id & Self::TYPE_MASK == 0, "ID too large");
        Self(((wr_type as u64) << Self::TYPE_BITS) | id)
    }

    /// Creates a WRID for a receive operation
    pub fn recv(id: u64) -> Self {
        Self::new(WRType::Recv, id)
    }

    /// Creates a WRID for a send data operation
    pub fn send_data(id: u64) -> Self {
        Self::new(WRType::SendData, id)
    }

    /// Creates a WRID for a send with immediate data operation
    pub fn send_imm(id: u64) -> Self {
        Self::new(WRType::SendImm, id)
    }

    /// Creates a WRID for an RDMA read operation
    pub fn read(id: u64) -> Self {
        Self::new(WRType::Read, id)
    }

    /// Returns the type of the work request
    pub fn get_type(&self) -> WRType {
        match (self.0 & Self::TYPE_MASK) >> Self::TYPE_BITS {
            0 => WRType::Recv,
            1 => WRType::SendData,
            2 => WRType::SendImm,
            3 => WRType::Read,
            _ => unreachable!(),
        }
    }

    /// Returns the ID portion of the WRID
    pub fn get_id(&self) -> u64 {
        self.0 & !Self::TYPE_MASK
    }

    /// Returns the raw underlying `u64` value.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for WRID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.get_type() {
            WRType::Recv => write!(f, "Recv({})", self.get_id()),
            WRType::SendData => write!(f, "SendData({})", self.get_id()),
            WRType::SendImm => write!(f, "SendImm({})", self.get_id()),
            WRType::Read => write!(f, "Read({})", self.get_id()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrid_recv() {
        let id = 12345u64;
        let wrid = WRID::recv(id);
        assert_eq!(wrid.get_type(), WRType::Recv);
        assert_eq!(wrid.get_id(), id);
    }

    #[test]
    fn test_wrid_send_data() {
        let id = 67890u64;
        let wrid = WRID::send_data(id);
        assert_eq!(wrid.get_type(), WRType::SendData);
        assert_eq!(wrid.get_id(), id);
    }

    #[test]
    fn test_wrid_send_imm() {
        let id = 54321u64;
        let wrid = WRID::send_imm(id);
        assert_eq!(wrid.get_type(), WRType::SendImm);
        assert_eq!(wrid.get_id(), id);
    }

    #[test]
    fn test_wrid_new() {
        let wrid = WRID::new(WRType::Recv, 1000);
        assert_eq!(wrid.get_type(), WRType::Recv);
        assert_eq!(wrid.get_id(), 1000);

        let wrid = WRID::new(WRType::SendData, 2000);
        assert_eq!(wrid.get_type(), WRType::SendData);
        assert_eq!(wrid.get_id(), 2000);

        let wrid = WRID::new(WRType::SendImm, 3000);
        assert_eq!(wrid.get_type(), WRType::SendImm);
        assert_eq!(wrid.get_id(), 3000);
    }

    #[test]
    fn test_wrid_id_overflow() {
        let large_id = 1u64 << 62;
        let result = std::panic::catch_unwind(|| {
            WRID::new(WRType::Recv, large_id);
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_wrid_debug() {
        let wrid = WRID::recv(123);
        let debug_str = format!("{:?}", wrid);
        assert_eq!(debug_str, "Recv(123)");

        let wrid = WRID::send_data(456);
        let debug_str = format!("{:?}", wrid);
        assert_eq!(debug_str, "SendData(456)");

        let wrid = WRID::send_imm(789);
        let debug_str = format!("{:?}", wrid);
        assert_eq!(debug_str, "SendImm(789)");
    }

    #[test]
    fn test_wrid_type_mask() {
        let mask = WRID::TYPE_MASK;
        let expected_mask: u64 = 0xC000000000000000;
        assert_eq!(mask, expected_mask);
    }

    #[test]
    fn test_wrid_encoding() {
        let wrid = WRID::recv(0x1234);
        let value = wrid.raw();
        assert_eq!(value & WRID::TYPE_MASK, 0);
        assert_eq!(value & !WRID::TYPE_MASK, 0x1234);

        let wrid = WRID::send_data(0x5678);
        let value = wrid.raw();
        assert_eq!((value & WRID::TYPE_MASK) >> WRID::TYPE_BITS, 1);
        assert_eq!(value & !WRID::TYPE_MASK, 0x5678);

        let wrid = WRID::send_imm(0x9ABC);
        let value = wrid.raw();
        assert_eq!((value & WRID::TYPE_MASK) >> WRID::TYPE_BITS, 2);
        assert_eq!(value & !WRID::TYPE_MASK, 0x9ABC);
    }
}
