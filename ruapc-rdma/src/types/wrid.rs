//! Work request ID
//!
//! The WRID (Work Request ID) is a transparent wrapper around a 64-bit value
//! that is carried on the wire as the verbs `wr_id` and echoed back in the
//! corresponding work completion (`ibv_wc::wr_id`).
//!
//! Historically the WRID packed an operation type into its top two bits. That
//! coupling has been removed: the WRID is now an opaque, caller-assigned
//! identifier. The meaning of a work request (receive / send / ack / RDMA read)
//! and the buffer it owns are tracked by the higher layer in a per-CQ work
//! request registry keyed by this id, so a single shared completion queue can
//! demultiplex completions for many queue pairs without inspecting the id bits.

/// Opaque work request identifier carried on the wire as the verbs `wr_id`.
///
/// The value is assigned by the caller (typically from a process-wide monotonic
/// counter) and must be unique among all work requests that may complete on the
/// same completion queue at the same time, so completions can be matched back to
/// their originating request via the registry.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct WRID(u64);

impl WRID {
    /// Creates a new WRID from a raw 64-bit identifier.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the identifier value.
    pub fn get_id(&self) -> u64 {
        self.0
    }

    /// Returns the raw underlying `u64` value.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl From<u64> for WRID {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl std::fmt::Debug for WRID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WRID({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrid_new_and_get_id() {
        let id = 12345u64;
        let wrid = WRID::new(id);
        assert_eq!(wrid.get_id(), id);
        assert_eq!(wrid.raw(), id);
    }

    #[test]
    fn test_wrid_from_u64() {
        let wrid: WRID = 67890u64.into();
        assert_eq!(wrid.get_id(), 67890);
    }

    #[test]
    fn test_wrid_full_range() {
        // The full 64-bit range is now usable; no bits are reserved.
        let wrid = WRID::new(u64::MAX);
        assert_eq!(wrid.get_id(), u64::MAX);
    }

    #[test]
    fn test_wrid_debug() {
        let wrid = WRID::new(123);
        assert_eq!(format!("{:?}", wrid), "WRID(123)");
    }

    #[test]
    fn test_wrid_default() {
        assert_eq!(WRID::default().get_id(), 0);
    }
}
