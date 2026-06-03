//! Work completion (ibv_wc) helper methods
//!
//! Provides type-safe helper methods for checking work completion status
//! and extracting completion data.
//!
//! Note: the operation kind (receive / send / ack / RDMA read) is **not**
//! derivable from the work completion alone anymore — the `wr_id` is an opaque
//! identifier. The higher layer resolves the operation kind by looking the
//! `wr_id` up in its work-request registry. The helpers here only expose
//! information that is intrinsic to the completion: success status and any
//! immediate data.

use crate::{ibv_wc, ibv_wc_flags, ibv_wc_status};

impl ibv_wc {
    /// Checks if the work completed successfully
    ///
    /// Returns true if the completion status is IBV_WC_SUCCESS
    pub fn succ(&self) -> bool {
        self.status == ibv_wc_status::IBV_WC_SUCCESS
    }

    /// Extracts immediate data from this work completion
    ///
    /// Returns Some with the immediate data value if the IBV_WC_WITH_IMM
    /// flag is set, otherwise returns None
    pub fn imm(&self) -> Option<u32> {
        if ibv_wc_flags(self.wc_flags) & ibv_wc_flags::IBV_WC_WITH_IMM != ibv_wc_flags(0) {
            Some(u32::from_be(unsafe { self.__bindgen_anon_1.imm_data }))
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ibv_wc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ibv_wc")
            .field("wr_id", &self.wr_id)
            .field("status", &self.status)
            .field("opcode", &self.opcode)
            .field("vendor_err", &self.vendor_err)
            .field("byte_len", &self.byte_len)
            .field("imm_data", &self.imm())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::WRID;

    use super::*;

    fn make_wc(wr_id: WRID, status: ibv_wc_status, wc_flags: u32) -> ibv_wc {
        ibv_wc {
            wr_id,
            status,
            wc_flags,
            ..Default::default()
        }
    }

    #[test]
    fn test_wc_succ() {
        let wc = make_wc(WRID::new(0), ibv_wc_status::IBV_WC_SUCCESS, 0);
        assert!(wc.succ());

        let wc = make_wc(WRID::new(0), ibv_wc_status::IBV_WC_LOC_LEN_ERR, 0);
        assert!(!wc.succ());
    }

    #[test]
    fn test_wc_imm_none() {
        let wc = make_wc(WRID::new(0), ibv_wc_status::IBV_WC_SUCCESS, 0);
        assert_eq!(wc.imm(), None);
    }

    #[test]
    fn test_wc_imm_some() {
        let mut wc = make_wc(
            WRID::new(0),
            ibv_wc_status::IBV_WC_SUCCESS,
            ibv_wc_flags::IBV_WC_WITH_IMM.0,
        );
        wc.__bindgen_anon_1.imm_data = 42u32.to_be();
        assert_eq!(wc.imm(), Some(42));
    }

    #[test]
    fn test_wc_debug() {
        let wc = make_wc(WRID::new(123), ibv_wc_status::IBV_WC_SUCCESS, 0);
        let debug = format!("{:?}", wc);
        assert!(debug.contains("WRID(123)"));
        assert!(debug.contains("IBV_WC_SUCCESS"));
    }
}
