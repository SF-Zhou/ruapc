//! Routing metadata and payload-format selection.

use crate::Result;
use bitflags::bitflags;
use ruapc_bufpool::RemoteBufferInfo;
use serde::{Deserialize, Serialize};
use std::io::Write;

/// Request/response markers and payload-format selection.
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone, Copy)]
#[repr(transparent)]
#[serde(transparent)]
pub struct MsgFlags(u8);

bitflags! {
    impl MsgFlags: u8 {
        /// Message is a request.
        const IsReq = 1;
        /// Message is a response.
        const IsRsp = 2;
        /// Encode the payload as MessagePack; otherwise use JSON.
        const UseMessagePack = 4;
    }
}

/// Routing, request identity, memory regions, and time budget.
///
/// Always encoded as named-field MessagePack. [`MsgFlags::UseMessagePack`]
/// selects only the payload format: metadata must be decodable before reading
/// that flag. New fields use `#[serde(default)]` for missing values and
/// `skip_serializing_if` where appropriate; unknown fields are ignored.
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct MsgMeta {
    /// The fully qualified method name (e.g., "ServiceName/method_name").
    /// Empty for responses, which are correlated by `msgid` alone.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub method: String,
    /// Message flags controlling behavior and format.
    #[serde(default)]
    pub flags: MsgFlags,
    /// Message ID for correlating requests and responses.
    #[serde(default)]
    pub msgid: u64,
    /// Regions of the sender's registered memory the receiver may *read*
    /// (RDMA READ or reverse-RPC copy). In order, they form one logical
    /// contiguous space. Attached by `Client::with_read_buffers`; also
    /// used by the reverse `_ruapc.memory/read_into_target` request to advertise the
    /// server's source buffers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_regions: Vec<RemoteBufferInfo>,
    /// Regions of the sender's registered memory the receiver may *write*
    /// (through the internal remote-memory protocol). In order, they form one logical
    /// contiguous space. Attached by `Client::with_write_buffers`; active RDMA
    /// retains the destination even if the request has already resolved.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_regions: Vec<RemoteBufferInfo>,
    /// Remaining time budget of the request in milliseconds, set by the
    /// client. The server derives a deadline from it on arrival (relative
    /// budgets avoid clock-skew issues of absolute timestamps), drops the
    /// request if it expires before execution, and shrinks the budget of
    /// nested RPCs issued while handling it. Zero means the request is
    /// already expired; messages without timeout semantics also use zero.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub timeout_ms: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl MsgMeta {
    /// Encodes named-field MessagePack metadata.
    pub(super) fn encode_to<W: Write>(&self, mut w: W) -> Result<()> {
        rmp_serde::encode::write_named(&mut w, self)?;
        Ok(())
    }

    /// Decodes metadata encoded by [`encode_to`](Self::encode_to).
    pub(super) fn decode(buf: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(buf)?)
    }

    /// Checks if this message is a request.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::{MsgMeta, MsgFlags};
    /// let mut meta = MsgMeta::default();
    /// meta.flags = MsgFlags::IsReq;
    /// assert!(meta.is_req());
    /// ```
    #[must_use]
    pub fn is_req(&self) -> bool {
        self.flags.contains(MsgFlags::IsReq)
    }

    /// Checks if this message is a response.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use ruapc::{MsgMeta, MsgFlags};
    /// let mut meta = MsgMeta::default();
    /// meta.flags = MsgFlags::IsRsp;
    /// assert!(meta.is_rsp());
    /// ```
    #[must_use]
    pub fn is_rsp(&self) -> bool {
        self.flags.contains(MsgFlags::IsRsp)
    }
}
