//! Routing metadata and payload-format selection.

use crate::Result;
use bitflags::bitflags;
use ruapc_bufpool::RemoteBufferInfo;
use serde::{Deserialize, Serialize};
use std::io::Write;

/// Message flags for RPC communication.
///
/// Flags control message behavior and serialization format:
/// - `IsReq`: Indicates this is a request (vs. response)
/// - `UseMessagePack`: Use MessagePack instead of JSON for serialization
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
        /// Use MessagePack serialization format.
        const UseMessagePack = 4;
    }
}

/// Message metadata containing routing and control information.
///
/// The metadata is serialized at the beginning of each message and contains:
/// - Method name for routing
/// - Flags controlling message behavior
/// - Message ID for request/response correlation
///
/// # Wire encoding
///
/// The whole struct is MessagePack-encoded with field names
/// ([`rmp_serde::encode::write_named`]), *regardless* of the
/// `UseMessagePack` flag — the flag only selects the payload format. The
/// meta encoding cannot depend on a flag stored inside itself, and a fixed
/// format keeps decoding self-contained. Named encoding makes the meta
/// extensible: new fields are added with `#[serde(default)]` (+
/// `skip_serializing_if` to keep them free when absent) and old peers
/// ignore them.
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
    /// contiguous space. Attached by `Client::with_write_buffers`; the
    /// buffers stay pinned client-side until the request resolves.
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
    /// Encodes the metadata (MessagePack, named fields — see the type-level
    /// docs for the rationale).
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
