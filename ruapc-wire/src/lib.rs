//! RuaPC message metadata, codecs and transport framing.
//!
//! Metadata always uses named-field MessagePack. [`MsgFlags::UseMessagePack`]
//! selects MessagePack for the payload; otherwise the payload uses JSON.
//! [`MsgMeta::serialize_to`] writes directly into storage supplied by a
//! statically dispatched [`SendMsg`] implementation. [`parse_body`] returns
//! metadata and a payload offset, leaving storage ownership with the caller;
//! [`deserialize_payload`] decodes a borrowed payload slice.
//!
//! [`frame`] implements framing shared by TCP, HTTP/2, WebSocket binary messages
//! and RDMA SENDs. Codec failures use [`CodecError`].
//! Storage hooks and frame callbacks retain the caller's error type through
//! static dispatch. RPC errors, connection policy and registered-memory
//! completion authority belong to the runtime.
//! No RDMA libraries, device or async executor are required.
//!
//! Message metadata and MessagePack payloads are decoded
//! with one explicit nesting-depth limit, [`MAX_DECODE_DEPTH`], so every
//! transport and both payload codecs use the same bound. JSON payloads are
//! bounded internally by `serde_json` at the same depth.

#![forbid(unsafe_code)]

/// Maximum nesting depth for structure that this crate decodes.
///
/// Message metadata and MessagePack payloads share this
/// bound across all transports. The wire formats nest only a few levels, so it
/// only rejects malformed or hostile input before recursive decoding can
/// exhaust the stack. JSON payloads are limited internally by `serde_json` to
/// the same depth.
pub const MAX_DECODE_DEPTH: usize = 128;

mod error;
pub use error::{CodecError, CodecErrorKind, CodecResult};

mod message;
pub use message::parse_body;

mod meta;
pub use meta::{MsgFlags, MsgMeta};

mod payload;
pub use payload::deserialize_payload;

mod encode;
pub use encode::SendMsg;

pub mod frame;

pub(crate) use ruapc_bufpool::Buffer;
