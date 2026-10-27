//! RuaPC message metadata, codecs and transport framing.
//!
//! Metadata always uses named-field MessagePack. [`MsgFlags::UseMessagePack`]
//! selects MessagePack for the payload; otherwise the payload uses JSON.
//! [`MsgMeta::serialize_to`] writes directly into storage supplied by a
//! statically dispatched [`SendMsg`] implementation. [`parse_body`] returns
//! metadata and a payload offset, leaving storage ownership with the caller;
//! [`deserialize_payload`] decodes a borrowed payload slice.
//!
//! [`frame`] implements TCP and HTTP/2 stream framing. [`rdma`] encodes and
//! decodes RDMA frames without executing transfers or certifying completion.
//! Codec failures use [`CodecError`]. Storage hooks and frame callbacks retain
//! the caller's error type through static dispatch. RPC errors, connection
//! policy and registered-memory completion authority belong to the runtime.
//! No RDMA libraries, device or async executor are required.

#![forbid(unsafe_code)]

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
pub mod rdma;

pub(crate) use ruapc_bufpool::Buffer;
