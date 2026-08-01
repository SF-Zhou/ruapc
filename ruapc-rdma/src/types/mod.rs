//! # Custom RDMA types
//!
//! Crate-defined, type-safe data structures. Several of them (`FwVer`,
//! `Guid`, `LinkLayer`, `WRID`) are substituted into the generated FFI
//! bindings at build time; the rest are plain value types used across the
//! public API.
//!
//! ## Module Organization
//!
//! - [`device_info`]: serializable device/port/GID snapshots
//! - [`fw_ver`]: firmware version wrapper for null-terminated strings
//! - [`guid`]: Globally Unique Identifier with colon-separated formatting
//! - [`link_layer`]: link layer type (InfiniBand/Ethernet)
//! - [`wrid`]: work request ID with type encoding
//!
//! ## Features
//!
//! All types in this module support:
//! - JSON serialization/deserialization via serde
//! - JSON Schema generation via schemars
//! - Custom display and debug formatting

mod device_info;
pub use device_info::{DeviceInfo, Gid, GidType, Port};

mod fw_ver;
pub use fw_ver::FwVer;

mod guid;
pub use guid::Guid;

mod link_layer;
pub use link_layer::LinkLayer;

mod wrid;
pub use wrid::{WRID, WRType};
