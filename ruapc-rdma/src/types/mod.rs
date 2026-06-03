//! # Custom RDMA types
//!
//! This module provides type-safe wrappers for RDMA-related data structures
//! with enhanced serialization and formatting support.
//!
//! ## Module Organization
//!
//! - [`fw_ver`]: Firmware version wrapper for null-terminated strings
//! - [`gid`]: Global Identifier (GID) with IPv6 conversion
//! - [`guid`]: Globally Unique Identifier with colon-separated formatting
//! - [`link_layer`]: Link layer type (InfiniBand/Ethernet)
//! - [`wrid`]: Work Request ID with type encoding
//! - [`wc`]: Work completion helper methods
//! - [`pthread`]: pthread wrapper types for RDMA bindings
//!
//! ## Features
//!
//! All types in this module support:
//! - JSON serialization/deserialization via serde
//! - JSON Schema generation via schemars
//! - Custom display and debug formatting

mod fw_ver;
pub use fw_ver::FwVer;

mod gid;
mod wc;

mod guid;
pub use guid::Guid;

mod link_layer;
pub use link_layer::LinkLayer;

mod pthread;
pub use pthread::{pthread_cond_t, pthread_mutex_t};

mod wrid;
pub use wrid::WRID;

mod buffer;
pub use buffer::RdmaBuffer;

mod device_info;
pub use device_info::{DeviceInfo, Gid, GidType, Port};

mod device_cap_flags;

mod port_cap_flags;

mod port_cap_flags2;
