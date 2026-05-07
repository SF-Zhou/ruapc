//! # RDMA verbs bindings for ruapc
//!
//! This crate provides type-safe device management and low-level FFI bindings
//! to libibverbs (RDMA verbs) with JSON serialization support.
//!
//! ## Architecture
//!
//! - **RAII wrappers**: All libibverbs resources (context, PD, CQ, QP, MR,
//!   completion channel) are wrapped in types that automatically clean up on drop.
//! - **QueuePair\<B\>**: Generic over a user-provided [`RdmaBuffer`] type.
//!   High-level [`send`](QueuePair::send) / [`recv`](QueuePair::recv) /
//!   [`read`](QueuePair::read) take ownership of the buffer and return it via
//!   [`Completion`] when the work request completes.  Low-level
//!   [`post_send`](QueuePair::post_send) / [`post_recv`](QueuePair::post_recv)
//!   accept raw work request pointers for advanced use cases.
//! - **Type-safe bindings**: Generated FFI types have custom Rust wrappers
//!   (`FwVer`, `Guid`, `WRID`, `LinkLayer`) substituted at build time.
//!   Raw C functions are hidden behind a private `ffi` module.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! let devices = ruapc_rdma_sys::ActiveDevice::available()?;
//! for dev in &devices {
//!     println!("{}: {}", dev.info().name, dev.info().guid);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![allow(dead_code)]
#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]
#![allow(clippy::missing_safety_doc, clippy::too_many_arguments)]

mod ffi;
pub(crate) use ffi::*;

// Publicly expose the generated types that appear in public API signatures
pub use ffi::{
    ibv_access_flags, ibv_atomic_cap, ibv_comp_channel, ibv_context, ibv_cq, ibv_device_attr,
    ibv_device_cap_flags, ibv_gid, ibv_mr, ibv_mtu, ibv_pd, ibv_port_attr, ibv_port_state, ibv_qp,
    ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_state, ibv_qp_type,
    ibv_recv_wr, ibv_send_flags, ibv_send_wr, ibv_wc, ibv_wc_flags, ibv_wc_status,
};

mod error;
pub use error::{Error, ErrorKind, Result};

mod types;
pub use types::{DeviceInfo, FwVer, Gid, GidType, Guid, LinkLayer, Port, RdmaBuffer, WRID, WRType};

mod verbs;
pub use verbs::{
    ActiveDevice, CompChannel, Completion, CompletionQueue, Context, Device, DeviceList,
    MemoryRegion, ProtectionDomain, QueuePair,
};

#[cfg(test)]
mod test_utils;
