//! # Generated libibverbs FFI bindings and extensions
//!
//! The `bindings` submodule includes the bindgen output produced by
//! `build.rs` (see the crate README for the C shim rationale). The sibling
//! modules extend the generated types with safe, idiomatic helpers:
//!
//! - [`flags`]: typed accessors and static names for `enumflags2` capability
//!   enums generated from the installed header
//! - [`gid`]: `ibv_gid` accessors, IPv6 conversion, and serialization
//! - [`wc`]: `ibv_wc` status/type helpers and immediate-data extraction
//! - [`pthread`]: opaque pthread wrappers referenced by the bindings
//!
//! Lint allowances for the generated code are scoped to the `bindings`
//! submodule so that handwritten code stays fully linted.

mod flags;
mod gid;
mod pthread;
mod wc;

#[allow(
    dead_code,
    nonstandard_style,
    clippy::missing_safety_doc,
    clippy::too_many_arguments,
    clippy::useless_transmute
)]
mod bindings {
    use enumflags2::BitFlags;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::pthread::{pthread_cond_t, pthread_mutex_t};
    use crate::types::{FwVer, Guid, LinkLayer, WRID};

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::*;

#[allow(clippy::derivable_impls)]
impl Default for ibv_transport_type {
    fn default() -> Self {
        ibv_transport_type::IBV_TRANSPORT_UNKNOWN
    }
}
