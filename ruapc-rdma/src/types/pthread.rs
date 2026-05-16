//! pthread wrapper types for RDMA bindings
//!
//! This module provides wrapper types around libc pthread types to allow
//! proper serialization and Debug implementation for FFI bindings.
//!
//! These types are used by generated libibverbs bindings to replace
//! raw pthread types that cannot derive serde/schemars traits.

/// Wrapper for pthread mutex
///
/// Transparent wrapper around `libc::pthread_mutex_t` that implements
/// Debug for use in generated RDMA bindings.
#[repr(transparent)]
pub struct pthread_mutex_t(pub libc::pthread_mutex_t);

impl std::fmt::Debug for pthread_mutex_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("pthread_mutex_t").finish()
    }
}

/// Wrapper for pthread condition variable
#[repr(transparent)]
pub struct pthread_cond_t(pub libc::pthread_cond_t);

impl std::fmt::Debug for pthread_cond_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("pthread_cond_t").finish()
    }
}
