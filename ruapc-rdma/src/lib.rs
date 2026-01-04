//! # ruapc-rdma
//!
//! RDMA (Remote Direct Memory Access) support for the RuaPC RPC library.
//!
//! This crate provides low-level RDMA functionality including:
//! - Device discovery and management
//! - Queue pair creation and management
//! - Completion queues and channels
//! - Memory registration and buffer management
//!
//! ## Features
//!
//! - **Zero-copy data transfer**: Direct memory access without CPU involvement
//! - **High throughput**: Optimized for maximum bandwidth
//! - **Low latency**: Minimal overhead for small messages
//! - **Reliable connections**: RC (Reliable Connection) queue pairs
//!
//! ## Example
//!
//! ```rust,no_run
//! # use ruapc_rdma::Devices;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Discover available RDMA devices
//! let devices = Devices::availables()?;
//! let device = devices.first().unwrap();
//!
//! println!("Device: {:?}", device.info());
//! # Ok(())
//! # }
//! ```
//!
//! ## Requirements
//!
//! This crate requires:
//! - InfiniBand Verbs library (libibverbs)
//! - RDMA-capable hardware or software emulation

/// InfiniBand Verbs API bindings and types.
pub mod verbs;

mod error;
pub use error::*;

mod config;
pub use config::{Config, DeviceConfig, GidType};

mod devices;
pub use devices::{Device, DeviceInfo, Devices};

mod comp_channel;
pub use comp_channel::CompChannel;

mod comp_queues;
pub use comp_queues::CompQueue;

mod queue_pair;
pub use queue_pair::{Endpoint, QueuePair};

mod buf;
pub use buf::*;
