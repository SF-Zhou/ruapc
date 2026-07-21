//! # RDMA verbs resource management
//!
//! Type-safe RAII wrappers for libibverbs resources with automatic
//! lifetime management via `Arc` ownership chains:
//!
//! ```text
//! Context
//!   ├─ ProtectionDomain
//!   │    ├─ MemoryRegion
//!   │    └─ QueuePair  (+ send CQ + recv CQ)
//!   ├─ CompChannel
//!   └─ CompletionQueue (+ optional CompChannel)
//! ```
//!
//! Each child type holds an `Arc` to its parent, so resources are always
//! destroyed in the correct order regardless of user drop ordering.

mod comp_channel;
pub use comp_channel::CompChannel;

mod completion_queue;
pub use completion_queue::CompletionQueue;

mod context;
pub use context::Context;

mod device;
pub use device::{ActiveDevice, Device};

mod device_list;
pub use device_list::DeviceList;

mod memory_region;
pub use memory_region::MemoryRegion;

mod protection_domain;
pub use protection_domain::ProtectionDomain;

mod queue_pair;
pub use queue_pair::{Completion, MAX_GATHER_SGE, QueuePair, WrBuffers};

mod wr_slots;
