mod types;
pub use types::{MemoryKey, RemoteBufferInfo};

mod memory_registration;
pub use memory_registration::MemoryRegistration;

pub type Buffer = ruapc_bufpool::Buffer<crate::device::Devices>;

pub type BufferPool = ruapc_bufpool::BufferPool<crate::device::Devices>;
