#[allow(unsafe_code)]
mod aligned_memory;
pub use aligned_memory::AlignedMemory;

mod memory_key;
pub use memory_key::MemoryKey;

mod memory_registration;
pub use memory_registration::MemoryRegistration;

mod registered;
pub use registered::Memory;
