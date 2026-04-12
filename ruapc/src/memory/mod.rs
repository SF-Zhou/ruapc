mod memory_key;
pub use memory_key::MemoryKey;

mod memory_registration;
pub use memory_registration::MemoryRegistration;

mod remote_buffer_info;
pub use remote_buffer_info::RemoteBufferInfo;

// Re-export AlignedMemory from ruapc-bufpool.
pub use ruapc_bufpool::AlignedMemory;

/// Type alias for a registered memory block parameterized with ruapc's
/// `MemoryRegistration`.
pub type Memory = ruapc_bufpool::Memory<MemoryRegistration>;

/// Type alias for a buffer from the pool, parameterized with ruapc's `Device`.
pub type Buffer = ruapc_bufpool::Buffer<crate::device::Device>;

/// Type alias for the buffer pool, parameterized with ruapc's `Device`.
pub type BufferPool = ruapc_bufpool::BufferPool<crate::device::Device>;

/// Extension trait providing ruapc-specific methods on `Buffer`.
pub trait BufferExt {
    /// Returns the [`MemoryKey`] for this buffer on the given device.
    fn memory_key(&self, device: &crate::device::Device) -> crate::Result<MemoryKey>;

    /// Builds the [`RemoteBufferInfo`] for this buffer on the given device.
    ///
    /// This information can be sent to a remote peer via RPC so it can
    /// perform Remote Read/Write operations on this buffer.
    fn remote_buffer_info(&self, device: &crate::device::Device)
    -> crate::Result<RemoteBufferInfo>;
}

impl BufferExt for Buffer {
    fn memory_key(&self, device: &crate::device::Device) -> crate::Result<MemoryKey> {
        let reg = self
            .memory()
            .registration(device.index())
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
        Ok(reg.memory_key())
    }

    fn remote_buffer_info(
        &self,
        device: &crate::device::Device,
    ) -> crate::Result<RemoteBufferInfo> {
        let key = self.memory_key(device)?;
        Ok(RemoteBufferInfo {
            key,
            addr: self.as_ptr() as u64,
            len: self.capacity() as u64,
        })
    }
}

/// Extension trait providing ruapc-specific methods on `Memory`.
pub trait MemoryExt {
    /// Returns the `MemoryKey` for the given device, looked up by device index.
    fn get_memory_key(&self, device: &crate::device::Device) -> crate::Result<MemoryKey>;
}

impl MemoryExt for Memory {
    fn get_memory_key(&self, device: &crate::device::Device) -> crate::Result<MemoryKey> {
        let reg = self
            .registration(device.index())
            .map_err(|e| crate::Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
        Ok(reg.memory_key())
    }
}
