use crate::*;

/// Raw memory region wrapper with automatic deregistration.
///
/// Wraps an InfiniBand memory region pointer and ensures
/// proper cleanup on drop.
struct RawMemoryRegion(*mut verbs::ibv_mr);
impl std::ops::Deref for RawMemoryRegion {
    type Target = verbs::ibv_mr;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}
impl Drop for RawMemoryRegion {
    fn drop(&mut self) {
        let _ = unsafe { verbs::ibv_dereg_mr(self.0) };
    }
}
unsafe impl Send for RawMemoryRegion {}
unsafe impl Sync for RawMemoryRegion {}

/// RDMA-registered memory buffer.
///
/// A buffer that has been registered with RDMA devices for
/// zero-copy data transfer. The buffer is automatically
/// deregistered when dropped.
///
/// # Examples
///
/// ```rust,no_run
/// # use ruapc_rdma::{RegisteredBuffer, Devices};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let devices = Devices::availables()?;
/// let buffer = RegisteredBuffer::create(&devices, 4096)?;
/// println!("Buffer address: {:p}", buffer.as_ptr());
/// # Ok(())
/// # }
/// ```
pub struct RegisteredBuffer {
    memory_regions: Vec<RawMemoryRegion>,
    aligned_buffer: AlignedBuffer,
    _devices: Devices,
}

impl RegisteredBuffer {
    /// Creates a new RDMA-registered buffer.
    ///
    /// The buffer is registered with all devices in the provided collection,
    /// allowing it to be used for RDMA operations with any of those devices.
    ///
    /// # Arguments
    ///
    /// * `devices` - Collection of RDMA devices to register with
    /// * `size` - Size of the buffer in bytes
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Memory allocation fails
    /// - Memory registration fails
    pub fn create(devices: &Devices, size: usize) -> Result<Self> {
        let mut buf = AlignedBuffer::new(size)?;
        let mut memory_regions = Vec::with_capacity(devices.len());
        for device in devices {
            let mr = unsafe {
                verbs::ibv_reg_mr(
                    device.pd_ptr(),
                    buf.as_mut_ptr() as _,
                    buf.len(),
                    verbs::ACCESS_FLAGS as _,
                )
            };
            if mr.is_null() {
                return Err(ErrorKind::IBRegMemoryRegionFail.with_errno());
            }
            memory_regions.push(RawMemoryRegion(mr));
        }
        Ok(Self {
            memory_regions,
            aligned_buffer: buf,
            _devices: devices.clone(),
        })
    }

    /// Returns the local key for the specified device.
    ///
    /// The lkey is used for local RDMA operations.
    ///
    /// # Arguments
    ///
    /// * `index` - Device index
    ///
    /// # Returns
    ///
    /// The local key for this buffer on the specified device.
    pub fn lkey(&self, index: usize) -> u32 {
        self.memory_regions[index].lkey
    }

    /// Returns the remote key for the specified device.
    ///
    /// The rkey is used for remote RDMA operations.
    ///
    /// # Arguments
    ///
    /// * `index` - Device index
    ///
    /// # Returns
    ///
    /// The remote key for this buffer on the specified device.
    pub fn rkey(&self, index: usize) -> u32 {
        self.memory_regions[index].lkey
    }
}

impl std::ops::Deref for RegisteredBuffer {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.aligned_buffer
    }
}

impl std::fmt::Debug for RegisteredBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredBuffer")
            .field("addr", &self.aligned_buffer.as_ptr())
            .field("len", &self.aligned_buffer.len())
            .field("num_mrs", &self.memory_regions.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_region() {
        let size = 4096usize;
        let devices = Devices::availables().unwrap();
        let registered_buffer = RegisteredBuffer::create(&devices, size).unwrap();
        assert_eq!(registered_buffer.len(), size);
        println!("{:#?}", registered_buffer);
    }
}
