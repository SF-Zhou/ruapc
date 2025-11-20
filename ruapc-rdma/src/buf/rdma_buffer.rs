use crate::*;

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

/// A registered buffer that can be used for RDMA operations.
pub struct RegisteredBuffer {
    memory_regions: Vec<RawMemoryRegion>,
    aligned_buffer: AlignedBuffer,
    _devices: Devices,
}

impl RegisteredBuffer {
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
                return Err(ErrorKind::IBRegMemoryRegionFail.into());
            }
            memory_regions.push(RawMemoryRegion(mr));
        }
        Ok(Self {
            memory_regions,
            aligned_buffer: buf,
            _devices: devices.clone(),
        })
    }

    pub fn lkey(&self, index: usize) -> u32 {
        self.memory_regions[index].lkey
    }

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
