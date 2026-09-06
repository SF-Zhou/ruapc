//! Immutable, owned source buffers shared by a client and its pending reads.

use crate::{Buffer, CopyOp, Result};

use super::scatter::{self, SpaceLayout};

#[derive(Debug)]
pub(crate) struct ReadSource {
    pub(crate) buffers: Vec<Buffer>,
}

impl ReadSource {
    /// Copy directly from the request's own buffers. No peer-provided address
    /// or registration key can select a different allocation.
    pub(crate) fn read_inline(&self, ops: &[CopyOp]) -> Result<Vec<u8>> {
        let layout = SpaceLayout::from_lens(self.buffers.iter().map(|buffer| buffer.len() as u64))?;
        let total = scatter::validate_ops(ops, layout.total(), u64::MAX)?;
        let mut bytes = Vec::with_capacity(usize::try_from(total)?);
        for op in ops {
            layout.for_each_slice::<crate::Error>(
                op.src_offset,
                op.len,
                |segment, offset, len| {
                    let offset = usize::try_from(offset)?;
                    let len = usize::try_from(len)?;
                    bytes.extend_from_slice(&self.buffers[segment][offset..offset + len]);
                    Ok(())
                },
            )?;
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn reads_cross_buffer_boundaries_and_rejects_out_of_bounds() {
        let pool = ruapc_bufpool::BufferPool::new(Arc::new(ruapc_bufpool::EmptyDevices));
        let buffers = [b"abcd", b"efgh"]
            .into_iter()
            .map(|bytes| {
                let mut buffer = pool.allocate(bytes.len()).unwrap();
                buffer.set_len(bytes.len());
                buffer.copy_from_slice(bytes);
                buffer
            })
            .collect();
        let source = ReadSource { buffers };
        assert_eq!(
            source
                .read_inline(&[CopyOp::new(2, 0, 4), CopyOp::new(0, 4, 2)])
                .unwrap(),
            b"cdefab"
        );
        assert_eq!(
            source
                .read_inline(&[CopyOp::new(7, 0, 2)])
                .unwrap_err()
                .kind,
            crate::ErrorKind::InvalidCopyOp
        );
    }
}
