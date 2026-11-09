//! Stable logical lengths and CPU access for owned buffers and transport views.

use ruapc_bufpool::{Buffer, WriteBuffer};

mod sealed {
    pub trait Sealed {}
}

/// A buffer or transport view whose length cannot change through shared access.
///
/// Implementations are restricted to the pool's buffer types so a cached
/// [`crate::SpaceLayout`] cannot become stale while its collection owns them.
/// Reading the logical length does not access the payload or acquire permission
/// to access it. Payload access follows the ownership contract of the backing buffer.
pub trait BufferView: sealed::Sealed {
    /// Returns the logical byte length, without accessing the payload.
    fn len(&self) -> usize;

    /// Returns whether the logical byte length is zero.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows the initialized logical payload under this value's ownership contract.
    fn as_slice(&self) -> &[u8];

    /// Returns registered region metadata without accessing payload bytes.
    fn remote_buffer_info(
        &self,
        registrar_index: usize,
    ) -> std::io::Result<ruapc_bufpool::RemoteBufferInfo>;
}

/// A buffer or exclusive transport view that permits synchronous CPU writes.
///
/// The mutable slice cannot change the logical length used by a cached layout.
pub trait BufferViewMut: BufferView {
    /// Mutably borrows the initialized logical payload.
    fn as_mut_slice(&mut self) -> &mut [u8];
}

macro_rules! readable {
    ($($buffer:ty),+ $(,)?) => {
        $(
            impl sealed::Sealed for $buffer {}

            impl BufferView for $buffer {
                fn len(&self) -> usize {
                    <$buffer>::len(self)
                }

                fn as_slice(&self) -> &[u8] {
                    self.as_ref()
                }

                fn remote_buffer_info(
                    &self,
                    registrar_index: usize,
                ) -> std::io::Result<ruapc_bufpool::RemoteBufferInfo> {
                    <$buffer>::remote_buffer_info(self, registrar_index)
                }
            }
        )+
    };
}

readable!(Buffer, WriteBuffer);

impl BufferViewMut for Buffer {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl BufferViewMut for WriteBuffer {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}
