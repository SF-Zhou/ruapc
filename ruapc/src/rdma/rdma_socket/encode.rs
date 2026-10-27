//! Serialize once, growing registered storage without re-running user Serialize.

use std::{io::Write, sync::Arc};

use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::RdmaSocket;
use crate::{Buffer, BufferPool, Error, ErrorKind, MsgMeta, Result, msg::SendMsg};

pub(super) struct Encoded {
    pub(super) buffer: Buffer,
    pub(super) payload_offset: usize,
    pub(super) permit: Option<OwnedSemaphorePermit>,
}

impl Encoded {
    /// Refresh the remaining budget after queue admission without invoking
    /// user serialization again. Only timeout_ms changes, and its encoding
    /// can only shrink as the remaining request budget decreases.
    pub(super) fn refresh_timeout(&mut self, meta: &MsgMeta) -> Result<()> {
        let metadata = rmp_serde::to_vec_named(meta)?;
        let payload_offset = 8 + metadata.len();
        if payload_offset > self.payload_offset {
            return Err(Error::new(
                ErrorKind::SerializeFailed,
                "RDMA timeout refresh grew message metadata".into(),
            ));
        }
        let len = self.buffer.len();
        self.buffer
            .copy_within(self.payload_offset..len, payload_offset);
        self.buffer
            .set_len(len - (self.payload_offset - payload_offset));
        self.buffer[8..payload_offset].copy_from_slice(&metadata);
        let meta_len = u32::try_from(metadata.len())?;
        let frame_len = u32::try_from(self.buffer.len() - 4)?;
        self.buffer[4..8].copy_from_slice(&meta_len.to_be_bytes());
        self.buffer[..4].copy_from_slice(&frame_len.to_be_bytes());
        self.payload_offset = payload_offset;
        Ok(())
    }
}

struct GrowingMessage<'a> {
    encoded: Encoded,
    pool: &'a Arc<BufferPool>,
    budget: &'a Arc<Semaphore>,
    inline_limit: usize,
    limit: usize,
}

impl RdmaSocket {
    pub(super) fn serialize_msg<P: Serialize>(
        &self,
        meta: &MsgMeta,
        payload: &P,
    ) -> Result<Encoded> {
        let mut message = GrowingMessage {
            encoded: Encoded {
                buffer: self.rdmabuf_pool.allocate(4096.min(self.max_msg_size))?,
                payload_offset: 0,
                permit: None,
            },
            pool: &self.rdmabuf_pool,
            budget: &self.large.send_bytes,
            inline_limit: self.max_msg_size,
            limit: self.max_msg_size.max(self.max_large_msg_size),
        };
        meta.serialize_to(payload, &mut message)?;
        Ok(message.encoded)
    }
}

impl Write for GrowingMessage<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.write_all(bytes)?;
        Ok(bytes.len())
    }

    #[inline]
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let inline_capacity = self
            .encoded
            .buffer
            .capacity()
            .min(self.inline_limit)
            .min(self.limit);
        if bytes.len() <= inline_capacity.saturating_sub(self.encoded.buffer.len()) {
            return self.encoded.buffer.extend_from_slice(bytes);
        }
        self.write_slow(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl GrowingMessage<'_> {
    // Keep growth and byte-budget admission out of the small-message writer.
    #[cold]
    fn write_slow(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let end = self
            .encoded
            .buffer
            .len()
            .checked_add(bytes.len())
            .filter(|end| *end <= self.limit)
            .ok_or_else(|| {
                std::io::Error::other("message exceeds negotiated RDMA message limit")
            })?;
        if end > self.encoded.buffer.capacity() {
            let size = end
                .max(self.encoded.buffer.capacity().saturating_mul(4))
                .min(self.limit);
            let capacity = BufferPool::allocation_capacity(size)?;
            // Both the old and new allocation coexist while copying. Reserve
            // the entire replacement before allocating, then return the old
            // permit only once its buffer has been released.
            let replacement_permit = if end > self.inline_limit {
                Some(
                    self.budget
                        .clone()
                        .try_acquire_many_owned(
                            u32::try_from(capacity).map_err(std::io::Error::other)?,
                        )
                        .map_err(|_| {
                            std::io::Error::other("RDMA large-message send byte budget exhausted")
                        })?,
                )
            } else {
                None
            };
            let mut replacement = self.pool.allocate(size).map_err(std::io::Error::other)?;
            replacement.set_len(0);
            replacement.extend_from_slice(&self.encoded.buffer)?;
            self.encoded.buffer = replacement;
            self.encoded.permit = replacement_permit;
        } else if end > self.inline_limit {
            self.reserve(self.encoded.buffer.capacity())?;
        }
        self.encoded.buffer.extend_from_slice(bytes)
    }

    fn reserve(&mut self, capacity: usize) -> std::io::Result<()> {
        let held = self
            .encoded
            .permit
            .as_ref()
            .map_or(0, OwnedSemaphorePermit::num_permits);
        if capacity > held {
            let additional = u32::try_from(capacity - held).map_err(std::io::Error::other)?;
            let permit = self
                .budget
                .clone()
                .try_acquire_many_owned(additional)
                .map_err(|_| {
                    std::io::Error::other("RDMA large-message send byte budget exhausted")
                })?;
            match &mut self.encoded.permit {
                Some(held) => held.merge(permit),
                slot @ None => *slot = Some(permit),
            }
        }
        Ok(())
    }
}

impl SendMsg for GrowingMessage<'_> {
    type Error = Error;

    fn size(&self) -> usize {
        self.encoded.buffer.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.encoded.buffer.set_len(0);
        self.write_all(&[0; 4])?;
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        self.encoded.buffer.finish(meta_offset, payload_offset)?;
        let frame_len = u32::try_from(self.encoded.buffer.len() - 4)?;
        self.encoded.buffer[..4].copy_from_slice(&frame_len.to_be_bytes());
        self.encoded.payload_offset = payload_offset;
        Ok(())
    }

    fn writer(&mut self) -> impl Write {
        self
    }
}

#[cfg(test)]
mod tests;
