//! Serialize once into registered storage without re-running user Serialize.
//!
//! Inline messages grow one contiguous buffer up to the inline limit. Past it,
//! the message is large: later bytes go to appended chunks instead of being
//! copied into ever larger allocations, and each chunk becomes one region of
//! the large-payload offer.

use std::{io::Write, sync::Arc};

use ruapc_wire::frame::{HEADER_LEN, encode_header};
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::RdmaSocket;
use crate::{Buffer, BufferPool, Error, ErrorKind, MsgMeta, Result, msg::SendMsg};

/// Smallest and largest chunk appended for small writes to a large message.
/// Chunk sizes follow the encoded length, so a message at the 64 MiB limit
/// needs at most about 20 offer regions (well under the 64-region space
/// limit); one larger write gets a single chunk of its own size.
const MIN_CHUNK: usize = 64 * 1024;
const MAX_CHUNK: usize = 4 * 1024 * 1024;

pub(super) struct Encoded {
    /// Frame header, metadata and the payload's beginning.
    pub(super) buffer: Buffer,
    /// Payload continuation of a large message, in order. Always empty for
    /// messages within the inline limit.
    pub(super) chunks: Vec<Buffer>,
    pub(super) payload_offset: usize,
    pub(super) permit: Option<OwnedSemaphorePermit>,
}

impl Encoded {
    /// Complete framed length across every chunk.
    pub(super) fn len(&self) -> usize {
        self.buffer.len() + self.chunks.iter().map(Buffer::len).sum::<usize>()
    }

    /// Refresh the remaining budget after queue admission without invoking
    /// user serialization again. Only timeout_ms changes, and its encoding
    /// can only shrink as the remaining request budget decreases.
    pub(super) fn refresh_timeout(&mut self, meta: &MsgMeta) -> Result<()> {
        // Only inline sends refresh in place; a chunked message is transferred
        // by the offer RPC, which carries its metadata separately.
        debug_assert!(self.chunks.is_empty());
        // Typical metadata fits on the stack; this runs for every
        // window-blocked send whose remaining budget changed while queued.
        let mut stack = [0u8; 512];
        let mut cursor = std::io::Cursor::new(&mut stack[..]);
        let heap;
        let metadata: &[u8] = if rmp_serde::encode::write_named(&mut cursor, meta).is_ok() {
            let len = cursor.position() as usize;
            &stack[..len]
        } else {
            heap = rmp_serde::to_vec_named(meta)?;
            &heap
        };
        let payload_offset = HEADER_LEN + 4 + metadata.len();
        if payload_offset > self.payload_offset {
            return Err(Error::new(
                ErrorKind::SerializeFailed,
                "RDMA timeout refresh grew message metadata".into(),
            ));
        }
        if payload_offset < self.payload_offset {
            // Only a narrower integer encoding shifts the payload.
            let len = self.buffer.len();
            self.buffer
                .copy_within(self.payload_offset..len, payload_offset);
            self.buffer
                .set_len(len - (self.payload_offset - payload_offset));
            let meta_len = u32::try_from(metadata.len())?;
            self.buffer[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&meta_len.to_be_bytes());
            let header = encode_header(self.buffer.len() - HEADER_LEN)?;
            self.buffer[..HEADER_LEN].copy_from_slice(&header);
            self.payload_offset = payload_offset;
        }
        self.buffer[HEADER_LEN + 4..payload_offset].copy_from_slice(metadata);
        Ok(())
    }
}

struct GrowingMessage<'a> {
    encoded: Encoded,
    pool: &'a Arc<BufferPool>,
    budget: &'a Arc<Semaphore>,
    inline_limit: usize,
    limit: usize,
    /// Bytes the fast path may append to the current chunk without growth,
    /// budget or limit checks. Recomputed by the slow path.
    room: usize,
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
                chunks: Vec::new(),
                payload_offset: 0,
                permit: None,
            },
            pool: &self.rdmabuf_pool,
            budget: &self.large.send_bytes,
            inline_limit: self.max_msg_size,
            limit: self.max_msg_size.max(self.max_large_msg_size),
            room: 0,
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
        if bytes.len() <= self.room {
            self.room -= bytes.len();
            return self.current().extend_from_slice(bytes);
        }
        self.write_slow(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl GrowingMessage<'_> {
    fn current(&mut self) -> &mut Buffer {
        self.encoded
            .chunks
            .last_mut()
            .unwrap_or(&mut self.encoded.buffer)
    }

    fn refresh_room(&mut self) {
        let len = self.encoded.len();
        let bound = if self.encoded.permit.is_some() {
            self.limit
        } else {
            self.inline_limit.min(self.limit)
        };
        let current = self.current();
        self.room = (current.capacity() - current.len()).min(bound.saturating_sub(len));
    }

    fn reserve_budget(&self, bytes: usize) -> std::io::Result<OwnedSemaphorePermit> {
        self.budget
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes).map_err(std::io::Error::other)?)
            .map_err(|_| std::io::Error::other("RDMA large-message send byte budget exhausted"))
    }

    // Keep growth and byte-budget admission out of the small-message writer.
    // Every failure leaves the encoded bytes, chunks and budget unchanged.
    #[cold]
    fn write_slow(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let len = self.encoded.len();
        let end = len
            .checked_add(bytes.len())
            .filter(|end| *end <= self.limit)
            .ok_or_else(|| {
                std::io::Error::other("message exceeds negotiated RDMA message limit")
            })?;
        if self.encoded.permit.is_none() && end <= self.inline_limit {
            // Inline messages stay one contiguous SEND buffer.
            let capacity = self.encoded.buffer.capacity();
            if end > capacity {
                let size = end.max(capacity.saturating_mul(4)).min(self.inline_limit);
                let mut replacement = self.pool.allocate(size).map_err(std::io::Error::other)?;
                replacement.set_len(0);
                replacement.extend_from_slice(&self.encoded.buffer)?;
                self.encoded.buffer = replacement;
            }
            self.encoded.buffer.extend_from_slice(bytes)?;
            self.refresh_room();
            return Ok(());
        }
        // Large message. Its first crossing charges the existing buffer; all
        // storage is then held until the source settles.
        let mut permit = match self.encoded.permit.is_none() {
            true => Some(self.reserve_budget(self.encoded.buffer.capacity())?),
            false => None,
        };
        let current = self.current();
        let mut space = current.capacity() - current.len();
        let mut capacity = len + space;
        let mut added = Vec::new();
        while space < bytes.len() {
            // A single large write gets one chunk covering it: fewer regions
            // and READ WRs, and still no copy of earlier bytes.
            let size = capacity
                .clamp(MIN_CHUNK, MAX_CHUNK)
                .max(bytes.len() - space)
                .min(self.limit - capacity.min(self.limit))
                .max(1);
            let chunk_permit = self.reserve_budget(BufferPool::allocation_capacity(size)?)?;
            let mut chunk = self.pool.allocate(size).map_err(std::io::Error::other)?;
            chunk.set_len(0);
            space += chunk.capacity();
            capacity += chunk.capacity();
            match &mut permit {
                Some(held) => held.merge(chunk_permit),
                slot @ None => *slot = Some(chunk_permit),
            }
            added.push(chunk);
        }
        match (&mut self.encoded.permit, permit) {
            (Some(held), Some(permit)) => held.merge(permit),
            (slot @ None, permit) => *slot = permit,
            (Some(_), None) => {}
        }
        let mut rest = bytes;
        let current = self.current();
        let take = (current.capacity() - current.len()).min(rest.len());
        current.extend_from_slice(&rest[..take])?;
        rest = &rest[take..];
        for mut chunk in added {
            let take = chunk.capacity().min(rest.len());
            chunk.extend_from_slice(&rest[..take])?;
            rest = &rest[take..];
            self.encoded.chunks.push(chunk);
        }
        debug_assert!(rest.is_empty());
        self.refresh_room();
        Ok(())
    }
}

impl SendMsg for GrowingMessage<'_> {
    type Error = Error;

    fn size(&self) -> usize {
        self.encoded.len()
    }

    fn prepare(&mut self) -> Result<()> {
        self.encoded.buffer.set_len(0);
        self.encoded.chunks.clear();
        self.encoded.permit = None;
        self.room = 0;
        self.write_all(&[0; HEADER_LEN])?;
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<()> {
        // Metadata, and therefore its backfilled length, must lie in the
        // first chunk; it is far smaller than the inline limit in practice.
        if meta_offset != HEADER_LEN || payload_offset > self.encoded.buffer.len() {
            return Err(ErrorKind::InvalidArgument.into());
        }
        self.encoded.buffer.finish(meta_offset, payload_offset)?;
        let header = encode_header(self.encoded.len() - HEADER_LEN)?;
        self.encoded.buffer[..HEADER_LEN].copy_from_slice(&header);
        self.encoded.payload_offset = payload_offset;
        Ok(())
    }

    fn writer(&mut self) -> impl Write {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use ruapc_wire::frame;
    use serde::{Serializer, ser::SerializeSeq};

    use crate::{Message, MsgFlags};

    const INLINE: usize = 16 * 1024;

    fn concatenated(encoded: &Encoded) -> Vec<u8> {
        let mut bytes = encoded.buffer.to_vec();
        for chunk in &encoded.chunks {
            bytes.extend_from_slice(chunk);
        }
        bytes
    }

    fn held_capacity(encoded: &Encoded) -> usize {
        encoded.buffer.capacity() + encoded.chunks.iter().map(Buffer::capacity).sum::<usize>()
    }

    fn pool() -> Arc<BufferPool> {
        ruapc_bufpool::BufferPoolBuilder::new(Vec::new()).build()
    }

    fn message<'a>(
        pool: &'a Arc<BufferPool>,
        budget: &'a Arc<Semaphore>,
        inline_limit: usize,
        limit: usize,
    ) -> GrowingMessage<'a> {
        let mut buffer = pool.allocate(64).unwrap();
        buffer.set_len(0);
        GrowingMessage {
            encoded: Encoded {
                buffer,
                chunks: Vec::new(),
                payload_offset: 0,
                permit: None,
            },
            pool,
            budget,
            inline_limit,
            limit,
            room: 0,
        }
    }

    struct CountedPayload<'a> {
        calls: &'a AtomicUsize,
        chunks: &'a [String],
    }

    impl Serialize for CountedPayload<'_> {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut sequence = serializer.serialize_seq(Some(self.chunks.len()))?;
            for chunk in self.chunks {
                sequence.serialize_element(chunk)?;
            }
            sequence.end()
        }
    }

    #[test]
    fn finish_rejects_invalid_metadata_without_mutating_encoded_state() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(128 * 1024));
        for bytes in [&[][..], &[42; 16][..]] {
            let mut message = message(&pool, &budget, INLINE, 256 * 1024);
            message.write_all(bytes).unwrap();
            message.encoded.payload_offset = 11;

            for (meta_offset, payload_offset) in [
                (4, 7),
                (4, 8),
                (HEADER_LEN, HEADER_LEN + 3),
                (HEADER_LEN, HEADER_LEN + 4),
                (8, 4),
                (4, bytes.len() + 1),
                (usize::MAX, usize::MAX),
            ] {
                assert_eq!(
                    message
                        .finish(meta_offset, payload_offset)
                        .unwrap_err()
                        .kind,
                    ErrorKind::InvalidArgument,
                    "offsets ({meta_offset}, {payload_offset}) in {} bytes",
                    bytes.len(),
                );
                assert_eq!(&message.encoded.buffer[..], bytes);
                assert_eq!(message.encoded.payload_offset, 11);
            }
        }
    }

    #[test]
    fn large_messages_append_chunks_without_serializing_user_value_again() {
        let pool = pool();
        for use_msgpack in [true, false] {
            let budget = Arc::new(Semaphore::new(512 * 1024));
            let mut message = message(&pool, &budget, 16 * 1024, 256 * 1024);
            let calls = AtomicUsize::new(0);
            let chunks: Vec<_> = (0..256)
                .map(|index| format!("{index}:\"\\\n{}", "payload".repeat(64)))
                .collect();
            let value = CountedPayload {
                calls: &calls,
                chunks: &chunks,
            };
            let meta = MsgMeta {
                method: "LargePayload/test".into(),
                flags: MsgFlags::IsReq
                    | if use_msgpack {
                        MsgFlags::UseMessagePack
                    } else {
                        MsgFlags::empty()
                    },
                msgid: 19,
                timeout_ms: 1234,
                ..Default::default()
            };
            meta.serialize_to(&value, &mut message).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);

            let encoded = &message.encoded;
            // The first buffer stopped at the inline limit; nothing was copied
            // into larger allocations.
            assert_eq!(encoded.buffer.len(), 16 * 1024);
            assert!(!encoded.chunks.is_empty());
            let bytes = concatenated(encoded);
            assert_eq!(bytes.len(), encoded.len());
            assert_eq!(
                u32::from_be_bytes(bytes[4..HEADER_LEN].try_into().unwrap()) as usize,
                bytes.len() - HEADER_LEN
            );
            assert_eq!(bytes, frame::encode(&meta, &chunks).unwrap().as_ref());
            let meta_len =
                u32::from_be_bytes(bytes[HEADER_LEN..HEADER_LEN + 4].try_into().unwrap()) as usize;
            assert_eq!(encoded.payload_offset, HEADER_LEN + 4 + meta_len);
            let decoded = Message::parse(Bytes::copy_from_slice(&bytes[HEADER_LEN..])).unwrap();
            assert_eq!(decoded.meta, meta);
            assert_eq!(
                decoded.payload.deserialize::<Vec<String>>(&meta).unwrap(),
                chunks
            );
            let held = held_capacity(encoded);
            assert_eq!(encoded.permit.as_ref().unwrap().num_permits(), held);
            assert_eq!(budget.available_permits() + held, 512 * 1024);
            drop(message);
            assert_eq!(budget.available_permits(), 512 * 1024);
        }
    }

    #[test]
    fn exact_inline_limit_keeps_fast_path_and_next_byte_starts_a_chunk() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(128 * 1024));
        let mut message = message(&pool, &budget, INLINE, 256 * 1024);
        message.write_all(&[17; INLINE]).unwrap();
        assert_eq!(message.encoded.buffer.len(), INLINE);
        assert!(message.encoded.permit.is_none());
        assert_eq!(budget.available_permits(), 128 * 1024);

        message.write_all(&[18]).unwrap();
        assert_eq!(message.encoded.buffer.len(), INLINE);
        assert_eq!(message.encoded.chunks.len(), 1);
        assert_eq!(&message.encoded.chunks[0][..], &[18]);
        assert_eq!(
            message.encoded.permit.as_ref().unwrap().num_permits(),
            held_capacity(&message.encoded)
        );
        // Later bytes take the fast path into the new chunk.
        message.write_all(&[19; 100]).unwrap();
        assert_eq!(message.encoded.chunks.len(), 1);
        assert_eq!(message.encoded.len(), INLINE + 101);
    }

    #[test]
    fn one_write_larger_than_max_chunk_gets_a_single_chunk() {
        let pool = pool();
        // One 5 MiB serializer write, above MAX_CHUNK (4 MiB), under a 6 MiB
        // negotiated limit. It must land in exactly one chunk instead of being
        // split, and the allocation is rounded up to the pool size class.
        let budget = Arc::new(Semaphore::new(32 * 1024 * 1024));
        let mut message = message(&pool, &budget, INLINE, 6 * 1024 * 1024);
        let payload = vec![0x5a; 5 * 1024 * 1024];
        message.write_all(&payload).unwrap();
        let encoded = &message.encoded;
        assert_eq!(encoded.chunks.len(), 1);
        assert!(encoded.chunks[0].capacity() > MAX_CHUNK);
        assert_eq!(concatenated(encoded), payload);
        let held = held_capacity(encoded);
        assert_eq!(encoded.permit.as_ref().unwrap().num_permits(), held);
        assert_eq!(budget.available_permits() + held, 32 * 1024 * 1024);
        drop(message);
        assert_eq!(budget.available_permits(), 32 * 1024 * 1024);
    }

    #[test]
    fn later_large_writes_merge_into_the_held_reservation() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(4 * 1024 * 1024));
        let mut message = message(&pool, &budget, INLINE, 4 * 1024 * 1024);
        message.write_all(&[1; INLINE]).unwrap();
        // Each later write crosses the current chunk and must merge its byte
        // permit into the reservation the message already holds.
        for _ in 0..3 {
            message.write_all(&[2; 64 * 1024]).unwrap();
        }
        let encoded = &message.encoded;
        assert_eq!(encoded.len(), INLINE + 3 * 64 * 1024);
        assert!(!encoded.chunks.is_empty());
        let held = held_capacity(encoded);
        assert_eq!(encoded.permit.as_ref().unwrap().num_permits(), held);
        assert_eq!(budget.available_permits() + held, 4 * 1024 * 1024);
        assert_eq!(concatenated(encoded).len(), INLINE + 3 * 64 * 1024);
        drop(message);
        assert_eq!(budget.available_permits(), 4 * 1024 * 1024);
    }

    #[test]
    fn exact_negotiated_limit_is_accepted_and_overflow_does_not_change_message() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(128 * 1024));
        let mut message = message(&pool, &budget, INLINE, 64 * 1024);
        message.write_all(&[73; 64 * 1024]).unwrap();
        let held = message.encoded.permit.as_ref().unwrap().num_permits();
        assert!(message.write_all(&[74]).is_err());
        assert_eq!(concatenated(&message.encoded), vec![73; 64 * 1024]);
        assert_eq!(budget.available_permits(), 128 * 1024 - held);
        drop(message);
        assert_eq!(budget.available_permits(), 128 * 1024);
    }

    #[test]
    fn peer_without_rendezvous_capability_cannot_exceed_inline_limit() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(128 * 1024));
        let mut message = message(&pool, &budget, INLINE, INLINE);
        message.write_all(&[29; INLINE]).unwrap();
        assert!(message.write_all(&[30]).is_err());
        assert_eq!(&message.encoded.buffer[..], &[29; INLINE]);
        assert!(message.encoded.permit.is_none());
        assert_eq!(budget.available_permits(), 128 * 1024);
    }

    #[test]
    fn failed_chunk_reservation_preserves_existing_bytes_and_budget() {
        let pool = pool();
        // Exactly the first buffer plus one minimum chunk.
        let budget = Arc::new(Semaphore::new(INLINE + MIN_CHUNK));
        let mut message = message(&pool, &budget, INLINE, 256 * 1024);
        message.write_all(&[43; INLINE]).unwrap();
        message.write_all(&[44]).unwrap();
        assert_eq!(budget.available_permits(), 0);
        assert!(message.write_all(&[45; MIN_CHUNK]).is_err());
        let mut expected = vec![43; INLINE];
        expected.push(44);
        assert_eq!(concatenated(&message.encoded), expected);
        assert_eq!(message.encoded.chunks.len(), 1);
        assert_eq!(budget.available_permits(), 0);
        drop(message);
        assert_eq!(budget.available_permits(), INLINE + MIN_CHUNK);
    }

    #[test]
    fn first_large_allocation_failure_does_not_hold_partial_budget() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(INLINE));
        let mut message = message(&pool, &budget, INLINE, 256 * 1024);
        message.write_all(&[11; INLINE]).unwrap();
        assert!(message.write_all(&[12]).is_err());
        assert_eq!(message.encoded.buffer.len(), INLINE);
        assert!(message.encoded.permit.is_none());
        assert_eq!(budget.available_permits(), INLINE);
    }

    #[test]
    fn refreshing_timeout_preserves_payload_across_messagepack_integer_widths() {
        let pool = pool();
        for use_msgpack in [true, false] {
            let budget = Arc::new(Semaphore::new(128 * 1024));
            let mut message = message(&pool, &budget, INLINE, 256 * 1024);
            let calls = AtomicUsize::new(0);
            let chunks = vec!["payload with \"quotes\", \\slashes and \nnewlines".repeat(16)];
            let value = CountedPayload {
                calls: &calls,
                chunks: &chunks,
            };
            let mut meta = MsgMeta {
                method: "Timeout/test".into(),
                flags: MsgFlags::IsReq
                    | if use_msgpack {
                        MsgFlags::UseMessagePack
                    } else {
                        MsgFlags::empty()
                    },
                msgid: 27,
                timeout_ms: u32::MAX,
                ..Default::default()
            };
            meta.serialize_to(&value, &mut message).unwrap();
            let original_payload =
                message.encoded.buffer[message.encoded.payload_offset..].to_vec();

            for timeout in [u32::MAX - 1, 65536, 65535, 256, 255, 128, 127, 1, 0] {
                meta.timeout_ms = timeout;
                message.encoded.refresh_timeout(&meta).unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                let buffer = &message.encoded.buffer;
                assert_eq!(
                    &buffer[message.encoded.payload_offset..],
                    original_payload.as_slice()
                );
                assert_eq!(
                    u32::from_be_bytes(buffer[4..HEADER_LEN].try_into().unwrap()) as usize,
                    buffer.len() - HEADER_LEN
                );
                assert_eq!(&buffer[..4], &frame::MAGIC_NUM.to_be_bytes());
                assert_eq!(
                    buffer.as_ref(),
                    frame::encode(&meta, &chunks).unwrap().as_ref()
                );
                let meta_len =
                    u32::from_be_bytes(buffer[HEADER_LEN..HEADER_LEN + 4].try_into().unwrap())
                        as usize;
                assert_eq!(message.encoded.payload_offset, HEADER_LEN + 4 + meta_len);
                let decoded =
                    Message::parse(Bytes::copy_from_slice(&buffer[HEADER_LEN..])).unwrap();
                assert_eq!(decoded.meta, meta);
                assert_eq!(
                    decoded.payload.deserialize::<Vec<String>>(&meta).unwrap(),
                    chunks
                );
            }
            assert!(message.encoded.permit.is_none());
            assert_eq!(budget.available_permits(), 128 * 1024);
        }
    }

    #[test]
    fn metadata_growth_during_timeout_refresh_preserves_encoded_message() {
        let pool = pool();
        let budget = Arc::new(Semaphore::new(128 * 1024));
        let mut message = message(&pool, &budget, INLINE, 256 * 1024);
        let mut meta = MsgMeta {
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            timeout_ms: 1,
            ..Default::default()
        };
        meta.serialize_to(&"payload", &mut message).unwrap();
        let original = message.encoded.buffer.to_vec();
        let payload_offset = message.encoded.payload_offset;
        meta.timeout_ms = u32::MAX;
        assert_eq!(
            message.encoded.refresh_timeout(&meta).unwrap_err().kind,
            ErrorKind::SerializeFailed
        );
        assert_eq!(&message.encoded.buffer[..], original.as_slice());
        assert_eq!(message.encoded.payload_offset, payload_offset);
    }
}
