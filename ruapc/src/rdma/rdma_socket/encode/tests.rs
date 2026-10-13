use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use serde::{Serializer, ser::SerializeSeq};

use crate::{Devices, Message, MsgFlags};

const INLINE: usize = 16 * 1024;

fn pool() -> Arc<BufferPool> {
    ruapc_bufpool::BufferPoolBuilder::new(Arc::new(Devices::default())).build()
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
            payload_offset: 0,
            permit: None,
        },
        pool,
        budget,
        inline_limit,
        limit,
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
fn grows_across_multiple_buffers_without_serializing_user_value_again() {
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

        let buffer = &message.encoded.buffer;
        assert!(buffer.len() > 16 * 1024);
        assert_eq!(
            u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize,
            buffer.len() - 4
        );
        let meta_len = u32::from_be_bytes(buffer[4..8].try_into().unwrap()) as usize;
        assert_eq!(message.encoded.payload_offset, 8 + meta_len);
        let decoded = Message::parse(Bytes::copy_from_slice(&buffer[4..])).unwrap();
        assert_eq!(decoded.meta, meta);
        assert_eq!(
            decoded.payload.deserialize::<Vec<String>>(&meta).unwrap(),
            chunks
        );
        assert_eq!(
            message.encoded.permit.as_ref().unwrap().num_permits(),
            buffer.capacity()
        );
        assert_eq!(budget.available_permits() + buffer.capacity(), 512 * 1024);
        drop(message);
        assert_eq!(budget.available_permits(), 512 * 1024);
    }
}

#[test]
fn exact_inline_limit_keeps_fast_path_and_next_byte_reserves_capacity() {
    let pool = pool();
    let budget = Arc::new(Semaphore::new(128 * 1024));
    let mut message = message(&pool, &budget, INLINE, 256 * 1024);
    message.write_all(&[17; INLINE]).unwrap();
    assert_eq!(message.encoded.buffer.len(), INLINE);
    assert!(message.encoded.permit.is_none());
    assert_eq!(budget.available_permits(), 128 * 1024);

    message.write_all(&[18]).unwrap();
    assert_eq!(message.encoded.buffer.len(), INLINE + 1);
    assert_eq!(
        message.encoded.permit.as_ref().unwrap().num_permits(),
        message.encoded.buffer.capacity()
    );
    assert_eq!(&message.encoded.buffer[..INLINE], &[17; INLINE]);
    assert_eq!(message.encoded.buffer[INLINE], 18);
}

#[test]
fn exact_negotiated_limit_is_accepted_and_overflow_does_not_change_message() {
    let pool = pool();
    let budget = Arc::new(Semaphore::new(128 * 1024));
    let mut message = message(&pool, &budget, INLINE, 64 * 1024);
    message.write_all(&[73; 64 * 1024]).unwrap();
    let held = message.encoded.permit.as_ref().unwrap().num_permits();
    assert!(message.write_all(&[74]).is_err());
    assert_eq!(&message.encoded.buffer[..], &[73; 64 * 1024]);
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
fn failed_growth_preserves_existing_bytes_and_releases_budget_on_drop() {
    let pool = pool();
    let budget = Arc::new(Semaphore::new(256 * 1024));
    let mut message = message(&pool, &budget, INLINE, 256 * 1024);
    message.write_all(&[43; INLINE]).unwrap();
    message.write_all(&[44]).unwrap();
    assert_eq!(budget.available_permits(), 192 * 1024);
    // The final 256 KiB allocation fits the total budget, but the old 64 KiB
    // source remains live during copying. Reserving only the capacity delta
    // would incorrectly allow 320 KiB of simultaneous registered storage.
    assert!(message.write_all(&[45; 48 * 1024]).is_err());
    assert_eq!(message.encoded.buffer.len(), INLINE + 1);
    assert_eq!(&message.encoded.buffer[..INLINE], &[43; INLINE]);
    assert_eq!(message.encoded.buffer[INLINE], 44);
    assert_eq!(budget.available_permits(), 192 * 1024);
    drop(message);
    assert_eq!(budget.available_permits(), 256 * 1024);
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
