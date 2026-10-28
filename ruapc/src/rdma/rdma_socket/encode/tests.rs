use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use ruapc_wire::frame;
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
            u32::from_be_bytes(buffer[4..HEADER_LEN].try_into().unwrap()) as usize,
            buffer.len() - HEADER_LEN
        );
        assert_eq!(&buffer[..4], &frame::MAGIC_NUM.to_be_bytes());
        assert_eq!(
            buffer.as_ref(),
            frame::encode(&meta, &chunks).unwrap().as_ref()
        );
        let meta_len =
            u32::from_be_bytes(buffer[HEADER_LEN..HEADER_LEN + 4].try_into().unwrap()) as usize;
        assert_eq!(message.encoded.payload_offset, HEADER_LEN + 4 + meta_len);
        let decoded = Message::parse(Bytes::copy_from_slice(&buffer[HEADER_LEN..])).unwrap();
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
        let original_payload = message.encoded.buffer[message.encoded.payload_offset..].to_vec();

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
                u32::from_be_bytes(buffer[HEADER_LEN..HEADER_LEN + 4].try_into().unwrap()) as usize;
            assert_eq!(message.encoded.payload_offset, HEADER_LEN + 4 + meta_len);
            let decoded = Message::parse(Bytes::copy_from_slice(&buffer[HEADER_LEN..])).unwrap();
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
