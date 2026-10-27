use super::*;
use crate::{MsgFlags, SendMsg, deserialize_payload};
use bytes::Bytes;
use bytes::BytesMut;
use serde::Serialize;

fn make_meta(method: &str, use_msgpack: bool) -> MsgMeta {
    let mut flags = MsgFlags::IsReq;
    if use_msgpack {
        flags |= MsgFlags::UseMessagePack;
    }
    MsgMeta {
        method: method.to_string(),
        flags,
        msgid: 42,
        read_regions: Vec::new(),
        write_regions: Vec::new(),
        timeout_ms: 0,
    }
}

/// Serialize `meta` + `payload` into a `BytesMut` and return the bytes.
fn serialize_to_bytes<P: serde::Serialize>(meta: &MsgMeta, payload: &P) -> BytesMut {
    let mut buf = BytesMut::new();
    meta.serialize_to(payload, &mut buf).unwrap();
    buf
}

#[test]
fn test_msgflags_is_req_is_rsp() {
    let mut meta = MsgMeta::default();
    assert!(!meta.is_req());
    assert!(!meta.is_rsp());

    meta.flags = MsgFlags::IsReq;
    assert!(meta.is_req());
    assert!(!meta.is_rsp());

    meta.flags = MsgFlags::IsRsp;
    assert!(!meta.is_req());
    assert!(meta.is_rsp());
}

#[test]
fn test_meta_roundtrip_with_regions() {
    let region = ruapc_bufpool::RemoteBufferInfo {
        key: ruapc_bufpool::MemoryKey {
            lkey: 0x1122_3344,
            rkey: 0x5566_7788,
        },
        addr: 0xdead_beef_cafe_f00d,
        len: 1 << 40,
    };
    let meta = MsgMeta {
        method: "_ruapc.memory/read_into_target".into(),
        flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
        msgid: u64::MAX - 1,
        read_regions: vec![region, region],
        write_regions: vec![region],
        timeout_ms: 1500,
    };
    let buf = serialize_to_bytes(&meta, &serde_json::json!({"x": 1}));
    let (decoded, _) = parse_body(&buf).unwrap();
    assert_eq!(decoded, meta);
}

#[test]
fn test_meta_decode_rejects_garbage() {
    // Empty and truncated inputs are not valid MessagePack maps.
    assert!(MsgMeta::decode(&[]).is_err());
    // 0xc1 is reserved ("never used") in MessagePack.
    assert!(MsgMeta::decode(&[0xc1]).is_err());
    // A msgpack array is not a named struct map.
    assert!(MsgMeta::decode(&[0x93, 0x01, 0x02, 0x03]).is_err());
}

#[test]
fn test_meta_decode_defaults_missing_timeout_to_zero() {
    #[derive(Serialize)]
    struct MetaWithoutTimeout {
        method: String,
        flags: MsgFlags,
        msgid: u64,
    }

    let encoded = rmp_serde::to_vec_named(&MetaWithoutTimeout {
        method: "Svc/m".into(),
        flags: MsgFlags::IsReq,
        msgid: 1,
    })
    .unwrap();
    assert_eq!(MsgMeta::decode(&encoded).unwrap().timeout_ms, 0);
}

#[test]
fn test_minimal_response_meta_roundtrip() {
    // Response metas use zero, so timeout_ms is omitted on the wire.
    let meta = MsgMeta {
        method: String::new(),
        flags: MsgFlags::IsRsp,
        msgid: 42,
        read_regions: Vec::new(),
        write_regions: Vec::new(),
        timeout_ms: 0,
    };
    let buf = serialize_to_bytes(&meta, &serde_json::json!({"ok": true}));
    let (decoded, _) = parse_body(&buf).unwrap();
    assert_eq!(decoded, meta);
}

#[test]
fn test_meta_encoding_is_flag_independent() {
    // The meta encoding must not vary with UseMessagePack: it only
    // selects the payload format.
    let mut json_meta = make_meta("Svc/m", false);
    let msgpack_meta = make_meta("Svc/m", true);
    let mut json_buf = Vec::new();
    let mut msgpack_buf = Vec::new();
    json_meta.encode_to(&mut json_buf).unwrap();
    msgpack_meta.encode_to(&mut msgpack_buf).unwrap();
    // Identical except for the flags byte content inside the map; both
    // decode back losslessly.
    json_meta.flags = msgpack_meta.flags;
    json_buf.clear();
    json_meta.encode_to(&mut json_buf).unwrap();
    assert_eq!(json_buf, msgpack_buf);
    assert_eq!(MsgMeta::decode(&msgpack_buf).unwrap(), msgpack_meta);
}

#[test]
fn test_serialize_parse_json_roundtrip() {
    let meta = make_meta("TestService/hello", false);
    let payload_value = serde_json::json!({"key": "value", "num": 123});

    let buf = serialize_to_bytes(&meta, &payload_value);

    let (meta, offset) = parse_body(&buf).unwrap();
    assert_eq!(meta.method, "TestService/hello");
    assert_eq!(meta.msgid, 42);
    assert!(meta.is_req());
    assert!(!meta.flags.contains(MsgFlags::UseMessagePack));

    let recovered: serde_json::Value = deserialize_payload(&buf[offset..], meta.flags).unwrap();
    assert_eq!(recovered, payload_value);
}

#[test]
fn test_serialize_parse_msgpack_roundtrip() {
    let meta = make_meta("TestService/hello", true);
    let payload_str = "hello msgpack";

    let buf = serialize_to_bytes(&meta, &payload_str);

    let (meta, offset) = parse_body(&buf).unwrap();
    assert_eq!(meta.method, "TestService/hello");
    assert!(meta.flags.contains(MsgFlags::UseMessagePack));

    let recovered: String = deserialize_payload(&buf[offset..], meta.flags).unwrap();
    assert_eq!(recovered, payload_str);
}

#[test]
fn test_parse_too_short_returns_error() {
    for len in 0..4 {
        let truncated = Bytes::from(vec![0; len]);
        assert!(parse_body(&truncated).is_err(), "length {len}");
    }
}

#[test]
fn test_parse_zero_meta_len_returns_error() {
    // 4 bytes of zeros => meta_len == 0, which is invalid.
    let buf = Bytes::from_static(&[0u8, 0, 0, 0]);
    assert!(parse_body(&buf).is_err());
}

#[test]
fn test_parse_meta_len_exceeds_payload_returns_error() {
    // meta_len says 100, but total is only 8 bytes.
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&100u32.to_be_bytes()); // meta_len = 100
    buf.extend_from_slice(b"short");
    assert!(parse_body(&buf).is_err());
}

#[test]
fn parsing_leaves_raw_payload_bytes_untouched() {
    let meta = MsgMeta {
        method: "Svc/method".into(),
        flags: MsgFlags::IsRsp,
        msgid: 7,
        ..Default::default()
    };
    let metadata = rmp_serde::to_vec_named(&meta).unwrap();
    let mut body = u32::try_from(metadata.len())
        .unwrap()
        .to_be_bytes()
        .to_vec();
    body.extend_from_slice(&metadata);
    body.extend_from_slice(b"data");
    let original = body.clone();
    let (decoded, offset) = parse_body(&body).unwrap();
    assert_eq!(decoded, meta);
    assert_eq!(offset, 4 + metadata.len());
    assert_eq!(&body[offset..], b"data");
    assert_eq!(body, original);
}

#[test]
fn test_msgmeta_default() {
    let meta = MsgMeta::default();
    assert!(meta.method.is_empty());
    assert_eq!(meta.flags, MsgFlags::default());
    assert_eq!(meta.msgid, 0);
    assert_eq!(meta.timeout_ms, 0);
}

#[test]
fn test_msgflags_serde_roundtrip() {
    let flags = MsgFlags::IsReq | MsgFlags::UseMessagePack;
    let json = serde_json::to_string(&flags).unwrap();
    let recovered: MsgFlags = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered, flags);
}

#[test]
fn test_bytesmut_sendmsg_prepare_clears_without_reallocating() {
    let mut buf = BytesMut::from(&b"existing"[..]);
    let pointer = buf.as_ptr();
    let capacity = buf.capacity();
    buf.prepare().unwrap();
    assert!(buf.is_empty());
    assert_eq!(buf.as_ptr(), pointer);
    assert_eq!(buf.capacity(), capacity);
}

#[test]
fn test_bytesmut_writer_write_and_flush() {
    use std::io::Write as _;
    let mut bm = BytesMut::new();
    {
        let mut w = bm.writer();
        // `write()` calls `write_all()` internally.
        assert_eq!(w.write(b"hello").unwrap(), 5);
        // `flush()` is a no-op but must be reachable.
        w.flush().unwrap();
    }
    assert_eq!(&bm[..], b"hello");
}

#[test]
fn test_buffer_sendmsg_serialize() {
    use ruapc_bufpool::DeviceSet;
    use std::sync::Arc;
    let devices = Arc::new(DeviceSet::<ruapc_bufpool::TcpDevice>::default());
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices).build();
    let mut buf = pool.allocate(1024 * 1024).unwrap();

    let meta = make_meta("SomeService/rpc", false);
    // serialize_to exercises Buffer::prepare, Buffer::writer, and Buffer::finish.
    meta.serialize_to(&serde_json::json!({"x": 1}), &mut buf)
        .unwrap();
    assert!(!buf.is_empty());
}

#[test]
fn test_buffer_sendmsg_writer_write_and_flush() {
    use ruapc_bufpool::DeviceSet;
    use std::io::Write as _;
    use std::sync::Arc;
    let devices = Arc::new(DeviceSet::<ruapc_bufpool::TcpDevice>::default());
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices).build();
    let mut buf = pool.allocate(1024 * 1024).unwrap();
    buf.set_len(0);
    {
        let mut w = buf.writer();
        // Explicitly call `write()` (not `write_all()`).
        assert_eq!(w.write(b"test").unwrap(), 4);
        // Explicitly call `flush()` (no-op but must be reachable).
        w.flush().unwrap();
    }
    assert_eq!(buf.len(), 4);
}

fn assert_finish_rejects_invalid_offsets<
    M: SendMsg<Error = CodecError> + std::ops::Deref<Target = [u8]>,
>(
    storage: &mut M,
) {
    let original = storage.to_vec();
    for (meta_offset, payload_offset) in [
        (0, 3),
        (0, 4),
        (4, 8),
        (12, 16),
        (8, 4),
        (13, 16),
        (0, 17),
        (20, 24),
        (usize::MAX - 3, usize::MAX),
        (usize::MAX, usize::MAX),
    ] {
        assert_eq!(
            storage
                .finish(meta_offset, payload_offset)
                .unwrap_err()
                .kind,
            CodecErrorKind::InvalidArgument,
            "offsets ({meta_offset}, {payload_offset})"
        );
        assert_eq!(&**storage, original.as_slice());
    }

    storage.finish(4, 12).unwrap();
    assert_eq!(&storage[..4], &original[..4]);
    assert_eq!(&storage[4..8], &4u32.to_be_bytes());
    assert_eq!(&storage[8..], &original[8..]);
}

#[test]
fn public_finish_validates_offsets_without_mutating_invalid_frames() {
    let mut bytes = BytesMut::from(&[42; 16][..]);
    assert_finish_rejects_invalid_offsets(&mut bytes);

    let devices =
        std::sync::Arc::new(ruapc_bufpool::DeviceSet::<ruapc_bufpool::TcpDevice>::default());
    let pool = ruapc_bufpool::BufferPoolBuilder::new(devices).build();
    let mut buffer = pool.allocate(16).unwrap();
    buffer.set_len(0);
    buffer.extend_from_slice(&[42; 16]).unwrap();
    assert_finish_rejects_invalid_offsets(&mut buffer);
}
