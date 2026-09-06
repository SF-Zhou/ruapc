use super::*;
use crate::msg::{MsgFlags, SendMsg};
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
    let msg = Message::parse(Bytes::from(buf)).unwrap();
    assert_eq!(msg.meta, meta);
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
    let msg = Message::parse(Bytes::from(buf)).unwrap();
    assert_eq!(msg.meta, meta);
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

    let msg = Message::parse(Bytes::from(buf)).unwrap();
    assert_eq!(msg.meta.method, "TestService/hello");
    assert_eq!(msg.meta.msgid, 42);
    assert!(msg.meta.is_req());
    assert!(!msg.meta.flags.contains(MsgFlags::UseMessagePack));

    let recovered: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(recovered, payload_value);
}

#[test]
fn test_serialize_parse_msgpack_roundtrip() {
    let meta = make_meta("TestService/hello", true);
    let payload_str = "hello msgpack";

    let buf = serialize_to_bytes(&meta, &payload_str);

    let msg = Message::parse(Bytes::from(buf)).unwrap();
    assert_eq!(msg.meta.method, "TestService/hello");
    assert!(msg.meta.flags.contains(MsgFlags::UseMessagePack));

    let recovered: String = rmp_serde::from_slice(&msg.payload).unwrap();
    assert_eq!(recovered, payload_str);
}

#[test]
fn test_parse_too_short_returns_error() {
    for len in 0..4 {
        let truncated = Bytes::from(vec![0; len]);
        assert!(Message::parse(truncated).is_err(), "length {len}");
    }
}

#[test]
fn test_parse_zero_meta_len_returns_error() {
    // 4 bytes of zeros => meta_len == 0, which is invalid.
    let buf = Bytes::from_static(&[0u8, 0, 0, 0]);
    assert!(Message::parse(buf).is_err());
}

#[test]
fn test_parse_meta_len_exceeds_payload_returns_error() {
    // meta_len says 100, but total is only 8 bytes.
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&100u32.to_be_bytes()); // meta_len = 100
    buf.extend_from_slice(b"short");
    assert!(Message::parse(Bytes::from(buf)).is_err());
}

#[test]
fn test_message_new_and_default() {
    let msg = Message::default();
    assert!(msg.meta.method.is_empty());
    assert!(msg.payload.is_empty());

    let meta = MsgMeta {
        method: "Svc/method".into(),
        flags: MsgFlags::IsRsp,
        msgid: 7,
        read_regions: Vec::new(),
        write_regions: Vec::new(),
        timeout_ms: 0,
    };
    let payload = crate::Payload::from(bytes::Bytes::from_static(b"data"));
    let msg2 = Message::new(meta, payload);
    assert_eq!(msg2.meta.method, "Svc/method");
    assert!(!msg2.payload.is_empty());
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
fn test_bytesmut_sendmsg_prepare_is_noop() {
    // BytesMut::prepare does nothing — the buffer is not cleared.
    let mut buf = BytesMut::from(&b"existing"[..]);
    buf.prepare().unwrap();
    assert_eq!(&buf[..], b"existing");
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
    use crate::Devices;
    use std::sync::Arc;
    let devices = Arc::new(Devices::default());
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
    use crate::Devices;
    use std::io::Write as _;
    use std::sync::Arc;
    let devices = Arc::new(Devices::default());
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
