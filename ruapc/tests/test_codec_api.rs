use bytes::{Bytes, BytesMut};
use ruapc::{Error, ErrorKind, Message, MsgFlags, MsgMeta, Payload, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// These direct return expressions also check the original public error types.
fn parse_message(input: impl Into<Payload>) -> Result<Message> {
    Message::parse(input)
}

fn deserialize_payload<T: for<'de> Deserialize<'de>>(
    payload: &Payload,
    meta: &MsgMeta,
) -> Result<T> {
    payload.deserialize(meta)
}

fn encode_body(meta: &MsgMeta, payload: &impl Serialize) -> Bytes {
    let mut body = BytesMut::new();
    meta.serialize_to(payload, &mut body).unwrap();
    body.freeze()
}

#[test]
fn public_message_and_payload_apis_keep_rpc_result_types() {
    let Message { meta, payload } = Message::default();
    assert_eq!(meta, MsgMeta::default());
    assert!(matches!(payload, Payload::Empty));

    let meta = MsgMeta {
        method: "Example/echo".into(),
        flags: MsgFlags::IsReq,
        ..Default::default()
    };
    let message = Message::new(meta.clone(), Payload::from(Bytes::from_static(b"null")));
    assert_eq!(message.meta, meta);
    deserialize_payload::<()>(&message.payload, &message.meta).unwrap();

    let body = encode_body(&meta, &"response");
    let message = parse_message(body).unwrap();
    assert_eq!(message.meta, meta);
    assert_eq!(
        deserialize_payload::<String>(&message.payload, &message.meta).unwrap(),
        "response"
    );
    assert_eq!(
        parse_message(Bytes::from_static(b"short"))
            .unwrap_err()
            .kind,
        ErrorKind::DeserializeFailed
    );
    let invalid = Payload::from(Bytes::from_static(b"\xc1"));
    assert_eq!(
        deserialize_payload::<String>(&invalid, &meta)
            .unwrap_err()
            .kind,
        ErrorKind::SerdeJsonError
    );
    let packed = MsgMeta {
        flags: MsgFlags::UseMessagePack,
        ..meta
    };
    assert_eq!(
        deserialize_payload::<String>(&invalid, &packed)
            .unwrap_err()
            .kind,
        ErrorKind::DeserializeFailed
    );
}

#[test]
fn parsing_and_extracting_normal_payload_keep_the_owned_bytes() {
    let meta = MsgMeta::default();
    let encoded_payload = b"\"owned payload\"";
    let body = encode_body(&meta, &"owned payload");
    let expected_pointer = body[body.len() - encoded_payload.len()..].as_ptr();

    let message = parse_message(body.clone()).unwrap();
    drop(body);

    assert_eq!(message.payload.as_ptr(), expected_pointer);
    assert_eq!(message.payload.as_slice(), encoded_payload);
    let retained: Bytes = message.payload.into();
    assert_eq!(retained.as_ptr(), expected_pointer);
    assert_eq!(retained.as_ref(), encoded_payload);
}

#[test]
fn empty_payloads_keep_null_semantics_for_both_formats() {
    for flags in [MsgFlags::empty(), MsgFlags::UseMessagePack] {
        let meta = MsgMeta {
            flags,
            ..Default::default()
        };
        for payload in [Payload::Empty, Payload::from(Bytes::new())] {
            assert_eq!(
                deserialize_payload::<Option<String>>(&payload, &meta).unwrap(),
                None
            );
            deserialize_payload::<()>(&payload, &meta).unwrap();
        }
    }
}

#[test]
fn codec_errors_convert_to_rpc_categories_without_copying_the_message() {
    let mut frame = BytesMut::from(&b"BAD!\x00\x00\x00\x00"[..]);
    let error = ruapc_wire::frame::parse_message(&mut frame).unwrap_err();
    let expected = error.msg.clone();
    let message_pointer = error.msg.as_ptr();
    let error: Error = error.into();
    assert_eq!(error.kind, ErrorKind::ParseMsgFailed);
    assert_eq!(error.msg, expected);
    assert_eq!(error.msg.as_ptr(), message_pointer);

    let error = ruapc_wire::SendMsg::finish(&mut frame, usize::MAX, 0).unwrap_err();
    assert_eq!(Error::from(error).kind, ErrorKind::InvalidArgument);

    struct Rejected;

    impl Serialize for Rejected {
        fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("payload rejected"))
        }
    }

    let meta = MsgMeta {
        flags: MsgFlags::UseMessagePack,
        ..Default::default()
    };
    let error = meta
        .serialize_to(&Rejected, &mut BytesMut::new())
        .unwrap_err();
    assert_eq!(Error::from(error).kind, ErrorKind::SerializeFailed);
}

#[test]
fn rpc_error_envelope_and_schema_identity_keep_the_original_contract() {
    assert_eq!(Error::schema_name(), "Error");
    assert_eq!(ErrorKind::schema_name(), "ErrorKind");
    assert_eq!(Error::schema_id(), "ruapc::error::Error");
    assert_eq!(ErrorKind::schema_id(), "ruapc::error::ErrorKind");
    let schema = schemars::schema_for!(Error);
    assert!(
        schema.as_value()["description"]
            .as_str()
            .unwrap()
            .contains("use ruapc::{Error, ErrorKind};")
    );

    let result: Result<()> = Err(Error::kind(ErrorKind::Timeout));
    let json = serde_json::json!({"Err": {"kind": "Timeout", "msg": ""}});
    let packed: &[u8] = b"\x81\xa3Err\x82\xa4kind\xa7Timeout\xa3msg\xa0";
    assert_eq!(serde_json::to_value(&result).unwrap(), json);
    assert_eq!(serde_json::from_value::<Result<()>>(json).unwrap(), result);
    assert_eq!(rmp_serde::to_vec_named(&result).unwrap(), packed);
    assert_eq!(rmp_serde::from_slice::<Result<()>>(packed).unwrap(), result);
}

#[cfg(feature = "rdma")]
#[test]
fn registered_payload_parsing_retains_the_buffer_until_drop() {
    const CAPACITY: usize = 64 * 1024 * 1024;
    let pool =
        ruapc_bufpool::BufferPoolBuilder::new(std::sync::Arc::new(ruapc_bufpool::EmptyDevices))
            .max_memory(CAPACITY)
            .build();
    let mut buffer = pool.allocate(CAPACITY).unwrap();
    let meta = MsgMeta::default();
    meta.serialize_to(&"registered payload", &mut buffer)
        .unwrap();
    let encoded_payload = b"\"registered payload\"";
    let expected_pointer = buffer[buffer.len() - encoded_payload.len()..].as_ptr();

    let message = parse_message(buffer).unwrap();
    assert!(matches!(&message.payload, Payload::RDMA(_, _)));
    assert_eq!(message.payload.as_ptr(), expected_pointer);
    assert_eq!(message.payload.as_slice(), encoded_payload);
    assert_eq!(
        deserialize_payload::<String>(&message.payload, &message.meta).unwrap(),
        "registered payload"
    );
    assert!(pool.allocate(CAPACITY).is_err());
    drop(message);
    assert!(pool.allocate(CAPACITY).is_ok());
}

#[cfg(feature = "rdma")]
#[test]
fn rdma_error_conversion_keeps_the_original_public_type() {
    let error: Error = ruapc_rdma::Error::new(
        ruapc_rdma::ErrorKind::IBOpenDeviceFail,
        "unavailable device".into(),
    )
    .into();
    assert_eq!(
        error.kind,
        ErrorKind::RdmaError(ruapc_rdma::ErrorKind::IBOpenDeviceFail)
    );
    let value = serde_json::to_value(&error).unwrap();
    assert_eq!(
        value["kind"],
        serde_json::json!({"RdmaError": "IBOpenDeviceFail"})
    );
    assert_eq!(serde_json::from_value::<Error>(value).unwrap(), error);
}
