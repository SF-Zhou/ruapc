use bytes::{Bytes, BytesMut};
use ruapc_wire::{CodecError, CodecErrorKind, MsgFlags, MsgMeta, frame, parse_body};

#[test]
fn decoded_message_owns_the_original_storage_after_input_is_dropped() {
    let body = b"a heap-backed RPC body";
    let mut encoded = frame::encode_header(body.len()).unwrap().to_vec();
    encoded.extend_from_slice(body);
    let input = Bytes::from(encoded);
    let expected_pointer = input[frame::HEADER_LEN..].as_ptr();
    let mut held = None;

    frame::decode_frames::<CodecError>(&input, |message| {
        assert!(held.is_none());
        assert_eq!(message.as_ptr(), expected_pointer);
        held = Some(message);
        Ok(())
    })
    .unwrap();
    drop(input);

    let message = held.expect("decoded message");
    assert_eq!(message.as_ptr(), expected_pointer);
    assert_eq!(&message[..], body);
}

#[test]
fn aggregate_decoder_accepts_stream_encoder_output_without_copying() {
    let mut expected = Vec::new();
    let mut input = BytesMut::new();
    for (index, flags) in [MsgFlags::IsReq, MsgFlags::IsReq | MsgFlags::UseMessagePack]
        .into_iter()
        .enumerate()
    {
        let meta = MsgMeta {
            method: "Echo/echo".into(),
            msgid: index as u64,
            flags,
            ..Default::default()
        };
        let encoded = frame::encode(&meta, &"payload").unwrap();
        input.extend_from_slice(&encoded);
        expected.push(meta);
    }
    let input = input.freeze();
    let mut stream_input = BytesMut::from(input.as_ref());
    let mut offset = frame::HEADER_LEN;
    let mut count = 0;
    frame::decode_frames::<CodecError>(&input, |body| {
        assert_eq!(body.as_ptr(), input[offset..].as_ptr());
        assert_eq!(body, frame::parse_message(&mut stream_input)?.unwrap());
        assert_eq!(parse_body(&body)?.0, expected[count]);
        offset += body.len() + frame::HEADER_LEN;
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, expected.len());
    assert!(stream_input.is_empty());
}

#[test]
fn bounded_decoder_rejects_every_truncated_header_and_body() {
    let encoded = frame::encode(&MsgMeta::default(), &"payload").unwrap();
    for end in 1..encoded.len() {
        let mut calls = 0;
        let error = frame::decode_frames::<CodecError>(&encoded.slice(..end), |_| {
            calls += 1;
            Ok(())
        })
        .unwrap_err();
        assert_eq!(error.kind, CodecErrorKind::InvalidFrame, "end {end}");
        assert_eq!(calls, 0);
    }
}

#[test]
fn aggregate_decoder_rejects_old_rdma_headers_and_oversized_frames() {
    for invalid in [
        Bytes::from_static(b"\0\0\0\x04body"),
        Bytes::from_static(b"\0\0\0\0\0\0\0\x02\x02\x80"),
        Bytes::from_static(b"RUA!\xff\xff\xff\xff"),
        Bytes::from_static(b"RUA!\x03\xff\xff\xf8"),
    ] {
        let error = frame::decode_frames::<CodecError>(&invalid, |_| {
            panic!("invalid frame must not reach callback")
        })
        .unwrap_err();
        assert_eq!(error.kind, CodecErrorKind::InvalidFrame);
    }
}

#[test]
fn valid_frame_is_dispatched_before_a_truncated_trailing_frame() {
    let encoded = frame::encode(&MsgMeta::default(), &"payload").unwrap();
    let mut input = BytesMut::from(encoded.as_ref());
    input.extend_from_slice(&encoded[..encoded.len() - 1]);
    let mut calls = 0;
    let error = frame::decode_frames::<CodecError>(&input.freeze(), |body| {
        assert_eq!(body, encoded.slice(frame::HEADER_LEN..));
        calls += 1;
        Ok(())
    })
    .unwrap_err();
    assert_eq!(error.kind, CodecErrorKind::InvalidFrame);
    assert_eq!(calls, 1);
}

#[test]
fn callback_error_is_returned_unchanged_and_stops_the_aggregate() {
    let encoded = frame::encode(&MsgMeta::default(), &"payload").unwrap();
    let mut input = BytesMut::from(encoded.as_ref());
    input.extend_from_slice(&encoded);
    let mut expected = Some(CodecError::new(
        CodecErrorKind::InvalidArgument,
        "rejected callback".into(),
    ));
    let pointer = expected.as_ref().unwrap().msg.as_ptr();
    let mut calls = 0;
    let error = frame::decode_frames(&input.freeze(), |_| {
        calls += 1;
        Err(expected.take().expect("callback runs only once"))
    })
    .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(error.kind, CodecErrorKind::InvalidArgument);
    assert_eq!(error.msg.as_ptr(), pointer);
}

#[test]
fn empty_body_has_the_same_meaning_for_all_frame_decoders() {
    let mut input = BytesMut::from(frame::encode_header(0).unwrap().as_slice());
    let encoded = frame::encode(&MsgMeta::default(), &()).unwrap();
    input.extend_from_slice(&encoded);
    let mut calls = 0;
    frame::decode_frames::<CodecError>(&input.freeze(), |body| {
        if calls == 0 {
            assert!(body.is_empty());
        } else {
            assert_eq!(body, encoded.slice(frame::HEADER_LEN..));
        }
        calls += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(calls, 2);
    frame::decode_frames::<CodecError>(&Bytes::new(), |_| panic!("no frames")).unwrap();
}
