use super::*;
use crate::MsgFlags;
use ruapc_bufpool::MemoryKey;

fn offer() -> LargePayloadOffer {
    LargePayloadOffer {
        transfer_id: 42,
        payload_len: 12,
        regions: vec![
            RemoteBufferInfo {
                addr: 4096,
                len: 5,
                key: MemoryKey { lkey: 1, rkey: 2 },
            },
            RemoteBufferInfo {
                addr: 8192,
                len: 7,
                key: MemoryKey { lkey: 3, rkey: 4 },
            },
        ],
        meta: MsgMeta {
            method: "Echo/echo".into(),
            msgid: 17,
            timeout_ms: 2000,
            flags: MsgFlags::IsReq | MsgFlags::UseMessagePack,
            ..MsgMeta::default()
        },
    }
}

fn controls() -> [ControlFrame; 4] {
    [
        ControlFrame::Offer(offer()),
        ControlFrame::Finish { transfer_id: 42 },
        ControlFrame::FinishReply {
            transfer_id: 42,
            completed: true,
        },
        ControlFrame::FinishReply {
            transfer_id: 42,
            completed: false,
        },
    ]
}

fn collect(bytes: &Bytes) -> Result<Vec<Frame>> {
    let mut frames = Vec::new();
    decode_frames(bytes, |frame| {
        frames.push(frame);
        Ok(())
    })?;
    Ok(frames)
}

fn control_bytes(kind: u8, body: &[u8]) -> Bytes {
    let mut bytes = 0u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&u32::try_from(body.len() + 1).unwrap().to_be_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(body);
    bytes.into()
}

#[test]
fn controls_round_trip_with_named_fields() {
    for control in controls() {
        let bytes = encode_control(&control).unwrap();
        assert_eq!(&bytes[..FRAME_HEADER], &0u32.to_be_bytes());
        assert!(matches!(bytes[CONTROL_HEADER + 1], 0x80..=0x8f));
        assert_eq!(collect(&bytes).unwrap(), [Frame::Control(control)]);
    }
}

#[test]
fn controls_preserve_metadata_payload_format_and_attachments() {
    for flags in [MsgFlags::IsReq, MsgFlags::IsRsp | MsgFlags::UseMessagePack] {
        let mut offer = offer();
        offer.meta.flags = flags;
        offer.meta.read_regions = offer.regions.clone();
        offer.meta.write_regions = offer.regions.clone();
        let control = ControlFrame::Offer(offer);
        assert_eq!(
            collect(&encode_control(&control).unwrap()).unwrap(),
            [Frame::Control(control)]
        );
    }
}

#[test]
fn ordinary_and_control_frames_can_be_aggregated() {
    let mut bytes = Vec::new();
    let mut expected = Vec::new();
    for control in controls() {
        let message = Bytes::from_static(b"ordinary message");
        bytes.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(&message);
        bytes.extend_from_slice(&encode_control(&control).unwrap());
        expected.push(Frame::Message(message));
        expected.push(Frame::Control(control));
    }
    let bytes = Bytes::from(bytes);
    let frames = collect(&bytes).unwrap();
    assert_eq!(frames, expected);
    let Frame::Message(first) = &frames[0] else {
        panic!("expected ordinary message");
    };
    assert_eq!(first.as_ptr(), bytes[FRAME_HEADER..].as_ptr());
}

#[test]
fn ordinary_frames_retain_the_legacy_wire_format() {
    let mut bytes = Vec::new();
    let messages: [&[u8]; 3] = [b"first", b"second", b"third-frame"];
    for message in messages {
        bytes.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(message);
    }
    assert_eq!(
        collect(&bytes.into()).unwrap(),
        messages.map(|message| Frame::Message(Bytes::from_static(message)))
    );
}

#[test]
fn complete_frame_before_a_truncated_header_is_dispatched() {
    let bytes = Bytes::from_static(&[0, 0, 0, 1, b'x', 0, 0]);
    let mut frames = Vec::new();
    assert!(
        decode_frames(&bytes, |frame| {
            frames.push(frame);
            Ok(())
        })
        .is_err()
    );
    assert_eq!(frames, [Frame::Message(Bytes::from_static(b"x"))]);
}

#[test]
fn every_truncated_control_is_rejected_without_dispatch() {
    for control in controls() {
        let bytes = encode_control(&control).unwrap();
        for end in 1..bytes.len() {
            let mut dispatched = false;
            assert!(
                decode_frames(&bytes.slice(..end), |_| {
                    dispatched = true;
                    Ok(())
                })
                .is_err(),
                "accepted truncation at {end} of {bytes:?}"
            );
            assert!(!dispatched);
        }
    }
    assert!(collect(&Bytes::new()).unwrap().is_empty());
}

#[test]
fn truncated_normal_frame_and_oversized_lengths_are_rejected() {
    assert!(collect(&Bytes::from_static(&[0, 0, 0, 4, 1, 2, 3])).is_err());
    assert!(collect(&Bytes::from_static(&[0xff, 0xff, 0xff, 0xff])).is_err());
    assert!(collect(&Bytes::from_static(&[0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff])).is_err());
    assert!(collect(&Bytes::from_static(&[0; CONTROL_HEADER])).is_err());
}

#[test]
fn malformed_control_bodies_are_rejected() {
    let finish = rmp_serde::to_vec_named(&Finish { transfer_id: 42 }).unwrap();
    assert!(collect(&control_bytes(255, &finish)).is_err());
    assert!(collect(&control_bytes(FINISH, &[])).is_err());
    assert!(collect(&control_bytes(FINISH, &[0xc1])).is_err());
    assert!(collect(&control_bytes(FINISH, &[0x80])).is_err());

    // Struct sequence encoding is intentionally not the control wire format.
    let sequence = rmp_serde::to_vec(&Finish { transfer_id: 42 }).unwrap();
    assert!(collect(&control_bytes(FINISH, &sequence)).is_err());

    let mut trailing = finish;
    trailing.push(0xc0);
    assert!(collect(&control_bytes(FINISH, &trailing)).is_err());

    // A finish reply requires an explicit boolean result, even when false.
    let missing_result = rmp_serde::to_vec_named(&Finish { transfer_id: 42 }).unwrap();
    assert!(collect(&control_bytes(FINISH_REPLY, &missing_result)).is_err());
}

#[test]
fn empty_payload_offers_are_rejected_on_both_paths() {
    let mut offer = offer();
    offer.payload_len = 0;
    let body = rmp_serde::to_vec_named(&offer).unwrap();
    assert!(encode_control(&ControlFrame::Offer(offer)).is_err());
    assert!(collect(&control_bytes(OFFER, &body)).is_err());
}

#[test]
fn unknown_control_fields_are_compatible() {
    #[derive(Serialize)]
    struct ExtendedFinish {
        transfer_id: u64,
        future_field: bool,
    }
    let body = rmp_serde::to_vec_named(&ExtendedFinish {
        transfer_id: 42,
        future_field: true,
    })
    .unwrap();
    assert_eq!(
        collect(&control_bytes(FINISH, &body)).unwrap(),
        [Frame::Control(ControlFrame::Finish { transfer_id: 42 })]
    );
}

#[test]
fn callback_failure_stops_before_next_frame() {
    let finish = encode_control(&ControlFrame::Finish { transfer_id: 42 }).unwrap();
    let mut bytes = finish.to_vec();
    bytes.extend_from_slice(&finish);
    let expected = Error::kind(ErrorKind::ConnectionClosed);
    let mut calls = 0;
    let error = decode_frames(&bytes.into(), |_| {
        calls += 1;
        Err(expected.clone())
    })
    .unwrap_err();
    assert_eq!(error, expected);
    assert_eq!(calls, 1);
}
