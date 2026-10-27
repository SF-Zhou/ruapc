use bytes::Bytes;
use ruapc_wire::CodecError;
use ruapc_wire::rdma::{FRAME_HEADER, Frame, decode_frames};

#[test]
fn decoded_message_owns_the_original_storage_after_input_is_dropped() {
    let body = b"a heap-backed RPC body";
    let mut encoded = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    encoded.extend_from_slice(body);
    let input = Bytes::from(encoded);
    let expected_pointer = input[FRAME_HEADER..].as_ptr();
    let mut held = None;

    decode_frames::<CodecError>(&input, |frame| {
        let Frame::Message(message) = frame else {
            panic!("expected an ordinary message");
        };
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
