//! Shared message framing; RPC dispatch remains in this crate.

pub(super) use ruapc_wire::frame::decode_frames;

#[cfg(test)]
mod tests {
    use super::decode_frames;
    use crate::{Error, ErrorKind};
    use bytes::Bytes;

    #[test]
    fn callback_preserves_typed_rdma_error_and_stops_before_the_next_frame() {
        let frames = Bytes::from_static(b"RUA!\0\0\0\x01aRUA!\0\0\0\x01b");
        let expected_kind = ErrorKind::RdmaError(ruapc_rdma::ErrorKind::InvalidCompletion);
        let mut failure = Some(Error::new(
            expected_kind.clone(),
            "callback rejected completion identity".into(),
        ));
        let expected_message_pointer = failure.as_ref().unwrap().msg.as_ptr();
        let mut calls = 0;

        let error = decode_frames(&frames, |message| {
            calls += 1;
            assert_eq!(message.as_ref(), b"a");
            Err(failure.take().expect("callback runs only once"))
        })
        .unwrap_err();

        assert_eq!(calls, 1);
        assert_eq!(error.kind, expected_kind);
        assert_eq!(error.msg, "callback rejected completion identity");
        assert_eq!(error.msg.as_ptr(), expected_message_pointer);
    }
}
