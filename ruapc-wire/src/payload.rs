use serde::Deserialize;

use crate::{CodecResult, MAX_DECODE_DEPTH, MsgFlags};

/// Decodes a borrowed payload according to its message flags.
///
/// [`MsgFlags::UseMessagePack`] selects MessagePack; otherwise JSON is used.
/// An empty payload is decoded as JSON `null` for either flag, allowing
/// body-less requests. This function does not take ownership of the storage.
///
/// MessagePack payloads enforce [`MAX_DECODE_DEPTH`]; JSON payloads are
/// bounded internally by `serde_json` at the same depth. Both limits only
/// reject malformed or hostile input before recursive decoding can exhaust the
/// stack.
///
/// # Errors
///
/// Returns an error if the payload cannot be decoded into `P`.
pub fn deserialize_payload<P: for<'de> Deserialize<'de>>(
    payload: &[u8],
    flags: MsgFlags,
) -> CodecResult<P> {
    if payload.is_empty() {
        Ok(serde_json::from_value(serde_json::Value::Null)?)
    } else if flags.contains(MsgFlags::UseMessagePack) {
        let mut deserializer = rmp_serde::Deserializer::from_read_ref(payload);
        deserializer.set_max_depth(MAX_DECODE_DEPTH);
        Ok(P::deserialize(&mut deserializer)?)
    } else {
        // `serde_json` has no public setter; its built-in 128-level limit
        // matches `MAX_DECODE_DEPTH`.
        Ok(serde_json::from_slice(payload)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodecErrorKind;

    #[test]
    fn empty_payload_is_json_null_for_both_formats() {
        for flags in [MsgFlags::empty(), MsgFlags::UseMessagePack] {
            assert_eq!(
                deserialize_payload::<Option<String>>(&[], flags).unwrap(),
                None
            );
            assert_eq!(
                deserialize_payload::<String>(&[], flags).unwrap_err().kind,
                CodecErrorKind::Json
            );
        }
    }

    #[test]
    fn json_and_messagepack_payloads_decode_to_their_values() {
        let json = serde_json::to_vec(&"hello json").unwrap();
        assert_eq!(
            deserialize_payload::<String>(&json, MsgFlags::empty()).unwrap(),
            "hello json"
        );
        let msgpack = rmp_serde::to_vec_named(&42u64).unwrap();
        assert_eq!(
            deserialize_payload::<u64>(&msgpack, MsgFlags::UseMessagePack).unwrap(),
            42
        );
    }

    #[test]
    fn malformed_payloads_report_the_selected_codec() {
        assert_eq!(
            deserialize_payload::<String>(b"not valid json!", MsgFlags::empty())
                .unwrap_err()
                .kind,
            CodecErrorKind::Json
        );
        assert_eq!(
            deserialize_payload::<String>(&[0xc1], MsgFlags::UseMessagePack)
                .unwrap_err()
                .kind,
            CodecErrorKind::DeserializeFailed
        );
    }

    #[test]
    fn payload_decoders_share_the_configured_nesting_limit() {
        // MessagePack enforces MAX_DECODE_DEPTH directly; serde_json's matching
        // built-in limit bounds JSON at the same depth.
        let shallow = crate::MAX_DECODE_DEPTH - 1;
        let too_deep = crate::MAX_DECODE_DEPTH;

        let mut msgpack_ok = vec![0x91u8; shallow]; // array(1) * n
        msgpack_ok.push(0x01);
        assert!(
            deserialize_payload::<serde_json::Value>(&msgpack_ok, MsgFlags::UseMessagePack).is_ok()
        );
        let mut msgpack_deep = vec![0x91u8; too_deep];
        msgpack_deep.push(0x01);
        assert_eq!(
            deserialize_payload::<serde_json::Value>(&msgpack_deep, MsgFlags::UseMessagePack)
                .unwrap_err()
                .kind,
            CodecErrorKind::DeserializeFailed
        );

        let json_ok = format!("{}1{}", "[".repeat(shallow), "]".repeat(shallow));
        assert!(
            deserialize_payload::<serde_json::Value>(json_ok.as_bytes(), MsgFlags::empty()).is_ok()
        );
        let json_deep = format!("{}1{}", "[".repeat(too_deep), "]".repeat(too_deep));
        assert_eq!(
            deserialize_payload::<serde_json::Value>(json_deep.as_bytes(), MsgFlags::empty())
                .unwrap_err()
                .kind,
            CodecErrorKind::Json
        );
    }
}
