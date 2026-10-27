use serde::Deserialize;

use crate::{CodecResult, MsgFlags};

/// Decodes a borrowed payload according to its message flags.
///
/// [`MsgFlags::UseMessagePack`] selects MessagePack; otherwise JSON is used.
/// An empty payload is decoded as JSON `null` for either flag, allowing
/// body-less requests. This function does not take ownership of the storage.
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
        Ok(rmp_serde::from_slice(payload)?)
    } else {
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
}
