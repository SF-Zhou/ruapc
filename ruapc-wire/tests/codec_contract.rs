// Fixed wire vectors were encoded by the implementation at bd7088a before
// extraction, so simultaneous encoder/decoder changes cannot hide drift.

use std::{cell::Cell, io::Write};

use bytes::{Bytes, BytesMut};
use ruapc_wire::{
    CodecError, CodecErrorKind, MsgFlags, MsgMeta, SendMsg, deserialize_payload, frame, parse_body,
};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeSeq};
use serde_json::Value;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    msgpack: bool,
    meta: MsgMeta,
    payload: Value,
    body: Vec<u8>,
    frame: Vec<u8>,
}

fn fixtures() -> Vec<Fixture> {
    serde_json::from_str(include_str!("fixtures/messages.json")).unwrap()
}

fn assert_decoded_message(body: &[u8], fixture: &Fixture) {
    let (meta, payload_offset) = parse_body(body).unwrap();
    assert_eq!(meta, fixture.meta, "{} metadata", fixture.name);
    let payload: Value = deserialize_payload(&body[payload_offset..], meta.flags).unwrap();
    assert_eq!(payload, fixture.payload, "{} payload", fixture.name);
}

#[test]
fn codecs_preserve_baseline_message_bytes_and_values() {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 4);
    for fixture in fixtures {
        assert_eq!(
            fixture.meta.flags.contains(MsgFlags::UseMessagePack),
            fixture.msgpack,
            "{} format flag",
            fixture.name
        );
        let mut encoded = BytesMut::new();
        fixture
            .meta
            .serialize_to(&fixture.payload, &mut encoded)
            .unwrap();
        assert_eq!(
            encoded.as_ref(),
            fixture.body.as_slice(),
            "{} body",
            fixture.name
        );
        assert_eq!(
            frame::encode(&fixture.meta, &fixture.payload)
                .unwrap()
                .as_ref(),
            fixture.frame.as_slice(),
            "{} stream frame",
            fixture.name
        );
        assert_decoded_message(&fixture.body, &fixture);
    }
}

struct InterruptedPayload;

impl Serialize for InterruptedPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(2))?;
        sequence.serialize_element("partially written payload")?;
        Err(serde::ser::Error::custom("payload interrupted"))
    }
}

fn assert_storage_reuse<M>(storage: &mut M)
where
    M: SendMsg<Error = CodecError> + std::ops::Deref<Target = [u8]>,
{
    let original_pointer = storage.as_ptr();
    let long_payload = "long payload".repeat(512);
    for fixture in fixtures() {
        fixture.meta.serialize_to(&long_payload, storage).unwrap();
        assert!(storage.len() > fixture.body.len());
        assert_eq!(storage.as_ptr(), original_pointer);

        fixture
            .meta
            .serialize_to(&fixture.payload, storage)
            .unwrap();
        assert_eq!(
            &**storage,
            fixture.body.as_slice(),
            "{} reuse",
            fixture.name
        );
        assert_eq!(storage.as_ptr(), original_pointer);
        assert_decoded_message(storage, &fixture);

        let error = fixture
            .meta
            .serialize_to(&InterruptedPayload, storage)
            .unwrap_err();
        assert_eq!(
            error.kind,
            if fixture.msgpack {
                CodecErrorKind::SerializeFailed
            } else {
                CodecErrorKind::Json
            }
        );
        let metadata_len = rmp_serde::to_vec_named(&fixture.meta).unwrap().len();
        assert!(storage.len() > 4 + metadata_len);
        assert_eq!(&storage[..4], &[0; 4]);
        assert_eq!(storage.as_ptr(), original_pointer);

        fixture
            .meta
            .serialize_to(&fixture.payload, storage)
            .unwrap();
        assert_eq!(
            &**storage,
            fixture.body.as_slice(),
            "{} recovery",
            fixture.name
        );
        assert_eq!(storage.as_ptr(), original_pointer);
        assert_decoded_message(storage, &fixture);
    }
}

#[test]
fn bytesmut_reuse_replaces_successful_and_failed_messages_without_reallocating() {
    let mut storage = BytesMut::with_capacity(16 * 1024);
    let capacity = storage.capacity();
    assert_storage_reuse(&mut storage);
    assert_eq!(storage.capacity(), capacity);
}

#[test]
fn pooled_buffer_reuse_replaces_successful_and_failed_messages_without_reallocating() {
    let pool =
        ruapc_bufpool::BufferPoolBuilder::new(std::sync::Arc::new(ruapc_bufpool::EmptyDevices))
            .build();
    let mut storage = pool.allocate(16 * 1024).unwrap();
    let capacity = storage.capacity();
    assert_storage_reuse(&mut storage);
    assert_eq!(storage.capacity(), capacity);
}

#[test]
fn complete_frames_keep_the_input_allocation_after_input_is_dropped() {
    for fixture in fixtures() {
        let mut input = Bytes::copy_from_slice(&fixture.frame);
        let expected_body_pointer = input[8..].as_ptr();
        let mut decoder = frame::FrameDecoder::default();
        let body = decoder.decode(&mut input).unwrap().unwrap();
        assert_eq!(
            body.as_ref(),
            fixture.body.as_slice(),
            "{} body",
            fixture.name
        );
        assert_eq!(
            body.as_ptr(),
            expected_body_pointer,
            "{} storage",
            fixture.name
        );
        assert!(input.is_empty());
        drop(input);
        drop(decoder);
        assert_decoded_message(&body, &fixture);
    }
}

struct CountingPayload<'a> {
    calls: &'a Cell<usize>,
}

impl Serialize for CountingPayload<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.calls.set(self.calls.get() + 1);
        serializer.serialize_str("serialized once")
    }
}

#[derive(Default)]
struct Sink {
    bytes: BytesMut,
    write_limit: Option<usize>,
    prepare_error: Option<StorageError>,
    finish_error: Option<StorageError>,
    prepare_calls: usize,
    finish_calls: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum StorageError {
    Codec(CodecError),
    Prepare(String),
    Finish(String),
}

impl From<CodecError> for StorageError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl SendMsg for Sink {
    type Error = StorageError;

    fn size(&self) -> usize {
        self.bytes.len()
    }

    fn prepare(&mut self) -> Result<(), Self::Error> {
        self.prepare_calls += 1;
        if let Some(error) = self.prepare_error.take() {
            return Err(error);
        }
        self.bytes.clear();
        Ok(())
    }

    fn finish(&mut self, meta_offset: usize, payload_offset: usize) -> Result<(), Self::Error> {
        self.finish_calls += 1;
        if let Some(error) = self.finish_error.take() {
            return Err(error);
        }
        self.bytes
            .finish(meta_offset, payload_offset)
            .map_err(Into::into)
    }

    fn writer(&mut self) -> impl Write {
        LimitedWriter {
            bytes: &mut self.bytes,
            limit: self.write_limit,
        }
    }
}

struct LimitedWriter<'a> {
    bytes: &'a mut BytesMut,
    limit: Option<usize>,
}

impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .limit
            .is_some_and(|limit| bytes.len() > limit.saturating_sub(self.bytes.len()))
        {
            return Err(std::io::Error::other("storage write rejected"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn metadata(msgpack: bool) -> MsgMeta {
    MsgMeta {
        flags: if msgpack {
            MsgFlags::UseMessagePack
        } else {
            MsgFlags::empty()
        },
        ..Default::default()
    }
}

#[test]
fn external_storage_serializes_the_payload_once() {
    for msgpack in [false, true] {
        let calls = Cell::new(0);
        let payload = CountingPayload { calls: &calls };
        let meta = metadata(msgpack);
        let mut sink = Sink::default();
        meta.serialize_to(&payload, &mut sink).unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(sink.prepare_calls, 1);
        assert_eq!(sink.finish_calls, 1);
        let (decoded_meta, payload_offset) = parse_body(&sink.bytes).unwrap();
        assert_eq!(decoded_meta, meta);
        assert_eq!(
            deserialize_payload::<String>(&sink.bytes[payload_offset..], decoded_meta.flags)
                .unwrap(),
            "serialized once"
        );
    }
}

#[test]
fn writer_failure_does_not_finish_or_serialize_again() {
    for msgpack in [false, true] {
        let calls = Cell::new(0);
        let payload = CountingPayload { calls: &calls };
        let meta = metadata(msgpack);
        // Allow the reserved length and all metadata, then fail on the payload.
        let metadata_bytes = rmp_serde::to_vec_named(&meta).unwrap();
        let mut sink = Sink {
            write_limit: Some(4 + metadata_bytes.len()),
            ..Default::default()
        };
        let error = meta.serialize_to(&payload, &mut sink).unwrap_err();
        let StorageError::Codec(error) = error else {
            panic!("writer failure must be converted from a codec error");
        };
        assert_eq!(
            error.kind,
            if msgpack {
                CodecErrorKind::SerializeFailed
            } else {
                CodecErrorKind::Json
            }
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(sink.prepare_calls, 1);
        assert_eq!(sink.finish_calls, 0);
        assert_eq!(&sink.bytes[..4], &[0; 4]);
        assert_eq!(&sink.bytes[4..], metadata_bytes.as_slice());
    }
}

#[test]
fn finish_failure_is_returned_unchanged_without_serializing_again() {
    for msgpack in [false, true] {
        let calls = Cell::new(0);
        let payload = CountingPayload { calls: &calls };
        let diagnostic = String::from("custom finish rejection");
        let diagnostic_pointer = diagnostic.as_ptr();
        let mut sink = Sink {
            finish_error: Some(StorageError::Finish(diagnostic)),
            ..Default::default()
        };
        let error = metadata(msgpack)
            .serialize_to(&payload, &mut sink)
            .unwrap_err();
        let StorageError::Finish(diagnostic) = error else {
            panic!("finish failure must retain the storage error variant");
        };
        assert_eq!(diagnostic, "custom finish rejection");
        assert_eq!(diagnostic.as_ptr(), diagnostic_pointer);
        assert_eq!(calls.get(), 1);
        assert_eq!(sink.prepare_calls, 1);
        assert_eq!(sink.finish_calls, 1);
    }
}

#[test]
fn prepare_failure_is_returned_unchanged_without_serializing() {
    let calls = Cell::new(0);
    let payload = CountingPayload { calls: &calls };
    let diagnostic = String::from("custom prepare rejection");
    let diagnostic_pointer = diagnostic.as_ptr();
    let mut sink = Sink {
        prepare_error: Some(StorageError::Prepare(diagnostic)),
        ..Default::default()
    };
    let error = metadata(true)
        .serialize_to(&payload, &mut sink)
        .unwrap_err();
    let StorageError::Prepare(diagnostic) = error else {
        panic!("prepare failure must retain the storage error variant");
    };
    assert_eq!(diagnostic, "custom prepare rejection");
    assert_eq!(diagnostic.as_ptr(), diagnostic_pointer);
    assert_eq!(calls.get(), 0);
    assert_eq!(sink.prepare_calls, 1);
    assert_eq!(sink.finish_calls, 0);
    assert!(sink.bytes.is_empty());
}
