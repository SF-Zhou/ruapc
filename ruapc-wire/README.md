# RuaPC wire codecs

`ruapc-wire` owns message metadata, payload codecs, and stream and RDMA framing.
It has no dependency on the RPC runtime, an async executor or `ruapc-rdma`.
`ruapc` retains its message storage types and RPC error envelope, and maps
`CodecError` into its existing error categories.

Metadata always uses named-field MessagePack. Payloads use MessagePack when
`MsgFlags::UseMessagePack` is set, and JSON otherwise. See the
[wire format](../DESIGN.md#message-ownership-and-framing) for frame layouts.
Message metadata and MessagePack payloads are decoded with the shared
`MAX_DECODE_DEPTH` nesting limit, so every
transport applies the same bound to structure this crate parses; JSON payloads
are bounded internally by `serde_json` at the same depth.

`MsgMeta::serialize_to` uses the generic `SendMsg` storage interface to write
directly into a destination and backfill lengths. It returns `Result<(), M::Error>`;
the built-in `BytesMut` and `ruapc_bufpool::Buffer` implementations use
`CodecError`. Core callers can convert that error into `ruapc::Error` with `?`
or `map_err(ruapc::Error::from)`.

Each serialization starts a new message. The built-in storage implementations
clear existing contents while retaining capacity; custom `prepare` hooks may
then write a transport header. Writers append to that prepared storage.
The built-in `finish` hooks check offsets, a nonempty metadata span and its
representable length before backfilling the prefix. They do not parse the
metadata bytes to validate their contents.

`parse_body` borrows a message body and returns its metadata and payload offset;
`deserialize_payload` decodes a borrowed payload slice. The caller retains
ownership of the storage.
The `frame` module supports TCP incremental buffers, HTTP/2 byte chunks,
one-frame WebSocket binary messages and complete RDMA SEND aggregates. Every
carrier uses `[RUA!: 4B][body_len: 4B][meta_len: 4B][meta][payload]` and the
same frame size limit. `frame::encode_header` lets registered-buffer serializers
backfill that header without an intermediate allocation.

Stream carriers also accept data frames,
`[RUAD: 4B][body_len: 4B][head_len: 4B][head][raw bytes]`, whose named-field
MessagePack `DataHead` lists destination ranges that account for exactly the
raw bytes, at most `MAX_DATA_LEN` (1 MiB). Each range is the two-element array
`[offset, len]`, since a head can list one per copy operation.
`frame::parse_frame` and `FrameDecoder::decode_frame` return a data frame once
its head is complete and leave its raw bytes in the stream for the caller to
consume; nothing buffers them on the frame's behalf. `encode_data_head` writes
the header and head so the raw bytes can follow as separate segments.
`parse_message`, `decode` and `decode_frames` accept message frames only. For
an incomplete frame, `parse_frame` returns `Parsed::Incomplete` with the bytes
still missing and sizes the receive buffer for them, reserving at most the
larger of `EAGER_RESERVE` and the bytes already received.
`FrameDecoder::has_partial`
reports bytes still held for an incomplete frame. A data head describes bytes;
it is no evidence that the receiver has the named request or that the ranges
fit its write space.

These codecs describe bytes and owned storage. Connection admission, sending,
RDMA transfer ownership and completion validation remain in the transport
runtime. A successfully encoded or decoded RPC message is not completion
evidence. See [safety boundaries](../docs/safe-boundaries.md) for those contracts.

Large-payload rendezvous uses built-in RPC methods, with its data types and
transfer state owned by the runtime. There is no separate RDMA frame format or
control-frame escape. `ruapc` pins `ruapc-wire` to the exact matching release,
so the runtime and codec cannot be upgraded independently within one build.
See [built-in services](../docs/builtin-services.md) and
[connection setup](../docs/rdma-connection.md) for the runtime contracts.

`SendMsg::Error` and the error parameter of `frame::decode_frames` let transport
hooks and callbacks return their own errors unchanged. Codec failures convert
into that type through `From<CodecError>`. These interfaces use static dispatch.

All codecs, including RDMA frame encoding and decoding, are available without
RDMA libraries or a device. This crate has no transport feature flags.

Run standalone codec tests with:

```bash
cargo test -p ruapc-wire
```

The [codec benchmark](../docs/benchmark.md#wire-codecs) measures encoding,
decoding and framing through this crate's public APIs.
