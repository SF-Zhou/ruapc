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
Metadata preserves flag bits without validating their combinations.

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

`frame::encode_parts` returns a `SendFrame` containing a small encoded prefix
and cloned `Bytes` owners, without copying payload segments. The combined frame
obeys the same size limit as contiguous encoding. `SendFrame::into_bytes`
coalesces only when the transport needs contiguous storage.
`SendFrame::from(Bytes)` only wraps caller-validated, already framed bytes,
such as the output of `frame::encode`; it does not validate untrusted input.
Frame equality compares the complete bytes regardless of segmentation.

Use `frame::encode_raw` for control values followed by unencoded byte segments.
It writes metadata and the control prefix into one allocation, selects the
control codec from the same metadata, and retains the data's `Bytes` owners.
The message must carry `HasRawBytes`. Ordinary `frame::encode` and
`MsgMeta::serialize_to` reject that flag, so a typed payload cannot accidentally
be sent as a raw payload. `encode_parts` remains the low-level framing entry
for callers that already own a complete, correctly encoded payload.

`raw::encode_header` writes only a length-prefixed control value. `raw::decode`
validates the control length and codec and borrows the trailing raw bytes from
the input. These payloads require `MsgFlags::HasRawBytes`; the ordinary payload
decoder rejects them. The control codec still follows `UseMessagePack`.
Callers must validate the raw byte count against their transfer plan before
using it. See the [remote-memory protocol](../docs/builtin-services.md#remote-memory-memoryservice).

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
