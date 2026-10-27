# RuaPC wire codecs

`ruapc-wire` owns message metadata, payload codecs, and stream and RDMA framing.
It has no dependency on the RPC runtime, an async executor or `ruapc-rdma`.
`ruapc` retains its message storage types and RPC error envelope, and maps
`CodecError` into its existing error categories.

Metadata always uses named-field MessagePack. Payloads use MessagePack when
`MsgFlags::UseMessagePack` is set, and JSON otherwise. See the
[wire format](../DESIGN.md#message-ownership-and-framing) for frame layouts and
compatibility rules.

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
The `frame` module supports TCP incremental buffers and
HTTP/2 byte chunks; the `rdma` module handles ordinary and control frames.

These codecs describe bytes and owned storage. Connection admission, sending,
RDMA transfer ownership and completion validation remain in the transport
runtime. A successfully encoded or decoded control frame is not completion
evidence. See [safety boundaries](../docs/safe-boundaries.md) for those contracts.

The `rdma` control types and codecs are public for transport-runtime integration;
they are not application RPC endpoints. This integration API may change between
alpha releases. `ruapc` pins `ruapc-wire` to the exact matching release, so the
runtime and codec cannot be upgraded independently within one build. This does
not establish compatibility between network peers: wire changes must preserve
compatibility or be explicitly negotiated through protocol versions and
capabilities. See [private controls](../docs/builtin-services.md#private-rdma-payload-controls)
and [connection setup](../docs/rdma-connection.md) for the runtime contracts.

`SendMsg::Error` and the error parameter of `rdma::decode_frames` let transport
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
