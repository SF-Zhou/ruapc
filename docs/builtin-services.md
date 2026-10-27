# Built-in RPC services

Built-in and application services default to their Rust trait name as their
wire name. Services can choose a different wire name with
`#[ruapc::service(name = "...")]`.

This page catalogs wire methods and their exposure. The methods return
serialized `Result<T, Error>` envelopes; a `()` request is JSON `null` or
MessagePack `nil`.

## Discovery and dispatch

Every registered method appears in `Router::method_names()`,
`Router::method_schemas()` and the OpenAPI document. Built-in and application
methods use the same dispatch, admission control and per-method metrics.
Registration rejects duplicate wire method names instead of replacing handlers.

An HTTP or unified listener serves the OpenAPI 3.0 document at `/openapi.json`
and interactive documentation at `/rapidoc`. It accepts unary JSON POSTs at
`/Service/method` for every registered method. Framed TCP, WebSocket, HTTP/2
stream and RDMA calls use the same method directory. Each handler still checks
its required transport, request state and buffer metadata; discovery does not
promise that a method can succeed over every transport. Routing does not
authenticate callers.

For a server listening on `127.0.0.1:8000` in `ListenMode::HTTP` or
`ListenMode::UNIFIED`:

```bash
curl -s http://127.0.0.1:8000/openapi.json

# Requires the rdma feature and an initialized RDMA pool.
curl -s -H 'content-type: application/json' -d 'null' \
  http://127.0.0.1:8000/RdmaBootstrapService/discover
```

Include the configured `http_base_path` in these URLs when it is nonempty.
A TCP-only listener does not serve these HTTP routes.
[`ruapc-schema`](../ruapc-schema/README.md) owns schema registration and OpenAPI
projection independently of the RPC runtime.

## Remote memory: `MemoryService`

These reverse RPCs implement the [remote-memory API](remote-memory.md).
Their Rust trait and request types are crate-private.

| Method | Request | Success value | Data path |
|---|---|---|---|
| `MemoryService/read_inline` | Original request ID and `CopyOp` batch | `ReadInlineResponse { bytes }` in operation order | TCP/WS/HTTP read; data in response. |
| `MemoryService/write_inline` | Original request ID, `CopyOp` batch, bytes | `()` | TCP/WS/HTTP write; data in request. |
| `MemoryService/read_into_target` | Original request ID and `CopyOp` batch; source in `MsgMeta.read_regions` | `()` | Client RDMA-READs server buffers into its pinned target. |
| `MemoryService/request_is_pending` | Original request ID | `bool` | Post-READ source-request waiter membership check. |

Inline reads and writes require the original request's retained source or
write target and valid copy ranges. `read_into_target` additionally requires an
established RDMA connection and advertised read regions. An ordinary HTTP POST
cannot supply those RDMA prerequisites; the route reaches the handler, which
returns an RPC error. Unary HTTP also lacks the reverse-RPC stream used by the
remote-memory API.

Inline byte fields use MessagePack `bin` encoding. The request layouts and
validation handlers are in [memory_service.rs](../ruapc/src/services/memory_service.rs).
For the meaning of a successful READ, waiter membership, error-buffer contents,
and source recovery, see
[read validation and recovery](safe-boundaries.md#application-read-source-validation-and-recovery).

## RDMA bootstrap: `RdmaBootstrapService`

With the `rdma` feature, the connection pool uses these methods over the RDMA
endpoint's declared bootstrap transport (`tcp`, `ws` or `http`).
Peers must advertise bootstrap protocol version **2**.
Bootstrap RPCs use the endpoint's full equivalent-address group; every address
must share the acceptor's `State` and lease records. See
[group identity](endpoints.md#identity-and-caller-obligations).

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `RdmaBootstrapService/discover` | `()` | `RdmaPeerAdvertisement` | Protocol version and currently connectable devices. |
| `RdmaBootstrapService/prepare_connection` | `PrepareConnectionRequest` | `PrepareConnectionResponse` | Acceptor QP endpoint, lease, and actual limits. |
| `RdmaBootstrapService/commit_connection` | `ConnectionLease` | `()` | Idempotently confirms initiator ownership. |
| `RdmaBootstrapService/cancel_connection` | `ConnectionLease` | `()` | Best-effort rollback. |

The advertisement carries device load, directional limits, ports, and GIDs.
A lease contains `attempt_id` and `accepted_connection_id`. See
[connection establishment](rdma-connection.md) for negotiation, activation,
lease expiry, and rollback; wire types live in
[rdma_service.rs](../ruapc/src/rdma/rdma_service.rs) and
[endpoint.rs](../ruapc/src/rdma/endpoint.rs).

## RDMA health probe: `RdmaProbeService`

With the `rdma` feature, both sides of an established RDMA connection use this
method to check that specific connection. It rejects other transports.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `RdmaProbeService/probe` | RPC nonce (`u64`); eight bytes of read space in `MsgMeta.read_regions` | `ProbeReply { rpc_nonce, read_nonce }` | Validate an ordinary RPC round trip and a remote READ on the same connection. |

The read-space nonce is a little-endian `u64` and differs from the RPC nonce.
The caller requires both
returned values to match; the receiver uses `Context::remote_read`, including
its post-READ waiter membership check. The method appears in OpenAPI, but a
unary HTTP call returns an RPC error because it has no established RDMA
connection. The Rust trait and reply type are
crate-private, defined in [probe.rs](../ruapc/src/rdma/probe.rs). See
[periodic connection probes](rdma-connection.md#periodic-connection-probes)
for scheduling, timeout thresholds and reconnection ownership.

## Private RDMA payload controls

An RDMA connection with large-payload support carries three transport frame
types in addition to ordinary RPC messages:

| Control | Fields | Purpose |
|---|---|---|
| Offer | Transfer ID, serialized payload length, payload regions, original metadata | Advertise immutable transport-owned source bytes for READ. |
| Finish | Transfer ID | Confirm that all payload READs completed, or that none was posted. |
| Finish reply | Transfer ID, completion-validity boolean | Report whether the completed payload may be dispatched. |

These named-field MessagePack controls are parsed by the RDMA dispatcher,
outside service routing; they have no OpenAPI entries or unary HTTP routes,
and `MemoryService` cannot invoke them. The offer preserves original application
read/write regions in its metadata and carries serialized-payload regions in a
separate field.
Their byte codecs live in [`ruapc-wire`](../ruapc-wire/src/rdma.rs) and are
available without verbs. These Rust APIs are public for runtime integration;
"private" describes their exclusion from application RPC routing. See the
[wire crate guide](../ruapc-wire/README.md) for release pairing and API stability.
The transport validates connection identity, negotiated
capabilities and transfer state before acting on a decoded control; decoding
does not provide completion evidence or authorize releasing advertised memory.

See [framing](../DESIGN.md#message-ownership-and-framing),
[source ownership](safe-boundaries.md#large-payload-source-ownership), and
[admission and cleanup](rdma-capacity.md#large-payload-admission-and-cleanup).
This private completion protocol does not extend to application attachments.
