# Built-in RPC services

RuaPC reserves `_ruapc.*` for framework services by convention. Application
services default to their Rust trait name or choose a wire name with
`#[ruapc::service(name = "...")]`.

This page catalogs wire methods and their exposure. The methods return
serialized `Result<T, Error>` envelopes; a `()` request is JSON `null` or
MessagePack `nil`.

## Exposure rules

| Entry point | Public methods | Internal methods |
|---|---:|---:|
| Framed peer RPC: TCP/WS/HTTP stream/RDMA | yes | yes |
| Unary HTTP `/Service/method` | yes | no (404) |
| Reflection and OpenAPI | yes | no |

`#[ruapc::service(internal)]` selects these visibility rules. Internal visibility
does not authenticate peers. Registration rejects duplicate wire method names
instead of replacing handlers.

## Public reflection: `_ruapc.meta`

| Method | Request | Success value |
|---|---|---|
| `_ruapc.meta/describe` | `DescribeRequest` | `ServerDescription` |
| `_ruapc.meta/openapi` | `()` | `serde_json::Value` |

`describe` returns public services and methods sorted by name, their request
and response schemas, the RuaPC version, and reflection protocol version **1**.
`DescribeRequest.service` filters by exact wire-service name; `None` selects
all public services. Response schemas include the `Result` envelope. Root-level
`components` resolves their `#/components/...` references. `openapi` returns
the full public OpenAPI 3.0 document.

`ReflectionService` and its response types are public Rust APIs. In-process,
use `Router::method_names()`, `method_schemas()`, or `is_public_method()` for
the same visibility rules. See [reflection types and handlers](../ruapc/src/services/meta_service.rs).

## Internal remote memory: `_ruapc.memory`

These reverse RPCs implement the [remote-memory API](remote-memory.md).
Their Rust trait and request types are crate-private.

| Method | Request | Success value | Data path |
|---|---|---|---|
| `_ruapc.memory/read_inline` | Original request ID and `CopyOp` batch | Bytes in operation order | TCP/WS/HTTP read; data in response. |
| `_ruapc.memory/write_inline` | Original request ID, `CopyOp` batch, bytes | `()` | TCP/WS/HTTP write; data in request. |
| `_ruapc.memory/read_into_target` | Original request ID and `CopyOp` batch; source in `MsgMeta.read_regions` | `()` | Client RDMA-READs server buffers into its pinned target. |
| `_ruapc.memory/request_is_pending` | Original request ID | `bool` | Post-READ source-request waiter membership check. |

Inline byte fields use MessagePack `bin` encoding. The request layouts and
validation handlers are in [memory_service.rs](../ruapc/src/services/memory_service.rs).
For the meaning of a successful READ, waiter membership, error-buffer contents,
and source recovery, see
[read validation and recovery](safe-boundaries.md#application-read-source-validation-and-recovery).

## Internal RDMA bootstrap: `_ruapc.rdma`

With the `rdma` feature, the connection pool uses these methods over TCP.
Peers must advertise bootstrap protocol version **2**.
Bootstrap RPCs use the endpoint's full equivalent-address group; every address
must share the acceptor's `State` and lease records. See
[group identity](endpoints.md#identity-and-caller-obligations).

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `_ruapc.rdma/discover` | `()` | `RdmaPeerAdvertisement` | Protocol version and currently connectable devices. |
| `_ruapc.rdma/prepare_connection` | `PrepareConnectionRequest` | `PrepareConnectionResponse` | Acceptor QP endpoint, lease, and actual limits. |
| `_ruapc.rdma/commit_connection` | `ConnectionLease` | `()` | Idempotently confirms initiator ownership. |
| `_ruapc.rdma/cancel_connection` | `ConnectionLease` | `()` | Best-effort rollback. |

The advertisement carries device load, directional limits, ports, and GIDs.
A lease contains `attempt_id` and `accepted_connection_id`. See
[connection establishment](rdma-connection.md) for negotiation, activation,
lease expiry, and rollback; wire types live in
[rdma_service.rs](../ruapc/src/rdma/rdma_service.rs) and
[endpoint.rs](../ruapc/src/rdma/endpoint.rs).

## Internal RDMA health probe

With the `rdma` feature, both sides of an established RDMA connection use this
method to check that specific connection. It rejects other transports.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `_ruapc.rdma/probe` | RPC nonce (`u64`); eight bytes of read space in `MsgMeta.read_regions` | `ProbeReply { rpc_nonce, read_nonce }` | Validate an ordinary RPC round trip and a remote READ on the same connection. |

The read-space nonce is a little-endian `u64` and differs from the RPC nonce.
The caller requires both
returned values to match; the receiver uses `Context::remote_read`, including
its post-READ waiter membership check. The Rust trait and reply type are
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
outside service routing; reflection, unary HTTP, and `_ruapc.memory` cannot
invoke them. The offer preserves original application read/write regions in
its metadata and carries serialized-payload regions in a separate field.

See [framing](../DESIGN.md#message-ownership-and-framing),
[source ownership](safe-boundaries.md#large-payload-source-ownership), and
[admission and cleanup](rdma-capacity.md#large-payload-admission-and-cleanup).
This private completion protocol does not extend to application attachments.
