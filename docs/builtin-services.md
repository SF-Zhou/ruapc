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
methods use the same dispatch, `max_inflight_requests` admission limit and
per-method metrics, including payload-transfer RPCs. Configure enough handler
capacity for nested calls and their transport RPCs; see
[large-payload admission](rdma-capacity.md#large-payload-admission-and-cleanup).
Registration rejects duplicate wire method names instead of replacing handlers.

Any [listener mode](endpoints.md#listener-modes) containing `ListenMode::HTTP`
serves the OpenAPI 3.0 document at `/openapi.json` and interactive documentation
at `/rapidoc`. It accepts unary JSON POSTs at
`/Service/method` for every registered method. Framed TCP, WebSocket, HTTP/2
stream and RDMA calls use the same method directory. Each handler still checks
its required transport, request state and buffer metadata; discovery does not
promise that a method can succeed over every transport. Routing does not
authenticate callers.

For a server listening on `127.0.0.1:8000` with `ListenMode::HTTP` enabled:

```bash
curl -s http://127.0.0.1:8000/openapi.json

# Requires the rdma feature and an initialized RDMA pool.
curl -s -H 'content-type: application/json' -d 'null' \
  http://127.0.0.1:8000/RdmaService/discover
```

Include the configured `http_base_path` in these URLs when it is nonempty.
These routes are unavailable when the `HTTP` flag is absent.
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
validation handlers are in [rpc_transfer.rs](../ruapc/src/remote_memory/rpc_transfer.rs).
For the meaning of a successful READ, waiter membership, error-buffer contents,
and source recovery, see
[read validation and recovery](safe-boundaries.md#application-read-source-validation-and-recovery).

## RDMA: `RdmaService`

With the `rdma` feature, `RdmaService` groups connection bootstrap, serialized
payload transfer and health probes in one built-in service. Its seven methods
share the router and method directory; each group has its own transport
prerequisites below. The crate-private trait, payload request types and probe
reply type are defined in
[rdma_service.rs](../ruapc/src/rdma/rdma_service.rs).

### Connection bootstrap

The connection pool uses these methods over the RDMA endpoint's declared
bootstrap transport (`tcp`, `ws` or `http`).
Peers must advertise bootstrap protocol version **3**.
Bootstrap RPCs use the endpoint's full equivalent-address group; every address
must share the acceptor's `State` and lease records. See
[group identity](endpoints.md#identity-and-caller-obligations).

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `RdmaService/discover` | `()` | `RdmaPeerAdvertisement` | Protocol version and currently connectable devices. |
| `RdmaService/prepare_connection` | `PrepareConnectionRequest` | `PrepareConnectionResponse` | Acceptor QP endpoint, lease, and actual limits. |
| `RdmaService/commit_connection` | `ConnectionLease` | `()` | Idempotently confirms initiator ownership. |
| `RdmaService/cancel_connection` | `ConnectionLease` | `()` | Best-effort rollback. |

The advertisement carries device load, directional limits, ports, and GIDs.
A lease contains `attempt_id` and `accepted_connection_id`. See
[connection establishment](rdma-connection.md) for negotiation, activation,
lease expiry, and rollback; wire types live in
[rdma_service.rs](../ruapc/src/rdma/rdma_service.rs) and
[endpoint.rs](../ruapc/src/rdma/endpoint.rs).

### Payload transfer

These methods transfer serialized payloads larger than the negotiated
inline-message limit. They require the established RDMA
connection that owns the transfer.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `RdmaService/offer` | `LargePayloadOffer { transfer_id, payload_len, regions, meta }` | `()` | Start receiving immutable transport-owned source bytes with READ. |
| `RdmaService/finish` | `FinishPayloadRequest { transfer_id }` | `bool` | Release a source after every READ settled, or none was posted, and report whether the payload remains valid. |

Both methods use ordinary RPC metadata, payload codecs, framing, routing and
response waiters. They appear in the method directory and OpenAPI; unary HTTP
and other non-RDMA transports receive `InvalidArgument`. There is no separate
control-frame format. The public RDMA send API reserves these method names for
transport code; application raw calls cannot assert READ completion. The offer
preserves original application read/write regions in its metadata and carries
serialized-payload regions in a separate field. Its successful reply
acknowledges the receive task, not completed READs or application execution.

The runtime checks negotiated limits and connection-local transfer state.
Incoming message dispatch is internal to the transports.
The receiver calls `finish` only after all payload READs completed or none was
posted, then dispatches the original message only after a positive reply.
These RPCs do not authenticate a peer: the peer must honor the advertised-memory
protocol, and a finish request is its assertion that READs no longer need the
source. Local posted destinations still require CQ-issued completion evidence.

See [framing](../DESIGN.md#message-ownership-and-framing),
[source ownership](safe-boundaries.md#large-payload-source-ownership), and
[admission and cleanup](rdma-capacity.md#large-payload-admission-and-cleanup).
This source-retention protocol does not extend to application attachments.

### Health probe

With the `rdma` feature, both sides of an established RDMA connection use this
method to check that specific connection. It rejects other transports.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `RdmaService/probe` | RPC nonce (`u64`); eight bytes of read space in `MsgMeta.read_regions` | `ProbeReply { rpc_nonce, read_nonce }` | Validate an ordinary RPC round trip and a remote READ on the same connection. |

The read-space nonce is a little-endian `u64` and differs from the RPC nonce.
The caller requires both
returned values to match; the receiver uses `Context::remote_read`, including
its post-READ waiter membership check. The method appears in OpenAPI, but a
unary HTTP call returns an RPC error because it has no established RDMA
connection. The handler and `ProbeReply` are defined in
[rdma_service.rs](../ruapc/src/rdma/rdma_service.rs);
[probe.rs](../ruapc/src/rdma/probe.rs) owns the client's `probe_connection`
round trip, nonce verification and total deadline. See
[periodic connection probes](rdma-connection.md#periodic-connection-probes)
for scheduling, timeout thresholds and reconnection ownership.
