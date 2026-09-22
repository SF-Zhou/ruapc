# Built-in RPC services

RuaPC reserves `_ruapc.*` for framework services by convention. Application
services default to their Rust trait name or choose a wire name with
`#[ruapc::service(name = "...")]`.

The built-in methods below return serialized `Result<T, Error>` envelopes.
A `()` request is JSON `null` or MessagePack `nil`.

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

These reverse RPCs implement `Context::remote_read` and `remote_write`.
Their Rust trait and request types are crate-private.

| Method | Request | Success value | Data path |
|---|---|---|---|
| `_ruapc.memory/read_inline` | Original request ID and `CopyOp` batch | Bytes in operation order | TCP/WS/HTTP read; data in response. |
| `_ruapc.memory/write_inline` | Original request ID, `CopyOp` batch, bytes | `()` | TCP/WS/HTTP write; data in request. |
| `_ruapc.memory/read_into_target` | Original request ID and `CopyOp` batch; source in `MsgMeta.read_regions` | `()` | Client RDMA-READs server buffers into its pinned target. |
| `_ruapc.memory/request_is_pending` | Original request ID | `bool` | Post-READ source-request liveness check. |

`read_inline` resolves the request ID to an owned source and validates logical
offsets; peer-provided addresses cannot select local allocations. The handler
retains the source through its CPU copy. Inline byte fields use MessagePack
`bin` encoding.

`read_into_target` moves the client destinations into a QP-owned READ plan;
CPU copies and competing DMA cannot access that target until buffers return.
The pending-request probe rejects stale one-sided READ results. It does not
acknowledge remote DMA completion or make source recovery wait for it.
See [request types and handlers](../ruapc/src/services/memory_service.rs) and
[ownership boundaries](safe-boundaries.md).

## Internal RDMA bootstrap: `_ruapc.rdma`

With the `rdma` feature, a TCP bootstrap connection discovers devices and
establishes queue pairs. Peers must advertise bootstrap protocol version **2**.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `_ruapc.rdma/discover` | `()` | `RdmaPeerAdvertisement` | Protocol version and currently connectable devices. |
| `_ruapc.rdma/prepare_connection` | `PrepareConnectionRequest` | `PrepareConnectionResponse` | Acceptor QP endpoint, lease, and actual limits. |
| `_ruapc.rdma/commit_connection` | `ConnectionLease` | `()` | Idempotently confirms initiator ownership. |
| `_ruapc.rdma/cancel_connection` | `ConnectionLease` | `()` | Best-effort rollback. |

The advertisement carries device load, directional connection limits, ports,
and GIDs. Prepare selects a device/port/GID and exchanges QP endpoints and
resolved limits. A lease identifies the attempt and accepted connection.
Local send limits match peer receive limits and vice versa; mismatched resolved
limits fail setup. Scatter/gather limits stay local, and CQs are shared rather
than negotiated per connection.

Commit records ownership; activation also requires a successful data-plane
receive. See [RDMA connection establishment](rdma-connection.md) for the
sequence and [bootstrap wire types](../ruapc/src/rdma/rdma_service.rs) for fields.

## Exposure rules

| Entry point | Public methods | Internal methods |
|---|---:|---:|
| Framed peer RPC: TCP/WS/HTTP stream/RDMA | yes | yes |
| Unary HTTP `/Service/method` | yes | no (404) |
| Reflection and OpenAPI | yes | no |

`#[ruapc::service(internal)]` selects these internal visibility rules.
Registration rejects duplicate wire method names instead of replacing handlers.
