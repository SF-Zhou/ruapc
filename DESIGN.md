# RuaPC architecture

This document maps modules and the RPC lifecycle. Detailed contracts live in
the [topic guides](docs/README.md); public types and methods are documented in
rustdoc.

## Workspace and dependency direction

| Crate | Responsibility | Workspace dependencies |
|---|---|---|
| `ruapc-bufpool` | Allocation, caches and device registration | None |
| `ruapc-rdma` | Verbs resources, owned work requests and completions | `ruapc-bufpool` |
| `ruapc-macro` | Service validation and code generation | None at runtime |
| `ruapc` | RPC lifecycle, routing, transports and remote memory | Bufpool, macro; optional RDMA |
| `ruapc-demo` | Service host, typed caller and verified workloads | `ruapc` |

Transport selection uses concrete enums (`Socket`, `SocketPool`, `HttpSocket`)
and statically dispatched futures. Registered service closures and device
registrars are the trait-object extension boundaries.

The core crate forbids unsafe code. Allocation, registration and verbs belong
in its dependencies, whose APIs enforce local memory ownership and completion
evidence. See [safety boundaries](docs/safe-boundaries.md) for these contracts
and the separate application-source validation scheme.

## Core library boundaries

| Module | Responsibility |
|---|---|
| `client` | Settings, attachments, connection acquisition, retries and response decoding |
| `core` | Contexts, listeners, method registration and handler admission |
| `core/router/schema` | Service schemas and OpenAPI projection |
| `msg` | Metadata, payload codecs, direct serialization and stream framing |
| `sockets` | Concrete transports, connection queues, maps and closure |
| `remote_memory` | Logical copy spaces, validation, inline copies and buffer recovery |
| `rdma` | Path selection, bootstrap, READ execution and CQ polling |
| `task` | Background-task supervision and response waiters |
| `metrics` | Cached recorder handles and scoped request accounting |

`Context` combines shared `State`, a destination, incoming metadata and a
deadline. Changing its destination preserves the deadline and clears message
metadata. Its `remote_*` methods live in `remote_memory/context.rs`.
An endpoint selects one transport and a complete group of equivalent server
addresses. Transport pools own address fallback, group connection caching and
coalesced acquisition; the client does not choose among servers or transports.
See [endpoint identity](docs/endpoints.md) for normalization and caller obligations.

## Request lifecycle

1. Generated methods enter `client/call.rs`. Plain and buffer-returning calls
   use the same executor in `client/request.rs`; the result type selects the
   response contract.
2. The client validates attachments and acquires a connection for its endpoint.
   The transport pool tries the group's equivalent addresses within the
   connection budget, independently of the request retry count.
3. The response budget starts after connection acquisition and includes
   bandwidth and send-queue admission. Nested calls cap both budgets at the
   parent's remaining time. The client allocates its waiter after bandwidth
   admission and attaches the read source and write target.
4. Before queueing, the transport binds the waiter to its connection ID and
   checks closure. A full queue waits for capacity, deadline or closure;
   submission rechecks state and advertises the remaining admission budget.
   TCP/WS/HTTP share this logic in `sockets/channel.rs`; RDMA uses the same
   admission helper. Accepted messages stay queued even if their requests expire.
5. Only failures before transport acceptance can retry. After acceptance the
   client waits for one result; ambiguous timeouts and connection loss do not
   trigger automatic retries.
6. Server dispatch reserves capacity atomically and drops expired requests
   before execution. Generated handlers contain panics. Running handlers are
   not cancelled by deadline expiry; they can poll `Context::is_expired()`.
7. A response, connection failure, cancellation guard or expiry sweep removes
   the waiter. Response expiry uses a shared 50 ms sweep, subject to scheduler
   delay. Unary HTTP allocates its response waiter after receiving the complete
   request body.

Connection closure is idempotent and eagerly fails its waiters. Eviction checks
connection identity so an old connection cannot remove its replacement. RDMA
shutdown additionally follows the [poller and DMA cleanup rules](docs/qp-registry.md#closing-a-connection).

Metrics use the `metrics` facade; applications install a recorder before
traffic. Per-method handles are cached per `State`, and guards settle accounting
on completion, cancellation and panic. Only `server_inflight` has a local value,
used for admission control.

## Message ownership and framing

Metadata is always named-field MessagePack. `UseMessagePack` selects only the
payload decoder; default clients use MessagePack, otherwise JSON. Compatible
metadata fields use `#[serde(default)]` and omit empty/default values where
appropriate. See [`MsgMeta`](ruapc/src/msg/meta.rs) for fields and defaults.

`msg/encode.rs` serializes directly into transport storage and backfills lengths.
`msg/message.rs` validates bounds before decoding metadata and advances the
payload view without copying.

| Carrier | Frame |
|---|---|
| TCP, HTTP/2 stream | `[RUA!: 4B][body_len: 4B][meta_len: 4B][meta][payload]` |
| WebSocket binary message | `[meta_len: 4B][meta][payload]` |
| RDMA inline message | `[frame_len: 4B][meta_len: 4B][meta][payload]` |
| RDMA large-payload control | `[0: 4B][control_len: 4B][kind: 1B][named MessagePack body]` |
| Unary HTTP | JSON bodies; routing comes from the URL |

Lengths are big-endian `u32`s. `body_len` and `frame_len` count the `meta_len`
prefix, metadata and payload. `control_len` counts the kind byte and control
body; kinds are offer (1), finish (2) and finish reply (3).

TCP and HTTP/2 share the size limit and incremental parser in `msg/frame.rs`.
Incomplete frames remain buffered; complete bodies are split without copying.
RDMA framing lives in `rdma/frame.rs`, outside the poll loop. Inline and control
frames can share a receive buffer; the whole SEND consumes one receive credit.
Large payloads use the [private offer/finish controls](docs/builtin-services.md#private-rdma-payload-controls),
with separate [storage and queue budgets](docs/rdma-capacity.md#large-payload-admission-and-cleanup).

## Remote memory

Client attachments and local buffer lists form logical spaces by concatenating
buffer lengths. `CopyOp` addresses those spaces independently of segmentation.
`remote_memory/scatter.rs` validates bounds, overflow, region/op limits and
destination overlap, then fragments operations at buffer boundaries. The RDMA
dependency independently validates the owned destination plan before posting.

| Component | Role |
|---|---|
| `read_source.rs` | Immutable sources shared by wrappers, pending requests and inline readers |
| `write_target.rs` | Exclusive destination checkout for CPU copies or owned RDMA READ plans |
| `inline.rs`, `services/memory_service.rs` | Reverse-RPC fallback over TCP/WS/HTTP |
| `with_buffers.rs` | Completed-write witnesses and typed buffer-returning responses |
| `rdma/rdma_socket/read.rs` | Concrete READ planning and admission |

RDMA transfers use READ in both directions: `remote_write` asks the client to
read the server's source. Application reads validate the original waiter after
READ completion; large serialized messages use a separate finish protocol.
See the [API guide](docs/remote-memory.md) for use and recovery, and
[safety boundaries](docs/safe-boundaries.md) for success assumptions and failure behavior.

## RDMA execution

CQ poll threads route completion tokens and batch received buffers. Dispatch
workers parse frames and invoke routing away from the poll thread. Each
connection owns its NIC pair; peer identity is its complete normalized group
of user-declared bootstrap addresses. Group addresses must share server state
and lease records; different or overlapping groups remain independent.

| Contract | Reference |
|---|---|
| Bootstrap, path selection and peer maintenance | [Connection lifecycle](docs/rdma-connection.md) |
| CQ-issued evidence, WR sequences and QPN reuse | [Completion identity](docs/wrid.md) |
| CQ reservations, send queues and READ limits | [Capacity](docs/rdma-capacity.md) |
| Completion routing, bounded drains and shutdown | [Poller](docs/qp-registry.md) |

## Buffer-pool ownership

The pool combines buddy allocation, slabs and thread caches. Public buffers
retain the pool; internal backing tokens avoid ownership cycles. See the
[bufpool guide](ruapc-bufpool/README.md) for allocation, lock order and internals.

`ruapc::Devices` aliases `DeviceSet<RdmaDevice>` with RDMA enabled and
`DeviceSet` otherwise. TCP occupies index zero. RDMA registration delegates to
`ActiveDevice` in `ruapc-rdma/src/buffer_registration.rs`; TCP registration stays
in `ruapc-bufpool/src/tcp_device.rs`. The unsafe registrar and collection contracts
are documented in [registration and byte access](docs/safe-boundaries.md#registration-and-byte-access).

## Generated services and examples

`ruapc-macro` separates attribute parsing, a validated service/method model and
token generation. Unsupported signatures produce diagnostics at their source
spans; conditional compilation carries through generated declarations and
registrations. Buffer-returning contracts follow types, including aliases.

<a id="validation"></a>

The [demo guide](ruapc-demo/README.md) owns binary setup, CLI behavior, workload
verification and its source map. See [CONTRIBUTING.md](CONTRIBUTING.md) for
checks and [benchmarks](docs/benchmark.md) for performance comparisons.
