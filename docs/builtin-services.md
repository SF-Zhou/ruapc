# Built-in RPC services

RuaPC uses the `_ruapc.*` wire-service namespace for framework services and
reserves it by convention. Application services keep their trait name by
default, or can select an independent wire name with
`#[ruapc::service(name = "...")]`.

All RPC responses use RuaPC's serialized `Result<T, Error>` envelope. A `()`
request is JSON `null` (and MessagePack `nil`). Method names below are the exact
wire names.

## Public reflection service

`ReflectionService` is part of the public Rust API and is registered as
`_ruapc.meta`.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `_ruapc.meta/describe` | `DescribeRequest` | `ServerDescription` | Returns a sorted, optionally filtered catalog of public services and their schemas. |
| `_ruapc.meta/openapi` | `()` | `serde_json::Value` | Returns the complete OpenAPI 3.0 document for public methods. |

The structured description types are:

```text
DescribeRequest
└─ service: Option<String>       exact wire-service filter; None means all

ServerDescription
├─ protocol_version: u32         reflection contract version
├─ ruapc_version: String         serving crate version
├─ services: Vec<ServiceDescription>
└─ components: serde_json::Value OpenAPI components used by schema $refs

ServiceDescription
├─ name: String                  wire-service name
└─ methods: Vec<MethodDescription>

MethodDescription
├─ name: String                  service-local method name
├─ request_schema: Schema
└─ response_schema: Schema       includes the Result envelope
```

`Router::method_names()` and `Router::method_schemas()` expose the same public
view in-process. `Router::is_public_method()` is useful at protocol boundaries.
Internal methods do not appear in any of these APIs or in OpenAPI.

## Internal remote-memory service

`_ruapc.memory` implements reverse RPCs used by `Context::remote_read` and
`Context::remote_write`. Its trait and data types are crate-private.

| Method | Request | Success value | Data path |
|---|---|---|---|
| `_ruapc.memory/read_inline` | `ReadInlineRequest` | `ReadInlineResponse` | TCP/WS/HTTP remote read; returns copied bytes inline. |
| `_ruapc.memory/write_inline` | `WriteInlineRequest` | `()` | TCP/WS/HTTP remote write; carries copied bytes inline. |
| `_ruapc.memory/read_into_target` | `ReadIntoTargetRequest` plus `MsgMeta.read_regions` | `()` | RDMA remote write; the client RDMA-READs into its pinned target. |
| `_ruapc.memory/request_is_pending` | `RequestStatusRequest` | `bool` | Post-READ request-liveness check for one-sided RDMA reads. |

```text
ReadInlineRequest
├─ ops: Vec<CopyOp>
└─ request_id: u64

ReadInlineResponse
└─ bytes: Vec<u8>

WriteInlineRequest
├─ request_id: u64
├─ ops: Vec<CopyOp>
└─ bytes: Vec<u8>

ReadIntoTargetRequest
├─ request_id: u64
└─ ops: Vec<CopyOp>

RequestStatusRequest
└─ request_id: u64
```

`read_inline` and `write_inline` are separate because they move bytes in
opposite request/response directions. `read_inline` resolves `request_id` to an
owned immutable source on the local waiter and validates the logical ops against
that source; the peer does not supply memory addresses. The handler retains
source ownership through the CPU copy, independently of the caller's future.
`read_into_target` is also distinct: it
starts client-side RDMA READ work and relies on pinned write buffers. The
pending-request probe rejects stale one-sided READ results. It is an internal
liveness check, not a source-side acknowledgement of remote DMA completion or a
public metadata API.

## Internal RDMA bootstrap service

When the `rdma` feature is enabled, `_ruapc.rdma` is used over a TCP bootstrap
connection to discover paths and establish queue pairs. The current bootstrap
protocol version is **2**; peers must advertise exactly this version. There is
no compatibility branch for older versions. See [RDMA connection establishment](rdma-connection.md)
for the sequence, negotiation, leases, rollback, and diagnostic logs.

| Method | Request | Success value | Purpose |
|---|---|---|---|
| `_ruapc.rdma/discover` | `()` | `RdmaPeerAdvertisement` | Advertises the protocol version and currently connectable devices. |
| `_ruapc.rdma/prepare_connection` | `PrepareConnectionRequest` | `PrepareConnectionResponse` | Creates the acceptor QP and returns its endpoint, lease, and actual limits. |
| `_ruapc.rdma/commit_connection` | `ConnectionLease` | `()` | Idempotently confirms that the initiator retained the QP. |
| `_ruapc.rdma/cancel_connection` | `ConnectionLease` | `()` | Best-effort cleanup when setup cannot complete. |

```text
RdmaPeerAdvertisement
├─ protocol_version: u32
└─ devices: Vec<RdmaDeviceInfo>
   ├─ name: String
   ├─ active_connections: u32
   ├─ limits: RdmaConnectionLimits
   └─ ports: Vec<RdmaPortInfo>

RdmaPortInfo
├─ port_num: u8
├─ link_layer: LinkLayer
└─ gids: Vec<Gid>

Gid
├─ index: u8
├─ gid: ibv_gid
└─ gid_type: GidType

RdmaConnectionLimits             owner's directional limits
├─ max_send_wr: u32
├─ max_recv_wr: u32
├─ recv_queue_len: u32
└─ max_msg_size: u32

RdmaQpEndpoint
├─ qp_num: u32
├─ port_num: u8
├─ gid_index: u8
├─ lid: u16
├─ gid: ibv_gid
├─ link_layer: LinkLayer
├─ active_mtu: ibv_mtu
├─ psn: u32
└─ rd_atomic_cap: u8

DeviceSelection
├─ device_name: String
├─ port_num: u8
└─ gid_index: u8

PrepareConnectionRequest
├─ attempt_id: u64
├─ endpoint: RdmaQpEndpoint
├─ source_device: String
├─ same_connectivity_domain: bool
├─ target: DeviceSelection
├─ limits: RdmaConnectionLimits  initiator's resolved limits
└─ traffic_class: u8

PrepareConnectionResponse
├─ endpoint: RdmaQpEndpoint
├─ lease: ConnectionLease
└─ limits: RdmaConnectionLimits  acceptor's actual resolved limits

ConnectionLease
├─ attempt_id: u64
└─ accepted_connection_id: u64
```

Limits cross directions during negotiation: local send is capped by peer
receive, and local receive is capped by peer send. Scatter/gather limits are
local QP properties and are therefore not sent on the wire. Completion queues
are shared per device, so there is no per-connection CQ field either. Devices
with no currently usable advertised port are omitted even when discovery keeps
a DOWN device locally for later port refresh.

The prepare response must exactly mirror the initiator's resolved send/receive
limits and match its receive-ring length and message-size limit. Changed
capabilities since discovery therefore fail setup instead of leaving the peers
with inconsistent runtime settings. Commit confirms ownership; activation also
requires a successful data-plane receive and completes asynchronously after
stripe publication.

## Exposure rules

| Entry point | Public methods | Internal methods |
|---|---:|---:|
| Framed peer RPC (TCP/WS/HTTP stream/RDMA) | yes | yes |
| Unary HTTP `/Service/method` | yes | no (404) |
| OpenAPI and reflection | yes | no |

`#[ruapc::service(internal)]` marks a dispatchable service as internal. Router
registration rejects duplicate wire method names instead of replacing the
existing handler.
