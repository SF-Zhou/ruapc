# RuaPC architecture

RuaPC is a Rust workspace for bidirectional RPC over TCP, WebSocket, HTTP
and optional RDMA. This document describes the implementation and the
invariants that a change must preserve.

## Workspace and dependency direction

| Crate | Responsibility | Depends on |
|---|---|---|
| `ruapc-bufpool` | Registered memory, buddy allocation, slab and thread caches | Device registration contracts |
| `ruapc-rdma` | libibverbs ownership, owned work requests and completion evidence | `ruapc-bufpool` |
| `ruapc-macro` | Parse and validate service traits, generate calls and handlers | Rust syntax and token libraries |
| `ruapc` | Request lifecycle, routing, transport policy and remote memory | The three crates above; RDMA is optional |
| `ruapc-demo` | Runnable services and verified workloads | `ruapc` |

Runtime transport selection uses concrete enum variants. Async transport
calls remain statically dispatched; no boxed future is introduced to unify
transports. The router's registered service closures are the application
extension boundary: they capture arbitrary user service implementations and
schedule concrete handler futures.

The core crate enforces `#![forbid(unsafe_code)]`. Allocator, registration and
verbs code belongs in its dependencies. Their safe APIs must own the memory
used by hardware and validate the evidence for returning it; accepting raw
addresses alongside a caller's ownership promise does not establish this boundary.

## Core library boundaries

| Module | Owns | Does not own |
|---|---|---|
| `client` | Client settings, attachments, acquire/send retries, response decoding, generated-call contracts | Transport framing and server dispatch |
| `core` | Contexts, endpoint health, server/listener state, method registry and handler admission | Memory-transfer algorithms |
| `core/router/schema` | Public/internal schema generation and OpenAPI projection | Runtime method invocation |
| `msg` | Metadata, payload decoding, direct serialization and shared stream framing | Connections or scheduling |
| `sockets` | Concrete transports, connection queues, connection maps and close-once state | Registered-memory ownership |
| `remote_memory` | Logical spaces, copy validation, inline fallback, owned read sources, pinned write targets and completion witnesses | QP work-request posting |
| `rdma` | Path selection, bootstrap, connections, READ execution and CQ polling | Application dispatch policy |
| `task` | Supervised background tasks and request/response correlation | Handler cancellation policy |
| `metrics` | Cached facade handles and scoped request accounting | Rendering or exporting metrics |

`Context` combines shared `State`, a destination, incoming metadata and a
propagated deadline. Its `remote_*` methods are implemented in
`remote_memory/context.rs`; the context module itself concerns request
lifetime and response delivery. A context created for another endpoint
inherits the deadline but starts with fresh message metadata.

## Request lifecycle

1. A generated service method enters `client/call.rs`, which selects the
   plain or buffer-returning contract by type. Both reach the same request
   executor in `client/request.rs`.
2. The executor validates the destination and attachments, ranks endpoints
   and acquires a connection under a connection deadline. Alternative
   endpoints can preconnect through the pool's task supervisor.
3. After acquisition and bandwidth admission, the executor allocates a
   waiter and binds the wrapper's existing read source and write target to it.
   Connection setup does not consume
   the response budget. Nested calls cap both budgets at the parent deadline.
4. The transport binds the waiter to its process-unique connection ID before
   checking closure and enqueueing the bytes. TCP, WebSocket and HTTP/2 share
   this ordering in `sockets/channel.rs`.
5. Acquire/send failures can retry only when the transport has not accepted
   the message. Once sent, the client awaits exactly one result: an ambiguous
   timeout or connection loss is never retried automatically.
6. The server reserves in-flight capacity atomically, rejects overload and
   drops expired requests before invoking a handler. Once started, a handler
   runs to completion; long-running handlers can inspect `Context::is_expired`.
7. Response, connection failure or the periodic expiry sweep removes the
   waiter. Unary HTTP creates its waiter only after the complete request body
   arrives, matching framed transports. Cancelling a pending receive removes
   its waiter through a cleanup guard.
   Method accounting settles through a guard on completion, cancellation and
   panic, without registering a timer for each request.

Connection teardown is idempotent. A stale connection may evict only itself,
never a replacement stored under the same peer address. RDMA additionally
waits for outstanding DMA ownership to settle before removal.

## Message ownership and framing

`msg/meta.rs` defines routing fields and payload-format flags. Metadata is
always MessagePack with field names, so decoding never depends on a flag
inside the yet-to-be-decoded metadata. `Client::default()` selects MessagePack
payloads; clearing its `use_msgpack` option selects JSON.

`msg/encode.rs` writes metadata and payload directly into transport-owned
storage. It backfills lengths after serialization. `msg/message.rs` checks
bounds before reading metadata and advances the payload view without copying
its bytes.

| Carrier | Frame |
|---|---|
| TCP, HTTP/2 bidirectional stream | `[RUA!: 4B][body_len: 4B][meta_len: 4B][meta][payload]` |
| WebSocket binary message | `[meta_len: 4B][meta][payload]` |
| RDMA receive buffer | One or more `[frame_len: 4B][meta_len: 4B][meta][payload]` frames |
| Unary HTTP | JSON request/response bodies with routing supplied by the URL |

TCP and HTTP/2 share `msg/frame.rs`, including the send/receive size limit and
incremental parser. Incomplete frames remain buffered; complete bodies are
split out without copying. RDMA framing lives in `rdma/frame.rs` and is
independent of the polling machinery. Aggregated RDMA frames consume one
receive credit for the entire SEND, irrespective of message count.

## Remote memory

A read or write space is an ordered list of registered regions, concatenated
by logical length. A local buffer list forms the same kind of space.
`CopyOp { src_offset, dst_offset, len }` addresses those logical spaces, so
local and remote segmentation can differ.

`remote_memory/scatter.rs` owns bounds, overflow, region/op limits and
non-overlapping destination validation, then fragments ranges at segment
boundaries. `remote_memory/context.rs` validates the logical operation while
borrowing the caller's buffers. After ownership moves into the RDMA dependency,
it independently validates the concrete destination plan before posting and
returns the buffers if validation fails. `RemoteIoError` carries recoverable
local buffers; once work has been posted, failures can return no buffers even
if the QP has since completed and recycled them.

Client read attachments take ownership through `with_read_buffer(Buffer)` or
`with_read_buffers(Vec<Buffer>)`. Calling either method on an existing wrapper
replaces its source list. `remote_memory/read_source.rs` owns the immutable source
and CPU-copy logic. The wrapper shares an `Arc<ReadSource>` with pending
requests and local reverse-RPC readers, so cancelling or forgetting a request
future cannot release memory while a CPU copy still uses it. `read_buffers()`
provides shared views, and the wrapper can issue more requests using the same
source. `take_read_buffers(&mut self)` recovers ownership only when its source
is uniquely held; otherwise it returns `None` and retains the source for a
later recovery attempt.

Byte-stream transports use `remote_memory/inline.rs`: reverse RPCs gather
source ranges into an inline body and scatter them into destinations. The
peer independently validates requests in `services/memory_service.rs`.
`read_inline` identifies the original request and copies from its owned source;
it does not accept peer-supplied addresses or look them up in a registration map.

RDMA transfers always use READ. For `remote_write`, the server advertises its
source buffers and the client reads into its own pinned destination buffers.
`Arc<WriteTarget>` is held both by the waiter and by in-flight writers. Its
`Mutex<Option<Vec<Buffer>>>` grants either CPU access or an exclusive transfer:
`take_for_read` removes the entire destination vector before passing it to the
dependency's owned READ plan. While the slot is empty, `copy_in`, region export
and another READ checkout return `BuffersInUse`. This excludes concurrent CPU
and NIC writes, including competing reverse RPCs for the same request.

`restore_after_read` puts back only buffers returned by the dependency: after
successful completion, or an error before anything was posted. Cancellation or
a posted-operation failure can leave the target empty; the QP then retains and
eventually recycles its buffers. A timeout cannot recover those destinations
through the now-empty target.

A completed write returns `SentBuffers`; combining it with a response yields
`WithBuffers<T>`. A handler with no transfer explicitly uses `sent_nothing()`.
The generated client contract returns completed write buffers with a successful
response when their target can be uniquely recovered. Failed calls can recover
available destinations with `take_write_buffers()`; any remaining transfer keeps
its buffers until it finishes. This contract is based on types, not the spelling
of an alias.

Read-source ownership covers local CPU readers. It is not a remote DMA completion
lease: after source-side timeout or cancellation, the source side cannot observe
when an already posted one-sided READ finishes. The existing post-READ
`request_is_pending` check rejects data for an expired request, but does not
acknowledge remote completion before source recovery or reuse. Destination-side
READ batches separately retain their memory until every posted completion arrives
or their QP is successfully destroyed. Local destination ownership does not
remove the source-side limitation.

## RDMA execution

`ruapc/src/rdma/rdma_socket.rs` owns connection identity, its QP and message
sending. `rdma_socket/read.rs` translates logical copy operations into remote
ranges and local buffer-index/offset/length descriptors, applies admission
limits and awaits results. It never supplies local raw addresses or keys.

`ruapc-rdma/src/verbs/queue_pair/read.rs` owns READ preparation and completion
lifetime. Its private bookkeeping is inline in the QP. The connection handshake
keeps its temporary `LocalConnection` in a box across peer negotiation, then
moves the QP directly into the established socket. RPC client futures therefore
do not carry the entire temporary QP during connection acquisition.
`QueuePair::prepare_reads` consumes the destination `Vec<Buffer>`,
validates bounds, overflow, scatter limits and destination overlap across all
requests, then resolves addresses and keys from those owned buffers. The
non-cloneable `ReadPosting` cursor is bound to the preparing QP and advances
once per successful post. The QP records its private batch hold before the NIC
can observe a work request and rolls that record back if posting fails.

Dropping a posting cursor accounts the unposted suffix. Already posted work
retains memory independently of the posting task and result receiver. Explicit
cancellation before the first successful post can recover the buffers; after
posting, neither dropping a future nor notifying an error releases them early.
Forgetting an owner can retain memory indefinitely. Successful completion of
the whole batch returns the vector; failed batches recycle it only after their
posted work has settled.

READ admission combines per-device concurrency, a per-connection SQ guard,
negotiated atomic-read capabilities and device-port bandwidth limits. The
poller's periodic sweep enforces READ timeouts without per-operation timers.
Timeout fails the waiter and moves the QP to ERR; it never force-recycles DMA
memory. Poller shutdown likewise fails receivers while leaving the QP's holds
intact. Normal connection removal waits for pending READs and the flow ledger
to settle. Final QP destruction precedes releasing any remaining work-request
buffers; a provider failure to destroy the QP aborts the process, since an error
state or failed destruction alone cannot prove that DMA has stopped.

`poller` separates CQ draining, maintenance and idle wakeup. Dispatch workers
parse received frames away from the poll thread. `poller/flow.rs` owns the
credit ledger: a data SEND slot is reusable only after local completion and
remote receive acknowledgement. ACK fields have explicit bounds; credit that
does not fit remains pending for a later ACK.

`CompletionQueue::poll_batch` fills private `CompletionBatch` storage and lends
one non-cloneable token per CQE. Its borrow prevents the entries from being
changed while a token is live. `QueuePair::complete` consumes that token and
checks its CQ identity, QP number and CQ-owned route before
recovering SEND/RECV buffers or settling READ ownership. Copying raw metadata
does not copy this authority. A later RC SQ completion also permits reclamation
of earlier unsignaled SENDs.

WRIDs contain 2 type bits, a 14-bit CQ route slot and a 48-bit per-direction
sequence. The CQ allocates an exclusive route lease at QP creation, before any
work can be posted. A shared send/receive CQ uses one lease; separate CQs each
allocate a lease. QP destruction succeeds before either lease returns its slot.
Reuse preserves the maximum next sequence across SQ and RQ, advancing at least
once even for an unused lease. Every replacement therefore starts above all
sequences allocated by earlier occupants. Sequence allocation cannot wrap;
exhaustion permanently retires the slot. The core poller indexes an array by
slot and checks the occupant's sequence floor before changing flow accounting.
This replaces poller-assigned generation tags and the per-CQ claimed-tag bitmap.

Initial receive posting and publication to the poller's registration inbox hold
the same mutex. An early completion that misses its route takes that mutex,
drains registrations and retries, even if the empty-inbox hint has not yet been
updated. Established-route lookup does not acquire it. SEND reclamation starts
at the current route's floor rather than scanning sequences of previous QPs.

The low-level `QueuePair` submission helper owns SQ locking, WR-slot
registration and post-failure rollback for SEND, SEND-with-immediate and
gather SEND. Rollback removes ownership bookkeeping but never reuses the
allocated WRID. `WrSlots` returns a buffer immediately when its array position
is occupied; the submission reports `WorkRequestSlotsExhausted` instead of
spinning on a completion that might need the posting thread to make progress.
Raw DMA APIs remain explicitly unsafe in the dependency; the core uses the
owned submission and CQ-issued completion interfaces. See
[WRID allocation and completion routing](docs/wrid.md) for the allocation
proof, bounds and validation evidence.

Bootstrap and multi-NIC placement are described in
[RDMA connection lifecycle](docs/rdma-connection.md). Peer identity is the
bootstrap address; the local/remote NIC pair belongs to each connection.

## Buffer-pool ownership

`pool.rs` exposes allocation and configuration. Its private modules separate
builder defaults, lock-protected buddy state, slab coordination and async
waiter/reservation state. The fast allocation path returns directly; only a
miss enters the shared slow-path state machine.

Buddy blocks provide intrusive free lists and lazy four-way coalescing.
Small allocations use per-class slab locks and bounded thread-local caches.
A slab owns a backing-allocation token without an `Arc<BufferPool>`; public
buffers hold the pool alive. This avoids a pool → slab → buffer → pool cycle.
Chunk ownership is transferred rather than copied across cache boundaries.

`BuddyBlock` keeps registrations and backing memory immutable. `BuddyState`
contains the mutable tree and intrusive nodes behind `UnsafeCell`; accessing it
requires the pool lock. The allocator never creates a mutable reference covering
the entire published block. Self-referencing nodes are initialized after the
block has aliasable ownership, so subsequent moves do not invalidate their links.

Registered memory is initialized before any safe byte slice is exposed.
64-bit Linux allocations use anonymous zero-filled mappings with the required
alignment, so initial allocation does not eagerly touch every byte. Other
platforms use zeroed allocation. Reusing a buffer does not clear it; callers
must set its logical length to the bytes they intend to expose. Mutable
access to an `AlignedMemory` slice requires an exclusive borrow.

Device registrations are destroyed before their backing memory. Growth
failure restores reserved budget and wakes eligible waiters; cancellation
passes reserved capacity to another waiter or returns it to the allocator.

`ruapc::Devices` is a type alias for `ruapc_bufpool::DeviceSet<RdmaDevice>`
with RDMA enabled, or `DeviceSet` for stream transports. `DeviceSet` assigns
stable device indices, keeps TCP at index zero and registers memory directly
through each device's associated `MemoryRegistrar`. Safe `Device` wrappers
provide identity, metadata and a registrar reference; they never receive the
pool's backing memory. The core's `RdmaDevice` delegates registration to
`ActiveDevice`, whose audited implementation lives in
`ruapc-rdma/src/buffer_registration.rs`; TCP's implementation stays in
`ruapc-bufpool/src/tcp_device.rs`.

`MemoryRegistrar` and the custom collection trait `Devices` have unsafe
implementation contracts. Retaining backing memory for registration grants no
independent byte access while pool buffers exist, and registration destruction
must end its use of that region before returning. `DeviceSet` drops completed
registrations on partial failure. The RDMA registrar accepts only ordinary
virtual-address mappings; failed deregistration aborts before releasing memory.
Low-level registered TCP reads carry an
explicit access contract; normal RPC reads use their owned request source and
safe slices.

## Generated services and examples

`ruapc-macro` has three stages: attribute arguments, a validated service/method
model, and token generation. The model guarantees the shape expected by the
generator. Unsupported signatures produce diagnostics at the offending
syntax rather than panicking or silently dropping items. Conditional
compilation is propagated to generated declarations and registrations.

The demo crate shares setup in `app.rs` and verified echo/read/write workloads
in `workload.rs`. Read workloads create their owning source wrapper before timing
starts and reuse it across calls. Executables select workloads and report results;
they do not duplicate buffer-recovery and payload-validation logic.

## Validation

Run the workspace checks from [CONTRIBUTING.md](CONTRIBUTING.md). Functional
checks and performance measurements serve different purposes: tests establish
lifetime/protocol behavior, while release benchmarks compare equivalent
workloads on the same machine, dependency lock and CPU/memory placement.

The end-to-end workload is documented in [docs/benchmark.md](docs/benchmark.md).
Buffer-pool benchmarks cover contention, merging and initial allocation.
[Refactoring validation](docs/refactoring.md) records the comparison for this
restructure and its environmental limits.
