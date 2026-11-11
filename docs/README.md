# Documentation

Start with the [crate quick start](../ruapc/README.md#quick-start). For public API details,
run `cargo doc -p ruapc --no-deps --open`.

## Use and operate RuaPC

| Task | Guide |
|---|---|
| Understand the service and remote-memory design through a complete example | [Project README](../README.md#example) |
| Define a service and make a typed call | [Quick start](../ruapc/README.md#quick-start) |
| Address one server through multiple equivalent addresses | [Server endpoints](endpoints.md) |
| Run examples, HTTP calls or verified load tests | [Demo](../ruapc-demo/README.md) |
| Attach buffers, copy logical ranges and recover memory | [Remote memory](remote-memory.md) |
| Allocate and register buffers | [Buffer pool](../ruapc-bufpool/README.md) |
| Validate and plan memory copies without the RPC runtime | [Memory crate](../ruapc-memory/README.md) |
| Use the low-level verbs crate | [RDMA crate](../ruapc-rdma/README.md) |
| Generate schemas and OpenAPI documents without the RPC runtime | [Schema crate](../ruapc-schema/README.md) |
| Encode and decode RPC messages without the RPC runtime | [Wire crate](../ruapc-wire/README.md) |
| Set up a development or test environment | [Contributing](../CONTRIBUTING.md) |
| Measure and compare performance | [Benchmarks](benchmark.md) |
| Upgrade between releases | [Changelog](../CHANGELOG.md) |

## Implementation contracts

Each guide owns a distinct part of the design. Start at
[architecture](../DESIGN.md) for the module map and request lifecycle.

| Contract | Reference |
|---|---|
| Metadata, payload formats and framing | [Wire format](../DESIGN.md#message-ownership-and-framing) |
| Endpoint strings, complete-group identity and address fallback | [Server endpoints](endpoints.md) |
| Registration, DMA ownership, source validation and recovery | [Safety boundaries](safe-boundaries.md) |
| Built-in RPCs, discovery and payload transfer methods | [Built-in services](builtin-services.md) |
| RDMA bootstrap, negotiation, paths and peer maintenance | [Connection lifecycle](rdma-connection.md) |
| WRIDs, completion authority and QPN reuse | [Completion identity](wrid.md) |
| Queue/byte budgets, READ admission and CQ credit retirement | [Capacity](rdma-capacity.md) |
| CQ routing, dispatch, bounded drains and shutdown | [Poller](qp-registry.md) |
| Service declaration rules and generated API | [Service macro](../ruapc-macro/src/lib.rs) |

## Maintaining the docs

Update the guide that owns a contract; other pages should summarize it and
link there. Keep the root [README](../README.md) focused on the project's core
design, illustrated by a complete service and remote-memory example. Crate
READMEs cover installation, first use and their APIs; DESIGN covers module
responsibilities, and rustdoc covers each caller's obligations.
The [core crate README](../ruapc/README.md) owns the minimal service quick start
and is published on crates.io; use absolute links there. Cross-reference the
two introductions and keep their examples complementary. Avoid copying
configuration tables, protocol proofs or validation commands between these layers.

Document current behavior in English, matching code names and units. Keep
historical changes in the changelog and raw benchmark samples, build logs and
rejected experiments outside `docs/`. Preserve useful section anchors and check
links and examples when moving content.
