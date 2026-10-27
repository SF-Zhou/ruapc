# Documentation

Start with the [project quick start](../README.md#example). For public API details,
run `cargo doc -p ruapc --no-deps --open`.

## Use and operate RuaPC

| Task | Guide |
|---|---|
| Define a service and make a typed call | [Quick start](../README.md#example) |
| Address one server through multiple equivalent addresses | [Server endpoints](endpoints.md) |
| Run examples, HTTP calls or verified load tests | [Demo](../ruapc-demo/README.md) |
| Attach buffers, copy logical ranges and recover memory | [Remote memory](remote-memory.md) |
| Allocate and register buffers | [Buffer pool](../ruapc-bufpool/README.md) |
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
| Built-in RPCs, discovery and private payload control fields | [Built-in services](builtin-services.md) |
| RDMA bootstrap, negotiation, paths and peer maintenance | [Connection lifecycle](rdma-connection.md) |
| WRIDs, completion authority and QPN reuse | [Completion identity](wrid.md) |
| Queue/byte budgets, READ admission and CQ credit retirement | [Capacity](rdma-capacity.md) |
| CQ routing, dispatch, bounded drains and shutdown | [Poller](qp-registry.md) |
| Service declaration rules and generated API | [Service macro](../ruapc-macro/src/lib.rs) |

## Maintaining the docs

Update the guide that owns a contract; other pages should summarize it and
link there. Keep README focused on first use, DESIGN on module responsibilities,
crate READMEs on their APIs, and rustdoc on each caller's obligations. Avoid
copying configuration tables, protocol proofs or validation commands between
these layers.

Document current behavior in English, matching code names and units. Keep
historical changes in the changelog and raw benchmark samples, build logs and
rejected experiments outside `docs/`. Preserve useful section anchors and check
links and examples when moving content.
