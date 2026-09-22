# Documentation

Start with the [project README](../README.md) for service definitions, clients
and examples. Public API details live in rustdoc; build them with
`cargo doc -p ruapc --no-deps`.

| Topic | Document |
|---|---|
| Workspace, request lifecycle and ownership | [Architecture](../DESIGN.md) |
| Build requirements and validation | [Contributing](../CONTRIBUTING.md) |
| Registration, DMA ownership and known limitations | [Safety boundaries](safe-boundaries.md) |
| Reflection and internal RPC services | [Built-in services](builtin-services.md) |
| RDMA discovery and connection setup | [Connection lifecycle](rdma-connection.md) |
| WRIDs, completion authority and QPN reuse | [Completion identity](wrid.md) |
| CQ budgets, READ admission and retirement | [RDMA capacity](rdma-capacity.md) |
| Completion routing and poller maintenance | [QP registry](qp-registry.md) |
| Workloads, measurement controls and comparison method | [Benchmarks](benchmark.md) |
| Released changes and migration notes | [Changelog](../CHANGELOG.md) |

These documents describe the current implementation. Historical experiment
reports, raw samples, build logs and rejected patches are available in Git
history; they are not maintained alongside current documentation. Keep new
benchmark outputs outside `docs/` and record their source revision, workload
and environment when sharing results.
