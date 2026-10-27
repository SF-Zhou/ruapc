# Contributing to RuaPC

Read [DESIGN.md](DESIGN.md) for module boundaries and use the
[documentation index](docs/README.md) to find the contract affected by a change.

## Prerequisites

| Work | Requirements |
|---|---|
| Standalone schema and wire tests | Stable Rust; no RDMA libraries or device required |
| Downstream TCP/WS/HTTP application | Stable Rust; RDMA disabled by default |
| Workspace builds, tests and RPC benchmarks | Stable Rust, C compiler, `pkg-config`, libclang and libibverbs development files (`libibverbs-dev` on Debian/Ubuntu) |
| Tests that open RDMA devices | Usable RDMA NIC or Soft-RoCE and sufficient locked-memory allowance |

`--all-features` enables the RDMA crate's optional
[`ibv-query-gid-ex` backend](ruapc-rdma/README.md#gid-query-backends), which
requires libibverbs headers and a runtime library from rdma-core v32 or later.

The core crate's self dev-dependency enables RDMA for its tests and benchmarks.
Selecting a TCP test does not remove that build dependency. Stream-only demo
tests and allocator, macro, schema and wire tests can run without an
RDMA device:

```bash
cargo test -p ruapc-bufpool -p ruapc-macro -p ruapc-schema
cargo test -p ruapc-wire
cargo test -p ruapc-demo --no-default-features
```

To check schema generation, OpenAPI projection and reflection without building
the RPC runtime, run `cargo test -p ruapc-schema`.

`cargo test -p ruapc-wire --all-features` covers message codecs, direct buffer
serialization and pure RDMA framing without the RDMA build prerequisites.
Registered-buffer payload ownership and typed RDMA error conversion belong
to `ruapc` and are covered by its tests.

## Workflow

Branch from `main`, add tests for changed behavior, and run:

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy -p ruapc-rdma --all-targets --no-default-features -- -D warnings
cargo test -p ruapc-rdma --no-default-features
cargo test --workspace --all-features
```

The separate `ruapc-rdma` checks exercise its default ioctl/sysfs GID backend;
the all-feature workspace checks exercise the libibverbs backend.

Run formatting and Clippy before committing. PRs target `main`, and all CI
checks must pass before merging. [CI](.github/workflows/rust.yml) runs formatting,
release Clippy and coverage on Linux x86-64 and ARM64 with Soft-RoCE.

Prefer file-level imports over inline absolute paths: import the item once
(`use crate::module::Item;`) and refer to `Item` in the body rather than
repeating `crate::module::Item`. Keep an inline path for visibility
restrictions such as `pub(in crate::module)`, for names that would collide in
the local scope, inside macro-generated token streams, and for feature-gated
items whose `cfg` cannot be carried by the import unchanged.

For public API, feature or rustdoc changes, also check:

```bash
cargo check -p ruapc --no-default-features --lib
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS='-D warnings' cargo doc -p ruapc --no-default-features --no-deps
```

## RDMA tests

For Soft-RoCE, replace `eth0` with an active Ethernet interface on the test host:

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe_0 type rxe netdev eth0
sudo prlimit --pid $$ -l=unlimited
RUAPC_PREFER_RXE=1 cargo test --workspace --all-features
```

`RUAPC_PREFER_RXE` selects devices whose names begin with `rxe`; it does not
create one. With physical RDMA hardware, omit that environment variable.

## Design and validation

- Allocation, serialization, dispatch and RDMA posting changes require release
  comparisons using the [benchmark method](docs/benchmark.md#comparing-results).
- Remote-memory changes must exercise cancellation, failure and forgotten
  futures against the [ownership and validation contracts](docs/safe-boundaries.md).
  Check both memory recovery and result validity; one does not imply the other.
- Documentation changes belong in the [owning guide](docs/README.md#maintaining-the-docs).
  Keep examples, links and public API descriptions consistent with the code.

Release tags publish bufpool, macro, schema, RDMA, wire and core in dependency
order through [the release workflow](.github/workflows/release.yml). The demo is
not published.

<a id="license"></a>

Contributions are dual-licensed under MIT and Apache-2.0.
