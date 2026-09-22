# Contributing to RuaPC

Install stable Rust, a C compiler, `pkg-config`, libclang and the libibverbs
development package (`libibverbs-dev` on Debian/Ubuntu). Workspace tests enable
RDMA even though downstream `ruapc` users can leave the feature disabled.

## Workflow

Create a branch from `main`, make the change, and run:

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run `cargo fmt --all` to apply formatting. Add tests for changed behavior and
target pull requests at `main`. All CI checks must pass before merging.
[CI](.github/workflows/rust.yml) checks formatting, release clippy and test coverage
on Linux x86-64 and ARM64 with Soft-RoCE.

Check feature boundaries and documentation when public APIs, features or rustdoc change:

```bash
cargo check -p ruapc --no-default-features --lib
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p ruapc --no-default-features --no-deps
```

## Design and validation

Read [DESIGN.md](DESIGN.md) for module responsibilities and ownership rules, and
the [documentation index](docs/README.md) for transport details.

Changes to allocation, serialization, dispatch or RDMA posting need release
benchmark comparisons with the same lockfile, CPU/NUMA placement and workload.
Run benchmarks sequentially and repeat measurements to separate changes from
noise. See [benchmark instructions](docs/benchmark.md).

For remote-memory changes, validate cancellation, failure and forgotten futures.
Local CPU readers must retain their sources; posted DMA destinations must stay
owned until completion or successful QP destruction. A Rust borrow or a
post-transfer liveness probe cannot replace that ownership. Source recovery
does not establish remote one-sided completion. See [safety boundaries](docs/safe-boundaries.md).

## RDMA tests

Tests that open devices need a working RDMA NIC or Soft-RoCE device. For Soft-RoCE,
replace `eth0` with an active Ethernet interface on the test host:

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe_0 type rxe netdev eth0
sudo prlimit --pid $$ -l=unlimited
RUAPC_PREFER_RXE=1 cargo test --workspace --all-features
```

`RUAPC_PREFER_RXE` restricts test device selection to names beginning with `rxe`;
it does not create a device. Without RDMA hardware, allocator and macro tests
can be run independently:

```bash
cargo test -p ruapc-bufpool -p ruapc-macro
```

## License

Contributions are dual-licensed under MIT and Apache-2.0.
