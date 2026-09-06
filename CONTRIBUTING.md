# Contributing to RuaPC

## Getting Started

1. Fork and clone the repository
2. Install stable Rust (via [rustup](https://rustup.rs))
3. Install a C compiler, pkg-config, libclang for bindgen, and the libibverbs
   development package. Workspace tests enable RDMA even though downstream
   `ruapc` users can disable that feature.
4. Build: `cargo build --workspace --all-features`
5. Test: `cargo test --workspace --all-features`

## Development Workflow

1. Create a feature branch from `main`
2. Make your changes
3. Run `cargo fmt` and `cargo clippy --workspace --all-targets --all-features -- -D warnings` — ensure zero warnings
4. Run `cargo test --workspace --all-features` — ensure all tests pass
5. Submit a pull request targeting `main`

Also check the optional-feature boundary and generated documentation:

```bash
cargo check -p ruapc --no-default-features --lib
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p ruapc --no-default-features --no-deps
```

Read [DESIGN.md](DESIGN.md) for module responsibilities and lifetime invariants.
When changing allocation, serialization, dispatch or RDMA posting, compare
release benchmarks against a saved baseline using the same `Cargo.lock`, CPU
placement and workload. Keep benchmark processes sequential and distinguish
repeatable changes from run-to-run noise. See [docs/benchmark.md](docs/benchmark.md).

For remote-memory changes, account for cancellation and forgotten futures:
background CPU copies must hold owned sources, and posted DMA work must retain
its destination through every completion. A Rust borrow or a post-transfer
liveness probe alone does not establish those ownership guarantees. Read-source
recovery is conditional and must preserve the source when readers still hold it.

## Code Style

- Follow standard Rust conventions
- Run `cargo fmt` before committing
- Run `cargo clippy --workspace --all-targets --all-features -- -D warnings` and resolve all warnings
- Add tests for new functionality

## RDMA Development

RDMA features require `libibverbs-dev`. For testing without physical RDMA hardware, use the `rxe` (Soft-RoCE) kernel module:

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe_0 type rxe netdev lo
```

## License

By contributing, you agree that your contributions will be dual-licensed under MIT and Apache-2.0.
