# Contributing to RuaPC

## Getting Started

1. Fork and clone the repository
2. Install Rust nightly (required for `#![feature(return_type_notation)]`)
3. Build: `cargo build --all-features`
4. Test: `cargo test --all-features`

## Development Workflow

1. Create a feature branch from `main`
2. Make your changes
3. Run `cargo fmt` and `cargo clippy --all-features` — ensure zero warnings
4. Run `cargo test --all-features` — ensure all tests pass
5. Submit a pull request targeting `main`

## Code Style

- Follow standard Rust conventions
- Run `cargo fmt` before committing
- Run `cargo clippy --all-features` and resolve all warnings
- Add tests for new functionality

## RDMA Development

RDMA features require `libibverbs-dev`. For testing without physical RDMA hardware, use the `rxe` (Soft-RoCE) kernel module:

```bash
sudo modprobe rdma_rxe
sudo rdma link add rxe_0 type rxe netdev lo
```

## License

By contributing, you agree that your contributions will be dual-licensed under MIT and Apache-2.0.
