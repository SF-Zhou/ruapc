# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.3] - 2026-04-11

### Added
- HTTP/2 h2c support with automatic HTTP/1.1 and HTTP/2 protocol negotiation (#37)
- Reverse RPC: server can call back into client services over HTTP/2 bidirectional streaming (#37)
- OpenAPI 3.0 specification auto-generation with JSON Schema support (#17)
- RapiDoc integration for interactive API documentation (#18)
- Message ID (UUID) validation on server side (#28)
- Comprehensive API documentation and doc comments (#26, #30)

### Changed
- HTTP client switched from HTTP/1.1 to HTTP/2 with single-connection multiplexing (#37)
- Message ID allocation moved to client side (#27)
- Socket module refactored for cleaner architecture (#34)

### Fixed
- Message ID leak in waiter (#15)
- RDMA `ibv_reg_mr` return value check (#24)
- HTTP socket content type (#21)
- RDMA socket periodic health check (#16)
- CI: prefer RXE device in RDMA unit tests for reliable CI (#36)

## [0.1.2] - 2025-08-26

### Added
- RDMA transport support (optional `rdma` feature) (#13)
- Unified socket pool: single port for TCP, WebSocket, and HTTP (#10)
- HTTP transport (#9)
- `MetaService::list_methods` for service discovery (#11)
- JSON Schema support for request/response types (#7)
- Task supervisor for graceful async task management (#6)
- Body-less request support (#12)

### Fixed
- MessagePack deserialization failure (#4)

## [0.1.1] - 2025-07-26

### Added
- Initial release
- TCP and WebSocket transport
- MessagePack serialization support
- RPC callback (bidirectional RPC over TCP)
- Proc macro `#[service]` for service definition

### Fixed
- Waiter cleanup on timeout (#1)
- RPC callback failure (#2)
