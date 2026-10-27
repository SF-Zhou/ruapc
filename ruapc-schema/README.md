# ruapc-schema

Schema generation and OpenAPI projection for RuaPC. The crate has no dependency
on `ruapc`, an async runtime, transports or registered memory.

`SchemaRegistry::register` generates request and complete wire-response schemas.
All methods share one definition store.
`SchemaRegistry::build` projects the caller's method directory into an
OpenAPI 3.0 document without consuming the definitions.

Callers retain responsibility for validated wire names; runtime registration
and dispatch remain in `ruapc`.

See the [architecture](../DESIGN.md) for runtime ownership. Public types
and their caller obligations are documented in rustdoc.

```bash
cargo test -p ruapc-schema
```
