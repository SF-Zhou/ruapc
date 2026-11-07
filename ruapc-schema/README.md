# ruapc-schema

Schema generation and OpenAPI projection for RuaPC. The crate has no dependency
on `ruapc`, an async runtime, transports or registered memory.

`SchemaRegistry::register` generates request and complete wire-response schemas.
All methods share one definition store.
`SchemaRegistry::build` projects the caller's method directory into an
OpenAPI 3.0 document without consuming the definitions.

`MethodSchema::protocol` describes request and response formats and any
connection or original-peer requirements. Its default describes ordinary
serialized methods and is omitted from serialized method descriptions;
older descriptions without it retain that meaning. For a raw payload, the
corresponding schema describes its control value. OpenAPI uses
`application/octet-stream` and `x-ruapc-control-schema` for that payload,
and `x-ruapc-transport` / `x-ruapc-context` on constrained operations.
These operations remain discoverable without advertising them as independent
unary JSON calls.

The runtime registers its common `Result<(), Error>` envelope through
`SchemaRegistry::register_framework_error`. The OpenAPI document describes it
once in `x-ruapc-framework-error`, including flag value `16`
(`IsFrameworkError`). Such responses use the ordinary payload codec regardless
of the method's declared response format. The extension also records the
equivalent unary HTTP response header, `x-ruapc-framework-error: true`.

Callers retain responsibility for validated wire names; runtime registration
and dispatch remain in `ruapc`.

See the [architecture](../DESIGN.md) for runtime ownership. Public types
and their caller obligations are documented in rustdoc.

```bash
cargo test -p ruapc-schema
```
