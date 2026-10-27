# ruapc-schema

Schema generation, OpenAPI projection and reflection data for RuaPC. The crate
has no dependency on `ruapc`, an async runtime, transports or registered memory.

`SchemaRegistry::register` generates request and complete wire-response schemas.
Public and internal methods use separate definition stores.
`SchemaRegistry::build` projects the caller's public method directory into an
OpenAPI 3.0 document without consuming the definitions.

`ServerDescription::from_methods` builds the structured reflection response
from the public directory, the serving implementation's version and the matching
OpenAPI components. It sorts services and methods and applies exact service-name
filtering. Callers retain responsibility for validated wire names and method
visibility; runtime routing and the reflection RPC service remain in `ruapc`.

See the [built-in services guide](../docs/builtin-services.md) for the reflection
contract and the [architecture](../DESIGN.md) for runtime ownership. Public types
and their caller obligations are documented in rustdoc.

```bash
cargo test -p ruapc-schema
```
