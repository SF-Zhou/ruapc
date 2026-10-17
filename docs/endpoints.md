# Server endpoints

An `Endpoint` names one server through one transport and a nonempty group of
equivalent socket addresses. TCP, WebSocket, HTTP and RDMA all accept address
groups. Applications choose the server and transport; connection acquisition
chooses an available address within that group.

## String format

Use one scheme followed by comma-separated IP addresses and ports:

```text
tcp://192.0.2.10:8000,192.0.2.11:8000
ws://192.0.2.10:8000,192.0.2.11:8000
http://192.0.2.10:8000,192.0.2.11:8000
rdma://192.0.2.10:8000,192.0.2.11:8000
tcp://[2001:db8::10]:8000,[2001:db8::11]:8000
```

Single-address strings keep their existing form. IPv6 addresses retain square
brackets. Addresses must be numeric `SocketAddr` values; hostnames, paths,
empty entries and additional schemes inside the list are rejected.

`Display`, `FromStr`, serde and the demo CLI use this same representation.
Construction sorts and deduplicates addresses; output always uses that
normalized order. Input order does not express a preference.

This is RuaPC's endpoint format. An HTTP address group is not an ordinary URL
for `curl` or a browser: those callers use one HTTP address. Configure the
typed HTTP client's base path separately with `SocketPoolConfig.http_base_path`.

## Storage and interchange

`Endpoint` implements serde `Serialize` and `Deserialize` using the canonical
endpoint string. JSON stores a string by default, for example:

```json
"rdma://127.0.0.1:8000,127.0.0.1:8001"
```

MessagePack stores a string with the same contents. Deserialization parses and
normalizes the address group, applying the same validation as `FromStr`.
Applications can store or pass endpoints without initializing a context or
connecting to the server.

`Transport::RDMA` and the `rdma://` scheme are available in every build.
Constructing, parsing, serializing and deserializing an RDMA endpoint do not
require the `rdma` Cargo feature. Acquiring an RDMA connection or making a call
requires that feature and an enabled pool (`SocketPoolConfig.rdma = Some(...)`);
otherwise it returns `InvalidArgument`.

## Identity and caller obligations

The transport and **complete normalized address group** identify an endpoint
within a client pool. Reordered or duplicated addresses identify the same
group. Different groups remain independent even when they overlap: `[a, b]`,
`[a]` and `[a, b, c]` have distinct connection caches and acquisition locks.
RuaPC does not merge groups by observed addresses or successful connections.

Callers guarantee that every address in a group reaches the same server state.
For RDMA, discovery and prepare/commit/cancel requests through any group address
must share the same `State` and lease records. There is no negotiated server
instance identifier that verifies this promise. Different groups pointing to
the same server still remain distinct. See [RDMA bootstrap](rdma-connection.md)
for the lifecycle consequences.

## Acquisition and retries

Connection acquisition reuses a group's established connection or tries its
addresses within one connection budget. Address fallback is part of that
acquisition and remains available with `Client::max_retries = 0`. Concurrent
acquisitions of the same normalized group share connection setup. TCP, WS and
HTTP each cache one outbound stream per group; RDMA maintains its configured
connection stripes for the whole group.
After first use, RDMA also preconnects compatible remote NICs in the background;
see [coverage targets and limits](rdma-connection.md#path-policy-and-maintenance).

A failed alternate address does not invalidate a healthy established
connection. Request retries remain limited to failures before transport
acceptance; an ambiguous in-flight timeout or disconnect never causes an
automatic replay. Nested calls inherit the parent's remaining deadline.

## Rust API

Constructors take address iterators and return `Result<Endpoint>`. `tcp` is the
TCP shorthand; `new` selects a transport. `addrs()` borrows the normalized
slice, and `transport()` returns the group's transport. `Endpoint` is `Clone`,
not `Copy`.

This example uses `ruapc`, `serde_json` and `rmp-serde`. It needs neither a Tokio
runtime nor the RDMA feature.

```rust
use ruapc::{Endpoint, Transport};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let single = Endpoint::tcp(["127.0.0.1:8000".parse()?])?;
    let group = Endpoint::new(Transport::HTTP, [
        "127.0.0.1:8001".parse()?,
        "127.0.0.1:8000".parse()?,
        "127.0.0.1:8000".parse()?,
    ])?;
    assert_eq!(group.to_string(), "http://127.0.0.1:8000,127.0.0.1:8001");
    assert_eq!(group.addrs().len(), 2);
    let parsed: Endpoint = group.to_string().parse()?;
    assert_eq!(parsed, group);
    assert_eq!(single.transport(), Transport::TCP);

    let rdma = Endpoint::new(Transport::RDMA, group.addrs().iter().copied())?;
    let json = serde_json::to_string(&rdma)?;
    assert_eq!(serde_json::from_str::<Endpoint>(&json)?, rdma);
    let messagepack = rmp_serde::to_vec(&rdma)?;
    assert_eq!(rmp_serde::from_slice::<Endpoint>(&messagepack)?, rdma);
    Ok(())
}
```

Pass the constructed endpoint to `Context::with_endpoint(endpoint)`. Clone it
when another context needs the same group; contexts sharing a client `State`
reuse that group's pool entry.
