# Local failure evidence

`Error::local_failure()` exposes optional process-local observations:

- `LocalFailure::TcpConnect(kind)` records an actual operating-system TCP
  connect failure, or `TimedOut` when the network connect deadline expires.
  Connection-lock waits and already-expired preflight deadlines do not supply
  network-failure evidence.
- `LocalFailure::ResponseWait` records a failed response wait after transport
  acceptance. For `ErrorKind::Timeout`, the actual response budget must have
  elapsed; an early closed response channel does not qualify as timeout evidence.

Always inspect the accompanying error kind and operation context. A local
shutdown can close a response wait, and a remote application error is not a
local transport observation. These markers do not establish that a request
was unexecuted: an accepted write remains uncertain after a lost reply, with
no automatic replay added.

The evidence field is private and excluded from MessagePack, JSON and JSON
schema. Deserialized errors always lack it, including forwarded local errors
and peer-supplied extra fields. Error variants and RPC wire bytes are unchanged.
The private field prevents external `Error` struct literals; use `Error::new`
or `Error::kind`.

## TCP teardown and connection locking

Either TCP I/O loop finishing cancels its sibling. EOF, write errors, stop and
cancellation release the transport tasks; a zero-byte write fails instead of
spinning. A disconnect or request deadline does not cancel a running application
handler. Pool shutdown joins transport tasks, not handlers.

The connection-lock helper releases its map guard before awaiting the
per-address async mutex. This preserves connection coalescing without holding
a map shard across the wait.

## Server metric names

Server error accounting validates the incoming method against the router and
interns only its registered canonical name, including internal methods. This
counts first-request overload and decode failures without an extra metric-cache
lookup on successful requests. Unknown incoming method names do not create
cached strings or recorder handles. Application client calls with arbitrary
method names can still grow the client metric cache.

Regression coverage lives in `ruapc/tests/test_failure_origin.rs` and focused
connection-lock and metrics unit tests. Run the workspace and feature-boundary
checks in [CONTRIBUTING.md](../CONTRIBUTING.md); TCP tests do not establish
RDMA behavior.
