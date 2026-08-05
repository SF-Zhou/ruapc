use std::{net::SocketAddr, sync::Arc};

use ruapc_bufpool::RemoteBufferInfo;
use serde::Serialize;
use tokio_util::sync::DropGuard;

use crate::{
    Buffer, CopyOp, Error, RemoteIoError, Result, Router, Socket, SocketPoolConfig, SocketTrait,
    State,
    core::scatter::{self, SpaceLayout},
    msg::{MsgFlags, MsgMeta},
};

/// A read or write view of the remote peer's registered memory, built from
/// the regions attached to the current request.
///
/// The regions form one logical contiguous space (in region order, each
/// contributing its advertised length); [`CopyOp`] offsets address this
/// space. Obtained via [`Context::remote_read_space`] /
/// [`Context::remote_write_space`].
#[derive(Debug)]
pub struct RemoteSpace<'a> {
    regions: &'a [RemoteBufferInfo],
    layout: SpaceLayout,
}

impl<'a> RemoteSpace<'a> {
    pub(crate) fn new(regions: &'a [RemoteBufferInfo]) -> Result<Self> {
        for region in regions {
            if region.addr.checked_add(region.len).is_none() {
                return Err(Error::new(
                    crate::ErrorKind::InvalidCopyOp,
                    "remote region addr + len overflows u64".into(),
                ));
            }
        }
        let layout = SpaceLayout::from_lens(regions.iter().map(|r| r.len))?;
        Ok(Self { regions, layout })
    }

    /// Total length of the logical space in bytes.
    #[must_use]
    pub fn total_len(&self) -> u64 {
        self.layout.total()
    }

    /// The raw regions composing the space, in order.
    #[must_use]
    pub fn regions(&self) -> &'a [RemoteBufferInfo] {
        self.regions
    }

    /// Number of regions.
    #[must_use]
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    /// Only the RDMA read planner consumes the layout directly.
    #[cfg_attr(not(feature = "rdma"), allow(dead_code))]
    pub(crate) fn layout(&self) -> &SpaceLayout {
        &self.layout
    }
}

/// Builds the logical-space layout of a set of local buffers (each
/// contributing its logical length).
fn local_layout(buffers: &[Buffer]) -> Result<SpaceLayout> {
    SpaceLayout::from_lens(buffers.iter().map(|b| b.len() as u64))
}

/// Socket endpoint information for RPC contexts.
///
/// Represents the connection endpoint for an RPC operation, which can be:
/// - Invalid: No endpoint specified
/// - Connected: An existing socket connection
/// - Address: A socket address to connect to
#[derive(Clone, Debug, Default)]
pub enum SocketEndpoint {
    /// No valid endpoint (default state).
    #[default]
    Invalid,
    /// An established socket connection.
    Connected(Socket),
    /// A socket address to establish a connection to.
    Address(SocketAddr),
    /// Several equivalent server addresses; requests pick one round-robin
    /// (quarantined addresses last) and connect-phase retries fail over
    /// to the next. See [`AddrSet`].
    Addresses(Arc<AddrSet>),
}

/// A set of equivalent server addresses (e.g. one per server NIC) with a
/// round-robin cursor and per-address failure cooldown.
///
/// Requests through a multi-address context spread over the addresses
/// round-robin. When an attempt fails in a way that indicts the address
/// (connect failure, send failure, or the connection dying while the
/// request was in flight), the address is quarantined for the cooldown
/// period: new requests prefer the remaining addresses, so one downed
/// server NIC costs at most one failed/slow request per cooldown window
/// instead of degrading `1/N` of the traffic. The quarantine is *soft* —
/// when every address is cooling down, requests proceed anyway (the fault
/// may have cleared, and failing fast helps nobody) — and a quarantined
/// address that works again is reinstated immediately.
///
/// The health state lives inside the set: share one `Arc<AddrSet>` across
/// contexts (see [`Context::with_addr_set`]) so all of them benefit from
/// the same observations.
#[derive(Debug)]
pub struct AddrSet {
    addrs: Vec<SocketAddr>,
    cursor: std::sync::atomic::AtomicUsize,
    cooldown: std::time::Duration,
    /// Per-address health state, index-aligned with `addrs`.
    health: std::sync::Mutex<Vec<AddrHealth>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct AddrHealth {
    down_until: Option<std::time::Instant>,
    version: u64,
}

impl AddrSet {
    /// Default quarantine period for a failed address.
    pub const DEFAULT_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(30);

    /// Creates an address set with the default failure cooldown. Empty
    /// sets are permitted but every request through them fails with
    /// `InvalidArgument`.
    #[must_use]
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        Self::with_cooldown(addrs, Self::DEFAULT_COOLDOWN)
    }

    /// Creates an address set with a custom failure cooldown: how long a
    /// failed address is avoided before a request probes it again.
    #[must_use]
    pub fn with_cooldown(addrs: Vec<SocketAddr>, cooldown: std::time::Duration) -> Self {
        let health = std::sync::Mutex::new(vec![AddrHealth::default(); addrs.len()]);
        Self {
            addrs,
            cursor: std::sync::atomic::AtomicUsize::new(0),
            cooldown,
            health,
        }
    }

    /// The addresses in the set, in construction order.
    #[must_use]
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }

    /// Builds the attempt order for one request: available addresses
    /// first (rotated by the round-robin cursor), quarantined ones last
    /// (same rotation) as a soft fallback. Successive retry attempts of
    /// the request walk this plan, so they always move to a different
    /// address.
    pub(crate) fn plan(&self) -> Vec<(SocketAddr, u64)> {
        let len = self.addrs.len();
        if len == 0 {
            return Vec::new();
        }
        let base = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = std::time::Instant::now();
        let mut health = self.health.lock().unwrap();
        let mut plan = Vec::with_capacity(len);
        let mut cooling = Vec::new();
        for i in 0..len {
            let idx = (base + i) % len;
            match health[idx].down_until {
                Some(deadline) if deadline > now => {
                    cooling.push((self.addrs[idx], health[idx].version));
                }
                _ => {
                    if health[idx].down_until.take().is_some() {
                        health[idx].version = health[idx].version.wrapping_add(1);
                    }
                    plan.push((self.addrs[idx], health[idx].version));
                }
            }
        }
        plan.append(&mut cooling);
        plan
    }

    /// Quarantines `addr` for the cooldown period.
    pub(crate) fn mark_failed(&self, addr: SocketAddr, observed_version: u64) {
        let deadline = std::time::Instant::now().checked_add(self.cooldown);
        let mut health = self.health.lock().unwrap();
        for (i, a) in self.addrs.iter().enumerate() {
            if *a == addr && health[i].version == observed_version {
                health[i].down_until = deadline;
                health[i].version = health[i].version.wrapping_add(1);
            }
        }
    }

    /// Reinstates `addr` immediately (it demonstrably works again).
    pub(crate) fn mark_ok(&self, addr: SocketAddr, observed_version: u64) -> u64 {
        let mut health = self.health.lock().unwrap();
        for (i, a) in self.addrs.iter().enumerate() {
            if *a == addr
                && health[i].version == observed_version
                && health[i].down_until.take().is_some()
            {
                health[i].version = health[i].version.wrapping_add(1);
            }
        }
        self.addrs
            .iter()
            .position(|candidate| *candidate == addr)
            .map_or(observed_version, |i| health[i].version)
    }
}

/// RPC context carrying request metadata and connection information.
///
/// The `Context` is passed to all RPC service methods and contains:
/// - Shared state (router, socket pool, etc.)
/// - Connection endpoint information
/// - Lifecycle management through drop guards
///
/// # Examples
///
/// Creating a client context:
///
/// ```rust,no_run
/// # use ruapc::{Context, SocketPoolConfig};
/// # use std::{net::SocketAddr, str::FromStr};
/// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
/// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
/// let ctx = ctx.with_addr(addr);
/// ```
#[derive(Clone)]
pub struct Context {
    pub(crate) drop_guard: Option<Arc<DropGuard>>,
    /// Shared state containing router and socket pool.
    pub state: Arc<State>,
    pub(crate) endpoint: SocketEndpoint,
    /// Message metadata for the current RPC operation.
    pub msg_meta: MsgMeta,
    /// Deadline of the request being handled, derived from the client's
    /// `timeout_ms` budget on arrival. `None` when the request carries no
    /// budget (or for client-created contexts).
    pub(crate) deadline: Option<std::time::Instant>,
    /// Optional constraint on the RDMA path (NIC pair) used by requests
    /// issued through this context. See [`Context::with_rdma_path`].
    #[cfg(feature = "rdma")]
    pub(crate) rdma_path: Option<crate::rdma::RdmaPathSelector>,
}

impl Context {
    /// Creates a new context with the given socket pool configuration.
    pub fn create(config: &SocketPoolConfig) -> Result<Self> {
        Self::create_with_router(Router::default(), config)
    }

    /// Creates a new context with a custom router and configuration.
    pub fn create_with_router(router: Router, config: &SocketPoolConfig) -> Result<Self> {
        let (state, drop_guard) = State::create(router, config)?;
        Ok(Self {
            state,
            endpoint: SocketEndpoint::Invalid,
            drop_guard: Some(Arc::new(drop_guard)),
            msg_meta: MsgMeta::default(),
            deadline: None,
            #[cfg(feature = "rdma")]
            rdma_path: None,
        })
    }

    /// Creates a context with a specific state and address.
    ///
    /// Internal method used by RDMA implementation.
    #[cfg(feature = "rdma")]
    pub(crate) fn create_with_state_and_addr(state: &Arc<State>, addr: &SocketAddr) -> Self {
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Address(*addr),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
            deadline: None,
            rdma_path: None,
        }
    }

    /// Creates a new context with the specified target address.
    ///
    /// The deadline (if any) is inherited: nested RPCs issued while handling
    /// a request keep the caller's remaining time budget.
    #[must_use]
    pub fn with_addr(&self, addr: SocketAddr) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Address(addr),
            drop_guard: self.drop_guard.clone(),
            msg_meta: MsgMeta::default(),
            deadline: self.deadline,
            #[cfg(feature = "rdma")]
            rdma_path: self.rdma_path.clone(),
        }
    }

    /// Creates a new context that load-balances across several equivalent
    /// addresses of the *same* server (e.g. one per NIC).
    ///
    /// Each request picks the next address round-robin; when an attempt
    /// fails before the request reaches the wire (connect or send failure),
    /// the retry moves on to the following address (see
    /// [`Client::max_retries`](crate::Client::max_retries)). Failed
    /// addresses are quarantined for [`AddrSet::DEFAULT_COOLDOWN`] so
    /// subsequent requests avoid them while healthy alternatives exist —
    /// see [`AddrSet`] for the exact semantics.
    ///
    /// The health state is scoped to this context (and its clones). To
    /// share it across several contexts — or to tune the cooldown — build
    /// the [`AddrSet`] yourself and use [`with_addr_set`](Self::with_addr_set).
    #[must_use]
    pub fn with_addrs(&self, addrs: Vec<SocketAddr>) -> Self {
        self.with_addr_set(Arc::new(AddrSet::new(addrs)))
    }

    /// Creates a new context that load-balances across the addresses of
    /// `addr_set`, like [`with_addrs`](Self::with_addrs), but with a
    /// caller-provided (possibly shared) [`AddrSet`].
    #[must_use]
    pub fn with_addr_set(&self, addr_set: Arc<AddrSet>) -> Self {
        Self {
            state: self.state.clone(),
            endpoint: SocketEndpoint::Addresses(addr_set),
            drop_guard: self.drop_guard.clone(),
            msg_meta: MsgMeta::default(),
            deadline: self.deadline,
            #[cfg(feature = "rdma")]
            rdma_path: self.rdma_path.clone(),
        }
    }

    /// Deadline of the request being handled, if the client sent a time
    /// budget.
    #[must_use]
    pub fn deadline(&self) -> Option<std::time::Instant> {
        self.deadline
    }

    /// Remaining time budget of the request being handled. Returns
    /// `Duration::ZERO` when the deadline already passed and `None` when
    /// the request carries no budget.
    #[must_use]
    pub fn remaining_time(&self) -> Option<std::time::Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(std::time::Instant::now()))
    }

    /// Whether the request's deadline has passed. Handlers of long-running
    /// methods can poll this to stop work the client no longer waits for.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.remaining_time() == Some(std::time::Duration::ZERO)
    }

    /// Constrains RDMA requests issued through this context to
    /// connections whose path (local NIC, remote NIC) matches `selector`;
    /// a matching connection is established on demand (and kept pinned:
    /// it is exempt from automatic rebalancing).
    ///
    /// Only affects requests using [`SocketType::RDMA`](crate::SocketType);
    /// other transports ignore the selector.
    ///
    /// ```rust,no_run
    /// # use ruapc::{Context, RdmaPathSelector, SocketPoolConfig};
    /// # use std::{net::SocketAddr, str::FromStr};
    /// let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
    /// let addr = SocketAddr::from_str("127.0.0.1:8000").unwrap();
    /// let ctx = ctx
    ///     .with_addr(addr)
    ///     .with_rdma_path(RdmaPathSelector::local_device("mlx5_0"));
    /// ```
    #[cfg(feature = "rdma")]
    #[must_use]
    pub fn with_rdma_path(&self, selector: crate::rdma::RdmaPathSelector) -> Self {
        let mut ctx = self.clone();
        ctx.rdma_path = Some(selector);
        ctx
    }

    /// Creates a server-side context with an established socket connection.
    ///
    /// Derives the request deadline from the client-provided `timeout_ms`
    /// budget, anchored at arrival time.
    #[must_use]
    pub(crate) fn server_ctx(state: &Arc<State>, socket: Socket, msg_meta: MsgMeta) -> Self {
        let deadline = msg_meta
            .timeout_ms
            .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(u64::from(ms)));
        Self {
            state: state.clone(),
            endpoint: SocketEndpoint::Connected(socket),
            drop_guard: None,
            msg_meta,
            deadline,
            #[cfg(feature = "rdma")]
            rdma_path: None,
        }
    }

    /// Sends an RPC response back to the client.
    pub async fn send_rsp<Rsp, E>(&mut self, rsp: std::result::Result<Rsp, E>)
    where
        Rsp: Serialize,
        E: std::error::Error + From<Error> + Serialize,
    {
        // Error accounting for the method being handled (server side).
        if rsp.is_err() && !self.msg_meta.method.is_empty() {
            self.state
                .metrics
                .server_method(&self.msg_meta.method)
                .errors
                .increment(1);
        }

        // Responses are correlated by msgid alone: drop the request's method
        // and regions so the response meta stays minimal (flags + msgid
        // only; absent fields are skipped by the meta encoding).
        let mut meta = MsgMeta {
            method: String::new(),
            flags: self.msg_meta.flags,
            msgid: self.msg_meta.msgid,
            read_regions: Vec::new(),
            write_regions: Vec::new(),
            timeout_ms: None,
        };
        meta.flags.remove(MsgFlags::IsReq);
        meta.flags.insert(MsgFlags::IsRsp);
        match &mut self.endpoint {
            SocketEndpoint::Connected(socket) => {
                let _ = socket.send(&mut meta, &rsp, &self.state).await;
            }
            _ => {
                tracing::error!("invalid argument: send rsp without connected socket");
            }
        }
    }

    /// Sends an error response back to the client.
    pub async fn send_err_rsp(&mut self, err: Error) {
        self.send_rsp::<(), Error>(Err(err)).await;
    }

    /// Returns the *read space* the client attached to the current request
    /// via [`Client::with_read_buffers`](crate::Client::with_read_buffers):
    /// the logical concatenation of the advertised regions, readable with
    /// [`remote_read`](Self::remote_read).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no read regions.
    pub fn remote_read_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.read_regions.is_empty() {
            return Err(Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "request carries no read regions; client must attach buffers \
                 via with_read_buffers()"
                    .into(),
            ));
        }
        RemoteSpace::new(&self.msg_meta.read_regions)
    }

    /// Returns the *write space* the client attached to the current request
    /// via [`Client::with_write_buffers`](crate::Client::with_write_buffers):
    /// the logical concatenation of the pinned destination regions,
    /// writable with [`remote_write`](Self::remote_write).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::MissingBufferInfo`](crate::ErrorKind::MissingBufferInfo)
    /// if the request carries no write regions.
    pub fn remote_write_space(&self) -> Result<RemoteSpace<'_>> {
        if self.msg_meta.write_regions.is_empty() {
            return Err(Error::new(
                crate::ErrorKind::MissingBufferInfo,
                "request carries no write regions; client must attach buffers \
                 via with_write_buffers()"
                    .into(),
            ));
        }
        RemoteSpace::new(&self.msg_meta.write_regions)
    }

    /// Executes a batch of reads from the client's read space into `local`.
    ///
    /// Both sides are logical contiguous spaces: the client's attached
    /// read buffers (source, see [`remote_read_space`](Self::remote_read_space))
    /// and the concatenation of the `local` buffers' logical lengths
    /// (destination). Each [`CopyOp`] copies `len` bytes from
    /// `src_offset` (client space) to `dst_offset` (local space).
    ///
    /// The batch is validated before anything is transferred: bounds,
    /// overflow, op count, and non-overlapping destination ranges. On RDMA
    /// the ops are fragmented into one-sided RDMA READ work requests
    /// (contiguous remote range + local scatter-gather list) executed
    /// concurrently; on TCP/WS/HTTP a reverse `MemoryService/read` RPC
    /// moves the bytes inline.
    ///
    /// Returns the same buffers, now filled at the ops' destination
    /// ranges. On failure they are handed back inside [`RemoteIoError`]
    /// whenever they survived the operation; propagating with `?` converts
    /// to [`Error`] and drops them back to the pool.
    pub async fn remote_read(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<Vec<Buffer>, RemoteIoError> {
        if ops.iter().all(|op| op.len == 0) {
            return Ok(local);
        }
        let space = match self.remote_read_space() {
            Ok(space) => space,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let dst_layout = match local_layout(&local) {
            Ok(layout) => layout,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if let Err(e) = scatter::validate_ops(ops, space.total_len(), dst_layout.total()) {
            return Err(RemoteIoError::new(e, Some(local)));
        }
        let socket = match &self.endpoint {
            SocketEndpoint::Connected(s) => s,
            _ => {
                return Err(RemoteIoError::new(
                    Error::new(
                        crate::ErrorKind::NotConnected,
                        "remote_read requires a connected socket (server-side handler context)"
                            .into(),
                    ),
                    Some(local),
                ));
            }
        };
        socket.remote_read(self, ops, local, &space).await
    }

    /// Reads the client's entire read space into freshly allocated
    /// buffers, one per region (mirroring the client's segmentation).
    ///
    /// Convenience wrapper around
    /// [`remote_read_space`](Self::remote_read_space) and
    /// [`remote_read`](Self::remote_read). The returned buffers' logical
    /// lengths equal the transferred sizes; zero-length regions are
    /// skipped.
    pub async fn remote_read_all(&self) -> Result<Vec<Buffer>> {
        let space = self.remote_read_space()?;
        let total = space.total_len();
        let mut local = Vec::new();
        for region in space.regions() {
            if region.len == 0 {
                continue;
            }
            let mut buf = self
                .state
                .buffer_pool
                .allocate(usize::try_from(region.len).map_err(|_| {
                    Error::new(crate::ErrorKind::InvalidArgument, "region too large".into())
                })?)
                .map_err(|e| Error::new(crate::ErrorKind::InvalidArgument, e.to_string()))?;
            buf.set_len(region.len as usize);
            local.push(buf);
        }
        if total == 0 {
            return Ok(local);
        }
        // The buffers are pool-allocated internally, so there is nothing
        // for the caller to recover on failure: flatten to a plain Error.
        Ok(self.remote_read(&[CopyOp::new(0, 0, total)], local).await?)
    }

    /// Executes a batch of writes from `local` into the client's write
    /// space and returns a [`SentBuffers`](crate::SentBuffers) witness of
    /// the completed transfer.
    ///
    /// Both sides are logical contiguous spaces: the concatenation of the
    /// `local` buffers' logical lengths (source) and the client's pinned
    /// write buffers (destination, see
    /// [`remote_write_space`](Self::remote_write_space)). Each [`CopyOp`]
    /// copies `len` bytes from `src_offset` (local space) to `dst_offset`
    /// (client space).
    ///
    /// The batch is validated before anything is transferred (bounds,
    /// overflow, op count, non-overlapping destination ranges; overlap
    /// across *separate* `remote_write` calls is the caller's
    /// responsibility). No one-sided RDMA WRITE is used: on RDMA the
    /// server sends a reverse `MemoryService/pull` RPC advertising `local`
    /// as readable regions, and the *client* executes the RDMA READs into
    /// its pinned buffers — their lifetime is anchored client-side, which
    /// makes the transfer safe against client timeouts. On TCP the data
    /// travels inline via `MemoryService/push`.
    ///
    /// The transfer happens *here*, inside the handler, so its latency and
    /// errors are directly observable. Pair the witness with a response
    /// value afterwards; several writes combine via
    /// [`SentBuffers::merge`](crate::SentBuffers::merge):
    ///
    /// ```rust,ignore
    /// let t0 = std::time::Instant::now();
    /// let sent = ctx.remote_write_all(bufs).await?;
    /// Ok(sent.reply(Stats { push_micros: t0.elapsed().as_micros() as u64 }))
    /// ```
    ///
    /// A batch moving zero bytes short-circuits without touching the
    /// network. On failure the local buffers are handed back inside
    /// [`RemoteIoError`] whenever they survived the operation.
    pub async fn remote_write(
        &self,
        ops: &[CopyOp],
        local: Vec<Buffer>,
    ) -> std::result::Result<crate::SentBuffers, RemoteIoError> {
        if ops.iter().all(|op| op.len == 0) {
            return Ok(crate::SentBuffers::new(local));
        }
        let space = match self.remote_write_space() {
            Ok(space) => space,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        let src_layout = match local_layout(&local) {
            Ok(layout) => layout,
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if let Err(e) = scatter::validate_ops(ops, src_layout.total(), space.total_len()) {
            return Err(RemoteIoError::new(e, Some(local)));
        }
        let socket = match &self.endpoint {
            SocketEndpoint::Connected(s) => s,
            _ => {
                return Err(RemoteIoError::new(
                    Error::new(
                        crate::ErrorKind::NotConnected,
                        "remote_write requires a connected socket (server-side handler context)"
                            .into(),
                    ),
                    Some(local),
                ));
            }
        };
        let buffers = socket.remote_write(self, ops, local).await?;
        Ok(crate::SentBuffers::new(buffers))
    }

    /// Writes the entire logical content of `local` to the beginning of
    /// the client's write space (a single 1:1 [`CopyOp`]).
    ///
    /// Zero total length (including an empty `local`) short-circuits
    /// without touching the network.
    pub async fn remote_write_all(
        &self,
        local: Vec<Buffer>,
    ) -> std::result::Result<crate::SentBuffers, RemoteIoError> {
        let total = match local_layout(&local) {
            Ok(layout) => layout.total(),
            Err(e) => return Err(RemoteIoError::new(e, Some(local))),
        };
        if total == 0 {
            return Ok(crate::SentBuffers::new(local));
        }
        self.remote_write(&[CopyOp::new(0, 0, total)], local).await
    }

    /// Produces an empty [`SentBuffers`](crate::SentBuffers) witness for
    /// handler paths that have nothing to transfer, fulfilling a
    /// `Result<WithBuffers<T>, E>` contract without touching the network.
    #[must_use]
    pub fn sent_nothing(&self) -> crate::SentBuffers {
        crate::SentBuffers::new(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, ErrorKind, SocketPoolConfig};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn plan_addrs(set: &AddrSet) -> Vec<SocketAddr> {
        set.plan().into_iter().map(|(addr, _)| addr).collect()
    }

    #[test]
    fn test_addr_set_plan_rotates_round_robin() {
        let set = AddrSet::new(vec![addr(1), addr(2), addr(3)]);
        assert_eq!(set.addrs().len(), 3);
        assert_eq!(plan_addrs(&set), vec![addr(1), addr(2), addr(3)]);
        assert_eq!(plan_addrs(&set), vec![addr(2), addr(3), addr(1)]);
        assert_eq!(plan_addrs(&set), vec![addr(3), addr(1), addr(2)]);
        // Empty sets yield empty plans.
        assert!(AddrSet::new(Vec::new()).plan().is_empty());
    }

    #[test]
    fn test_addr_set_quarantines_failed_addr_softly() {
        let set = AddrSet::new(vec![addr(1), addr(2)]);
        set.mark_failed(addr(1), 0);
        // The quarantined address moves to the back of every plan…
        assert_eq!(plan_addrs(&set), vec![addr(2), addr(1)]);
        assert_eq!(plan_addrs(&set), vec![addr(2), addr(1)]);
        // …and is reinstated once it demonstrably works again.
        set.mark_ok(addr(1), 1);
        assert_eq!(plan_addrs(&set), vec![addr(1), addr(2)]);

        // Soft semantics: with every address down, plans still cover all
        // of them (rotation preserved).
        set.mark_failed(addr(1), 2);
        set.mark_failed(addr(2), 0);
        assert_eq!(set.plan().len(), 2);
    }

    #[test]
    fn test_addr_set_cooldown_expires() {
        let set =
            AddrSet::with_cooldown(vec![addr(1), addr(2)], std::time::Duration::from_millis(1));
        set.mark_failed(addr(1), 0);
        std::thread::sleep(std::time::Duration::from_millis(10));
        // The cooldown elapsed: the address is available again.
        let plan = set.plan();
        assert!(plan.iter().any(|(a, _)| *a == addr(1)));
        assert!(plan.iter().any(|(a, _)| *a == addr(2)));
        assert_eq!(plan_addrs(&set), vec![addr(2), addr(1)]);
    }

    #[test]
    fn test_addr_set_marks_duplicate_addrs() {
        let set = AddrSet::new(vec![addr(1), addr(2), addr(1)]);
        set.mark_failed(addr(1), 0);
        // Both copies of the failed address are quarantined.
        assert_eq!(plan_addrs(&set), vec![addr(2), addr(1), addr(1)]);
    }

    #[test]
    fn test_addr_set_ignores_stale_health_observations() {
        let set = AddrSet::new(vec![addr(1), addr(2)]);
        set.mark_failed(addr(1), 0);

        // A success observed before the failure must not clear it.
        assert_eq!(set.mark_ok(addr(1), 0), 1);
        assert!(set.health.lock().unwrap()[0].down_until.is_some());

        // A current success reinstates the address and advances its version.
        assert_eq!(set.mark_ok(addr(1), 1), 2);
        assert!(set.health.lock().unwrap()[0].down_until.is_none());

        // A delayed failure from the original version cannot quarantine the
        // recovered address.
        set.mark_failed(addr(1), 0);
        assert!(set.health.lock().unwrap()[0].down_until.is_none());
    }

    #[test]
    fn test_addr_set_extreme_cooldown_does_not_panic() {
        let set = AddrSet::with_cooldown(vec![addr(1)], std::time::Duration::MAX);
        set.mark_failed(addr(1), 0);
        assert_eq!(plan_addrs(&set), vec![addr(1)]);
    }

    #[tokio::test]
    async fn test_send_rsp_invalid_endpoint_logs_and_does_not_panic() {
        // Context starts with SocketEndpoint::Invalid by default.
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        // This should log an error and silently return (no panic).
        ctx.send_err_rsp(Error::kind(ErrorKind::Timeout)).await;
    }

    fn ctx_with_read_regions(regions: Vec<RemoteBufferInfo>) -> Context {
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        ctx.msg_meta.read_regions = regions;
        ctx
    }

    fn region(len: u64) -> RemoteBufferInfo {
        RemoteBufferInfo {
            key: ruapc_bufpool::MemoryKey { lkey: 0, rkey: 0 },
            addr: 0x1000,
            len,
        }
    }

    #[tokio::test]
    async fn test_remote_read_missing_regions_recovers_buffers() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let local = vec![ctx.state.buffer_pool.allocate(1024 * 1024).unwrap()];
        let result = ctx.remote_read(&[CopyOp::new(0, 0, 1)], local).await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::MissingBufferInfo);
        let recovered = err.take_buffers().expect("buffers should be recovered");
        assert_eq!(recovered.len(), 1);
    }

    #[tokio::test]
    async fn test_remote_read_invalid_endpoint_returns_err() {
        let ctx = ctx_with_read_regions(vec![region(8)]);
        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local.set_len(8);
        let result = ctx.remote_read(&[CopyOp::new(0, 0, 8)], vec![local]).await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::NotConnected);
        // The consumed buffers are recoverable from the error.
        let recovered = err.take_buffers().expect("buffers should be recovered");
        assert_eq!(recovered[0].capacity(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_remote_read_rejects_out_of_bounds_ops() {
        let ctx = ctx_with_read_regions(vec![region(8), region(8)]);
        let space = ctx.remote_read_space().unwrap();
        assert_eq!(space.total_len(), 16);
        assert_eq!(space.region_count(), 2);

        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local.set_len(16);
        // Source range exceeds the 16-byte remote space.
        let result = ctx.remote_read(&[CopyOp::new(9, 0, 8)], vec![local]).await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
        let local = err.take_buffers().unwrap();

        // Overlapping destination ranges are rejected.
        let ops = [CopyOp::new(0, 0, 8), CopyOp::new(8, 4, 8)];
        let err = ctx.remote_read(&ops, local).await.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::InvalidCopyOp);
    }

    #[tokio::test]
    async fn test_remote_read_zero_len_ops_short_circuit() {
        // All-zero ops complete without a connection or regions.
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let local = ctx.remote_read(&[CopyOp::new(0, 0, 0)], vec![]).await;
        assert!(local.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_remote_write_invalid_endpoint_returns_err() {
        let mut ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        ctx.msg_meta.write_regions = vec![region(1024)];
        let mut local = ctx.state.buffer_pool.allocate(1024 * 1024).unwrap();
        local.set_len(1024);
        let result = ctx
            .remote_write(&[CopyOp::new(0, 0, 1024)], vec![local])
            .await;
        let mut err = result.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::NotConnected);
        assert!(err.take_buffers().is_some());
        // Once taken, the buffers are gone.
        assert!(err.take_buffers().is_none());
        // Converting to Error drops any remaining buffers and keeps the kind.
        let plain: Error = err.into();
        assert_eq!(plain.kind, ErrorKind::NotConnected);
    }

    #[tokio::test]
    async fn test_remote_write_zero_total_short_circuits() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        // No write regions attached, but nothing to transfer either.
        let sent = ctx.remote_write_all(vec![]).await.unwrap();
        assert!(sent.buffers().is_empty());
        let _rsp: crate::WithBuffers<u32> = sent.reply(7);
        // The explicit no-op witness works the same way.
        let _rsp: crate::WithBuffers<u32> = ctx.sent_nothing().reply(7);
    }

    #[tokio::test]
    async fn test_remote_spaces_missing_return_err() {
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        assert_eq!(
            ctx.remote_read_space().unwrap_err().kind,
            ErrorKind::MissingBufferInfo
        );
        assert_eq!(
            ctx.remote_write_space().unwrap_err().kind,
            ErrorKind::MissingBufferInfo
        );
        // remote_read_all surfaces the same error.
        let err = ctx.remote_read_all().await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::MissingBufferInfo);
    }
}
