//! Client side of RDMA connection setup: handshake, stripe establishment,
//! admission and the connect-plan/device-list machinery.

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::Arc,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use ruapc_bufpool::Device as _;
use ruapc_rdma::{QueuePair, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type};

use super::super::path::RdmaPathInfo;
use super::super::{
    ConnectRequest, ConnectionControl, RdmaConnectionConfig, RdmaDevice, RdmaInfo,
    RdmaService as _, RdmaSocket, RegisterConn,
};
use super::placement::{PathCandidate, PathPreference};
use super::{ConnCountGuard, PeerState, RdmaSocketPool, Stripe, next_connection_id};
use crate::{Buffer, Client, Context, Error, ErrorKind, Result, Socket, State};

pub(super) struct SocketRegistrationGuard {
    socket: Arc<RdmaSocket>,
    armed: bool,
    abort: Option<(
        crate::TaskSupervisorHandle,
        Client,
        Context,
        ConnectionControl,
    )>,
}

impl SocketRegistrationGuard {
    pub(super) fn new(
        socket: &Arc<RdmaSocket>,
        supervisor: crate::TaskSupervisorHandle,
        client: Client,
        context: Context,
        control: ConnectionControl,
    ) -> Self {
        Self {
            socket: socket.clone(),
            armed: true,
            abort: Some((supervisor, client, context, control)),
        }
    }

    pub(super) fn commit(&mut self) {
        self.armed = false;
        self.abort = None;
    }
}

impl Drop for SocketRegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.socket.set_error();
            if let Some((supervisor, client, context, control)) = self.abort.take() {
                let _ = supervisor.try_spawn(async move {
                    if let Err(err) = client.abort(&context, &control).await {
                        tracing::debug!(connection_id = control.connection_id, %err, "RDMA abort cleanup failed");
                    }
                });
            }
        }
    }
}

pub(super) struct EstablishedSocket {
    pub(super) socket: Arc<RdmaSocket>,
    pub(super) registration: SocketRegistrationGuard,
}

impl RdmaSocketPool {
    const DEVICE_LIST_CACHE_TTL: Duration = Duration::from_secs(30);

    /// Creates a QueuePair attached to the device's shared completion queue.
    pub(super) fn create_queue_pair(
        &self,
        device: &RdmaDevice,
        config: &RdmaConnectionConfig,
        poller: &super::super::poller::DevicePoller,
    ) -> Result<QueuePair> {
        let pd = device.pd();
        let cq = poller.cq();

        let mut init_attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: config.qp.max_send_wr,
                max_recv_wr: config.qp.max_recv_wr,
                max_send_sge: config.qp.max_send_sge,
                max_recv_sge: config.qp.max_recv_sge,
                max_inline_data: 0,
            },
            ..Default::default()
        };

        let mut queue_pair = QueuePair::create(pd, cq, cq, &mut init_attr, device.index())
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        queue_pair
            .set_send_signal_interval(self.config.send_signal_interval, config.qp.max_send_wr);

        Ok(queue_pair)
    }

    /// Wraps a connected QueuePair into an `RdmaSocket`, pre-posts receive
    /// buffers and registers the connection with the device poll thread.
    pub(super) fn register_socket(
        &self,
        mut queue_pair: QueuePair,
        state: &Arc<State>,
        poller: &super::super::poller::DevicePoller,
        config: &RdmaConnectionConfig,
        path: RdmaPathInfo,
        device_index: usize,
    ) -> Result<Arc<RdmaSocket>> {
        // Reserve the poller slot first: its tag must be stamped into the
        // QP before any work request is posted, since completions map back
        // to the connection through the tag in their `wr_id`.
        let qp_depth = (config.qp.max_send_wr + config.qp.max_recv_wr).saturating_mul(2);
        let reservation = poller.reserve(qp_depth)?;
        queue_pair.set_wr_tag(reservation.tag());

        // Account the registered memory this connection's receive ring
        // pins in the shared buffer pool, and warn when the pool is
        // undersized for the connection count (before the ring allocation
        // below starts failing under load). Steady-state traffic needs a
        // multiple of the ring size: zero-copy dispatch holds ring-sized
        // chunks for in-flight messages (each triggering a fresh repost
        // allocation), and send serialization draws from the same pool —
        // so rings exceeding a quarter of the pool are a reliable
        // exhaustion predictor.
        let ring_bytes = config.recv_queue_len as usize * config.max_msg_size as usize;
        let (ring_reservation, ring_total) =
            super::super::poller::RingReservation::add(&self.ring_bytes, ring_bytes);
        let pool_capacity = self.buffer_pool.max_memory();
        if ring_total.saturating_mul(4) >= pool_capacity
            && !self.pool_capacity_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                "RDMA buffer pool likely undersized: receive rings pin {ring_total}B of the \
                 {pool_capacity}B pool (each connection pins recv_queue_len ({}) x \
                 max_msg_size ({}) = {ring_bytes}B, and in-flight messages typically need a \
                 multiple of that); raise SocketPoolConfig::buffer_pool_memory to >= 4x the \
                 ring total, or lower rdma.recv_queue_len / rdma.max_msg_size / \
                 rdma.connections_per_peer",
                config.recv_queue_len,
                config.max_msg_size,
            );
        }

        // In-flight data WRs are bounded by the peer's receive ring; half
        // the (negotiated) ring keeps ample headroom for ACK latency (and
        // in-flight standalone ACKs) before the receiver could be overrun.
        let send_window = (config.recv_queue_len / 2).max(1);

        let (tx, rx) = tokio::sync::mpsc::channel::<Buffer>(1024);
        // Software timeout for RDMA READ completions (0 disables).
        let read_timeout = (self.config.read_timeout_ms > 0)
            .then(|| Duration::from_millis(self.config.read_timeout_ms));
        // In-flight READ budget: shared per local NIC (congestion
        // control), plus a per-connection SQ-overflow guard (the send
        // queue is shared with regular sends, so leave half to them).
        let read_permits = self
            .read_permits
            .get(device_index)
            .cloned()
            .unwrap_or_else(|| {
                Arc::new(tokio::sync::Semaphore::new(
                    self.config.max_inflight_read_wrs.max(1) as usize,
                ))
            });
        let sq_read_cap = (config.qp.max_send_wr / 2).max(1);
        let socket = Arc::new(RdmaSocket::new(
            queue_pair,
            self.buffer_pool.clone(),
            tx,
            poller.waker(),
            config.max_msg_size as usize,
            send_window,
            path,
            read_timeout,
            read_permits,
            sq_read_cap,
        ));

        // Pre-post receive buffers *before* the remote can send: the
        // registration is picked up asynchronously by the poll thread, but
        // the recv ring must be ready as soon as the handshake response
        // reaches the peer.
        for _ in 0..config.recv_queue_len {
            let buf = self.buffer_pool.allocate(config.max_msg_size as usize)?;
            socket
                .queue_pair
                .recv(buf)
                .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))?;
        }

        poller.register(
            reservation,
            RegisterConn {
                socket: socket.clone(),
                state: state.clone(),
                pending_receiver: rx,
                recv_submitted: u64::from(config.recv_queue_len),
                recv_buf_size: config.max_msg_size as usize,
                send_window,
                // Local-only send-side toggle; receivers walk the same
                // frame loop either way.
                msg_aggregation: self.config.msg_aggregation,
                supervisor_guard: self.task_supervisor.start_async_task(),
                ring_reservation,
                conn_count_guard: ConnCountGuard::acquire(&self.conn_counts, device_index),
            },
        )?;

        // Mark the socket as failed when the pool shuts down so the poll
        // thread tears the connection down.
        let socket_clone = socket.clone();
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            task_supervisor.stopped().await;
            socket_clone.set_error();
        });

        Ok(socket)
    }

    pub(super) async fn handshake(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        avoided_remote_nics: &HashSet<String>,
        deadline: Option<Instant>,
    ) -> Result<Socket> {
        let addr = &peer.addr;
        self.ensure_maintenance_task(state);
        // Re-check under the connect lock: another task may have connected,
        // or every stripe may have failed and must be replaced.
        let existing = {
            let mut stripes = peer.stripes.write().unwrap();
            if let Some(socket) = self.pick_stripe(&stripes.active, avoided_remote_nics) {
                return Ok(socket);
            }
            if stripes
                .active
                .iter()
                .any(|stripe| stripe.socket.state.is_ok())
            {
                stripes.active.clone()
            } else {
                if !stripes.active.is_empty() {
                    tracing::info!("all RDMA stripes to {addr} failed, reconnecting");
                    stripes.active.clear();
                }
                Vec::new()
            }
        };

        let fallback = (!avoided_remote_nics.is_empty())
            .then(|| self.pick_stripe(&existing, &HashSet::new()))
            .flatten();
        let plan = match self.prepare_connect_plan(peer, state, deadline).await {
            Ok(plan) => plan,
            Err(_) if fallback.is_some() => return Ok(fallback.expect("checked above")),
            Err(err) => return Err(err),
        };
        if !avoided_remote_nics.is_empty()
            && !plan
                .candidates
                .iter()
                .any(|candidate| !avoided_remote_nics.contains(&candidate.path.remote.device))
            && let Some(socket) = fallback
        {
            return Ok(socket);
        }
        let preference = PathPreference {
            remote_device: None,
            avoided_remote_nics,
        };

        if !existing.is_empty() {
            let max_connections = self.config.preconnect_max_per_peer.max(1) as usize;
            if existing
                .iter()
                .filter(|stripe| stripe.socket.state.is_ok())
                .count()
                >= max_connections
            {
                return fallback.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Overloaded,
                        format!(
                            "RDMA peer {addr} reached preconnect_max_per_peer ({max_connections})"
                        ),
                    )
                });
            }
            let established = match self
                .connect_with_failover(peer, state, &plan, preference, &existing)
                .await
            {
                Ok(established) => established,
                Err(_) if fallback.is_some() => return Ok(fallback.expect("checked above")),
                Err(err) => return Err(err),
            };
            let stripe = self.admit_established(peer, established);
            return Ok(Socket::from(&stripe.socket));
        }

        // Establish `connections_per_peer` stripes towards this peer;
        // requests are spread round-robin across them (and, with poll
        // thread shards, across cores). Each stripe picks its own path:
        // local side by least connections, remote side by
        // power-of-two-choices over the peer's advertised per-NIC load.
        let stripe_count = self.config.connections_per_peer.max(1);
        let mut stripes: Vec<Stripe> = Vec::with_capacity(stripe_count as usize);
        let mut established_sockets = Vec::with_capacity(stripe_count as usize);
        for _ in 0..stripe_count {
            match self
                .connect_with_failover(peer, state, &plan, preference, &stripes)
                .await
            {
                Ok(established) => {
                    stripes.push(Stripe {
                        socket: established.socket.clone(),
                    });
                    established_sockets.push(established);
                }
                Err(err) => return Err(err),
            }
        }

        let socket = self
            .pick_stripe(&stripes, avoided_remote_nics)
            .or_else(|| self.pick_stripe(&stripes, &HashSet::new()))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ConnectionClosed,
                    format!("all freshly established RDMA stripes to {addr} failed"),
                )
            })?;
        self.admit_initial(peer, established_sockets);
        Ok(socket)
    }

    /// Fetches the peer's device list and enumerates the compatible path
    /// candidates: everything a placement decision towards `addr` needs.
    pub(super) async fn prepare_connect_plan(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        deadline: Option<Instant>,
    ) -> Result<ConnectPlan> {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::new(
                ErrorKind::Timeout,
                "request deadline expired".into(),
            ));
        }
        let mut acquire_ctx = Context::create_with_state_and_addr(state, &peer.addr);
        acquire_ctx.deadline = deadline;
        let remote_info = self.fetch_remote_device_list(peer, &acquire_ctx).await?;
        let candidates = match self.enumerate_path_candidates(&remote_info) {
            Ok(candidates) => candidates,
            Err(err) => {
                self.invalidate_device_list_cache(peer);
                return Err(err);
            }
        };
        Ok(ConnectPlan {
            acquire_ctx,
            remote_info,
            candidates,
            deadline,
        })
    }

    /// Establishes one stripe towards `addr`, falling over to the next
    /// best candidate when a NIC pair turns out to be unreachable
    /// (device matching cannot verify routability). Each failed pair is
    /// blacklisted by `connect_stripe`, so later placements avoid it too.
    pub(super) async fn connect_with_failover(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        plan: &ConnectPlan,
        preference: PathPreference<'_>,
        existing: &[Stripe],
    ) -> Result<EstablishedSocket> {
        let mut remaining: Vec<PathCandidate> = plan.candidates.clone();
        loop {
            if plan
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(Error::new(
                    ErrorKind::Timeout,
                    "request deadline expired".into(),
                ));
            }
            let candidate =
                self.select_candidate(peer, &remaining, preference, &plan.remote_info, existing)?;
            match self
                .connect_stripe(peer, state, &plan.acquire_ctx, &candidate)
                .await
            {
                Ok(socket) => return Ok(socket),
                Err(err) => {
                    remaining.retain(|remaining| remaining.path != candidate.path);
                    if remaining.is_empty() {
                        return Err(err);
                    }
                    tracing::warn!(
                        peer = %peer.addr,
                        local = %candidate.path.local.device,
                        remote = %candidate.path.remote.device,
                        "RDMA path failed ({err}); trying another NIC pair"
                    );
                }
            }
        }
    }

    /// Establishes one RDMA connection (stripe) towards `addr` on the
    /// given path candidate.
    pub(super) async fn connect_stripe(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        acquire_ctx: &Context,
        candidate: &PathCandidate,
    ) -> Result<EstablishedSocket> {
        let device = self
            .devices
            .rdma_devices()
            .get(candidate.local_device_index)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    "selected RDMA device disappeared".into(),
                )
            })?;
        let connection_config = self.negotiate_connection_config(device, &candidate.remote_limits);
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            candidate.path.local.port_num,
            candidate.path.local.gid_index,
        )?;

        let connection_id = next_connection_id();
        let connect_request = ConnectRequest {
            connection_id,
            endpoint: local_endpoint,
            source_device: candidate.path.local.device.clone(),
            same_subnet: candidate.path.same_subnet,
            target: candidate.remote.clone(),
            config: connection_config,
        };
        // Box the recursive RPC call: `Client::connect` is generated by
        // `#[service]` and its future (through `SocketPool::acquire`)
        // contains this pool's futures — without the indirection this
        // coroutine's type would be infinitely sized. (No `Send`-proof
        // cycle arises from the recursion: the macro emits client impls
        // as `fn -> impl Future + Send`, so callers take `Send` from the
        // signature instead of inspecting the client bodies.)
        let remote_endpoint =
            match Box::pin(self.acquire_client.connect(acquire_ctx, &connect_request)).await {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    self.invalidate_device_list_cache(peer);
                    return Err(err);
                }
            };
        let control = ConnectionControl {
            connection_id,
            server_connection_cookie: remote_endpoint.connection_cookie,
        };
        if let Err(err) = self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &remote_endpoint,
            self.config.pkey_index,
            connection_config.traffic_class,
        ) {
            // QP setup failures are typically path problems (no route
            // between the selected NIC pair): penalize the pair so
            // placement falls over to other candidates.
            self.blacklist_path(peer, &candidate.path);
            self.schedule_abort_connection(&peer.addr, state, control);
            return Err(err);
        }

        let socket = match self.register_socket(
            queue_pair,
            state,
            &poller,
            &connection_config,
            candidate.path.clone(),
            candidate.local_device_index,
        ) {
            Ok(socket) => socket,
            Err(err) => {
                self.schedule_abort_connection(&peer.addr, state, control);
                return Err(err);
            }
        };
        let registration = SocketRegistrationGuard::new(
            &socket,
            self.task_supervisor.handle(),
            self.acquire_client.clone(),
            Context::create_with_state_and_addr(state, &peer.addr),
            control,
        );
        if acquire_ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA acquire deadline expired before confirmation".into(),
            ));
        }
        self.confirm_connection(acquire_ctx, &control).await?;
        if acquire_ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA acquire deadline expired after confirmation".into(),
            ));
        }
        tracing::info!(
            local_device = %candidate.path.local.device,
            local_port = candidate.path.local.port_num,
            local_gid_index = candidate.path.local.gid_index,
            remote_device = %candidate.remote.device_name,
            remote_port = candidate.remote.port_num,
            remote_gid_index = candidate.remote.gid_index,
            local_qp = socket.queue_pair.qp_num(),
            remote_qp = remote_endpoint.qp_num,
            "acquired RDMA socket"
        );
        Ok(EstablishedSocket {
            socket,
            registration,
        })
    }

    /// Runs the confirmation RPC, retrying once after an ambiguous
    /// response timeout. `confirm` is idempotent on the server (the lease
    /// state machine absorbs duplicates), so this local retry rescues an
    /// otherwise healthy QP from a lost response on the bootstrap
    /// connection instead of tearing it down and failing over paths. The
    /// generic client deliberately never retries after send — idempotency
    /// is knowledge only this call site has.
    async fn confirm_connection(&self, ctx: &Context, control: &ConnectionControl) -> Result<()> {
        match Box::pin(self.acquire_client.confirm(ctx, control)).await {
            Err(err) if matches!(err.kind, ErrorKind::Timeout) && !ctx.is_expired() => {
                tracing::debug!(
                    connection_id = control.connection_id,
                    "RDMA confirm timed out, retrying once"
                );
                Box::pin(self.acquire_client.confirm(ctx, control)).await
            }
            result => result,
        }
    }

    /// Admits one confirmed connection into request rotation.
    /// Ordering is always track -> commit -> publish -> activate.
    pub(super) fn admit_established(
        &self,
        peer: &Arc<PeerState>,
        established: EstablishedSocket,
    ) -> Stripe {
        let EstablishedSocket {
            socket,
            mut registration,
        } = established;
        let stripe = Stripe { socket };
        stripe.socket.set_peer_health(peer);
        registration.commit();
        peer.stripes.write().unwrap().active.push(stripe.clone());
        stripe.socket.request_activation();
        stripe
    }

    /// Atomically publishes an initial stripe set after every handshake
    /// succeeded. Dropping before commit aborts every unadmitted connection.
    fn admit_initial(&self, peer: &Arc<PeerState>, established: Vec<EstablishedSocket>) {
        let mut registrations = Vec::with_capacity(established.len());
        let stripes: Vec<Stripe> = established
            .into_iter()
            .map(|established| {
                let EstablishedSocket {
                    socket,
                    registration,
                } = established;
                socket.set_peer_health(peer);
                registrations.push(registration);
                Stripe { socket }
            })
            .collect();
        for registration in &mut registrations {
            registration.commit();
        }
        peer.stripes.write().unwrap().active = stripes.clone();
        for stripe in &stripes {
            stripe.socket.request_activation();
        }
    }

    /// Replaces `victim` only if it is still in rotation. The replacement is
    /// never visible unless its registration can be committed.
    pub(super) fn admit_replacing(
        &self,
        peer: &Arc<PeerState>,
        victim: &Arc<RdmaSocket>,
        established: EstablishedSocket,
    ) -> bool {
        let EstablishedSocket {
            socket,
            mut registration,
        } = established;
        let mut stripes = peer.stripes.write().unwrap();
        let Some(position) = stripes
            .active
            .iter()
            .position(|stripe| Arc::ptr_eq(&stripe.socket, victim))
        else {
            return false;
        };
        socket.set_peer_health(peer);
        registration.commit();
        stripes.active.push(Stripe {
            socket: socket.clone(),
        });
        let victim = stripes.active.remove(position);
        stripes.draining.push(victim.clone());
        drop(stripes);
        socket.request_activation();
        self.drain_then_close(peer, victim.socket);
        true
    }

    fn schedule_abort_connection(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        control: ConnectionControl,
    ) {
        let abort_ctx = Context::create_with_state_and_addr(state, addr);
        let abort_client = self.acquire_client.clone();
        let _ = self.task_supervisor.handle().try_spawn(async move {
            if let Err(err) = abort_client.abort(&abort_ctx, &control).await {
                tracing::debug!(connection_id = control.connection_id, %err, "RDMA abort cleanup failed");
            }
        });
    }

    async fn fetch_remote_device_list(
        &self,
        peer: &Arc<PeerState>,
        ctx: &Context,
    ) -> Result<RdmaInfo> {
        if let Some(info) = self.get_cached_device_list(peer) {
            return Ok(info);
        }

        // Boxed for the same reason as the `connect` call in
        // `connect_stripe`: keeps this coroutine's type finite.
        let info = Box::pin(self.acquire_client.info(ctx, &())).await?;
        peer.meta.lock().unwrap().device_cache = Some(CachedRdmaInfo {
            info: info.clone(),
            cached_at: Instant::now(),
        });
        Ok(info)
    }

    fn get_cached_device_list(&self, peer: &PeerState) -> Option<RdmaInfo> {
        let meta = peer.meta.lock().unwrap();
        let cached = meta.device_cache.as_ref()?;
        if cached.cached_at.elapsed() < Self::DEVICE_LIST_CACHE_TTL {
            Some(cached.info.clone())
        } else {
            None
        }
    }

    fn invalidate_device_list_cache(&self, peer: &PeerState) {
        peer.meta.lock().unwrap().device_cache = None;
    }
}

/// Everything a placement decision towards one peer needs: the bootstrap
/// context, the peer's advertised device list and the compatible path
/// candidates derived from it.
pub(super) struct ConnectPlan {
    pub(super) acquire_ctx: Context,
    pub(super) remote_info: RdmaInfo,
    pub(super) candidates: Vec<PathCandidate>,
    deadline: Option<Instant>,
}

pub(super) struct CachedRdmaInfo {
    info: RdmaInfo,
    cached_at: Instant,
}
