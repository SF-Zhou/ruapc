//! Client side of RDMA connection setup: handshake, stripe establishment,
//! admission and the connect-plan/advertisement machinery.

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use super::super::rdma_service::RDMA_BOOTSTRAP_PROTOCOL_VERSION;
use super::super::{
    ConnectionLease, PrepareConnectionRequest, RdmaBootstrapService as _, RdmaConnectionLimits,
    RdmaPeerAdvertisement, RdmaSocket,
};
use super::placement::{PathCandidate, PathPreference};
use super::{PeerState, RdmaSocketPool, Stripe, next_attempt_id};
use crate::{Client, Context, Error, ErrorKind, Result, Socket, State};

pub(super) struct SocketRegistrationGuard {
    socket: Arc<RdmaSocket>,
    armed: bool,
    cancel: Option<(
        crate::TaskSupervisorHandle,
        Client,
        Context,
        ConnectionLease,
    )>,
}

impl SocketRegistrationGuard {
    pub(super) fn new(
        socket: &Arc<RdmaSocket>,
        supervisor: crate::TaskSupervisorHandle,
        client: Client,
        context: Context,
        lease: ConnectionLease,
    ) -> Self {
        Self {
            socket: socket.clone(),
            armed: true,
            cancel: Some((supervisor, client, context, lease)),
        }
    }

    pub(super) fn commit(&mut self) {
        self.armed = false;
        self.cancel = None;
    }
}

impl Drop for SocketRegistrationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.socket.set_error();
            if let Some((supervisor, client, context, lease)) = self.cancel.take() {
                let _ = supervisor.try_spawn(async move {
                    if let Err(err) = client.cancel_connection(&context, &lease).await {
                        tracing::debug!(attempt_id = lease.attempt_id, %err, "RDMA cancellation cleanup failed");
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
    const ADVERTISEMENT_CACHE_TTL: Duration = Duration::from_secs(30);

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
            let max_connections = self.config.peers.preconnect_max_per_peer as usize;
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
        let stripe_count = self.config.peers.connections_per_peer;
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
        let remote_info = self.fetch_peer_advertisement(peer, &acquire_ctx).await?;
        let candidates = match self.enumerate_path_candidates(&remote_info) {
            Ok(candidates) => candidates,
            Err(err) => {
                self.invalidate_advertisement_cache(peer);
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
        let connection_config =
            self.negotiate_connection_config(device, &candidate.remote_limits)?;
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.polling.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
        let local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            candidate.path.local.port_num,
            candidate.path.local.gid_index,
        )?;

        let attempt_id = next_attempt_id();
        let prepare_request = PrepareConnectionRequest {
            attempt_id,
            endpoint: local_endpoint,
            source_device: candidate.path.local.device.clone(),
            same_connectivity_domain: candidate.path.same_connectivity_domain,
            target: candidate.remote.clone(),
            limits: RdmaConnectionLimits::from(connection_config),
            traffic_class: connection_config.traffic_class,
        };
        // Box the recursive RPC call: `Client::prepare_connection` is generated by
        // `#[service]` and its future (through `SocketPool::acquire`)
        // contains this pool's futures — without the indirection this
        // coroutine's type would be infinitely sized. (No `Send`-proof
        // cycle arises from the recursion: the macro emits client impls
        // as `fn -> impl Future + Send`, so callers take `Send` from the
        // signature instead of inspecting the client bodies.)
        let response = match Box::pin(
            self.acquire_client
                .prepare_connection(acquire_ctx, &prepare_request),
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                self.invalidate_advertisement_cache(peer);
                return Err(err);
            }
        };
        if response.lease.attempt_id != attempt_id || response.lease.accepted_connection_id == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "peer returned an invalid RDMA connection lease".into(),
            ));
        }
        let remote_endpoint = response.endpoint;
        let lease = response.lease;
        if let Err(err) = self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &remote_endpoint,
            self.config.connection.pkey_index,
            connection_config.traffic_class,
        ) {
            // QP setup failures are typically path problems (no route
            // between the selected NIC pair): penalize the pair so
            // placement falls over to other candidates.
            self.blacklist_path(peer, &candidate.path);
            self.schedule_cancel_connection(&peer.addr, state, lease);
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
                self.schedule_cancel_connection(&peer.addr, state, lease);
                return Err(err);
            }
        };
        let registration = SocketRegistrationGuard::new(
            &socket,
            self.task_supervisor.handle(),
            self.acquire_client.clone(),
            Context::create_with_state_and_addr(state, &peer.addr),
            lease,
        );
        if acquire_ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA acquire deadline expired before commit".into(),
            ));
        }
        self.commit_connection(acquire_ctx, &lease).await?;
        if acquire_ctx.is_expired() {
            return Err(Error::new(
                ErrorKind::Timeout,
                "RDMA acquire deadline expired after commit".into(),
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

    /// Runs the commit RPC, retrying once after an ambiguous response
    /// timeout. `commit_connection` is idempotent on the server (the lease
    /// state machine absorbs duplicates), so this local retry rescues an
    /// otherwise healthy QP from a lost response on the bootstrap
    /// connection instead of tearing it down and failing over paths. The
    /// generic client deliberately never retries after send — idempotency
    /// is knowledge only this call site has.
    async fn commit_connection(&self, ctx: &Context, lease: &ConnectionLease) -> Result<()> {
        match Box::pin(self.acquire_client.commit_connection(ctx, lease)).await {
            Err(err) if matches!(err.kind, ErrorKind::Timeout) && !ctx.is_expired() => {
                tracing::debug!(
                    attempt_id = lease.attempt_id,
                    "RDMA connection commit timed out, retrying once"
                );
                Box::pin(self.acquire_client.commit_connection(ctx, lease)).await
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
    /// succeeded. Dropping before commit cancels every unadmitted connection.
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

    fn schedule_cancel_connection(
        &self,
        addr: &SocketAddr,
        state: &Arc<State>,
        lease: ConnectionLease,
    ) {
        let cancel_ctx = Context::create_with_state_and_addr(state, addr);
        let cancel_client = self.acquire_client.clone();
        let _ = self.task_supervisor.handle().try_spawn(async move {
            if let Err(err) = cancel_client.cancel_connection(&cancel_ctx, &lease).await {
                tracing::debug!(attempt_id = lease.attempt_id, %err, "RDMA cancellation cleanup failed");
            }
        });
    }

    async fn fetch_peer_advertisement(
        &self,
        peer: &Arc<PeerState>,
        ctx: &Context,
    ) -> Result<RdmaPeerAdvertisement> {
        if let Some(info) = self.get_cached_advertisement(peer) {
            return Ok(info);
        }

        // Boxed for the same reason as the `prepare_connection` call in
        // `connect_stripe`: keeps this coroutine's type finite.
        let info = Box::pin(self.acquire_client.discover(ctx, &())).await?;
        if info.protocol_version != RDMA_BOOTSTRAP_PROTOCOL_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "unsupported RDMA bootstrap protocol version {} (expected {})",
                    info.protocol_version, RDMA_BOOTSTRAP_PROTOCOL_VERSION
                ),
            ));
        }
        peer.meta.lock().unwrap().device_cache = Some(CachedPeerAdvertisement {
            info: info.clone(),
            cached_at: Instant::now(),
        });
        Ok(info)
    }

    fn get_cached_advertisement(&self, peer: &PeerState) -> Option<RdmaPeerAdvertisement> {
        let meta = peer.meta.lock().unwrap();
        let cached = meta.device_cache.as_ref()?;
        if cached.cached_at.elapsed() < Self::ADVERTISEMENT_CACHE_TTL {
            Some(cached.info.clone())
        } else {
            None
        }
    }

    fn invalidate_advertisement_cache(&self, peer: &PeerState) {
        peer.meta.lock().unwrap().device_cache = None;
    }
}

/// Everything a placement decision towards one peer needs: the bootstrap
/// context, the peer's advertised device list and the compatible path
/// candidates derived from it.
pub(super) struct ConnectPlan {
    pub(super) acquire_ctx: Context,
    pub(super) remote_info: RdmaPeerAdvertisement,
    pub(super) candidates: Vec<PathCandidate>,
    deadline: Option<Instant>,
}

pub(super) struct CachedPeerAdvertisement {
    info: RdmaPeerAdvertisement,
    cached_at: Instant,
}
