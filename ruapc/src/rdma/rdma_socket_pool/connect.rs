//! Outbound pool orchestration: discovery, path failover, stripe publication.
//! The per-path bootstrap transaction lives in `handshake`.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use super::super::rdma_service::RDMA_BOOTSTRAP_PROTOCOL_VERSION;
use super::super::{RdmaBootstrapService as _, RdmaPeerAdvertisement, RdmaSocket};
use super::handshake::EstablishedSocket;
use super::placement::{PathCandidate, PathPreference};
use super::setup::at_stage;
use super::{PeerState, RdmaSocketPool, Stripe};
use crate::{Context, Error, ErrorKind, Result, Socket, State};

impl RdmaSocketPool {
    const ADVERTISEMENT_CACHE_TTL: Duration = Duration::from_secs(30);

    pub(super) async fn connect_peer(
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
            Err(err) if fallback.is_some() => {
                tracing::debug!(peer = %addr, %err, "RDMA discovery failed; using existing stripe");
                return Ok(fallback.expect("checked above"));
            }
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
                Err(err) if fallback.is_some() => {
                    tracing::debug!(peer = %addr, %err, "RDMA additional connection failed; using existing stripe");
                    return Ok(fallback.expect("checked above"));
                }
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

        // Earlier handshakes may have closed while later stripes were being
        // prepared. Keep the initial set all-or-nothing at publication.
        if let Some(stripe) = stripes.iter().find(|stripe| !stripe.socket.state.is_ok()) {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                format!(
                    "RDMA initial stripe {} to {addr} closed before publication",
                    stripe.socket.conn_id
                ),
            ));
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
                format!(
                    "RDMA discovery for {}: connection deadline expired",
                    peer.addr
                ),
            ));
        }
        let mut bootstrap_ctx = Context::create_with_state_and_addr(state, &peer.addr);
        bootstrap_ctx.deadline = deadline;
        let remote_info = self.fetch_peer_advertisement(peer, &bootstrap_ctx).await?;
        let candidates = match self.enumerate_path_candidates(&remote_info) {
            Ok(candidates) => candidates,
            Err(err) => {
                self.invalidate_advertisement_cache(peer);
                return Err(at_stage(&format!("select paths to {}", peer.addr), err));
            }
        };
        tracing::debug!(peer = %peer.addr, remote_devices = remote_info.devices.len(), candidates = candidates.len(), "RDMA connection paths enumerated");
        Ok(ConnectPlan {
            bootstrap_ctx,
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
        let mut last_failure = None;
        let mut attempted = 0;
        loop {
            if plan
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(Error::new(
                    ErrorKind::Timeout,
                    format!(
                        "RDMA path failover to {}: connection deadline expired after {attempted} attempts",
                        peer.addr
                    ),
                ));
            }
            let candidate = match self.select_candidate(
                peer,
                &remaining,
                preference,
                &plan.remote_info,
                existing,
            ) {
                Ok(candidate) => candidate,
                Err(err) => {
                    return Err(last_failure.unwrap_or_else(|| {
                        at_stage(&format!("select path to {}", peer.addr), err)
                    }));
                }
            };
            attempted += 1;
            match self
                .connect_stripe(peer, state, &plan.bootstrap_ctx, &candidate)
                .await
            {
                Ok(socket) => return Ok(socket),
                Err(err) => {
                    remaining.retain(|remaining| remaining.path != candidate.path);
                    let err = at_stage(
                        &format!(
                            "path failover exhausted {attempted} attempts to {}",
                            peer.addr
                        ),
                        err,
                    );
                    if remaining.is_empty() {
                        return Err(err);
                    }
                    tracing::debug!(
                        peer = %peer.addr,
                        local = %candidate.path.local.device,
                        remote = %candidate.path.remote.device,
                        remaining_candidates = remaining.len(),
                        "trying another RDMA NIC pair"
                    );
                    last_failure = Some(err);
                }
            }
        }
    }

    /// Admits one confirmed connection into request rotation.
    /// Under the publication lock: track -> disarm rollback -> publish;
    /// then request data-plane activation.
    pub(super) fn admit_established(
        &self,
        peer: &Arc<PeerState>,
        established: EstablishedSocket,
    ) -> Stripe {
        let EstablishedSocket { socket, rollback } = established;
        let stripe = Stripe { socket };
        let mut stripes = peer.stripes.write().unwrap();
        stripe.socket.set_peer_health(peer);
        rollback.disarm();
        stripes.active.push(stripe.clone());
        drop(stripes);
        stripe.socket.request_activation();
        tracing::debug!(peer = %peer.addr, conn_id = stripe.socket.conn_id, "RDMA stripe published; activation requested");
        stripe
    }

    /// Atomically publishes an initial stripe set after every handshake
    /// succeeded. Dropping before publication cancels every unadmitted connection.
    fn admit_initial(&self, peer: &Arc<PeerState>, established: Vec<EstablishedSocket>) {
        let mut rollbacks = Vec::with_capacity(established.len());
        let stripes: Vec<Stripe> = established
            .into_iter()
            .map(|established| {
                let EstablishedSocket { socket, rollback } = established;
                socket.set_peer_health(peer);
                rollbacks.push(rollback);
                Stripe { socket }
            })
            .collect();
        let mut published = peer.stripes.write().unwrap();
        for rollback in rollbacks {
            rollback.disarm();
        }
        published.active = stripes.clone();
        drop(published);
        for stripe in &stripes {
            stripe.socket.request_activation();
        }
        tracing::debug!(peer = %peer.addr, stripes = stripes.len(), "RDMA initial stripes published; activation requested");
    }

    /// Replaces `victim` only if it is still in rotation. The replacement is
    /// never visible unless its registration can be committed.
    pub(super) fn admit_replacing(
        &self,
        peer: &Arc<PeerState>,
        victim: &Arc<RdmaSocket>,
        established: EstablishedSocket,
    ) -> bool {
        let EstablishedSocket { socket, rollback } = established;
        let mut stripes = peer.stripes.write().unwrap();
        let Some(position) = stripes
            .active
            .iter()
            .position(|stripe| Arc::ptr_eq(&stripe.socket, victim))
        else {
            return false;
        };
        socket.set_peer_health(peer);
        rollback.disarm();
        stripes.active.push(Stripe {
            socket: socket.clone(),
        });
        let victim = stripes.active.remove(position);
        stripes.draining.push(victim.clone());
        drop(stripes);
        socket.request_activation();
        tracing::debug!(peer = %peer.addr, conn_id = socket.conn_id, previous_conn_id = victim.socket.conn_id, "RDMA replacement published; activation requested");
        self.drain_then_close(peer, victim.socket);
        true
    }

    async fn fetch_peer_advertisement(
        &self,
        peer: &Arc<PeerState>,
        ctx: &Context,
    ) -> Result<RdmaPeerAdvertisement> {
        if let Some(info) = self.get_cached_advertisement(peer) {
            tracing::debug!(peer = %peer.addr, devices = info.devices.len(), "using cached RDMA advertisement");
            return Ok(info);
        }

        // Boxed to break the bootstrap RPC / socket acquire future recursion.
        let info = Box::pin(self.bootstrap_client.discover(ctx, &()))
            .await
            .map_err(|err| at_stage(&format!("discover peer {}", peer.addr), err))?;
        if info.protocol_version != RDMA_BOOTSTRAP_PROTOCOL_VERSION {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA peer {} advertised unsupported bootstrap protocol version {} (expected {})",
                    peer.addr, info.protocol_version, RDMA_BOOTSTRAP_PROTOCOL_VERSION
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

    pub(super) fn invalidate_advertisement_cache(&self, peer: &PeerState) {
        peer.meta.lock().unwrap().device_cache = None;
    }
}

/// Everything a placement decision towards one peer needs: the bootstrap
/// context, the peer's advertised device list and the compatible path
/// candidates derived from it.
pub(super) struct ConnectPlan {
    pub(super) bootstrap_ctx: Context,
    pub(super) remote_info: RdmaPeerAdvertisement,
    pub(super) candidates: Vec<PathCandidate>,
    deadline: Option<Instant>,
}

pub(super) struct CachedPeerAdvertisement {
    info: RdmaPeerAdvertisement,
    cached_at: Instant,
}
