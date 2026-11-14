//! Acceptor bootstrap: prepare resources and lease the accepted connection
//! until the first data-plane receive proves the initiator published it.

use std::{
    sync::atomic::Ordering,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use super::super::path::{RdmaNicInfo, RdmaPathInfo, gid_ip};
use super::super::{
    ConnectionLease, PrepareConnectionRequest, PrepareConnectionResponse, RdmaConnectionLimits,
    RdmaPeerAdvertisement,
};
use super::{RdmaSocket, RdmaSocketPool};
use crate::{Error, ErrorKind, Result, State};

/// An accepted connection awaiting its first data-plane receive. The
/// initiator sends its activation only after publishing the connection, so
/// that receive proves initiator ownership; until then the lease bounds how
/// long an abandoned QP can live and lets the initiator cancel it.
pub(super) struct AcceptLease {
    socket: Weak<RdmaSocket>,
    accepted_connection_id: u64,
    expires_at: Instant,
}

impl AcceptLease {
    fn matches(&self, lease: &ConnectionLease) -> bool {
        self.accepted_connection_id == lease.accepted_connection_id
    }

    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at <= now
    }

    /// Closes the never-activated connection.
    fn expire(&self, attempt_id: u64) {
        if let Some(socket) = self.socket.upgrade() {
            tracing::warn!(
                attempt_id,
                conn_id = self.accepted_connection_id,
                local_qp = socket.queue_pair.qp_num(),
                path = ?socket.path,
                "RDMA accept lease expired before activation"
            );
            socket.set_error();
        }
    }
}

impl RdmaSocketPool {
    pub(crate) fn rdma_peer_advertisement(&self) -> Result<RdmaPeerAdvertisement> {
        Ok(RdmaPeerAdvertisement::from_devices(
            &self.devices,
            &self.config,
            &self.conn_counts,
        ))
    }

    fn accept_lease_duration(&self) -> Duration {
        Duration::from_millis(self.config.peers.connect_lease_ms)
    }

    pub(crate) fn rdma_prepare_connection(
        &self,
        request: &PrepareConnectionRequest,
        state: &Arc<State>,
    ) -> Result<PrepareConnectionResponse> {
        let span = tracing::debug_span!(
            "rdma.accept.prepare",
            attempt_id = request.attempt_id,
            local_device = %request.target.device_name,
            local_port = request.target.port_num,
            local_gid_index = request.target.gid_index,
            remote_device = %request.source_device,
            remote_qp = request.endpoint.qp_num,
        );
        let _entered = span.enter();
        let started = Instant::now();
        self.prepare_accepted_connection(request, state).map_err(|err| {
            tracing::warn!(error = %err, elapsed_ms = started.elapsed().as_millis(), "RDMA accept preparation failed");
            Error::new(
                err.kind,
                format!(
                    "RDMA accept prepare attempt {} on {}:{}/{} from {} QP {}: {}",
                    request.attempt_id,
                    request.target.device_name,
                    request.target.port_num,
                    request.target.gid_index,
                    request.source_device,
                    request.endpoint.qp_num,
                    err.msg,
                ),
            )
        })
    }

    fn prepare_accepted_connection(
        &self,
        request: &PrepareConnectionRequest,
        state: &Arc<State>,
    ) -> Result<PrepareConnectionResponse> {
        if request.attempt_id == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA bootstrap attempt id must be non-zero".into(),
            ));
        }
        request.endpoint.validate()?;
        request.limits.validate()?;
        let now = Instant::now();
        if let dashmap::mapref::entry::Entry::Occupied(entry) =
            self.accept_leases.entry(request.attempt_id)
        {
            if !entry.get().is_expired(now) && entry.get().socket.strong_count() > 0 {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "duplicate RDMA bootstrap attempt id {} (accepted connection {})",
                        request.attempt_id,
                        entry.get().accepted_connection_id,
                    ),
                ));
            }
            let (_, expired) = entry.remove_entry();
            expired.expire(request.attempt_id);
        }

        let (device_index, _) = self.find_device_by_name(&request.target)?;
        let local = self.prepare_local_connection(
            device_index,
            &request.target,
            request.limits,
            request.traffic_class,
        )?;
        local.connect(self, &request.endpoint)?;
        let local_endpoint = local.endpoint;
        let limits = RdmaConnectionLimits::from(local.config);
        let path = RdmaPathInfo {
            local: RdmaNicInfo {
                device: request.target.device_name.clone(),
                port_num: local_endpoint.port_num,
                gid_index: local_endpoint.gid_index,
                ip: gid_ip(&local_endpoint.gid),
            },
            remote: RdmaNicInfo {
                device: request.source_device.clone(),
                port_num: request.endpoint.port_num,
                gid_index: request.endpoint.gid_index,
                ip: gid_ip(&request.endpoint.gid),
            },
            same_connectivity_domain: request.same_connectivity_domain,
        };
        let socket = local.register(self, state, path)?;
        let lease = ConnectionLease {
            attempt_id: request.attempt_id,
            accepted_connection_id: socket.conn_id,
        };
        match self.accept_leases.entry(request.attempt_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(AcceptLease {
                    socket: Arc::downgrade(&socket),
                    accepted_connection_id: socket.conn_id,
                    expires_at: Instant::now() + self.accept_lease_duration(),
                });
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                socket.set_error();
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("duplicate RDMA bootstrap attempt id {}", request.attempt_id),
                ));
            }
        }
        socket.set_accept_lease(request.attempt_id);
        {
            let mut inbound = self.inbound.lock().unwrap();
            inbound.retain(|conn| conn.strong_count() > 0);
            inbound.push(Arc::downgrade(&socket));
        }
        self.ensure_accept_lease_sweeper(state);
        self.ensure_maintenance_task(state);
        self.ensure_probe_task(state);
        tracing::debug!(
            conn_id = socket.conn_id,
            local_qp = socket.queue_pair.qp_num(),
            lease_ms = self.config.peers.connect_lease_ms,
            limits = ?limits,
            "RDMA connection prepared; awaiting data-plane activation"
        );
        Ok(PrepareConnectionResponse {
            endpoint: local_endpoint,
            lease,
            limits,
        })
    }

    pub(crate) fn rdma_cancel_connection(&self, requested: &ConnectionLease) {
        if let Some((_, lease)) = self
            .accept_leases
            .remove_if(&requested.attempt_id, |_, active| active.matches(requested))
        {
            tracing::debug!(
                attempt_id = requested.attempt_id,
                conn_id = requested.accepted_connection_id,
                "RDMA accepted connection cancelled"
            );
            if let Some(socket) = lease.socket.upgrade() {
                socket.set_error();
            }
        } else {
            tracing::trace!(
                attempt_id = requested.attempt_id,
                conn_id = requested.accepted_connection_id,
                "RDMA cancellation ignored: no matching connection lease"
            );
        }
    }

    pub(crate) fn rdma_receive_observed(&self, attempt_id: u64, socket: &Arc<RdmaSocket>) {
        self.observe_accept_receive(attempt_id, &Arc::downgrade(socket));
    }

    /// The first receive activates the connection: drop its lease and allow
    /// probes. A receive after expiry cannot revive a closed connection.
    fn observe_accept_receive(&self, attempt_id: u64, socket: &Weak<RdmaSocket>) {
        let Some((_, lease)) = self
            .accept_leases
            .remove_if(&attempt_id, |_, lease| lease.socket.ptr_eq(socket))
        else {
            return;
        };
        if lease.is_expired(Instant::now()) {
            lease.expire(attempt_id);
            return;
        }
        if let Some(socket) = lease.socket.upgrade() {
            socket.enable_probes();
        }
        tracing::debug!(
            attempt_id,
            conn_id = lease.accepted_connection_id,
            "RDMA accepted connection activated by its first data-plane receive"
        );
    }

    fn sweep_accept_leases(&self, now: Instant) {
        self.accept_leases.retain(|attempt_id, lease| {
            if lease.socket.strong_count() == 0 {
                return false;
            }
            if !lease.is_expired(now) {
                return true;
            }
            lease.expire(*attempt_id);
            false
        });
    }

    fn ensure_accept_lease_sweeper(&self, state: &Arc<State>) {
        if self.lease_sweeper_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let interval =
            Duration::from_millis((self.config.peers.connect_lease_ms / 4).clamp(100, 1_000));
        let weak_state = Arc::downgrade(state);
        if self
            .task_supervisor
            .handle()
            .try_spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(state) = weak_state.upgrade() else {
                        break;
                    };
                    let Some(pool) = state.socket_pool.rdma_pool() else {
                        break;
                    };
                    pool.sweep_accept_leases(Instant::now());
                }
            })
            .is_none()
        {
            self.lease_sweeper_started.store(false, Ordering::Relaxed);
            tracing::debug!("RDMA accept lease sweeper did not start: pool is stopping");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEASE_DURATION: Duration = Duration::from_secs(30);

    fn pending_lease(now: Instant) -> AcceptLease {
        AcceptLease {
            socket: Weak::new(),
            accepted_connection_id: 7,
            expires_at: now + LEASE_DURATION,
        }
    }

    fn make_pool() -> RdmaSocketPool {
        // Lease bookkeeping has no hardware dependency.
        let devices = Arc::from([]);
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(Vec::new()).build();
        RdmaSocketPool::new(
            devices,
            buffer_pool,
            super::super::super::RdmaSocketPoolConfig::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn cancellation_requires_the_matching_connection_identity() {
        let pool = make_pool();
        let attempt_id = 42;
        pool.accept_leases
            .insert(attempt_id, pending_lease(Instant::now()));
        pool.rdma_cancel_connection(&ConnectionLease {
            attempt_id,
            accepted_connection_id: 8,
        });
        assert!(pool.accept_leases.contains_key(&attempt_id));
        pool.rdma_cancel_connection(&ConnectionLease {
            attempt_id,
            accepted_connection_id: 7,
        });
        assert!(!pool.accept_leases.contains_key(&attempt_id));
    }

    #[tokio::test]
    async fn first_receive_retires_only_its_own_lease() {
        let pool = make_pool();
        let attempt_id = 42;
        let lease = pending_lease(Instant::now());
        let socket = lease.socket.clone();
        pool.accept_leases.insert(attempt_id, lease);
        pool.observe_accept_receive(attempt_id + 1, &socket);
        assert!(pool.accept_leases.contains_key(&attempt_id));
        pool.observe_accept_receive(attempt_id, &socket);
        assert!(!pool.accept_leases.contains_key(&attempt_id));
    }

    #[tokio::test]
    async fn leases_expire_at_their_deadline() {
        let now = Instant::now();
        let lease = pending_lease(now);
        assert!(!lease.is_expired(now));
        assert!(lease.is_expired(lease.expires_at));
    }
}
