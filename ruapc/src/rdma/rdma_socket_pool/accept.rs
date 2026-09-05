//! Acceptor bootstrap: prepare resources, confirm ownership and observe the
//! first data-plane receive. Until both signals arrive, the connection is leased.

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptLeaseState {
    Pending,
    ReceiveObserved,
    Confirmed,
    Active,
}

#[derive(Clone, Copy, Debug)]
enum AcceptLeaseEvent {
    Confirm,
    Receive,
}

impl AcceptLeaseState {
    fn advance(self, event: AcceptLeaseEvent) -> Self {
        match (self, event) {
            (Self::Pending, AcceptLeaseEvent::Confirm) => Self::Confirmed,
            (Self::Pending, AcceptLeaseEvent::Receive) => Self::ReceiveObserved,
            (Self::Confirmed, AcceptLeaseEvent::Receive)
            | (Self::ReceiveObserved, AcceptLeaseEvent::Confirm) => Self::Active,
            _ => self,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum LeaseExpiry {
    CloseInactiveConnection,
    ForgetActiveTombstone,
}

pub(super) struct AcceptLease {
    socket: Weak<RdmaSocket>,
    accepted_connection_id: u64,
    state: AcceptLeaseState,
    expires_at: Instant,
}

impl AcceptLease {
    fn matches(&self, lease: &ConnectionLease) -> bool {
        self.accepted_connection_id == lease.accepted_connection_id
    }

    /// The first confirmation starts the activation budget. Replayed events
    /// cannot prolong an unfinished handshake. Activation starts only the
    /// tombstone retention period; it no longer limits the socket's lifetime.
    fn advance(&mut self, event: AcceptLeaseEvent, now: Instant, lease_duration: Duration) {
        let previous = self.state;
        self.state = previous.advance(event);
        if self.state != previous
            && matches!(
                self.state,
                AcceptLeaseState::Confirmed | AcceptLeaseState::Active
            )
        {
            self.expires_at = now + lease_duration;
        }
    }

    fn expiry(&self, now: Instant) -> Option<LeaseExpiry> {
        (self.expires_at <= now).then(|| {
            if self.state == AcceptLeaseState::Active {
                LeaseExpiry::ForgetActiveTombstone
            } else {
                LeaseExpiry::CloseInactiveConnection
            }
        })
    }

    /// All expiration paths use the same policy. An active entry is only an
    /// idempotency tombstone; forgetting it must never close a healthy QP.
    fn expire(&self, attempt_id: u64, now: Instant) {
        let Some(socket) = self.socket.upgrade() else {
            return;
        };
        match self.expiry(now) {
            Some(LeaseExpiry::CloseInactiveConnection) => {
                tracing::warn!(
                    attempt_id,
                    conn_id = self.accepted_connection_id,
                    local_qp = socket.queue_pair.qp_num(),
                    state = ?self.state,
                    path = ?socket.path,
                    "RDMA accept lease expired before activation"
                );
                socket.set_error();
            }
            Some(LeaseExpiry::ForgetActiveTombstone) => {
                tracing::trace!(
                    attempt_id,
                    conn_id = self.accepted_connection_id,
                    "RDMA active connection tombstone expired"
                );
            }
            None => {}
        }
    }
}

impl RdmaSocketPool {
    pub(crate) fn rdma_peer_advertisement(&self) -> Result<RdmaPeerAdvertisement> {
        Ok(RdmaPeerAdvertisement::from_devices(
            self.devices.rdma_devices(),
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
            if entry.get().expiry(now).is_none() && entry.get().socket.strong_count() > 0 {
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
            expired.expire(request.attempt_id, now);
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
                    state: AcceptLeaseState::Pending,
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
        tracing::debug!(
            conn_id = socket.conn_id,
            local_qp = socket.queue_pair.qp_num(),
            lease_ms = self.config.peers.connect_lease_ms,
            limits = ?limits,
            "RDMA connection prepared; awaiting commit and data-plane receive"
        );
        Ok(PrepareConnectionResponse {
            endpoint: local_endpoint,
            lease,
            limits,
        })
    }

    pub(crate) fn rdma_commit_connection(&self, lease: &ConnectionLease) -> Result<()> {
        let dashmap::mapref::entry::Entry::Occupied(mut entry) =
            self.accept_leases.entry(lease.attempt_id)
        else {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA commit attempt {} connection {}: unknown or expired lease",
                    lease.attempt_id, lease.accepted_connection_id,
                ),
            ));
        };
        if !entry.get().matches(lease) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA commit attempt {}: connection identity mismatch (expected {}, received {})",
                    lease.attempt_id,
                    entry.get().accepted_connection_id,
                    lease.accepted_connection_id,
                ),
            ));
        }
        let now = Instant::now();
        if entry.get().expiry(now).is_some() {
            let (_, expired) = entry.remove_entry();
            expired.expire(lease.attempt_id, now);
            return Err(Error::new(
                ErrorKind::Timeout,
                format!(
                    "RDMA commit attempt {} connection {}: lease expired in {:?} state",
                    lease.attempt_id, lease.accepted_connection_id, expired.state,
                ),
            ));
        }
        if !entry
            .get()
            .socket
            .upgrade()
            .is_some_and(|socket| socket.state.is_ok())
        {
            entry.remove();
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                format!(
                    "RDMA commit attempt {} connection {}: accepted connection already closed",
                    lease.attempt_id, lease.accepted_connection_id,
                ),
            ));
        }
        let previous = entry.get().state;
        entry
            .get_mut()
            .advance(AcceptLeaseEvent::Confirm, now, self.accept_lease_duration());
        tracing::debug!(
            attempt_id = lease.attempt_id,
            conn_id = lease.accepted_connection_id,
            previous_state = ?previous,
            state = ?entry.get().state,
            "RDMA connection commit acknowledged"
        );
        Ok(())
    }

    pub(crate) fn rdma_cancel_connection(&self, requested: &ConnectionLease) {
        if let Some((_, lease)) = self
            .accept_leases
            .remove_if(&requested.attempt_id, |_, active| active.matches(requested))
        {
            tracing::debug!(
                attempt_id = requested.attempt_id,
                conn_id = requested.accepted_connection_id,
                state = ?lease.state,
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

    fn observe_accept_receive(&self, attempt_id: u64, socket: &Weak<RdmaSocket>) {
        let dashmap::mapref::entry::Entry::Occupied(mut entry) =
            self.accept_leases.entry(attempt_id)
        else {
            return;
        };
        if !entry.get().socket.ptr_eq(socket) {
            return;
        }
        let now = Instant::now();
        if entry.get().expiry(now).is_some() {
            let (_, expired) = entry.remove_entry();
            expired.expire(attempt_id, now);
            return;
        }
        let previous = entry.get().state;
        entry
            .get_mut()
            .advance(AcceptLeaseEvent::Receive, now, self.accept_lease_duration());
        tracing::debug!(
            attempt_id,
            conn_id = entry.get().accepted_connection_id,
            previous_state = ?previous,
            state = ?entry.get().state,
            "RDMA accepted connection observed its first data-plane receive"
        );
    }

    fn sweep_accept_leases(&self, now: Instant) {
        self.accept_leases.retain(|attempt_id, lease| {
            if lease.socket.strong_count() == 0 {
                return false;
            }
            if lease.expiry(now).is_none() {
                return true;
            }
            lease.expire(*attempt_id, now);
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
            state: AcceptLeaseState::Pending,
            expires_at: now + LEASE_DURATION,
        }
    }

    fn make_pool() -> RdmaSocketPool {
        // Lease bookkeeping has no hardware dependency.
        let devices = Arc::new(crate::Devices::default());
        let buffer_pool = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
        RdmaSocketPool::new(
            devices,
            buffer_pool,
            super::super::super::RdmaSocketPoolConfig::default(),
        )
        .unwrap()
    }

    #[test]
    fn activation_requires_confirmation_and_receive_in_either_order() {
        let started = Instant::now();
        for (first, second, intermediate) in [
            (
                AcceptLeaseEvent::Confirm,
                AcceptLeaseEvent::Receive,
                AcceptLeaseState::Confirmed,
            ),
            (
                AcceptLeaseEvent::Receive,
                AcceptLeaseEvent::Confirm,
                AcceptLeaseState::ReceiveObserved,
            ),
        ] {
            let mut lease = pending_lease(started);
            lease.advance(first, started + Duration::from_secs(1), LEASE_DURATION);
            assert_eq!(lease.state, intermediate);
            assert_eq!(
                lease.expiry(lease.expires_at),
                Some(LeaseExpiry::CloseInactiveConnection),
            );

            let activated_at = started + Duration::from_secs(2);
            lease.advance(second, activated_at, LEASE_DURATION);
            assert_eq!(lease.state, AcceptLeaseState::Active);
            assert_eq!(lease.expires_at, activated_at + LEASE_DURATION);
            assert_eq!(lease.expiry(activated_at), None);
            assert_eq!(
                lease.expiry(lease.expires_at),
                Some(LeaseExpiry::ForgetActiveTombstone),
            );
        }
    }

    #[test]
    fn duplicate_commits_do_not_extend_activation_or_tombstone_deadlines() {
        let started = Instant::now();
        let mut lease = pending_lease(started);
        let confirmed_at = started + Duration::from_secs(5);
        lease.advance(AcceptLeaseEvent::Confirm, confirmed_at, LEASE_DURATION);
        let activation_deadline = confirmed_at + LEASE_DURATION;
        assert_eq!(lease.expires_at, activation_deadline);

        for elapsed in [1, 5, 29] {
            lease.advance(
                AcceptLeaseEvent::Confirm,
                confirmed_at + Duration::from_secs(elapsed),
                LEASE_DURATION,
            );
            assert_eq!(lease.expires_at, activation_deadline);
        }
        assert_eq!(
            lease.expiry(activation_deadline),
            Some(LeaseExpiry::CloseInactiveConnection),
        );

        let activated_at = activation_deadline - Duration::from_secs(1);
        lease.advance(AcceptLeaseEvent::Receive, activated_at, LEASE_DURATION);
        let tombstone_deadline = lease.expires_at;
        lease.advance(
            AcceptLeaseEvent::Confirm,
            tombstone_deadline - Duration::from_secs(1),
            LEASE_DURATION,
        );
        assert_eq!(lease.expires_at, tombstone_deadline);
        assert_eq!(
            lease.expiry(tombstone_deadline),
            Some(LeaseExpiry::ForgetActiveTombstone),
        );
    }

    #[test]
    fn unconfirmed_receives_do_not_extend_the_prepare_deadline() {
        let started = Instant::now();
        let mut lease = pending_lease(started);
        let deadline = lease.expires_at;
        for elapsed in [1, 5, 29] {
            lease.advance(
                AcceptLeaseEvent::Receive,
                started + Duration::from_secs(elapsed),
                LEASE_DURATION,
            );
            assert_eq!(lease.expires_at, deadline);
        }
        assert_eq!(lease.state, AcceptLeaseState::ReceiveObserved);
        assert_eq!(
            lease.expiry(deadline),
            Some(LeaseExpiry::CloseInactiveConnection),
        );
    }

    #[tokio::test]
    async fn lease_control_rejects_mismatched_connection_identity() {
        let pool = make_pool();
        let attempt_id = 42;
        pool.accept_leases
            .insert(attempt_id, pending_lease(Instant::now()));
        let mismatched = ConnectionLease {
            attempt_id,
            accepted_connection_id: 8,
        };
        let err = pool.rdma_commit_connection(&mismatched).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        assert!(err.msg.contains("expected 7, received 8"));
        pool.rdma_cancel_connection(&mismatched);
        assert!(pool.accept_leases.contains_key(&attempt_id));

        pool.rdma_cancel_connection(&ConnectionLease {
            attempt_id,
            accepted_connection_id: 7,
        });
        assert!(!pool.accept_leases.contains_key(&attempt_id));
    }

    #[tokio::test]
    async fn late_commit_removes_expired_leases_at_the_expiration_boundary() {
        let pool = make_pool();
        let attempt_id = 42;
        for state in [
            AcceptLeaseState::Pending,
            AcceptLeaseState::ReceiveObserved,
            AcceptLeaseState::Confirmed,
            AcceptLeaseState::Active,
        ] {
            let mut lease = pending_lease(Instant::now());
            lease.state = state;
            lease.expires_at = Instant::now();
            pool.accept_leases.insert(attempt_id, lease);
            let err = pool
                .rdma_commit_connection(&ConnectionLease {
                    attempt_id,
                    accepted_connection_id: 7,
                })
                .unwrap_err();
            assert_eq!(err.kind, ErrorKind::Timeout);
            assert!(err.msg.contains(&format!("{state:?}")));
            assert!(!pool.accept_leases.contains_key(&attempt_id));
        }
    }

    #[tokio::test]
    async fn receive_notification_updates_the_tracked_lease() {
        let pool = make_pool();
        let attempt_id = 42;
        let lease = pending_lease(Instant::now());
        let socket = lease.socket.clone();
        pool.accept_leases.insert(attempt_id, lease);
        pool.observe_accept_receive(attempt_id, &socket);
        assert_eq!(
            pool.accept_leases.get(&attempt_id).unwrap().state,
            AcceptLeaseState::ReceiveObserved,
        );
        pool.accept_leases.get_mut(&attempt_id).unwrap().state = AcceptLeaseState::Confirmed;
        pool.observe_accept_receive(attempt_id, &socket);
        assert_eq!(
            pool.accept_leases.get(&attempt_id).unwrap().state,
            AcceptLeaseState::Active,
        );
    }
}
