//! Server side of RDMA connection setup: accept, confirm/abort and the
//! accept-lease lifecycle.

use std::{
    sync::atomic::Ordering,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use super::super::path::{RdmaNicInfo, RdmaPathInfo, gid_ip};
use super::super::{ConnectRequest, ConnectionControl, Endpoint, RdmaInfo};
use super::{RdmaSocket, RdmaSocketPool};
use crate::{Error, ErrorKind, Result, State};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AcceptLeaseState {
    Pending,
    ReceiveObserved,
    Confirmed,
    Active,
}

#[derive(Clone, Copy)]
pub(super) enum AcceptLeaseEvent {
    Confirm,
    Receive,
}

pub(super) fn advance_accept_lease(
    state: AcceptLeaseState,
    event: AcceptLeaseEvent,
) -> AcceptLeaseState {
    match (state, event) {
        (AcceptLeaseState::Pending, AcceptLeaseEvent::Confirm) => AcceptLeaseState::Confirmed,
        (AcceptLeaseState::Pending, AcceptLeaseEvent::Receive) => AcceptLeaseState::ReceiveObserved,
        (AcceptLeaseState::Confirmed, AcceptLeaseEvent::Confirm) => AcceptLeaseState::Confirmed,
        (AcceptLeaseState::Active, AcceptLeaseEvent::Confirm) => AcceptLeaseState::Active,
        (AcceptLeaseState::ReceiveObserved, AcceptLeaseEvent::Receive) => {
            AcceptLeaseState::ReceiveObserved
        }
        (AcceptLeaseState::Confirmed, AcceptLeaseEvent::Receive)
        | (AcceptLeaseState::ReceiveObserved, AcceptLeaseEvent::Confirm) => {
            AcceptLeaseState::Active
        }
        (AcceptLeaseState::Active, AcceptLeaseEvent::Receive) => AcceptLeaseState::Active,
    }
}

pub(super) struct AcceptLease {
    pub(super) socket: Weak<RdmaSocket>,
    pub(super) server_connection_cookie: u64,
    pub(super) state: AcceptLeaseState,
    pub(super) expires_at: Instant,
}

impl RdmaSocketPool {
    pub(crate) fn rdma_device_list(&self) -> Result<RdmaInfo> {
        Ok(RdmaInfo::from_devices(
            self.devices.rdma_devices(),
            &self.config,
            &self.conn_counts,
        ))
    }

    pub(crate) fn rdma_accept(
        &self,
        request: &ConnectRequest,
        state: &Arc<State>,
    ) -> Result<Endpoint> {
        if request.connection_id == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA connection id must be non-zero".into(),
            ));
        }
        if let dashmap::mapref::entry::Entry::Occupied(entry) =
            self.accept_leases.entry(request.connection_id)
        {
            if entry.get().expires_at > Instant::now() && entry.get().socket.strong_count() > 0 {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("duplicate RDMA connection id {}", request.connection_id),
                ));
            }
            let (_, expired) = entry.remove_entry();
            if let Some(socket) = expired.socket.upgrade() {
                socket.set_error();
            }
        }
        let (device_index, device) = self.find_device_by_name(&request.target)?;
        let connection_config = self.clamp_connection_config(device, request.config);
        let poller = self.pollers.get_or_start(
            device,
            self.poller_config(),
            self.config.poll_threads_per_device,
        )?;
        let queue_pair = self.create_queue_pair(device, &connection_config, &poller)?;
        let mut local_endpoint = self.build_endpoint(
            &queue_pair,
            device,
            request.target.port_num,
            request.target.gid_index,
        )?;
        self.bring_qp_to_rts(
            &queue_pair,
            &local_endpoint,
            &request.endpoint,
            self.config.pkey_index,
            connection_config.traffic_class,
        )?;

        let (info, gid_zones) = device.info_with_zones();
        let local_ip = Self::find_port(&info, request.target.port_num)
            .ok()
            .and_then(|port| port.find_gid(request.target.gid_index))
            .and_then(|gid| gid_ip(&gid.gid));
        let path = RdmaPathInfo {
            local: RdmaNicInfo {
                device: info.name.clone(),
                port_num: request.target.port_num,
                gid_index: request.target.gid_index,
                ip: local_ip,
                zones: gid_zones
                    .get(&(request.target.port_num, request.target.gid_index))
                    .cloned()
                    .unwrap_or_default(),
            },
            remote: RdmaNicInfo {
                device: request.source_device.clone(),
                port_num: request.endpoint.port_num,
                gid_index: request.endpoint.gid_index,
                ip: gid_ip(&request.endpoint.gid),
                zones: request.source_zones.clone(),
            },
        };

        let socket = self.register_socket(
            queue_pair,
            state,
            &poller,
            &connection_config,
            path,
            device_index,
        )?;
        local_endpoint.connection_cookie = socket.conn_id;
        {
            let mut inbound = self.inbound.lock().unwrap();
            inbound.retain(|conn| conn.strong_count() > 0);
            inbound.push(Arc::downgrade(&socket));
        }
        match self.accept_leases.entry(request.connection_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(AcceptLease {
                    socket: Arc::downgrade(&socket),
                    server_connection_cookie: socket.conn_id,
                    state: AcceptLeaseState::Pending,
                    expires_at: Instant::now()
                        + Duration::from_millis(self.config.connect_lease_ms),
                });
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                socket.set_error();
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    format!("duplicate RDMA connection id {}", request.connection_id),
                ));
            }
        }
        socket.set_accept_lease(request.connection_id);
        self.ensure_accept_lease_sweeper(state);
        self.ensure_maintenance_task(state);
        tracing::debug!(
            local_qp = socket.queue_pair.qp_num(),
            remote_qp = request.endpoint.qp_num,
            "accepted RDMA connection"
        );
        Ok(local_endpoint)
    }

    pub(crate) fn rdma_confirm(&self, control: &ConnectionControl) -> Result<()> {
        match self.accept_leases.entry(control.connection_id) {
            dashmap::mapref::entry::Entry::Vacant(_) => Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "unknown or expired RDMA connection id {}",
                    control.connection_id
                ),
            )),
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !Self::lease_matches_control(entry.get(), control) {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!(
                            "RDMA connection {} identity mismatch",
                            control.connection_id
                        ),
                    ));
                }
                if entry.get().expires_at <= Instant::now() {
                    let (_, expired) = entry.remove_entry();
                    if let Some(socket) = expired.socket.upgrade() {
                        socket.set_error();
                    }
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!("expired RDMA connection id {}", control.connection_id),
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
                        format!("RDMA connection {} already closed", control.connection_id),
                    ));
                }
                entry.get_mut().state =
                    advance_accept_lease(entry.get().state, AcceptLeaseEvent::Confirm);
                entry.get_mut().expires_at =
                    Instant::now() + Duration::from_millis(self.config.connect_lease_ms);
                Ok(())
            }
        }
    }

    pub(crate) fn rdma_abort(&self, control: &ConnectionControl) {
        if let Some((_, lease)) = self
            .accept_leases
            .remove_if(&control.connection_id, |_, lease| {
                Self::lease_matches_control(lease, control)
            })
            && let Some(socket) = lease.socket.upgrade()
        {
            socket.set_error();
        }
    }

    pub(crate) fn rdma_receive_observed(&self, connection_id: u64, socket: &Arc<RdmaSocket>) {
        let weak_socket = Arc::downgrade(socket);
        self.observe_accept_receive(connection_id, &weak_socket);
    }

    pub(super) fn observe_accept_receive(&self, connection_id: u64, socket: &Weak<RdmaSocket>) {
        let dashmap::mapref::entry::Entry::Occupied(mut entry) =
            self.accept_leases.entry(connection_id)
        else {
            return;
        };
        if !entry.get().socket.ptr_eq(socket) {
            return;
        }
        if entry.get().expires_at <= Instant::now() {
            let (_, expired) = entry.remove_entry();
            if let Some(socket) = expired.socket.upgrade() {
                socket.set_error();
            }
            return;
        }
        entry.get_mut().state = advance_accept_lease(entry.get().state, AcceptLeaseEvent::Receive);
    }

    fn lease_matches_control(lease: &AcceptLease, control: &ConnectionControl) -> bool {
        lease.server_connection_cookie == control.server_connection_cookie
    }

    fn ensure_accept_lease_sweeper(&self, state: &Arc<State>) {
        if self.lease_sweeper_started.swap(true, Ordering::Relaxed) {
            return;
        }
        let interval = Duration::from_millis((self.config.connect_lease_ms / 4).clamp(100, 1_000));
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
                    let now = Instant::now();
                    pool.accept_leases.retain(|connection_id, lease| {
                        let Some(socket) = lease.socket.upgrade() else {
                            return false;
                        };
                        if lease.expires_at > now {
                            return true;
                        }
                        if lease.state == AcceptLeaseState::Active {
                            tracing::debug!(
                                connection_id,
                                qp = socket.queue_pair.qp_num(),
                                "RDMA active lease tombstone expired"
                            );
                        } else {
                            tracing::warn!(
                                connection_id,
                                qp = socket.queue_pair.qp_num(),
                                state = ?lease.state,
                                "RDMA accept lease expired"
                            );
                            socket.set_error();
                        }
                        false
                    });
                }
            })
            .is_none()
        {
            self.lease_sweeper_started.store(false, Ordering::Relaxed);
        }
    }
}
