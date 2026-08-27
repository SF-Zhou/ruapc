//! Shared RDMA device, queue-pair and socket setup.

use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use ruapc_bufpool::Device as _;
use ruapc_rdma::{DeviceInfo, Port, QueuePair, ibv_mtu, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type};

use super::super::path::RdmaPathInfo;
use super::super::{
    DeviceSelection, Endpoint, RdmaConnectionConfig, RdmaDevice, RdmaQueuePairConfig, RdmaSocket,
    RdmaSocketConfig, RegisterConn,
};
use super::{ConnCountGuard, RdmaSocketPool};
use crate::{Buffer, Error, ErrorKind, Result, State};

impl RdmaSocketPool {
    pub(super) fn find_device_by_name(
        &self,
        selection: &DeviceSelection,
    ) -> Result<(usize, &RdmaDevice)> {
        self.devices
            .rdma_devices()
            .iter()
            .enumerate()
            .find(|(_, device)| device.info().name.as_str() == selection.device_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA device {} not found", selection.device_name),
                )
            })
    }

    pub(super) fn negotiate_connection_config(
        &self,
        local_device: &RdmaDevice,
        remote: &RdmaConnectionConfig,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(local_device);
        let remote = *remote;
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: local.qp.max_send_wr.min(remote.qp.max_recv_wr),
                max_recv_wr: local.qp.max_recv_wr.min(remote.qp.max_send_wr),
                // Scatter/gather lists are local WQE properties.
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
            },
            cq_len: local.cq_len.min(remote.cq_len),
            recv_queue_len: local.recv_queue_len.min(remote.recv_queue_len),
            max_msg_size: local.max_msg_size.min(remote.max_msg_size),
            traffic_class: self.config.connection.traffic_class,
        }
    }

    pub(super) fn clamp_connection_config(
        &self,
        device: &RdmaDevice,
        requested: RdmaConnectionConfig,
    ) -> RdmaConnectionConfig {
        let local = self.local_connection_config(device);
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: requested.qp.max_send_wr.min(local.qp.max_send_wr),
                max_recv_wr: requested.qp.max_recv_wr.min(local.qp.max_recv_wr),
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
            },
            cq_len: requested.cq_len.min(local.cq_len),
            recv_queue_len: requested.recv_queue_len.min(local.recv_queue_len),
            max_msg_size: requested.max_msg_size.min(local.max_msg_size),
            traffic_class: requested.traffic_class,
        }
    }

    fn local_connection_config(&self, device: &RdmaDevice) -> RdmaConnectionConfig {
        let info = device.info();
        RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: self
                    .config
                    .connection
                    .qp
                    .max_send_wr
                    .min(info.device_attr.max_qp_wr as u32),
                max_recv_wr: self
                    .config
                    .connection
                    .qp
                    .max_recv_wr
                    .min(info.device_attr.max_qp_wr as u32),
                max_send_sge: self
                    .config
                    .connection
                    .qp
                    .max_send_sge
                    .min(info.device_attr.max_sge as u32),
                max_recv_sge: self
                    .config
                    .connection
                    .qp
                    .max_recv_sge
                    .min(info.device_attr.max_sge as u32),
            },
            cq_len: self
                .config
                .connection
                .cq_len
                .min(info.device_attr.max_cqe as u32),
            recv_queue_len: self.config.connection.recv_queue_len,
            max_msg_size: self.config.connection.max_msg_size,
            traffic_class: self.config.connection.traffic_class,
        }
    }

    /// Creates a QueuePair attached to the device's shared completion queue.
    pub(super) fn create_queue_pair(
        &self,
        device: &RdmaDevice,
        config: &RdmaConnectionConfig,
        poller: &super::super::poller::DevicePoller,
    ) -> Result<QueuePair> {
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
        let mut queue_pair = QueuePair::create(device.pd(), cq, cq, &mut init_attr, device.index())
            .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))?;
        queue_pair.set_send_signal_interval(
            self.config.connection.send_signal_interval,
            config.qp.max_send_wr,
        );
        Ok(queue_pair)
    }

    /// Constructs an Endpoint from a QueuePair and selected local port/GID.
    pub(super) fn build_endpoint(
        &self,
        qp: &QueuePair,
        device: &RdmaDevice,
        port_num: u8,
        gid_index: u8,
    ) -> Result<Endpoint> {
        let info = device.info();
        let port = Self::find_port(&info, port_num)?;
        if !port.is_usable() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("RDMA port {}:{} is not active", info.name, port_num),
            ));
        }
        let gid = port.find_gid(gid_index).map(|gid| gid.gid);
        if port.port_attr.link_layer.is_ethernet() && gid.is_none() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA port {}:{} does not have GID index {}",
                    info.name, port_num, gid_index
                ),
            ));
        }
        Ok(Endpoint {
            connection_cookie: 0,
            qp_num: qp.qp_num(),
            port_num,
            gid_index,
            lid: port.port_attr.lid,
            gid: gid.unwrap_or_default(),
            link_layer: port.port_attr.link_layer,
            active_mtu: port.port_attr.active_mtu,
            psn: Self::random_psn(qp.qp_num()),
            rd_atomic_cap: Self::rd_atomic_cap(&info),
        })
    }

    fn rd_atomic_cap(info: &DeviceInfo) -> u8 {
        const RD_ATOMIC_CEILING: i32 = 16;
        let cap = info
            .device_attr
            .max_qp_rd_atom
            .min(info.device_attr.max_qp_init_rd_atom)
            .clamp(1, RD_ATOMIC_CEILING);
        u8::try_from(cap).unwrap_or(1)
    }

    fn random_psn(qp_num: u32) -> u32 {
        use std::hash::BuildHasher as _;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.subsec_nanos())
            .unwrap_or(0);
        (foldhash::fast::RandomState::default().hash_one((qp_num, nanos)) as u32) & 0xFF_FFFF
    }

    pub(super) fn find_port(info: &DeviceInfo, port_num: u8) -> Result<&Port> {
        info.ports
            .iter()
            .find(|port| port.port_num == port_num)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("RDMA port {}:{} not found", info.name, port_num),
                )
            })
    }

    pub(super) fn bring_qp_to_rts(
        &self,
        qp: &QueuePair,
        local: &Endpoint,
        remote: &Endpoint,
        pkey_index: u16,
        traffic_class: u8,
    ) -> Result<()> {
        if local.link_layer != remote.link_layer {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "RDMA link layer mismatch: local {} remote {}",
                    local.link_layer, remote.link_layer
                ),
            ));
        }
        let path_mtu = Self::min_mtu(local.active_mtu, remote.active_mtu);
        let rd_atomic = local.rd_atomic_cap.min(remote.rd_atomic_cap).max(1);
        qp.connect(
            local.port_num,
            local.gid_index,
            pkey_index,
            local.link_layer,
            path_mtu,
            remote.qp_num,
            remote.gid,
            remote.lid,
            local.psn,
            remote.psn,
            rd_atomic,
            rd_atomic,
            traffic_class,
        )
        .map_err(|e| Error::new(ErrorKind::RdmaSendFailed, e.to_string()))
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
    }

    /// Wraps a connected QueuePair, pre-posts receives and registers it with the poller.
    pub(super) fn register_socket(
        &self,
        mut queue_pair: QueuePair,
        state: &Arc<State>,
        poller: &super::super::poller::DevicePoller,
        config: &RdmaConnectionConfig,
        path: RdmaPathInfo,
        device_index: usize,
    ) -> Result<Arc<RdmaSocket>> {
        let qp_depth = (config.qp.max_send_wr + config.qp.max_recv_wr).saturating_mul(2);
        let reservation = poller.reserve(qp_depth)?;
        queue_pair.set_wr_tag(reservation.tag());

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
                 ring total, or lower rdma.connection.recv_queue_len / \
                 rdma.connection.max_msg_size / rdma.peers.connections_per_peer",
                config.recv_queue_len,
                config.max_msg_size,
            );
        }

        let send_window = (config.recv_queue_len / 2).max(1);
        let (tx, rx) = tokio::sync::mpsc::channel::<Buffer>(1024);
        let read_timeout = (self.config.remote_memory.read_timeout_ms > 0)
            .then(|| Duration::from_millis(self.config.remote_memory.read_timeout_ms));
        let read_permits = self
            .read_permits
            .get(device_index)
            .cloned()
            .unwrap_or_else(|| {
                Arc::new(tokio::sync::Semaphore::new(
                    self.config.remote_memory.max_inflight_read_wrs as usize,
                ))
            });
        let bandwidth_limiter = self
            .devices
            .rdma_devices()
            .get(device_index)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidArgument,
                    format!("invalid local RDMA device index {device_index}"),
                )
            })?
            .bandwidth_limiter(path.local.port_num)?;
        let socket = Arc::new(RdmaSocket::new(
            queue_pair,
            self.buffer_pool.clone(),
            tx,
            poller.waker(),
            RdmaSocketConfig {
                max_msg_size: config.max_msg_size as usize,
                send_window,
                path,
                read_timeout,
                read_permits,
                bandwidth_limiter,
                sq_read_cap: (config.qp.max_send_wr / 2).max(1),
            },
        ));

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
                msg_aggregation: self.config.connection.msg_aggregation,
                supervisor_guard: self.task_supervisor.start_async_task(),
                ring_reservation,
                conn_count_guard: ConnCountGuard::acquire(&self.conn_counts, device_index),
            },
        )?;

        let socket_clone = socket.clone();
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            task_supervisor.stopped().await;
            socket_clone.set_error();
        });
        Ok(socket)
    }
}
