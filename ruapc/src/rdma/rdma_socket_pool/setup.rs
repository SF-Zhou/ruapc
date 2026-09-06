//! Shared RDMA device, queue-pair and socket setup.

use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use ruapc_bufpool::Device as _;
use ruapc_rdma::{
    DeviceInfo, Port, QpConnectionConfig, QueuePair, ibv_mtu, ibv_qp_cap, ibv_qp_init_attr,
    ibv_qp_type,
};

use super::super::path::RdmaPathInfo;
use super::super::{
    DeviceSelection, RdmaConnectionConfig, RdmaConnectionLimits, RdmaDevice, RdmaQpEndpoint,
    RdmaQueuePairConfig, RdmaSocket, RdmaSocketConfig, RegisterConn,
};
use super::{ConnCountGuard, RdmaSocketPool};
use crate::{Buffer, Error, ErrorKind, Result, State};

/// Resources owned by one local endpoint before it is handed to the poller.
/// Both initiator and acceptor use the same negotiation and setup sequence.
/// Kept in a Box across the peer's prepare RPC: shared transport acquisition
/// futures carry only that pointer instead of an entire temporary QP. The box
/// is consumed at registration; established connections own the QP directly.
pub(super) struct LocalConnection {
    queue_pair: QueuePair,
    pub(super) endpoint: RdmaQpEndpoint,
    pub(super) config: RdmaConnectionConfig,
    poller: Arc<super::super::poller::DevicePoller>,
    device_index: usize,
}

impl LocalConnection {
    pub(super) fn connect(&self, pool: &RdmaSocketPool, remote: &RdmaQpEndpoint) -> Result<()> {
        pool.bring_qp_to_rts(
            &self.queue_pair,
            &self.endpoint,
            remote,
            pool.config.connection.pkey_index,
            self.config.traffic_class,
        )
        .map_err(|err| at_stage("connect queue pair", err))
    }

    pub(super) fn register(
        self: Box<Self>,
        pool: &RdmaSocketPool,
        state: &Arc<State>,
        path: RdmaPathInfo,
    ) -> Result<Arc<RdmaSocket>> {
        let Self {
            queue_pair,
            config,
            poller,
            device_index,
            ..
        } = *self;
        pool.register_socket(queue_pair, state, &poller, &config, path, device_index)
            .map_err(|err| at_stage("register socket", err))
    }
}

/// Add setup context without losing the original error kind (notably timeout,
/// overload and the specific verbs failure).
pub(super) fn at_stage(stage: &str, err: Error) -> Error {
    Error::new(err.kind, format!("RDMA {stage}: {}", err.msg))
}

impl RdmaSocketPool {
    pub(super) fn prepare_local_connection(
        &self,
        device_index: usize,
        selection: &DeviceSelection,
        peer_limits: RdmaConnectionLimits,
        traffic_class: u8,
    ) -> Result<Box<LocalConnection>> {
        let device = self.devices.devices().get(device_index).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("local RDMA device index {device_index} is unavailable"),
            )
        })?;
        let config = self
            .resolve_connection_config(device, peer_limits, traffic_class)
            .map_err(|err| at_stage("negotiate limits", err))?;
        let poller = self
            .pollers
            .get_or_start(
                device,
                self.poller_config(),
                self.config.polling.poll_threads_per_device,
            )
            .map_err(|err| at_stage("start completion poller", err))?;
        let queue_pair = self
            .create_queue_pair(device, &config, &poller)
            .map_err(|err| at_stage("create queue pair", err))?;
        let endpoint = self
            .build_endpoint(&queue_pair, device, selection.port_num, selection.gid_index)
            .map_err(|err| at_stage("build local endpoint", err))?;
        endpoint
            .validate()
            .map_err(|err| at_stage("validate local endpoint", err))?;
        tracing::debug!(
            local_qp = endpoint.qp_num,
            ?endpoint,
            ?config,
            "RDMA local endpoint prepared"
        );
        Ok(Box::new(LocalConnection {
            queue_pair,
            endpoint,
            config,
            poller,
            device_index,
        }))
    }

    pub(super) fn find_device_by_name(
        &self,
        selection: &DeviceSelection,
    ) -> Result<(usize, &RdmaDevice)> {
        self.devices
            .devices()
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

    pub(super) fn resolve_connection_config(
        &self,
        device: &RdmaDevice,
        peer: RdmaConnectionLimits,
        traffic_class: u8,
    ) -> Result<RdmaConnectionConfig> {
        let local = self.local_connection_config(device);
        let negotiated = RdmaConnectionLimits::from(local).negotiate(peer)?;
        Ok(RdmaConnectionConfig {
            qp: RdmaQueuePairConfig {
                max_send_wr: negotiated.max_send_wr,
                max_recv_wr: negotiated.max_recv_wr,
                max_send_sge: local.qp.max_send_sge,
                max_recv_sge: local.qp.max_recv_sge,
            },
            recv_queue_len: negotiated.recv_queue_len,
            max_msg_size: negotiated.max_msg_size,
            traffic_class,
        })
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
            recv_queue_len: self
                .config
                .connection
                .recv_queue_len
                .min(self.config.connection.qp.max_recv_wr)
                .min(info.device_attr.max_qp_wr as u32),
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
            .map_err(Error::from)?;
        queue_pair.set_send_signal_interval(
            self.config.connection.send_signal_interval,
            config.qp.max_send_wr,
        );
        Ok(queue_pair)
    }

    /// Constructs an endpoint from a QueuePair and selected local port/GID.
    pub(super) fn build_endpoint(
        &self,
        qp: &QueuePair,
        device: &RdmaDevice,
        port_num: u8,
        gid_index: u8,
    ) -> Result<RdmaQpEndpoint> {
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
        Ok(RdmaQpEndpoint {
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
            .clamp(0, RD_ATOMIC_CEILING);
        cap as u8
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
        local: &RdmaQpEndpoint,
        remote: &RdmaQpEndpoint,
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
        local.validate()?;
        remote.validate()?;
        let rd_atomic = local.rd_atomic_cap.min(remote.rd_atomic_cap);
        qp.connect(&QpConnectionConfig {
            local_port_num: local.port_num,
            local_gid_index: local.gid_index,
            pkey_index,
            link_layer: local.link_layer,
            path_mtu,
            remote_qp_num: remote.qp_num,
            remote_gid: remote.gid,
            remote_lid: remote.lid,
            local_psn: local.psn,
            remote_psn: remote.psn,
            max_rd_atomic: rd_atomic,
            max_dest_rd_atomic: rd_atomic,
            traffic_class,
        })
        .map_err(Error::from)
    }

    fn min_mtu(a: ibv_mtu, b: ibv_mtu) -> ibv_mtu {
        if (a as u32) <= (b as u32) { a } else { b }
    }

    /// Wraps a connected QueuePair, pre-posts receives and registers it with the poller.
    pub(super) fn register_socket(
        &self,
        queue_pair: QueuePair,
        state: &Arc<State>,
        poller: &super::super::poller::DevicePoller,
        config: &RdmaConnectionConfig,
        path: RdmaPathInfo,
        device_index: usize,
    ) -> Result<Arc<RdmaSocket>> {
        let qp_depth = config
            .qp
            .max_send_wr
            .saturating_add(config.qp.max_recv_wr)
            .saturating_mul(2);
        let reservation = poller.reserve(qp_depth)?;

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
            .devices()
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

        let receive_stage = |posted: usize| {
            format!(
                "pre-post receive {}/{} ({} bytes, QP {})",
                posted + 1,
                config.recv_queue_len,
                config.max_msg_size,
                socket.queue_pair.qp_num()
            )
        };
        // Allocate outside the registration barrier. Only posting and inbox
        // publication need to exclude an early completion's routing retry.
        let receive_buffers = (0..config.recv_queue_len as usize)
            .map(|posted| {
                self.buffer_pool
                    .allocate(config.max_msg_size as usize)
                    .map_err(|err| at_stage(&receive_stage(posted), err.into()))
            })
            .collect::<Result<Vec<_>>>()?;
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
            || {
                for (posted, buf) in receive_buffers.into_iter().enumerate() {
                    socket
                        .queue_pair
                        .recv(buf)
                        .map_err(|err| at_stage(&receive_stage(posted), err.into()))?;
                }
                Ok(())
            },
        )?;

        // The shutdown watcher must not retain a failed/rolled-back QP after
        // its poller entry is drained. The poller owns the live socket.
        let weak_socket = Arc::downgrade(&socket);
        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            task_supervisor.stopped().await;
            if let Some(socket) = weak_socket.upgrade() {
                socket.set_error();
            }
        });
        Ok(socket)
    }
}
