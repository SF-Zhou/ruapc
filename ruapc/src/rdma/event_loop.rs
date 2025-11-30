//! RDMA event loop implementation for handling completion queue events and flow control
//!
//! This module provides:
//! - RDMA completion queue event processing and event management
//! - Flow control management with configurable thresholds and limits
//! - Separate handling of send/receive operations with dedicated handlers
//! - Performance statistics tracking for sends, receives and acknowledgments
//! - Reliable delivery through an acknowledgment mechanism with configurable thresholds
//! - Buffer management and work request (WR) processing for RDMA operations

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use ruapc_rdma::{Buffer, verbs};
use tokio::io::{Interest, unix::AsyncFd};

use crate::{Error, ErrorKind, Message, Result, Socket, State};

use super::RdmaSocket;

/// Handler for RDMA receive operations
///
/// Responsible for managing RDMA receive operations including:
/// - Allocating and managing receive buffers
/// - Processing receive completions and work requests (WRs)
/// - Tracking detailed operation statistics (submitted, completed, messages)
/// - Handling both immediate and data messages
/// - Managing acknowledgments for received messages
#[derive(Debug, Default)]
struct RecvHandler {
    /// Number of recv WRs submitted
    submitted: u64,
    /// Number of recv WRs completed
    completed: u64,
    /// Number of received immediate messages
    imm_received: u64,
    /// Number of acknowledged immediate messages
    imm_acked: u64,
    /// Number of received data messages
    data_received: u64,
    /// Number of acknowledged data messages
    data_acked: u64,
    /// Receive buffers for incoming messages
    buffers: Vec<Buffer>,
}

impl RecvHandler {
    fn init_buffers(&mut self, count: usize, socket: &RdmaSocket) -> Result<()> {
        assert!(self.buffers.is_empty());
        self.buffers.reserve(count);

        for wrid in 0..count as u64 {
            let buf = socket.rdmabuf_pool.allocate()?;
            self.buffers.push(buf);
            self.post_recv(wrid, socket)?;
        }
        Ok(())
    }

    fn post_recv(&mut self, wrid: u64, socket: &RdmaSocket) -> Result<()> {
        let buf = &self.buffers[usize::try_from(wrid).unwrap()];
        socket.queue_pair.recv(verbs::WRID::recv(wrid), buf)?;
        self.submitted += 1;
        Ok(())
    }

    fn handle_completion(
        &mut self,
        wc: &verbs::ibv_wc,
        socket: &Arc<RdmaSocket>,
        send: &mut SendHandler,
        state: &Arc<State>,
    ) -> Result<()> {
        self.completed += 1;

        if !wc.succ() {
            socket.queue_pair.set_error();
            return Err(Error::new(
                ErrorKind::RdmaRecvFailed,
                format!(
                    "recv completion error: {:?}, {:?}",
                    wc.status, wc.vendor_err
                ),
            ));
        } else if let Some(ack) = wc.imm() {
            self.imm_received += 1;

            send.update_confirmed(ack);
        } else {
            self.data_received += 1;

            // Handle regular data message
            let mut new_buf = socket.rdmabuf_pool.allocate()?;
            std::mem::swap(
                &mut self.buffers[usize::try_from(wc.wr_id.get_id()).unwrap()],
                &mut new_buf,
            );
            new_buf.set_len(wc.byte_len as usize);

            if let Ok(msg) = Message::parse(new_buf)
                && let Err(e) = state.handle_recv(&Socket::from(socket), msg)
            {
                tracing::error!("Failed to handle message: {}", e);
            }
        }

        self.post_recv(wc.wr_id.get_id(), socket)?;
        Ok(())
    }
}

/// Handler for RDMA send operations
///
/// Responsibilities:
/// - Managing send completions
/// - Tracking data and ACK statistics
/// - Handling acknowledgment messages
#[derive(Debug, Default)]
struct SendHandler {
    /// Number of completed send WRs
    data_completed: u64,
    /// Number of sends confirmed by remote
    data_confirmed: u64,

    /// Number of ACKs submitted
    ack_submitted: u64,
    /// Number of ACKs completed locally
    ack_completed: u64,
    /// Number of ACKs confirmed by remote
    ack_confirmed: u64,
}

impl SendHandler {
    fn handle_completion(&mut self, wc: &verbs::ibv_wc, socket: &RdmaSocket) -> Result<()> {
        if wc.is_send_imm() {
            self.ack_completed += 1;
        } else if wc.is_rdma_read() {
            // Handle RDMA Read completion
            socket.complete_rdma_read(wc.wr_id.get_id(), wc.succ());
        } else {
            self.data_completed += 1;
            socket.send_buffers.remove(&wc.wr_id.get_id());
        }

        if wc.succ() {
            Ok(())
        } else {
            tracing::error!("send completion error: {wc:?}",);
            Err(Error::new(
                ErrorKind::RdmaSendFailed,
                format!("send completion error: {wc:?}",),
            ))
        }
    }

    fn update_confirmed(&mut self, ack: u32) {
        self.data_confirmed += u64::from(ack & 0xFFFF);
        self.ack_confirmed += u64::from(ack >> 16);
    }

    fn submit_ack(
        &mut self,
        socket: &RdmaSocket,
        pending_imm: u32,
        pending_data: u32,
    ) -> Result<()> {
        let mut wr = verbs::ibv_send_wr {
            wr_id: verbs::WRID::send_imm(self.ack_submitted),
            opcode: verbs::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
            send_flags: verbs::ibv_send_flags::IBV_SEND_SIGNALED.0,
            __bindgen_anon_1: verbs::ibv_send_wr__bindgen_ty_1 {
                imm_data: ((pending_imm << 16) + pending_data).to_be(),
            },
            ..Default::default()
        };

        let ret = socket.queue_pair.post_send(&mut wr);
        self.ack_submitted += 1;
        if ret == 0 {
            Ok(())
        } else {
            self.ack_completed += 1;
            self.ack_confirmed += 1;
            tracing::error!(
                "{} submit ack error: {}, {:?}",
                socket.queue_pair.qp_num,
                ret,
                std::io::Error::last_os_error()
            );
            socket.queue_pair.set_error();
            Err(Error::new(
                ErrorKind::RdmaSendFailed,
                format!("failed to post ack: {ret}"),
            ))
        }
    }
}

/// Flow control configuration for RDMA operations
///
/// Controls acknowledgment behavior by setting:
/// - Threshold for triggering acknowledgments
/// - Maximum number of unacknowledged messages
/// - Rate limiting for acknowledgment messages
#[derive(Debug)]
pub struct FlowConfig {
    /// Number of unacknowledged messages before triggering an acknowledgment
    ack_threshold: u32,
    /// Maximum number of unacknowledged messages allowed
    ack_max_limit: u32,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            ack_threshold: 16,
            ack_max_limit: 32,
        }
    }
}

/// RDMA event loop for managing queue pair operations
///
/// Responsibilities:
/// - Processing completion queue events
/// - Managing receive buffers and work requests
/// - Implementing flow control logic
/// - Handling acknowledgments
/// - Maintaining operation statistics
#[derive(Debug)]
pub struct EventLoop {
    socket: Arc<RdmaSocket>,
    state: Arc<State>,
    pending_receiver: tokio::sync::mpsc::Receiver<u64>,
    pending_sends: BTreeSet<u64>,
    unack_cq_events: usize,

    recv: RecvHandler,
    send: SendHandler,
    last_ack_timestamp: Instant,
    flow_config: FlowConfig,
}

impl EventLoop {
    /// Creates a new event loop instance
    ///
    /// # Arguments
    /// * `socket` - RDMA socket with initialized queue pair
    /// * `state` - Shared state for coordination
    ///
    /// # Returns
    /// New `EventLoop` instance configured for the given socket
    pub fn new(
        socket: Arc<RdmaSocket>,
        state: Arc<State>,
        pending_receiver: tokio::sync::mpsc::Receiver<u64>,
    ) -> Self {
        Self {
            socket,
            state,
            pending_receiver,
            pending_sends: BTreeSet::new(),
            unack_cq_events: 0,
            recv: RecvHandler::default(),
            send: SendHandler::default(),
            last_ack_timestamp: Instant::now(),
            flow_config: FlowConfig::default(),
        }
    }

    /// Submits initial receive work requests
    ///
    /// # Arguments
    /// * `count` - Number of receive buffers to allocate and post
    ///
    /// # Returns
    /// * `Result<()>` - Success or error with detailed message
    ///
    /// # Errors
    /// Returns error if buffer allocation or work request posting fails
    pub fn submit_recv_tasks(&mut self, count: usize) -> Result<()> {
        self.recv.init_buffers(count, &self.socket)
    }

    /// Runs the main event processing loop
    ///
    /// Continuously:
    /// 1. Monitors completion queue for events
    /// 2. Processes work completions
    /// 3. Updates flow control state
    /// 4. Handles acknowledgments
    ///
    /// # Returns
    /// * `Result<()>` - Success or error with cause
    pub async fn run(&mut self) -> Result<()> {
        let socket = self.socket.clone();
        let async_fd = AsyncFd::with_interest(socket.queue_pair.notify_fd(), Interest::READABLE)
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;

        let mut wcs_buf = [verbs::ibv_wc::default(); 32];
        let mut pending_sends = Vec::with_capacity(256);

        loop {
            self.socket.queue_pair.req_notify()?;

            loop {
                let wcs = self.socket.queue_pair.poll_cq(&mut wcs_buf)?;
                for wc in wcs.iter() {
                    if wc.is_recv() {
                        if self
                            .recv
                            .handle_completion(wc, &self.socket, &mut self.send, &self.state)
                            .is_err()
                        {
                            self.socket.set_error();
                        }
                    } else if self.send.handle_completion(wc, &self.socket).is_err() {
                        self.socket.set_error();
                    }
                }
                if wcs.len() < wcs_buf.len() {
                    break;
                }
            }

            if let Err(e) = self.update_flow_control() {
                tracing::error!("Flow control update error: {e}");
            }

            if self.ready_to_remove() {
                break;
            }

            tokio::select! {
                guard = async_fd.readable() => {
                    self.unack_cq_events += self.socket.queue_pair.get_cq_events()?;
                    guard.unwrap().clear_ready();
                },
                _ = self.pending_receiver.recv_many(&mut pending_sends, 256) => {
                    self.pending_sends.extend(pending_sends.iter());
                    pending_sends.clear();
                },
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }

        Ok(())
    }

    /// Checks if the event loop is ready to be removed/cleaned up.
    fn ready_to_remove(&self) -> bool {
        self.socket.state.ready_to_remove(self.send.data_completed)
            && self.send.ack_submitted == self.send.ack_completed
            && self.recv.submitted == self.recv.completed
    }

    /// Updates flow control state and sends acknowledgments when necessary.
    fn update_flow_control(&mut self) -> Result<()> {
        if !self.socket.state.is_ok() {
            while let Some(msgid) = self.pending_sends.pop_first() {
                self.socket.send_buffers.remove(&msgid);
                self.send.data_completed += 1;
            }
            return Ok(());
        }

        let completed_sends = std::cmp::min(self.send.data_completed, self.send.data_confirmed);
        let sendable_bound = self.socket.state.update_send_finished(completed_sends);
        while let Some(&msgid) = self.pending_sends.first()
            && msgid < sendable_bound
        {
            self.socket.queue_pair.send(
                verbs::WRID::send_data(msgid),
                &self.socket.send_buffers.get(&msgid).unwrap(),
            )?;
            self.pending_sends.pop_first();
        }

        let ack_done = std::cmp::min(self.send.ack_completed, self.send.ack_confirmed);
        if self.send.ack_submitted >= ack_done + u64::from(self.flow_config.ack_max_limit) {
            return Ok(());
        }

        let pending_data = u32::try_from(self.recv.data_received - self.recv.data_acked).unwrap();
        let pending_imm = u32::try_from(self.recv.imm_received - self.recv.imm_acked).unwrap();

        if pending_data >= self.flow_config.ack_threshold
            || pending_imm >= self.flow_config.ack_max_limit / 2
            || self.last_ack_timestamp.elapsed().as_secs() >= 5
        {
            self.send
                .submit_ack(&self.socket, pending_imm, pending_data)?;
            self.recv.data_acked = self.recv.data_received;
            self.recv.imm_acked = self.recv.imm_received;
            self.last_ack_timestamp = Instant::now();
        }

        Ok(())
    }
}

impl Drop for EventLoop {
    fn drop(&mut self) {
        self.socket.queue_pair.ack_cq_events(self.unack_cq_events);
    }
}
