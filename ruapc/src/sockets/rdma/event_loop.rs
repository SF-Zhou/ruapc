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

use ruapc_rdma_sys::{CompChannel, CompletionQueue, WRType, ibv_send_flags, ibv_wc};
use tokio::io::{Interest, unix::AsyncFd};

use crate::{Error, ErrorKind, Message, Result, Socket, State, memory::Buffer};

use super::{RdmaBufferRef, RdmaSocket};

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
}

impl RecvHandler {
    fn init_buffers(&mut self, count: usize, socket: &RdmaSocket) -> Result<()> {
        for _ in 0..count {
            let buf = socket.rdmabuf_pool.allocate()?;
            self.post_recv(buf, socket)?;
        }
        Ok(())
    }

    fn post_recv(&mut self, buf: Buffer, socket: &RdmaSocket) -> Result<()> {
        let buf_ref = socket.make_buffer_ref(buf).ok_or_else(|| {
            Error::new(
                ErrorKind::RdmaRecvFailed,
                "buffer not registered on QP device".into(),
            )
        })?;
        socket
            .queue_pair
            .recv(buf_ref)
            .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))?;
        self.submitted += 1;
        Ok(())
    }

    fn handle_completion(
        &mut self,
        wc: &ibv_wc,
        buffer: Option<RdmaBufferRef>,
        socket: &Arc<RdmaSocket>,
        send: &mut SendHandler,
        state: &Arc<State>,
    ) -> Result<()> {
        self.completed += 1;

        if !wc.succ() {
            self.set_error(socket);
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
            if let Some(buf_ref) = buffer {
                let mut buf = buf_ref.into_buffer();
                buf.set_len(wc.byte_len as usize);

                if let Ok(msg) = Message::parse(buf)
                    && let Err(e) = state.handle_recv(&Socket::from(socket), msg)
                {
                    tracing::error!("Failed to handle message: {}", e);
                }
            }
        }

        // Post a new recv buffer
        let new_buf = socket.rdmabuf_pool.allocate()?;
        self.post_recv(new_buf, socket)?;
        Ok(())
    }

    fn set_error(&self, socket: &Arc<RdmaSocket>) {
        socket.set_error();
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
    fn handle_completion(
        &mut self,
        wc: &ibv_wc,
        buffer: Option<RdmaBufferRef>,
        socket: &RdmaSocket,
    ) -> Result<()> {
        // Check if this is an RDMA one-sided operation completion.
        // If so, return the buffer (whose ownership was held by the QP) back
        // to the caller through the oneshot channel.
        if let Some((_, sender)) = socket.rdma_completions.remove(&wc.wr_id) {
            let _ = sender.send((wc.status, buffer));
            return Ok(());
        }

        match wc.wr_id.get_type() {
            WRType::SendImm => {
                self.ack_completed += 1;
            }
            _ => {
                self.data_completed += 1;
            }
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
        let imm_data = (pending_imm << 16) + pending_data;

        let ret = socket
            .queue_pair
            .send_imm_only(imm_data, ibv_send_flags::IBV_SEND_SIGNALED);
        self.ack_submitted += 1;
        match ret {
            Ok(()) => Ok(()),
            Err(err) => {
                self.ack_completed += 1;
                self.ack_confirmed += 1;
                tracing::error!("submit ack error: {err}",);
                socket.set_error();
                Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("failed to post ack: {err}"),
                ))
            }
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
///
/// # Drop order
///
/// RDMA resources must be destroyed in the correct order:
///   1. Ack CQ events (in the custom `Drop` impl)
///   2. Drop `recv` / `send` handlers — releases Buffers back to the pool
///   3. Drop `socket` — may be the last `Arc<RdmaSocket>`, destroying the QP
///
/// Rust drops fields in **declaration order**, so `recv` and `send` are
/// intentionally placed before `socket`.
#[derive(Debug)]
pub struct EventLoop {
    // --- handlers first: release Buffers before the QP is destroyed ---
    recv: RecvHandler,
    send: SendHandler,
    last_ack_timestamp: Instant,
    flow_config: FlowConfig,
    unack_cq_events: u32,

    // --- completion infrastructure ---
    comp_channel: Arc<CompChannel>,
    cq: Arc<CompletionQueue>,

    // --- then the socket (QP) ---
    socket: Arc<RdmaSocket>,
    state: Arc<State>,
    pending_receiver: tokio::sync::mpsc::Receiver<u64>,
    pending_sends: BTreeSet<u64>,
}

impl EventLoop {
    /// Creates a new event loop instance
    ///
    /// # Arguments
    /// * `socket` - RDMA socket with initialized queue pair
    /// * `state` - Shared state for coordination
    /// * `comp_channel` - Completion channel for event notifications
    /// * `cq` - Completion queue shared between send and recv
    ///
    /// # Returns
    /// New `EventLoop` instance configured for the given socket
    pub fn new(
        socket: Arc<RdmaSocket>,
        state: Arc<State>,
        comp_channel: Arc<CompChannel>,
        cq: Arc<CompletionQueue>,
        pending_receiver: tokio::sync::mpsc::Receiver<u64>,
    ) -> Self {
        Self {
            recv: RecvHandler::default(),
            send: SendHandler::default(),
            last_ack_timestamp: Instant::now(),
            flow_config: FlowConfig::default(),
            unack_cq_events: 0,
            comp_channel,
            cq,
            socket,
            state,
            pending_receiver,
            pending_sends: BTreeSet::new(),
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
    #[allow(unsafe_code)]
    pub async fn run(&mut self) -> Result<()> {
        // Extract the raw fd to avoid borrowing `self` through BorrowedFd.
        let comp_channel = self.comp_channel.clone();
        let async_fd = AsyncFd::with_interest(comp_channel.fd(), Interest::READABLE)
            .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))?;

        let mut wcs_buf = [ibv_wc::default(); 32];
        let mut pending_sends = Vec::with_capacity(256);

        loop {
            self.cq
                .req_notify(false)
                .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))?;

            loop {
                let n = self
                    .cq
                    .poll(&mut wcs_buf)
                    .map_err(|e| Error::new(ErrorKind::RdmaRecvFailed, e.to_string()))?;
                let wcs = &wcs_buf[..n];

                for wc in wcs.iter() {
                    // Retrieve the buffer from the QP's internal wrs map.
                    let buffer = self.socket.queue_pair.take_buffer(&wc.wr_id);

                    if wc.is_recv() {
                        if self
                            .recv
                            .handle_completion(
                                wc,
                                buffer,
                                &self.socket,
                                &mut self.send,
                                &self.state,
                            )
                            .is_err()
                        {
                            self.socket.set_error();
                        }
                    } else if self
                        .send
                        .handle_completion(wc, buffer, &self.socket)
                        .is_err()
                    {
                        self.socket.set_error();
                    }
                }
                if n < wcs_buf.len() {
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
                    match guard {
                        Ok(mut g) => {
                            match self.comp_channel.get_event() {
                                Ok(_) => self.unack_cq_events += 1,
                                Err(e) => {
                                    // EAGAIN means no event ready yet, that's fine
                                    if e.kind != ruapc_rdma_sys::ErrorKind::IBGetCompQueueEventFail {
                                        tracing::error!("get_cq_event error: {e}");
                                    }
                                }
                            }
                            g.clear_ready();
                        }
                        Err(e) => {
                            tracing::error!("async_fd readable error: {e}");
                            self.socket.set_error();
                            break;
                        }
                    }
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
                self.socket.pending_buffers.remove(&msgid);
                self.send.data_completed += 1;
            }
            return Ok(());
        }

        let completed_sends = std::cmp::min(self.send.data_completed, self.send.data_confirmed);
        let sendable_bound = self.socket.state.update_send_finished(completed_sends);
        while let Some(&msgid) = self.pending_sends.first()
            && msgid < sendable_bound
        {
            self.pending_sends.pop_first();
            // Retrieve the stored buffer and post it to the QP.
            if let Some((_, buf_ref)) = self.socket.pending_buffers.remove(&msgid)
                && let Err(e) = self
                    .socket
                    .queue_pair
                    .send(buf_ref, ibv_send_flags::IBV_SEND_SIGNALED)
            {
                tracing::error!("failed to send pending buffer: {e}");
                self.socket.set_error();
                return Err(Error::new(
                    ErrorKind::RdmaSendFailed,
                    format!("failed to send pending buffer: {e}"),
                ));
            }
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
        if self.unack_cq_events > 0 {
            self.cq.ack_events(self.unack_cq_events);
        }
    }
}
