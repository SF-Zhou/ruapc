use crate::{CompQueue, Device, ErrorKind, Result, verbs};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{ffi::c_int, ops::Deref, os::fd::BorrowedFd, sync::Arc};

/// RDMA connection endpoint information.
///
/// Contains the necessary information to establish an RDMA connection
/// between two queue pairs, including queue pair number, local ID, and GID.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
pub struct Endpoint {
    /// Queue pair number.
    pub qp_num: u32,
    /// Local Identifier for InfiniBand routing.
    pub lid: u16,
    /// Global Identifier for RoCE routing.
    pub gid: verbs::ibv_gid,
}

/// Raw queue pair wrapper with automatic cleanup.
///
/// This type wraps a raw InfiniBand queue pair pointer
/// and ensures proper cleanup on drop.
struct RawQueuePair(*mut verbs::ibv_qp);
impl Drop for RawQueuePair {
    fn drop(&mut self) {
        let _ = unsafe { verbs::ibv_destroy_qp(self.0) };
    }
}
unsafe impl Send for RawQueuePair {}
unsafe impl Sync for RawQueuePair {}

/// RDMA queue pair for reliable connected communication.
///
/// A queue pair consists of a send queue and a receive queue, which are
/// used to send and receive messages. This implementation provides a
/// reliable connection (RC) queue pair type with associated completion
/// queue for operation status.
///
/// # Examples
///
/// ```rust,no_run
/// # use ruapc_rdma::{QueuePair, Device, verbs};
/// # use std::sync::Arc;
/// # fn example(device: &Arc<Device>) -> Result<(), Box<dyn std::error::Error>> {
/// let cap = verbs::ibv_qp_cap {
///     max_send_wr: 128,
///     max_recv_wr: 128,
///     max_send_sge: 1,
///     max_recv_sge: 1,
///     max_inline_data: 0,
/// };
/// let qp = QueuePair::create(device, cap)?;
/// # Ok(())
/// # }
/// ```
pub struct QueuePair {
    queue_pair: RawQueuePair,
    comp_queue: CompQueue,
    /// The RDMA device this queue pair belongs to.
    pub device: Arc<Device>,
}

impl QueuePair {
    pub fn create(device: &Arc<Device>, cap: verbs::ibv_qp_cap) -> Result<Self> {
        let max_cqe = cap.max_send_wr + cap.max_recv_wr;
        let comp_queue = CompQueue::create(device, max_cqe)?;
        let mut attr = verbs::ibv_qp_init_attr {
            qp_context: std::ptr::null_mut(),
            send_cq: comp_queue.comp_queue_ptr(),
            recv_cq: comp_queue.comp_queue_ptr(),
            srq: std::ptr::null_mut(),
            cap,
            qp_type: verbs::ibv_qp_type::IBV_QPT_RC,
            sq_sig_all: 0,
        };
        let ptr = unsafe { verbs::ibv_create_qp(device.pd_ptr(), &mut attr) };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateQueuePairFail.with_errno());
        }
        Ok(Self {
            queue_pair: RawQueuePair(ptr),
            comp_queue,
            device: device.clone(),
        })
    }

    pub fn init(&self, port_num: u8, pkey_index: u16) -> Result<()> {
        let mut attr = verbs::ibv_qp_attr {
            qp_state: verbs::ibv_qp_state::IBV_QPS_INIT,
            pkey_index,
            port_num,
            qp_access_flags: verbs::ACCESS_FLAGS,
            ..Default::default()
        };

        const MASK: verbs::ibv_qp_attr_mask = verbs::ibv_qp_attr_mask(
            verbs::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX.0
                | verbs::ibv_qp_attr_mask::IBV_QP_STATE.0
                | verbs::ibv_qp_attr_mask::IBV_QP_PORT.0
                | verbs::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS.0,
        );

        self.modify_qp(&mut attr, MASK)
    }

    pub fn endpoint(&self) -> Endpoint {
        Endpoint {
            qp_num: self.qp_num,
            lid: 0,
            gid: self.device.info().ports[0].gids[1].gid,
        }
    }

    pub fn connect(&self, endpoint: &Endpoint) -> Result<()> {
        self.init(1, 0)?;
        self.ready_to_recv(endpoint)?;
        self.ready_to_send()?;
        Ok(())
    }

    pub fn recv_raw(&self, wr_id: verbs::WRID, addr: u64, length: u32, lkey: u32) -> Result<()> {
        let mut recv_sge = verbs::ibv_sge { addr, length, lkey };
        let mut recv_wr = verbs::ibv_recv_wr {
            wr_id,
            sg_list: &mut recv_sge as *mut _,
            num_sge: 1,
            next: std::ptr::null_mut(),
        };

        match self.post_recv(&mut recv_wr) {
            0 => Ok(()),
            _ => Err(ErrorKind::IBPostRecvFailed.with_errno()),
        }
    }

    pub fn send_raw(&self, wr_id: verbs::WRID, addr: u64, length: u32, lkey: u32) -> Result<()> {
        let mut send_sge = verbs::ibv_sge { addr, length, lkey };
        let mut send_wr = verbs::ibv_send_wr {
            wr_id,
            sg_list: &mut send_sge as *mut _,
            num_sge: 1,
            opcode: verbs::ibv_wr_opcode::IBV_WR_SEND,
            send_flags: verbs::ibv_send_flags::IBV_SEND_SIGNALED.0,
            ..Default::default()
        };

        match self.post_send(&mut send_wr) {
            0 => Ok(()),
            _ => Err(ErrorKind::IBPostSendFailed.with_errno()),
        }
    }

    pub fn rdma_read_raw(
        &self,
        wr_id: verbs::WRID,
        local_addr: u64,
        local_len: u32,
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<()> {
        let mut send_sge = verbs::ibv_sge {
            addr: local_addr,
            length: local_len,
            lkey: local_lkey,
        };
        let mut send_wr = verbs::ibv_send_wr {
            wr_id,
            sg_list: &mut send_sge as *mut _,
            num_sge: 1,
            opcode: verbs::ibv_wr_opcode::IBV_WR_RDMA_READ,
            send_flags: verbs::ibv_send_flags::IBV_SEND_SIGNALED.0,
            ..Default::default()
        };
        send_wr.wr.rdma.remote_addr = remote_addr;
        send_wr.wr.rdma.rkey = remote_rkey;

        match self.post_send(&mut send_wr) {
            0 => Ok(()),
            _ => Err(ErrorKind::IBPostSendFailed.with_errno()),
        }
    }

    pub fn rdma_write_raw(
        &self,
        wr_id: verbs::WRID,
        local_addr: u64,
        local_len: u32,
        local_lkey: u32,
        remote_addr: u64,
        remote_rkey: u32,
    ) -> Result<()> {
        let mut send_sge = verbs::ibv_sge {
            addr: local_addr,
            length: local_len,
            lkey: local_lkey,
        };
        let mut send_wr = verbs::ibv_send_wr {
            wr_id,
            sg_list: &mut send_sge as *mut _,
            num_sge: 1,
            opcode: verbs::ibv_wr_opcode::IBV_WR_RDMA_WRITE,
            send_flags: verbs::ibv_send_flags::IBV_SEND_SIGNALED.0,
            ..Default::default()
        };
        send_wr.wr.rdma.remote_addr = remote_addr;
        send_wr.wr.rdma.rkey = remote_rkey;

        match self.post_send(&mut send_wr) {
            0 => Ok(()),
            _ => Err(ErrorKind::IBPostSendFailed.with_errno()),
        }
    }

    pub fn ready_to_recv(&self, remote: &Endpoint) -> Result<()> {
        let mut attr = verbs::ibv_qp_attr {
            qp_state: verbs::ibv_qp_state::IBV_QPS_RTR,
            path_mtu: verbs::ibv_mtu::IBV_MTU_512,
            dest_qp_num: remote.qp_num,
            rq_psn: 0,
            max_dest_rd_atomic: 1,
            min_rnr_timer: 0x12,
            ah_attr: verbs::ibv_ah_attr {
                grh: verbs::ibv_global_route {
                    dgid: remote.gid,
                    flow_label: 0,
                    sgid_index: 1,
                    hop_limit: 0xff,
                    traffic_class: 0,
                },
                dlid: remote.lid,
                sl: 0,
                src_path_bits: 0,
                static_rate: 0,
                is_global: 1,
                port_num: 1,
            },
            ..Default::default()
        };

        const MASK: verbs::ibv_qp_attr_mask = verbs::ibv_qp_attr_mask(
            verbs::ibv_qp_attr_mask::IBV_QP_STATE.0
                | verbs::ibv_qp_attr_mask::IBV_QP_AV.0
                | verbs::ibv_qp_attr_mask::IBV_QP_PATH_MTU.0
                | verbs::ibv_qp_attr_mask::IBV_QP_DEST_QPN.0
                | verbs::ibv_qp_attr_mask::IBV_QP_RQ_PSN.0
                | verbs::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC.0
                | verbs::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER.0,
        );

        self.modify_qp(&mut attr, MASK)
    }

    pub fn ready_to_send(&self) -> Result<()> {
        let mut attr = verbs::ibv_qp_attr {
            qp_state: verbs::ibv_qp_state::IBV_QPS_RTS,
            timeout: 0x12,
            retry_cnt: 6,
            rnr_retry: 6,
            sq_psn: 0,
            max_rd_atomic: 1,
            ..Default::default()
        };

        const MASK: verbs::ibv_qp_attr_mask = verbs::ibv_qp_attr_mask(
            verbs::ibv_qp_attr_mask::IBV_QP_STATE.0
                | verbs::ibv_qp_attr_mask::IBV_QP_TIMEOUT.0
                | verbs::ibv_qp_attr_mask::IBV_QP_RETRY_CNT.0
                | verbs::ibv_qp_attr_mask::IBV_QP_RNR_RETRY.0
                | verbs::ibv_qp_attr_mask::IBV_QP_SQ_PSN.0
                | verbs::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC.0,
        );

        self.modify_qp(&mut attr, MASK)
    }

    pub fn set_error(&self) {
        let mut attr = verbs::ibv_qp_attr {
            qp_state: verbs::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };

        const MASK: verbs::ibv_qp_attr_mask = verbs::ibv_qp_attr_mask::IBV_QP_STATE;

        // assuming this operation succeeds.
        self.modify_qp(&mut attr, MASK).unwrap()
    }

    pub fn post_send(&self, wr: &mut verbs::ibv_send_wr) -> c_int {
        let mut bad_wr = std::ptr::null_mut();
        unsafe { verbs::ibv_post_send(self.queue_pair.0, wr, &mut bad_wr) }
    }

    pub fn post_recv(&self, wr: &mut verbs::ibv_recv_wr) -> c_int {
        let mut bad_wr = std::ptr::null_mut();
        unsafe { verbs::ibv_post_recv(self.queue_pair.0, wr, &mut bad_wr) }
    }

    pub fn notify_fd<'a>(&'a self) -> BorrowedFd<'a> {
        self.comp_queue.comp_channel.notify_fd()
    }

    pub fn req_notify(&self) -> Result<()> {
        self.comp_queue.req_notify()
    }

    pub fn poll_cq<'a>(&self, wcs: &'a mut [verbs::ibv_wc]) -> Result<&'a mut [verbs::ibv_wc]> {
        self.comp_queue.poll_cq(wcs)
    }

    pub fn get_cq_events(&self) -> Result<usize> {
        self.comp_queue.get_cq_events()
    }

    pub fn ack_cq_events(&self, nevents: usize) {
        self.comp_queue.ack_cq_events(nevents)
    }

    fn modify_qp(
        &self,
        attr: &mut verbs::ibv_qp_attr,
        mask: verbs::ibv_qp_attr_mask,
    ) -> Result<()> {
        let ret = unsafe { verbs::ibv_modify_qp(self.queue_pair.0, attr, mask.0 as _) };
        if ret == 0_i32 {
            Ok(())
        } else {
            Err(ErrorKind::IBModifyQueuePairFail.with_errno())
        }
    }
}

impl Deref for QueuePair {
    type Target = verbs::ibv_qp;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.queue_pair.0 }
    }
}

impl std::fmt::Debug for QueuePair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePair")
            .field("handle", &self.handle)
            .field("qp_num", &self.qp_num)
            .field("state", &self.state)
            .field("qp_type", &self.qp_type)
            .field("events_completiond", &self.events_completed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::{RegisteredBuffer, verbs::WRID};

    use super::*;
    use crate::Devices;

    #[test]
    fn test_queue_pair_create() {
        let devices = Devices::availables().unwrap();
        let cap = verbs::ibv_qp_cap {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        };
        let queue_pair = QueuePair::create(&devices[0], cap).unwrap();
        println!("{:#?}", queue_pair);

        queue_pair.init(1, 0).unwrap();
        queue_pair.set_error();
    }

    #[test]
    fn test_queue_pair_send_recv() {
        // 1. list all available devices.
        let devices = Devices::availables().unwrap();

        // 2. create two queue pairs.
        let cap = verbs::ibv_qp_cap {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        };

        let queue_pair_a = QueuePair::create(&devices[0], cap).unwrap();
        let queue_pair_b = QueuePair::create(&devices[0], cap).unwrap();

        // 3. init all queue pairs.
        queue_pair_a.connect(&queue_pair_b.endpoint()).unwrap();
        queue_pair_b.connect(&queue_pair_a.endpoint()).unwrap();

        // 4. post recv wr.
        const LEN: usize = 1 << 20;
        let recv_buffer = RegisteredBuffer::create(&devices, LEN).unwrap();
        let recv_lkey = recv_buffer.lkey(0);
        let recv_ptr = recv_buffer.as_ptr();
        let recv_slice = &*recv_buffer;
        queue_pair_b
            .recv_raw(WRID::recv(1), recv_ptr as u64, LEN as u32, recv_lkey)
            .unwrap();

        // 5. try to poll cq.
        let mut wcs_b = vec![verbs::ibv_wc::default(); 128];
        assert!(queue_pair_b.poll_cq(&mut wcs_b).unwrap().is_empty());

        // 6. post send wr.
        let send_buffer = RegisteredBuffer::create(&devices, LEN).unwrap();
        let send_lkey = send_buffer.lkey(0);
        let send_ptr = send_buffer.as_ptr();
        assert_ne!(recv_ptr, send_ptr);
        // Fill send buffer with test data.
        unsafe {
            std::ptr::write_bytes(send_ptr as *mut u8, 1u8, LEN);
        }
        let send_slice = &*send_buffer;
        let send_len = LEN;
        queue_pair_a
            .send_raw(
                WRID::send_data(2),
                send_ptr as u64,
                send_len as u32,
                send_lkey,
            )
            .unwrap();

        // 7. poll cq.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut wcs_a = vec![verbs::ibv_wc::default(); 128];
        let comp_a = queue_pair_a.poll_cq(&mut wcs_a).unwrap();
        assert_eq!(comp_a.len(), 1);
        assert_eq!(comp_a[0].wr_id, WRID::send_data(2));
        assert_eq!(comp_a[0].qp_num, queue_pair_a.qp_num);
        assert_eq!(comp_a[0].status, verbs::ibv_wc_status::IBV_WC_SUCCESS);

        let comp_b = queue_pair_b.poll_cq(&mut wcs_b).unwrap();
        assert_eq!(comp_b.len(), 1);
        assert_eq!(comp_b[0].wr_id, WRID::recv(1));
        assert_eq!(comp_b[0].qp_num, queue_pair_b.qp_num);
        assert_eq!(comp_b[0].status, verbs::ibv_wc_status::IBV_WC_SUCCESS);
        assert_eq!(comp_b[0].byte_len, send_len as u32);
        assert!(&recv_slice[..send_len] == send_slice);
    }

    #[test]
    fn test_rdma_read() {
        let devices = Devices::availables().unwrap();

        let cap = verbs::ibv_qp_cap {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        };

        let qp_a = QueuePair::create(&devices[0], cap).unwrap();
        let qp_b = QueuePair::create(&devices[0], cap).unwrap();
        qp_a.connect(&qp_b.endpoint()).unwrap();
        qp_b.connect(&qp_a.endpoint()).unwrap();

        const LEN: usize = 4096;

        // A side: register memory and fill with test data.
        let buf_a = RegisteredBuffer::create(&devices, LEN).unwrap();
        let test_data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
        unsafe {
            std::ptr::copy_nonoverlapping(test_data.as_ptr(), buf_a.as_ptr() as *mut u8, LEN);
        }
        let remote_addr = buf_a.as_ptr() as u64;
        let rkey_a = buf_a.rkey(0);

        // B side: allocate empty buffer, issue RDMA Read from A.
        let buf_b = RegisteredBuffer::create(&devices, LEN).unwrap();
        let lkey_b = buf_b.lkey(0);
        qp_b.rdma_read_raw(
            WRID::send_data(1),
            buf_b.as_ptr() as u64,
            LEN as u32,
            lkey_b,
            remote_addr,
            rkey_a,
        )
        .unwrap();

        // Wait and poll completion on B side.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut wcs = vec![verbs::ibv_wc::default(); 128];
        let comp = qp_b.poll_cq(&mut wcs).unwrap();
        assert_eq!(comp.len(), 1);
        assert_eq!(comp[0].status, verbs::ibv_wc_status::IBV_WC_SUCCESS);

        // Verify the data was read correctly.
        assert_eq!(&buf_b[..LEN], &test_data[..]);
    }

    #[test]
    fn test_rdma_write() {
        let devices = Devices::availables().unwrap();

        let cap = verbs::ibv_qp_cap {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        };

        let qp_a = QueuePair::create(&devices[0], cap).unwrap();
        let qp_b = QueuePair::create(&devices[0], cap).unwrap();
        qp_a.connect(&qp_b.endpoint()).unwrap();
        qp_b.connect(&qp_a.endpoint()).unwrap();

        const LEN: usize = 4096;

        // A side: register empty buffer (target for write).
        let buf_a = RegisteredBuffer::create(&devices, LEN).unwrap();
        let remote_addr = buf_a.as_ptr() as u64;
        let rkey_a = buf_a.rkey(0);

        // B side: fill buffer with test data, issue RDMA Write to A.
        let buf_b = RegisteredBuffer::create(&devices, LEN).unwrap();
        let test_data: Vec<u8> = (0..LEN).map(|i| (i % 199) as u8).collect();
        unsafe {
            std::ptr::copy_nonoverlapping(test_data.as_ptr(), buf_b.as_ptr() as *mut u8, LEN);
        }
        let lkey_b = buf_b.lkey(0);
        qp_b.rdma_write_raw(
            WRID::send_data(1),
            buf_b.as_ptr() as u64,
            LEN as u32,
            lkey_b,
            remote_addr,
            rkey_a,
        )
        .unwrap();

        // Wait and poll completion on B side.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut wcs = vec![verbs::ibv_wc::default(); 128];
        let comp = qp_b.poll_cq(&mut wcs).unwrap();
        assert_eq!(comp.len(), 1);
        assert_eq!(comp[0].status, verbs::ibv_wc_status::IBV_WC_SUCCESS);

        // Verify A side memory was modified.
        assert_eq!(&buf_a[..LEN], &test_data[..]);
    }
}
