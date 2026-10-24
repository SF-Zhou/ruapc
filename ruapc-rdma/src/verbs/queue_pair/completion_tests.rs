//! Hardware-backed regression for mixed SEND/READ ownership retirement.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ruapc_bufpool::{
    AlignedMemory, BufferPoolBuilder, Device as PoolDevice, DeviceIndex, DeviceSet,
};

use super::*;
use crate::{
    ActiveDevice, CompletionBatch, GidType, MemoryRegion, ibv_access_flags, ibv_qp_cap,
    ibv_qp_init_attr, ibv_qp_type, ibv_send_flags,
};

#[derive(Debug)]
struct RegisteredDevice {
    active: ActiveDevice,
    index: DeviceIndex,
}

impl PoolDevice for RegisteredDevice {
    type Registrar = ActiveDevice;

    fn registrar(&self) -> &ActiveDevice {
        &self.active
    }
    fn index(&self) -> DeviceIndex {
        self.index
    }
    fn set_index(&mut self, index: DeviceIndex) {
        self.index = index;
    }
}

#[test]
fn read_completion_sweeps_unsignaled_send_and_settles_read_batch() {
    for success in [true, false] {
        let mut devices = DeviceSet::default();
        devices.push(RegisteredDevice {
            active: crate::test_utils::open_device(),
            index: DeviceIndex::default(),
        });
        let devices = Arc::new(devices);
        let device = &devices.devices()[0];
        let pool = BufferPoolBuilder::new(devices.clone()).build();
        // The remote source must outlive both provider QPs even on assertion
        // failure. Its declaration precedes the QPs for unwinding drop order.
        let mut source = pool.allocate(64).unwrap();
        source.set_len(64);
        source.as_mut_slice().fill(0x5a);
        let remote = source.remote_buffer_info(&device.index).unwrap();
        // Use an actual MR without remote READ permission for the error case;
        // no numeric rkey value is assumed to be universally invalid. Declare
        // both the MR and its backing before the QPs so panic cleanup destroys
        // the provider QPs before deregistering or freeing this source too.
        let denied_source = (!success).then(|| Arc::new(AlignedMemory::new(64).unwrap()));
        let denied_region = denied_source.as_ref().map(|memory| {
            MemoryRegion::register(
                device.active.pd(),
                memory,
                ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as _,
            )
            .unwrap()
        });
        let (remote_addr, rkey) = denied_region
            .as_ref()
            .map_or((remote.addr, remote.key.rkey), |region| {
                (region.addr() as u64, region.rkey())
            });
        let cq = CompletionQueue::create(device.active.context(), 16, None).unwrap();
        let create = || {
            QueuePair::create(
                device.active.pd(),
                &cq,
                &cq,
                &mut ibv_qp_init_attr {
                    qp_type: ibv_qp_type::IBV_QPT_RC,
                    cap: ibv_qp_cap {
                        max_send_wr: 8,
                        max_recv_wr: 8,
                        max_send_sge: 1,
                        max_recv_sge: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                device.index,
            )
            .unwrap()
        };
        let mut sender = create();
        let receiver = create();
        let port = device
            .active
            .info()
            .ports
            .iter()
            .find(|p| p.is_usable())
            .unwrap();
        let gid = port
            .gids
            .iter()
            .find(|g| g.gid_type == GidType::RoCEv2)
            .or_else(|| port.gids.first())
            .unwrap();
        let config = |remote: &QueuePair| QpConnectionConfig {
            local_port_num: port.port_num,
            local_gid_index: gid.index,
            pkey_index: 0,
            link_layer: port.port_attr.link_layer,
            path_mtu: port.port_attr.active_mtu,
            remote_qp_num: remote.qp_num(),
            remote_gid: gid.gid,
            remote_lid: port.port_attr.lid,
            local_psn: 7,
            remote_psn: 7,
            max_rd_atomic: 1,
            max_dest_rd_atomic: 1,
            traffic_class: 0,
        };
        sender.connect(&config(&receiver)).unwrap();
        receiver.connect(&config(&sender)).unwrap();
        sender.set_send_signal_interval(8, 8);
        // Model a failed post's consumed sequence: the following data SEND
        // is offset 1 and therefore unsignaled, leaving the READ to sweep it.
        drop(sender.send_identity.lock_send().unwrap());
        let make_buffer = || {
            let mut buffer = pool.allocate(64).unwrap();
            buffer.set_len(64);
            buffer
        };
        receiver.recv(make_buffer()).unwrap();
        let mut sent = make_buffer();
        sent.as_mut_slice().fill(0x3c);
        let send_id = sender.send(sent, ibv_send_flags(0)).unwrap();
        let (mut posting, mut result) = sender
            .prepare_reads(
                vec![make_buffer()],
                vec![ReadRequest {
                    remote_addr,
                    // Error CQEs must dispatch ownership using WRID: their opcode
                    // need not be meaningful even for this known READ request.
                    rkey,
                    segments: vec![ReadSegment {
                        buffer: 0,
                        offset: 0,
                        len: 64,
                    }],
                }],
                None,
            )
            .unwrap();
        sender.post_read(&mut posting).unwrap();
        assert_eq!(sender.pending_read_count(), 1);
        let mut send_cursor = CompletionCursor::default();
        let mut recv_cursor = CompletionCursor::default();
        let mut batch = CompletionBatch::<8>::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut received = false;
        let mut read = false;
        while !received || !read {
            assert!(
                Instant::now() < deadline,
                "mixed SEND/READ did not complete"
            );
            for completion in cq.poll_batch(&mut batch).unwrap() {
                if completion.qp_num() == receiver.qp_num() {
                    let completed = receiver.complete(completion, &mut recv_cursor).unwrap();
                    assert!(completed.wc.succ());
                    let buffer = completed.buffer.unwrap().into_single().unwrap();
                    assert_eq!(buffer.as_slice(), &[0x3c; 64]);
                    received = true;
                } else {
                    let completed = sender.complete(completion, &mut send_cursor).unwrap();
                    assert_eq!(completed.wc.wr_id.get_type(), WRType::Read);
                    assert_eq!(completed.wc.succ(), success);
                    assert_eq!(completed.swept_sends, 1);
                    assert!(completed.buffer.is_none());
                    assert!(sender.send_wrs.take(send_id).is_none());
                    assert_eq!(sender.pending_read_count(), 0);
                    read = true;
                }
            }
            std::thread::yield_now();
        }
        if success {
            let buffers = result.try_recv().unwrap().unwrap();
            assert_eq!(buffers[0].as_slice(), &[0x5a; 64]);
        } else {
            assert_eq!(
                result.try_recv().unwrap().unwrap_err(),
                ReadFailure::Completion
            );
        }
        // A single in-flight READ leaves its slot vacant, so this path must
        // never report fallback-map use.
        assert_eq!(sender.read_slot_spills(), 0);
    }
}
