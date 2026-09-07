//! Verified SEND/RECV with many simultaneous QPs on a deliberately tiny CQ.
#![forbid(unsafe_code)]
use ruapc_bufpool::{BufferPoolBuilder, Device, DeviceIndex, DeviceSet};
use ruapc_rdma::{
    ActiveDevice, CompletionBatch, CompletionCursor, CompletionQueue, GidType, QpConnectionConfig,
    QueuePair, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type, ibv_send_flags,
};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct BenchDevice {
    inner: ActiveDevice,
    index: DeviceIndex,
}
impl Device for BenchDevice {
    type Registrar = ActiveDevice;
    fn registrar(&self) -> &ActiveDevice {
        &self.inner
    }
    fn index(&self) -> DeviceIndex {
        self.index
    }
    fn set_index(&mut self, index: DeviceIndex) {
        self.index = index;
    }
}
fn arg(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |s| s.parse().unwrap())
}
fn main() {
    let count = arg("RUAPC_CAPACITY_QPS", 20_000);
    assert!(count >= 2);
    let name = std::env::var("RUAPC_BENCH_RDMA_DEVICE").unwrap_or_else(|_| "mlx5_0".into());
    let device = ActiveDevice::available()
        .unwrap()
        .into_iter()
        .find(|d| d.info().name == name)
        .unwrap();
    let mut devices = DeviceSet::default();
    devices.push(BenchDevice {
        inner: device,
        index: DeviceIndex::default(),
    });
    let devices = Arc::new(devices);
    let device = &devices.devices()[0];
    let cq_len = arg("RUAPC_CAPACITY_CQ_LEN", 8);
    let cq = CompletionQueue::create(device.inner.context(), cq_len as i32, None).unwrap();
    let mut qps = Vec::with_capacity(count);
    for index in 0..count {
        let mut attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: 4,
                max_recv_wr: 4,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 0,
            },
            ..Default::default()
        };
        match QueuePair::create(device.inner.pd(), &cq, &cq, &mut attr, device.index()) {
            Ok(qp) => {
                assert!(Arc::ptr_eq(qp.send_cq(), &cq));
                assert!(Arc::ptr_eq(qp.recv_cq(), &cq));
                qps.push(qp);
            }
            Err(error) => {
                println!(
                    "{}",
                    json!({"phase":"capacity_failure","cq_requested":cq_len,"cq_actual":cq.capacity(),"live_qps":qps.len(),"failed_index":index,"kind":format!("{:?}",error.kind),"message":error.to_string()})
                );
                return;
            }
        }
    }
    let first = &qps[count - 2];
    let second = &qps[count - 1];
    let port = device
        .inner
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
    let cfg = |remote: &QueuePair| QpConnectionConfig {
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
    first.connect(&cfg(second)).unwrap();
    second.connect(&cfg(first)).unwrap();
    let pool = BufferPoolBuilder::new(devices.clone())
        .max_memory(64 << 20)
        .build();
    let payload: Vec<u8> = (0..64).map(|i| (i * 3 + 7) as u8).collect();
    let mut send = pool.allocate(payload.len()).unwrap();
    send.set_len(payload.len());
    send.copy_from_slice(&payload);
    second.recv(pool.allocate(payload.len()).unwrap()).unwrap();
    first
        .send_signaled(send, ibv_send_flags::IBV_SEND_SIGNALED)
        .unwrap();
    let mut cursor_first = CompletionCursor::default();
    let mut cursor_second = CompletionCursor::default();
    let mut batch = CompletionBatch::<4>::new();
    let start = Instant::now();
    let mut completed = Vec::new();
    let mut received = false;
    while completed.len() < 2 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "completion timed out"
        );
        for completion in cq.poll_batch(&mut batch).unwrap() {
            let info = completion.info();
            let raw = info.wr_id.raw();
            let qp_num = info.qp_num;
            let (qp, cursor) = if qp_num == first.qp_num() {
                (first, &mut cursor_first)
            } else {
                assert_eq!(qp_num, second.qp_num());
                (second, &mut cursor_second)
            };
            let work = qp.complete(completion, cursor).unwrap();
            assert!(work.wc.succ(), "completion failed: {:?}", work.wc);
            if work.wc.is_recv() {
                let mut buffer = work.buffer.unwrap().into_single().unwrap();
                buffer.set_len(work.wc.byte_len as usize);
                assert_eq!(buffer.as_slice(), payload);
                received = true;
            }
            completed.push(json!({"wrid":raw,"qp_num":qp_num,"recv":work.wc.is_recv()}));
        }
        std::hint::spin_loop();
    }
    assert!(received);
    println!(
        "{}",
        json!({"phase":"exchanged","live_qps":qps.len(),"same_cq":true,"cq_requested":cq_len,"cq_actual":cq.capacity(),"source_qp":first.qp_num(),"destination_qp":second.qp_num(),"payload_bytes":payload.len(),"verified":true,"completions":completed})
    );
    drop(qps);
    drop(cq);
    println!("{}", json!({"phase":"dropped","destroyed_qps":count}));
}
