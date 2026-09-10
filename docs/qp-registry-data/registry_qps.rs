//! Physical same-CQ QP capacity probe. Never posts work requests.
#![forbid(unsafe_code)]
use ruapc_bufpool::DeviceIndex;
use ruapc_rdma::{
    ActiveDevice, CompletionQueue, QueuePair, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type,
};
use serde_json::json;
use std::{collections::HashSet, sync::Arc, time::Instant};

fn arg(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |s| s.parse().unwrap())
}
fn memory() -> serde_json::Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let kb = |key: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(key)
                .map(|v| v.split_whitespace().next().unwrap().parse::<u64>().unwrap())
        })
    };
    json!({"rss_kib":kb("VmRSS:"), "locked_kib":kb("VmLck:"),
        "cgroup_bytes":std::fs::read_to_string("/sys/fs/cgroup/memory.current").ok().and_then(|s| s.trim().parse::<u64>().ok())})
}
fn main() {
    let count = arg("RUAPC_CAPACITY_QPS", 20_000);
    let cq_len = arg("RUAPC_CAPACITY_CQ_LEN", 8);
    let depth = arg("RUAPC_CAPACITY_DEPTH", 4);
    let name = std::env::var("RUAPC_BENCH_RDMA_DEVICE").unwrap_or_else(|_| "mlx5_0".into());
    let device = ActiveDevice::available()
        .unwrap()
        .into_iter()
        .find(|d| d.info().name == name)
        .unwrap();
    assert!(count <= device.info().device_attr.max_qp as usize);
    let before = memory();
    let cq = CompletionQueue::create(device.context(), cq_len as i32, None).unwrap();
    let start = Instant::now();
    let mut qps = Vec::with_capacity(count);
    let mut numbers = HashSet::with_capacity(count);
    let mut failure = None;
    let mut actual_cap = None;
    for i in 0..count {
        let mut attr = ibv_qp_init_attr {
            qp_type: ibv_qp_type::IBV_QPT_RC,
            cap: ibv_qp_cap {
                max_send_wr: depth as u32,
                max_recv_wr: depth as u32,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 0,
            },
            ..Default::default()
        };
        match QueuePair::create(device.pd(), &cq, &cq, &mut attr, DeviceIndex::default()) {
            Ok(qp) => {
                assert!(Arc::ptr_eq(qp.send_cq(), &cq));
                assert!(Arc::ptr_eq(qp.recv_cq(), &cq));
                assert!(numbers.insert(qp.qp_num()));
                if actual_cap.is_none() {
                    actual_cap = Some(
                        json!({"send_wr":attr.cap.max_send_wr,"recv_wr":attr.cap.max_recv_wr,"send_sge":attr.cap.max_send_sge,"recv_sge":attr.cap.max_recv_sge}),
                    );
                }
                qps.push(qp);
            }
            Err(error) => {
                failure = Some(
                    json!({"at_index":i,"kind":format!("{:?}", error.kind),"message":error.to_string()}),
                );
                break;
            }
        }
        if qps.len() % 512 == 0 {
            println!(
                "{}",
                json!({"phase":"progress","live_qps":qps.len(),"elapsed_s":start.elapsed().as_secs_f64(),"memory":memory()})
            );
        }
    }
    let created = qps.len();
    println!(
        "{}",
        json!({"phase":"created","device":name,"requested_qps":count,"live_qps":created,"cq_requested":cq_len,"requested_depth":depth,"actual_qp_cap":actual_cap,"cq_actual":cq.capacity(),"all_share_one_cq":true,"failure":failure,"elapsed_s":start.elapsed().as_secs_f64(),"memory_before":before,"memory_after":memory()})
    );
    let drop_start = Instant::now();
    drop(qps);
    drop(cq);
    println!(
        "{}",
        json!({"phase":"dropped","destroyed_qps":created,"elapsed_s":drop_start.elapsed().as_secs_f64(),"memory":memory()})
    );
}
