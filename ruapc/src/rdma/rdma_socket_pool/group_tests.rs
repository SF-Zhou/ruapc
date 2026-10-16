//! Address-group behavior needs no RDMA device or queue pair.

use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use super::*;
use crate::rdma::{RdmaBootstrapService as _, rdma_service::RDMA_BOOTSTRAP_PROTOCOL_VERSION};
use crate::{Context, Endpoint, Listener, Router, Server, Transport};

fn pool_without_rdma_devices() -> RdmaSocketPool {
    let devices = Arc::new(Devices::default());
    let buffers = ruapc_bufpool::BufferPoolBuilder::new(devices.clone()).build();
    RdmaSocketPool::new(devices, buffers, RdmaSocketPoolConfig::default()).unwrap()
}

fn endpoint(ports: &[u16]) -> Endpoint {
    Endpoint::new(
        Transport::RDMA,
        ports
            .iter()
            .map(|port| SocketAddr::from(([127, 0, 0, 1], *port))),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalized_groups_share_one_peer_and_connect_lock() {
    let pool = Arc::new(pool_without_rdma_devices());
    let group = endpoint(&[41001, 41002]);
    let equivalent = endpoint(&[41002, 41001, 41002]);
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for index in 0..16 {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let group = if index % 2 == 0 { &group } else { &equivalent }.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            pool.peer(&group)
        }));
    }
    let mut tasks = tasks.into_iter();
    let peer = tasks.next().unwrap().await.unwrap();
    for task in tasks {
        assert!(Arc::ptr_eq(&peer, &task.await.unwrap()));
    }
    assert!(Arc::ptr_eq(&peer, &pool.peer(&group)));
    assert_eq!(pool.peers.len(), 1);
    let now = Instant::now();
    peer.touch(now);
    assert_eq!(
        pool.existing_peer(&equivalent)
            .unwrap()
            .meta
            .lock()
            .unwrap()
            .last_used,
        Some(now),
    );

    // Exercise the actual acquire path while the equivalent spelling owns
    // the connect lock. A separate peer would instead attempt TCP discovery.
    let _guard = peer.connect.lock().await;
    let context = Context::create(&SocketPoolConfig {
        rdma: None,
        ..Default::default()
    })
    .unwrap();
    let error = pool
        .acquire_with_options(
            &equivalent,
            &HashSet::new(),
            Some(Instant::now() + Duration::from_millis(20)),
            &context.state,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(
        error.msg.contains("waiting for peer connect lock"),
        "{error}"
    );
    assert_eq!(pool.peers.len(), 1);
}

#[tokio::test]
async fn overlapping_groups_keep_independent_peer_state() {
    let pool = pool_without_rdma_devices();
    let first = pool.peer(&endpoint(&[41001, 41002]));
    let overlapping = pool.peer(&endpoint(&[41002, 41003]));
    let subset = pool.peer(&endpoint(&[41001]));
    assert!(!Arc::ptr_eq(&first, &overlapping));
    assert!(!Arc::ptr_eq(&first, &subset));
    assert_eq!(pool.peers.len(), 3);

    let _guard = first.connect.lock().await;
    assert!(overlapping.connect.try_lock().is_ok());
    assert!(subset.connect.try_lock().is_ok());
    first.touch(Instant::now());
    assert!(overlapping.meta.lock().unwrap().last_used.is_none());
    assert!(subset.meta.lock().unwrap().last_used.is_none());
}

#[tokio::test]
async fn bootstrap_group_reconnects_through_another_listener_without_rdma_hardware() {
    // Keep the real bootstrap service and RDMA pool, but register no RDMA
    // devices. Discovery and TCP entry-point failover require no verbs work.
    let mut config = SocketPoolConfig::default();
    config.rdma.as_mut().unwrap().path.device_filter =
        vec!["ruapc-test-no-matching-rdma-device".into()];
    let server = Server::create(Router::default(), &config).unwrap();
    assert!(server.state().devices.devices().is_empty());
    let first = Listener::new();
    let second = Listener::new();
    let first_addr = first
        .start_listen("127.0.0.1:0".parse().unwrap(), server.state())
        .await
        .unwrap();
    let second_addr = second
        .start_listen("127.0.0.1:0".parse().unwrap(), server.state())
        .await
        .unwrap();
    let unavailable = "127.0.0.1:0".parse().unwrap();
    let group = Endpoint::new(Transport::RDMA, [second_addr, unavailable, first_addr]).unwrap();
    let peer = PeerState::new(group.clone());
    let context = Context::create(&SocketPoolConfig {
        rdma: None,
        ..Default::default()
    })
    .unwrap();
    let bootstrap = peer.bootstrap_context(&context.state);
    let crate::core::ContextEndpoint::Endpoint(tcp_endpoint) = &bootstrap.endpoint else {
        panic!("bootstrap must retain an address group");
    };
    assert_eq!(tcp_endpoint.transport(), Transport::TCP);
    assert_eq!(tcp_endpoint.addrs(), group.addrs());
    let client = Client {
        max_retries: 0,
        ..Default::default()
    };
    let info = client.discover(&bootstrap, &()).await.unwrap();
    assert_eq!(info.protocol_version, RDMA_BOOTSTRAP_PROTOCOL_VERSION);
    assert!(info.devices.is_empty());
    let Socket::TCP(original) = context
        .state
        .socket_pool
        .acquire(tcp_endpoint, &context.state)
        .await
        .unwrap()
    else {
        panic!("bootstrap must use TCP");
    };

    // The normalized TCP group tries port zero, then the lower live port.
    // Stop that accept loop and invalidate its cached connection. The same
    // bootstrap context must still discover through the other listener.
    let (closed, remaining) = if first_addr < second_addr {
        (&first, &second)
    } else {
        (&second, &first)
    };
    closed.stop();
    closed.join().await;
    assert!(original.mark_closed());
    let info = client.discover(&bootstrap, &()).await.unwrap();
    assert_eq!(info.protocol_version, RDMA_BOOTSTRAP_PROTOCOL_VERSION);
    let replacement = context
        .state
        .socket_pool
        .acquire(tcp_endpoint, &context.state)
        .await
        .unwrap();
    assert_ne!(replacement.conn_id(), Some(original.conn_id()));

    remaining.stop();
    remaining.join().await;
    server.stop();
    tokio::time::timeout(Duration::from_secs(5), server.join())
        .await
        .unwrap();
}
