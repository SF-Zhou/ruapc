//! Real bootstrap handshakes verify that replenishment yields the peer lock
//! between connections while still completing without a maintenance timer.

use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::timeout};

use super::backoff_delay;
use crate::rdma::test_utils::small_rdma_config;
use crate::{
    Context, Endpoint, Error, ErrorKind, Payload, Result, Router, Server, Socket, Transport,
};

#[test]
fn reconnect_backoff_is_bounded() {
    assert_eq!(backoff_delay(1), Duration::from_millis(100));
    assert_eq!(backoff_delay(4), Duration::from_millis(800));
    assert!(backoff_delay(100) <= Duration::from_secs(30));
}

type Handler = Arc<dyn Fn(Context, Payload) -> Result<()> + Send + Sync>;

struct PendingPrepare {
    handler: Handler,
    context: Context,
    payload: Payload,
}

impl PendingPrepare {
    fn release(self) {
        (self.handler)(self.context, self.payload).unwrap();
    }
}

/// Keeping requests in a channel delays only prepare RPC dispatch. Closing
/// the receiver drops every held context; there are no detached gate tasks.
fn gated_router() -> (Router, mpsc::UnboundedReceiver<PendingPrepare>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let mut router = Router::default();
    router.wrap_method("RdmaBootstrapService/prepare_connection", move |original| {
        let handler: Handler = Arc::from(original);
        Box::new(move |context, payload| {
            sender
                .send(PendingPrepare {
                    handler: handler.clone(),
                    context,
                    payload,
                })
                .map_err(|_| Error::kind(ErrorKind::ConnectionClosed))
        })
    });
    (router, receiver)
}

async fn next_prepare(receiver: &mut mpsc::UnboundedReceiver<PendingPrepare>) -> PendingPrepare {
    timeout(Duration::from_secs(10), receiver.recv())
        .await
        .expect("background reconciliation did not attempt the next connection")
        .expect("prepare handler was dropped before reconciliation completed")
}

async fn assert_reconcile_yields_to_foreground(periodic: bool) {
    let mut config = small_rdma_config();
    let rdma = config.rdma.as_mut().unwrap();
    rdma.peers.connections_per_peer = 1;
    rdma.peers.min_connections_per_remote_nic = 4;
    rdma.peers.preconnect_max_per_peer = 4;
    assert_eq!(rdma.maintenance.interval_ms, 0);
    assert_eq!(rdma.probe.interval_ms, 0);

    let (router, mut requests) = gated_router();
    let server = Server::create(router, &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let endpoint = Endpoint::rdma(Transport::TCP, [address]).unwrap();
    let context = Context::create(&config).unwrap();
    let pool = context.state.socket_pool.rdma_pool().unwrap();
    let peer = pool.peer(&endpoint);
    let periodic_task = if periodic {
        let state = context.state.clone();
        let peer = peer.clone();
        Some(tokio::spawn(async move {
            state
                .socket_pool
                .rdma_pool()
                .unwrap()
                .replenish_peer(&peer, &state)
                .await;
        }))
    } else {
        pool.schedule_reconcile(&peer, &context.state);
        None
    };

    let first = next_prepare(&mut requests).await;
    assert!(peer.active_snapshot().is_empty());

    // Poll the actual acquire path while the first background handshake holds
    // the lock. This queues the foreground waiter before the next action.
    let avoided = HashSet::new();
    let mut foreground = std::pin::pin!(pool.acquire_with_options(
        &endpoint,
        &avoided,
        Some(std::time::Instant::now() + Duration::from_secs(5)),
        &context.state,
    ));
    assert!(futures_util::poll!(foreground.as_mut()).is_pending());
    first.release();

    // Only the first handshake may finish. A pass holding the lock throughout
    // would block this acquisition on its second, still-gated prepare RPC.
    let socket = timeout(Duration::from_secs(2), foreground.as_mut())
        .await
        .expect("foreground acquisition waited for the entire reconciliation pass")
        .unwrap();
    assert!(matches!(socket, Socket::RDMA(_)));
    assert_eq!(peer.active_snapshot().len(), 1);

    for _ in 1..4 {
        next_prepare(&mut requests).await.release();
    }
    timeout(Duration::from_secs(10), async {
        loop {
            let stripes = peer.active_snapshot();
            if stripes.len() == 4 && stripes.iter().all(|stripe| stripe.socket.state.is_ok()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("yielding reconciliation did not finish coverage with maintenance disabled");
    if let Some(task) = periodic_task {
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    drop(requests);
    context.state.socket_pool.stop();
    server.stop();
    timeout(Duration::from_secs(5), async {
        tokio::join!(context.state.socket_pool.join(), server.join());
    })
    .await
    .expect("reconciliation left shutdown waiting for a held prepare request");
    assert_eq!(context.state.waiter.pending_count(), 0);
    assert_eq!(server.state().waiter.pending_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_reconcile_yields_to_foreground_and_finishes_without_maintenance() {
    assert_reconcile_yields_to_foreground(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_reconcile_yields_to_foreground_between_connections() {
    assert_reconcile_yields_to_foreground(true).await;
}
