//! Shared reconciliation for first-use coverage, periodic upkeep and recovery.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::MutexGuard;

use super::placement::{self, PathPreference, ReconcileAction};
use super::{PeerState, RdmaSocketPool, Stripe};
use crate::{Result, State};

#[derive(Clone, Copy)]
pub(super) struct RetryBackoff {
    pub(super) failures: u32,
    pub(super) retry_at: Instant,
}

pub(super) fn backoff_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(8);
    Duration::from_millis((100u64 << exponent).min(30_000))
}

impl RdmaSocketPool {
    /// First use and detected failures share one queued reconciliation pass,
    /// independent of the periodic timer and the foreground request's deadline.
    pub(super) fn schedule_reconcile(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        if self.config.peers.min_connections_per_remote_nic == 0 && !peer.needs_reconnect() {
            return;
        }
        let Some(reconcile) = peer.try_reconcile() else {
            return;
        };
        let peer = peer.clone();
        let state = Arc::downgrade(state);
        let _ = self.task_supervisor.handle().try_spawn(async move {
            // Capture the guard before spawning so rejection or cancellation
            // of an unpolled future also releases the single-flight marker.
            let _reconcile = reconcile;
            let Some(state) = state.upgrade() else {
                return;
            };
            let Some(pool) = state.socket_pool.rdma_pool() else {
                return;
            };
            // Wait instead of dropping this one-shot trigger when an acquire
            // or periodic pass currently owns the peer. Recheck under lock.
            let connect = peer.connect.lock().await;
            pool.reconcile_peer(&peer, &state, connect).await;
        });
    }

    /// Periodic passes skip groups already reconciling or acquiring a
    /// connection. Once started, they yield between actions like first use.
    pub(super) async fn replenish_peer(&self, peer: &Arc<PeerState>, state: &Arc<State>) {
        let Ok(connect) = peer.connect.try_lock() else {
            return;
        };
        let Some(_reconcile) = peer.try_reconcile() else {
            return;
        };
        self.reconcile_peer(peer, state, connect).await;
    }

    /// Fills eligible remote-NIC coverage, then the total stripe target.
    /// The initial lock serializes discovery. Each action takes the lock
    /// afresh, holding it from the healthy snapshot through publication;
    /// queued foreground acquisitions run between those actions.
    async fn reconcile_peer(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        connect: MutexGuard<'_, ()>,
    ) {
        let endpoint = &peer.endpoint;
        const PEER_BACKOFF_KEY: &str = "";
        if !self.reconnect_ready(peer, PEER_BACKOFF_KEY) {
            return;
        }
        let result: Result<()> = async {
            // Fresh bootstrap budgets are independent of the request that
            // scheduled this task; the supervisor can cancel the whole pass.
            let plan = self.prepare_connect_plan(peer, state, None).await?;
            // Discovery can itself wait for RPCs. Yield its lock before the
            // first handshake as well, so queued acquisitions get a turn.
            drop(connect);
            let max_connections = self.config.peers.preconnect_max_per_peer as usize;
            let min_per_remote = self.config.peers.min_connections_per_remote_nic as usize;
            let avoided_remote_nics = HashSet::new();
            let remote_names = placement::eligible_remote_nics(
                &plan.candidates,
                self.config.path.subnet_policy,
            );
            let mut blocked: HashSet<String> = remote_names
                .iter()
                .filter(|remote| !self.reconnect_ready(peer, remote))
                .cloned()
                .collect();

            // Replan against actual admitted sockets after every attempt.
            // A failed or incompatible NIC must not reserve slots that keep
            // later eligible NICs out. Bound work even if admitted QPs fail
            // immediately; the periodic pass retries them under backoff.
            for _ in 0..max_connections.saturating_add(remote_names.len()) {
                // Tokio's FIFO mutex hands queued acquisitions the next turn
                // when the previous action releases its guard. Re-read all
                // placement state after reacquiring; foreground work may have
                // filled coverage or replaced a potential victim meanwhile.
                let _connect = peer.connect.lock().await;
                let existing: Vec<Stripe> = peer
                    .active_snapshot()
                    .into_iter()
                    .filter(|stripe| stripe.socket.state.is_ok())
                    .collect();
                let healthy_remotes: Vec<String> = existing
                    .iter()
                    .map(|stripe| stripe.socket.path.remote.device.clone())
                    .collect();
                let Some(action) = placement::next_connection_action(
                    &remote_names,
                    &healthy_remotes,
                    &blocked,
                    min_per_remote,
                    self.config.peers.connections_per_peer as usize,
                    max_connections,
                ) else {
                    break;
                };
                let (remote, victim) = match &action {
                    ReconcileAction::ConnectCoverage(remote) => (Some(remote), None),
                    ReconcileAction::ReplaceCoverage { remote, victim } => {
                        let victim = existing
                            .iter()
                            .find(|stripe| stripe.socket.path.remote.device == *victim)
                            .expect("coverage plan refers to an existing stripe");
                        (Some(remote), Some(victim.socket.clone()))
                    }
                    ReconcileAction::ConnectTarget => (None, None),
                };
                let preference = PathPreference {
                    remote_device: remote.map(String::as_str),
                    avoided_remote_nics: &avoided_remote_nics,
                };
                match self
                    .connect_with_failover(peer, state, &plan, preference, &existing)
                    .await
                {
                    Ok(established) => {
                        if let Some(remote) = remote {
                            self.clear_reconnect_failure(peer, remote);
                        }
                        if let Some(victim) = victim {
                            self.admit_replacing(peer, &victim, established);
                        } else {
                            self.admit_established(peer, established);
                        }
                    }
                    Err(err) => {
                        let Some(remote) = remote else {
                            return Err(err);
                        };
                        self.record_reconnect_failure(peer, remote);
                        blocked.insert(remote.clone());
                        tracing::debug!(peer = %endpoint, remote_device = %remote, %err, "RDMA coverage connection failed");
                    }
                }
            }
            peer.finish_recovery(&remote_names, &self.config.peers);
            Ok(())
        }
        .await;
        match result {
            Err(err) => {
                self.record_reconnect_failure(peer, PEER_BACKOFF_KEY);
                tracing::debug!("replenishing RDMA stripes to {endpoint} failed: {err}");
            }
            Ok(()) => {
                self.clear_reconnect_failure(peer, PEER_BACKOFF_KEY);
            }
        }
    }

    fn reconnect_ready(&self, peer: &PeerState, remote: &str) -> bool {
        peer.meta
            .lock()
            .unwrap()
            .backoff
            .get(remote)
            .is_none_or(|state| Instant::now() >= state.retry_at)
    }

    fn record_reconnect_failure(&self, peer: &PeerState, remote: &str) {
        let mut meta = peer.meta.lock().unwrap();
        let failures = meta
            .backoff
            .get(remote)
            .map_or(1, |state| state.failures.saturating_add(1));
        let base = backoff_delay(failures);
        let jitter =
            Duration::from_millis(self.pseudo_random() % (base.as_millis() as u64 / 2 + 1));
        meta.backoff.insert(
            remote.to_owned(),
            RetryBackoff {
                failures,
                retry_at: Instant::now() + base + jitter,
            },
        );
    }

    fn clear_reconnect_failure(&self, peer: &PeerState, remote: &str) {
        peer.meta.lock().unwrap().backoff.remove(remote);
    }
}

#[cfg(test)]
mod tests {
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
        router.wrap_method("RdmaService/prepare_connection", move |original| {
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

    async fn next_prepare(
        receiver: &mut mpsc::UnboundedReceiver<PendingPrepare>,
    ) -> PendingPrepare {
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
}
