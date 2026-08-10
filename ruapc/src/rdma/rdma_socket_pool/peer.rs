use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use foldhash::fast::RandomState;

use super::{CachedRdmaInfo, PathKey, RetryBackoff, Stripe};

/// Connection stripes of one peer, guarded by a *synchronous* lock:
/// critical sections must stay short and must never await. Structural
/// mutations of `active` happen under the peer's `connect` lock
/// (admission) or in maintenance pruning; `draining` is additionally
/// touched by the drain timers.
#[derive(Default)]
pub(super) struct PeerStripes {
    pub(super) active: Vec<Stripe>,
    pub(super) draining: Vec<Stripe>,
}

#[derive(Default)]
pub(super) struct PeerMeta {
    pub(super) last_used: Option<Instant>,
    pub(super) device_cache: Option<CachedRdmaInfo>,
    pub(super) backoff: HashMap<String, RetryBackoff, RandomState>,
    pub(super) blacklist: HashMap<PathKey, Instant, RandomState>,
}

pub(crate) struct PeerState {
    pub(super) addr: SocketAddr,
    pub(super) connect: tokio::sync::Mutex<()>,
    pub(super) stripes: RwLock<PeerStripes>,
    pub(super) meta: Mutex<PeerMeta>,
}

impl PeerState {
    pub(super) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            connect: tokio::sync::Mutex::new(()),
            stripes: RwLock::new(PeerStripes::default()),
            meta: Mutex::new(PeerMeta::default()),
        }
    }

    pub(super) fn touch(&self, now: Instant) {
        self.meta.lock().unwrap().last_used = Some(now);
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.stripes
            .read()
            .unwrap()
            .active
            .iter()
            .any(|stripe| stripe.socket.state.is_ok())
    }

    pub(super) fn active_snapshot(&self) -> Vec<Stripe> {
        self.stripes.read().unwrap().active.clone()
    }

    pub(super) fn all_sockets(&self) -> Vec<Arc<super::RdmaSocket>> {
        let stripes = self.stripes.read().unwrap();
        stripes
            .active
            .iter()
            .chain(&stripes.draining)
            .map(|stripe| stripe.socket.clone())
            .collect()
    }
}

impl std::fmt::Debug for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerState")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}
