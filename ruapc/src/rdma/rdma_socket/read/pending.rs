//! READ failure policy. The verbs crate alone owns and settles DMA work.

use std::{sync::Mutex, time::Instant};

use ruapc_rdma::{ReadFailure, ReadHandle, ReadObservation};

/// Bound weak metadata even when idle connections skip deadline sweeps.
const PRUNE_INTERVAL: usize = 64;

type Notifications = Vec<(ReadObservation, Option<ReadFailure>)>;

#[derive(Debug, Default)]
pub(in super::super) struct PendingReads(Mutex<Inner>);

#[derive(Debug, Default)]
struct Inner {
    entries: Vec<PendingRead>,
    /// Cached empty: neither completed batches nor DMA holds survive a scan.
    scan: Notifications,
    registrations: usize,
}

#[derive(Debug)]
struct PendingRead {
    handle: ReadHandle,
    deadlines: Deadlines,
}

#[derive(Debug)]
struct Deadlines {
    read: Option<Instant>,
    request: Option<Instant>,
    watchdog_reported: bool,
}

impl Deadlines {
    fn expire(&mut self, now: Instant) -> (Option<ReadFailure>, bool) {
        let read_expired = self.read.is_some_and(|deadline| deadline <= now);
        let failure = if self
            .request
            .is_some_and(|request| request <= now && self.read.is_none_or(|read| request <= read))
        {
            Some(ReadFailure::RequestTimeout)
        } else if read_expired {
            Some(ReadFailure::Timeout)
        } else {
            None
        };
        // Request expiry or a dropped receiver must never disarm the READ
        // watchdog. The boolean belongs to the registry, not to its sender.
        let close = read_expired && !self.watchdog_reported;
        self.watchdog_reported |= read_expired;
        (failure, close)
    }
}

impl PendingReads {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn register(
        &self,
        handle: ReadHandle,
        read: Option<Instant>,
        request: Option<Instant>,
    ) {
        let mut inner = self.lock();
        inner.registrations += 1;
        let scan = if inner.registrations >= PRUNE_INTERVAL {
            inner.registrations = 0;
            Some(Self::collect(&mut inner, |_, _| None))
        } else {
            None
        };
        inner.entries.push(PendingRead {
            handle,
            deadlines: Deadlines {
                read,
                request,
                watchdog_reported: false,
            },
        });
        drop(inner);
        if let Some(scan) = scan {
            self.restore(scan);
        }
    }

    /// Upgrade under the registry lock but defer every strong-reference drop
    /// until after unlocking. An observation can become the final owner during
    /// concurrent completion or QP destruction and then wake the receiver.
    fn collect(
        inner: &mut Inner,
        mut failure: impl FnMut(&mut PendingRead, &ReadObservation) -> Option<ReadFailure>,
    ) -> Notifications {
        let mut scan = std::mem::take(&mut inner.scan);
        debug_assert!(scan.is_empty(), "cached scan retained a READ observation");
        inner.entries.retain_mut(|entry| {
            let Some(observation) = entry.handle.observe() else {
                return false;
            };
            let pending = observation.is_pending();
            let reason = if pending {
                failure(entry, &observation)
            } else {
                None
            };
            scan.push((observation, reason));
            pending
        });
        scan
    }

    fn restore(&self, mut scan: Notifications) {
        // Wakers and Buffer destructors must run outside the registry lock.
        scan.clear();
        if scan.capacity() != 0 {
            let mut inner = self.lock();
            if scan.capacity() > inner.scan.capacity() {
                inner.scan = scan;
            }
        }
    }

    pub(super) fn fail(&self) {
        let scan = Self::collect(&mut self.lock(), |_, _| Some(ReadFailure::ConnectionClosed));
        for (observation, reason) in &scan {
            if let Some(reason) = reason {
                observation.fail(*reason);
            }
        }
        self.restore(scan);
    }

    pub(super) fn expire(&self, now: Instant) -> bool {
        let mut expired = false;
        let scan = Self::collect(&mut self.lock(), |entry, observation| {
            // Remaining includes admission-blocked work. A deadline can imply
            // a QP fault only while the provider still has an accepted READ.
            if observation.has_in_flight() {
                let (reason, close) = entry.deadlines.expire(now);
                expired |= close;
                reason
            } else {
                None
            }
        });
        for (observation, reason) in &scan {
            if let Some(reason) = reason {
                observation.fail(*reason);
            }
        }
        self.restore(scan);
        expired
    }
}

#[cfg(test)]
mod tests;
