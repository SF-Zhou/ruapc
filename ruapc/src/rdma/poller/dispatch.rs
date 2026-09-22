//! Dispatch-worker machinery: hands received buffers from the poll
//! threads to a fixed pool of long-lived dispatch worker tasks that walk
//! and parse the frames on tokio worker threads.

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;

use crate::{Message, Socket, State, rdma::frame::for_each_frame};

/// One received buffer awaiting dispatch: the connection's shared state,
/// the socket it arrived on and the raw `[4B len][message]` frames.
type DispatchItem = (Arc<State>, Socket, Bytes);

/// Received buffers dispatched together to amortize queueing and worker wakeups.
pub(super) type DispatchBatch = Vec<DispatchItem>;

/// Flush threshold for a dispatch batch (bounds latency and memory).
pub(super) const MAX_DISPATCH_BATCH: usize = 256;

/// Prefer the home worker below this backlog; spill to share heavier loads.
const SPILL_BACKLOG: usize = 16;

/// Fall back to spawning when every worker reaches this backlog. This is a
/// routing threshold, not a hard limit: pollers can enqueue concurrently.
const MAX_WORKER_BACKLOG: usize = 32;

/// One worker's mpsc queue and unfinished-batch count, shared by the pollers.
struct DispatchWorker {
    tx: tokio::sync::mpsc::UnboundedSender<DispatchBatch>,
    /// Incremented by the sender before each send, decremented by the
    /// worker *after* processing a batch — `0` therefore means "drained
    /// and done", i.e. sending now cannot queue behind anything.
    backlog: Arc<std::sync::atomic::AtomicUsize>,
}

/// Hands received buffers to a fixed pool of Tokio tasks for parsing and
/// dispatch, keeping that work off the CQ poll threads.
///
/// Try the home worker first, then scan for a worker below [`SPILL_BACKLOG`].
/// If none qualifies, use the least loaded below [`MAX_WORKER_BACKLOG`], or
/// spawn a one-shot task when all are saturated. Queue sends never wait;
/// sending to a stopped worker drops that batch.
pub(crate) struct Dispatcher {
    workers: Arc<[DispatchWorker]>,
    /// First worker tried for every batch; spilling does not change it.
    home: usize,
    /// Assigns clone homes round-robin; homes repeat after `workers.len()`.
    next_home: Arc<std::sync::atomic::AtomicUsize>,
}

impl Clone for Dispatcher {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            home: self.next_home.fetch_add(1, Ordering::Relaxed) % self.workers.len(),
            next_home: self.next_home.clone(),
        }
    }
}

impl Dispatcher {
    /// Spawns `workers` long-lived dispatch worker tasks. Must be called
    /// from within a tokio runtime. The workers exit once every
    /// `Dispatcher` clone (one per poll thread, plus the owning pool's)
    /// has been dropped.
    pub fn start(workers: u32) -> Self {
        debug_assert!(workers > 0);
        let workers: Arc<[DispatchWorker]> = (0..workers)
            .map(|_| {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DispatchBatch>();
                let backlog = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let worker_backlog = backlog.clone();
                tokio::spawn(async move {
                    while let Some(batch) = rx.recv().await {
                        run_dispatch_batch(batch);
                        worker_backlog.fetch_sub(1, Ordering::Release);
                    }
                });
                DispatchWorker { tx, backlog }
            })
            .collect();
        Self {
            workers,
            home: 0,
            next_home: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        }
    }

    /// Enqueues the accumulated buffers of one CQ drain for parsing.
    /// Called from the poll threads; never blocks.
    pub(super) fn flush(&mut self, batch: &mut DispatchBatch) {
        if batch.is_empty() {
            return;
        }
        let batch = std::mem::take(batch);

        // Stay with the home worker while it is not too far behind;
        // otherwise spill — for this batch only — to the next worker
        // below the spill threshold.
        let n = self.workers.len();
        for i in 0..n {
            let idx = (self.home + i) % n;
            if self.workers[idx].backlog.load(Ordering::Acquire) < SPILL_BACKLOG {
                self.send(idx, batch);
                return;
            }
        }

        // Every worker is backlogged: try the least loaded below the threshold.
        let (idx, backlog) = self
            .workers
            .iter()
            .enumerate()
            .map(|(idx, worker)| (idx, worker.backlog.load(Ordering::Acquire)))
            .min_by_key(|(_, backlog)| *backlog)
            .expect("at least one dispatch worker");
        if backlog < MAX_WORKER_BACKLOG {
            self.send(idx, batch);
            return;
        }

        // Workers saturated beyond the backlog threshold: fall back to a
        // one-shot task doing the same work rather than blocking.
        tokio::spawn(async move { run_dispatch_batch(batch) });
    }

    /// Sends one batch to the chosen worker, keeping its backlog counter
    /// consistent. Send failures only happen when the runtime is shutting
    /// down (the worker task is gone); the messages are dropped.
    fn send(&self, idx: usize, batch: DispatchBatch) {
        let worker = &self.workers[idx];
        worker.backlog.fetch_add(1, Ordering::AcqRel);
        if let Err(e) = worker.tx.send(batch) {
            worker.backlog.fetch_sub(1, Ordering::AcqRel);
            tracing::debug!("dispatch worker gone; dropping {} buffer(s)", e.0.len());
        }
    }
}

/// Handles one batch of dispatched buffers on a runtime worker thread.
fn run_dispatch_batch(batch: DispatchBatch) {
    for item in batch {
        dispatch_item(item);
    }
}

/// Handles one dispatched buffer: frame walk + parse + routing to the
/// router (requests) or waiter (responses) on a runtime worker thread.
fn dispatch_item((state, socket, frames): DispatchItem) {
    for_each_frame(&frames, |frame| match Message::parse(frame) {
        Ok(msg) => {
            if let Err(e) = state.handle_recv(&socket, msg) {
                tracing::error!("Failed to handle message: {e}");
            }
        }
        Err(e) => tracing::error!("Failed to parse message: {e}"),
    });
}
