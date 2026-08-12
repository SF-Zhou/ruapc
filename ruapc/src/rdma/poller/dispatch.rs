//! Dispatch-worker machinery: hands received buffers from the poll
//! threads to a fixed pool of long-lived dispatch worker tasks that walk
//! and parse the frames on tokio worker threads.

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;

use crate::{Error, ErrorKind, Message, Socket, State, rdma::RdmaSocket};

/// Size of the per-frame header: a big-endian u32 frame length.
///
/// Every RDMA send is a sequence of `[4B frame_len][4B meta_len][meta]
/// [payload]` frames — usually one. Uniform framing makes messages
/// self-delimiting, so aggregation is plain concatenation and the receive
/// path has a single parse loop.
pub(crate) const FRAME_HEADER: usize = 4;

/// One received buffer awaiting dispatch: the connection's shared state,
/// the socket it arrived on and the raw `[4B len][message]` frames.
type DispatchItem = (Arc<State>, Socket, Bytes);

/// Received buffers of one completion batch, dispatched together. Handing
/// batches (instead of single buffers) to the queue amortizes the enqueue
/// and — more importantly — the worker wakeup over an entire CQ drain:
/// per-buffer enqueueing measurably collapses throughput because nearly
/// every message then pays one parked-task wakeup.
pub(super) type DispatchBatch = Vec<DispatchItem>;

/// Flush threshold for a dispatch batch (bounds latency and memory).
pub(super) const MAX_DISPATCH_BATCH: usize = 256;

/// Batches a sticky worker may have queued before the router spills to
/// the next worker.
///
/// Queueing a backlog on the current (busy, hence running and cache-hot)
/// worker is cheaper than engaging another one: a drained worker is a
/// parked task, and waking it is a cross-thread wake through tokio's
/// remote-injection path — benchmarks show eager spilling (threshold 4)
/// quadruples context switches and costs ~30% QPS at high load. Spilling
/// only under real pressure keeps one hot worker per poll thread in the
/// common case — wakeups coalesce exactly as they would with a dedicated
/// dispatcher task — while still growing parallelism when a worker
/// genuinely falls behind (a threshold of 16 batches is multiple
/// milliseconds of parse backlog).
const SPILL_BACKLOG: usize = 16;

/// Batches queued per worker before the dispatcher considers it saturated
/// and moves on (ultimately to the one-shot spawn fallback). Bounds the
/// standing backlog per worker without a bounded channel.
const MAX_WORKER_BACKLOG: usize = 32;

/// One dispatch worker endpoint: an SPSC queue plus the number of batches
/// sent to it that it has not finished processing yet.
struct DispatchWorker {
    tx: tokio::sync::mpsc::UnboundedSender<DispatchBatch>,
    /// Incremented by the sender before each send, decremented by the
    /// worker *after* processing a batch — `0` therefore means "drained
    /// and done", i.e. sending now cannot queue behind anything.
    backlog: Arc<std::sync::atomic::AtomicUsize>,
}

/// Hands received buffers from the poll threads to a fixed pool of
/// long-lived dispatch worker tasks, each owning one SPSC queue.
///
/// Dispatching (frame walk, parse, request spawn / response oneshot wake)
/// from the poll thread would serialize that work on the shard and — for
/// spawns — go through tokio's remote-injection path, whose shared lock
/// becomes the global throughput ceiling. Spawning a task per batch has
/// the same problem: every spawn from the poll thread is a remote inject
/// plus a task allocation.
///
/// Routing is *home worker + spill on pressure* — not blind round-robin,
/// and deliberately not a shared MPMC queue:
///
/// - Each poll thread has a private home worker; while it keeps up
///   (backlog below [`SPILL_BACKLOG`]) it receives every batch: it stays
///   cache-hot, and its wakeups coalesce exactly like a dedicated
///   dispatcher task's would (a send to a busy worker is just a
///   lock-free push, no wake at all).
/// - Only when it falls genuinely behind do batches spill to the next
///   worker, so parallelism grows with load instead of rotating every
///   batch through a different cold, parked task. (A shared MPMC queue
///   does the opposite — each send wakes the longest-parked consumer —
///   which benchmarked 20-30% slower at high load.)
/// - With every worker past the spill threshold, batches queue (bounded
///   by [`MAX_WORKER_BACKLOG`]) on the least-loaded worker; only beyond
///   that does the poll thread degrade to a one-shot `tokio::spawn` per
///   batch, so it never blocks and no buffer is dropped.
pub(crate) struct Dispatcher {
    workers: Arc<[DispatchWorker]>,
    /// This clone's *home* worker. Every batch is offered to the home
    /// worker first and only spills forward for that single batch, so a
    /// poll thread always returns to its own worker once a burst is over.
    /// A *sticky cursor* that moves on spill was measurably worse: two
    /// poll threads whose cursors land on the same worker herd there —
    /// both then spill in lockstep and keep sharing one worker, halving
    /// dispatch throughput.
    home: usize,
    /// Hands every clone a distinct home, so poll threads stick to
    /// *different* workers instead of piling onto the same one.
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
        let workers: Arc<[DispatchWorker]> = (0..workers.max(1))
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

        // Every worker is backlogged: queue (bounded) on the least loaded.
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

        // Workers saturated beyond the backlog cap: fall back to a
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

/// Walks the `[4B len][message]` frames of one received buffer, invoking
/// `f` with each frame (a zero-copy slice of the refcounted buffer).
pub(super) fn for_each_frame(frames: &Bytes, mut f: impl FnMut(Bytes)) {
    let mut offset = 0;
    while offset < frames.len() {
        let Some(header) = frames.get(offset..offset + FRAME_HEADER) else {
            tracing::error!("truncated frame header at {offset}");
            return;
        };
        let frame_len = u32::from_be_bytes(header.try_into().unwrap()) as usize;
        let start = offset + FRAME_HEADER;
        let Some(end) = start
            .checked_add(frame_len)
            .filter(|end| *end <= frames.len())
        else {
            tracing::error!("truncated frame ({frame_len}B) at {offset}");
            return;
        };
        f(frames.slice(start..end));
        offset = end;
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

/// Resolves every outstanding read batch of a socket with
/// `ConnectionClosed` (without releasing their memory holds).
pub(super) fn fail_read_batches(socket: &RdmaSocket) {
    for entry in socket.rdma_completions.iter() {
        entry.value().fail(Error::new(
            ErrorKind::ConnectionClosed,
            "rdma poll thread shut down with reads in flight".into(),
        ));
    }
}
