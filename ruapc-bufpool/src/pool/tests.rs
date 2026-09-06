use super::*;
use crate::EmptyDevices;
use crate::buddy::{LEVEL_SIZES, SIZE_1MIB};

fn test_pool() -> Arc<BufferPool> {
    BufferPoolBuilder::new(Arc::new(EmptyDevices)).build()
}

fn test_pool_with_max(max: usize) -> Arc<BufferPool> {
    BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(max)
        .build()
}

#[test]
fn test_pool_builder_defaults() {
    let pool = test_pool();
    drop(pool);
}

#[test]
fn test_pool_builder_custom() {
    let pool = test_pool_with_max(128 * 1024 * 1024);
    drop(pool);
}

#[test]
fn test_simple_allocation() {
    let pool = test_pool();
    let buffer = pool.allocate(SIZE_1MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_1MIB);
}

#[test]
fn test_allocation_sizes() {
    let pool = test_pool();

    // Small sizes are served by the slab layer.
    let s1 = pool.allocate(1).unwrap();
    assert_eq!(s1.len(), 16 * 1024);

    let s1b = pool.allocate(16 * 1024 + 1).unwrap();
    assert_eq!(s1b.len(), 64 * 1024);

    let s2 = pool.allocate(64 * 1024).unwrap();
    assert_eq!(s2.len(), 64 * 1024);

    let s3 = pool.allocate(64 * 1024 + 1).unwrap();
    assert_eq!(s3.len(), 256 * 1024);

    let s4 = pool.allocate(256 * 1024).unwrap();
    assert_eq!(s4.len(), 256 * 1024);

    // Larger sizes go to the buddy allocator.
    let b1 = pool.allocate(256 * 1024 + 1).unwrap();
    assert_eq!(b1.len(), SIZE_1MIB);

    let b2 = pool.allocate(SIZE_1MIB + 1).unwrap();
    assert_eq!(b2.len(), LEVEL_SIZES[1]);

    let b3 = pool.allocate(LEVEL_SIZES[1] + 1).unwrap();
    assert_eq!(b3.len(), LEVEL_SIZES[2]);

    let b4 = pool.allocate(LEVEL_SIZES[2] + 1).unwrap();
    assert_eq!(b4.len(), LEVEL_SIZES[3]);
}

#[test]
fn test_allocation_reuse() {
    let pool = test_pool_with_max(SIZE_64MIB);

    let addr1 = {
        let buffer = pool.allocate(SIZE_1MIB).unwrap();
        buffer.as_ptr() as usize
    };

    let buffer2 = pool.allocate(SIZE_1MIB).unwrap();
    let addr2 = buffer2.as_ptr() as usize;

    assert!(addr1 > 0);
    assert!(addr2 > 0);
}

#[test]
fn test_memory_limit_sync() {
    let pool = test_pool_with_max(SIZE_64MIB);

    let _b1 = pool.allocate(SIZE_64MIB).unwrap();

    let result = pool.allocate(SIZE_1MIB);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::OutOfMemory);
}

#[test]
fn test_invalid_size() {
    let pool = test_pool();

    let result = pool.allocate(0);
    assert!(result.is_err());

    let result = pool.allocate(SIZE_64MIB + 1);
    assert!(result.is_err());
}

#[test]
fn test_buddy_splitting() {
    let pool = test_pool_with_max(SIZE_64MIB);

    let buffers: Vec<_> = (0..64).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();

    assert_eq!(buffers.len(), 64);

    let base = buffers[0].as_ptr() as usize;
    for buf in &buffers {
        let addr = buf.as_ptr() as usize;
        assert!(addr >= base - SIZE_64MIB && addr < base + SIZE_64MIB);
        assert_eq!(buf.len(), SIZE_1MIB);
    }
}

#[test]
fn test_buddy_merging() {
    let pool = test_pool_with_max(SIZE_64MIB);

    let b1 = pool.allocate(LEVEL_SIZES[2]).unwrap();
    let b2 = pool.allocate(LEVEL_SIZES[2]).unwrap();
    let b3 = pool.allocate(LEVEL_SIZES[2]).unwrap();
    let b4 = pool.allocate(LEVEL_SIZES[2]).unwrap();

    assert!(pool.allocate(SIZE_1MIB).is_err());

    drop(b1);
    drop(b2);
    drop(b3);
    drop(b4);

    let b5 = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(b5.len(), SIZE_64MIB);
}

#[tokio::test]
async fn test_async_allocation() {
    let pool = test_pool();
    let buffer = pool.async_allocate(SIZE_1MIB).await.unwrap();
    assert_eq!(buffer.len(), SIZE_1MIB);
}

#[tokio::test]
async fn test_async_allocation_waiting() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = test_pool_with_max(SIZE_64MIB);

    let buffer = pool.async_allocate(SIZE_64MIB).await.unwrap();

    let pool_clone = pool.clone();

    let handle = tokio::spawn(async move { pool_clone.async_allocate(SIZE_1MIB).await });

    tokio::time::sleep(Duration::from_millis(10)).await;

    drop(buffer);

    let result = timeout(Duration::from_secs(1), handle).await;
    assert!(result.is_ok());
    let buffer = result.unwrap().unwrap().unwrap();
    assert_eq!(buffer.len(), SIZE_1MIB);
}

#[tokio::test]
async fn test_pool_stats() {
    let pool = test_pool_with_max(SIZE_64MIB * 2);

    assert_eq!(pool.allocated_memory(), 0);
    assert_eq!(pool.max_memory(), SIZE_64MIB * 2);

    let _buffer = pool.async_allocate(SIZE_1MIB).await.unwrap();
    assert_eq!(pool.allocated_memory(), SIZE_64MIB);
}

#[test]
fn test_buffer_write_read() {
    let pool = test_pool();
    let mut buffer = pool.allocate(SIZE_1MIB).unwrap();

    for (i, byte) in buffer.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }

    for (i, byte) in buffer.iter().enumerate() {
        assert_eq!(*byte, (i % 256) as u8);
    }
}

#[test]
fn test_multiple_pools() {
    let pool1 = test_pool_with_max(SIZE_64MIB);
    let pool2 = test_pool_with_max(SIZE_64MIB);

    let b1 = pool1.allocate(SIZE_64MIB).unwrap();
    let b2 = pool2.allocate(SIZE_64MIB).unwrap();

    assert_eq!(b1.len(), SIZE_64MIB);
    assert_eq!(b2.len(), SIZE_64MIB);
}

#[test]
fn test_lazy_merge_keeps_nodes_below_watermark() {
    // High watermarks: merging on free is always deferred.
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .merge_watermarks([64, 16, 4, 0])
        .build();

    let buffer = pool.allocate(SIZE_1MIB).unwrap();
    // Splitting 64MiB down to 1MiB leaves 3 free nodes at each level.
    assert_eq!(pool.free_counts(), [3, 3, 3, 0]);
    assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);

    drop(buffer);
    // Lazy merge: the freed node stays at level 0 instead of merging
    // back up to the root; its parent is tracked as a pending quad.
    assert_eq!(pool.free_counts(), [4, 3, 3, 0]);
    assert_eq!(pool.pending_counts(), [0, 1, 0, 0]);

    // Same-size reallocation is served without any split, and the
    // broken quad leaves the pending list.
    let _buffer = pool.allocate(SIZE_1MIB).unwrap();
    assert_eq!(pool.free_counts(), [3, 3, 3, 0]);
    assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);
}

#[test]
fn test_zero_watermarks_restore_eager_merge() {
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .merge_watermarks([0, 0, 0, 0])
        .build();

    let buffer = pool.allocate(SIZE_1MIB).unwrap();
    assert_eq!(pool.free_counts(), [3, 3, 3, 0]);

    drop(buffer);
    // Eager merge: everything coalesces back to the 64MiB root.
    assert_eq!(pool.free_counts(), [0, 0, 0, 1]);
}

#[test]
fn test_demand_driven_coalescing_on_alloc() {
    // Max memory allows two blocks, so this also verifies that deferred
    // quads are coalesced and reused instead of growing the pool.
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB * 2)
        .merge_watermarks([64, 16, 4, 0])
        .build();

    let buffer = pool.allocate(SIZE_1MIB).unwrap();
    drop(buffer);
    assert_eq!(pool.free_counts(), [4, 3, 3, 0]);
    assert_eq!(pool.pending_counts(), [0, 1, 0, 0]);
    assert_eq!(pool.allocated_memory(), SIZE_64MIB);

    // No free 64MiB node exists; demand-driven coalescing must rebuild
    // the root from the pending quads (cascading up all three levels)
    // rather than allocating a second block.
    let big = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(big.len(), SIZE_64MIB);
    assert_eq!(pool.allocated_memory(), SIZE_64MIB);
    assert_eq!(pool.free_counts(), [0, 0, 0, 0]);
    assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);
}

#[test]
fn test_coalescing_is_minimal_and_on_demand() {
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .merge_watermarks([64, 16, 4, 0])
        .build();

    // Occupy four full level-1 quads with 1MiB buffers and drain level 2,
    // so that only deferred level-0 nodes remain after dropping.
    let small: Vec<_> = (0..16).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
    let _large: Vec<_> = (0..3)
        .map(|_| pool.allocate(LEVEL_SIZES[2]).unwrap())
        .collect();
    assert_eq!(pool.free_counts(), [0, 0, 0, 0]);

    drop(small);
    // Lazy merge keeps all 16 freed nodes at level 0; the 4 complete
    // quads are tracked on the pending list.
    assert_eq!(pool.free_counts(), [16, 0, 0, 0]);
    assert_eq!(pool.pending_counts(), [0, 4, 0, 0]);

    // A 4MiB allocation cannot be served directly; demand-driven
    // coalescing merges exactly ONE pending quad (early exit) and keeps
    // the rest deferred at level 0 for future small allocations.
    let b = pool.allocate(LEVEL_SIZES[1]).unwrap();
    assert_eq!(b.len(), LEVEL_SIZES[1]);
    assert_eq!(pool.free_counts(), [12, 0, 0, 0]);
    assert_eq!(pool.pending_counts(), [0, 3, 0, 0]);
    assert_eq!(pool.allocated_memory(), SIZE_64MIB);

    // Each further 4MiB demand consumes exactly one more pending quad.
    let b2 = pool.allocate(LEVEL_SIZES[1]).unwrap();
    assert_eq!(b2.len(), LEVEL_SIZES[1]);
    assert_eq!(pool.free_counts(), [8, 0, 0, 0]);
    assert_eq!(pool.pending_counts(), [0, 2, 0, 0]);
}

#[tokio::test]
async fn test_waiter_triggers_merge_on_free() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .merge_watermarks([64, 16, 4, 0])
        .build();

    let buffers: Vec<_> = (0..4)
        .map(|_| pool.allocate(LEVEL_SIZES[2]).unwrap())
        .collect();

    let pool_clone = pool.clone();
    let handle = tokio::spawn(async move { pool_clone.async_allocate(SIZE_64MIB).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Freeing with a 64MiB waiter present must force merging despite the
    // high watermarks (demand-driven path in should_merge).
    drop(buffers);

    let result = timeout(Duration::from_secs(1), handle).await;
    let buffer = result.unwrap().unwrap().unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[test]
fn test_lazy_merge_randomized_stress() {
    // Random alloc/free mix across all levels; debug_asserts in
    // try_merge/coalesce_pending/demote_pending_parent verify the
    // pending-list invariant on every transition.
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB * 4)
        .merge_watermarks([4, 2, 1, 0])
        .build();

    let mut held: Vec<Buffer> = Vec::new();
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as usize
    };

    for _ in 0..10_000 {
        let r = next();
        if r % 100 < 60 || held.is_empty() {
            let level = [0, 0, 0, 1, 1, 2, 3][next() % 7];
            if let Ok(buffer) = pool.allocate(LEVEL_SIZES[level]) {
                held.push(buffer);
            }
        } else {
            held.swap_remove(next() % held.len());
        }
    }

    drop(held);
    // Full drain: everything must be reachable again via coalescing.
    let all: Vec<_> = (0..4).map(|_| pool.allocate(SIZE_64MIB).unwrap()).collect();
    assert_eq!(all.len(), 4);
    assert_eq!(pool.free_counts(), [0, 0, 0, 0]);
    assert_eq!(pool.pending_counts(), [0, 0, 0, 0]);
}

#[test]
fn test_concurrent_allocation_and_growth() {
    // Many threads allocate and free concurrently, forcing concurrent
    // pool growth (block creation happens outside the pool mutex).
    let pool = test_pool_with_max(SIZE_64MIB * 16);
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                let mut held = Vec::new();
                for i in 0..500 {
                    let level = (t + i) % 3;
                    match pool.allocate(LEVEL_SIZES[level]) {
                        Ok(buffer) => held.push(buffer),
                        Err(e) => assert_eq!(e.kind(), ErrorKind::OutOfMemory),
                    }
                    if i % 3 == 0 {
                        held.clear();
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    // Budget must never be exceeded, even with racing growers.
    assert!(pool.allocated_memory() <= pool.max_memory());

    // With everything freed, the pool must be fully recoverable.
    let blocks = pool.allocated_memory() / SIZE_64MIB;
    let all: Vec<_> = (0..blocks)
        .map(|_| pool.allocate(SIZE_64MIB).unwrap())
        .collect();
    assert_eq!(all.len(), blocks);
}

#[tokio::test]
async fn test_handoff_serves_multiple_waiters_from_one_free() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = test_pool_with_max(SIZE_64MIB);
    let held = pool.allocate(SIZE_64MIB).unwrap();

    // Queue 4 waiters for 16MiB each; the pool is exhausted.
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(LEVEL_SIZES[2]).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // A single free must hand capacity to ALL of them (the old
    // notify-one design would wake only one and strand the rest).
    drop(held);

    for handle in handles {
        let buffer = timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter starved: free did not serve all waiters")
            .unwrap()
            .unwrap();
        assert_eq!(buffer.len(), LEVEL_SIZES[2]);
    }
}

#[tokio::test]
async fn test_handoff_cancelled_waiter_returns_buffer() {
    use std::time::Duration;

    let pool = test_pool_with_max(SIZE_64MIB);
    let held = pool.allocate(SIZE_64MIB).unwrap();

    // Register a waiter, then cancel it before capacity arrives.
    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(SIZE_1MIB).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    waiter.abort();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The free hands a buffer to the cancelled waiter; the failed send
    // must reclaim it into the pool instead of leaking it.
    drop(held);

    // The full 64MiB must be recoverable again.
    let buffer = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[tokio::test]
async fn test_handoff_mixed_sizes_smallest_first() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = test_pool_with_max(SIZE_64MIB);
    let held = pool.allocate(SIZE_64MIB).unwrap();

    // One large and several small waiters; one 64MiB free fits them all.
    let big = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(LEVEL_SIZES[2]).await })
    };
    let smalls: Vec<_> = (0..3)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_1MIB).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(held);

    for handle in smalls.into_iter().chain(std::iter::once(big)) {
        let buffer = timeout(Duration::from_secs(1), handle)
            .await
            .expect("waiter starved")
            .unwrap()
            .unwrap();
        assert!(!buffer.is_empty());
    }
}

#[tokio::test]
async fn test_starving_waiter_reservation_drains_and_blocks_theft() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .starvation_timeout(Duration::from_millis(10))
        .build();

    // Fill the pool with 64 small buffers, then queue a 64MiB waiter.
    let mut held: Vec<_> = (0..64).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The first free after the timeout activates the reservation.
    held.pop();
    // From now on, freed capacity is absorbed by the reservation:
    // fast-path allocations must NOT be able to steal it. (Without the
    // reservation this allocate would succeed and the waiter would
    // starve for as long as the churn continues.)
    assert!(pool.allocate(SIZE_1MIB).is_err());

    // Drain the rest; every free flows to the reserved waiter.
    held.clear();
    let buffer = timeout(Duration::from_secs(1), waiter)
        .await
        .expect("64MiB waiter starved despite reservation")
        .unwrap()
        .unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[tokio::test]
async fn test_starving_waiter_priority_over_later_small_waiters() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .starvation_timeout(Duration::from_millis(10))
        .build();

    let held = pool.allocate(SIZE_64MIB).unwrap();

    // A large waiter queues first and ages past the timeout...
    let big = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ...then small waiters arrive. Under plain smallest-first they
    // would carve up the freed 64MiB and strand the large waiter.
    let smalls: Vec<_> = (0..3)
        .map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move { pool.async_allocate(SIZE_1MIB).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(held);

    // The aged large waiter must win the freed 64MiB.
    let big_buffer = timeout(Duration::from_secs(1), big)
        .await
        .expect("aged 64MiB waiter lost to later small waiters")
        .unwrap()
        .unwrap();
    assert_eq!(big_buffer.len(), SIZE_64MIB);

    // Releasing it then serves the small waiters normally.
    drop(big_buffer);
    for handle in smalls {
        let buffer = timeout(Duration::from_secs(1), handle)
            .await
            .expect("small waiter starved")
            .unwrap()
            .unwrap();
        assert_eq!(buffer.len(), SIZE_1MIB);
    }
}

#[tokio::test]
async fn test_cancelled_reserved_waiter_releases_absorbed_capacity() {
    use std::time::Duration;

    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .starvation_timeout(Duration::from_millis(10))
        .build();

    let mut held: Vec<_> = (0..64).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Activate the reservation and drain part of the pool into it.
    for _ in 0..16 {
        held.pop();
    }
    assert!(pool.allocate(SIZE_1MIB).is_err());

    // Cancel the reserved waiter; the next free detects the cancellation
    // and releases all absorbed capacity back to the pool.
    waiter.abort();
    tokio::time::sleep(Duration::from_millis(20)).await;
    held.pop();

    let reclaimed = pool.allocate(SIZE_1MIB);
    assert!(
        reclaimed.is_ok(),
        "absorbed capacity leaked after waiter cancellation"
    );
    drop(reclaimed);

    // Full recovery: everything must merge back into one 64MiB node.
    held.clear();
    let buffer = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_starvation_reservation_randomized_stress() {
    use std::time::Duration;

    // Small pool + tiny starvation timeout + aggressive request timeouts:
    // exercises reservation activation, drain, priority completion and
    // cancellation-release concurrently. The debug_asserts in the merge,
    // pending and reservation paths check the invariants throughout.
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB * 2)
        .starvation_timeout(Duration::from_millis(5))
        .build();

    let tasks: Vec<_> = (0..8u64)
        .map(|t| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut rng: u64 = t * 7919 + 12345;
                let mut next = move || {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    (rng >> 33) as usize
                };
                const SIZES: [usize; 9] = [
                    64 * 1024,
                    64 * 1024,
                    256 * 1024,
                    LEVEL_SIZES[0],
                    LEVEL_SIZES[0],
                    LEVEL_SIZES[1],
                    LEVEL_SIZES[1],
                    LEVEL_SIZES[2],
                    LEVEL_SIZES[3],
                ];
                for _ in 0..200 {
                    let size = SIZES[next() % SIZES.len()];
                    let wait_ms = (next() % 25) as u64;
                    let result = tokio::time::timeout(
                        Duration::from_millis(wait_ms),
                        pool.async_allocate(size),
                    )
                    .await;
                    if let Ok(Ok(buffer)) = result {
                        if next() % 3 == 0 {
                            tokio::task::yield_now().await;
                        }
                        drop(buffer);
                    }
                    // Timed-out requests exercise waiter/reservation
                    // cancellation.
                }
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    // Full drain: no capacity may be stranded in reservations or lists.
    let a = pool.allocate(SIZE_64MIB).unwrap();
    let b = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(a.len() + b.len(), 2 * SIZE_64MIB);
}

#[test]
fn test_chunks_share_slab() {
    const KIB64: usize = 64 * 1024;
    // Exact slab accounting: keep chunks out of the per-thread cache.
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .thread_cache(false)
        .build();

    // 16 x 64KiB fit in one slab (one 1MiB buddy leaf).
    let chunks: Vec<_> = (0..16).map(|_| pool.allocate(KIB64).unwrap()).collect();
    assert_eq!(pool.allocated_memory(), SIZE_64MIB);

    let base = chunks[0].as_ptr() as usize & !(SIZE_1MIB - 1);
    let mut addrs: Vec<usize> = chunks.iter().map(|c| c.as_ptr() as usize).collect();
    addrs.sort_unstable();
    addrs.dedup();
    assert_eq!(addrs.len(), 16, "chunks must not overlap");
    for &addr in &addrs {
        assert_eq!(addr & !(SIZE_1MIB - 1), base, "chunks must share one slab");
        assert_eq!(addr % KIB64, 0, "chunks must be 64KiB-aligned");
    }
    assert_eq!(pool.slab_free_counts(), [0, 0, 0]);

    // The 17th chunk opens a second slab.
    let extra = pool.allocate(KIB64).unwrap();
    assert_ne!(extra.as_ptr() as usize & !(SIZE_1MIB - 1), base);
    assert_eq!(pool.slab_free_counts(), [0, 15, 0]);
}

#[test]
fn test_slab_release_beyond_watermark() {
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .slab_empty_watermark(0)
        .thread_cache(false)
        .build();

    let chunk = pool.allocate(64 * 1024).unwrap();
    drop(chunk);

    // Watermark 0: the empty slab returns to the buddy pool at once,
    // and the whole 64MiB is recoverable.
    assert_eq!(pool.slab_free_counts(), [0, 0, 0]);
    let buffer = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[test]
fn test_slab_watermark_caches_empty_slab() {
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .slab_empty_watermark(1)
        .thread_cache(false)
        .build();

    let chunk = pool.allocate(64 * 1024).unwrap();
    let buddy_free = pool.free_counts();
    drop(chunk);

    // The empty slab stays cached: buddy free lists are untouched and
    // all 16 chunks are available for reuse without a refill.
    assert_eq!(pool.slab_free_counts(), [0, 16, 0]);
    assert_eq!(pool.free_counts(), buddy_free);

    let chunk = pool.allocate(64 * 1024).unwrap();
    assert_eq!(pool.slab_free_counts(), [0, 15, 0]);
    assert_eq!(pool.free_counts(), buddy_free);
    drop(chunk);
}

#[test]
fn test_buddy_miss_reclaims_cached_slabs() {
    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .slab_empty_watermark(8)
        .thread_cache(false)
        .build();

    let chunk = pool.allocate(64 * 1024).unwrap();
    drop(chunk);
    assert_eq!(pool.slab_free_counts(), [0, 16, 0]);

    // The 64MiB allocation misses the buddy free lists; the reclaim
    // hook must release the cached empty slab instead of failing.
    let buffer = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
    assert_eq!(pool.slab_free_counts(), [0, 0, 0]);
}

#[tokio::test]
async fn test_demand_releases_cached_slab_for_waiter() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
        .max_memory(SIZE_64MIB)
        .slab_empty_watermark(8)
        .build();

    // 63 x 1MiB + one slab (16 x 64KiB) fill the whole pool.
    let bufs: Vec<_> = (0..63).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
    let chunks: Vec<_> = (0..16).map(|_| pool.allocate(64 * 1024).unwrap()).collect();

    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(bufs);
    // The last MiB is held by the slab. Freeing its chunks must release
    // the (now empty) slab immediately — demand overrides the watermark —
    // so the waiter can complete.
    drop(chunks);

    let buffer = timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter starved: cached empty slab was not released")
        .unwrap()
        .unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[tokio::test]
async fn test_async_small_allocation_waits_and_completes() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = test_pool_with_max(SIZE_64MIB);
    let held = pool.allocate(SIZE_64MIB).unwrap();

    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(64 * 1024).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(held);

    let buffer = timeout(Duration::from_secs(1), waiter)
        .await
        .expect("small waiter starved")
        .unwrap()
        .unwrap();
    assert_eq!(buffer.len(), 64 * 1024);
}

#[test]
fn test_chunk_write_read_and_drop() {
    let pool = test_pool();
    let mut chunk = pool.allocate(100).unwrap();
    assert_eq!(chunk.capacity(), 16 * 1024);

    chunk.set_len(0);
    chunk.extend_from_slice(&[0xAB; 128]).unwrap();
    assert_eq!(chunk.len(), 128);
    assert!(chunk.iter().all(|&b| b == 0xAB));

    // Overflowing the (now smaller) capacity must fail cleanly.
    chunk.set_len(16 * 1024);
    assert!(chunk.extend_from_slice(&[0u8; 1]).is_err());
}

#[test]
fn test_stats_consistency() {
    let pool = test_pool_with_max(SIZE_64MIB);
    assert_eq!(pool.allocated_memory(), 0);
    assert_eq!(pool.free_counts(), [0, 0, 0, 0]);

    let buffer = pool.allocate(SIZE_1MIB).unwrap();
    assert_eq!(pool.allocated_memory(), SIZE_64MIB);
    // Atomic counts must mirror the free lists exactly when quiescent.
    assert_eq!(pool.free_counts(), [3, 3, 3, 0]);

    drop(buffer);
    let counts = pool.free_counts();
    let free_bytes: usize = counts
        .iter()
        .zip(LEVEL_SIZES.iter())
        .map(|(c, s)| c * s)
        .sum();
    assert_eq!(free_bytes, SIZE_64MIB);
}

#[test]
fn test_thread_cache_roundtrip() {
    let pool = test_pool_with_max(SIZE_64MIB);

    // Freed chunks go to the thread cache and come back on allocation.
    let addr = {
        let chunk = pool.allocate(64 * 1024).unwrap();
        chunk.as_ptr() as usize
    };
    let chunk = pool.allocate(64 * 1024).unwrap();
    assert_eq!(chunk.as_ptr() as usize, addr, "must reuse the cached chunk");
    drop(chunk);

    // Overflow: freeing more chunks than the magazine holds must not
    // lose any (all chunks remain allocatable).
    let chunks: Vec<_> = (0..64).map(|_| pool.allocate(64 * 1024).unwrap()).collect();
    drop(chunks);
    let chunks: Vec<_> = (0..64).map(|_| pool.allocate(64 * 1024).unwrap()).collect();
    assert_eq!(chunks.len(), 64);
}

#[test]
fn test_thread_cache_flushes_on_buddy_miss() {
    // A chunk cached on this thread keeps its slab non-empty; a buddy
    // allocation of the whole pool must flush the cache to succeed.
    let pool = test_pool_with_max(SIZE_64MIB);
    let chunk = pool.allocate(64 * 1024).unwrap();
    drop(chunk); // now cached in this thread's magazine

    let buffer = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[test]
fn test_thread_cache_cross_thread_free() {
    // Chunks allocated here, freed on another thread (its cache), then
    // the pool must still be fully recoverable from this thread.
    let pool = test_pool_with_max(SIZE_64MIB);
    let chunks: Vec<_> = (0..16).map(|_| pool.allocate(64 * 1024).unwrap()).collect();

    let pool2 = Arc::clone(&pool);
    std::thread::spawn(move || drop(chunks)).join().unwrap();
    drop(pool2);

    // The other thread exited: its cache was flushed on thread exit.
    let buffer = pool.allocate(SIZE_64MIB).unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[test]
fn buffer_in_late_thread_local_drop_returns_after_cache_destruction() {
    use std::cell::RefCell;

    thread_local! {
        static LATE_BUFFER: RefCell<Option<Buffer>> = const { RefCell::new(None) };
    }
    let pool = test_pool_with_max(SIZE_64MIB);
    let worker_pool = Arc::clone(&pool);
    std::thread::spawn(move || {
        // Initialize this slot before allocation initializes the cache registry.
        // TLS destructors run in reverse order, so the buffer is returned only
        // after its cache registry is gone.
        LATE_BUFFER.with(|slot| {
            *slot.borrow_mut() = Some(worker_pool.allocate(64 * 1024).unwrap());
        });
    })
    .join()
    .unwrap();
    assert_eq!(pool.allocate(SIZE_64MIB).unwrap().len(), SIZE_64MIB);
}

#[tokio::test]
async fn test_thread_cache_demand_flush_unblocks_waiter() {
    use std::time::Duration;
    use tokio::time::timeout;

    let pool = test_pool_with_max(SIZE_64MIB);
    // Fill the pool: 63 x 1MiB + one full slab.
    let bufs: Vec<_> = (0..63).map(|_| pool.allocate(SIZE_1MIB).unwrap()).collect();
    let chunks: Vec<_> = (0..16).map(|_| pool.allocate(64 * 1024).unwrap()).collect();

    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.async_allocate(SIZE_64MIB).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(bufs);
    // Demand is signalled: these frees must bypass the thread cache so
    // the slab can be released to the waiter.
    drop(chunks);

    let buffer = timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter starved: thread cache held the last slab")
        .unwrap()
        .unwrap();
    assert_eq!(buffer.len(), SIZE_64MIB);
}

#[test]
fn test_clone_pool() {
    let pool = test_pool_with_max(SIZE_64MIB);

    let pool_clone = pool.clone();

    let b1 = pool.allocate(SIZE_64MIB).unwrap();

    let result = pool_clone.allocate(SIZE_1MIB);
    assert!(result.is_err());

    drop(b1);

    let _b2 = pool_clone.allocate(SIZE_1MIB).unwrap();
}

#[test]
fn cached_slabs_and_magazines_do_not_keep_pool_alive() {
    for thread_cache in [false, true] {
        let pool = BufferPoolBuilder::new(Arc::new(EmptyDevices))
            .thread_cache(thread_cache)
            .build();
        let weak = Arc::downgrade(&pool);
        for size in crate::slab::SLAB_CLASS_SIZES {
            drop(pool.allocate(size).unwrap());
        }
        assert_eq!(pool.allocated_memory(), SIZE_64MIB);
        drop(pool);
        assert!(weak.upgrade().is_none(), "cached memory retained its pool");
    }
}

#[test]
fn live_chunk_keeps_pool_alive_until_final_drop() {
    let pool = test_pool();
    let weak = Arc::downgrade(&pool);
    let mut buffer = pool.allocate(16 * 1024).unwrap();
    drop(pool);
    assert!(weak.upgrade().is_some());
    buffer[0] = 42;
    assert_eq!(buffer[0], 42);
    drop(buffer);
    assert!(weak.upgrade().is_none());
}

#[test]
fn registrations_drop_before_their_backing_memory() {
    use crate::{MemoryKey, Registration};
    use std::sync::Weak;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct TrackedRegistration {
        memory: Weak<AlignedMemory>,
        drops: Arc<AtomicUsize>,
    }
    impl Registration for TrackedRegistration {
        fn memory_key(&self) -> MemoryKey {
            MemoryKey::default()
        }
    }
    impl Drop for TrackedRegistration {
        fn drop(&mut self) {
            assert!(
                self.memory.upgrade().is_some(),
                "deregistered after freeing memory"
            );
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[derive(Debug)]
    struct TrackedDevices(Arc<AtomicUsize>);
    // SAFETY: registrations retain only weak references and never access bytes.
    unsafe impl Devices for TrackedDevices {
        fn len(&self) -> usize {
            1
        }
        fn register(&self, memory: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>> {
            Ok(vec![Box::new(TrackedRegistration {
                memory: Arc::downgrade(memory),
                drops: Arc::clone(&self.0),
            })])
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let pool = BufferPoolBuilder::new(Arc::new(TrackedDevices(Arc::clone(&drops)))).build();
    drop(pool.allocate(16 * 1024).unwrap());
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(pool);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_registration_returns_reserved_budget() {
    #[derive(Debug)]
    struct FailingDevices;
    // SAFETY: registration always fails without retaining or accessing memory.
    unsafe impl Devices for FailingDevices {
        fn len(&self) -> usize {
            1
        }
        fn register(&self, _: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn crate::Registration>>> {
            Err(Error::other("registration failed"))
        }
    }
    let pool = BufferPoolBuilder::new(Arc::new(FailingDevices))
        .max_memory(SIZE_64MIB)
        .build();
    for _ in 0..2 {
        assert_eq!(
            pool.allocate(SIZE_1MIB).unwrap_err().to_string(),
            "registration failed"
        );
        assert_eq!(pool.allocated_memory(), 0);
    }
}

#[test]
fn registration_keys_remain_readable_during_allocator_mutation() {
    use crate::{DeviceIndex, MemoryKey, Registration};

    #[derive(Debug)]
    struct FixedRegistration;
    impl Registration for FixedRegistration {
        fn memory_key(&self) -> MemoryKey {
            MemoryKey { lkey: 7, rkey: 11 }
        }
    }
    #[derive(Debug)]
    struct RegisteredDevices;
    // SAFETY: registrations contain only constant keys and never access memory.
    unsafe impl Devices for RegisteredDevices {
        fn len(&self) -> usize {
            1
        }
        fn register(&self, _: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>> {
            Ok(vec![Box::new(FixedRegistration)])
        }
    }

    let pool = BufferPoolBuilder::new(Arc::new(RegisteredDevices))
        .max_memory(SIZE_64MIB)
        .build();
    let held = pool.allocate(SIZE_1MIB).unwrap();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for _ in 0..10_000 {
                drop(pool.allocate(SIZE_1MIB).unwrap());
            }
        });
        for _ in 0..10_000 {
            let key = held.memory_key(&DeviceIndex::default()).unwrap();
            assert_eq!((key.lkey, key.rkey), (7, 11));
        }
    });
}
