use super::*;

use crate::{Context, SocketPoolConfig};

const SOURCE_ID: u64 = 7;
const CONNECTION_ID: u64 = 41;
const BYTE_LIMIT: usize = 16 * 1024;

fn context() -> Context {
    Context::create(&SocketPoolConfig {
        rdma: None,
        ..Default::default()
    })
    .unwrap()
}

fn install_source(
    large: &LargeTransfers,
    state: &State,
    id: u64,
    request_id: Option<u64>,
    deadline: Option<Instant>,
) {
    let mut buffer = state.buffer_pool.allocate(BYTE_LIMIT).unwrap();
    assert_eq!(buffer.capacity(), BYTE_LIMIT);
    buffer.set_len(6);
    buffer.copy_from_slice(b"source");
    let permit = large
        .send_bytes
        .clone()
        .try_acquire_many_owned(BYTE_LIMIT as u32)
        .unwrap();
    assert!(
        large
            .sources
            .lock()
            .unwrap()
            .active
            .insert(
                id,
                Source {
                    _buffer: buffer,
                    _permit: permit,
                    request_id,
                    deadline,
                    cleanup_at: Instant::now() + TRANSFER_TIMEOUT,
                },
            )
            .is_none()
    );
}

#[tokio::test]
async fn valid_source_finishes_once_and_repeated_confirmation_is_idempotent() {
    let ctx = context();
    let large = LargeTransfers::new(BYTE_LIMIT);
    let (msgid, receiver) = ctx.state.waiter.alloc(Duration::from_secs(30));
    ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
    install_source(
        &large,
        &ctx.state,
        SOURCE_ID,
        Some(msgid),
        Some(Instant::now() + Duration::from_secs(30)),
    );
    assert_eq!(large.send_bytes.available_permits(), 0);
    assert!(large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
    assert!(large.sources.lock().unwrap().active.is_empty());

    // Once the original READ was confirmed, the result is independent of the
    // later business waiter lifetime. A retransmitted finish cannot release
    // memory or return its budget twice.
    drop(receiver);
    assert!(large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
    assert_eq!(large.sources.lock().unwrap().finished.len(), 1);
}

#[tokio::test]
async fn cancelled_request_keeps_source_until_peer_finishes_reading() {
    let ctx = context();
    let large = LargeTransfers::new(BYTE_LIMIT);
    let (msgid, receiver) = ctx.state.waiter.alloc(Duration::from_secs(30));
    ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
    install_source(&large, &ctx.state, SOURCE_ID, Some(msgid), None);
    drop(receiver);

    assert!(!ctx.state.waiter.pending_on_connection(msgid, CONNECTION_ID));
    assert_eq!(large.send_bytes.available_permits(), 0);
    assert_eq!(
        &large.sources.lock().unwrap().active[&SOURCE_ID]._buffer[..],
        b"source"
    );
    assert!(!large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
    assert!(!large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
}

#[tokio::test]
async fn source_deadline_and_unswept_waiter_deadline_both_reject_expired_payloads() {
    let ctx = context();
    for source_expired in [true, false] {
        let large = LargeTransfers::new(BYTE_LIMIT);
        let waiter_timeout = if source_expired {
            Duration::from_secs(30)
        } else {
            Duration::ZERO
        };
        let (msgid, receiver) = ctx.state.waiter.alloc(waiter_timeout);
        ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
        let deadline = if source_expired {
            Instant::now() - Duration::from_secs(1)
        } else {
            Instant::now() + Duration::from_secs(30)
        };
        install_source(&large, &ctx.state, SOURCE_ID, Some(msgid), Some(deadline));

        // Expiry only invalidates acceptance. It is never completion evidence
        // for the source buffer, even before the periodic waiter sweep runs.
        assert_eq!(large.send_bytes.available_permits(), 0);
        assert_eq!(large.sources.lock().unwrap().active.len(), 1);
        assert!(!large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
        drop(receiver);
    }
}

#[tokio::test]
async fn source_confirmation_requires_matching_waiter_connection_and_health() {
    let ctx = context();
    for (connection_id, healthy) in [(CONNECTION_ID + 1, true), (CONNECTION_ID, false)] {
        let large = LargeTransfers::new(BYTE_LIMIT);
        let (msgid, receiver) = ctx.state.waiter.alloc(Duration::from_secs(30));
        ctx.state.waiter.bind_connection(msgid, CONNECTION_ID);
        install_source(&large, &ctx.state, SOURCE_ID, Some(msgid), None);
        assert!(!large.finish(SOURCE_ID, &ctx.state, connection_id, healthy));
        assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
        assert!(ctx.state.waiter.pending_on_connection(msgid, CONNECTION_ID));
        drop(receiver);
    }
}

#[tokio::test]
async fn response_sources_do_not_require_a_request_waiter_and_unknown_ids_do_not_release_them() {
    let ctx = context();
    let large = LargeTransfers::new(BYTE_LIMIT);
    install_source(&large, &ctx.state, SOURCE_ID, None, None);
    assert!(!large.finish(SOURCE_ID + 1, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), 0);
    assert!(large.finish(SOURCE_ID, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), BYTE_LIMIT);
}

#[tokio::test]
async fn connection_close_clears_receivers_but_preserves_unconfirmed_sources() {
    let ctx = context();
    let large = LargeTransfers::new(BYTE_LIMIT);
    let send_budget = large.send_bytes.clone();
    install_source(&large, &ctx.state, SOURCE_ID, None, None);
    large.receivers.lock().unwrap().active.insert(11);
    large.receivers.lock().unwrap().active.insert(12);

    large.close();
    large.close();
    assert!(large.send_bytes.is_closed());
    assert!(large.recv_bytes.is_closed());
    assert!(large.receivers.lock().unwrap().active.is_empty());
    assert_eq!(send_budget.available_permits(), 0);
    assert_eq!(
        &large.sources.lock().unwrap().active[&SOURCE_ID]._buffer[..],
        b"source"
    );

    // The socket drops this owner only after destroying its provider QP.
    drop(large);
    assert_eq!(send_budget.available_permits(), BYTE_LIMIT);
}

#[tokio::test]
async fn finished_confirmation_cache_is_bounded_without_releasing_unrelated_sources() {
    let ctx = context();
    let large = LargeTransfers::new(BYTE_LIMIT);
    for id in 1..=FINISHED_CACHE as u64 + 1 {
        install_source(&large, &ctx.state, id, None, None);
        assert!(large.finish(id, &ctx.state, CONNECTION_ID, true));
    }
    assert_eq!(large.sources.lock().unwrap().finished.len(), FINISHED_CACHE);
    install_source(&large, &ctx.state, 999, None, None);
    assert!(!large.finish(1, &ctx.state, CONNECTION_ID, true));
    assert!(large.finish(2, &ctx.state, CONNECTION_ID, true));
    assert_eq!(large.send_bytes.available_permits(), 0);
    assert_eq!(large.sources.lock().unwrap().active.len(), 1);
}

#[test]
fn seen_merges_disordered_ids_and_permanently_rejects_replays() {
    let mut seen = Seen::default();
    for id in [4, 2, 7, 3, 1, 6, 5] {
        seen.insert(id).unwrap();
    }
    assert_eq!(seen.ranges, BTreeMap::from([(1, 7)]));
    for id in 1..=7 {
        let error = seen.insert(id).unwrap_err();
        assert_eq!(error.kind, ErrorKind::RdmaRecvFailed);
    }
    assert_eq!(seen.ranges, BTreeMap::from([(1, 7)]));
    assert!(seen.insert(0).is_err());
}

#[test]
fn seen_handles_maximum_id_without_wraparound() {
    let mut seen = Seen::default();
    seen.insert(u64::MAX).unwrap();
    seen.insert(1).unwrap();
    seen.insert(u64::MAX - 2).unwrap();
    seen.insert(u64::MAX - 1).unwrap();
    assert_eq!(
        seen.ranges,
        BTreeMap::from([(1, 1), (u64::MAX - 2, u64::MAX)])
    );
    assert!(seen.insert(u64::MAX).is_err());
    assert!(seen.insert(0).is_err());
}

#[test]
fn seen_bounds_disorder_without_limiting_sequential_transfers() {
    let mut disordered = Seen::default();
    for id in (1..).step_by(2).take(MAX_TRANSFERS) {
        disordered.insert(id).unwrap();
    }
    assert!(disordered.insert((MAX_TRANSFERS * 2 + 1) as u64).is_err());

    let mut ordered = Seen::default();
    for id in 1..=(MAX_TRANSFERS * 10) as u64 {
        ordered.insert(id).unwrap();
    }
    assert_eq!(ordered.ranges.len(), 1);
    assert!(ordered.insert(1).is_err());
}

#[tokio::test]
async fn received_payload_keeps_its_budget_until_all_payload_views_are_dropped() {
    let ctx = context();
    let large = LargeTransfers::new(BYTE_LIMIT);
    let mut buffer = ctx.state.buffer_pool.allocate(BYTE_LIMIT).unwrap();
    buffer.set_len(7);
    buffer.copy_from_slice(b"payload");
    let permit = large
        .recv_bytes
        .clone()
        .try_acquire_many_owned(BYTE_LIMIT as u32)
        .unwrap();
    let bytes = Bytes::from_owner(ReceivedPayload {
        buffer,
        _permit: permit,
    });
    let cloned = bytes.clone();
    let slice = bytes.slice(1..4);
    assert_eq!(slice.as_ref(), b"ayl");
    assert_eq!(large.recv_bytes.available_permits(), 0);
    drop(bytes);
    drop(cloned);
    large.close();
    assert_eq!(large.recv_bytes.available_permits(), 0);
    assert_eq!(slice.as_ref(), b"ayl");
    drop(slice);
    assert_eq!(large.recv_bytes.available_permits(), BYTE_LIMIT);
}
