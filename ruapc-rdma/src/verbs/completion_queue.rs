//! [`CompletionQueue`]: RAII wrapper for `ibv_cq`.

use std::{
    os::raw::c_int,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use super::{comp_channel::CompChannel, context::Context};
use crate::{ErrorKind, Result, WRID, ibv_wc};

/// Reusable stack storage for polling authenticated work completions.
/// Entries cannot be changed while any completion from the batch is borrowed.
pub struct CompletionBatch<const N: usize> {
    entries: [ibv_wc; N],
}

impl<const N: usize> CompletionBatch<N> {
    pub fn new() -> Self {
        Self {
            entries: [ibv_wc::default(); N],
        }
    }

    pub const fn capacity(&self) -> usize {
        N
    }
}

impl<const N: usize> Default for CompletionBatch<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// A completion obtained directly from its CQ. Consuming this non-cloneable
/// proof in [`super::QueuePair::complete`] can release that QP's buffers.
/// Its immutable metadata alone never authorizes buffer reclamation.
///
/// ```compile_fail
/// use ruapc_rdma::{Completion, CompletionQueue, ibv_wc};
/// fn forge(cq: &CompletionQueue, wc: &ibv_wc) {
///     let _ = Completion { cq, wc };
/// }
/// ```
#[derive(Debug)]
pub struct Completion<'a> {
    cq: &'a CompletionQueue,
    wc: &'a ibv_wc,
}

impl<'a> Completion<'a> {
    #[inline]
    pub fn info(&self) -> &'a ibv_wc {
        self.wc
    }

    pub(super) fn belongs_to(&self, cq: &CompletionQueue, qp_num: u32, tag: u32) -> bool {
        ptr::eq(self.cq, cq) && self.wc.qp_num == qp_num && self.wc.wr_id.get_tag() == tag
    }
}

/// Iterator over one CQ poll. Neither the iterator nor its items can be cloned.
pub struct Completions<'a> {
    cq: &'a CompletionQueue,
    entries: std::slice::Iter<'a, ibv_wc>,
}

impl<'a> Iterator for Completions<'a> {
    type Item = Completion<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(|wc| Completion { cq: self.cq, wc })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl ExactSizeIterator for Completions<'_> {}

/// A tag is never reused during the CQ's lifetime, including after QP drop.
/// This makes a retained completion harmless even when a provider reuses a QPN.
struct ClaimedTags(Box<[AtomicU64]>);

impl ClaimedTags {
    fn new() -> Self {
        Self(
            (0..=(WRID::TAG_MAX as usize / 64))
                .map(|_| AtomicU64::new(0))
                .collect(),
        )
    }

    fn claim(&self, tag: u32) -> Result<()> {
        if tag > WRID::TAG_MAX {
            return Err(ErrorKind::InvalidQueuePairConfig.into());
        }
        let mask = 1u64 << (tag % 64);
        if self.0[tag as usize / 64].fetch_or(mask, Ordering::Relaxed) & mask != 0 {
            return Err(ErrorKind::InvalidQueuePairConfig.into());
        }
        Ok(())
    }
}

/// A completion queue (CQ).
///
/// Holds work completions from send/recv operations. Maintains shared
/// references to the [`Context`] and optional [`CompChannel`] to ensure
/// they outlive this CQ.
pub struct CompletionQueue {
    ptr: *mut crate::ibv_cq,
    /// Prevents the context from being closed while this CQ exists.
    _context: Arc<Context>,
    /// Prevents the completion channel from being destroyed while this CQ exists.
    _channel: Option<Arc<CompChannel>>,
    /// 512 KiB of setup-only tag history; completion validation needs no lookup.
    claimed_tags: ClaimedTags,
}

impl CompletionQueue {
    /// Creates a new completion queue.
    ///
    /// # Arguments
    ///
    /// * `context` - The device context
    /// * `cq_size` - Minimum number of CQ entries
    /// * `channel` - Optional completion channel for event notifications
    pub fn create(
        context: &Arc<Context>,
        cq_size: c_int,
        channel: Option<&Arc<CompChannel>>,
    ) -> Result<Arc<Self>> {
        let channel_ptr = channel.map(|c| c.as_ptr()).unwrap_or(ptr::null_mut());
        let ptr = unsafe {
            crate::ruapc_ibv_create_cq(
                context.as_ptr(),
                cq_size,
                ptr::null_mut(), // cq_context
                channel_ptr,
                0, // comp_vector
            )
        };
        if ptr.is_null() {
            return Err(ErrorKind::IBCreateCompQueueFail.with_errno());
        }
        Ok(Arc::new(Self {
            ptr,
            _context: Arc::clone(context),
            _channel: channel.cloned(),
            claimed_tags: ClaimedTags::new(),
        }))
    }

    /// Returns the raw CQ pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_cq {
        self.ptr
    }

    pub(super) fn context(&self) -> &Arc<Context> {
        &self._context
    }

    /// Requests notification for the next completion event.
    pub fn req_notify(&self, solicited_only: bool) -> Result<()> {
        let ret = unsafe { crate::ruapc_ibv_req_notify_cq(self.ptr, solicited_only as c_int) };
        if ret != 0 {
            return Err(ErrorKind::IBReqNotifyCompQueueFail.with_errno());
        }
        Ok(())
    }

    /// Polls the completion queue for work completions.
    ///
    /// Returns the number of completions written to the `wc` slice.
    pub fn poll(&self, wc: &mut [ibv_wc]) -> Result<usize> {
        let count = c_int::try_from(wc.len()).map_err(|_| ErrorKind::IBPollCompQueueFail)?;
        let ret = unsafe { crate::ruapc_ibv_poll_cq(self.ptr, count, wc.as_mut_ptr()) };
        if ret < 0 {
            return Err(ErrorKind::IBPollCompQueueFail.with_errno());
        }
        Ok(ret as usize)
    }

    /// Polls into reusable storage and lends one unforgeable proof per CQE.
    /// No allocation, reference-count update or synchronization is added to polling.
    pub fn poll_batch<'a, const N: usize>(
        &'a self,
        batch: &'a mut CompletionBatch<N>,
    ) -> Result<Completions<'a>> {
        let count = self.poll(&mut batch.entries)?;
        Ok(Completions {
            cq: self,
            entries: batch.entries[..count].iter(),
        })
    }

    pub(super) fn claim_tag(&self, tag: u32) -> Result<()> {
        self.claimed_tags.claim(tag)
    }

    /// Acknowledges CQ events received via [`CompChannel::get_event`].
    pub fn ack_events(&self, nevents: u32) {
        unsafe { crate::ruapc_ibv_ack_cq_events(self.ptr, nevents) };
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let _ = unsafe { crate::ruapc_ibv_destroy_cq(self.ptr) };
    }
}

impl std::fmt::Debug for CompletionQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionQueue")
            .field("ptr", &self.ptr)
            .finish()
    }
}

unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::test_utils::open_device;
    use crate::*;

    #[test]
    fn claimed_tags_never_reuse_a_connection_generation() {
        let tags = super::ClaimedTags::new();
        for tag in [0, 1, 63, 64, WRID::TAG_MAX] {
            tags.claim(tag).unwrap();
            assert_eq!(
                tags.claim(tag).unwrap_err().kind,
                ErrorKind::InvalidQueuePairConfig
            );
        }
        assert!(tags.claim(WRID::TAG_MAX + 1).is_err());
    }

    #[test]
    fn claimed_tags_are_exclusive_between_concurrent_registrars() {
        let tags = super::ClaimedTags::new();
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| tags.claim(42).is_ok()))
                .collect();
            assert_eq!(
                threads
                    .into_iter()
                    .filter_map(|thread| thread.join().ok())
                    .filter(|claimed| *claimed)
                    .count(),
                1
            );
        });
    }

    #[test]
    fn completion_requires_its_original_cq_qp_and_tag() {
        let dev = open_device();
        let cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let other_cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        // Only this module can construct a token from raw metadata. The public
        // API requires an actual provider completion, covered by the doctest.
        let wc = ibv_wc {
            qp_num: 42,
            wr_id: WRID::recv(7, 0),
            ..Default::default()
        };
        let completion = super::Completion { cq: &cq, wc: &wc };
        assert!(completion.belongs_to(&cq, 42, 7));
        assert!(!completion.belongs_to(&other_cq, 42, 7));
        assert!(!completion.belongs_to(&cq, 43, 7));
        assert!(!completion.belongs_to(&cq, 42, 8));
    }

    #[test]
    fn test_cq_create_without_channel() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        assert!(!cq.as_ptr().is_null());
    }

    #[test]
    fn test_cq_create_with_channel() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cc = CompChannel::create(&ctx).unwrap();
        let cq = CompletionQueue::create(&ctx, 16, Some(&cc)).unwrap();
        assert!(!cq.as_ptr().is_null());
    }

    #[test]
    fn test_cq_poll_empty() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        let mut wc = [ibv_wc::default(); 4];
        let n = cq.poll(&mut wc).unwrap();
        assert_eq!(n, 0);
        let mut batch = CompletionBatch::<4>::new();
        assert_eq!(cq.poll_batch(&mut batch).unwrap().len(), 0);
    }

    #[test]
    fn test_cq_req_notify() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cc = CompChannel::create(&ctx).unwrap();
        let cq = CompletionQueue::create(&ctx, 16, Some(&cc)).unwrap();
        cq.req_notify(false).unwrap();
    }
}
