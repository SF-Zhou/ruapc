//! [`CompletionQueue`]: RAII wrapper for `ibv_cq`.

use std::{
    os::raw::c_int,
    ptr,
    sync::{Arc, Mutex},
};

use super::{comp_channel::CompChannel, context::Context};
use crate::{ErrorKind, Result, ibv_wc};

mod routes;
pub use routes::CompletionRoute;
pub(super) use routes::RouteLease;
use routes::{RouteAllocator, WrIdLayout};

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

    /// Decodes the route slot using this completion's originating CQ.
    #[inline]
    pub fn slot(&self) -> usize {
        self.cq.layout.slot(self.wc.wr_id)
    }

    /// Decodes the work-queue sequence using this completion's originating CQ.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.cq.layout.sequence(self.wc.wr_id)
    }

    #[inline]
    pub(super) fn belongs_to(
        &self,
        cq: &CompletionQueue,
        qp_num: u32,
        route: CompletionRoute,
    ) -> bool {
        ptr::eq(self.cq, cq) && self.wc.qp_num == qp_num && route.contains(self.wc.wr_id)
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
    /// Fixed at CQ creation; changing it could reinterpret retained completions.
    layout: WrIdLayout,
    /// Setup-only slot allocation and reuse watermarks. Polling never locks it.
    routes: Mutex<RouteAllocator>,
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
        if cq_size <= 0 {
            return Err(ErrorKind::InvalidCompletionQueueConfig.into());
        }
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
        // Providers can round the requested capacity up. Use the returned
        // capacity both as the route limit and to choose the immutable layout.
        let actual_capacity = unsafe { (*ptr).cqe };
        if actual_capacity < cq_size {
            let _ = unsafe { crate::ruapc_ibv_destroy_cq(ptr) };
            return Err(crate::Error::new(
                ErrorKind::IBCreateCompQueueFail,
                format!("provider CQ capacity {actual_capacity} is below requested {cq_size}"),
            ));
        }
        let layout = WrIdLayout::new(actual_capacity as u32);
        Ok(Arc::new(Self {
            ptr,
            _context: Arc::clone(context),
            _channel: channel.cloned(),
            layout,
            routes: Mutex::new(RouteAllocator::new(layout)),
        }))
    }

    /// Returns the raw CQ pointer.
    pub fn as_ptr(&self) -> *mut crate::ibv_cq {
        self.ptr
    }

    /// Actual CQE capacity returned by the provider, at least the requested size.
    pub fn capacity(&self) -> u32 {
        self.layout.capacity()
    }

    /// Number of CQ-local route positions available to QPs.
    ///
    /// Equals [`Self::capacity`]. Exhausted sequence spaces retire positions;
    /// the layout is never resized or reinterpreted during this CQ's lifetime.
    pub fn route_capacity(&self) -> u32 {
        self.layout.capacity()
    }

    /// Work-queue sequence width after reserving two type bits and enough
    /// route bits for every position in [`Self::route_capacity`].
    pub fn sequence_bits(&self) -> u32 {
        self.layout.sequence_bits()
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

    pub(super) fn allocate_route(self: &Arc<Self>) -> Result<RouteLease> {
        let route = self.routes.lock().unwrap().allocate()?;
        Ok(RouteLease::new(Arc::clone(self), route))
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
            .field("capacity", &self.capacity())
            .field("sequence_bits", &self.sequence_bits())
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
    fn completion_requires_its_original_cq_qp_and_route() {
        let dev = open_device();
        let cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let other_cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let lease = cq.allocate_route().unwrap();
        let route = lease.route();
        let other_lease = cq.allocate_route().unwrap();
        // Only this module can construct a token from raw metadata. The public
        // API requires an actual provider completion, covered by the doctest.
        let wc = ibv_wc {
            qp_num: 42,
            wr_id: route.encode(WRType::Recv, lease.alloc_recv().unwrap()),
            ..Default::default()
        };
        let completion = super::Completion { cq: &cq, wc: &wc };
        assert_eq!(completion.slot(), route.slot());
        assert_eq!(completion.sequence(), route.first_sequence());
        assert!(completion.belongs_to(&cq, 42, route));
        assert!(!completion.belongs_to(&other_cq, 42, route));
        assert!(!completion.belongs_to(&cq, 43, route));
        assert!(!completion.belongs_to(&cq, 42, other_lease.route()));

        // A retained CQ-issued token must remain harmless when both the slot
        // and the provider's QPN have been reused by a replacement QP.
        drop(lease);
        let replacement = cq.allocate_route().unwrap();
        assert_eq!(replacement.route().slot(), route.slot());
        assert!(!completion.belongs_to(&cq, 42, replacement.route()));
    }

    #[test]
    fn lease_drop_preserves_both_directions_and_unused_incarnations() {
        let dev = open_device();
        let cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let lease = cq.allocate_route().unwrap();
        let first = lease.route();
        assert!(Arc::ptr_eq(lease.cq(), &cq));
        for expected in 0..5 {
            assert_eq!(lease.lock_send().unwrap().sequence(), expected);
        }
        assert_eq!(lease.alloc_recv().unwrap(), 0);
        drop(lease);

        let unused = cq.allocate_route().unwrap();
        assert_eq!(unused.route().slot(), first.slot());
        assert_eq!(unused.route().first_sequence(), 5);
        drop(unused);

        let current = cq.allocate_route().unwrap();
        assert_eq!(current.route().slot(), first.slot());
        assert_eq!(current.route().first_sequence(), 6);
        assert_eq!(current.lock_send().unwrap().sequence(), 6);
        assert_eq!(current.alloc_recv().unwrap(), 6);
    }

    #[test]
    fn test_cq_create_without_channel() {
        let dev = open_device();
        let ctx = Arc::clone(dev.context());
        let cq = CompletionQueue::create(&ctx, 16, None).unwrap();
        assert!(!cq.as_ptr().is_null());
        assert!(cq.capacity() >= 16);
        assert_eq!(cq.capacity(), unsafe { (*cq.as_ptr()).cqe as u32 });
        assert_eq!(cq.route_capacity(), cq.capacity());
        let slot_bits = u32::BITS - (cq.capacity() - 1).leading_zeros();
        assert_eq!(cq.sequence_bits(), 62 - slot_bits);
    }

    #[test]
    fn cq_rejects_nonpositive_requested_capacity() {
        let dev = open_device();
        for capacity in [0, -1, i32::MIN] {
            assert_eq!(
                CompletionQueue::create(dev.context(), capacity, None)
                    .unwrap_err()
                    .kind,
                ErrorKind::InvalidCompletionQueueConfig
            );
        }
    }

    #[test]
    fn token_decoding_uses_its_originating_cq_layout() {
        let dev = open_device();
        let cq = CompletionQueue::create(dev.context(), 16, None).unwrap();
        let other_cq = CompletionQueue::create(dev.context(), 1024, None).unwrap();
        let first = cq.allocate_route().unwrap();
        let second = cq.allocate_route().unwrap();
        let route = second.route();
        let sequence = route.max_sequence();
        let wc = ibv_wc {
            qp_num: 42,
            wr_id: route.encode(WRType::Read, sequence),
            ..Default::default()
        };
        let token = super::Completion { cq: &cq, wc: &wc };
        assert_eq!(token.slot(), 1);
        assert_eq!(token.sequence(), sequence);
        assert!(token.belongs_to(&cq, 42, route));
        assert!(!token.belongs_to(&other_cq, 42, route));
        assert!(!token.belongs_to(&cq, 42, first.route()));

        // Equal raw bits need not describe equal slot/sequence pairs on two
        // CQs. The token chooses its own decoder without accepting caller input.
        let other = super::Completion {
            cq: &other_cq,
            wc: &wc,
        };
        assert_eq!(other.slot(), other_cq.layout.slot(wc.wr_id));
        assert_eq!(other.sequence(), other_cq.layout.sequence(wc.wr_id));
        if cq.sequence_bits() != other_cq.sequence_bits() {
            assert_ne!(
                (token.slot(), token.sequence()),
                (other.slot(), other.sequence())
            );
        }
        assert!(!other.belongs_to(&cq, 42, route));
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
