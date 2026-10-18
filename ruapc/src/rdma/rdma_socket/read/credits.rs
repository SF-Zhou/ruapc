//! READ admission and completion credits, independent of buffer ownership.

use std::sync::Arc;

use tokio::sync::{Notify, Semaphore, SemaphorePermit};

use super::{ReadDeadline, read_closed_error, read_failure_error};
use crate::Result;

/// Owns this QP's READ admission and its share of the NIC-wide limit.
///
/// The socket must declare this owner after its QP. Normal completions return
/// both permits immediately; final destruction returns only the NIC permits
/// for WRs whose completions were never processed, after destroying the QP.
#[derive(Debug)]
pub(crate) struct ReadCredits {
    pub(super) device: Arc<Semaphore>,
    sq: Semaphore,
    cap: usize,
    closed: Notify,
    /// Captured from the QP during exclusive socket destruction. SQ occupancy
    /// is insufficient: an unposted task may hold SQ capacity while awaiting
    /// the NIC, and forgetting that future need not keep the socket alive.
    unpolled_on_drop: usize,
}

impl ReadCredits {
    pub(crate) fn new(device: Arc<Semaphore>, cap: u32) -> Self {
        let cap = cap.max(1) as usize;
        Self {
            device,
            sq: Semaphore::new(cap),
            cap,
            closed: Notify::new(),
            unpolled_on_drop: 0,
        }
    }

    pub(super) async fn acquire(
        &self,
        deadline: Option<ReadDeadline>,
    ) -> Result<(SemaphorePermit<'_>, SemaphorePermit<'_>)> {
        if self.is_closed() {
            return Err(read_closed_error());
        }
        // Only contended admission registers a closure waiter and timer.
        if let Ok(sq) = self.sq.try_acquire()
            && let Ok(device) = self.device.try_acquire()
        {
            return Ok((sq, device));
        }
        tokio::select! {
            biased;
            () = self.closed() => Err(read_closed_error()),
            error = async {
                match deadline {
                    Some(deadline) => {
                        tokio::time::sleep_until(deadline.at.into()).await;
                        read_failure_error(deadline.reason)
                    }
                    None => std::future::pending().await,
                }
            } => Err(error),
            permits = async {
                // A congested connection must not reserve the whole NIC's capacity.
                let sq = self.sq.acquire().await.map_err(|_| read_closed_error())?;
                let device = self.device.acquire().await.map_err(|_| read_closed_error())?;
                Ok((sq, device))
            } => permits,
        }
    }

    /// Called once for each authentic READ completion, including errors.
    pub(crate) fn complete(&self) {
        self.device.add_permits(1);
        self.sq.add_permits(1);
    }

    pub(crate) fn close(&self) {
        self.sq.close();
        self.closed.notify_waiters();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.sq.is_closed()
    }

    pub(crate) async fn closed(&self) {
        let notified = self.closed.notified();
        tokio::pin!(notified);
        // Register before checking the persistent close state so a close
        // cannot be lost before this future first waits for notification.
        notified.as_mut().enable();
        if !self.is_closed() {
            notified.await;
        }
    }

    /// Record the outstanding WR count during exclusive socket destruction.
    /// Returning those credits is deferred until this owner drops after the QP.
    pub(crate) fn record_unpolled(&mut self, count: usize) {
        self.unpolled_on_drop = count;
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.sq.available_permits() == self.cap
    }
}

impl Drop for ReadCredits {
    fn drop(&mut self) {
        // The QP's successful destruction has ended DMA. Forgotten unposted
        // futures may leak their own permits, but must never invent NIC credit.
        if self.unpolled_on_drop != 0 {
            self.device.add_permits(self.unpolled_on_drop);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use crate::ErrorKind;

    async fn retain_posted_credits(credits: &ReadCredits) {
        let (sq, device) = credits.acquire(None).await.unwrap();
        sq.forget();
        device.forget();
    }

    #[tokio::test]
    async fn deadline_wakes_sq_admission_without_reserving_nic_capacity() {
        let device = Arc::new(Semaphore::new(2));
        let credits = ReadCredits::new(device.clone(), 1);
        let outstanding = credits.sq.acquire().await.unwrap();
        let deadline =
            ReadDeadline::earliest(Some(Instant::now() + Duration::from_millis(10)), None);
        let acquiring = credits.acquire(deadline);
        tokio::pin!(acquiring);
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(device.available_permits(), 2);

        let error = tokio::time::timeout(Duration::from_secs(1), acquiring)
            .await
            .expect("SQ admission must observe the READ deadline")
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::RdmaReadTimeout);
        assert!(!credits.is_closed());
        assert_eq!(device.available_permits(), 2);
        drop(outstanding);
        assert_eq!(credits.sq.available_permits(), 1);
    }

    #[tokio::test]
    async fn completion_before_post_returns_does_not_double_return_credits() {
        let device = Arc::new(Semaphore::new(2));
        let credits = ReadCredits::new(device.clone(), 2);
        let (sq_permit, device_permit) = credits.acquire(None).await.unwrap();
        assert_eq!(device.available_permits(), 1);
        assert_eq!(credits.sq.available_permits(), 1);

        // A poll thread can process the CQE while the posting thread still
        // holds both permits, before the provider call returns to post_read.
        credits.complete();
        assert_eq!(device.available_permits(), 2);
        assert_eq!(credits.sq.available_permits(), 2);
        sq_permit.forget();
        device_permit.forget();
        assert!(credits.is_idle());
        assert_eq!(device.available_permits(), 2);
        drop(credits);
        assert_eq!(device.available_permits(), 2);
    }

    #[tokio::test]
    async fn completion_after_closing_admission_returns_both_credits_once() {
        let device = Arc::new(Semaphore::new(2));
        let credits = ReadCredits::new(device.clone(), 2);
        retain_posted_credits(&credits).await;
        assert_eq!(device.available_permits(), 1);
        assert!(!credits.is_idle());

        credits.close();
        assert!(credits.is_closed());
        credits.complete();
        assert!(credits.is_idle());
        assert_eq!(device.available_permits(), 2);
        drop(credits);
        assert_eq!(device.available_permits(), 2);
    }

    #[tokio::test]
    async fn destruction_refunds_only_unpolled_reads_and_preserves_other_qp_usage() {
        let device = Arc::new(Semaphore::new(4));
        let other_qp_read = device.acquire().await.unwrap();
        let mut credits = ReadCredits::new(device.clone(), 2);
        retain_posted_credits(&credits).await;
        retain_posted_credits(&credits).await;
        assert_eq!(device.available_permits(), 1);

        credits.complete();
        assert_eq!(device.available_permits(), 2);
        // Model the exclusive QP snapshot: one of the two posted WRs still
        // lacks a processed CQE. Merely recording it must not refund anything.
        credits.record_unpolled(1);
        assert_eq!(device.available_permits(), 2);
        drop(credits);
        assert_eq!(device.available_permits(), 3);
        drop(other_qp_read);
        assert_eq!(device.available_permits(), 4);
    }

    #[tokio::test]
    async fn forgetting_an_unposted_nic_waiter_does_not_invent_credit() {
        let device = Arc::new(Semaphore::new(1));
        let other_qp_read = device.acquire().await.unwrap();
        let credits = ReadCredits::new(device.clone(), 2);
        let mut acquiring = Box::pin(credits.acquire(None));
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(credits.sq.available_permits(), 1);
        assert_eq!(device.available_permits(), 0);

        // Forgetting ends the borrow without releasing the held SQ permit.
        // No READ was posted, so the QP's final pending count remains zero.
        std::mem::forget(acquiring);
        drop(credits);
        assert_eq!(device.available_permits(), 0);
        // Prevent the intentionally leaked waiter from taking the unrelated
        // read's returned permit. Leaking a future may retain resources, but
        // must not make more NIC credit available than was originally issued.
        device.close();
        drop(other_qp_read);
        assert_eq!(device.available_permits(), 1);
    }

    #[tokio::test]
    async fn closing_read_admission_wakes_a_task_waiting_for_sq_capacity() {
        let device = Arc::new(Semaphore::new(2));
        let credits = ReadCredits::new(device.clone(), 1);
        let outstanding = credits.sq.acquire().await.unwrap();
        let acquiring = credits.acquire(None);
        tokio::pin!(acquiring);
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(device.available_permits(), 2);

        credits.close();
        assert_eq!(
            acquiring.await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(credits.sq.available_permits(), 0);
        drop(outstanding);
        assert_eq!(credits.sq.available_permits(), 1);
        assert_eq!(device.available_permits(), 2);
    }

    #[tokio::test]
    async fn closing_read_admission_releases_sq_capacity_while_device_is_full() {
        let device = Arc::new(Semaphore::new(1));
        let credits = ReadCredits::new(device.clone(), 2);
        let unrelated_read = device.acquire().await.unwrap();
        let acquiring = credits.acquire(None);
        tokio::pin!(acquiring);
        assert!(futures_util::poll!(&mut acquiring).is_pending());
        assert_eq!(credits.sq.available_permits(), 1);

        credits.close();
        assert_eq!(
            acquiring.await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(credits.sq.available_permits(), 2);
        assert_eq!(device.available_permits(), 0);
        assert!(!device.is_closed());
        drop(unrelated_read);
        assert_eq!(device.available_permits(), 1);
    }

    #[tokio::test]
    async fn closure_before_registering_a_waiter_rejects_without_taking_permits() {
        let device = Arc::new(Semaphore::new(2));
        let credits = ReadCredits::new(device.clone(), 2);
        credits.close();

        assert_eq!(
            credits.acquire(None).await.unwrap_err().kind,
            ErrorKind::ConnectionClosed
        );
        assert_eq!(credits.sq.available_permits(), 2);
        assert_eq!(device.available_permits(), 2);
    }
}
