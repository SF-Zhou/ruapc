//! Lock-free GCRA rate limiter used for per-NIC RDMA bandwidth shaping.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use crate::{Error, ErrorKind, Result};

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// A continuously refilling token bucket represented by one atomic
/// theoretical-arrival timestamp (TAT).
#[derive(Debug)]
pub(crate) struct GcraRateLimiter {
    start: Instant,
    tat_nanos: AtomicU64,
}

/// Per-port bandwidth policy and independent SEND/RECV budgets.
#[derive(Debug)]
pub(crate) struct RdmaBandwidthLimiter {
    device: String,
    port_num: u8,
    bytes_per_sec: u64,
    burst: Duration,
    max_wait: Duration,
    send: GcraRateLimiter,
    recv: GcraRateLimiter,
}

impl Default for GcraRateLimiter {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            tat_nanos: AtomicU64::new(0),
        }
    }
}

impl GcraRateLimiter {
    /// Atomically reserves `bytes` and returns the required delay. If that
    /// delay exceeds `max_wait`, no reservation is made.
    ///
    /// `rate` is measured in bytes per second. The burst tolerance is always
    /// at least this request's cost so one large request can make progress
    /// when the limiter is otherwise idle. A successful reservation is not
    /// rolled back if its caller is later cancelled.
    pub(crate) fn reserve_within(
        &self,
        bytes: u64,
        rate: u64,
        burst: Duration,
        max_wait: Duration,
    ) -> Option<Duration> {
        if bytes == 0 || rate == 0 {
            return Some(Duration::ZERO);
        }
        self.reserve_within_at(
            bytes,
            rate,
            duration_nanos(burst),
            duration_nanos(max_wait),
            duration_nanos(self.start.elapsed()),
        )
        .map(Duration::from_nanos)
    }

    fn reserve_within_at(
        &self,
        bytes: u64,
        rate: u64,
        burst_nanos: u64,
        max_wait_nanos: u64,
        now: u64,
    ) -> Option<u64> {
        let cost = cost_nanos(bytes, rate);
        let burst = burst_nanos.max(cost);

        loop {
            let tat = self.tat_nanos.load(Ordering::Relaxed);
            let new_tat = tat.max(now).saturating_add(cost);
            if new_tat > now.saturating_add(burst).saturating_add(max_wait_nanos) {
                return None;
            }
            if self
                .tat_nanos
                .compare_exchange_weak(tat, new_tat, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Some(new_tat.saturating_sub(burst).saturating_sub(now));
            }
            std::hint::spin_loop();
        }
    }
}

impl RdmaBandwidthLimiter {
    pub(crate) fn new(
        device: String,
        port_num: u8,
        bytes_per_sec: u64,
        burst: Duration,
        max_wait: Duration,
    ) -> Self {
        Self {
            device,
            port_num,
            bytes_per_sec,
            burst,
            max_wait,
            send: GcraRateLimiter::default(),
            recv: GcraRateLimiter::default(),
        }
    }

    pub(crate) async fn reserve_send(
        &self,
        bytes: u64,
        request_remaining: Option<Duration>,
    ) -> Result<()> {
        self.reserve(&self.send, "SEND", bytes, request_remaining)
            .await
    }

    pub(crate) async fn reserve_recv(
        &self,
        bytes: u64,
        request_remaining: Option<Duration>,
    ) -> Result<()> {
        self.reserve(&self.recv, "RECV", bytes, request_remaining)
            .await
    }

    async fn reserve(
        &self,
        limiter: &GcraRateLimiter,
        direction: &'static str,
        bytes: u64,
        request_remaining: Option<Duration>,
    ) -> Result<()> {
        if request_remaining == Some(Duration::ZERO) {
            return Err(Error::new(
                ErrorKind::Timeout,
                format!("request deadline expired before RDMA {direction} bandwidth admission"),
            ));
        }
        let request_deadline = request_remaining.map(|remaining| Instant::now() + remaining);
        let wait_limit = request_remaining
            .map(|remaining| remaining.min(self.max_wait))
            .unwrap_or(self.max_wait);
        let Some(wait) = limiter.reserve_within(bytes, self.bytes_per_sec, self.burst, wait_limit)
        else {
            let deadline_limited =
                request_remaining.is_some_and(|remaining| remaining <= self.max_wait);
            return Err(Error::new(
                if deadline_limited {
                    ErrorKind::Timeout
                } else {
                    ErrorKind::RdmaRateLimited
                },
                format!(
                    "RDMA {direction} transfer of {bytes} bytes on {} port {} exceeds the bandwidth wait limit",
                    self.device, self.port_num
                ),
            ));
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        if request_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::new(
                ErrorKind::Timeout,
                format!("request deadline expired while waiting for RDMA {direction} bandwidth"),
            ));
        }
        Ok(())
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn cost_nanos(bytes: u64, rate: u64) -> u64 {
    ((bytes as u128).saturating_mul(NANOS_PER_SEC as u128) / rate as u128).min(u64::MAX as u128)
        as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn idle_request_is_immediately_admitted() {
        let limiter = GcraRateLimiter::default();
        assert_eq!(limiter.reserve_within_at(100, 100, 0, 0, 1_000), Some(0));
    }

    #[test]
    fn bounded_wait_rejection_does_not_charge() {
        let limiter = GcraRateLimiter::default();
        assert_eq!(limiter.reserve_within_at(100, 100, 0, 0, 1_000), Some(0));
        assert_eq!(
            limiter.reserve_within_at(100, 100, 0, 999_999_999, 1_000),
            None
        );
        assert_eq!(
            limiter.reserve_within_at(100, 100, 0, 1_000_000_000, 1_000),
            Some(1_000_000_000)
        );
    }

    #[test]
    fn burst_allows_multiple_requests_immediately() {
        let limiter = GcraRateLimiter::default();
        let burst = 3_000_000_000;
        for _ in 0..3 {
            assert_eq!(
                limiter.reserve_within_at(100, 100, burst, 0, 1_000),
                Some(0)
            );
        }
        assert_eq!(limiter.reserve_within_at(100, 100, burst, 0, 1_000), None);
    }

    #[test]
    fn concurrent_reservations_are_accounted_once() {
        let limiter = Arc::new(GcraRateLimiter::default());
        let admitted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let limiter = limiter.clone();
                    scope.spawn(move || {
                        limiter
                            .reserve_within_at(1, 1, 8 * NANOS_PER_SEC, 0, 1_000)
                            .is_some()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|admitted| *admitted)
                .count()
        });
        assert_eq!(admitted, 8);
    }

    #[test]
    fn zero_rate_or_bytes_are_unlimited() {
        let limiter = GcraRateLimiter::default();
        assert_eq!(
            limiter.reserve_within(1, 0, Duration::ZERO, Duration::ZERO),
            Some(Duration::ZERO)
        );
        assert_eq!(
            limiter.reserve_within(0, 1, Duration::ZERO, Duration::ZERO),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn cost_calculation_saturates() {
        assert_eq!(cost_nanos(1_000, 1_000), NANOS_PER_SEC);
        assert_eq!(cost_nanos(u64::MAX, 1), u64::MAX);
    }

    #[test]
    fn send_and_recv_budgets_are_independent() {
        let limiters =
            RdmaBandwidthLimiter::new("mlx5_0".into(), 1, 100, Duration::ZERO, Duration::ZERO);
        assert_eq!(
            limiters.send.reserve_within_at(100, 100, 0, 0, 1_000),
            Some(0)
        );
        assert_eq!(
            limiters.recv.reserve_within_at(100, 100, 0, 0, 1_000),
            Some(0)
        );
        assert_eq!(limiters.send.reserve_within_at(100, 100, 0, 0, 1_000), None);
        assert_eq!(limiters.recv.reserve_within_at(100, 100, 0, 0, 1_000), None);
    }
}
