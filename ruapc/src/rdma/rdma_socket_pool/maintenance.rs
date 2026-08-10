use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) struct RetryBackoff {
    pub(super) failures: u32,
    pub(super) retry_at: Instant,
}

pub(super) fn preconnect_backoff_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(8);
    Duration::from_millis((100u64 << exponent).min(30_000))
}
