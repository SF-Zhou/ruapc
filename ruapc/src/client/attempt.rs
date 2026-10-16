//! Machinery for one pre-wire request attempt: socket acquisition,
//! failure classification, retry accounting, and time-budget arithmetic.
//! Only transient failures before transport acceptance may be retried.

use std::{collections::HashSet, sync::Arc, time::Duration};

use ruapc_bufpool::RemoteBufferInfo;

use crate::{
    Buffer, Endpoint, Socket, State,
    error::{Error, ErrorKind},
    remote_memory::WriteTarget,
    sockets::AcquireOptions,
};

/// Destination of one attempt: an already-connected socket (server-side
/// reverse RPC) or an endpoint to acquire a connection for.
#[derive(Clone)]
pub(super) enum AttemptEndpoint {
    Connected(Socket),
    Endpoint(Endpoint),
}

pub(super) struct AttemptOptions<'a> {
    pub(super) endpoint: AttemptEndpoint,
    pub(super) connect_deadline: std::time::Instant,
    pub(super) remaining_acquire_attempts: u64,
    pub(super) avoided_remote_nics: Option<&'a HashSet<String>>,
}

/// How a failed attempt may be answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AttemptDisposition {
    /// Transient connection trouble before transport acceptance.
    Retryable,
    /// Retrying cannot help (e.g. the request deadline expired).
    Terminal,
}

pub(super) struct AttemptFailure {
    pub(super) disposition: AttemptDisposition,
    pub(super) error: Error,
    /// Remote RDMA NIC of the connection the attempt failed on, if any;
    /// steers the retry's placement away from it.
    pub(super) failed_remote_nic: Option<String>,
}

impl AttemptFailure {
    pub(super) fn deadline() -> Self {
        Self {
            disposition: AttemptDisposition::Terminal,
            error: Error::new(ErrorKind::Timeout, "request deadline expired".into()),
            failed_remote_nic: None,
        }
    }

    pub(super) fn acquire_deadline() -> Self {
        Self {
            disposition: AttemptDisposition::Retryable,
            error: Error::new(
                ErrorKind::Timeout,
                "connection attempt deadline expired".into(),
            ),
            failed_remote_nic: None,
        }
    }

    pub(super) fn acquire(error: Error) -> Self {
        Self {
            disposition: if is_connection_failure(&error) {
                AttemptDisposition::Retryable
            } else {
                AttemptDisposition::Terminal
            },
            error,
            failed_remote_nic: None,
        }
    }

    /// A `send` failure on an established connection. Connection-level
    /// errors are retryable (the request never reached the wire);
    /// anything else is terminal.
    pub(super) fn send(error: Error, failed_remote_nic: Option<String>) -> Self {
        Self {
            disposition: if is_connection_failure(&error) {
                AttemptDisposition::Retryable
            } else {
                AttemptDisposition::Terminal
            },
            error,
            failed_remote_nic,
        }
    }
}

impl From<Error> for AttemptFailure {
    fn from(error: Error) -> Self {
        Self {
            disposition: AttemptDisposition::Terminal,
            error,
            failed_remote_nic: None,
        }
    }
}

/// Retry accounting for one logical endpoint's pre-acceptance failures.
pub(super) struct AttemptCycle {
    /// Remote RDMA NICs to steer away from, learned from send failures.
    avoided_remote_nics: HashSet<String>,
    attempt: u64,
    max_attempts: u64,
}

impl AttemptCycle {
    pub(super) fn new(max_attempts: u64) -> Self {
        Self {
            avoided_remote_nics: HashSet::new(),
            attempt: 0,
            max_attempts,
        }
    }

    /// Index of the attempt currently running (0-based).
    pub(super) fn attempt_index(&self) -> u64 {
        self.attempt
    }

    pub(super) fn remaining_attempts(&self) -> u64 {
        self.max_attempts - self.attempt
    }

    pub(super) fn avoided_remote_nics(&self) -> Option<&HashSet<String>> {
        (!self.avoided_remote_nics.is_empty()).then_some(&self.avoided_remote_nics)
    }

    /// Records a failed attempt and advances only when a retry is allowed.
    pub(super) fn note_failure(&mut self, failure: &AttemptFailure) -> bool {
        if let Some(remote) = &failure.failed_remote_nic {
            self.avoided_remote_nics.insert(remote.clone());
        }
        if failure.disposition != AttemptDisposition::Retryable
            || self.attempt + 1 >= self.max_attempts
        {
            return false;
        }
        self.attempt += 1;
        true
    }
}

/// Reuses a connected socket, or asks the pool to acquire a connection for
/// the complete endpoint. Address selection belongs to the connection pool.
pub(super) async fn acquire_for_attempt(
    state: &Arc<State>,
    attempt_endpoint: AttemptEndpoint,
    connect_deadline: std::time::Instant,
    remaining_acquire_attempts: u64,
    avoided_remote_nics: Option<&HashSet<String>>,
) -> std::result::Result<Socket, AttemptFailure> {
    let endpoint = match attempt_endpoint {
        AttemptEndpoint::Connected(socket) => return Ok(socket),
        AttemptEndpoint::Endpoint(endpoint) => endpoint,
    };
    if let Some(result) = try_acquire_direct(state, &endpoint, avoided_remote_nics) {
        return result.map_err(AttemptFailure::acquire);
    }
    if let Some(acquire_deadline) = split_connection_deadline(
        std::time::Instant::now(),
        connect_deadline,
        remaining_acquire_attempts,
    ) {
        let options = AcquireOptions {
            avoided_remote_nics,
            deadline: Some(acquire_deadline),
        };
        match tokio::time::timeout_at(
            tokio::time::Instant::from_std(acquire_deadline),
            acquire_direct(state, &endpoint, options),
        )
        .await
        {
            Err(_elapsed) => Err(AttemptFailure::acquire_deadline()),
            Ok(Ok(socket)) => Ok(socket),
            Ok(Err(error)) if matches!(error.kind, ErrorKind::Timeout) => {
                Err(AttemptFailure::acquire_deadline())
            }
            Ok(Err(error)) => Err(AttemptFailure::acquire(error)),
        }
    } else {
        // No time left to connect: an existing connection can still serve
        // this attempt without consuming a fresh connection budget.
        acquire_existing_direct(state, &endpoint, avoided_remote_nics)
            .await
            .ok_or_else(AttemptFailure::acquire_deadline)?
            .map_err(AttemptFailure::acquire)
    }
}

/// Exports the attached buffers as regions for the device the connection
/// actually runs on (TCP device, or the specific RDMA NIC of this
/// connection). Invalid registrations are terminal for this call.
pub(super) fn export_attached_regions(
    socket: &Socket,
    state: &State,
    read_buffers: &[Buffer],
    write_target: Option<&Arc<WriteTarget>>,
) -> std::result::Result<(Vec<RemoteBufferInfo>, Vec<RemoteBufferInfo>), AttemptFailure> {
    let mut read_regions = Vec::new();
    let mut write_regions = Vec::new();
    if read_buffers.is_empty() && write_target.is_none() {
        return Ok((read_regions, write_regions));
    }
    let device_index = socket.device_index(state);
    for buf in read_buffers {
        read_regions.push(
            buf.remote_buffer_info(&device_index)
                .map_err(|e| Error::new(ErrorKind::InvalidArgument, e.to_string()))
                .map_err(AttemptFailure::from)?,
        );
    }
    if let Some(target) = write_target {
        write_regions = target
            .export_regions(&device_index)
            .map_err(AttemptFailure::from)?;
    }
    Ok((read_regions, write_regions))
}

/// `now + budget`, additionally capped by the parent context's deadline
/// (nested RPCs inherit the caller's remaining time).
pub(super) fn capped_deadline(
    now: std::time::Instant,
    budget: Duration,
    parent: Option<std::time::Instant>,
) -> std::time::Instant {
    let deadline = now + budget;
    parent.map_or(deadline, |parent| deadline.min(parent))
}

/// Splits the remaining connect budget evenly across the remaining
/// attempts, so one slow acquisition cannot consume every retry. `None` when
/// no usable slice is left.
fn split_connection_deadline(
    now: std::time::Instant,
    deadline: std::time::Instant,
    remaining_attempts: u64,
) -> Option<std::time::Instant> {
    let remaining = deadline.saturating_duration_since(now);
    if remaining.is_zero() {
        return None;
    }
    let attempts = u32::try_from(remaining_attempts.max(1)).unwrap_or(u32::MAX);
    let slice = remaining / attempts;
    (!slice.is_zero()).then_some(now + slice)
}

/// The effective response budget as it travels on the wire (rounded up to
/// whole milliseconds so a nonzero budget never becomes zero).
pub(super) fn wire_timeout_ms(timeout: Duration) -> u32 {
    let millis = timeout.as_nanos().div_ceil(1_000_000);
    u32::try_from(millis).unwrap_or(u32::MAX)
}

async fn acquire_direct(
    state: &Arc<State>,
    endpoint: &Endpoint,
    options: AcquireOptions<'_>,
) -> std::result::Result<Socket, Error> {
    state
        .socket_pool
        .acquire_with_options(endpoint, options, state)
        .await
}

fn try_acquire_direct(
    state: &Arc<State>,
    endpoint: &Endpoint,
    avoided_remote_nics: Option<&HashSet<String>>,
) -> Option<std::result::Result<Socket, Error>> {
    state.socket_pool.try_acquire(
        endpoint,
        AcquireOptions {
            avoided_remote_nics,
            deadline: None,
        },
    )
}

async fn acquire_existing_direct(
    state: &Arc<State>,
    endpoint: &Endpoint,
    avoided_remote_nics: Option<&HashSet<String>>,
) -> Option<std::result::Result<Socket, Error>> {
    state
        .socket_pool
        .acquire_existing(
            endpoint,
            AcquireOptions {
                avoided_remote_nics,
                deadline: None,
            },
        )
        .await
}

/// Connection-level errors eligible for pre-acceptance retries.
/// Classification alone does not make a failure after send safe to retry.
pub(super) fn is_connection_failure(err: &Error) -> bool {
    let common = matches!(
        err.kind,
        ErrorKind::TcpConnectFailed
            | ErrorKind::TcpSendMsgFailed
            | ErrorKind::TcpRecvMsgFailed
            | ErrorKind::WebSocketConnectFailed
            | ErrorKind::WebSocketSendFailed
            | ErrorKind::WebSocketRecvFailed
            | ErrorKind::WebSocketClosed
            | ErrorKind::HttpWaitRspFailed
            | ErrorKind::HttpSendReqFailed
            | ErrorKind::ConnectionClosed
            | ErrorKind::RdmaSendFailed
            | ErrorKind::RdmaRecvFailed
            | ErrorKind::RdmaReadTimeout
    );
    #[cfg(feature = "rdma")]
    if matches!(err.kind, ErrorKind::RdmaError(_)) {
        return true;
    }
    common
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_deadlines_use_their_own_budget_domain() {
        let now = std::time::Instant::now();
        let connect_deadline = now + Duration::from_secs(6);
        let parent_deadline = now + Duration::from_secs(4);

        assert_eq!(
            capped_deadline(now, Duration::from_secs(6), None),
            connect_deadline
        );
        assert_eq!(
            capped_deadline(now, Duration::from_secs(6), Some(parent_deadline)),
            parent_deadline
        );

        assert_eq!(
            split_connection_deadline(now, connect_deadline, 1),
            Some(connect_deadline)
        );
        assert_eq!(
            split_connection_deadline(now, connect_deadline, 3),
            Some(now + Duration::from_secs(2))
        );
        assert_eq!(
            split_connection_deadline(now, now + Duration::from_nanos(1), u64::from(u32::MAX),),
            None
        );
        assert_eq!(wire_timeout_ms(Duration::from_nanos(1)), 1);
        assert_eq!(wire_timeout_ms(Duration::from_micros(999)), 1);
        assert_eq!(wire_timeout_ms(Duration::from_millis(1)), 1);
    }

    #[test]
    fn unknown_errors_are_not_connection_failures() {
        assert!(!is_connection_failure(&Error::new(
            ErrorKind::Unknown("other".into()),
            "unclassified".into(),
        )));
        assert!(is_connection_failure(&Error::kind(
            ErrorKind::TcpConnectFailed
        )));
    }

    #[test]
    fn invalid_acquire_and_send_errors_are_terminal() {
        let failure = AttemptFailure::acquire(Error::kind(ErrorKind::InvalidArgument));
        assert_eq!(failure.disposition, AttemptDisposition::Terminal);
        let failure = AttemptFailure::send(Error::kind(ErrorKind::InvalidArgument), None);
        assert_eq!(failure.disposition, AttemptDisposition::Terminal);
        let mut cycle = AttemptCycle::new(3);
        assert!(!cycle.note_failure(&failure));
    }

    #[test]
    fn attempt_cycle_stops_at_the_attempt_budget() {
        let failure = AttemptFailure::acquire(Error::kind(ErrorKind::TcpConnectFailed));
        let mut cycle = AttemptCycle::new(2);
        assert_eq!(cycle.remaining_attempts(), 2);
        assert!(cycle.note_failure(&failure));
        assert_eq!(cycle.attempt_index(), 1);
        assert_eq!(cycle.remaining_attempts(), 1);
        assert!(!cycle.note_failure(&failure));

        let mut no_retries = AttemptCycle::new(1);
        assert!(!no_retries.note_failure(&failure));
    }

    #[test]
    fn attempt_cycle_preserves_failed_remote_nics_across_retries() {
        let mut cycle = AttemptCycle::new(4);
        assert!(cycle.avoided_remote_nics().is_none());
        for nic in ["mlx5_1", "mlx5_2", "mlx5_1"] {
            let failure =
                AttemptFailure::send(Error::kind(ErrorKind::RdmaSendFailed), Some(nic.into()));
            assert!(cycle.note_failure(&failure));
        }
        let avoided = cycle.avoided_remote_nics().unwrap();
        assert_eq!(avoided.len(), 2);
        assert!(avoided.contains("mlx5_1"));
        assert!(avoided.contains("mlx5_2"));
    }
}
