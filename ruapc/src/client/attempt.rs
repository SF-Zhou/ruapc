//! Machinery for one pre-wire request attempt: socket acquisition,
//! failure classification, endpoint failover accounting, and time-budget
//! arithmetic. Everything here happens *before* a request reaches the
//! wire, so failures are safe to retry.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use ruapc_bufpool::RemoteBufferInfo;

use crate::{
    Buffer, Endpoint, Socket, State,
    core::{EndpointState, WriteTarget},
    error::{Error, ErrorKind},
    sockets::AcquireOptions,
};

/// Destination of one attempt: an already-connected socket (server-side
/// reverse RPC) or an endpoint to acquire a connection for.
pub(super) enum AttemptEndpoint {
    Connected(Socket),
    Endpoint(Arc<EndpointState>),
}

pub(super) struct AttemptOptions<'a> {
    pub(super) endpoint: AttemptEndpoint,
    pub(super) connect_deadline: std::time::Instant,
    pub(super) remaining_acquire_attempts: u64,
    pub(super) avoided_remote_nics: Option<&'a HashSet<String>>,
}

pub(super) struct AcquiredSocket {
    pub(super) socket: Socket,
    pub(super) endpoint_state: Option<Arc<EndpointState>>,
}

/// How a failed attempt may be answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AttemptDisposition {
    /// Transient connection trouble: retry, on any endpoint.
    Retryable,
    /// The endpoint (not the request) is unusable: only moving to an
    /// untried endpoint can help.
    FailoverOnly,
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
                AttemptDisposition::FailoverOnly
            },
            error,
            failed_remote_nic: None,
        }
    }

    pub(super) fn failover(error: Error) -> Self {
        Self {
            disposition: AttemptDisposition::FailoverOnly,
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

/// Endpoint-failover accounting for one request's pre-wire retry cycle.
pub(super) struct AttemptCycle {
    /// Ranked endpoint candidates; empty when the context carries an
    /// already-connected socket.
    candidates: Vec<Arc<EndpointState>>,
    /// Endpoints attempted in the current pass; cleared once every
    /// candidate was tried, so retries cycle back to the first.
    tried: HashSet<Endpoint>,
    /// Remote RDMA NICs to steer away from, learned from send failures;
    /// empty (and never fed) on other transports.
    avoided_remote_nics: HashMap<SocketAddr, HashSet<String>>,
    /// Failed attempts so far.
    attempt: u64,
    max_attempts: u64,
}

impl AttemptCycle {
    pub(super) fn new(candidates: Vec<Arc<EndpointState>>, max_attempts: u64) -> Self {
        Self {
            candidates,
            tried: HashSet::new(),
            avoided_remote_nics: HashMap::new(),
            attempt: 0,
            max_attempts,
        }
    }

    /// Picks the endpoint for the next attempt: re-ranks by current health
    /// (except on the first attempt, which keeps the caller's round-robin
    /// order) and prefers candidates not yet tried in this pass. `None`
    /// with an already-connected context (no candidates).
    pub(super) fn next_candidate(&mut self) -> Option<Arc<EndpointState>> {
        if self.candidates.is_empty() {
            return None;
        }
        if self.attempt != 0 {
            self.candidates
                .sort_by_key(|candidate| candidate.selection_rank());
        }
        if self.tried.len() == self.candidates.len() {
            self.tried.clear();
        }
        let candidate = self
            .candidates
            .iter()
            .find(|candidate| !self.tried.contains(&candidate.endpoint()))
            .expect("a non-empty candidate cycle has an untried endpoint")
            .clone();
        Some(candidate)
    }

    /// Index of the attempt currently running (0-based).
    pub(super) fn attempt_index(&self) -> u64 {
        self.attempt
    }

    pub(super) fn remaining_attempts(&self) -> u64 {
        self.max_attempts - self.attempt
    }

    pub(super) fn avoided_remote_nics(&self, addr: Option<SocketAddr>) -> Option<&HashSet<String>> {
        addr.and_then(|addr| self.avoided_remote_nics.get(&addr))
    }

    /// Records a failed attempt and decides whether another attempt should
    /// run; advances the attempt counter when it should.
    pub(super) fn note_failure(
        &mut self,
        attempted: Option<&Arc<EndpointState>>,
        failure: &AttemptFailure,
    ) -> bool {
        if let (Some(state), Some(remote)) = (attempted, &failure.failed_remote_nic) {
            self.avoided_remote_nics
                .entry(state.endpoint().addr())
                .or_default()
                .insert(remote.clone());
        }
        if let Some(state) = attempted {
            self.tried.insert(state.endpoint());
        }
        let has_untried_endpoint = self.tried.len() < self.candidates.len();
        let retry = should_retry_attempt(
            failure.disposition,
            self.attempt,
            self.max_attempts,
            has_untried_endpoint,
        );
        if retry {
            self.attempt += 1;
        }
        retry
    }
}

fn should_retry_attempt(
    disposition: AttemptDisposition,
    attempt: u64,
    max_attempts: u64,
    has_untried_endpoint: bool,
) -> bool {
    if attempt + 1 >= max_attempts {
        return false;
    }
    match disposition {
        AttemptDisposition::Retryable => true,
        AttemptDisposition::FailoverOnly => has_untried_endpoint,
        AttemptDisposition::Terminal => false,
    }
}

/// Resolves the socket for one attempt: reuses the connected socket, or
/// acquires (possibly establishing) a connection for the endpoint while
/// keeping its health state up to date.
pub(super) async fn acquire_for_attempt(
    state: &Arc<State>,
    attempt_endpoint: AttemptEndpoint,
    connect_deadline: std::time::Instant,
    remaining_acquire_attempts: u64,
    avoided_remote_nics: Option<&HashSet<String>>,
) -> std::result::Result<AcquiredSocket, AttemptFailure> {
    let endpoint_state = match attempt_endpoint {
        AttemptEndpoint::Connected(socket) => {
            return Ok(AcquiredSocket {
                socket,
                endpoint_state: None,
            });
        }
        AttemptEndpoint::Endpoint(endpoint_state) => endpoint_state,
    };
    let endpoint = endpoint_state.endpoint();
    let socket = if let Some(result) = try_acquire_direct(state, endpoint, avoided_remote_nics) {
        let socket = result.map_err(AttemptFailure::acquire)?;
        if !endpoint_state.is_current(&socket) {
            endpoint_state.record_observed_connection(&socket);
        }
        socket
    } else if let Some(acquire_deadline) = split_connection_deadline(
        std::time::Instant::now(),
        connect_deadline,
        remaining_acquire_attempts,
    ) {
        // A fresh connection may be needed: span the whole attempt with a
        // ConnectActivity so concurrent preconnects are suppressed and a
        // delayed failure cannot penalize a newer connection.
        let options = AcquireOptions {
            avoided_remote_nics,
            deadline: Some(acquire_deadline),
        };
        let activity = endpoint_state.begin_connect();
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(acquire_deadline),
            acquire_direct(state, endpoint, options),
        )
        .await;
        match result {
            Err(_elapsed) => {
                activity.record_failure();
                return Err(AttemptFailure::acquire_deadline());
            }
            Ok(Ok(socket)) => {
                if !endpoint_state.is_current(&socket) {
                    activity.record_connection(&socket);
                }
                socket
            }
            Ok(Err(error)) => {
                let timed_out = matches!(error.kind, ErrorKind::Timeout);
                if timed_out || is_connection_failure(&error) {
                    activity.record_failure();
                }
                return Err(if timed_out {
                    AttemptFailure::acquire_deadline()
                } else {
                    AttemptFailure::acquire(error)
                });
            }
        }
    } else {
        // No time left to connect: only an existing connection can serve
        // this attempt.
        let socket = acquire_existing_direct(state, endpoint, avoided_remote_nics)
            .await
            .ok_or_else(AttemptFailure::acquire_deadline)?
            .map_err(AttemptFailure::acquire)?;
        if !endpoint_state.is_current(&socket) {
            endpoint_state.record_observed_connection(&socket);
        }
        socket
    };
    Ok(AcquiredSocket {
        socket,
        endpoint_state: Some(endpoint_state),
    })
}

/// Exports the attached buffers as regions for the device the connection
/// actually runs on (TCP device, or the specific RDMA NIC of this
/// connection). Region export failures are endpoint problems (a buffer
/// not registered for this NIC), so they fail over instead of retrying.
pub(super) fn export_attached_regions(
    socket: &Socket,
    state: &State,
    read_buffers: &[&Buffer],
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
                .map_err(AttemptFailure::failover)?,
        );
    }
    if let Some(target) = write_target {
        write_regions = target
            .export_regions(&device_index)
            .map_err(AttemptFailure::failover)?;
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
/// attempts, so one slow endpoint cannot starve the others. `None` when
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

pub(super) async fn acquire_direct(
    state: &Arc<State>,
    endpoint: Endpoint,
    options: AcquireOptions<'_>,
) -> std::result::Result<Socket, Error> {
    state
        .socket_pool
        .acquire_with_options(endpoint, options, state)
        .await
}

fn try_acquire_direct(
    state: &Arc<State>,
    endpoint: Endpoint,
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
    endpoint: Endpoint,
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

/// Whether `err` indicates connection-level trouble (as opposed to a
/// request-level problem): only these penalize endpoint health and are
/// safe to retry blindly.
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
    let rdma = matches!(err.kind, ErrorKind::RdmaError(_));
    #[cfg(not(feature = "rdma"))]
    let rdma = false;
    common || rdma
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
    fn unknown_errors_do_not_penalize_endpoints() {
        assert!(!is_connection_failure(&Error::new(
            ErrorKind::Unknown("other".into()),
            "unclassified".into(),
        )));
        assert!(is_connection_failure(&Error::kind(
            ErrorKind::TcpConnectFailed
        )));
    }

    #[test]
    fn retry_disposition_respects_failure_scope() {
        assert!(should_retry_attempt(
            AttemptDisposition::Retryable,
            0,
            3,
            false,
        ));
        assert!(should_retry_attempt(
            AttemptDisposition::FailoverOnly,
            0,
            3,
            true,
        ));
        assert!(!should_retry_attempt(
            AttemptDisposition::FailoverOnly,
            0,
            3,
            false,
        ));
        assert!(!should_retry_attempt(
            AttemptDisposition::Terminal,
            0,
            3,
            true,
        ));
    }

    #[test]
    fn invalid_acquire_error_only_fails_over() {
        let failure = AttemptFailure::acquire(Error::kind(ErrorKind::InvalidArgument));
        assert_eq!(failure.disposition, AttemptDisposition::FailoverOnly);
        let failure = AttemptFailure::failover(Error::kind(ErrorKind::InvalidArgument));
        assert_eq!(failure.disposition, AttemptDisposition::FailoverOnly);
    }

    fn endpoint_state(port: u16) -> Arc<EndpointState> {
        Arc::new(EndpointState::new(Endpoint::tcp(
            format!("127.0.0.1:{port}").parse().unwrap(),
        )))
    }

    #[test]
    fn attempt_cycle_prefers_untried_endpoints_before_recycling() {
        let mut cycle = AttemptCycle::new(vec![endpoint_state(10001), endpoint_state(10002)], 5);
        let failure = AttemptFailure::acquire(Error::kind(ErrorKind::TcpConnectFailed));

        let first = cycle.next_candidate().unwrap();
        assert!(cycle.note_failure(Some(&first), &failure));
        let second = cycle.next_candidate().unwrap();
        assert_ne!(first.endpoint(), second.endpoint());
        assert!(cycle.note_failure(Some(&second), &failure));
        // Every endpoint was tried: the cycle restarts instead of starving.
        assert!(cycle.next_candidate().is_some());
        assert_eq!(cycle.attempt_index(), 2);
        assert_eq!(cycle.remaining_attempts(), 3);
    }

    #[test]
    fn attempt_cycle_stops_at_the_attempt_budget() {
        let mut cycle = AttemptCycle::new(vec![endpoint_state(10001)], 2);
        let failure = AttemptFailure::acquire(Error::kind(ErrorKind::TcpConnectFailed));

        let first = cycle.next_candidate().unwrap();
        assert!(cycle.note_failure(Some(&first), &failure));
        let second = cycle.next_candidate().unwrap();
        assert!(!cycle.note_failure(Some(&second), &failure));
    }

    #[test]
    fn attempt_cycle_records_failed_remote_nics_per_peer() {
        let state = endpoint_state(10001);
        let addr = state.endpoint().addr();
        let mut cycle = AttemptCycle::new(vec![state.clone()], 3);
        let failure = AttemptFailure::send(
            Error::kind(ErrorKind::RdmaSendFailed),
            Some("mlx5_1".into()),
        );

        assert!(cycle.avoided_remote_nics(Some(addr)).is_none());
        assert!(cycle.note_failure(Some(&state), &failure));
        let avoided = cycle.avoided_remote_nics(Some(addr)).unwrap();
        assert!(avoided.contains("mlx5_1"));
        assert!(cycle.avoided_remote_nics(None).is_none());
    }
}
