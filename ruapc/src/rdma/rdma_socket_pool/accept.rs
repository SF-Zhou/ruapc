use std::{sync::Weak, time::Instant};

use super::RdmaSocket;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AcceptLeaseState {
    Pending,
    ReceiveObserved,
    Confirmed,
    Active,
}

#[derive(Clone, Copy)]
pub(super) enum AcceptLeaseEvent {
    Confirm,
    Receive,
}

pub(super) fn advance_accept_lease(
    state: AcceptLeaseState,
    event: AcceptLeaseEvent,
) -> AcceptLeaseState {
    match (state, event) {
        (AcceptLeaseState::Pending, AcceptLeaseEvent::Confirm) => AcceptLeaseState::Confirmed,
        (AcceptLeaseState::Pending, AcceptLeaseEvent::Receive) => AcceptLeaseState::ReceiveObserved,
        (AcceptLeaseState::Confirmed, AcceptLeaseEvent::Confirm) => AcceptLeaseState::Confirmed,
        (AcceptLeaseState::Active, AcceptLeaseEvent::Confirm) => AcceptLeaseState::Active,
        (AcceptLeaseState::ReceiveObserved, AcceptLeaseEvent::Receive) => {
            AcceptLeaseState::ReceiveObserved
        }
        (AcceptLeaseState::Confirmed, AcceptLeaseEvent::Receive)
        | (AcceptLeaseState::ReceiveObserved, AcceptLeaseEvent::Confirm) => {
            AcceptLeaseState::Active
        }
        (AcceptLeaseState::Active, AcceptLeaseEvent::Receive) => AcceptLeaseState::Active,
    }
}

pub(super) struct AcceptLease {
    pub(super) socket: Weak<RdmaSocket>,
    pub(super) server_connection_cookie: u64,
    pub(super) state: AcceptLeaseState,
    pub(super) expires_at: Instant,
}
