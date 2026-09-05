//! One outbound connection attempt. Pool placement and publication live in
//! `connect`; local verbs/resource setup is shared with the acceptor in `setup`.

use std::{sync::Arc, time::Instant};

use tracing::Instrument as _;

use super::super::{
    ConnectionLease, DeviceSelection, PrepareConnectionRequest, RdmaBootstrapService as _,
    RdmaConnectionLimits, RdmaSocket,
};
use super::placement::PathCandidate;
use super::setup::at_stage;
use super::{PeerState, RdmaSocketPool, next_attempt_id};
use crate::{Client, Context, Error, ErrorKind, Result, State, TaskSupervisorHandle};

/// Owns rollback from the moment the peer returns a valid lease until the
/// socket is published. Dropping the handshake future or an unpublished stripe
/// closes the local QP and schedules peer cleanup with a fresh deadline.
pub(super) struct BootstrapRollback {
    socket: Option<Arc<RdmaSocket>>,
    lease: Option<ConnectionLease>,
    supervisor: TaskSupervisorHandle,
    client: Client,
    context: Context,
}

impl BootstrapRollback {
    fn new(
        pool: &RdmaSocketPool,
        peer: &PeerState,
        state: &Arc<State>,
        lease: ConnectionLease,
    ) -> Self {
        Self {
            socket: None,
            lease: Some(lease),
            supervisor: pool.task_supervisor.handle(),
            client: pool.bootstrap_client.clone(),
            context: Context::create_with_state_and_addr(state, &peer.addr),
        }
    }

    pub(super) fn disarm(mut self) {
        self.lease = None;
    }
}

impl Drop for BootstrapRollback {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        if let Some(socket) = &self.socket {
            socket.set_error();
        }
        let client = self.client.clone();
        let context = self.context.clone();
        let span = tracing::debug_span!(
            "rdma_bootstrap_cancel",
            attempt_id = lease.attempt_id,
            accepted_conn_id = lease.accepted_connection_id,
        );
        if self
            .supervisor
            .try_spawn(
                async move {
                    tracing::debug!("rolling back unpublished RDMA connection");
                    if let Err(err) = client.cancel_connection(&context, &lease).await {
                        tracing::debug!(%err, "RDMA peer cleanup failed; accept lease will expire");
                    }
                }
                .instrument(span),
            )
            .is_none()
        {
            tracing::debug!(
                attempt_id = lease.attempt_id,
                "RDMA cleanup supervisor stopped; accept lease will expire"
            );
        }
    }
}

pub(super) struct EstablishedSocket {
    pub(super) socket: Arc<RdmaSocket>,
    pub(super) rollback: BootstrapRollback,
}

impl RdmaSocketPool {
    /// Prepare -> exchange -> connect/register -> confirm. Publication and the
    /// first data-plane activation are the caller's responsibility.
    pub(super) async fn connect_stripe(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        bootstrap_ctx: &Context,
        candidate: &PathCandidate,
    ) -> Result<EstablishedSocket> {
        let attempt_id = next_attempt_id();
        let started = Instant::now();
        let span = tracing::info_span!(
            "rdma_connect",
            peer = %peer.addr,
            attempt_id,
            local_device = %candidate.path.local.device,
            local_port = candidate.path.local.port_num,
            local_gid_index = candidate.path.local.gid_index,
            remote_device = %candidate.path.remote.device,
            remote_port = candidate.path.remote.port_num,
            remote_gid_index = candidate.path.remote.gid_index,
            local_qp = tracing::field::Empty,
            remote_qp = tracing::field::Empty,
            conn_id = tracing::field::Empty,
        );
        async {
            tracing::debug!("RDMA connection attempt started");
            let result = self.establish_connection(peer, state, bootstrap_ctx, candidate, attempt_id).await;
            match result {
                Ok(established) => {
                    tracing::info!(elapsed_ms = started.elapsed().as_millis() as u64, "RDMA connection confirmed; awaiting publication");
                    Ok(established)
                }
                Err(err) => {
                    tracing::warn!(%err, error_kind = ?err.kind, elapsed_ms = started.elapsed().as_millis() as u64, "RDMA connection attempt failed");
                    Err(Error::new(err.kind, format!(
                        "RDMA connection to {} (attempt {attempt_id}, {}:{}/gid{} -> {}:{}/gid{}): {}",
                        peer.addr, candidate.path.local.device, candidate.path.local.port_num,
                        candidate.path.local.gid_index, candidate.path.remote.device,
                        candidate.path.remote.port_num, candidate.path.remote.gid_index, err.msg,
                    )))
                }
            }
        }.instrument(span).await
    }

    async fn establish_connection(
        &self,
        peer: &Arc<PeerState>,
        state: &Arc<State>,
        ctx: &Context,
        candidate: &PathCandidate,
        attempt_id: u64,
    ) -> Result<EstablishedSocket> {
        check_deadline(ctx, "before local setup")?;
        let local = self.prepare_local_connection(
            candidate.local_device_index,
            &DeviceSelection {
                device_name: candidate.path.local.device.clone(),
                port_num: candidate.path.local.port_num,
                gid_index: candidate.path.local.gid_index,
            },
            candidate.remote_limits,
            self.config.connection.traffic_class,
        )?;
        tracing::Span::current().record("local_qp", local.endpoint.qp_num);
        let request = PrepareConnectionRequest {
            attempt_id,
            endpoint: local.endpoint,
            source_device: candidate.path.local.device.clone(),
            same_connectivity_domain: candidate.path.same_connectivity_domain,
            target: candidate.remote.clone(),
            limits: RdmaConnectionLimits::from(local.config),
            traffic_class: local.config.traffic_class,
        };
        // Bootstrap RPCs recurse through SocketPool::acquire. Boxing keeps the
        // generated future finite; the bootstrap context uses TCP transport.
        let response = Box::pin(self.bootstrap_client.prepare_connection(ctx, &request))
            .await
            .map_err(|err| {
                self.invalidate_advertisement_cache(peer);
                at_stage("peer prepare RPC", err)
            })?;
        // Only cancel a lease belonging to this attempt. A lost/malformed lease
        // response is reclaimed by the acceptor's finite preparation lease.
        let mut rollback = (response.lease.attempt_id == attempt_id
            && response.lease.accepted_connection_id != 0)
            .then(|| BootstrapRollback::new(self, peer, state, response.lease));
        response.validate_for(&request).map_err(|err| {
            self.invalidate_advertisement_cache(peer);
            at_stage("validate prepare response", err)
        })?;
        let mut rollback = rollback.take().expect("validated connection lease");
        tracing::Span::current().record("remote_qp", response.endpoint.qp_num);
        tracing::debug!(
            accepted_conn_id = response.lease.accepted_connection_id,
            "RDMA peer prepared"
        );
        check_deadline(ctx, "after peer prepare")?;
        local.connect(self, &response.endpoint).inspect_err(|_| {
            self.blacklist_path(peer, &candidate.path);
        })?;
        let socket = local.register(self, state, candidate.path.clone())?;
        rollback.socket = Some(socket.clone());
        tracing::Span::current().record("conn_id", socket.conn_id);
        check_deadline(ctx, "before peer commit")?;
        self.confirm_connection(ctx, &response.lease)
            .await
            .map_err(|err| at_stage("peer commit RPC", err))?;
        check_deadline(ctx, "after peer commit")?;
        if !socket.state.is_ok() {
            return Err(Error::new(
                ErrorKind::ConnectionClosed,
                "RDMA local connection closed before publication".into(),
            ));
        }
        Ok(EstablishedSocket { socket, rollback })
    }

    /// Commit is idempotent while its lease is retained. Retry one ambiguous
    /// response timeout if the caller still has budget; generic RPC retries
    /// cannot make this method-specific idempotency assumption.
    async fn confirm_connection(&self, ctx: &Context, lease: &ConnectionLease) -> Result<()> {
        match Box::pin(self.bootstrap_client.commit_connection(ctx, lease)).await {
            Err(err) if err.kind == ErrorKind::Timeout && !ctx.is_expired() => {
                tracing::debug!(%err, "RDMA connection commit timed out; retrying once");
                Box::pin(self.bootstrap_client.commit_connection(ctx, lease)).await
            }
            result => result,
        }
    }
}

fn check_deadline(ctx: &Context, stage: &str) -> Result<()> {
    if ctx.is_expired() {
        return Err(Error::new(
            ErrorKind::Timeout,
            format!("RDMA connection deadline expired {stage}"),
        ));
    }
    Ok(())
}
