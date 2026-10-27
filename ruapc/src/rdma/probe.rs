//! Per-connection RPC and remote READ health checks.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::RdmaSocket;
use crate::{
    Client, Context, CopyOp, Error, ErrorKind, MsgMeta, Result, Socket, State,
    core::ContextEndpoint, service,
};

const PROBE_BYTES: usize = std::mem::size_of::<u64>();
static NEXT_PROBE_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ProbeReply {
    pub(crate) rpc_nonce: u64,
    pub(crate) read_nonce: u64,
}

#[service]
pub(crate) trait RdmaProbeService {
    /// Checks RPC delivery and reads the attached nonce over the same RDMA connection.
    async fn probe(&self, ctx: &Context, nonce: &u64) -> Result<ProbeReply>;
}

impl RdmaProbeService for () {
    async fn probe(&self, ctx: &Context, nonce: &u64) -> Result<ProbeReply> {
        if !matches!(&ctx.endpoint, ContextEndpoint::Connected(Socket::RDMA(_))) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA probe requires an established RDMA connection".into(),
            ));
        }
        if ctx.remote_read_space()?.total_len() != PROBE_BYTES as u64 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA probe requires exactly eight bytes of read space".into(),
            ));
        }
        let mut destination = ctx
            .state
            .buffer_pool
            .allocate(PROBE_BYTES)
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))?;
        destination.set_len(PROBE_BYTES);
        let destinations = ctx
            .remote_read(&[CopyOp::new(0, 0, PROBE_BYTES as u64)], vec![destination])
            .await?;
        let mut read_bytes = [0; PROBE_BYTES];
        read_bytes.copy_from_slice(&destinations[0]);
        Ok(ProbeReply {
            rpc_nonce: *nonce,
            read_nonce: u64::from_le_bytes(read_bytes),
        })
    }
}

/// Probes exactly this socket, without pool acquisition, retries or an inherited deadline.
pub(crate) async fn probe_connection(
    socket: &Arc<RdmaSocket>,
    state: &Arc<State>,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                "RDMA probe timeout is too large".into(),
            )
        })?;
        let ctx = Context {
            state: state.clone(),
            endpoint: ContextEndpoint::Connected(Socket::RDMA(socket.clone())),
            drop_guard: None,
            msg_meta: MsgMeta::default(),
            deadline: Some(deadline),
        };
        let rpc_nonce = NEXT_PROBE_NONCE
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(socket.conn_id.rotate_left(29));
        // Distinct values ensure that merely echoing the RPC payload cannot
        // masquerade as successful remote-memory access.
        let read_nonce = rpc_nonce ^ 0xa57c_3e91_d608_b24f;
        let mut source = state
            .buffer_pool
            .allocate(PROBE_BYTES)
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))?;
        source[..PROBE_BYTES].copy_from_slice(&read_nonce.to_le_bytes());
        source.set_len(PROBE_BYTES);
        let client = Client {
            timeout,
            connect_timeout: timeout,
            max_retries: 0,
            ..Default::default()
        };
        let attached = client.with_read_buffers(vec![source]);
        let reply = Box::pin(attached.probe(&ctx, &rpc_nonce)).await?;
        if reply.rpc_nonce != rpc_nonce || reply.read_nonce != read_nonce {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA probe returned an incorrect RPC or remote READ nonce".into(),
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::new(ErrorKind::Timeout, "RDMA connection probe timed out".into()))?
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::{Endpoint, Router, Server, SocketPoolConfig, Transport};

    async fn stop(server: Server, context: Context) {
        context.state.socket_pool.stop();
        tokio::time::timeout(Duration::from_secs(5), context.state.socket_pool.join())
            .await
            .unwrap();
        drop(context);
        server.stop();
        tokio::time::timeout(Duration::from_secs(5), server.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn probe_rejects_unconnected_and_tcp_contexts() {
        let config = SocketPoolConfig {
            rdma: None,
            ..Default::default()
        };
        let context = Context::create(&config).unwrap();
        let error = ().probe(&context, &17).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidArgument);
        assert!(error.msg.contains("established RDMA connection"));

        let server = Server::create(Router::default(), &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let context = context.with_endpoint(Endpoint::new(Transport::TCP, [address]).unwrap());
        let error = Client::default().probe(&context, &17).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidArgument);
        assert!(error.msg.contains("established RDMA connection"));
        stop(server, context).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probe_rejects_invalid_read_spaces_and_forged_read_result() {
        let config = super::super::test_utils::small_rdma_config();

        let forge_reply = Arc::new(AtomicBool::new(false));
        let mut router = Router::default();
        let forge = forge_reply.clone();
        router.wrap_method("RdmaProbeService/probe", move |original| {
            Box::new(move |mut ctx, payload| {
                if !forge.load(Ordering::Acquire) {
                    return original(ctx, payload);
                }
                let nonce: u64 = payload.deserialize(&ctx.msg_meta)?;
                tokio::spawn(async move {
                    ctx.send_rsp::<_, Error>(Ok(ProbeReply {
                        rpc_nonce: nonce,
                        // Echoing the request proves RPC delivery, but must
                        // never pass as the independently attached READ nonce.
                        read_nonce: nonce,
                    }))
                    .await;
                });
                Ok(())
            })
        });
        let server = Server::create(router, &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let endpoint = Endpoint::rdma(Transport::TCP, [address]).unwrap();
        let mut context = Context::create(&config).unwrap();
        let Socket::RDMA(socket) = context
            .state
            .socket_pool
            .acquire(&endpoint, &context.state)
            .await
            .unwrap()
        else {
            panic!("RDMA endpoint must produce an RDMA socket");
        };
        context.endpoint = ContextEndpoint::Connected(Socket::RDMA(socket.clone()));
        let client = Client {
            max_retries: 0,
            ..Default::default()
        };
        let error = client.probe(&context, &23).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingBufferInfo);
        for len in [7, 9] {
            let mut buffer = context.state.buffer_pool.allocate(len).unwrap();
            buffer.set_len(len);
            let error = client
                .with_read_buffers(vec![buffer])
                .probe(&context, &23)
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidArgument);
            assert!(error.msg.contains("exactly eight bytes"));
        }

        forge_reply.store(true, Ordering::Release);
        let error = probe_connection(&socket, &context.state, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidArgument);
        assert!(error.msg.contains("incorrect RPC or remote READ nonce"));
        drop(socket);
        stop(server, context).await;
    }
}
