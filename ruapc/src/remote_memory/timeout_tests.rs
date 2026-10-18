//! Exercise public remote-memory calls with a live connection whose peer
//! consumes reverse RPCs without replying. Observe the server's API return
//! independently of the original caller's timeout.

use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{
    Buffer, Client, Context, Endpoint, Error, ErrorKind, ListenMode, Result, Router, Server,
    Socket, SocketPoolConfig, State, Transport, core::ContextEndpoint,
};

const BYTES: usize = 64;
const GUARD: Duration = Duration::from_secs(5);
const METHODS: [&str; 4] = [
    "_ruapc.memory/read_inline",
    "_ruapc.memory/write_inline",
    "_ruapc.memory/request_is_pending",
    "_ruapc.memory/read_into_target",
];

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
enum Operation {
    Read,
    Write,
}

#[crate::service]
trait TimeoutRpc {
    async fn ping(&self, ctx: &Context, req: &()) -> Result<u64>;
    async fn transfer(&self, ctx: &Context, operation: &Operation) -> Result<()>;
}

struct Report {
    error: Option<ErrorKind>,
    parent_expired: bool,
    conn_id: u64,
}

struct Service {
    reports: mpsc::UnboundedSender<Report>,
    transport: Transport,
}

impl TimeoutRpc for Service {
    async fn ping(&self, ctx: &Context, _: &()) -> Result<u64> {
        Ok(connection_id(ctx))
    }

    async fn transfer(&self, ctx: &Context, operation: &Operation) -> Result<()> {
        if self.transport == Transport::HTTP {
            assert!(matches!(
                ctx.endpoint,
                ContextEndpoint::Connected(Socket::HTTP(crate::http::HttpSocket::Stream(_)))
            ));
        }
        let result = match operation {
            Operation::Read => ctx.remote_read_all().await.map(|_| ()),
            Operation::Write => ctx
                .remote_write_all(vec![buffer(&ctx.state)])
                .await
                .map(|_| ())
                .map_err(Error::from),
        };
        self.reports
            .send(Report {
                error: result.as_ref().err().map(|error| error.kind.clone()),
                parent_expired: ctx.is_expired(),
                conn_id: connection_id(ctx),
            })
            .unwrap();
        result
    }
}

fn connection_id(ctx: &Context) -> u64 {
    let ContextEndpoint::Connected(socket) = &ctx.endpoint else {
        panic!("test requires a connected handler context");
    };
    socket
        .conn_id()
        .expect("test requires a bidirectional stream")
}

fn buffer(state: &State) -> Buffer {
    let mut buffer = state.buffer_pool.allocate(BYTES).unwrap();
    buffer.set_len(BYTES);
    buffer.fill(0x5a);
    buffer
}

struct Fixture {
    server: Server,
    client: Context,
    transport: Transport,
    reports: mpsc::UnboundedReceiver<Report>,
    dropped: mpsc::UnboundedReceiver<(&'static str, u32)>,
}

impl Fixture {
    async fn new(transport: Transport) -> Self {
        let config = if transport == Transport::RDMA {
            let mut config = crate::rdma::test_utils::small_rdma_config();
            let rdma = config.rdma.as_mut().unwrap();
            rdma.remote_memory.read_timeout_ms = 0;
            rdma.remote_memory.bandwidth_limit_ratio = 0.0;
            // The fixture also disables preconnect, maintenance and probes:
            // only request/response deadlines may resolve the injected fault.
            config
        } else {
            SocketPoolConfig {
                listen_mode: ListenMode::UNIFIED,
                buffer_pool_memory: 128 << 20,
                rdma: None,
                ..Default::default()
            }
        };
        let (reports_tx, reports) = mpsc::unbounded_channel();
        let mut router = Router::default();
        TimeoutRpc::ruapc_export(
            Arc::new(Service {
                reports: reports_tx,
                transport,
            }),
            &mut router,
        );
        let server = Server::create(router, &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let (dropped_tx, dropped) = mpsc::unbounded_channel();
        let mut router = Router::default();
        for method in METHODS {
            let dropped = dropped_tx.clone();
            router.wrap_method(method, move |_| {
                Box::new(move |ctx, _| {
                    dropped.send((method, ctx.msg_meta.timeout_ms)).unwrap();
                    Ok(())
                })
            });
        }
        let client = Context::create_with_router(router, &config)
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [address]).unwrap());
        Client {
            timeout: GUARD,
            ..Default::default()
        }
        .ping(&client, &())
        .await
        .unwrap();
        Self {
            server,
            client,
            transport,
            reports,
            dropped,
        }
    }

    async fn check(&mut self, operation: Operation, parent_timeout: Duration) {
        let client = Client {
            timeout: parent_timeout,
            max_retries: 0,
            ..Default::default()
        };
        let mut attached = match operation {
            Operation::Read => client.with_read_buffer(buffer(&self.client.state)),
            Operation::Write => client.with_write_buffers(vec![buffer(&self.client.state)]),
        };
        // Keep the read source wrapper alive until both observations settle.
        // A client-side timeout alone does not prove that the server stopped.
        let (result, report) = tokio::time::timeout(GUARD, async {
            tokio::join!(
                attached.transfer(&self.client, &operation),
                self.reports.recv(),
            )
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{:?} {operation:?}: server API stayed pending",
                self.transport
            )
        });
        assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
        let report = report.expect("server must independently report its remote I/O result");
        assert_eq!(report.error, Some(ErrorKind::Timeout));
        let default_timeout = Client::default().timeout;
        assert_eq!(report.parent_expired, parent_timeout < default_timeout);
        let (method, wire_budget) = self.dropped.try_recv().unwrap();
        let expected_method = match (self.transport, operation) {
            (Transport::RDMA, Operation::Read) => METHODS[2],
            (Transport::RDMA, Operation::Write) => METHODS[3],
            (_, Operation::Read) => METHODS[0],
            (_, Operation::Write) => METHODS[1],
        };
        assert_eq!(method, expected_method);
        assert!(wire_budget > 0);
        assert!(u128::from(wire_budget) <= parent_timeout.min(default_timeout).as_millis());
        assert!(
            self.dropped.try_recv().is_err(),
            "accepted RPCs must not retry"
        );
        assert_eq!(self.server.state().waiter.pending_count(), 0);
        assert_eq!(self.client.state.waiter.pending_count(), 0);
        let recovered = match operation {
            Operation::Read => attached.take_read_buffers(),
            Operation::Write => attached.take_write_buffers(),
        }
        .expect("no DMA or inline copy remains after the injected reverse-RPC fault");
        assert_eq!(recovered.len(), 1);
        assert!(recovered[0].iter().all(|&byte| byte == 0x5a));
        // A request deadline must not tear down the healthy connection.
        assert_eq!(
            Client::default().ping(&self.client, &()).await.unwrap(),
            report.conn_id,
            "request expiry must preserve the same connection"
        );
    }

    async fn shutdown(self) {
        self.server.stop();
        self.client.state.socket_pool.stop();
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(self.server.join(), self.client.state.socket_pool.join());
        })
        .await
        .expect("test connections must shut down");
    }
}

async fn check_transports(parent_timeout: Duration) {
    for transport in [
        Transport::TCP,
        Transport::WS,
        Transport::HTTP,
        Transport::RDMA,
    ] {
        let mut fixture = Fixture::new(transport).await;
        for operation in [Operation::Read, Operation::Write] {
            fixture.check(operation, parent_timeout).await;
        }
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn parent_deadline_returns_from_remote_io_when_reverse_replies_are_lost() {
    check_transports(Duration::from_millis(250)).await;
}

#[tokio::test]
async fn reverse_rpc_timeout_returns_from_remote_io_before_parent_deadline() {
    check_transports(Duration::from_secs(10)).await;
}
