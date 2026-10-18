//! Public server-side remote I/O must return when admission outlives its
//! request, even with the independent READ watchdog and peer probes disabled.

use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, mpsc};

use super::super::RdmaSocket;
use crate::{
    Buffer, Client, Context, CopyOp, Endpoint, ErrorKind, Result, ResultWithBuffers, Router,
    Server, Socket, SocketPoolConfig, Transport, core::ContextEndpoint, remote_memory::WriteTarget,
    services::ReadIntoTargetRequest,
};

const READ_PERMITS: u32 = 1;
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct BlockedAdmission {
    socket: Arc<RdmaSocket>,
    _permit: OwnedSemaphorePermit,
}

type AdmissionBlock = Arc<Mutex<Option<BlockedAdmission>>>;

fn connected_socket(ctx: &Context) -> &Arc<RdmaSocket> {
    let ContextEndpoint::Connected(Socket::RDMA(socket)) = &ctx.endpoint else {
        panic!("the fault must apply to the RDMA connection executing this handler");
    };
    socket
}

fn block_admission(ctx: &Context, blocked: &AdmissionBlock) {
    let socket = connected_socket(ctx);
    assert!(socket.read_timeout.is_none());
    assert!(ctx.deadline().is_some());
    // Client and server can use the same physical NIC with different State
    // instances. Take the semaphore of this exact receiving connection.
    let permit = socket
        .read_credits
        .device
        .clone()
        .try_acquire_many_owned(READ_PERMITS)
        .expect("the fixture starts with idle READ admission");
    assert!(
        blocked
            .lock()
            .unwrap()
            .replace(BlockedAdmission {
                socket: socket.clone(),
                _permit: permit,
            })
            .is_none()
    );
}

struct Completion {
    kind: ErrorKind,
    recovered: Option<Vec<Buffer>>,
}

struct TimeoutService {
    blocked: AdmissionBlock,
    completed: mpsc::UnboundedSender<Completion>,
}

#[crate::service]
trait AdmissionTimeoutRpc {
    async fn ping(&self, ctx: &Context, req: &()) -> Result<u64>;
    async fn read(&self, ctx: &Context, req: &()) -> Result<()>;
    async fn write(&self, ctx: &Context, req: &()) -> ResultWithBuffers<()>;
}

impl AdmissionTimeoutRpc for TimeoutService {
    async fn ping(&self, ctx: &Context, _: &()) -> Result<u64> {
        Ok(connected_socket(ctx).conn_id)
    }

    async fn read(&self, ctx: &Context, _: &()) -> Result<()> {
        block_admission(ctx, &self.blocked);
        let mut destination = ctx.state.buffer_pool.allocate(64).unwrap();
        destination.set_len(64);
        destination.fill(0xa5);
        let ops = [CopyOp::new(0, 0, 64)];
        let reading = ctx.remote_read(&ops, vec![destination]);
        tokio::pin!(reading);
        assert!(
            futures_util::poll!(&mut reading).is_pending(),
            "the handler must enter READ admission before its deadline"
        );
        let error = reading
            .await
            .expect_err("the NIC permit stays held until this call has returned");
        let (error, recovered) = error.into_parts();
        self.completed
            .send(Completion {
                kind: error.kind.clone(),
                recovered,
            })
            .unwrap();
        Err(error)
    }

    async fn write(&self, ctx: &Context, _: &()) -> ResultWithBuffers<()> {
        let mut source = ctx.state.buffer_pool.allocate(64).unwrap();
        source.set_len(64);
        source.fill(0x5a);
        let error = ctx
            .remote_write(&[CopyOp::new(0, 0, 64)], vec![source])
            .await
            .expect_err("the client cannot admit the reverse READ before its deadline");
        let (error, recovered) = error.into_parts();
        self.completed
            .send(Completion {
                kind: error.kind.clone(),
                recovered,
            })
            .unwrap();
        Err(error)
    }
}

struct Fixture {
    server: Server,
    context: Context,
    server_conn_id: u64,
    completed: mpsc::UnboundedReceiver<Completion>,
}

impl Fixture {
    async fn new(client_router: Router, blocked: AdmissionBlock) -> Self {
        let mut config: SocketPoolConfig = crate::rdma::test_utils::small_rdma_config();
        let rdma = config.rdma.as_mut().unwrap();
        rdma.remote_memory.read_timeout_ms = 0;
        rdma.remote_memory.max_inflight_read_wrs = READ_PERMITS;
        rdma.remote_memory.bandwidth_limit_ratio = 0.0;
        rdma.peers.connections_per_peer = 1;
        rdma.peers.preconnect_max_per_peer = 1;
        assert_eq!(rdma.peers.min_connections_per_remote_nic, 0);
        assert_eq!(rdma.maintenance.interval_ms, 0);
        assert_eq!(rdma.probe.interval_ms, 0);
        let (completed, receiver) = mpsc::unbounded_channel();
        let mut router = Router::default();
        AdmissionTimeoutRpc::ruapc_export(
            Arc::new(TimeoutService { blocked, completed }),
            &mut router,
        );
        let server = Server::create(router, &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let context = Context::create_with_router(client_router, &config)
            .unwrap()
            .with_endpoint(Endpoint::new(Transport::RDMA, [address]).unwrap());
        // Bootstrap and activation do not consume the short transfer budget.
        let server_conn_id = Client::default().ping(&context, &()).await.unwrap();
        Self {
            server,
            context,
            server_conn_id,
            completed: receiver,
        }
    }

    async fn handler_completed(&mut self) -> Completion {
        tokio::time::timeout(TEST_TIMEOUT, self.completed.recv())
            .await
            .expect("the server's remote I/O call itself must return")
            .expect("the server handler must report its result")
    }

    async fn shutdown(self) {
        self.context.state.socket_pool.stop();
        self.server.stop();
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(self.context.state.socket_pool.join(), self.server.join());
        })
        .await
        .expect("the fixture must shut down after fault injection is released");
    }
}

fn assert_admission_still_blocked(blocked: &AdmissionBlock) {
    let held = blocked.lock().unwrap();
    let held = held
        .as_ref()
        .expect("the real RDMA handler entered admission");
    assert_eq!(held.socket.read_credits.device.available_permits(), 0);
    assert_eq!(held.socket.queue_pair.pending_read_count(), 0);
    assert!(held.socket.state.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_remote_read_returns_when_nic_admission_exceeds_parent_deadline() {
    let blocked = AdmissionBlock::default();
    let mut fixture = Fixture::new(Router::default(), blocked.clone()).await;
    let context = fixture.context.clone();
    let caller = tokio::spawn(async move {
        let client = Client {
            timeout: REQUEST_TIMEOUT,
            max_retries: 0,
            ..Default::default()
        };
        let mut source = context.state.buffer_pool.allocate(64).unwrap();
        source.set_len(64);
        source.fill(0x5a);
        client
            .with_read_buffers(vec![source])
            .read(&context, &())
            .await
    });

    // The outer client's own Timeout is not evidence that its handler exited.
    // Keep the NIC fault installed until the handler reports remote_read's result.
    let completed = fixture.handler_completed().await;
    assert_eq!(completed.kind, ErrorKind::Timeout);
    let recovered = completed.recovered.expect("no destination reached DMA");
    assert_eq!(recovered[0].as_slice(), &[0xa5; 64]);
    assert_admission_still_blocked(&blocked);
    let result = tokio::time::timeout(TEST_TIMEOUT, caller)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);
    assert_eq!(
        Client::default().ping(&fixture.context, &()).await.unwrap(),
        fixture.server_conn_id,
        "request expiry must leave the established connection usable"
    );
    drop(recovered);
    drop(blocked.lock().unwrap().take());
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_remote_write_and_client_read_handler_return_at_parent_deadline() {
    let blocked = AdmissionBlock::default();
    let observed_target: Arc<Mutex<Option<Weak<WriteTarget>>>> = Arc::default();
    let mut router = Router::default();
    let gate = blocked.clone();
    let target_observer = observed_target.clone();
    router.wrap_method("_ruapc.memory/read_into_target", move |original| {
        Box::new(move |ctx, payload| {
            let request: ReadIntoTargetRequest = payload.deserialize(&ctx.msg_meta)?;
            let target = ctx
                .state
                .waiter
                .write_target(request.request_id)
                .expect("the reverse READ arrives while its original request is live");
            *target_observer.lock().unwrap() = Some(Arc::downgrade(&target));
            drop(target);
            block_admission(&ctx, &gate);
            original(ctx, payload)
        })
    });
    let mut fixture = Fixture::new(router, blocked.clone()).await;
    let context = fixture.context.clone();
    let caller = tokio::spawn(async move {
        let client = Client {
            timeout: REQUEST_TIMEOUT,
            max_retries: 0,
            ..Default::default()
        };
        let mut destination = context.state.buffer_pool.allocate(64).unwrap();
        destination.set_len(64);
        client
            .with_write_buffers(vec![destination])
            .write(&context, &())
            .await
    });

    // Prove that the actual reverse handler checked out its destination,
    // rather than merely timing out before it was dispatched.
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let target = observed_target
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade);
            if let Some(target) = target
                && target
                    .copy_in(0, &[])
                    .is_err_and(|e| e.kind == ErrorKind::BuffersInUse)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the client reverse handler must actually reach READ admission");
    let completed = fixture.handler_completed().await;
    assert_eq!(completed.kind, ErrorKind::Timeout);
    assert_admission_still_blocked(&blocked);
    let result = tokio::time::timeout(TEST_TIMEOUT, caller)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout);

    // A server-side reverse-RPC timeout alone could hide a stranded client
    // handler. Its target must also be released while NIC admission stays full.
    let target = observed_target.lock().unwrap().clone().unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while target.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the client READ handler must exit and release its target independently");
    assert_admission_still_blocked(&blocked);
    assert_eq!(
        Client::default().ping(&fixture.context, &()).await.unwrap(),
        fixture.server_conn_id
    );
    drop(completed);
    drop(blocked.lock().unwrap().take());
    fixture.shutdown().await;
}
