//! Only locally observed transport failures carry local diagnostic provenance.
#![forbid(unsafe_code)]

use ruapc::{
    Client, Context, Endpoint, Error, ErrorKind, LocalFailure, Router, Server, SocketPoolConfig,
};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpSocket, TcpStream},
    sync::Semaphore,
};

#[ruapc::service]
trait Origin {
    async fn echo(&self, _: &Context, _: &()) -> ruapc::Result<()>;
    async fn wait(&self, _: &Context, _: &()) -> ruapc::Result<()>;
    async fn fail(&self, _: &Context, request: &u8) -> ruapc::Result<()>;
}

struct Service {
    calls: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
    finished: AtomicBool,
    refused: SocketAddr,
    forwarded_local_origin: AtomicBool,
}

impl Origin for Service {
    async fn echo(&self, _: &Context, _: &()) -> ruapc::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn wait(&self, _: &Context, _: &()) -> ruapc::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn fail(&self, _: &Context, request: &u8) -> ruapc::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match request {
            0 => Err(Error::new(
                ErrorKind::Timeout,
                "remote timeout claim".into(),
            )),
            1 => Err(Error::new(
                ErrorKind::TcpConnectFailed,
                "remote connection-refused claim".into(),
            )),
            2 => {
                // This error really is local to the server's nested call. Its
                // marker must disappear when forwarded across a new RPC hop.
                let ctx = context(&SocketPoolConfig::default(), self.refused);
                let error = client().echo(&ctx, &()).await.unwrap_err();
                self.forwarded_local_origin.store(
                    matches!(
                        error.local_failure(),
                        Some(LocalFailure::TcpConnect(
                            std::io::ErrorKind::ConnectionRefused
                        ))
                    ),
                    Ordering::SeqCst,
                );
                Err(error)
            }
            _ => unreachable!("test request"),
        }
    }
}

struct Running(Server);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.stop();
    }
}

impl Running {
    async fn start(refused: SocketAddr) -> (Self, Arc<Service>, SocketAddr) {
        let service = Arc::new(Service {
            calls: AtomicUsize::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            finished: AtomicBool::new(false),
            refused,
            forwarded_local_origin: AtomicBool::new(false),
        });
        let mut router = Router::default();
        service.clone().ruapc_export(&mut router);
        let server = Server::create(router, &SocketPoolConfig::default()).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        (Self(server), service, address)
    }

    async fn stop(self) {
        self.0.stop();
        tokio::time::timeout(Duration::from_secs(3), self.0.join())
            .await
            .expect("server shutdown");
    }
}

fn client() -> Client {
    Client {
        max_retries: 0,
        timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

fn context(config: &SocketPoolConfig, address: SocketAddr) -> Context {
    Context::create(config)
        .unwrap()
        .with_endpoint(Endpoint::tcp(address))
}

fn refused_address() -> (TcpSocket, SocketAddr) {
    // Keeping a bound, non-listening socket avoids the free-port race caused by
    // dropping a listener before attempting the refused connection.
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = socket.local_addr().unwrap();
    (socket, address)
}

#[tokio::test]
async fn actual_loopback_refusal_carries_local_os_connect_origin() {
    let (_reserved, address) = refused_address();
    let ctx = context(&SocketPoolConfig::default(), address);
    let error = client().echo(&ctx, &()).await.unwrap_err();
    assert_eq!(error.kind, ErrorKind::TcpConnectFailed);
    assert_eq!(
        error.local_failure(),
        Some(LocalFailure::TcpConnect(
            std::io::ErrorKind::ConnectionRefused
        ))
    );
}

#[tokio::test]
async fn saturated_loopback_backlog_tags_only_the_actual_connect_deadline() {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let address = listener.local_addr().unwrap();
    let mut queued = Vec::new();
    let mut saturated = false;
    // Keep completed connections in the listener's unaccepted queue. Detect
    // saturation with an actual bounded connect instead of assuming the OS
    // implements the requested backlog as an exact queue length.
    for _ in 0..16 {
        match tokio::time::timeout(Duration::from_millis(100), TcpStream::connect(address)).await {
            Ok(Ok(stream)) => queued.push(stream),
            Ok(Err(error)) => panic!("loopback backlog setup failed: {error}"),
            Err(_) => {
                saturated = true;
                break;
            }
        }
    }
    assert!(
        saturated,
        "OS did not expose a bounded saturated listen backlog"
    );
    assert!(!queued.is_empty());
    let ctx = context(&SocketPoolConfig::default(), address);
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        Client {
            connect_timeout: Duration::from_millis(100),
            ..client()
        }
        .echo(&ctx, &()),
    )
    .await
    .expect("finite TCP connect budget")
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_eq!(
        error.local_failure(),
        Some(LocalFailure::TcpConnect(std::io::ErrorKind::TimedOut))
    );
    // Neither listener nor its pending connections may close before the probe:
    // doing so would turn this into a connection-refused test.
    drop((listener, queued));
}

#[tokio::test]
async fn response_deadline_is_local_wait_origin_and_does_not_cancel_handler() {
    let (_reserved, refused) = refused_address();
    let (server, service, address) = Running::start(refused).await;
    let ctx = context(&SocketPoolConfig::default(), address);
    client().echo(&ctx, &()).await.unwrap();
    let request = {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            Client {
                timeout: Duration::from_millis(100),
                max_retries: 2,
                ..client()
            }
            .wait(&ctx, &())
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), service.entered.acquire())
        .await
        .expect("handler entered")
        .unwrap()
        .forget();
    let error = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .expect("local response wait expired")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert_eq!(error.local_failure(), Some(LocalFailure::ResponseWait));
    assert_eq!(service.calls.load(Ordering::SeqCst), 2);
    assert!(!service.finished.load(Ordering::SeqCst));
    service.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !service.finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler completed after caller timeout");
    server.stop().await;
}

#[tokio::test]
async fn remote_errors_never_inherit_local_origin_even_when_forwarding_a_real_failure() {
    let (_reserved, refused) = refused_address();
    let (server, service, address) = Running::start(refused).await;
    let ctx = context(&SocketPoolConfig::default(), address);
    for use_msgpack in [true, false] {
        let client = Client {
            use_msgpack,
            ..client()
        };
        for (request, kind) in [
            (0, ErrorKind::Timeout),
            (1, ErrorKind::TcpConnectFailed),
            (2, ErrorKind::TcpConnectFailed),
        ] {
            let error = client.fail(&ctx, &request).await.unwrap_err();
            assert_eq!(error.kind, kind);
            assert_eq!(error.local_failure(), None);
        }
    }
    assert!(service.forwarded_local_origin.load(Ordering::SeqCst));
    assert_eq!(service.calls.load(Ordering::SeqCst), 6);
    server.stop().await;
}
