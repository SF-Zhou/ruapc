//! One endpoint groups equivalent addresses of one server. Connections are
//! shared only by identical normalized groups, independently of address overlap.

#![forbid(unsafe_code)]

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use ruapc::{
    Client, Context, Endpoint, ListenMode, Result, Router, Server, SocketPoolConfig, Transport,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Barrier, oneshot},
    task::{JoinHandle, JoinSet},
};

const TRANSPORTS: [Transport; 3] = [Transport::TCP, Transport::WS, Transport::HTTP];

#[ruapc::service]
trait GroupService {
    async fn echo(&self, ctx: &Context, request: &u64) -> Result<u64>;
}

struct GroupServiceImpl;

impl GroupService for GroupServiceImpl {
    async fn echo(&self, _ctx: &Context, request: &u64) -> Result<u64> {
        Ok(*request)
    }
}

/// Counts physical connections without depending on private socket-pool state.
/// Both proxies forward to listeners owned by the same server and State.
struct Proxy {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Proxy {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    incoming = listener.accept() => {
                        let (mut incoming, _) = incoming.unwrap();
                        count.fetch_add(1, Ordering::SeqCst);
                        connections.spawn(async move {
                            let mut outgoing = TcpStream::connect(target).await.unwrap();
                            let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            drop(listener);
            // Closing the proxy must also drop every accepted stream before
            // the test issues its next request.
            connections.shutdown().await;
        });
        Self {
            addr,
            accepted,
            stop: Some(stop),
            task,
        }
    }

    fn connections(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    async fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        (&mut self.task).await.unwrap();
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn setup() -> (Server, Context, [Proxy; 2]) {
    let config = SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 64 << 20,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    };
    let mut router = Router::default();
    GroupService::ruapc_export(Arc::new(GroupServiceImpl), &mut router);
    let server = Server::create(router, &config).unwrap();
    let first = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let second = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let proxies = [Proxy::start(first).await, Proxy::start(second).await];
    let ctx = Context::create(&config).unwrap();
    (server, ctx, proxies)
}

fn client() -> Client {
    Client {
        connect_timeout: Duration::from_secs(2),
        timeout: Duration::from_secs(2),
        max_retries: 0,
        ..Default::default()
    }
}

#[tokio::test]
async fn address_failover_does_not_consume_request_retry_budget() {
    for transport in TRANSPORTS {
        let (server, base, mut proxies) = setup().await;
        proxies.sort_by_key(|proxy| proxy.addr);
        // The first normalized address is unavailable; the second reaches
        // the same server. One acquisition must try both even with no retries.
        proxies[0].close().await;
        let endpoint = Endpoint::new(transport, proxies.iter().map(|proxy| proxy.addr)).unwrap();
        let ctx = base.with_endpoint(endpoint);
        let client = client();
        assert_eq!(client.echo(&ctx, &11).await.unwrap(), 11, "{transport:?}");
        assert_eq!(proxies[1].connections(), 1, "{transport:?}");

        // A failed alternate address must not invalidate the live connection.
        let client = Client {
            connect_timeout: Duration::ZERO,
            ..client
        };
        for value in 12..20 {
            assert_eq!(
                client.echo(&ctx, &value).await.unwrap(),
                value,
                "{transport:?}"
            );
        }
        assert_eq!(proxies[1].connections(), 1, "{transport:?}");
        server.stop();
        server.join().await;
    }
}

#[tokio::test]
async fn disconnected_group_reconnects_through_another_address() {
    for transport in TRANSPORTS {
        let (server, base, mut proxies) = setup().await;
        proxies.sort_by_key(|proxy| proxy.addr);
        let endpoint = Endpoint::new(transport, proxies.iter().map(|proxy| proxy.addr)).unwrap();
        let ctx = base.with_endpoint(endpoint);
        let client = client();
        assert_eq!(client.echo(&ctx, &41).await.unwrap(), 41);
        assert_eq!(proxies[0].connections(), 1, "{transport:?}");
        assert_eq!(proxies[1].connections(), 0, "{transport:?}");

        // Await listener and forwarding-task teardown, not a fixed delay.
        proxies[0].close().await;
        tokio::time::timeout(Duration::from_secs(10), async {
            match client.echo(&ctx, &42).await {
                Ok(value) => assert_eq!(value, 42),
                Err(error) => {
                    // EOF may race with acquisition of the old connection.
                    // max_retries is zero: allow that call to fail, then make
                    // one explicitly new call with a different request value.
                    assert_eq!(
                        client.echo(&ctx, &43).await.unwrap_or_else(|next| {
                            panic!("{transport:?}: recovery failed after {error}: {next}")
                        }),
                        43
                    );
                }
            }
        })
        .await
        .expect("a fresh call should reconnect after the proxy closes");
        assert_eq!(proxies[0].connections(), 1, "{transport:?}");
        assert_eq!(proxies[1].connections(), 1, "{transport:?}");

        let client = Client {
            connect_timeout: Duration::ZERO,
            ..client
        };
        for value in 44..48 {
            assert_eq!(
                client.echo(&ctx, &value).await.unwrap(),
                value,
                "{transport:?}"
            );
        }
        assert_eq!(proxies[1].connections(), 1, "{transport:?}");
        server.stop();
        server.join().await;
    }
}

#[tokio::test]
async fn normalized_address_groups_reuse_one_connection() {
    for transport in TRANSPORTS {
        let (server, base, proxies) = setup().await;
        let [first, second] = proxies.each_ref().map(|proxy| proxy.addr);
        let original = Endpoint::new(transport, [first, second]).unwrap();
        let reordered = Endpoint::new(transport, [second, first, second]).unwrap();
        assert_eq!(original, reordered);
        assert_eq!(
            client()
                .echo(&base.with_endpoint(original), &21)
                .await
                .unwrap(),
            21
        );
        let client = Client {
            connect_timeout: Duration::ZERO,
            ..client()
        };
        assert_eq!(
            client
                .echo(&base.with_endpoint(reordered), &22)
                .await
                .unwrap(),
            22
        );
        assert_eq!(
            proxies.iter().map(Proxy::connections).sum::<usize>(),
            1,
            "{transport:?}"
        );
        server.stop();
        server.join().await;
    }
}

#[tokio::test]
async fn overlapping_groups_keep_independent_connections() {
    for transport in TRANSPORTS {
        let (server, base, proxies) = setup().await;
        let first = proxies.iter().map(|proxy| proxy.addr).min().unwrap();
        let group = Endpoint::new(transport, proxies.iter().map(|proxy| proxy.addr)).unwrap();
        let single = Endpoint::new(transport, [first]).unwrap();
        let group_ctx = base.with_endpoint(group.clone());
        let single_ctx = base.with_endpoint(single);
        let client = client();
        assert_eq!(client.echo(&group_ctx, &31).await.unwrap(), 31);
        assert_eq!(client.echo(&single_ctx, &32).await.unwrap(), 32);
        assert_eq!(
            proxies.iter().map(Proxy::connections).sum::<usize>(),
            2,
            "{transport:?}"
        );

        let client = Client {
            connect_timeout: Duration::ZERO,
            ..client
        };
        assert_eq!(
            client.echo(&base.with_endpoint(group), &33).await.unwrap(),
            33
        );
        assert_eq!(client.echo(&single_ctx, &34).await.unwrap(), 34);
        assert_eq!(
            proxies.iter().map(Proxy::connections).sum::<usize>(),
            2,
            "{transport:?}"
        );
        server.stop();
        server.join().await;
    }
}

#[tokio::test]
async fn concurrent_group_acquisition_opens_one_connection() {
    for transport in TRANSPORTS {
        let (server, base, proxies) = setup().await;
        let endpoint = Endpoint::new(transport, proxies.iter().map(|proxy| proxy.addr)).unwrap();
        let barrier = Arc::new(Barrier::new(32));
        let mut calls = JoinSet::new();
        for value in 0..32 {
            let ctx = base.with_endpoint(endpoint.clone());
            let barrier = barrier.clone();
            calls.spawn(async move {
                barrier.wait().await;
                assert_eq!(client().echo(&ctx, &value).await.unwrap(), value);
            });
        }
        while let Some(result) = calls.join_next().await {
            result.unwrap();
        }
        assert_eq!(
            proxies.iter().map(Proxy::connections).sum::<usize>(),
            1,
            "{transport:?}"
        );
        server.stop();
        server.join().await;
    }
}
