//! Framework failures use the application's conversion, while handler errors
//! retain the application's own wire representation.

use std::{fmt, sync::Arc, time::Duration};

use ruapc::{
    BufferReply, Client, Context, Endpoint, Error, ErrorKind, ListenMode, Router, Server,
    SocketPoolConfig, Transport,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
enum AppError {
    Framework(Error),
    Rejected { code: u32 },
}

impl From<Error> for AppError {
    fn from(error: Error) -> Self {
        Self::Framework(error)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AppError {}

type AppResult = Result<u32, AppError>;
type BufferResult = Result<BufferReply<u32>, AppError>;

#[derive(Serialize, Deserialize, JsonSchema)]
enum Request {
    Value(u32),
    Reject,
    WrappedError,
    Panic,
}

#[ruapc::service]
trait FrameworkErrors {
    async fn run(&self, ctx: &Context, req: &Request) -> AppResult;
    async fn buffered(&self, ctx: &Context, req: &Request) -> BufferResult;
    async fn hold(&self, ctx: &Context, req: &()) -> AppResult;
}

#[derive(Default)]
struct Service {
    entered: Notify,
    release: Notify,
}

fn execute(req: &Request) -> AppResult {
    match req {
        Request::Value(value) => Ok(*value),
        Request::Reject => Err(AppError::Rejected { code: 42 }),
        Request::WrappedError => Err(Error::new(
            ErrorKind::InvalidArgument,
            "returned by the application".into(),
        )
        .into()),
        Request::Panic => panic!("framework error regression"),
    }
}

impl FrameworkErrors for Service {
    async fn run(&self, _: &Context, req: &Request) -> AppResult {
        execute(req)
    }

    async fn buffered(&self, ctx: &Context, req: &Request) -> BufferResult {
        Ok(ctx.sent_nothing().into_reply(execute(req)?))
    }

    async fn hold(&self, _: &Context, _: &()) -> AppResult {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(1)
    }
}

// These declarations deliberately disagree with the server's method directory
// and request schema, so failures occur before an application handler runs.
#[ruapc::service(name = "FrameworkErrors")]
trait IncompatibleClient {
    async fn run(&self, ctx: &Context, req: &bool) -> AppResult;
    async fn missing(&self, ctx: &Context, req: &()) -> AppResult;
}

fn config() -> SocketPoolConfig {
    SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    }
}

fn server(service: Arc<Service>, config: &SocketPoolConfig) -> Server {
    let mut router = Router::default();
    service.ruapc_export(&mut router);
    Server::create(router, config).unwrap()
}

fn framework_error(error: AppError, expected: ErrorKind) -> Error {
    let AppError::Framework(error) = error else {
        panic!("expected converted framework error, received {error:?}");
    };
    assert_eq!(error.kind, expected, "{error}");
    error
}

#[tokio::test]
async fn framework_and_application_errors_keep_their_types_across_transports_and_codecs() {
    let config = config();
    let server = server(Arc::new(Service::default()), &config);
    let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let base = Context::create(&config).unwrap();

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        let ctx = base.with_endpoint(Endpoint::new(transport, [addr]).unwrap());
        for use_msgpack in [true, false] {
            let client = Client {
                use_msgpack,
                timeout: Duration::from_secs(5),
                ..Default::default()
            };

            let error = IncompatibleClient::missing(&client, &ctx, &())
                .await
                .unwrap_err();
            let error = framework_error(error, ErrorKind::InvalidArgument);
            assert!(error.msg.contains("FrameworkErrors/missing"));

            let error = IncompatibleClient::run(&client, &ctx, &false)
                .await
                .unwrap_err();
            let expected = if use_msgpack {
                ErrorKind::DeserializeFailed
            } else {
                ErrorKind::SerdeJsonError
            };
            assert!(!framework_error(error, expected).msg.is_empty());

            let error = FrameworkErrors::run(&client, &ctx, &Request::Panic)
                .await
                .unwrap_err();
            let error = framework_error(error, ErrorKind::HandlerPanic);
            assert!(error.msg.contains("framework error regression"));

            assert_eq!(
                FrameworkErrors::run(&client, &ctx, &Request::Reject).await,
                Err(AppError::Rejected { code: 42 })
            );
            assert_eq!(
                FrameworkErrors::run(&client, &ctx, &Request::WrappedError).await,
                Err(AppError::Framework(Error::new(
                    ErrorKind::InvalidArgument,
                    "returned by the application".into(),
                )))
            );
            assert_eq!(
                FrameworkErrors::run(&client, &ctx, &Request::Value(7)).await,
                Ok(7)
            );

            // A framework failure also preserves the BufferReply contract and
            // returns untouched write destinations through the wrapper.
            let mut buffer = ctx.state.buffer_pool.allocate(4).unwrap();
            buffer[..4].copy_from_slice(b"kept");
            buffer.set_len(4);
            let attached = client.with_write_buffers(vec![buffer]);
            let error = attached.buffered(&ctx, &Request::Panic).await.unwrap_err();
            framework_error(error, ErrorKind::HandlerPanic);
            let recovered = attached.take_write_buffers().into_available().unwrap();
            assert_eq!(&recovered[0][..], b"kept");

            let attached = client.with_write_buffers(recovered);
            let reply = attached.buffered(&ctx, &Request::Value(9)).await.unwrap();
            assert_eq!(*reply.response(), 9);
            assert_eq!(&reply.destinations().available().unwrap()[0][..], b"kept");
        }
    }

    server.stop();
    server.join().await;
}

#[tokio::test]
async fn overload_is_converted_to_the_application_error() {
    let config = SocketPoolConfig {
        max_inflight_requests: 1,
        ..config()
    };

    for transport in [Transport::TCP, Transport::WS, Transport::HTTP] {
        for use_msgpack in [true, false] {
            let service = Arc::new(Service::default());
            let server = server(service.clone(), &config);
            let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
            let ctx = Context::create(&config)
                .unwrap()
                .with_endpoint(Endpoint::new(transport, [addr]).unwrap());
            let client = Client {
                use_msgpack,
                timeout: Duration::from_secs(5),
                ..Default::default()
            };

            let holding = {
                let client = client.clone();
                let ctx = ctx.clone();
                tokio::spawn(async move { client.hold(&ctx, &()).await })
            };
            tokio::time::timeout(Duration::from_secs(5), service.entered.notified())
                .await
                .unwrap();

            let error = FrameworkErrors::run(&client, &ctx, &Request::Value(7))
                .await
                .unwrap_err();
            let error = framework_error(error, ErrorKind::Overloaded);
            assert!(error.msg.contains("FrameworkErrors/run"));

            service.release.notify_one();
            assert_eq!(holding.await.unwrap(), Ok(1));
            server.stop();
            server.join().await;
        }
    }
}
