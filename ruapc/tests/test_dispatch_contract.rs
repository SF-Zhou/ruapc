//! Framework failures retain their identity independently of application errors.

use std::{future::Future, sync::Arc, time::Duration};

use ruapc::{
    Client, Context, Error, ErrorKind, ListenMode, RawCall, Router, Server, SocketPoolConfig,
};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::Error as _};
use tokio::sync::Notify;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "source", content = "detail")]
enum AppError {
    Application(String),
    Framework(Error),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AppError {}

impl From<Error> for AppError {
    fn from(error: Error) -> Self {
        Self::Framework(error)
    }
}

#[derive(Serialize, JsonSchema)]
struct PanickingRequest;

impl<'de> Deserialize<'de> for PanickingRequest {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        panic!("request decoder panic")
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FailingResponse {
    panic: bool,
}

impl Serialize for FailingResponse {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        if self.panic {
            panic!("response encoder panic");
        }
        Err(S::Error::custom("response encoder failure"))
    }
}

#[ruapc::service]
trait DispatchContract {
    async fn body(&self, ctx: &Context, panic: &bool) -> Result<u32, AppError>;
    async fn construct(&self, ctx: &Context, req: &()) -> Result<u32, AppError>;
    async fn decode(&self, ctx: &Context, req: &PanickingRequest) -> Result<u32, AppError>;
    async fn encode(&self, ctx: &Context, panic: &bool) -> Result<FailingResponse, AppError>;
    async fn block(&self, ctx: &Context, req: &()) -> Result<u32, AppError>;
}

#[derive(Default)]
struct Handler {
    started: Notify,
    release: Notify,
}

impl DispatchContract for Handler {
    async fn body(&self, _: &Context, panic: &bool) -> Result<u32, AppError> {
        if *panic {
            panic!("handler body panic");
        }
        Err(AppError::Application("business rejection".into()))
    }

    fn construct(&self, _: &Context, _: &()) -> impl Future<Output = Result<u32, AppError>> + Send {
        // This executes before the returned future is constructed or polled.
        assert!(std::hint::black_box(false), "handler constructor panic");
        std::future::ready(Ok(7))
    }

    async fn decode(&self, _: &Context, _: &PanickingRequest) -> Result<u32, AppError> {
        unreachable!("a panicking decoder must prevent service execution")
    }

    async fn encode(&self, _: &Context, panic: &bool) -> Result<FailingResponse, AppError> {
        Ok(FailingResponse { panic: *panic })
    }

    async fn block(&self, _: &Context, _: &()) -> Result<u32, AppError> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(7)
    }
}

fn config() -> SocketPoolConfig {
    SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 16 << 20,
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    }
}

fn router(handler: Arc<Handler>) -> Router {
    let mut router = Router::default();
    DispatchContract::ruapc_export(handler, &mut router);
    router.add_method::<(), Result<u32, AppError>>(
        "Custom/error",
        Box::new(|_, _| Err(Error::kind(ErrorKind::InvalidArgument))),
    );
    router.add_method::<(), Result<u32, AppError>>(
        "Custom/panic",
        Box::new(|_, _| panic!("low-level callback panic")),
    );
    router.add_handler::<(), Result<u32, AppError>, _, _>(
        "Custom/constructor",
        |_, _| -> std::future::Ready<ruapc::Result<()>> {
            panic!("async callback constructor panic")
        },
    );
    router
}

fn assert_framework<T: std::fmt::Debug>(result: Result<T, AppError>, expected: ErrorKind) {
    match result.unwrap_err() {
        AppError::Framework(error) => assert_eq!(error.kind, expected, "{error}"),
        other => panic!("expected a converted framework error, got {other:?}"),
    }
}

#[tokio::test]
async fn framework_failures_preserve_custom_errors_across_stream_transports() {
    let config = config();
    let server = Server::create(router(Arc::new(Handler::default())), &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    for scheme in ["tcp", "ws", "http"] {
        let ctx = Context::create(&config)
            .unwrap()
            .with_endpoint(format!("{scheme}://{address}").parse().unwrap());
        for use_msgpack in [true, false] {
            let client = Client {
                use_msgpack,
                timeout: Duration::from_secs(2),
                max_retries: 0,
                ..Default::default()
            };
            let application = client.body(&ctx, &false).await;
            assert!(
                matches!(
                    &application,
                    Err(AppError::Application(message)) if message == "business rejection"
                ),
                "{scheme}, msgpack={use_msgpack}: {application:?}"
            );
            assert_framework(client.body(&ctx, &true).await, ErrorKind::HandlerPanic);
            assert_framework(client.construct(&ctx, &()).await, ErrorKind::HandlerPanic);
            assert_framework(
                client.decode(&ctx, &PanickingRequest).await,
                ErrorKind::HandlerPanic,
            );
            assert_framework(client.encode(&ctx, &true).await, ErrorKind::HandlerPanic);
            assert_framework(
                client.encode(&ctx, &false).await,
                if use_msgpack {
                    ErrorKind::SerializeFailed
                } else {
                    ErrorKind::SerdeJsonError
                },
            );
            for (method, expected) in [
                ("Custom/error", ErrorKind::InvalidArgument),
                ("Custom/panic", ErrorKind::HandlerPanic),
                ("Custom/constructor", ErrorKind::HandlerPanic),
                ("Missing/method", ErrorKind::InvalidArgument),
            ] {
                assert_framework(
                    client
                        .ruapc_raw_call::<(), u32, AppError>(&ctx, &(), None, method)
                        .await,
                    expected,
                );
            }
            assert_framework(
                client
                    .ruapc_raw_call::<u32, u32, AppError>(&ctx, &7, None, "DispatchContract/body")
                    .await,
                if use_msgpack {
                    ErrorKind::DeserializeFailed
                } else {
                    ErrorKind::SerdeJsonError
                },
            );
        }
    }
    server.stop();
    server.join().await;
}

#[tokio::test]
async fn overload_uses_the_framework_error_contract() {
    let config = SocketPoolConfig {
        max_inflight_requests: 1,
        ..config()
    };
    let handler = Arc::new(Handler::default());
    let server = Server::create(router(handler.clone()), &config).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let ctx = Context::create(&config)
        .unwrap()
        .with_endpoint(format!("tcp://{address}").parse().unwrap());
    let blocked = {
        let ctx = ctx.clone();
        tokio::spawn(async move { Client::default().block(&ctx, &()).await })
    };
    handler.started.notified().await;
    assert_framework(
        Client::default().body(&ctx, &false).await,
        ErrorKind::Overloaded,
    );
    handler.release.notify_one();
    assert_eq!(blocked.await.unwrap().unwrap(), 7);
    server.stop();
    server.join().await;
}

#[tokio::test]
async fn unary_http_distinguishes_framework_and_application_errors() {
    let server = Server::create(router(Arc::new(Handler::default())), &config()).unwrap();
    let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = format!("http://{address}/DispatchContract/body");
    let response = client.post(&url).json(&false).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(!response.headers().contains_key("x-ruapc-framework-error"));
    assert!(
        matches!(response.json::<Result<u32, AppError>>().await.unwrap(),
        Err(AppError::Application(message)) if message == "business rejection")
    );

    let response = client.post(&url).json(&true).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-ruapc-framework-error"], "true");
    assert_eq!(
        response
            .json::<ruapc::Result<()>>()
            .await
            .unwrap()
            .unwrap_err()
            .kind,
        ErrorKind::HandlerPanic
    );
    server.stop();
    server.join().await;
}
