use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use foldhash::fast::RandomState;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    server::conn::http1::Builder,
};
use hyper_util::rt::TokioIo;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::DropGuard;

use crate::{
    BoxFuture, Error, ErrorKind, Message, MessageHandler, MsgFlags, MsgMeta, RawStream, Result,
    RuapcSocket, RuapcSocketPool, SocketType, TaskSupervisor,
    http_socket::{Connections, HttpSocket},
};

/// Trait for handling HTTP-specific requests.
///
/// This trait allows the HTTP transport to delegate certain request handling
/// back to the main RPC layer (e.g., for serving OpenAPI documentation or
/// handling WebSocket upgrades).
pub trait HttpRequestHandler: Send + Sync + 'static {
    /// Returns the OpenAPI JSON specification, if available.
    fn get_openapi_json(&self) -> Option<String>;

    /// Handles a WebSocket upgrade request.
    fn handle_websocket_upgrade(
        &self,
        stream: RawStream,
        addr: SocketAddr,
    ) -> BoxFuture<'_, Result<()>>;

    /// Allocates a message ID and returns a receiver for the response.
    fn alloc_waiter(&self) -> (u64, tokio::sync::oneshot::Receiver<Message>);

    /// Posts a response message to the waiter.
    fn post_waiter(&self, msgid: u64, msg: Message);
}

/// Inner state for the HTTP socket pool.
struct HttpSocketPoolInner {
    socket_map: RwLock<HashMap<SocketAddr, HttpSocket, RandomState>>,
    http: Builder,
    task_supervisor: TaskSupervisor,
}

/// HTTP socket pool for managing HTTP connections.
///
/// This pool manages HTTP connections for both client and server modes:
/// - Client mode: Connection pooling for making HTTP requests
/// - Server mode: HTTP server handling incoming connections
#[derive(Clone)]
pub struct HttpSocketPool {
    inner: Arc<HttpSocketPoolInner>,
}

impl HttpSocketPool {
    /// Creates a new HTTP socket pool.
    pub fn new() -> Self {
        let mut http = Builder::new();
        http.keep_alive(true);
        Self {
            inner: Arc::new(HttpSocketPoolInner {
                socket_map: RwLock::default(),
                http,
                task_supervisor: TaskSupervisor::create(),
            }),
        }
    }

    /// Handles a new incoming HTTP stream.
    pub fn handle_new_stream_with_handler(
        &self,
        handler: Arc<dyn MessageHandler>,
        request_handler: Arc<dyn HttpRequestHandler>,
        stream: RawStream,
        addr: SocketAddr,
    ) -> Result<()> {
        let RawStream::TCP(tcp_stream) = stream else {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "invalid socket type".into(),
            ));
        };

        let this = self.clone();
        let connection = self
            .inner
            .http
            .serve_connection(
                TokioIo::new(tcp_stream),
                hyper::service::service_fn(move |req: Request<Incoming>| {
                    this.clone()
                        .handle_request(req, handler.clone(), request_handler.clone(), addr)
                }),
            )
            .with_upgrades();

        let task_guard = self.inner.task_supervisor.start_async_task();
        tokio::spawn(async move {
            tokio::select! {
                () = task_guard.stopped() => {},
                r = connection => {
                    if let Err(e) = r {
                        tracing::error!("recv loop for {addr} failed: {e}");
                    }
                }
            }
        });

        Ok(())
    }

    /// Handles an HTTP request.
    pub async fn handle_request(
        self,
        mut req: Request<Incoming>,
        handler: Arc<dyn MessageHandler>,
        request_handler: Arc<dyn HttpRequestHandler>,
        addr: SocketAddr,
    ) -> Result<Response<Full<Bytes>>> {
        // Handle WebSocket upgrade
        if hyper_tungstenite::is_upgrade_request(&req) {
            let (response, websocket) = hyper_tungstenite::upgrade(&mut req, None)
                .map_err(|e| Error::new(ErrorKind::HttpUpgradeFailed, e.to_string()))?;

            let request_handler = request_handler.clone();
            tokio::spawn(async move {
                let websocket = match websocket.await {
                    Ok(socket) => socket,
                    Err(err) => {
                        tracing::error!("upgrade HTTP to WebSocket failed: {err}");
                        return;
                    }
                };
                let _ = request_handler
                    .handle_websocket_upgrade(RawStream::WS(Box::new(websocket)), addr)
                    .await;
            });

            return Ok(response);
        }

        // Handle GET requests for static content
        if req.method() == hyper::Method::GET {
            match req.uri().path() {
                "/openapi.json" => {
                    if let Some(openapi_json) = request_handler.get_openapi_json() {
                        return Ok(Response::builder()
                            .header("Content-Type", "application/json")
                            .body(Full::new(Bytes::from(openapi_json)))
                            .unwrap());
                    }
                }
                "/rapidoc/rapidoc-min.js" => {
                    return Ok(Response::builder()
                        .header("Content-Type", "application/javascript")
                        .body(Full::new(Bytes::from(crate::rapidoc::RAPIDOC_MIN_JS)))
                        .unwrap());
                }
                "/rapidoc" | "/rapidoc/" | "/rapidoc/index.html" => {
                    return Ok(Response::builder()
                        .header("Content-Type", "text/html; charset=utf-8")
                        .body(Full::new(Bytes::from(crate::rapidoc::INDEX_HTML)))
                        .unwrap());
                }
                _ => {
                    return Ok(Response::builder()
                        .status(404)
                        .body(Full::new(Bytes::from("Not Found")))
                        .unwrap());
                }
            }
        }

        // Handle RPC POST requests
        let (msgid, rx) = request_handler.alloc_waiter();
        let meta = MsgMeta {
            method: req.uri().path().trim_start_matches('/').to_string(),
            flags: MsgFlags::IsReq,
            msgid,
        };
        let bytes = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                return Ok(Response::builder()
                    .status(500)
                    .body(Full::new(Bytes::from("Internal Server Error")))
                    .unwrap());
            }
        };
        let msg = Message::new(meta, bytes.into());

        let socket: Box<dyn RuapcSocket> = Box::new(HttpSocket::ForResponse(msgid));
        handler.handle_recv(socket.as_ref(), msg)?;

        let msg: Message = rx
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;

        Ok(Response::builder()
            .header("Content-Type", "application/json")
            .body(Full::new(msg.payload.into()))
            .unwrap())
    }

    async fn acquire_internal(
        &self,
        addr: SocketAddr,
        _handler: Arc<dyn MessageHandler>,
    ) -> Result<Box<dyn RuapcSocket>> {
        // Check if the socket is already in the socket map.
        if let Ok(socket_map) = self.inner.socket_map.try_read()
            && let Some(socket) = socket_map.get(&addr)
        {
            return Ok(Box::new(socket.clone()));
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.inner.socket_map.write().await;
        if let Some(socket) = socket_map.get(&addr) {
            return Ok(Box::new(socket.clone()));
        }

        let connections = Arc::new(Connections {
            addr,
            vec: Mutex::default(),
        });
        let socket = HttpSocket::ForRequest(connections);
        socket_map.insert(addr, socket.clone());
        Ok(Box::new(socket))
    }
}

impl Default for HttpSocketPool {
    fn default() -> Self {
        Self::new()
    }
}

impl RuapcSocketPool for HttpSocketPool {
    fn socket_type(&self) -> SocketType {
        SocketType::HTTP
    }

    fn acquire(
        &self,
        addr: SocketAddr,
        handler: Arc<dyn MessageHandler>,
    ) -> BoxFuture<'_, Result<Box<dyn RuapcSocket>>> {
        Box::pin(self.acquire_internal(addr, handler))
    }

    fn handle_new_stream(
        &self,
        _handler: Arc<dyn MessageHandler>,
        _stream: RawStream,
        _addr: SocketAddr,
    ) -> BoxFuture<'_, Result<()>> {
        // HTTP needs a custom handler, so this is a placeholder
        Box::pin(async {
            Err(Error::new(
                ErrorKind::InvalidArgument,
                "Use handle_new_stream_with_handler for HTTP".into(),
            ))
        })
    }

    fn stop(&self) {
        self.inner.task_supervisor.stop();
    }

    fn drop_guard(&self) -> DropGuard {
        self.inner.task_supervisor.drop_guard()
    }

    fn join(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.inner.task_supervisor.all_stopped())
    }
}

impl std::fmt::Debug for HttpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSocketPool").finish()
    }
}
