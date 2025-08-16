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

use super::http_socket::{Connections, HttpSocket};
use crate::{
    Error, ErrorKind, Message, MsgFlags, MsgMeta, RawStream, Result, Socket, State, TaskSupervisor,
};

pub struct HttpSocketPool {
    socket_map: RwLock<HashMap<SocketAddr, HttpSocket, RandomState>>,
    http: Builder,
    task_supervisor: TaskSupervisor,
}

impl HttpSocketPool {
    pub fn new() -> Arc<Self> {
        let mut http = Builder::new();
        http.keep_alive(true);
        Arc::new(Self {
            socket_map: RwLock::default(),
            http,
            task_supervisor: TaskSupervisor::create(),
        })
    }

    pub fn handle_new_stream(
        self: &Arc<Self>,
        state: &Arc<State>,
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
        let state = state.clone();
        let connection = self
            .http
            .serve_connection(
                TokioIo::new(tcp_stream),
                hyper::service::service_fn(move |req: Request<Incoming>| {
                    this.clone().handle_request(req, state.clone(), addr)
                }),
            )
            .with_upgrades();

        let task_supervisor = self.task_supervisor.start_async_task();
        tokio::spawn(async move {
            tokio::select! {
                () = task_supervisor.stopped() => {},
                r = connection => {
                    if let Err(e) = r {
                        tracing::error!("recv loop for {addr} failed: {e}");
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn handle_request(
        self: Arc<Self>,
        mut req: Request<Incoming>,
        state: Arc<State>,
        addr: SocketAddr,
    ) -> Result<Response<Full<Bytes>>> {
        if hyper_tungstenite::is_upgrade_request(&req) {
            let (response, websocket) = hyper_tungstenite::upgrade(&mut req, None)
                .map_err(|e| Error::new(ErrorKind::HttpUpgradeFailed, e.to_string()))?;

            let state = state.clone();
            tokio::spawn(async move {
                let websocket = match websocket.await {
                    Ok(socket) => socket,
                    Err(err) => {
                        tracing::error!("upgrade HTTP to WebSocket failed: {err}");
                        return;
                    }
                };
                state
                    .handle_new_stream(RawStream::WS(Box::new(websocket)), addr)
                    .await;
            });

            return Ok(response);
        }

        let (msgid, rx) = state.waiter.alloc();
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

        let socket = Socket::HTTP(HttpSocket::ForResponse(msgid));
        state.handle_recv(&socket, msg)?;

        let msg = rx
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;

        Ok(Response::new(Full::new(msg.payload.into())))
    }

    pub fn stop(&self) {
        self.task_supervisor.stop();
    }

    pub fn drop_guard(&self) -> DropGuard {
        self.task_supervisor.drop_guard()
    }

    pub async fn join(&self) {
        self.task_supervisor.all_stopped().await;
    }

    pub async fn acquire(
        self: &Arc<Self>,
        addr: &SocketAddr,
        _state: &Arc<State>,
    ) -> Result<HttpSocket> {
        // Check if the socket is already in the socket map.
        if let Ok(socket_map) = self.socket_map.try_read()
            && let Some(socket) = socket_map.get(addr)
        {
            return Ok(socket.clone());
        }

        // If not, create a new socket and insert it into the socket map.
        let mut socket_map = self.socket_map.write().await;
        if let Some(socket) = socket_map.get(addr) {
            return Ok(socket.clone());
        }

        let connections = Arc::new(Connections {
            addr: *addr,
            vec: Mutex::default(),
        });
        let socket = HttpSocket::ForRequest(connections);
        socket_map.insert(*addr, socket.clone());
        Ok(socket)
    }
}

impl std::fmt::Debug for HttpSocketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSocketPool").finish()
    }
}
