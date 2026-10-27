//! HTTP routing and unary JSON responses, separate from connection management.

use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use http_body_util::{BodyExt, Either, Full};
use hyper::{Request, Response, body::Incoming};

use super::{
    HttpSocketPool,
    http_socket::{ChannelBody, HttpSocket},
};
use crate::{
    Error, ErrorKind, Message, MsgFlags, MsgMeta, RawStream, Result, Socket, State,
    TaskSupervisorHandle,
};

/// Immutable routing configuration shared by all connections in this pool.
/// Only the HTML depends solely on this configuration; OpenAPI is read from
/// the current router when requested.
pub(super) struct HttpService {
    base_path: String,
    rapidoc: OnceLock<Bytes>,
}

impl HttpService {
    pub(super) fn new(base_path: String) -> Self {
        Self {
            base_path,
            rapidoc: OnceLock::new(),
        }
    }

    pub(super) fn base_path(&self) -> &str {
        &self.base_path
    }

    pub(super) async fn handle_request(
        self: Arc<Self>,
        mut req: Request<Incoming>,
        state: Arc<State>,
        addr: SocketAddr,
        supervisor: TaskSupervisorHandle,
    ) -> Result<Response<Either<Full<Bytes>, ChannelBody>>> {
        if hyper_tungstenite::is_upgrade_request(&req) {
            let ws_config = crate::sockets::ws::web_socket_config();
            let (response, websocket) = hyper_tungstenite::upgrade(&mut req, Some(ws_config))
                .map_err(|e| Error::new(ErrorKind::HttpUpgradeFailed, e.to_string()))?;

            let state = state.clone();
            let _ = supervisor.try_spawn(async move {
                match websocket.await {
                    Ok(socket) => {
                        state
                            .handle_new_stream(RawStream::WS(Box::new(socket)), addr)
                            .await;
                    }
                    Err(err) => tracing::error!("upgrade HTTP to WebSocket failed: {err}"),
                }
            });

            return Ok(response.map(Either::Left));
        }

        let Some(path) = self.strip_base_path(req.uri().path()) else {
            return Ok(Self::not_found());
        };

        // Handle /_rpc: bidirectional streaming for reverse RPC.
        if req.method() == hyper::Method::POST && path == "/_rpc" {
            return HttpSocketPool::handle_rpc_stream(req, state, addr, &supervisor);
        }

        if req.method() == hyper::Method::GET {
            match path {
                "/openapi.json" => {
                    let openapi_json = serde_json::to_string_pretty(&state.router.openapi)?;
                    return Ok(Response::builder()
                        .header("Content-Type", "application/json")
                        .body(Either::Left(Full::new(Bytes::from(openapi_json))))
                        .unwrap());
                }
                "/rapidoc/rapidoc-min.js" => {
                    return Ok(Response::builder()
                        .header("Content-Type", "application/javascript")
                        .body(Either::Left(Full::new(Bytes::from(include_str!(
                            "rapidoc/rapidoc-min.js"
                        )))))
                        .unwrap());
                }
                "/rapidoc" | "/rapidoc/" | "/rapidoc/index.html" => {
                    let html = self.rapidoc.get_or_init(|| {
                        Bytes::from(
                            include_str!("rapidoc/index.html")
                                .replace("{{BASE_PATH}}", &self.base_path),
                        )
                    });
                    return Ok(Response::builder()
                        .header("Content-Type", "text/html; charset=utf-8")
                        .body(Either::Left(Full::new(html.clone())))
                        .unwrap());
                }
                _ => {
                    return Ok(Self::not_found());
                }
            }
        }

        // Unary HTTP is part of the public RPC surface. Internal control
        // methods remain available to trusted peers over the framed `/_rpc`
        // stream, but must not be callable (or discoverable) as ordinary
        // HTTP endpoints.
        let method = path.trim_start_matches('/');
        if req.method() != hyper::Method::POST || !state.router.is_public_method(method) {
            return Ok(Self::not_found());
        }

        let method = method.to_owned();
        // Cap the request body at the wire-format message limit; an
        // unauthenticated POST must not be able to buffer unbounded data.
        let limited =
            http_body_util::Limited::new(req.into_body(), crate::msg::frame::MAX_MSG_SIZE);
        let bytes = match limited.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) if e.is::<http_body_util::LengthLimitError>() => {
                return Ok(Response::builder()
                    .status(413)
                    .body(Either::Left(Full::new(Bytes::from("Payload Too Large"))))
                    .unwrap());
            }
            Err(_) => {
                return Ok(Response::builder()
                    .status(500)
                    .body(Either::Left(Full::new(Bytes::from(
                        "Internal Server Error",
                    ))))
                    .unwrap());
            }
        };
        // A unary request starts its response budget when its complete body
        // arrives, just like framed TCP, WebSocket and HTTP/2 messages. Pending
        // uploads own no waiter entry and cannot expire a future handler's slot.
        const UNARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        let (msgid, rx) = state.waiter.alloc(UNARY_TIMEOUT);
        let meta = MsgMeta {
            method,
            flags: MsgFlags::IsReq,
            msgid,
            read_regions: Vec::new(),
            write_regions: Vec::new(),
            timeout_ms: u32::try_from(UNARY_TIMEOUT.as_millis()).unwrap_or(u32::MAX),
        };
        let msg = Message::new(meta, bytes.into());

        let socket = Socket::HTTP(HttpSocket::ForResponse(msgid));
        state.handle_recv(&socket, msg)?;

        let (msg, _write_buffer) = rx
            .recv()
            .await
            .map_err(|e| Error::new(ErrorKind::HttpWaitRspFailed, e.to_string()))?;

        Ok(Response::builder()
            .header("Content-Type", "application/json")
            .body(Either::Left(Full::new(msg.payload.into())))
            .unwrap())
    }

    fn strip_base_path<'a>(&self, path: &'a str) -> Option<&'a str> {
        if self.base_path.is_empty() {
            return Some(path);
        }

        let rest = path.strip_prefix(&self.base_path)?;
        if rest.is_empty() {
            Some("/")
        } else if rest.starts_with('/') {
            Some(rest)
        } else {
            None
        }
    }

    fn not_found() -> Response<Either<Full<Bytes>, ChannelBody>> {
        Response::builder()
            .status(404)
            .body(Either::Left(Full::new(Bytes::from("Not Found"))))
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SocketPoolConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn unary_body_collection_precedes_waiter_and_handler_budget() {
        let mut router = crate::Router::default();
        router.add_method::<(), (u32, u64, usize)>(
            "Budget/check",
            Box::new(|mut ctx, payload| {
                tokio::spawn(async move {
                    payload.deserialize::<()>(&ctx.msg_meta).unwrap();
                    let observed = (
                        ctx.msg_meta.timeout_ms,
                        ctx.remaining_time().unwrap().as_millis() as u64,
                        ctx.state.waiter.pending_count(),
                    );
                    ctx.send_rsp::<_, Error>(Ok(observed)).await;
                });
                Ok(())
            }),
        );
        let config = SocketPoolConfig {
            listen_mode: crate::ListenMode::HTTP,
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        let server = crate::Server::create(router, &config).unwrap();
        let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(
            b"POST /Budget/check HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        ).await.unwrap();

        // Hyper sends 100 Continue only when the body is polled. This proves
        // the request reached collection, without sleeps or a 30s timeout.
        let mut interim = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !interim.ends_with(b"\r\n\r\n") {
                interim.push(stream.read_u8().await.unwrap());
            }
        })
        .await
        .unwrap();
        assert!(interim.starts_with(b"HTTP/1.1 100 Continue"));
        assert_eq!(server.state().waiter.pending_count(), 0);

        stream.write_all(b"4\r\nnull\r\n0\r\n\r\n").await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_to_end(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        let body_start = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            + 4;
        let observed: Result<(u32, u64, usize)> =
            serde_json::from_slice(&response[body_start..]).unwrap();
        let (advertised, remaining, pending) = observed.unwrap();
        assert_eq!(advertised, 30_000);
        assert!((25_000..=30_000).contains(&remaining));
        assert_eq!(pending, 1);
        assert_eq!(server.state().waiter.pending_count(), 0);
        server.stop();
        server.join().await;
    }
}
