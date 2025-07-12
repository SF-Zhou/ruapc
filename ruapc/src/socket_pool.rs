use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::{
    Router, Socket,
    context::{Context, SocketEndpoint},
    error::{Error, ErrorKind, Result},
};

#[derive(Default)]
pub struct SocketPool {
    map: Mutex<HashMap<SocketAddr, Vec<TcpStream>>>,
    router: Router,
}

impl SocketPool {
    #[must_use]
    pub fn create_for_server(router: Router) -> Self {
        Self {
            map: Mutex::default(),
            router,
        }
    }

    /// # Errors
    pub async fn acquire_socket(self: &Arc<Self>, addr: SocketAddr) -> Result<Socket> {
        let mut map = self.map.lock().await;
        let stream = if let Some(stream) = map.get_mut(&addr).and_then(std::vec::Vec::pop) {
            stream
        } else {
            drop(map);
            TcpStream::connect(addr)
                .await
                .map_err(|e| Error::new(ErrorKind::TcpConnectFailed, e.to_string()))?
        };

        Ok(Socket {
            socket_pool: self.clone(),
            tcp_stream: Some(stream),
            for_send: true,
        })
    }

    pub fn add_socket_for_send(self: &Arc<Self>, stream: TcpStream) {
        let this = self.clone();
        if let Ok(peer_addr) = stream.peer_addr() {
            tokio::spawn(async move {
                let mut map = this.map.lock().await;
                map.entry(peer_addr).or_default().push(stream);
            });
        }
    }

    pub fn add_socket_for_recv(self: &Arc<Self>, stream: TcpStream) {
        let socket = Socket {
            socket_pool: self.clone(),
            tcp_stream: Some(stream),
            for_send: false,
        };
        let this = self.clone();
        tokio::spawn(async move {
            let _ = this.handle_request(socket).await;
        });
    }

    /// # Errors
    async fn handle_request(self: &Arc<Self>, mut socket: Socket) -> Result<()> {
        let msg = socket.recv().await?;
        let ctx = Context {
            socket_pool: self.clone(),
            endpoint: SocketEndpoint::Connected(socket),
        };
        self.router.dispatch(ctx, msg);
        Ok(())
    }
}
