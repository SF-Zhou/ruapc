use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::{Router, SocketPool};

pub struct Server {
    socket_pool: Arc<SocketPool>,
    stop_token: CancellationToken,
}

impl Server {
    #[must_use]
    pub fn create(router: Router) -> Self {
        let socket_pool = SocketPool::create_for_server(router);

        Self {
            socket_pool: Arc::new(socket_pool),
            stop_token: CancellationToken::default(),
        }
    }

    pub fn stop(&self) {
        self.stop_token.cancel();
    }

    /// # Errors
    pub async fn listen(
        &self,
        addr: SocketAddr,
    ) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let listener_addr = listener.local_addr()?;
        let stop_token = self.stop_token.clone();
        let socket_pool = self.socket_pool.clone();

        let listen_routine = tokio::spawn(async move {
            tokio::select! {
                () = stop_token.cancelled() => {
                    tracing::info!("stop accept loop");
                }
                () = async {
                    tracing::info!("start listening: {listener_addr}");
                    while let Ok((stream, _)) = listener.accept().await {
                        socket_pool.add_socket_for_recv(stream);
                    }
                } => {}
            }
        });

        Ok((listener_addr, listen_routine))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}
