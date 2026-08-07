mod web_socket;
pub(crate) use web_socket::{WebSocket, WebSocketInner};

mod web_socket_pool;
pub(crate) use web_socket_pool::{WebSocketPool, web_socket_config};
