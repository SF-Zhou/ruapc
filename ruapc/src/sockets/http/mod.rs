mod http_socket;
pub(crate) use http_socket::{HttpSocket, StreamSocketInner};

mod http_socket_pool;
pub(crate) use http_socket_pool::HttpSocketPool;
