const MAGIC_NUM: u32 = u32::from_be_bytes(*b"RUA!");
const MAX_MSG_SIZE: usize = 64 << 20;

mod tcp_socket;
pub(crate) use tcp_socket::TcpSocket;

mod tcp_socket_pool;
pub(crate) use tcp_socket_pool::TcpSocketPool;
