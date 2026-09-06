/// Applies RPC-friendly socket options to a TCP stream.
///
/// - `TCP_NODELAY`: RPC messages are latency-sensitive; Nagle's algorithm
///   would add up to an RTT of delay to small requests.
/// - TCP keepalive: detects silently vanished peers (power loss, NAT
///   drops) so half-open connections are torn down instead of hanging
///   until every request times out. Detection takes
///   `time + interval * retries` ≈ 50s at the settings below.
///
/// Failures are logged and ignored: a socket without these options is
/// degraded, not broken.
pub(crate) fn configure_stream(stream: &tokio::net::TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::warn!("failed to set TCP_NODELAY: {e}");
    }
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(30))
        .with_interval(std::time::Duration::from_secs(5));
    #[cfg(not(windows))]
    let keepalive = keepalive.with_retries(4);
    if let Err(e) = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive) {
        tracing::warn!("failed to set TCP keepalive: {e}");
    }
}

mod tcp_socket;
pub(crate) use tcp_socket::TcpSocket;

mod tcp_socket_pool;
pub(crate) use tcp_socket_pool::TcpSocketPool;
