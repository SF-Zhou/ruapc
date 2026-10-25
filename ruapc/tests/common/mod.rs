use ruapc::{Endpoint, Transport};

pub fn endpoint(transport: Transport, address: std::net::SocketAddr) -> Endpoint {
    if transport == Transport::RDMA {
        Endpoint::rdma(Transport::TCP, [address]).unwrap()
    } else {
        Endpoint::new(transport, [address]).unwrap()
    }
}
