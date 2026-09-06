fn main() {
    println!("{{\"QueuePair\":{{\"size\":{},\"alignment\":{}}},\"CompletionQueue\":{{\"size\":{},\"alignment\":{}}},\"CompletionCursor\":{{\"size\":{},\"alignment\":{}}}}}",
        std::mem::size_of::<ruapc_rdma::QueuePair>(),
        std::mem::align_of::<ruapc_rdma::QueuePair>(),
        std::mem::size_of::<ruapc_rdma::CompletionQueue>(),
        std::mem::align_of::<ruapc_rdma::CompletionQueue>(),
        std::mem::size_of::<ruapc_rdma::CompletionCursor>(),
        std::mem::align_of::<ruapc_rdma::CompletionCursor>());
}
