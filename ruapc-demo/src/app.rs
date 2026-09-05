//! Shared process and transport setup for the demo binaries.

use ruapc::SocketPoolConfig;

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

#[derive(clap::Args, Debug, Clone)]
pub struct RuntimeOptions {
    /// Tokio worker threads per runtime (0 = number of CPUs).
    #[arg(long, default_value = "0")]
    pub worker_threads: usize,
}

impl RuntimeOptions {
    pub fn build(&self) -> tokio::runtime::Runtime {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        if self.worker_threads > 0 {
            builder.worker_threads(self.worker_threads);
        }
        builder.build().expect("failed to build Tokio runtime")
    }
}

#[derive(clap::Args, Debug, Clone)]
pub struct PoolOptions {
    /// Buffer pool memory limit in MiB (0 = library default).
    #[arg(long = "pool-mem-mb", default_value = "0", value_parser = parse_memory_limit)]
    memory_limit: usize,

    #[cfg(feature = "rdma")]
    #[command(flatten)]
    rdma: RdmaOptions,
}

impl PoolOptions {
    pub fn config(&self, _enable_rdma: bool) -> SocketPoolConfig {
        let mut config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: _enable_rdma.then(|| self.rdma.config()),
            ..Default::default()
        };
        if self.memory_limit != 0 {
            config.buffer_pool_memory = self.memory_limit;
        }
        config
    }
}

fn parse_memory_limit(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())?
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "buffer pool limit exceeds addressable memory".to_owned())
}

#[cfg(feature = "rdma")]
#[derive(clap::Args, Debug, Clone)]
struct RdmaOptions {
    /// RDMA: number of CQ and poll-thread shards per device.
    #[arg(long, default_value = "1")]
    poll_threads: u32,

    /// RDMA: connections per peer; requests are striped across them.
    #[arg(long, default_value = "1")]
    conns_per_peer: u32,

    /// RDMA: comma-separated device allowlist (e.g. "mlx5_0").
    #[arg(long, value_delimiter = ',')]
    rdma_devices: Vec<String>,

    /// RDMA: poll-thread busy-poll window in microseconds.
    #[arg(long, default_value = "50")]
    poll_spin_us: u64,

    /// RDMA: dispatch worker tasks shared by all poll threads.
    #[arg(long, default_value = "32")]
    dispatch_workers: u32,

    /// RDMA: receive ring depth per connection; the send window is half of it.
    #[arg(long, default_value = "8")]
    recv_queue_len: u32,

    /// RDMA: GRH traffic class (RoCE DSCP/ECN byte) for outbound connections.
    #[arg(long, default_value = "0")]
    traffic_class: u8,
}

#[cfg(feature = "rdma")]
impl RdmaOptions {
    fn config(&self) -> ruapc::rdma::RdmaSocketPoolConfig {
        let mut config = ruapc::rdma::RdmaSocketPoolConfig::default();
        config.polling.poll_threads_per_device = self.poll_threads;
        config.polling.poll_spin_us = self.poll_spin_us;
        config.polling.dispatch_workers = self.dispatch_workers;
        config.peers.connections_per_peer = self.conns_per_peer;
        config.path.device_filter = self.rdma_devices.clone();
        config.connection.recv_queue_len = self.recv_queue_len;
        config.connection.traffic_class = self.traffic_class;
        config
    }
}
