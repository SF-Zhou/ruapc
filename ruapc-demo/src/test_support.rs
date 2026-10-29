//! Shared setup for the demo integration tests.

//! Process fixtures and configuration for the demo's integration tests.

use std::{net::SocketAddr, process::Stdio, time::Duration};

use ruapc::{ListenMode, SocketPoolConfig};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    time::timeout,
};

pub const TEST_TIMEOUT: Duration = Duration::from_secs(20);

pub fn config(base_path: &str) -> SocketPoolConfig {
    SocketPoolConfig {
        listen_mode: ListenMode::UNIFIED,
        buffer_pool_memory: 64 * 1024 * 1024,
        http_base_path: base_path.into(),
        #[cfg(feature = "rdma")]
        rdma: None,
        ..Default::default()
    }
}

pub struct TestServer {
    pub addr: SocketAddr,
    child: Child,
}

impl TestServer {
    pub async fn start(server_binary: &str, base_path: &str) -> Self {
        let mut command = Command::new(server_binary);
        command
            .args([
                "127.0.0.1:0",
                "--worker-threads",
                "2",
                "--http-base-path",
                base_path,
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(feature = "rdma")]
        command.arg("--no-rdma");
        let mut child = command.spawn().expect("demo server should start");
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let addr = timeout(TEST_TIMEOUT, async {
            while let Some(line) = lines.next_line().await.unwrap() {
                if line.contains("Serving ")
                    && let Some((_, address)) = line.rsplit_once(" on ")
                {
                    // Ignore any trailing ANSI formatting from tracing.
                    let address = address
                        .split(|c: char| !c.is_ascii_digit() && c != '.' && c != ':')
                        .next()
                        .unwrap();
                    return address
                        .parse()
                        .expect("server should report its listening address");
                }
            }
            panic!("demo server exited before announcing its listening address");
        })
        .await
        .expect("demo server startup timed out");
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        Self { addr, child }
    }

    pub async fn stop(mut self) {
        self.child
            .start_kill()
            .expect("demo server should be running");
        timeout(TEST_TIMEOUT, self.child.wait())
            .await
            .expect("demo server shutdown timed out")
            .expect("demo server should be reaped");
    }
}
