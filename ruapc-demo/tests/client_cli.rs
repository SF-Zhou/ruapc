mod common;

use std::{process::Output, sync::Arc};

use common::{TEST_TIMEOUT, TestServer, config};
use ruapc::{BufferReply, Context, Result, Router, Server};
use ruapc_demo::{EchoService, MemBenchService, ReadCrcReq, Request, WriteCrcReq};
use tokio::{process::Command, time::timeout};

async fn client(args: &[&str]) -> Output {
    timeout(
        TEST_TIMEOUT,
        Command::new(env!("CARGO_BIN_EXE_client"))
            .args(args)
            .args(["--worker-threads", "2"])
            .env("RUST_LOG", "off")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("client command timed out")
    .expect("client should start")
}

async fn rejects(args: &[&str], message: &str) {
    let output = client(args).await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded: {args:?}"
    );
    assert!(stderr.contains(message), "{args:?}: {stderr}");
}

#[tokio::test]
async fn client_runs_individual_calls_and_load_modes() {
    let server = TestServer::start("").await;
    let endpoint = format!("tcp://{}", server.addr);
    for rpc in ["echo", "read", "write"] {
        let output = client(&[&endpoint, "--rpc", rpc, "--buffer-size", "4097"]).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    for (rpc, mode) in [("read", "--stress"), ("write", "--bench")] {
        let output = client(&[
            &endpoint,
            "--rpc",
            rpc,
            mode,
            "--secs",
            "1",
            "--warmup-secs",
            "0",
            "--coroutines",
            "2",
            "--buffer-size",
            "4097",
        ])
        .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn client_accepts_one_address_group_and_rejects_multiple_schemes() {
    let server = TestServer::start("").await;
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = unavailable.local_addr().unwrap();
    drop(unavailable);
    for transport in ["tcp", "ws", "http"] {
        let endpoint = format!("{transport}://{dead},{},{}", server.addr, server.addr);
        let output = client(&[&endpoint, "--rpc", "read", "--buffer-size", "4097"]).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let endpoint = format!("tcp://{}", server.addr);
    rejects(
        &[&format!("{endpoint},ws://{}", server.addr)],
        "invalid value",
    )
    .await;
    rejects(&[&endpoint, &endpoint], "unexpected argument").await;
    server.stop().await;
}

#[tokio::test]
async fn client_reports_argument_connection_and_preparation_errors() {
    for (flags, message) in [
        (vec!["--secs", "0"], "--secs"),
        (vec!["--coroutines", "0"], "--coroutines"),
        (vec!["--bench", "--stress"], "cannot be used"),
        (vec!["--bench", "--secs", "3"], "--secs must"),
        (vec!["--buffer-size", "67108865"], "--buffer-size"),
    ] {
        rejects(&flags, message).await;
    }
    // Logging is disabled by client(); calls must still execute and fail.
    rejects(&["tcp://127.0.0.1:0"], "TcpConnectFailed").await;
    // One worker fills the pool and waits at the barrier; the other's
    // preparation must fail and abort the run before its default 60 seconds.
    rejects(
        &[
            "tcp://127.0.0.1:0",
            "--bench",
            "--rpc",
            "read",
            "--pool-mem-mb",
            "64",
            "--buffer-size",
            "67108864",
            "--coroutines",
            "2",
        ],
        "InvalidArgument",
    )
    .await;
}

struct CorruptService;

impl EchoService for CorruptService {
    async fn echo(&self, _: &Context, _: &Request) -> Result<String> {
        Ok("incorrect echo".into())
    }
}

impl MemBenchService for CorruptService {
    async fn read_crc(&self, _: &Context, _: &ReadCrcReq) -> Result<u32> {
        Ok(1)
    }
    async fn write_crc(&self, ctx: &Context, _: &WriteCrcReq) -> Result<BufferReply<u32>> {
        Ok(ctx.sent_nothing().into_reply(1))
    }
}

#[tokio::test]
async fn client_rejects_incorrect_echo_and_crc() {
    let mut router = Router::default();
    EchoService::ruapc_export(Arc::new(CorruptService), &mut router);
    MemBenchService::ruapc_export(Arc::new(CorruptService), &mut router);
    let server = Server::create(router, &config("")).unwrap();
    let addr = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let endpoint = format!("tcp://{addr}");
    for (rpc, message) in [
        ("echo", "echo response"),
        ("read", "crc32c mismatch"),
        ("write", "crc32c mismatch"),
    ] {
        // Empty read/write spaces have CRC32C zero; the service returns one.
        rejects(
            &[
                &endpoint,
                "--rpc",
                rpc,
                "--stress",
                "--secs",
                "1",
                "--coroutines",
                "1",
                "--buffer-size",
                "0",
            ],
            message,
        )
        .await;
    }
    server.stop();
    timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("fake server should stop");
}
