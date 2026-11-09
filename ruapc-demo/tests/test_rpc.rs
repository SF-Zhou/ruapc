use ruapc::{Buffer, Client, Context, Endpoint, Transport};
use ruapc_demo::test_support::{TEST_TIMEOUT, TestServer, config};
use ruapc_demo::{
    EchoService, GreetService, MemBenchService, ReadCrcReq, Request, WriteCrcReq, crc32c_of,
};
use tokio::time::timeout;

fn buffer(ctx: &Context, len: usize) -> Buffer {
    let mut buffer = ctx.state.buffer_pool.allocate(len.max(1)).unwrap();
    buffer.set_len(len);
    buffer
}

#[tokio::test]
async fn demo_services_round_trip_over_stream_transports() {
    for (transport, base_path) in [
        (Transport::TCP, ""),
        (Transport::WS, ""),
        (Transport::HTTP, ""),
        (Transport::HTTP, "/demo"),
    ] {
        let server = TestServer::start(env!("CARGO_BIN_EXE_server"), base_path).await;
        let ctx = Context::create(&config(base_path))
            .unwrap()
            .with_endpoint(Endpoint::new(transport, [server.addr]).unwrap());
        let result = timeout(TEST_TIMEOUT, async {
            let clients = [
                Client {
                    use_msgpack: false,
                    ..Default::default()
                },
                Client::default(),
            ];
            let mut sequence = 0;
            for client in &clients {
                for value in ["", "你好, RuaPC!", "alice"] {
                    let request = Request(value.into());
                    assert_eq!(client.echo(&ctx, &request).await.unwrap(), value);
                    assert_eq!(
                        client.greet(&ctx, &request).await.unwrap(),
                        format!("hello {value}({sequence})!")
                    );
                    sequence += 1;
                }
            }
            for client in &clients {
                for len in [0, 4097] {
                    let mut data = buffer(&ctx, len);
                    for (i, byte) in data.iter_mut().enumerate() {
                        *byte = (i % 251) as u8;
                    }
                    let expected = crc32c_of([&data]);
                    let sources = vec![data];
                    let mut destinations = vec![buffer(&ctx, len)];
                    for _ in 0..3 {
                        let completed = client
                            .with_buffers(&sources, vec![])
                            .run(async |source| source.read_crc(&ctx, &ReadCrcReq {}).await)
                            .await;
                        assert_eq!(completed.result.unwrap(), expected);
                        assert!(completed.buffers.unwrap().is_empty());
                        let completed = client
                            .with_buffers(&[], destinations)
                            .run(async |target| target.write_crc(&ctx, &WriteCrcReq { len }).await)
                            .await;
                        destinations = completed.buffers.unwrap();
                        assert_eq!(destinations.iter().map(|buf| buf.len()).sum::<usize>(), len);
                        assert_eq!(crc32c_of(&destinations), completed.result.unwrap());
                    }
                }
            }
        })
        .await;
        server.stop().await;
        result.expect("demo RPC calls did not finish");
    }
}
