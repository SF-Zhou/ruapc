mod common;

use common::{TEST_TIMEOUT, TestServer, config};
use ruapc::{Buffer, Client, Context, Endpoint, Transport};
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
        let server = TestServer::start(base_path).await;
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
                    let source = client.with_read_buffer(data);
                    let mut destinations = vec![buffer(&ctx, len)];
                    for _ in 0..3 {
                        assert_eq!(
                            source.read_crc(&ctx, &ReadCrcReq {}).await.unwrap(),
                            expected
                        );
                        let response = client
                            .with_write_buffers(destinations)
                            .write_crc(&ctx, &WriteCrcReq { len })
                            .await
                            .unwrap();
                        let (expected, returned) = response.into_parts();
                        assert_eq!(returned.iter().map(|buf| buf.len()).sum::<usize>(), len);
                        assert_eq!(crc32c_of(&returned), expected);
                        destinations = returned;
                    }
                }
            }
        })
        .await;
        server.stop().await;
        result.expect("demo RPC calls did not finish");
    }
}
