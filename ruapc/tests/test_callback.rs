#![feature(return_type_notation)]
use ruapc::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct FooReq(u64);

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct FooRsp(u64);

#[service]
pub trait FooService {
    async fn foo(&self, c: &Context, r: &FooReq) -> Result<FooRsp>;
}

struct FooServiceImpl;

impl FooService for FooServiceImpl {
    async fn foo(&self, c: &Context, r: &FooReq) -> Result<FooRsp> {
        let client = Client::default();
        let rsp = client.bar(c, &BarReq(r.0)).await?;
        Ok(FooRsp(rsp.0 + 1))
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct BarReq(u64);

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct BarRsp(u64);

#[service]
pub trait BarService {
    async fn bar(&self, c: &Context, r: &BarReq) -> Result<BarRsp>;
}

struct BarServiceImpl;

impl BarService for BarServiceImpl {
    async fn bar(&self, _c: &Context, r: &BarReq) -> Result<BarRsp> {
        Ok(BarRsp(r.0 * 2))
    }
}

#[tokio::test]
async fn test_callback() {
    let foo = Arc::new(FooServiceImpl);
    let mut router = Router::default();
    foo.clone().ruapc_export(&mut router);
    let server = Server::create(router, &SocketPoolConfig::default()).unwrap();
    let server = Arc::new(server);
    let addr = std::net::SocketAddr::from_str("0.0.0.0:0").unwrap();
    let addr = server.clone().listen(addr).await.unwrap();

    let bar = Arc::new(BarServiceImpl);
    let mut router = Router::default();
    bar.clone().ruapc_export(&mut router);
    let ctx = Context::create_with_router(router, &SocketPoolConfig::default()).unwrap();
    let ctx = ctx.with_addr(addr);

    let client = Client::default();
    assert_eq!(client.foo(&ctx, &FooReq(0)).await, Ok(FooRsp(1)));
    assert_eq!(client.foo(&ctx, &FooReq(1)).await, Ok(FooRsp(3)));

    client.bar(&ctx, &BarReq(0)).await.unwrap_err();

    server.stop();
    server.join().await;
}
