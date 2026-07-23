//! A well-formed service compiles: the trait keeps its methods, the router
//! registration and the generated `Client` / `ClientWithBuffer` impls all
//! type-check.
#![feature(return_type_notation)]

use std::sync::Arc;

#[ruapc::service]
pub trait Greeter {
    async fn greet(&self, ctx: &ruapc::Context, req: &String) -> ruapc::Result<String>;
    async fn ping(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

struct GreeterImpl;

impl Greeter for GreeterImpl {
    async fn greet(&self, _ctx: &ruapc::Context, req: &String) -> ruapc::Result<String> {
        Ok(format!("hello {req}!"))
    }

    async fn ping(&self, _ctx: &ruapc::Context, _req: &()) -> ruapc::Result<()> {
        Ok(())
    }
}

fn assert_impls_greeter<T: Greeter>() {}

fn main() {
    assert_eq!(<GreeterImpl as Greeter>::NAME, "Greeter");

    let mut router = ruapc::Router::default();
    Arc::new(GreeterImpl).ruapc_export(&mut router);

    // The macro implements the trait for the RPC clients as well.
    assert_impls_greeter::<ruapc::Client>();
}
