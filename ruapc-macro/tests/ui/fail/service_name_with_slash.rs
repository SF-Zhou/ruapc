//! `/` is reserved as the service/method separator in wire names.

#[ruapc::service(name = "foo/bar")]
pub trait Foo {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

fn main() {}
