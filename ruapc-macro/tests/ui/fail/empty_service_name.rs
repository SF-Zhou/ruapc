//! An explicit service name must not be empty.

#[ruapc::service(name = "")]
pub trait Foo {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

fn main() {}
