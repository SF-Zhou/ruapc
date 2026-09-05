//! Unknown service arguments are rejected instead of being ignored.

#[ruapc::service(public)]
pub trait Foo {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

fn main() {}
