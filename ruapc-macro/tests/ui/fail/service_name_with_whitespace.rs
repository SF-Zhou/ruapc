//! Service names cannot have ambiguous leading or trailing whitespace.

#[ruapc::service(name = " Foo")]
pub trait Foo {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

fn main() {}
