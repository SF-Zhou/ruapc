//! Service methods must take exactly `&self, &Context, &Req`.

#[ruapc::service]
pub trait Foo {
    async fn hello(&self, ctx: &ruapc::Context) -> ruapc::Result<String>;
}

fn main() {}
