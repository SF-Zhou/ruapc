//! Service methods must be `async`.

#[ruapc::service]
pub trait Foo {
    fn hello(&self, ctx: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

fn main() {}
