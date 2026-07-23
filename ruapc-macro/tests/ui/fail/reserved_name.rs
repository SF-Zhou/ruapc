//! `ruapc_export` and `ruapc_request` are reserved method names.

#[ruapc::service]
pub trait Foo {
    async fn ruapc_export(&self, ctx: &ruapc::Context, req: &String) -> ruapc::Result<String>;
}

fn main() {}
