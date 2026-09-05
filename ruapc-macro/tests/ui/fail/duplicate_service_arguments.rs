//! Each trait-level service argument can occur at most once.

#[ruapc::service(name = "First", name = "Second")]
pub trait DuplicateName {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

#[ruapc::service(internal, internal)]
pub trait DuplicateInternal {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

fn main() {}
