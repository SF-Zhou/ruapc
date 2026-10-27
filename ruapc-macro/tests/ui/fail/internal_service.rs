//! Services do not have a separate internal visibility.

#[ruapc::service(internal)]
pub trait InternalService {
    async fn hello(&self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
}

fn main() {}
