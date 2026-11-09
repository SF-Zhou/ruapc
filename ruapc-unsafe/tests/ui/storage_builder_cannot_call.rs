use ruapc::{Client, Context, Result};

#[ruapc::service]
trait ScopeRpc {
    async fn ping(&self, ctx: &Context, request: &()) -> Result<()>;
}

async fn direct(client: &Client, ctx: &Context) {
    let attached = client.with_buffer_storage(ruapc_bufpool::ReadSources::new(&[]), Vec::new());
    let _ = attached.ping(ctx, &()).await;
}

fn main() {}
