use ruapc::{Client, Context, Result};

#[ruapc::service]
trait ScopeRpc {
    async fn ping(&self, ctx: &Context, request: &()) -> Result<()>;
}

async fn direct(client: &Client, ctx: &Context) {
    let attached = client.with_buffers(&[], Vec::new());
    let _ = attached.ping(ctx, &()).await;
}

fn main() {}
