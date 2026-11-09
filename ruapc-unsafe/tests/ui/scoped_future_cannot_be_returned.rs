use ruapc::{Client, Context, Result};

#[ruapc::service]
trait ScopeRpc {
    async fn ping(&self, ctx: &Context, request: &()) -> Result<()>;
}

async fn escape(client: &Client, ctx: &Context) {
    let completed = client
        .with_buffers(&[], Vec::new())
        .run(async |attached| attached.ping(ctx, &()))
        .await;
    let _ = completed.result.await;
}

fn main() {}
