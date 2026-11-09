use ruapc::{Buffer, Client, Context, Result};
use ruapc_unsafe::ClientBuffersExt;

#[ruapc::service]
trait ScopeRpc {
    async fn ping(&self, ctx: &Context, request: &()) -> Result<()>;
}

async fn direct(client: &Client, ctx: &Context, read: &[Buffer], write: &mut [Buffer]) {
    // No operation starts: this fixture verifies the missing typed-call API.
    let attached = unsafe { client.borrow_buffers(read, write) };
    let _ = attached.ping(ctx, &()).await;
}

fn main() {}
