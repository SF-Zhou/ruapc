//! Report invalid method shapes at the offending syntax, in a single pass.

#[ruapc::service]
trait InvalidSignatures {
    async fn owned_self(self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
    async fn mutable_self(&mut self, ctx: &ruapc::Context, req: &()) -> ruapc::Result<()>;
    async fn owned_context(&self, ctx: ruapc::Context, req: &()) -> ruapc::Result<()>;
    async fn owned_request(&self, ctx: &ruapc::Context, req: ()) -> ruapc::Result<()>;
    async fn mutable_request(&self, ctx: &ruapc::Context, req: &mut ()) -> ruapc::Result<()>;
    async fn generic<T>(&self, ctx: &ruapc::Context, req: &T) -> ruapc::Result<()>;
    async fn missing_return(&self, ctx: &ruapc::Context, req: &());
}

fn main() {}
