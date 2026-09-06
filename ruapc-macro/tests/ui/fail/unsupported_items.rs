//! Unsupported Rust items must not silently disappear from the service API.

#[ruapc::service]
trait UnsupportedItems {
    type Request;
    const VERSION: u32 = 1;
    async fn default_handler(&self, _: &ruapc::Context, _: &()) -> ruapc::Result<()> {
        Ok(())
    }
}

#[ruapc::service]
trait GenericService<T> {
    async fn generic(&self, ctx: &ruapc::Context, req: &T) -> ruapc::Result<()>;
}

fn main() {}
