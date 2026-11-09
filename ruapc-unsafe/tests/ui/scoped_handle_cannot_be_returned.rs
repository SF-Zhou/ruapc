use ruapc::{Client, ClientWithBuffers};

async fn escape<'a>(client: &'a Client) -> &'a ClientWithBuffers<'a> {
    client
        .with_buffer_storage(ruapc_bufpool::ReadSources::new(&[]), Vec::new())
        .run(async |attached| attached)
        .await
}

fn main() {}
