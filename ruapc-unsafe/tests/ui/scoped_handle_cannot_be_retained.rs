use ruapc::Client;

async fn escape(client: &Client) {
    let mut retained = None;
    client
        .with_buffer_storage(ruapc_bufpool::ReadSources::new(&[]), Vec::new())
        .run(async |attached| {
            retained = Some(attached);
        })
        .await;
    drop(retained);
}

fn main() {}
