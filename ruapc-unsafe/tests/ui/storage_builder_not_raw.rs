use ruapc::{Client, RawCall};

fn requires_raw_call<T: RawCall>(_: T) {}

fn direct(client: &Client) {
    let attached = client.with_buffer_storage(ruapc_bufpool::ReadSources::new(&[]), Vec::new());
    requires_raw_call(attached);
}

fn main() {}
