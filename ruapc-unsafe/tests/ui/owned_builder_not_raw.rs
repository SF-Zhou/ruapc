use ruapc::{Client, RawCall};

fn requires_raw_call<T: RawCall>(_: T) {}

fn direct(client: &Client) {
    let attached = client.with_buffers(&[], Vec::new());
    requires_raw_call(attached);
}

fn main() {}
