use ruapc::{Buffer, Client, RawCall};
use ruapc_unsafe::ClientBuffersExt;

fn requires_raw_call<T: RawCall>(_: T) {}

fn direct(client: &Client, read: &[Buffer], write: &mut [Buffer]) {
    // No operation starts: this fixture verifies the missing raw-call capability.
    let attached = unsafe { client.borrow_buffers(read, write) };
    requires_raw_call(attached);
}

fn main() {}
