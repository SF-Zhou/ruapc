//! In-memory stand-ins for a stream connection's two loops.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes, BytesMut};
use ruapc_memory::BufferSet;

use super::Outbound;
use crate::{
    Message, Socket, State,
    msg::frame::{self, DataHead, DataPlacement, Frame},
    remote_memory::{IncomingData, WriteTarget},
    task::MemoryRoutes,
};

/// What one queued frame carries.
#[derive(Debug)]
pub(crate) enum Sent {
    Message(Message),
    Data { head: DataHead, bytes: Bytes },
}

impl Sent {
    /// Decodes one frame as its receiver would.
    pub(crate) fn decode(frame: Outbound) -> Self {
        let mut encoded = BytesMut::from(frame.into_bytes().as_ref());
        let sent = match frame::parse_frame(&mut encoded).unwrap().frame().unwrap() {
            Frame::Message(body) => Self::Message(Message::parse(body).unwrap()),
            Frame::Data { head, len } => Self::Data {
                head,
                bytes: encoded.split_to(len).freeze(),
            },
        };
        assert!(!encoded.has_remaining(), "one queue entry is one frame");
        sent
    }

    pub(crate) fn message(self) -> Message {
        match self {
            Self::Message(message) => message,
            Self::Data { head, .. } => panic!("expected a message, got data frame {head:?}"),
        }
    }

    pub(crate) fn data(self) -> (DataHead, Bytes) {
        match self {
            Self::Data { head, bytes } => (head, bytes),
            Self::Message(message) => panic!("expected a data frame, got {:?}", message.meta),
        }
    }
}

/// Delivers a queued frame to `state` as if `socket` had received it. A data
/// frame arrives in pieces of `piece` bytes.
pub(crate) fn deliver(state: &Arc<State>, socket: &Socket, frame: Outbound, piece: usize) {
    match Sent::decode(frame) {
        Sent::Message(message) => state.handle_recv(socket, message).unwrap(),
        Sent::Data { head, bytes } => {
            let mut incoming = IncomingData::begin(state, socket, head, bytes.len());
            for piece in bytes.chunks(piece.max(1)) {
                incoming.write(piece);
            }
            incoming.finish(state, socket).unwrap();
        }
    }
}

/// An encoded data frame that carries `bytes` to `ranges` of the write space
/// of request `id`.
pub(crate) fn data_frame(id: u64, ack: Option<u64>, ranges: &[(u64, u64)], bytes: &[u8]) -> Bytes {
    let placement = DataPlacement::Ranges {
        ack,
        ranges: ranges.to_vec(),
    };
    encoded_data_frame(id, placement, bytes)
}

pub(crate) fn encoded_data_frame(id: u64, placement: DataPlacement, bytes: &[u8]) -> Bytes {
    let mut encoded = BytesMut::new();
    frame::encode_data_head(&DataHead { id, placement }, bytes.len(), &mut encoded).unwrap();
    encoded.extend_from_slice(bytes);
    encoded.freeze()
}

/// A request pending on the data connection `socket` that lends a write
/// space with `contents`. Returns the space, the request's ID and the
/// receiver that keeps the request pending.
pub(crate) fn pending_write_space<'s>(
    state: &'s State,
    socket: &Socket,
    contents: &[u8],
) -> (Arc<WriteTarget>, u64, impl Sized + use<'s>) {
    let mut buffer = state.buffer_pool.allocate(contents.len().max(1)).unwrap();
    buffer.set_len(contents.len());
    buffer.copy_from_slice(contents);
    let target = WriteTarget::new(BufferSet::new(vec![buffer]));
    let (id, response) = state.waiter.alloc_until(
        Instant::now() + Duration::from_secs(30),
        MemoryRoutes {
            write_target: Some(target.clone()),
            ..Default::default()
        },
    );
    let channel = socket.data_channel().expect("a data connection");
    state.waiter.bind_connection(id, channel.conn_id());
    (target, id, response)
}
