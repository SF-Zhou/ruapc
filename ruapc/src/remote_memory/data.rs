//! Data frames on stream connections: raw bytes pushed into the write space
//! of a request that is pending at the receiver.
//!
//! This is the one primitive behind both stream transfers. `remote_write`
//! pushes the handler's buffers into the original request's write space.
//! `remote_read` makes a reverse call that lends the handler's buffers as its
//! own write space, and the client pushes the source bytes into that.
//!
//! A frame names a request by its message ID. As for the reverse RPCs, the ID
//! selects a write space only on the connection that carries the request.

use std::{
    io::IoSliceMut,
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use futures_util::FutureExt;
use ruapc_memory::{CopyBatch, SpaceLayout};
use tokio::sync::Notify;

use super::WriteTarget;
use crate::{
    Buffer, Context, CopyOp, Error, ErrorKind, MAX_COPY_OPS, MsgFlags, MsgMeta, Result, Socket,
    State,
    msg::frame::{DataHead, DataRange, MAX_DATA_LEN, encode_data_head},
    sockets::{ChannelConnection, Outbound},
    task::MemoryRoutes,
};

/// The request named by a reverse RPC or data frame is not pending on the
/// connection that delivered it, or attached no such memory. The cases are
/// deliberately indistinguishable to the peer.
pub(super) fn request_gone(operation: &str, attachment: &str) -> Error {
    Error::new(
        ErrorKind::Timeout,
        format!(
            "{operation}: original request is gone (timed out, completed, on another \
             connection, or attached no {attachment} buffers)"
        ),
    )
}

/// The destination ranges that a piece of a batch fills, in order.
fn destination_ranges(piece: &[CopyOp]) -> Vec<DataRange> {
    piece
        .iter()
        .map(|op| DataRange {
            offset: op.dst_offset,
            len: op.len,
        })
        .collect()
}

/// Encodes the header and head of the data frame that carries `piece` to the
/// write space of request `id`, into storage with room for the piece's bytes
/// when `inline` is set. Also returns the number of raw bytes that complete
/// the frame.
pub(super) fn encode_head(
    id: u64,
    ack: Option<u64>,
    piece: &[CopyOp],
    inline: bool,
) -> Result<(BytesMut, usize)> {
    let head = DataHead {
        id,
        ack,
        ranges: destination_ranges(piece),
    };
    // A capacity hint only: the storage grows if the head needs more.
    let mut capacity = 64 + 20 * piece.len();
    if inline {
        let bytes: u64 = piece.iter().map(|op| op.len).sum();
        capacity += usize::try_from(bytes).unwrap_or(0).min(MAX_DATA_LEN);
    }
    let mut encoded = BytesMut::with_capacity(capacity);
    let len = encode_data_head(&head, &mut encoded)?;
    Ok((encoded, len))
}

/// The stream connection that carries the data frames of `socket`.
pub(super) fn data_channel(socket: &Socket) -> Result<&ChannelConnection> {
    socket.data_channel().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidArgument,
            "this connection carries no data frames (unary HTTP has JSON bodies only, and RDMA \
             transfers bulk bytes with READs)"
                .into(),
        )
    })
}

/// Queues one data frame that carries `len` raw bytes, waiting for the
/// connection's capacity until `deadline`, and counts it as sent. Frames
/// queued by one task reach the peer in that order, and before any message
/// the task sends afterwards.
pub(super) async fn send_frame(
    state: &State,
    channel: &ChannelConnection,
    frame: Outbound,
    len: usize,
    deadline: Instant,
) -> Result<()> {
    channel.send_data(frame, deadline).await?;
    state.metrics.data_frame_sent(len);
    Ok(())
}

/// Splits a checked batch into the pieces that one data frame each carries.
///
/// Pieces stay within [`MAX_DATA_LEN`] bytes and [`MAX_COPY_OPS`] operations,
/// whose worst-case encoded head fits
/// [`MAX_DATA_HEAD_LEN`](crate::msg::frame::MAX_DATA_HEAD_LEN).
pub(super) fn frame_pieces<'a>(
    batch: &CopyBatch<'a>,
) -> Result<impl Iterator<Item = Vec<CopyOp>> + use<'a>> {
    Ok(batch.split(MAX_DATA_LEN as u64, MAX_COPY_OPS)?)
}

/// Pushes the source bytes of `batch` into the write space of the request
/// that `ctx` handles, and waits for the peer to confirm the last frame.
///
/// `sources` are the buffers of the source space described by `layout`.
/// Frames share them instead of copying, and can keep sharing them after
/// this returns: a frame stays queued even when the transfer fails.
///
/// The peer applies frames in order and acknowledges only the last one. Its
/// write space refuses every frame after one it could not accept, so that
/// acknowledgement covers the whole transfer.
pub(super) async fn push(
    ctx: &Context,
    socket: &Socket,
    batch: &CopyBatch<'_>,
    layout: &SpaceLayout,
    sources: &[Bytes],
    deadline: Instant,
) -> Result<()> {
    let channel = data_channel(socket)?;
    let mut pieces = frame_pieces(batch)?.peekable();
    if pieces.peek().is_none() {
        return Ok(());
    }
    // The acknowledgement is an ordinary response to this message ID. Bind
    // it first, so that closing the connection fails the wait eagerly.
    let waiter = &ctx.state.waiter;
    let (ack, acknowledged) = waiter.alloc_until(deadline, MemoryRoutes::default());
    waiter.bind_connection(ack, channel.conn_id());
    while let Some(piece) = pieces.next() {
        let ack = pieces.peek().is_none().then_some(ack);
        let (head, len) = encode_head(ctx.msg_meta.msgid, ack, &piece, false)?;
        let mut data = Vec::with_capacity(piece.len());
        for op in &piece {
            layout.for_each_slice::<Error>(op.src_offset, op.len, |segment, offset, len| {
                let (offset, len) = (offset as usize, len as usize);
                data.push(sources[segment].slice(offset..offset + len));
                Ok(())
            })?;
        }
        let frame = Outbound::with_data(head.freeze(), data);
        send_frame(&ctx.state, channel, frame, len, deadline).await?;
    }
    let reply = acknowledged.recv().await?;
    reply.payload.deserialize::<Result<()>>(&reply.meta)?
}

/// Owned source buffers shared with queued data frames.
///
/// Each buffer becomes a `Bytes` that frames slice without copying. A buffer
/// comes home when the last such slice is dropped: after the send loop wrote
/// it, or when the connection's queue is torn down. Until then nobody can
/// reuse it, however the transfer ended.
///
/// Home is this handle while it exists. Once it is dropped without having
/// recovered the complete set, every buffer goes back to its pool on its own,
/// as soon as its last share is dropped.
pub(super) struct LentSources {
    home: Arc<Home>,
}

#[derive(Debug)]
struct Home {
    slots: Mutex<Slots>,
    returned: Notify,
}

#[derive(Debug)]
struct Slots {
    /// The lent buffers at their original positions, once returned.
    buffers: Vec<Option<Buffer>>,
    missing: usize,
    /// Nobody waits for the set any more: returning buffers are released.
    abandoned: bool,
}

/// One lent buffer, as the owner of the `Bytes` that shares it.
struct LentBuffer {
    buffer: Option<Buffer>,
    index: usize,
    home: Arc<Home>,
}

impl AsRef<[u8]> for LentBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().map_or(&[], Buffer::as_slice)
    }
}

impl Drop for LentBuffer {
    fn drop(&mut self) {
        let mut slots = self.home.slots.lock().unwrap();
        if slots.abandoned {
            // Release the buffer itself outside the lock, with `self`.
            return;
        }
        slots.buffers[self.index] = self.buffer.take();
        slots.missing -= 1;
        if slots.missing == 0 {
            drop(slots);
            self.home.returned.notify_one();
        }
    }
}

impl Drop for LentSources {
    fn drop(&mut self) {
        let parked = {
            let mut slots = self.home.slots.lock().unwrap();
            slots.abandoned = true;
            std::mem::take(&mut slots.buffers)
        };
        // Buffers that came home before nobody wanted the set.
        drop(parked);
    }
}

impl LentSources {
    /// Shares `buffers` as `Bytes`, in the same order.
    pub(super) fn lend(buffers: Vec<Buffer>) -> (Self, Vec<Bytes>) {
        let home = Arc::new(Home {
            slots: Mutex::new(Slots {
                buffers: buffers.iter().map(|_| None).collect(),
                missing: buffers.len(),
                abandoned: false,
            }),
            returned: Notify::new(),
        });
        let shared = buffers
            .into_iter()
            .enumerate()
            .map(|(index, buffer)| {
                Bytes::from_owner(LentBuffer {
                    buffer: Some(buffer),
                    index,
                    home: home.clone(),
                })
            })
            .collect();
        (Self { home }, shared)
    }

    /// Takes the complete original set once every share of it is dropped,
    /// waiting until `until` at most; `None` does not wait.
    ///
    /// Without the complete set this returns `None`, and each buffer goes
    /// back to its pool when its last share is dropped.
    pub(super) async fn recover(self, until: Option<Instant>) -> Option<Vec<Buffer>> {
        loop {
            if let Some(buffers) = self.take() {
                return Some(buffers);
            }
            // A return that precedes this wait left its permit behind.
            let returned = self.home.returned.notified();
            match until {
                Some(until)
                    if tokio::time::timeout_at(until.into(), returned)
                        .await
                        .is_ok() => {}
                _ => return self.take(),
            }
        }
    }

    fn take(&self) -> Option<Vec<Buffer>> {
        let mut slots = self.home.slots.lock().unwrap();
        (slots.missing == 0).then(|| {
            std::mem::take(&mut slots.buffers)
                .into_iter()
                .map(|buffer| buffer.expect("every lent buffer has returned"))
                .collect()
        })
    }
}

/// One data frame being received: its bytes go to the write space it names,
/// or nowhere.
///
/// The transport owns the framing and hands over exactly the frame's bytes,
/// from memory or by letting the destination be read into directly. Whatever
/// happens to the write space, all of the frame's bytes are consumed, so the
/// stream stays in step.
pub(crate) struct IncomingData {
    /// `None` once the remaining bytes are only discarded.
    target: Option<Arc<WriteTarget>>,
    ranges: Vec<DataRange>,
    /// The range being filled and the bytes of it already written.
    range: usize,
    filled: u64,
    /// Raw bytes of the whole frame, and those not yet consumed.
    len: usize,
    remaining: usize,
    ack: Option<u64>,
    /// The first failure, which the acknowledgement reports.
    result: Result<()>,
}

impl IncomingData {
    /// Routes a frame that `socket` received and that carries `len` bytes.
    /// The frame reaches a write space only if the request it names is
    /// pending on this very connection.
    pub(crate) fn begin(state: &State, socket: &Socket, head: DataHead, len: usize) -> Self {
        let target = socket
            .data_channel()
            .and_then(|channel| state.waiter.write_target(head.id, channel.conn_id()))
            .ok_or_else(|| request_gone("data frame", "write"))
            .and_then(|target| target.admit(&head.ranges).map(|()| target));
        let (target, result) = match target {
            Ok(target) => (Some(target), Ok(())),
            Err(error) => (None, Err(error)),
        };
        Self {
            target,
            ranges: head.ranges,
            range: 0,
            filled: 0,
            len,
            remaining: len,
            ack: head.ack,
            result,
        }
    }

    /// Bytes of the frame that the transport has not handed over yet.
    pub(crate) fn remaining(&self) -> usize {
        self.remaining
    }

    /// The range that the frame's next byte belongs to, as its absolute
    /// offset and the bytes of the range still missing.
    fn next_range(&mut self) -> Option<(u64, u64)> {
        loop {
            let range = self.ranges.get(self.range)?;
            if self.filled < range.len {
                // `filled` is nonzero only after `admit` checked `offset + len`
                // against the write space, so this cannot overflow.
                return Some((range.offset + self.filled, range.len - self.filled));
            }
            (self.range, self.filled) = (self.range + 1, 0);
        }
    }

    /// Stops writing: the frame's remaining bytes are discarded.
    fn fail(&mut self, error: Error) {
        self.target = None;
        if self.result.is_ok() {
            self.result = Err(error);
        }
    }

    /// Consumes the frame's next bytes from memory.
    pub(crate) fn write(&mut self, mut bytes: &[u8]) {
        debug_assert!(bytes.len() <= self.remaining);
        self.remaining = self.remaining.saturating_sub(bytes.len());
        while !bytes.is_empty() {
            // The ranges describe exactly the frame's bytes.
            let Some((offset, missing)) = self.next_range() else {
                return;
            };
            let Some(target) = &self.target else { return };
            let count = usize::try_from(missing).map_or(bytes.len(), |n| n.min(bytes.len()));
            let (piece, rest) = bytes.split_at(count);
            if let Err(error) = target.push(offset, piece) {
                return self.fail(error);
            }
            self.filled += count as u64;
            bytes = rest;
        }
    }

    /// Lets `read` fill the rest of the current range directly, as far as one
    /// vectored socket read gets, and consumes the bytes it reports.
    ///
    /// Returns `None` without calling `read` when fewer than `min` bytes of
    /// the range are missing, or when the frame's bytes are being discarded;
    /// the caller then reads into memory and uses [`write`](Self::write).
    /// `read` runs while the write space is locked and must not block.
    pub(crate) fn read_with(
        &mut self,
        min: usize,
        read: impl FnOnce(&mut [IoSliceMut<'_>]) -> std::io::Result<usize>,
    ) -> Option<std::io::Result<usize>> {
        let (offset, missing) = self.next_range()?;
        if missing < min as u64 {
            return None;
        }
        let target = self.target.as_ref()?;
        match target.push_with(offset, missing, read) {
            Ok(Ok(count)) => {
                self.filled += count as u64;
                self.remaining = self.remaining.saturating_sub(count);
                Some(Ok(count))
            }
            Ok(Err(error)) => Some(Err(error)),
            Err(error) => {
                self.fail(error);
                None
            }
        }
    }

    /// Ends a completely consumed frame and answers it if its sender asked.
    ///
    /// The answer is a response message for the sender's waiter. It reports
    /// this frame's fate, and through the write space's refusal that of every
    /// earlier frame of the same transfer.
    ///
    /// # Errors
    ///
    /// Fails when the connection refuses the answer, as a closed one does,
    /// and with [`ErrorKind::Overloaded`] when the answer can neither be
    /// queued nor deferred: the peer asks for answers faster than it reads
    /// them. The receive loop that gets an error must end its connection.
    pub(crate) fn finish(self, state: &Arc<State>, socket: &Socket) -> Result<()> {
        debug_assert_eq!(self.remaining, 0);
        state
            .metrics
            .data_frame_received(self.len, self.result.is_ok());
        let Some(ack) = self.ack else {
            if let Err(error) = &self.result {
                tracing::debug!("discarded a data frame: {error}");
            }
            return Ok(());
        };
        let mut meta = MsgMeta {
            msgid: ack,
            flags: MsgFlags::IsRsp | MsgFlags::UseMessagePack,
            ..Default::default()
        };
        let result = self.result;
        // The receive loop must never wait for its own connection's send
        // queue: two peers doing so for each other would both stop reading.
        // An answer that cannot be queued at once fails here, as on a closed
        // connection; only a full queue hands the answer to a task instead.
        if let Some(sent) = socket.send(&mut meta, &result, state).now_or_never() {
            return sent;
        }
        // Such tasks are bounded per connection. A peer that reads its
        // socket never gets near the bound; one that does not would make
        // this side keep a task for every frame it sends.
        let Some(slot) = socket
            .data_channel()
            .and_then(ChannelConnection::try_defer_response)
        else {
            return Err(Error::new(
                ErrorKind::Overloaded,
                "peer requests data frame acknowledgements without reading them".into(),
            ));
        };
        // The task is not counted by the pool's task supervisor: the bound
        // above limits it, and closing the connection ends its wait.
        let (state, socket) = (state.clone(), socket.clone());
        tokio::spawn(async move {
            let _slot = slot;
            let _ = socket.send(&mut meta, &result, &state).await;
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ruapc_memory::BufferSet;

    use super::*;
    use crate::{
        Message, Payload, SocketPoolConfig,
        core::ContextEndpoint,
        msg::frame::MAX_DATA_HEAD_LEN,
        sockets::{MAX_DEFERRED_RESPONSES, test_util::Sent},
        tcp::TcpSocket,
    };

    type Queue = tokio::sync::mpsc::Receiver<Outbound>;

    fn state() -> Arc<State> {
        let config = SocketPoolConfig {
            #[cfg(feature = "rdma")]
            rdma: None,
            ..Default::default()
        };
        Context::create(&config).unwrap().state
    }

    fn filled(state: &State, bytes: &[u8]) -> Buffer {
        let mut buffer = state.buffer_pool.allocate(bytes.len().max(1)).unwrap();
        buffer.set_len(bytes.len());
        buffer.copy_from_slice(bytes);
        buffer
    }

    /// A handler context for request `msgid` on a fresh TCP connection.
    fn handler(state: &Arc<State>, msgid: u64) -> (Context, Queue) {
        let (sender, queue) = tokio::sync::mpsc::channel(64);
        let ctx = Context::server_ctx(
            state,
            Socket::TCP(TcpSocket::new(sender)),
            MsgMeta {
                msgid,
                ..Default::default()
            },
        );
        (ctx, queue)
    }

    fn socket(ctx: &Context) -> &Socket {
        match &ctx.endpoint {
            ContextEndpoint::Connected(socket) => socket,
            _ => unreachable!("handler contexts are connected"),
        }
    }

    fn answer(state: &State, id: u64, result: &Result<()>) {
        state.waiter.post(
            id,
            Message::new(
                MsgMeta {
                    msgid: id,
                    flags: MsgFlags::UseMessagePack | MsgFlags::IsRsp,
                    ..Default::default()
                },
                Payload::from(Bytes::from(rmp_serde::to_vec_named(result).unwrap())),
            ),
        );
    }

    #[tokio::test]
    async fn lent_sources_come_home_in_order_once_every_share_is_dropped() {
        let state = state();
        let buffers = vec![filled(&state, b"ab"), filled(&state, b"cde")];
        let pointers: Vec<_> = buffers.iter().map(Buffer::as_ptr).collect();
        let (lent, shared) = LentSources::lend(buffers);
        assert_eq!(&shared[1][..], b"cde");
        // A slice of a share keeps its buffer away, past any deadline.
        let held = shared[1].slice(1..);
        drop(shared);
        let soon = Instant::now() + Duration::from_millis(20);
        let (other, other_shared) = LentSources::lend(vec![filled(&state, b"x")]);
        assert!(other.recover(Some(soon)).await.is_none());
        assert!(
            LentSources::lend(Vec::new())
                .0
                .recover(None)
                .await
                .is_some()
        );
        drop(other_shared);

        let waiting = tokio::spawn(lent.recover(Some(Instant::now() + Duration::from_secs(5))));
        tokio::task::yield_now().await;
        drop(held);
        let recovered = waiting.await.unwrap().unwrap();
        assert_eq!(
            recovered.iter().map(Buffer::as_ptr).collect::<Vec<_>>(),
            pointers
        );
        assert_eq!(recovered[1].as_slice(), b"cde");
    }

    /// Once nobody waits for the set, a buffer goes back to its pool when
    /// its own last share is dropped, not when the set's last one is.
    #[tokio::test]
    async fn abandoned_sources_return_to_the_pool_one_by_one() {
        let state = state();
        let pool = &state.buffer_pool;
        let buffers: Vec<_> = (0..2).map(|_| pool.allocate(1 << 20).unwrap()).collect();
        let free = || (pool.free_counts(), pool.pending_counts());
        let both_lent = free();
        let (lent, mut shared) = LentSources::lend(buffers);
        let queued = shared.pop().unwrap();
        // The first buffer comes home while the set is still awaited.
        drop(shared);
        assert_eq!(free(), both_lent);
        // A failed transfer does not wait for the frame that is still queued.
        assert!(lent.recover(None).await.is_none());
        let one_lent = free();
        assert_ne!(one_lent, both_lent);
        drop(queued);
        assert_ne!(free(), one_lent);

        // A buffer still shared when the set is abandoned returns directly.
        let (lent, shared) = LentSources::lend(vec![pool.allocate(1 << 20).unwrap()]);
        let one_lent = free();
        drop(lent);
        assert_eq!(free(), one_lent);
        drop(shared);
        assert_ne!(free(), one_lent);
    }

    /// Pushed frames share the sources, follow the batch's operation order and
    /// ask for one acknowledgement, on the last frame. The transfer's result
    /// is that acknowledgement, or a timeout without it.
    #[tokio::test]
    async fn pushes_share_sources_and_wait_for_the_last_frames_acknowledgement() {
        let state = state();
        let (ctx, mut queue) = handler(&state, 41);
        let len = MAX_DATA_LEN + 5;
        let pattern: Vec<u8> = (0..len).map(|index| (index % 253) as u8).collect();
        let set = BufferSet::new(vec![
            filled(&state, &pattern[..7]),
            filled(&state, &pattern[7..]),
        ]);
        let layout = set.layout().clone();
        let (lent, sources) = LentSources::lend(set.into_buffers());
        let ops = [
            CopyOp::new(3, 0, len as u64 - 3),
            CopyOp::new(0, len as u64, 3),
        ];
        let batch = CopyBatch::new(&ops).unwrap();

        for outcome in [
            Some(Ok(())),
            Some(Err(Error::kind(ErrorKind::BuffersInUse))),
            None,
        ] {
            let deadline = Instant::now() + Duration::from_millis(300);
            let pushing = push(&ctx, socket(&ctx), &batch, &layout, &sources, deadline);
            let peer = async {
                let mut frames = Vec::new();
                while frames.len() < 2 {
                    frames.push(Sent::decode(queue.recv().await.unwrap()).data());
                }
                let ack = frames[1].0.ack.expect("the last frame asks for an answer");
                assert_eq!(frames[0].0.ack, None);
                assert!(frames.iter().all(|(head, _)| head.id == 41));
                let bytes: Vec<u8> = frames
                    .iter()
                    .flat_map(|(_, bytes)| bytes.to_vec())
                    .collect();
                assert!(bytes[..len - 3] == pattern[3..]);
                assert_eq!(&bytes[len - 3..], &pattern[..3]);
                if let Some(result) = &outcome {
                    answer(&state, ack, result);
                }
            };
            let (result, ()) = tokio::join!(pushing, peer);
            match outcome {
                Some(Ok(())) => result.unwrap(),
                Some(Err(error)) => assert_eq!(result.unwrap_err().kind, error.kind),
                None => assert_eq!(result.unwrap_err().kind, ErrorKind::Timeout),
            }
            assert_eq!(state.waiter.pending_count(), 0);
        }
        drop(sources);
        assert_eq!(lent.recover(None).await.unwrap().len(), 2);
    }

    /// A target and the ID of the request that attaches it on the
    /// connection of `ctx`, which stays pending while its waiter is kept.
    fn pending_target<'c>(
        ctx: &'c Context,
        contents: &[&[u8]],
    ) -> (Arc<WriteTarget>, u64, impl Sized + use<'c>) {
        let buffers = contents
            .iter()
            .map(|bytes| filled(&ctx.state, bytes))
            .collect();
        let target = WriteTarget::new(BufferSet::new(buffers));
        let (id, response) = ctx.state.waiter.alloc_until(
            Instant::now() + Duration::from_secs(5),
            MemoryRoutes {
                write_target: Some(target.clone()),
                ..Default::default()
            },
        );
        ctx.state.waiter.bind_connection(id, ctx.conn_id().unwrap());
        (target, id, response)
    }

    fn head(id: u64, ranges: &[(u64, u64)]) -> DataHead {
        DataHead {
            id,
            ack: None,
            ranges: ranges
                .iter()
                .map(|&(offset, len)| DataRange { offset, len })
                .collect(),
        }
    }

    /// A direct read fills the rest of one range at most, spanning buffers,
    /// and leaves short remainders to buffered writes.
    #[tokio::test]
    async fn direct_reads_fill_one_range_across_buffers() {
        let state = state();
        let (ctx, _queue) = handler(&state, 1);
        let (target, id, _pending) = pending_target(&ctx, &[b"aaaa", b"bbbbbbbb"]);
        let mut incoming =
            IncomingData::begin(&state, socket(&ctx), head(id, &[(2, 6), (9, 2)]), 8);
        // Too short a remainder for a read of its own.
        assert!(incoming.read_with(7, |_| unreachable!()).is_none());
        let read = incoming.read_with(6, |slices| {
            assert_eq!(
                slices.iter().map(|slice| slice.len()).collect::<Vec<_>>(),
                [2, 4]
            );
            slices[0].copy_from_slice(b"XY");
            slices[1][..1].copy_from_slice(b"Z");
            Ok(3)
        });
        assert_eq!(read.unwrap().unwrap(), 3);
        assert_eq!(incoming.remaining(), 5);
        // A failed read consumes nothing.
        let failed = incoming.read_with(1, |_| Err(std::io::ErrorKind::WouldBlock.into()));
        assert!(failed.unwrap().is_err());
        incoming.write(b"uvw");
        incoming.write(b"12");
        assert_eq!(incoming.remaining(), 0);
        incoming.finish(&state, socket(&ctx)).unwrap();
        assert_eq!(target.received(), 8);
        let buffers = target.seal(None).await.unwrap();
        assert_eq!(buffers[0].as_slice(), b"aaXY");
        assert_eq!(buffers[1].as_slice(), b"Zuvwb12b");
    }

    /// A frame for nobody is consumed without a destination, and never
    /// offers a direct read.
    #[tokio::test]
    async fn frames_without_a_destination_are_consumed() {
        let state = state();
        let (ctx, _queue) = handler(&state, 1);
        let mut incoming = IncomingData::begin(&state, socket(&ctx), head(99, &[(0, 4)]), 4);
        assert!(incoming.read_with(0, |_| unreachable!()).is_none());
        incoming.write(b"lost");
        assert_eq!(incoming.remaining(), 0);
        incoming.finish(&state, socket(&ctx)).unwrap();
    }

    /// An answer that a closed connection refuses fails the frame at once;
    /// it is neither deferred nor reported as sent.
    #[tokio::test]
    async fn acknowledgements_refused_by_a_closed_connection_fail_the_frame() {
        let state = state();
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        let tcp = TcpSocket::new(sender);
        let socket = Socket::TCP(tcp.clone());
        let frame = |ack| {
            let head = DataHead {
                id: u64::MAX,
                ack,
                ranges: Vec::new(),
            };
            IncomingData::begin(&state, &socket, head, 0)
        };
        assert!(tcp.mark_closed());
        let error = frame(Some(1)).finish(&state, &socket).unwrap_err();
        assert_eq!(error.kind, ErrorKind::ConnectionClosed);
        assert!(queue.try_recv().is_err());
        // A frame that asks for no answer has nothing to send.
        frame(None).finish(&state, &socket).unwrap();
    }

    /// An acknowledgement that finds the send queue full waits in a task,
    /// never in the receive loop. A peer that keeps asking without reading
    /// gets only so many such tasks before the frame fails its connection.
    #[tokio::test]
    async fn acknowledgements_for_a_peer_that_does_not_read_are_bounded() {
        let state = state();
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(Bytes::from_static(b"occupied").into())
            .unwrap();
        let socket = Socket::TCP(TcpSocket::new(sender.clone()));
        // The frames name no pending request; each still asks for an answer.
        let finish = |ack: u64| {
            let head = DataHead {
                id: u64::MAX,
                ack: Some(ack),
                ranges: Vec::new(),
            };
            IncomingData::begin(&state, &socket, head, 0).finish(&state, &socket)
        };
        for ack in 0..MAX_DEFERRED_RESPONSES as u64 {
            finish(ack).unwrap();
        }
        assert_eq!(finish(u64::MAX).unwrap_err().kind, ErrorKind::Overloaded);

        // A peer that reads gets every deferred answer, and its slots back.
        assert_eq!(queue.recv().await.unwrap().into_bytes(), b"occupied"[..]);
        let mut answered = Vec::new();
        while answered.len() < MAX_DEFERRED_RESPONSES {
            let answer = Sent::decode(queue.recv().await.unwrap()).message();
            let result = answer.payload.deserialize::<Result<()>>(&answer.meta);
            assert_eq!(result.unwrap().unwrap_err().kind, ErrorKind::Timeout);
            answered.push(answer.meta.msgid);
        }
        answered.sort_unstable();
        assert!(
            answered
                .iter()
                .copied()
                .eq(0..MAX_DEFERRED_RESPONSES as u64)
        );
        // Every slot came back with its task: a full queue defers again.
        sender
            .try_send(Bytes::from_static(b"occupied").into())
            .unwrap();
        finish(7).unwrap();
        assert_eq!(queue.recv().await.unwrap().into_bytes(), b"occupied"[..]);
        let answer = Sent::decode(queue.recv().await.unwrap()).message();
        assert_eq!(answer.meta.msgid, 7);
    }

    /// Every frame is counted once where it is sent and once where it is
    /// consumed, with its raw bytes; a frame that reached no write space
    /// also counts as discarded.
    #[test]
    fn data_frames_are_counted_where_they_are_sent_and_consumed() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let len = MAX_DATA_LEN + 5;
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let state = state();
                let (base, mut queue) = handler(&state, 0);
                let (target, id, _pending) = pending_target(&base, &[&vec![0; len]]);
                // The connection loops back: the handler pushes to a request
                // that is pending on its own connection.
                let mut ctx = base.clone();
                ctx.msg_meta.msgid = id;
                let set = BufferSet::new(vec![filled(&state, &vec![7; len])]);
                let layout = set.layout().clone();
                let (_lent, sources) = LentSources::lend(set.into_buffers());
                let ops = [CopyOp::new(0, 0, len as u64)];
                let batch = CopyBatch::new(&ops).unwrap();
                let deadline = Instant::now() + Duration::from_secs(5);
                let pushing = push(&ctx, socket(&ctx), &batch, &layout, &sources, deadline);
                let peer = async {
                    loop {
                        let frame = queue.recv().await.unwrap();
                        crate::sockets::test_util::deliver(&state, socket(&ctx), frame, 4096);
                    }
                };
                tokio::select! {
                    result = pushing => result.unwrap(),
                    () = peer => unreachable!(),
                }
                assert_eq!(target.received(), len as u64);
                // A frame for nobody is consumed and discarded.
                let mut lost =
                    IncomingData::begin(&state, socket(&ctx), head(u64::MAX, &[(0, 3)]), 3);
                lost.write(b"abc");
                lost.finish(&state, socket(&ctx)).unwrap();
            });
        });
        let values = snapshotter.snapshot().into_vec();
        let counter = |name: &str, direction: Option<&str>| {
            values
                .iter()
                .find(|(key, ..)| {
                    let key = key.key();
                    key.name() == name
                        && direction.is_none_or(|direction| {
                            key.labels().any(|label| label.value() == direction)
                        })
                })
                .map(|(.., value)| match value {
                    DebugValue::Counter(value) => *value,
                    other => panic!("{name} is not a counter: {other:?}"),
                })
                .unwrap_or_else(|| panic!("{name} was not recorded"))
        };
        assert_eq!(counter("ruapc_data_frames_total", Some("sent")), 2);
        assert_eq!(counter("ruapc_data_bytes_total", Some("sent")), len as u64);
        assert_eq!(counter("ruapc_data_frames_total", Some("received")), 3);
        assert_eq!(
            counter("ruapc_data_bytes_total", Some("received")),
            len as u64 + 3
        );
        assert_eq!(counter("ruapc_data_frames_discarded_total", None), 1);
    }

    /// The largest piece the splitter can produce still encodes its head
    /// within the wire limit: the batch and frame limits are coupled, because
    /// one piece becomes one frame head.
    #[test]
    fn worst_case_piece_head_fits_the_data_frame_limit() {
        let len = (MAX_DATA_LEN as u64) / MAX_COPY_OPS as u64;
        // 64-bit offsets make every range encode at its longest, and the
        // lengths fill exactly one frame.
        let ops: Vec<CopyOp> = (0..MAX_COPY_OPS)
            .map(|index| CopyOp::new(0, (1u64 << 40) + index as u64 * len, len))
            .collect();
        assert_eq!(
            ops.iter().map(|op| op.len).sum::<u64>(),
            MAX_DATA_LEN as u64
        );
        let (head, len) = encode_head(7, None, &ops, false).unwrap();
        assert_eq!(len, MAX_DATA_LEN);
        assert!(
            head.len() <= MAX_DATA_HEAD_LEN,
            "worst-case piece head is {} bytes, over the {MAX_DATA_HEAD_LEN}-byte limit",
            head.len()
        );
    }
}
