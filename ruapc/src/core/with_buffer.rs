//! Typed response-with-buffer contract for `remote_write`.

use crate::Buffer;

/// Witness that a buffer has been transferred to the client.
///
/// Returned by [`Context::remote_write`](crate::Context::remote_write) after
/// the push completed; the pushed buffer stays inside. Pair it with a
/// response value via [`reply`](Self::reply) to build the [`WithBuffer`]
/// return value — the response can thus be computed *after* the transfer
/// (e.g. include the observed push latency):
///
/// ```rust,ignore
/// let t0 = std::time::Instant::now();
/// let sent = ctx.remote_write(buf).await?;      // transfer completes here
/// let rsp = Stats { push_micros: t0.elapsed().as_micros() as u64 };
/// Ok(sent.reply(rsp))
/// ```
#[derive(Debug)]
pub struct SentBuffer {
    buffer: Buffer,
}

impl SentBuffer {
    /// Crate-internal: only a completed `remote_write` produces a witness.
    pub(crate) fn new(buffer: Buffer) -> Self {
        Self { buffer }
    }

    /// Returns a reference to the buffer that was pushed.
    #[must_use]
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Pairs the completed transfer with a response value.
    #[must_use]
    pub fn reply<T>(self, rsp: T) -> WithBuffer<T> {
        WithBuffer::assemble(rsp, self.buffer)
    }
}

/// A response value paired with a buffer transferred via `remote_write`.
///
/// Declaring a `#[service]` method with the return type
/// `Result<WithBuffer<T>, E>` (any alias of it, e.g. [`ResultWithBuffer`],
/// works — detection is by type, not by name) makes the buffer transfer part
/// of the method's contract on both sides:
///
/// - **Server**: `WithBuffer` can only be produced by a completed
///   [`Context::remote_write`] + [`SentBuffer::reply`]. The push happens
///   inside the handler — measurable, retryable, impossible to forget. For
///   code paths with no payload, use [`Buffer::empty`] to create a
///   zero-length buffer: `remote_write` short-circuits without touching
///   the network.
/// - **Client**: the generated client method returns
///   `Result<WithBuffer<T>, E>`, so the transferred buffer arrives together
///   with the response and cannot be forgotten either. When the server
///   replied without a payload, the buffer is empty (`len() == 0`).
///
/// On the wire the response is just `T` (`WithBuffer` serializes
/// transparently); the buffer travels out-of-band, and an empty reply
/// involves no transfer at all.
///
/// # Examples
///
/// ```rust,ignore
/// #[ruapc::service]
/// trait BlobService {
///     async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffer<u64>>;
/// }
///
/// // Server handler: push first, decide the response afterwards.
/// async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffer<u64>> {
///     let mut buf = ctx.state.buffer_pool.allocate(req.len)?;
///     // ... fill buf ...
///     buf.set_len(req.len);
///     let t0 = std::time::Instant::now();
///     let sent = ctx.remote_write(buf).await?;
///     Ok(sent.reply(t0.elapsed().as_micros() as u64))
/// }
///
/// // Empty payload: Buffer::empty costs no memory.
/// async fn metadata_only(&self, ctx: &Context, _req: &()) -> Result<WithBuffer<Meta>> {
///     let sent = ctx.remote_write(Buffer::empty(&ctx.state.buffer_pool)).await?;
///     Ok(sent.reply(Meta { version: 1 }))
/// }
///
/// // Client:
/// let (push_micros, buffer) = client.download(&ctx, &req).await?.into_parts();
/// ```
///
/// [`Context::remote_write`]: crate::Context::remote_write
/// [`Buffer::empty`]: ruapc_bufpool::Buffer::empty
#[derive(Debug)]
pub struct WithBuffer<T> {
    /// The response payload carried on the wire.
    rsp: T,
    /// Server side: the pushed buffer (kept alive until the response is
    /// sent). Client side: the received buffer.
    buffer: Buffer,
}

impl<T> WithBuffer<T> {
    /// Crate-internal constructor.
    ///
    /// Deliberately not public: on the server it is only reachable through
    /// a completed push (`SentBuffer::reply`), and on the client through
    /// the generated call glue — which is what makes the value a witness
    /// of the contract being fulfilled.
    pub(crate) fn assemble(rsp: T, buffer: Buffer) -> Self {
        Self { rsp, buffer }
    }

    /// Returns a reference to the response value.
    #[must_use]
    pub fn rsp(&self) -> &T {
        &self.rsp
    }

    /// Returns a reference to the transferred buffer.
    #[must_use]
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Decomposes into the response value and the transferred buffer.
    #[must_use]
    pub fn into_parts(self) -> (T, Buffer) {
        (self.rsp, self.buffer)
    }

    /// Consumes the pair, returning only the transferred buffer.
    #[must_use]
    pub fn into_buffer(self) -> Buffer {
        self.buffer
    }
}

/// Serializes transparently as the inner response value; the buffer never
/// travels inline (it is transferred out-of-band via `remote_write`).
impl<T: serde::Serialize> serde::Serialize for WithBuffer<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.rsp.serialize(serializer)
    }
}

/// Schema-transparent: the wire response schema is the inner `T`'s.
impl<T: schemars::JsonSchema> schemars::JsonSchema for WithBuffer<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        T::schema_id()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }

    fn inline_schema() -> bool {
        T::inline_schema()
    }
}

/// Convenience alias for `#[service]` methods that push a buffer to the
/// client; see [`WithBuffer`].
///
/// This is an ordinary type alias — the contract is recognized by the
/// underlying type, so custom aliases (including ones fixing a custom error
/// type) work just as well.
pub type ResultWithBuffer<T, E = crate::Error> = std::result::Result<WithBuffer<T>, E>;
