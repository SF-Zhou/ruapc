//! Typed response-with-buffers contract for `remote_write`.

use crate::Buffer;

/// Witness that data has been transferred into the client's pre-pinned
/// write buffers (or that there was provably nothing to transfer).
///
/// Returned by [`Context::remote_write`](crate::Context::remote_write)
/// after the transfer completed; the server-side source buffers ride
/// inside and can be reclaimed for reuse via
/// [`take_buffers`](Self::take_buffers). Pair the witness with a response
/// value via [`reply`](Self::reply) to build the [`WithBuffers`] return
/// value — the response can thus be computed *after* the transfer (e.g.
/// include the observed push latency):
///
/// ```rust,ignore
/// let t0 = std::time::Instant::now();
/// let sent = ctx.remote_write_all(bufs).await?;   // transfer completes here
/// let rsp = Stats { push_micros: t0.elapsed().as_micros() as u64 };
/// Ok(sent.reply(rsp))
/// ```
///
/// Several writes within one handler combine with [`merge`](Self::merge).
#[derive(Debug)]
pub struct SentBuffers {
    buffers: Vec<Buffer>,
}

impl SentBuffers {
    /// Crate-internal: only a completed `remote_write` (or the explicit
    /// no-op [`Context::sent_nothing`](crate::Context::sent_nothing))
    /// produces a witness.
    pub(crate) fn new(buffers: Vec<Buffer>) -> Self {
        Self { buffers }
    }

    /// The local source buffers of the completed transfer(s).
    #[must_use]
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Reclaims the local source buffers for reuse; the witness itself
    /// stays valid for [`reply`](Self::reply).
    pub fn take_buffers(&mut self) -> Vec<Buffer> {
        std::mem::take(&mut self.buffers)
    }

    /// Combines the witness of another completed write into this one.
    pub fn merge(&mut self, mut other: SentBuffers) {
        self.buffers.append(&mut other.buffers);
    }

    /// Pairs the completed transfer(s) with a response value.
    #[must_use]
    pub fn reply<T>(self, rsp: T) -> WithBuffers<T> {
        // The server-side source buffers are released here (the transfer
        // already completed); only the response value goes on the wire.
        drop(self.buffers);
        WithBuffers::assemble(rsp, Vec::new())
    }
}

/// A response value paired with the buffers transferred out-of-band.
///
/// Declaring a `#[service]` method with the return type
/// `Result<WithBuffers<T>, E>` (any alias of it, e.g. [`ResultWithBuffers`],
/// works — detection is by type, not by name) makes the buffer transfer
/// part of the method's contract on both sides:
///
/// - **Server**: `WithBuffers` can only be produced by a completed
///   [`Context::remote_write`] + [`SentBuffers::reply`] (or the explicit
///   [`Context::sent_nothing`](crate::Context::sent_nothing) for paths
///   with no payload). The transfer happens inside the handler —
///   measurable, retryable, impossible to forget.
/// - **Client**: the caller pre-provides the destination buffers with
///   [`Client::with_write_buffers`](crate::Client::with_write_buffers);
///   the generated method returns `Result<WithBuffers<T>, E>` carrying
///   *all* of those buffers back, whether or not the server wrote into
///   them. A call made without attached write buffers yields an empty
///   buffer list.
///
/// On the wire the response is just `T` (`WithBuffers` serializes
/// transparently); the data travels out-of-band through the pull/push
/// protocol into the client's pinned buffers.
///
/// # Examples
///
/// ```rust,ignore
/// #[ruapc::service]
/// trait BlobService {
///     async fn download(&self, ctx: &Context, req: &DownloadReq)
///         -> Result<WithBuffers<u64>>;
/// }
///
/// // Server handler: write first, decide the response afterwards.
/// async fn download(&self, ctx: &Context, req: &DownloadReq) -> Result<WithBuffers<u64>> {
///     let mut buf = ctx.state.buffer_pool.allocate(req.len)?;
///     // ... fill buf, set_len ...
///     let sent = ctx.remote_write_all(vec![buf]).await?;
///     Ok(sent.reply(req.len as u64))
/// }
///
/// // Client: provide the destination buffers, get them all back.
/// let (len, bufs) = client
///     .with_write_buffers(vec![buf_a, buf_b])
///     .download(&ctx, &req)
///     .await?
///     .into_parts();
/// ```
///
/// [`Context::remote_write`]: crate::Context::remote_write
#[derive(Debug)]
pub struct WithBuffers<T> {
    /// The response payload carried on the wire.
    rsp: T,
    /// Client side: every buffer the caller attached via
    /// `with_write_buffers`, returned after the call. Server side: empty
    /// (the source buffers were released by `reply`).
    buffers: Vec<Buffer>,
}

impl<T> WithBuffers<T> {
    /// Crate-internal constructor.
    ///
    /// Deliberately not public: on the server it is only reachable through
    /// a completed transfer (`SentBuffers::reply`), and on the client
    /// through the generated call glue — which is what makes the value a
    /// witness of the contract being fulfilled.
    pub(crate) fn assemble(rsp: T, buffers: Vec<Buffer>) -> Self {
        Self { rsp, buffers }
    }

    /// Returns a reference to the response value.
    #[must_use]
    pub fn rsp(&self) -> &T {
        &self.rsp
    }

    /// Returns the returned buffers.
    #[must_use]
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Decomposes into the response value and the returned buffers.
    #[must_use]
    pub fn into_parts(self) -> (T, Vec<Buffer>) {
        (self.rsp, self.buffers)
    }

    /// Consumes the pair, returning only the buffers.
    #[must_use]
    pub fn into_buffers(self) -> Vec<Buffer> {
        self.buffers
    }
}

/// Serializes transparently as the inner response value; the buffers never
/// travel inline (data is transferred out-of-band via `remote_write`).
impl<T: serde::Serialize> serde::Serialize for WithBuffers<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.rsp.serialize(serializer)
    }
}

/// Schema-transparent: the wire response schema is the inner `T`'s.
impl<T: schemars::JsonSchema> schemars::JsonSchema for WithBuffers<T> {
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

/// Convenience alias for `#[service]` methods whose response carries
/// buffers transferred via `remote_write`; see [`WithBuffers`].
///
/// This is an ordinary type alias — the contract is recognized by the
/// underlying type, so custom aliases (including ones fixing a custom error
/// type) work just as well.
pub type ResultWithBuffers<T, E = crate::Error> = std::result::Result<WithBuffers<T>, E>;
