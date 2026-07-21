//! Type-driven dispatch glue for `#[service]`-generated client methods.
//!
//! The macro emits one uniform body for every client method:
//!
//! ```rust,ignore
//! (&RpcCall::<ReturnType>::new()).ruapc_call(self, ctx, req, "Svc/method").await
//! ```
//!
//! Which implementation runs is decided by the *type system*, not by the
//! macro (no name-based detection, so type aliases and custom error types
//! work):
//!
//! - [`CallWithBuffer`] is implemented for
//!   `RpcCall<Result<WithBuffer<T>, E>>` and additionally delivers the
//!   buffer pushed by the server.
//! - [`CallPlain`] is implemented for `&RpcCall<Result<T, E>>` and performs
//!   an ordinary request.
//!
//! The two impl sets cannot overlap: `WithBuffer<T>` is deliberately not
//! `Deserialize`, so it never satisfies the plain impl's bounds, while the
//! buffer impl only exists for `Result<WithBuffer<T>, E>`. Method resolution
//! prefers `CallWithBuffer` (fewer autorefs) whenever it applies.

use std::marker::PhantomData;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Buffer, Client, ClientWithBuffer, Context, Error, WithBuffer};

/// Uniform request entry point implemented by [`Client`] and
/// [`ClientWithBuffer`]; used by the generated call glue.
#[doc(hidden)]
pub trait RawCall {
    /// Sends a request; when `slot` is provided, a buffer pushed by the
    /// server during the call is delivered into it.
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut Option<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>;
}

impl RawCall for Client {
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut Option<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        self.ruapc_request(ctx, req, None, slot, method_name).await
    }
}

impl RawCall for ClientWithBuffer<'_> {
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut Option<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        let result = self.ruapc_request(ctx, req, method_name).await;
        // Only drain the internally stored write buffer when the caller
        // asked for it; plain calls keep it available for the untyped
        // `take_write_buffer` API.
        if let Some(slot) = slot {
            *slot = self.take_write_buffer();
        }
        result
    }
}

/// Zero-sized dispatcher tag parameterized by a method's return type.
#[doc(hidden)]
pub struct RpcCall<Rt>(PhantomData<Rt>);

impl<Rt> RpcCall<Rt> {
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Rt> Default for RpcCall<Rt> {
    fn default() -> Self {
        Self::new()
    }
}

/// Call glue for methods returning `Result<WithBuffer<T>, E>`.
#[doc(hidden)]
pub trait CallWithBuffer {
    type Rsp;
    type Err;

    async fn ruapc_call<C, Req>(
        &self,
        client: &C,
        ctx: &Context,
        req: &Req,
        method_name: &str,
    ) -> std::result::Result<WithBuffer<Self::Rsp>, Self::Err>
    where
        C: RawCall,
        Req: Serialize + JsonSchema;
}

impl<T, E> CallWithBuffer for RpcCall<std::result::Result<WithBuffer<T>, E>>
where
    T: for<'c> Deserialize<'c> + JsonSchema,
    E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
{
    type Rsp = T;
    type Err = E;

    async fn ruapc_call<C, Req>(
        &self,
        client: &C,
        ctx: &Context,
        req: &Req,
        method_name: &str,
    ) -> std::result::Result<WithBuffer<T>, E>
    where
        C: RawCall,
        Req: Serialize + JsonSchema,
    {
        let mut slot: Option<Buffer> = None;
        let result: std::result::Result<T, E> = client
            .ruapc_raw_call(ctx, req, Some(&mut slot), method_name)
            .await;
        match (result, slot) {
            (Err(e), _) => Err(e),
            (Ok(rsp), Some(buffer)) => Ok(WithBuffer::assemble(rsp, buffer)),
            // The absence of a pushed buffer is the wire encoding of an
            // empty reply: the server provably constructed a WithBuffer
            // (the return type demands it), so materialize an empty buffer
            // locally without allocating memory.
            (Ok(rsp), None) => Ok(WithBuffer::assemble(
                rsp,
                Buffer::empty(&ctx.state.buffer_pool),
            )),
        }
    }
}

/// Call glue for plain `Result<T, E>` methods.
#[doc(hidden)]
pub trait CallPlain {
    type Rsp;
    type Err;

    async fn ruapc_call<C, Req>(
        &self,
        client: &C,
        ctx: &Context,
        req: &Req,
        method_name: &str,
    ) -> std::result::Result<Self::Rsp, Self::Err>
    where
        C: RawCall,
        Req: Serialize + JsonSchema;
}

impl<T, E> CallPlain for &RpcCall<std::result::Result<T, E>>
where
    T: for<'c> Deserialize<'c> + JsonSchema,
    E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
{
    type Rsp = T;
    type Err = E;

    async fn ruapc_call<C, Req>(
        &self,
        client: &C,
        ctx: &Context,
        req: &Req,
        method_name: &str,
    ) -> std::result::Result<T, E>
    where
        C: RawCall,
        Req: Serialize + JsonSchema,
    {
        client.ruapc_raw_call(ctx, req, None, method_name).await
    }
}
