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
//!   `RpcCall<Result<WithBuffers<T>, E>>` and additionally returns the
//!   write buffers the caller attached to the request.
//! - [`CallPlain`] is implemented for `&RpcCall<Result<T, E>>` and performs
//!   an ordinary request.
//!
//! The two impl sets cannot overlap: `WithBuffers<T>` is deliberately not
//! `Deserialize`, so it never satisfies the plain impl's bounds, while the
//! buffer impl only exists for `Result<WithBuffers<T>, E>`. Method
//! resolution prefers `CallWithBuffer` (fewer autorefs) whenever it
//! applies.

use std::marker::PhantomData;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Buffer, Client, ClientWithBuffers, Context, Error, WithBuffers};

/// Uniform request entry point implemented by [`Client`] and
/// [`ClientWithBuffers`]; used by the generated call glue.
#[doc(hidden)]
pub trait RawCall {
    /// Sends a request; when `slot` is provided, the write buffers
    /// attached to the request are delivered into it after the call.
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut Vec<Buffer>>,
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
        slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        self.ruapc_request(ctx, req, &[], &mut None, slot, method_name)
            .await
    }
}

impl RawCall for ClientWithBuffers<'_> {
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut Vec<Buffer>>,
        method_name: &str,
    ) -> std::result::Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        self.ruapc_request(ctx, req, slot, method_name).await
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

/// Call glue for methods returning `Result<WithBuffers<T>, E>`.
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
    ) -> std::result::Result<WithBuffers<Self::Rsp>, Self::Err>
    where
        C: RawCall,
        Req: Serialize + JsonSchema;
}

impl<T, E> CallWithBuffer for RpcCall<std::result::Result<WithBuffers<T>, E>>
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
    ) -> std::result::Result<WithBuffers<T>, E>
    where
        C: RawCall,
        Req: Serialize + JsonSchema,
    {
        let mut slot: Vec<Buffer> = Vec::new();
        let result: std::result::Result<T, E> = client
            .ruapc_raw_call(ctx, req, Some(&mut slot), method_name)
            .await;
        // Every buffer the caller attached via `with_write_buffers` comes
        // back in `slot`; a call without attached write buffers yields an
        // empty list.
        result.map(|rsp| WithBuffers::assemble(rsp, slot))
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
